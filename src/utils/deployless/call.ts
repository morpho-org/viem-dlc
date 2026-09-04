import { BaseError, type EIP1193RequestFn, type Hex, type PublicRpcSchema } from "viem";

import type { Facet } from "../../observability.js";
import type { EIP1193Parameters } from "../../types.js";
import { isTimeoutLikeError } from "../errors.js";
import type { Tail } from "../tuples.js";

import {
  type DeploylessTarget,
  envelopeConfig,
  extractRevertData,
  isCounterfactualDeployFailedRevert,
  isMalformedInputRevert,
  isMalformedResultRevert,
  isOutOfGasRevert,
  wrapDeploylessFactoryCall,
} from "./codec.envelope.js";
import { arrayToWire, hexToPage, type Page, type PageGas, type ResolvedArrayFunction } from "./codec.inner.js";

type RestOfEthCallParams = Tail<EIP1193Parameters<PublicRpcSchema, "eth_call">["params"]>;

type FactorisedFactoryCallParams = {
  target: DeploylessTarget;
  elements: readonly Hex[];
  solidity: ResolvedArrayFunction;
  batch?: {
    batchSize?: number;
    compress?: boolean;
    gas?: LensGas;
  };
  /** The provider's `eth_call` gas cap; with `batch.gas`, sizes the opening wave. */
  gasLimit?: number;
  restOfEthCallParams: RestOfEthCallParams;
  /**
   * Invoked with each freshly fetched element as its chunk lands, before siblings finish, and
   * awaited — so a caller's results survive a later chunk failing.
   */
  onResolved?: (entries: readonly ResolvedElement[]) => void | Promise<void>;
  facet?: Facet;
};

/**
 * A lens's cost as the caller states it, in the units the wide event reports: `fixed` is what a
 * frame spends before its first attempt (`fixed_gas`), `item` the per-attempt mean and deviation
 * (`item_gas_avg`, `item_gas_stddev`). Both are properties of the lens, not of any provider.
 */
export type LensGas = { fixed: number; item: { avg: number; stddev?: number } };

/** An input element's index paired with the raw output bytes fetched for it. */
export type ResolvedElement = { index: number; output: Hex };

export type FactorisedFactoryCallResult = {
  /** Per-element outputs aligned to `elements`, sparse exactly at {@link FactorisedFactoryCallResult.missing}. */
  outputs: readonly (Hex | undefined)[];
  /** Ascending indices no chunk could serve: declined by the lens, declined for size, or unresolved by gas. */
  missing: readonly number[];
  /** The subset of `missing` that gas could not resolve even as a singleton; another provider might. */
  unresolved: readonly number[];
  /** The subset of `missing` declined client-side for size, with no request made. */
  oversize: readonly number[];
};

/**
 * Packs `elements` into deployless-factory `eth_call` chunks under the wire budget
 * (`batch.batchSize`, at most EIP-3860's initcode cap) and, for the opening wave, under the gas
 * `gasLimit` and `batch.gas` predict; fetches them in parallel; returns per-element outputs aligned
 * to `elements`. Every page reports how far it got and what its attempts cost, so the waves after
 * the first are packed by {@link predictItems} and an element gas could not resolve is retried once
 * alone.
 */
export async function factorisedFactoryCall(
  requestFn: EIP1193RequestFn<PublicRpcSchema>,
  { target, elements, solidity, batch, gasLimit, restOfEthCallParams, onResolved, facet }: FactorisedFactoryCallParams,
): Promise<FactorisedFactoryCallResult> {
  const compress = batch?.compress ?? false;
  const missing: number[] = [];
  const unresolved: number[] = [];
  const oversize: number[] = [];
  const config = envelopeConfig(solidity, compress);
  const wrap = (start: number, end: number): Hex =>
    wrapDeploylessFactoryCall(
      { target, targetData: arrayToWire(solidity.inputLayout, elements.slice(start, end)) },
      { compress, config },
    );

  let referenceWrapped: Hex | undefined;
  const getReferenceWrapped = () => {
    if (!referenceWrapped) referenceWrapped = wrap(0, elements.length);
    return referenceWrapped;
  };

  // Static layouts contribute `layout.size` per element; dynamic ones a length word plus their
  // padded bytes. Both are multiples of 32, so the wrapper's own padding is a per-batch constant.
  const prefixBytes = [0];
  const prefixZeros = [0];
  const layout = solidity.inputLayout;
  for (let pos = 0; pos < elements.length; pos++) {
    const element = elements[pos]!;
    const bytes = layout.mode === "static" ? layout.size : 32 + hexByteLength(element);
    const zeros = zeroBytes(element) + (layout.mode === "static" ? 0 : 32 - nonzeroBytesOf(hexByteLength(element)));
    prefixBytes.push(prefixBytes[pos]! + bytes);
    prefixZeros.push(prefixZeros[pos]! + zeros);
  }
  let overhead: WireSize | undefined;
  const measureWire = compress
    ? (start: number, end: number): WireSize =>
        wireSize(start === 0 && end === elements.length ? getReferenceWrapped() : wrap(start, end))
    : (start: number, end: number): WireSize => {
        if (overhead === undefined) {
          const whole = wireSize(getReferenceWrapped());
          const n = elements.length;
          overhead = {
            bytes: whole.bytes - prefixBytes[n]!,
            zeros: whole.zeros - prefixZeros[n]! - countingWordZeros(n, prefixBytes[n]!),
          };
        }
        const body = prefixBytes[end]! - prefixBytes[start]!;
        return {
          bytes: overhead.bytes + body,
          zeros: overhead.zeros + prefixZeros[end]! - prefixZeros[start]! + countingWordZeros(end - start, body),
        };
      };

  const wireCap = batch?.batchSize && batch.batchSize > 0 ? batch.batchSize : Infinity;
  const opening = openingBudget(gasLimit, batch?.gas);
  // The prediction only ever shortens a chunk: a lone element is sent whatever it is predicted to
  // cost, so the envelope, not the estimate, decides whether it is served.
  const fits = (start: number, end: number) => {
    if (wireCap === Infinity && opening === undefined) return true;
    const size = measureWire(start, end);
    if (size.bytes > wireCap) return false;
    const k = end - start;
    if (opening === undefined || k === 1) return true;
    return intrinsicGas(size) + opening.fixed + chunkCost(k, opening.avg, opening.stddev) <= opening.gasLimit;
  };

  const packed = packBatches({ count: elements.length, maxItems: Infinity, fits });
  oversize.push(...packed.oversize);
  missing.push(...oversize);
  const ranges = packed.ranges;
  const outputs = new Array<Hex>(elements.length);

  facet?.set({
    elements_requested: elements.length,
    nominal_batches: ranges.length,
    ...(opening === undefined ? {} : { gas_limit: opening.gasLimit }),
  });
  // Sizes of the *initial* packing, to compare realized utilization against the wire budget.
  // Halved children and continuations are not resampled. Guarded rather than
  // `facet?.stat(...)` so unobserved calls skip re-measuring.
  if (facet) for (const [start, end] of ranges) facet.stat("batch_bytes", measureWire(start, end).bytes);
  let fetched = 0;
  const splits = { count: 0, size: 0, timeout: 0, maxDepth: 0 };
  // A lens stopping early is a continuation, a mid-page gas death an escalation; neither is a
  // split, which means only "the provider refused the request's size or timed out".
  const pages = { continued: 0, waves: 0, escalated: 0, unresolvedAttempts: 0, allSkipped: 0 };
  let gas: GasStats | undefined;

  const commit = async (entries: readonly ResolvedElement[]) => {
    for (const { index, output } of entries) outputs[index] = output;
    fetched += entries.length;
    if (entries.length > 0) await onResolved?.(entries);
  };

  /** Re-packs `[from, to)` under the wire budget and an item cap. */
  const packRange = (from: number, to: number, maxItems: number): BatchRange[] =>
    packBatches({
      count: to - from,
      maxItems,
      fits: (s, e) => fits(from + s, from + e),
    }).ranges.map(([s, e]) => [from + s, from + e] as const);

  const fetchRecursive = async (
    [start, end]: BatchRange,
    nextWave: Deferred,
    precomputed?: Hex,
    timeoutSplitsRemaining = 1,
    depth = 0,
  ): Promise<void> => {
    if (depth > splits.maxDepth) splits.maxDepth = depth;
    const count = end - start;
    const wrapped = precomputed ?? wrap(start, end);

    let returndata: Hex;
    try {
      returndata = await fetchChunk(requestFn, wrapped, restOfEthCallParams);
    } catch (e) {
      if (isMalformedResultRevert(e)) {
        throw new Error("[deployless] lens returned a per-item result that does not fit its declared layout", {
          cause: e,
        });
      }
      if (isMalformedInputRevert(e)) {
        throw new Error("[deployless] envelope rejected the input wire (codec bug)", { cause: e });
      }
      if (isCounterfactualDeployFailedRevert(e)) {
        throw new Error(
          "[deployless] counterfactual deploy failed: target occupied, constructor reverted, or no code",
          {
            cause: e,
          },
        );
      }
      if (isOutOfGasRevert(e)) {
        throw new Error(
          "[deployless] counterfactual deploy (factory or constructor) ran out of gas under this node's cap",
          {
            cause: e,
          },
        );
      }
      const halve = (nextBudget = timeoutSplitsRemaining) => {
        const mid = start + Math.floor(count / 2);
        return settleAll([
          fetchRecursive([start, mid], nextWave, undefined, nextBudget, depth + 1),
          fetchRecursive([mid, end], nextWave, undefined, nextBudget, depth + 1),
        ]);
      };
      const cause = classifyChunkError(e);
      if (cause === "size" && count > 1) {
        splits.count += 1;
        splits.size += 1;
        return halve();
      }
      if (cause === "timeout" && count > 1 && timeoutSplitsRemaining > 0) {
        splits.count += 1;
        splits.timeout += 1;
        return halve(timeoutSplitsRemaining - 1);
      }
      throw e;
    }

    const page = hexToPage(solidity.outputLayout, returndata);
    const attempted = validatePage(page, count);
    gas = pool(gas, page.gas, attempted - (page.died === undefined ? 0 : 1), intrinsicGas(wireSize(wrapped)));
    facet?.stat("page_adjudicated", attempted);
    if (page.died === undefined && page.results.length === 0 && page.skipped.length > 0) pages.allSkipped += 1;

    const declined = new Set(page.skipped);
    const entries: ResolvedElement[] = [];
    for (let i = 0, served = 0; i < attempted; i++) {
      if (i === page.died) continue;
      if (declined.has(i)) missing.push(start + i);
      else entries.push({ index: start + i, output: page.results[served++]! });
    }
    await commit(entries);

    if (page.died !== undefined) {
      pages.unresolvedAttempts += 1;
      const pos = start + page.died;
      if (count > 1) {
        pages.escalated += 1;
        nextWave.singletons.push([pos, pos + 1]);
      } else {
        missing.push(pos);
        unresolved.push(pos);
      }
    }

    if (attempted < count) {
      pages.continued += 1;
      nextWave.tails.push([start + attempted, end]);
    }
  };

  let wave: BatchRange[] = ranges;
  try {
    while (wave.length > 0) {
      pages.waves += 1;
      const nextWave: Deferred = { tails: [], singletons: [] };
      const isWholeInput = wave.length === 1 && wave[0]![0] === 0 && wave[0]![1] === elements.length;
      await settleAll(
        wave.map((range) => fetchRecursive(range, nextWave, isWholeInput ? getReferenceWrapped() : undefined)),
      );
      // Tails are packed only once the whole wave has reported, so every one sees the same pool.
      const cap = gas === undefined ? Infinity : predictItems(gas);
      wave = [...nextWave.singletons, ...nextWave.tails.flatMap(([from, to]) => packRange(from, to, cap))];
    }
  } finally {
    if (gas !== undefined) facet?.set(gasFields(gas));
    facet?.set({
      elements_fetched: fetched,
      splits_count: splits.count,
      splits_size: splits.size,
      splits_timeout: splits.timeout,
      splits_max_depth: splits.maxDepth,
      attempts_unresolved: pages.unresolvedAttempts,
      pages_escalated: pages.escalated,
      pages_all_skipped: pages.allSkipped,
      pages_continued: pages.continued,
      pages_waves: pages.waves,
      elements_declined_oversize: oversize.length,
      elements_missing: missing.length,
      elements_unresolved: unresolved.length,
    });
  }

  return {
    outputs,
    missing: missing.sort((a, b) => a - b),
    unresolved: unresolved.sort((a, b) => a - b),
    oversize: oversize.sort((a, b) => a - b),
  };
}

/**
 * The gas telemetry of every page a request has seen, pooled: `budget` is the smallest frame's,
 * `fixed` the largest prologue, `cap` the smallest gas limit a page's frame implies.
 */
type GasStats = PageGas & { served: bigint; cap: bigint };

/** `intrinsic` is what the node deducted for the chunk's calldata before the frame began. */
function pool(stats: GasStats | undefined, page: PageGas, served: number, intrinsic: number): GasStats {
  const cap = BigInt(intrinsic) + page.fixed + page.budget;
  if (stats === undefined) return { ...page, served: BigInt(served), cap };
  return {
    budget: page.budget < stats.budget ? page.budget : stats.budget,
    fixed: page.fixed > stats.fixed ? page.fixed : stats.fixed,
    served: stats.served + BigInt(served),
    sum: stats.sum + page.sum,
    sumSquares: stats.sumSquares + page.sumSquares,
    max: page.max > stats.max ? page.max : stats.max,
    cap: cap < stats.cap ? cap : stats.cap,
  };
}

/** `gasLimit` and `batch.gas` as the opening wave uses them, or nothing when either is missing or unusable. */
function openingBudget(gasLimit: number | undefined, gas: LensGas | undefined) {
  if (gasLimit === undefined || typeof gas !== "object" || gas === null) return undefined;
  const item = typeof gas.item === "object" && gas.item !== null ? gas.item : undefined;
  if (item === undefined) return undefined;
  const { fixed } = gas;
  const { avg } = item;
  const stddev = item.stddev ?? 0;
  const usable =
    Number.isSafeInteger(gasLimit) &&
    gasLimit > 0 &&
    Number.isFinite(fixed) &&
    fixed >= 0 &&
    Number.isFinite(avg) &&
    avg > 0 &&
    Number.isFinite(stddev) &&
    stddev >= 0;
  return usable ? { gasLimit, fixed, avg, stddev } : undefined;
}

type WireSize = { bytes: number; zeros: number };

function wireSize(hex: Hex): WireSize {
  return { bytes: hexByteLength(hex), zeros: zeroBytes(hex) };
}

/**
 * What a node deducts from its cap before the envelope's first `gas()` returns: the transaction and
 * creation base, calldata by byte (EIP-2028), initcode by word (EIP-3860) and that opcode's own 2.
 * Ethereum's schedule; a chain that prices differently shifts `gas_limit_observed` by the difference.
 */
function intrinsicGas({ bytes, zeros }: WireSize): number {
  return 21_000 + 32_000 + 4 * zeros + 16 * (bytes - zeros) + 2 * Math.ceil(bytes / 32) + 2;
}

/**
 * Zero bytes in the four words of a clear chunk's wrapper that count its `k` elements and `body`
 * bytes: the wire's `n` and `bodyLen`, the ABI length of the wire (`64 + body`) and the offset of
 * `factoryData` behind it (`256 + body`). Everything else in the wrapper is the same for every chunk.
 */
function countingWordZeros(k: number, body: number): number {
  return 128 - nonzeroBytesOf(k) - nonzeroBytesOf(body) - nonzeroBytesOf(64 + body) - nonzeroBytesOf(256 + body);
}

function zeroBytes(hex: Hex): number {
  let zeros = 0;
  for (let i = 2; i < hex.length; i += 2) if (hex.charCodeAt(i) === 48 && hex.charCodeAt(i + 1) === 48) zeros++;
  return zeros;
}

/** Non-zero bytes in the 32-byte big-endian encoding of `n`. */
function nonzeroBytesOf(n: number): number {
  let count = 0;
  for (let rest = n; rest > 0; rest = Math.floor(rest / 256)) if (rest % 256 !== 0) count++;
  return count;
}

/**
 * Deviations of headroom a predicted chunk keeps below the budget. A target, not a bound: attempt
 * costs are correlated, so Cantelli's `1 / (1 + z²)` does not hold (see the page-telemetry TIB).
 * An overshoot costs one continuation, packed from more data.
 */
const PACKING_SIGMAS = 2;

/** The cost of a chunk of `k` attempts at mean `mean` and deviation `sigma` each, with headroom for the spread. */
function chunkCost(k: number, mean: number, sigma: number): number {
  return k * mean + PACKING_SIGMAS * sigma * Math.sqrt(k);
}

/**
 * The largest chunk whose {@link chunkCost}, at the pooled mean and deviation of one attempt, fits
 * the budget. `Infinity` before any attempt has been costed.
 */
function predictItems(gas: GasStats): number {
  if (gas.served === 0n) return Infinity;
  const { mean, sigma, budget } = moments(gas);
  const z = PACKING_SIGMAS;
  const fits = (k: number) => chunkCost(k, mean, sigma) <= budget;
  const root = (-z * sigma + Math.sqrt(z * z * sigma * sigma + 4 * mean * budget)) / (2 * mean);
  let k = Math.floor(root * root);
  if (fits(k + 1)) k += 1;
  while (k > 1 && !fits(k)) k -= 1;
  return Math.max(1, k);
}

/** Mean and population deviation of one attempt's cost; the variance's numerator stays exact in bigint. */
function moments({ budget, served, sum, sumSquares }: GasStats) {
  const n = Number(served);
  return {
    budget: Number(budget),
    mean: Number(sum) / n,
    sigma: Math.sqrt(Number(served * sumSquares - sum * sum)) / n,
  };
}

function gasFields(gas: GasStats): Record<string, number> {
  const frame = { frame_gas: Number(gas.budget), fixed_gas: Number(gas.fixed), gas_limit_observed: Number(gas.cap) };
  if (gas.served === 0n) return frame;
  const { mean, sigma } = moments(gas);
  return {
    ...frame,
    item_gas_avg: mean,
    item_gas_stddev: sigma,
    item_gas_max: Number(gas.max),
    page_size_suggested: predictItems(gas),
  };
}

/** `Promise.all`, but every branch settles before the first failure surfaces. */
async function settleAll(promises: readonly Promise<void>[]): Promise<void> {
  const settled = await Promise.allSettled(promises);
  const failure = settled.find((s) => s.status === "rejected");
  if (failure) throw (failure as PromiseRejectedResult).reason;
}

/**
 * Returns the number of elements the page adjudicated, in `1..count`. The floor is what keeps every
 * wave making progress; the decoder has already bound each record to its ordinal.
 */
function validatePage({ results, skipped, died }: Page, count: number): number {
  const attempted = results.length + skipped.length + (died === undefined ? 0 : 1);
  if (attempted < 1 || attempted > count) {
    throw new Error(`paginated lens attempted ${attempted} of ${count} elements, expected 1..${count}`);
  }
  return attempted;
}

async function fetchChunk(requestFn: EIP1193RequestFn<PublicRpcSchema>, data: Hex, rest: RestOfEthCallParams) {
  try {
    await requestFn({ method: "eth_call", params: [{ data }, ...rest] }, { retryCount: 0 });
  } catch (e) {
    const decoded = extractRevertData(e);
    if (!decoded.ok) throw e;
    return decoded.returnData;
  }
  throw new Error("revert-mode wrapper returned without reverting");
}

type BatchRange = readonly [start: number, end: number];

/** What a wave hands the next one: continuation tails, packed once the wave has settled, and singleton escalations. */
type Deferred = { tails: BatchRange[]; singletons: BatchRange[] };

type PackBatchesArgs = {
  count: number;
  maxItems: number;
  /** Whether `[start, end)` fits every budget. Must be monotonic in `end` for a fixed `start`. */
  fits: (start: number, end: number) => boolean;
};

/**
 * Greedy packer: each batch takes the longest prefix of the remainder that `fits`, at most
 * `maxItems` long, found by binary search with a defensive linear shrink for measures that are
 * not perfectly monotonic. An element that does not fit alone is reported in `oversize` and
 * left out rather than sent.
 */
function packBatches({ count, maxItems, fits }: PackBatchesArgs): { ranges: BatchRange[]; oversize: number[] } {
  const ranges: BatchRange[] = [];
  const oversize: number[] = [];
  const itemCap = maxItems > 0 ? maxItems : Infinity;

  let start = 0;
  while (start < count) {
    const itemCappedEnd = Math.min(count, start + itemCap);

    if (fits(start, itemCappedEnd)) {
      ranges.push([start, itemCappedEnd]);
      start = itemCappedEnd;
      continue;
    }
    if (!fits(start, start + 1)) {
      oversize.push(start);
      start += 1;
      continue;
    }

    let end = start + 1;
    let hi = itemCappedEnd;
    while (end < hi) {
      const mid = Math.floor((end + hi + 1) / 2);
      if (fits(start, mid)) {
        end = mid;
      } else {
        hi = mid - 1;
      }
    }
    while (end > start + 1 && !fits(start, end)) {
      end--;
    }

    ranges.push([start, end]);
    start = end;
  }
  return { ranges, oversize };
}

function hexByteLength(hex: Hex): number {
  return (hex.length - 2) / 2;
}

/**
 * Classifies an upstream error for the deployless batcher. Returns `null` for unrelated
 * errors (which should propagate without retry). Timeout is checked first so a TimeoutError
 * with an incidentally size-shaped message still routes through the cautious-bisect path.
 *
 * `"timeout"` covers errors that *may* be batch-induced but can also indicate a slow/flaky
 * upstream. Callers should bisect cautiously (e.g. limited splits per chunk) so a downed node
 * doesn't get hammered with `2^depth` retries:
 *   - viem TimeoutError, HTTP 408 / 504 / 524, generic "timed out" / "timeout" messages
 *
 * `"size"` covers errors that scale deterministically with batch size; bisecting always helps:
 *   - Calldata size:   HTTP 413; messages containing "too large" or "request size"
 *   - Initcode size (EIP-3860): "max initcode size exceeded" — matched by /code.*size/
 */
function classifyChunkError(error: unknown): "size" | "timeout" | null {
  if (isTimeoutLikeError(error)) return "timeout";

  const e = error instanceof BaseError ? error.walk() : error;
  const status = (e as { status?: number }).status;
  const msg = (e as { message?: string }).message ?? "";

  if (status === 413) return "size";
  if (/too large/i.test(msg) || /request.{0,10}size/i.test(msg) || /code.{0,10}size/i.test(msg)) {
    return "size";
  }

  return null;
}
