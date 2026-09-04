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
 * (`batch.batchSize`, at most EIP-3860's initcode cap) and the gas each chunk is predicted to
 * need; fetches them in parallel; returns per-element outputs aligned to `elements`. The prediction
 * runs on the stated `gasLimit` and `batch.gas` until the first page lands and on the pages' own
 * telemetry after, so the stated figures size the opening wave and nothing else. An element gas
 * could not resolve is retried once alone.
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
  const wrap = (indices: Chunk): Hex =>
    wrapDeploylessFactoryCall(
      {
        target,
        targetData: arrayToWire(
          solidity.inputLayout,
          indices.map((i) => elements[i]!),
        ),
      },
      { compress, config },
    );

  let referenceWrapped: Hex | undefined;
  const getReferenceWrapped = () => {
    if (!referenceWrapped) referenceWrapped = wrap(everything);
    return referenceWrapped;
  };

  // Static layouts contribute `layout.size` per element; dynamic ones a length word plus their
  // padded bytes. Both are multiples of 32, so the wrapper's own padding is a per-batch constant.
  const everything: number[] = [];
  const bytesOf: number[] = [];
  const zerosOf: number[] = [];
  const layout = solidity.inputLayout;
  let totalBytes = 0;
  let totalZeros = 0;
  for (let pos = 0; pos < elements.length; pos++) {
    const element = elements[pos]!;
    const bytes = layout.mode === "static" ? layout.size : 32 + hexByteLength(element);
    const zeros = zeroBytes(element) + (layout.mode === "static" ? 0 : 32 - nonzeroBytesOf(hexByteLength(element)));
    everything.push(pos);
    bytesOf.push(bytes);
    zerosOf.push(zeros);
    totalBytes += bytes;
    totalZeros += zeros;
  }
  let overhead: WireSize | undefined;
  /** Sizes the sub-lists `[start, end)` of `indices` as the wire would carry them. */
  const measurer = (indices: Chunk): ((start: number, end: number) => WireSize) => {
    if (compress) {
      return (start, end) =>
        wireSize(
          start === 0 && end === indices.length && end === elements.length
            ? getReferenceWrapped()
            : wrap(indices.slice(start, end)),
        );
    }
    if (overhead === undefined) {
      const whole = wireSize(getReferenceWrapped());
      overhead = {
        bytes: whole.bytes - totalBytes,
        zeros: whole.zeros - totalZeros - countingWordZeros(elements.length, totalBytes),
      };
    }
    const constant = overhead;
    const prefixBytes = [0];
    const prefixZeros = [0];
    for (let pos = 0; pos < indices.length; pos++) {
      prefixBytes.push(prefixBytes[pos]! + bytesOf[indices[pos]!]!);
      prefixZeros.push(prefixZeros[pos]! + zerosOf[indices[pos]!]!);
    }
    return (start, end) => {
      const body = prefixBytes[end]! - prefixBytes[start]!;
      return {
        bytes: constant.bytes + body,
        zeros: constant.zeros + prefixZeros[end]! - prefixZeros[start]! + countingWordZeros(end - start, body),
      };
    };
  };

  const wireCap = batch?.batchSize && batch.batchSize > 0 ? batch.batchSize : Infinity;
  const stated = statedGas(gasLimit, batch?.gas);
  let gas: GasStats | undefined;
  /** The stated figures until a page has landed, the pooled observations after. */
  const gasParams = (): GasParams | undefined => {
    if (gas === undefined) return stated;
    const item = gas.served === 0n ? stated : moments(gas);
    if (item === undefined) return undefined;
    return { cap: Number(gas.cap), fixed: Number(gas.fixed), avg: item.avg, stddev: item.stddev };
  };

  /** Chunks `indices` under the wire cap and the gas prediction; an element that fits neither alone is `oversize`. */
  const pack = (indices: Chunk): { chunks: Chunk[]; oversize: number[] } => {
    const params = gasParams();
    if (wireCap === Infinity && params === undefined) return { chunks: [indices], oversize: [] };
    const measure = measurer(indices);
    const fits = (start: number, end: number) => {
      const size = measure(start, end);
      if (size.bytes > wireCap) return false;
      return params === undefined || fitsGas(size, end - start, params);
    };
    return packBatches(indices, fits);
  };

  const packed = pack(everything);
  oversize.push(...packed.oversize);
  missing.push(...oversize);
  const chunks = packed.chunks;
  const outputs = new Array<Hex>(elements.length);

  facet?.set({
    elements_requested: elements.length,
    nominal_batches: chunks.length,
    ...(stated === undefined ? {} : { gas_limit: stated.cap }),
  });
  // Sizes of the *initial* packing, to compare realized utilization against the wire budget.
  // Halved children and continuations are not resampled. Guarded rather than
  // `facet?.stat(...)` so unobserved calls skip re-measuring.
  if (facet) for (const chunk of chunks) facet.stat("batch_bytes", measurer(chunk)(0, chunk.length).bytes);
  let fetched = 0;
  const splits = { count: 0, size: 0, timeout: 0, maxDepth: 0 };
  // A lens stopping early is a continuation, a mid-page gas death an escalation; neither is a
  // split, which means only "the provider refused the request's size or timed out".
  const pages = { continued: 0, waves: 0, escalated: 0, unresolvedAttempts: 0, allSkipped: 0 };

  const commit = async (entries: readonly ResolvedElement[]) => {
    for (const { index, output } of entries) outputs[index] = output;
    fetched += entries.length;
    if (entries.length > 0) await onResolved?.(entries);
  };

  const fetchRecursive = async (
    indices: Chunk,
    nextWave: Deferred,
    precomputed?: Hex,
    timeoutSplitsRemaining = 1,
    depth = 0,
  ): Promise<void> => {
    if (depth > splits.maxDepth) splits.maxDepth = depth;
    const count = indices.length;
    const wrapped = precomputed ?? wrap(indices);

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
        const mid = Math.floor(count / 2);
        return settleAll([
          fetchRecursive(indices.slice(0, mid), nextWave, undefined, nextBudget, depth + 1),
          fetchRecursive(indices.slice(mid), nextWave, undefined, nextBudget, depth + 1),
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
      if (declined.has(i)) missing.push(indices[i]!);
      else entries.push({ index: indices[i]!, output: page.results[served++]! });
    }
    await commit(entries);

    if (page.died !== undefined) {
      pages.unresolvedAttempts += 1;
      const pos = indices[page.died]!;
      if (count > 1) {
        pages.escalated += 1;
        nextWave.singletons.push([pos]);
      } else {
        missing.push(pos);
        unresolved.push(pos);
      }
    }

    if (attempted < count) {
      pages.continued += 1;
      nextWave.tails.push(indices.slice(attempted));
    }
  };

  let wave: Chunk[] = chunks;
  try {
    while (wave.length > 0) {
      pages.waves += 1;
      const nextWave: Deferred = { tails: [], singletons: [] };
      const isWholeInput = wave.length === 1 && wave[0]!.length === elements.length;
      await settleAll(
        wave.map((chunk) => fetchRecursive(chunk, nextWave, isWholeInput ? getReferenceWrapped() : undefined)),
      );
      // Tails are packed only once the whole wave has reported, so every one sees the same pool.
      wave = [...nextWave.singletons];
      for (const tail of nextWave.tails) {
        const repacked = pack(tail);
        wave.push(...repacked.chunks);
        oversize.push(...repacked.oversize);
        missing.push(...repacked.oversize);
      }
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

/** Ascending indices into `elements`, sent as one request. */
type Chunk = readonly number[];

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

/** What {@link fitsGas} needs: a provider's cap, and a lens's prologue and per-attempt cost. */
type GasParams = { cap: number; fixed: number; avg: number; stddev: number };

/**
 * Whether a chunk of `k` elements over `size` bytes is predicted to fit the cap after its own
 * intrinsic gas, the prologue and its attempts with headroom. A lone element always fits: the
 * estimate may shorten a chunk but never withhold an element, so the envelope decides what is served.
 */
function fitsGas(size: WireSize, k: number, { cap, fixed, avg, stddev }: GasParams): boolean {
  return k === 1 || intrinsicGas(size) + fixed + chunkCost(k, avg, stddev) <= cap;
}

/** `gasLimit` and `batch.gas` as the prediction uses them, or nothing when either is missing or unusable. */
function statedGas(gasLimit: number | undefined, gas: LensGas | undefined): GasParams | undefined {
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
  return usable ? { cap: gasLimit, fixed, avg, stddev } : undefined;
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
 * costs are correlated, so Cantelli's `1 / (1 + z²)` does not hold (see docs/000016-tib-paginated-lenses.md).
 * An overshoot costs one continuation, packed from more data.
 */
const PACKING_SIGMAS = 2;

/** The cost of a chunk of `k` attempts at mean `mean` and deviation `sigma` each, with headroom for the spread. */
function chunkCost(k: number, mean: number, sigma: number): number {
  return k * mean + PACKING_SIGMAS * sigma * Math.sqrt(k);
}

/** Mean and population deviation of one attempt's cost; the variance's numerator stays exact in bigint. */
function moments({ served, sum, sumSquares }: GasStats) {
  const n = Number(served);
  return {
    avg: Number(sum) / n,
    stddev: Math.sqrt(Number(served * sumSquares - sum * sum)) / n,
  };
}

function gasFields(gas: GasStats): Record<string, number> {
  const frame = { frame_gas: Number(gas.budget), fixed_gas: Number(gas.fixed), gas_limit_observed: Number(gas.cap) };
  if (gas.served === 0n) return frame;
  const { avg, stddev } = moments(gas);
  return { ...frame, item_gas_avg: avg, item_gas_stddev: stddev, item_gas_max: Number(gas.max) };
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

/** What a wave hands the next one: continuation tails, packed once the wave has settled, and singleton escalations. */
type Deferred = { tails: Chunk[]; singletons: Chunk[] };

/**
 * Greedy packer over positions in `indices`: each chunk takes the longest prefix of the remainder
 * that `fits`, found by binary search with a defensive linear shrink for measures that are not
 * perfectly monotone. An element that does not fit alone is reported in `oversize` and left out
 * rather than sent.
 */
function packBatches(
  indices: Chunk,
  fits: (start: number, end: number) => boolean,
): { chunks: Chunk[]; oversize: number[] } {
  const chunks: Chunk[] = [];
  const oversize: number[] = [];
  const count = indices.length;

  let start = 0;
  while (start < count) {
    if (fits(start, count)) {
      chunks.push(indices.slice(start));
      break;
    }
    if (!fits(start, start + 1)) {
      oversize.push(indices[start]!);
      start += 1;
      continue;
    }

    let end = start + 1;
    let hi = count;
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

    chunks.push(indices.slice(start, end));
    start = end;
  }
  return { chunks, oversize };
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
