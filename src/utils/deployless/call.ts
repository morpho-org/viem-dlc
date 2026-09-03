import { BaseError, type EIP1193RequestFn, type Hex, type PublicRpcSchema } from "viem";

import type { Facet } from "../../observability.js";
import type { EIP1193Parameters } from "../../types.js";
import { isTimeoutLikeError, serializeError } from "../errors.js";
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
import { arrayToWire, hexToPage, type Page, type ResolvedArrayFunction } from "./codec.inner.js";

type RestOfEthCallParams = Tail<EIP1193Parameters<PublicRpcSchema, "eth_call">["params"]>;

/**
 * Most bytes of memory one chunk may make the envelope allocate before its first element — the
 * constructor args and, when compressed, the decompressed body. Memory expansion is quadratic, so
 * this caps the gas a chunk can spend in its prologue (~2M at the cap) against the smallest
 * `eth_call` gas cap the package supports, 10M; it is never tuned per lens, chain, or provider.
 */
export const MAX_ALLOC_BYTES = 1 << 20;

/** Envelope memory that does not scale with the chunk: scratch, headers, the page header. */
const ALLOC_BYTES_FIXED = 1024;

type FactorisedFactoryCallParams = {
  target: DeploylessTarget;
  elements: readonly Hex[];
  solidity: ResolvedArrayFunction;
  batch?: {
    batchSize?: number;
    compress?: boolean;
  };
  restOfEthCallParams: RestOfEthCallParams;
  /**
   * Invoked with each freshly fetched element as its chunk lands, before siblings finish, and
   * awaited — so a caller's results survive a later chunk failing.
   */
  onResolved?: (entries: readonly ResolvedElement[]) => void | Promise<void>;
  facet?: Facet;
};

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
 * Packs `elements` into deployless-factory `eth_call` chunks under two byte budgets — the wire
 * (`batch.batchSize`, at most EIP-3860's initcode cap) and {@link MAX_ALLOC_BYTES} — fetches them
 * in parallel, and returns per-element outputs aligned to `elements`. No gas is modelled: the
 * envelope reports how far it got, an element gas could not resolve is retried once alone, and
 * only a frame that dies without reporting (a prologue death) is halved.
 */
export async function factorisedFactoryCall(
  requestFn: EIP1193RequestFn<PublicRpcSchema>,
  { target, elements, solidity, batch, restOfEthCallParams, onResolved, facet }: FactorisedFactoryCallParams,
): Promise<FactorisedFactoryCallResult> {
  const compress = batch?.compress ?? false;
  const missing: number[] = [];
  const unresolved: number[] = [];
  const oversize: number[] = [];
  const wrapAs = (start: number, end: number, compress: boolean): Hex =>
    wrapDeploylessFactoryCall(
      { target, targetData: arrayToWire(solidity.inputLayout, elements.slice(start, end)) },
      { compress, config: envelopeConfig(solidity, compress) },
    );
  const wrap = (start: number, end: number): Hex => wrapAs(start, end, compress);

  let referenceWrapped: Hex | undefined;
  const getReferenceWrapped = () => {
    if (!referenceWrapped) referenceWrapped = wrap(0, elements.length);
    return referenceWrapped;
  };

  // Static layouts contribute `layout.size` per element; dynamic ones a length word plus their
  // padded bytes. Both are multiples of 32, so the wrapper's own padding is a per-batch constant.
  const prefixBytes = [0];
  for (let pos = 0; pos < elements.length; pos++) {
    const bytes =
      solidity.inputLayout.mode === "static" ? solidity.inputLayout.size : 32 + hexByteLength(elements[pos]!);
    prefixBytes.push(prefixBytes[pos]! + bytes);
  }
  let overheadBytes: number | undefined;
  const measureUncompressedBytes = (start: number, end: number) => {
    overheadBytes ??=
      hexByteLength(compress ? wrapAs(0, 1, false) : getReferenceWrapped()) -
      prefixBytes[compress ? 1 : elements.length]!;
    return overheadBytes + prefixBytes[end]! - prefixBytes[start]!;
  };
  const measureWireBytes = compress
    ? (start: number, end: number) =>
        hexByteLength(start === 0 && end === elements.length ? getReferenceWrapped() : wrap(start, end))
    : measureUncompressedBytes;
  // A compressed call keeps the compressed args resident while the decompressed copy is used.
  const measureAllocBytes = (start: number, end: number, wireBytes: number) =>
    measureUncompressedBytes(start, end) + (compress ? wireBytes : 0) + ALLOC_BYTES_FIXED;

  const wireCap = batch?.batchSize && batch.batchSize > 0 ? batch.batchSize : Infinity;
  const fits = (start: number, end: number) => {
    const wireBytes = compress || wireCap !== Infinity ? measureWireBytes(start, end) : 0;
    return wireBytes <= wireCap && measureAllocBytes(start, end, wireBytes) <= MAX_ALLOC_BYTES;
  };

  const packed = packBatches({ count: elements.length, maxItems: Infinity, fits });
  oversize.push(...packed.oversize);
  missing.push(...oversize);
  const ranges = packed.ranges;
  const outputs = new Array<Hex>(elements.length);

  facet?.set({ elements_requested: elements.length, nominal_batches: ranges.length });
  // Sizes of the *initial* packing, to compare realized utilization against the two budgets.
  // Halved children and continuations are not resampled. Guarded rather than
  // `facet?.stat(...)` so unobserved calls skip re-measuring.
  if (facet) {
    for (const [start, end] of ranges) {
      const wireBytes = measureWireBytes(start, end);
      facet.stat("batch_bytes", wireBytes);
      facet.stat("batch_alloc_bytes", measureAllocBytes(start, end, wireBytes));
    }
  }
  let fetched = 0;
  const splits = { count: 0, size: 0, corpse: 0, timeout: 0, maxDepth: 0 };
  // A lens stopping early is a continuation, a mid-page gas death an escalation; neither is a
  // split, which now means only "the frame died without reporting".
  const pages = { continued: 0, waves: 0, escalated: 0, unresolvedAttempts: 0, allSkipped: 0 };
  const corpseErrors: unknown[] = [];

  const commit = async (entries: readonly ResolvedElement[]) => {
    for (const { index, output } of entries) outputs[index] = output;
    fetched += entries.length;
    if (entries.length > 0) await onResolved?.(entries);
  };

  /** Re-packs `[from, to)` under both budgets and an item cap the lens just demonstrated. */
  const packRange = (from: number, to: number, maxItems: number): BatchRange[] =>
    packBatches({
      count: to - from,
      maxItems,
      fits: (s, e) => fits(from + s, from + e),
    }).ranges.map(([s, e]) => [from + s, from + e] as const);

  const fetchRecursive = async (
    [start, end]: BatchRange,
    /** Ranges deferred to the next wave: continuations and singleton escalations. */
    nextWave: BatchRange[],
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
      const halve = (nextBudget = timeoutSplitsRemaining) => {
        const mid = start + Math.floor(count / 2);
        return settleAll([
          fetchRecursive([start, mid], nextWave, undefined, nextBudget, depth + 1),
          fetchRecursive([mid, end], nextWave, undefined, nextBudget, depth + 1),
        ]);
      };
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
          "[deployless] counterfactual deploy failed: target occupied, constructor reverted, or no code left",
          {
            cause: e,
          },
        );
      }
      const cause = classifyChunkError(e);
      if (cause !== null && cause !== "timeout" && count > 1) {
        splits.count += 1;
        splits[cause] += 1;
        return halve();
      }
      if (cause === "timeout" && count > 1 && timeoutSplitsRemaining > 0) {
        splits.count += 1;
        splits.timeout += 1;
        return halve(timeoutSplitsRemaining - 1);
      }
      if (cause === "corpse" && count === 1) {
        missing.push(start);
        unresolved.push(start);
        return;
      }
      if (cause === null && corpseErrors.length < 3) corpseErrors.push(serializeError(e));
      throw e;
    }

    const page = hexToPage(solidity.outputLayout, returndata);
    const attempted = validatePage(page, count);
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
        nextWave.push([pos, pos + 1]);
      } else {
        missing.push(pos);
        unresolved.push(pos);
      }
    }

    if (attempted < count) {
      pages.continued += 1;
      const served = attempted - (page.died === undefined ? 0 : 1);
      nextWave.push(...packRange(start + attempted, end, served > 0 ? served : Infinity));
    }
  };

  let wave: BatchRange[] = ranges;
  try {
    while (wave.length > 0) {
      pages.waves += 1;
      const nextWave: BatchRange[] = [];
      const isWholeInput = wave.length === 1 && wave[0]![0] === 0 && wave[0]![1] === elements.length;
      await settleAll(
        wave.map((range) => fetchRecursive(range, nextWave, isWholeInput ? getReferenceWrapped() : undefined)),
      );
      wave = nextWave;
    }
  } finally {
    facet?.set({
      elements_fetched: fetched,
      splits_count: splits.count,
      splits_size: splits.size,
      splits_corpse: splits.corpse,
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
    for (const error of corpseErrors) facet?.push("corpse_errors", error, 3);
  }

  return {
    outputs,
    missing: missing.sort((a, b) => a - b),
    unresolved: unresolved.sort((a, b) => a - b),
    oversize: oversize.sort((a, b) => a - b),
  };
}

/** `Promise.all`, but every branch settles before the first failure surfaces. */
async function settleAll(promises: readonly Promise<void>[]): Promise<void> {
  const settled = await Promise.allSettled(promises);
  const failure = settled.find((s) => s.status === "rejected");
  if (failure) throw (failure as PromiseRejectedResult).reason;
}

/**
 * Returns the number of elements the page adjudicated. The decoder has already bound every record
 * to its ordinal; what only the request knows is the count, and the `attempted >= 1` floor is what
 * bounds the wave loop: without it a lens could stall forever.
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
 * left out rather than sent — a chunk that cannot fit is never the way to make progress.
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
 * `"corpse"` is a frame that died of gas without reporting — the envelope's deploy out-of-gas
 * marker ({@link isOutOfGasRevert}) or a provider's "out of gas" text for the envelope's own
 * prologue. Halving shrinks the prologue; alone, the element is unresolved.
 *
 * `"size"` covers errors that scale deterministically with batch size; bisecting always helps:
 *   - Calldata size:   HTTP 413; messages containing "too large" or "request size"
 *   - Initcode size (EIP-3860): "max initcode size exceeded" — matched by /code.*size/
 */
function classifyChunkError(error: unknown): "corpse" | "size" | "timeout" | null {
  if (isTimeoutLikeError(error)) return "timeout";
  if (isOutOfGasRevert(error)) return "corpse";

  const e = error instanceof BaseError ? error.walk() : error;
  const status = (e as { status?: number }).status;
  const msg = (e as { message?: string }).message ?? "";

  if (/out of gas/i.test(msg)) return "corpse";
  if (status === 413) return "size";
  if (/too large/i.test(msg) || /request.{0,10}size/i.test(msg) || /code.{0,10}size/i.test(msg)) {
    return "size";
  }

  return null;
}
