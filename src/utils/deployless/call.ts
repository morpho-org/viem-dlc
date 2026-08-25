import { BaseError, type EIP1193RequestFn, type Hex, type PublicRpcSchema } from "viem";

import type { EIP1193Parameters } from "../../types.js";
import { isTimeoutLikeError } from "../errors.js";
import type { Tail } from "../tuples.js";

import {
  type DeploylessTarget,
  extractRevertData,
  isOutOfGasRevert,
  wrapDeploylessFactoryCall,
} from "./codec.envelope.js";
import { arrayToCalldata, hexToArray, hexToPage, type Page, type ResolvedArrayFunction } from "./codec.inner.js";

type RestOfEthCallParams = Tail<EIP1193Parameters<PublicRpcSchema, "eth_call">["params"]>;

type GasModel = { constant: number; linear: number; quadratic: number };

type FactorisedFactoryCallParams = {
  target: DeploylessTarget;
  elements: readonly Hex[];
  solidity: ResolvedArrayFunction;
  batch?: {
    batchSize?: number;
    compress?: boolean;
    gas?: GasModel;
  };
  gasLimit?: number;
  restOfEthCallParams: RestOfEthCallParams;
  /**
   * Invoked with each freshly fetched element as its chunk lands, before siblings finish, and
   * awaited — so a caller's results survive a later chunk failing.
   */
  onResolved?: (entries: readonly ResolvedElement[]) => void | Promise<void>;
};

/** An input element's index paired with the raw output bytes fetched for it. */
export type ResolvedElement = { index: number; output: Hex };

export type FactorisedFactoryCallResult = {
  /** Per-element outputs aligned to `elements`, sparse exactly at {@link FactorisedFactoryCallResult.missing}. */
  outputs: readonly (Hex | undefined)[];
  /** Ascending indices no chunk could serve. Always empty for an unpaged lens. */
  missing: readonly number[];
};

type MeasureBytes = (start: number, end: number) => number;

/**
 * Packs `elements` into deployless-factory `eth_call` chunks honoring the byte budget
 * (`batch.batchSize`) and the gas budget (largest `N` with `batch.gas(N) ≤ gasLimit`),
 * fetches them in parallel, and returns per-element outputs aligned to `elements`. Either
 * budget can be unset; with neither, sends all elements in a single upstream call.
 *
 * When `solidity.paged`, chunks that stop early are re-requested from where they stopped rather
 * than bisected, and `missing` collects the indices no chunk could serve. `missing` is always
 * empty for an unpaged lens, which has no way to report a per-element failure.
 */
export async function factorisedFactoryCall(
  requestFn: EIP1193RequestFn<PublicRpcSchema>,
  { target, elements, solidity, batch, gasLimit, restOfEthCallParams, onResolved }: FactorisedFactoryCallParams,
): Promise<FactorisedFactoryCallResult> {
  const compress = batch?.compress ?? false;
  const wrap = (els: readonly Hex[]): Hex =>
    wrapDeploylessFactoryCall({ target, targetData: arrayToCalldata(solidity, els) }, { compress });

  let referenceWrapped: Hex | undefined;
  const getReferenceWrapped = () => {
    if (!referenceWrapped) referenceWrapped = wrap(elements);
    return referenceWrapped;
  };

  let uncompressedMeasure: MeasureBytes | undefined;

  const measureWrappedBytes: MeasureBytes = (start, end) => {
    const wrapped = start === 0 && end === elements.length ? getReferenceWrapped() : wrap(elements.slice(start, end));
    return hexByteLength(wrapped);
  };

  const measureUncompressedBytes: MeasureBytes = (start, end) => {
    if (!uncompressedMeasure) {
      // Per-element byte contribution: static layouts contribute a constant `layout.size`;
      // dynamic layouts contribute one offset word plus the already-padded element bytes.
      // Both are multiples of 32, so the outer `bytes` wrapper padding for `targetData` stays
      // invariant and `overhead = referenceBytes - sum(perElementBytes)` is an exact per-batch
      // constant for uncompressed calls.
      const elementByteCost = (e: Hex) =>
        solidity.inputLayout.mode === "static"
          ? (solidity.inputLayout as { mode: "static"; size: number }).size
          : 32 + hexByteLength(e);
      const prefixBytes = [0];
      for (const element of elements) {
        prefixBytes.push(prefixBytes[prefixBytes.length - 1]! + elementByteCost(element));
      }
      const overheadBytes = hexByteLength(getReferenceWrapped()) - prefixBytes[elements.length]!;
      uncompressedMeasure = (s, e) => overheadBytes + prefixBytes[e]! - prefixBytes[s]!;
    }
    return uncompressedMeasure(start, end);
  };

  const measureBytes = compress ? measureWrappedBytes : measureUncompressedBytes;

  let maxItemsByGas: number | undefined;
  if (gasLimit && batch?.gas) {
    maxItemsByGas = solveMaxItemsByGas(batch.gas, gasLimit);
    if (maxItemsByGas < 1) {
      const { constant, linear, quadratic } = batch.gas;
      throw new Error(
        `[deployless] gasLimit=${gasLimit} cannot fit a single item under G(N) = ${constant} + ${linear}·N + ${quadratic}·N²`,
      );
    }
  }

  const ranges = packBatches({
    count: elements.length,
    maxBytes: batch?.batchSize,
    maxItems: maxItemsByGas,
    measureBytes,
  });
  const outputs = new Array<Hex>(elements.length);

  const commit = async (entries: readonly ResolvedElement[]) => {
    for (const { index, output } of entries) outputs[index] = output;
    if (entries.length > 0) await onResolved?.(entries);
  };

  /** Re-packs `[from, to)` under the byte budget and an item cap the lens just demonstrated. */
  const packRange = (from: number, to: number, maxItems: number): BatchRange[] =>
    packBatches({
      count: to - from,
      maxBytes: batch?.batchSize,
      maxItems: Math.min(maxItems, maxItemsByGas ?? Infinity),
      measureBytes: (s, e) => measureBytes(from + s, from + e),
    }).map(([s, e]) => [from + s, from + e] as const);

  const missing: number[] = [];

  const fetchRecursive = async (
    [start, end]: BatchRange,
    /** Ranges the paged path defers to the next wave. Unused (and never appended to) otherwise. */
    nextWave: BatchRange[],
    precomputed?: Hex,
    timeoutSplitsRemaining = 1,
  ): Promise<void> => {
    const count = end - start;
    const wrapped = precomputed ?? wrap(elements.slice(start, end));

    let returndata: Hex;
    try {
      returndata = await fetchChunk(requestFn, wrapped, restOfEthCallParams);
    } catch (e) {
      if (count > 1) {
        const cause = classifyBatchSizeError(e);
        if (cause === "size" || (cause === "timeout" && timeoutSplitsRemaining > 0)) {
          const nextBudget = cause === "timeout" ? timeoutSplitsRemaining - 1 : timeoutSplitsRemaining;
          const mid = start + Math.floor(count / 2);
          return settleAll([
            fetchRecursive([start, mid], nextWave, undefined, nextBudget),
            fetchRecursive([mid, end], nextWave, undefined, nextBudget),
          ]);
        }
      } else if (solidity.paged && isOutOfGasRevert(e)) {
        // A dead frame is a paged lens's only way to say "unservable": it may not decline index 0.
        missing.push(start);
        return;
      }
      throw e;
    }

    if (!solidity.paged) {
      const chunkOutputs = hexToArray(solidity.outputLayout, returndata);
      if (chunkOutputs.length !== count) {
        throw new Error(`eth_call returned ${chunkOutputs.length} output elements, expected ${count}`);
      }
      await commit(chunkOutputs.map((output, j) => ({ index: start + j, output })));
      return;
    }

    const page = hexToPage(solidity.outputLayout, returndata);
    const attempted = validatePage(page, count);
    const declined = new Set(page.skipped);
    const entries: ResolvedElement[] = [];
    for (let i = 0, served = 0; i < attempted; i++) {
      if (declined.has(i)) missing.push(start + i);
      else entries.push({ index: start + i, output: page.results[served++]! });
    }
    await commit(entries);

    if (attempted < count) nextWave.push(...packRange(start + attempted, end, attempted));
  };

  let wave: BatchRange[] = ranges;
  while (wave.length > 0) {
    const nextWave: BatchRange[] = [];
    const isWholeInput = wave.length === 1 && wave[0]![0] === 0 && wave[0]![1] === elements.length;
    await settleAll(
      wave.map((range) => fetchRecursive(range, nextWave, isWholeInput ? getReferenceWrapped() : undefined)),
    );
    wave = nextWave;
  }

  return { outputs, missing: missing.sort((a, b) => a - b) };
}

/** `Promise.all`, but every branch settles before the first failure surfaces. */
async function settleAll(promises: readonly Promise<void>[]): Promise<void> {
  const settled = await Promise.allSettled(promises);
  const failure = settled.find((s) => s.status === "rejected");
  if (failure) throw (failure as PromiseRejectedResult).reason;
}

/**
 * Returns the number of elements the page attempted, rejecting responses that break the parts of
 * the lens contract visible in the tuple. The rest of the contract is not observable here.
 *
 * The `attempted >= 1` floor is what bounds the wave loop: without it a lens can stall forever.
 */
function validatePage({ results, skipped }: Page, count: number): number {
  const attempted = results.length + skipped.length;
  if (attempted < 1 || attempted > count) {
    throw new Error(`paged lens attempted ${attempted} of ${count} elements, expected 1..${count}`);
  }
  for (let k = 0; k < skipped.length; k++) {
    const index = skipped[k]!;
    if (index >= attempted || (k > 0 && index <= skipped[k - 1]!)) {
      throw new Error(`paged lens returned skipped indices that are not strictly increasing below ${attempted}`);
    }
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

/**
 * Largest non-negative integer `N` such that `constant + linear·N + quadratic·N² ≤ gasLimit`.
 * Returns 0 when even `N=0` (the constant term alone) overflows the budget. With `quadratic = 0`
 * and `linear = 0`, the polynomial is constant and any `N` fits → returns `Infinity`.
 */
function solveMaxItemsByGas({ constant, linear, quadratic }: GasModel, gasLimit: number): number {
  const budget = gasLimit - constant;
  if (budget < 0) return 0;
  if (quadratic === 0) {
    if (linear === 0) return Infinity;
    return Math.floor(budget / linear);
  }
  const discriminant = linear * linear + 4 * quadratic * budget;
  return Math.floor((-linear + Math.sqrt(discriminant)) / (2 * quadratic));
}

type PackBatchesArgs = {
  count: number;
  maxBytes: number | undefined;
  maxItems: number | undefined;
  measureBytes: MeasureBytes;
};

/**
 * Greedy batch packer enforcing both a byte budget and an item-count budget. For each batch,
 * the search upper bound is `min(count, start + maxItems)`. Within that window: first checks
 * whether all remaining elements fit the byte budget, then binary searches for a fitting end
 * and shrinks defensively if measurement is not perfectly monotonic.
 *
 * Always includes at least one element per batch on the byte path, so a single oversized
 * element still makes progress. Either budget can be omitted (`undefined` or non-positive).
 */
function packBatches({ count, maxBytes, maxItems, measureBytes }: PackBatchesArgs): BatchRange[] {
  if (count === 0) return [];
  const itemCap = maxItems && maxItems > 0 ? maxItems : Infinity;
  const byteCap = maxBytes && maxBytes > 0 ? maxBytes : Infinity;

  const ranges: BatchRange[] = [];
  let start = 0;
  while (start < count) {
    const itemCappedEnd = Math.min(count, start + itemCap);

    if (byteCap === Infinity || measureBytes(start, itemCappedEnd) <= byteCap) {
      ranges.push([start, itemCappedEnd]);
      start = itemCappedEnd;
      continue;
    }

    let end = start + 1;
    let hi = itemCappedEnd;
    while (end < hi) {
      const mid = Math.floor((end + hi + 1) / 2);
      if (measureBytes(start, mid) <= byteCap) {
        end = mid;
      } else {
        hi = mid - 1;
      }
    }

    while (end > start + 1 && measureBytes(start, end) > byteCap) {
      end--;
    }

    ranges.push([start, end]);
    start = end;
  }
  return ranges;
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
 *   - Gas limit:       the wrapper's out-of-gas marker ({@link isOutOfGasRevert}), or "out of gas"
 *   - Initcode size (EIP-3860): "max initcode size exceeded" — matched by /code.*size/
 */
function classifyBatchSizeError(error: unknown): "size" | "timeout" | null {
  if (isTimeoutLikeError(error)) return "timeout";
  if (isOutOfGasRevert(error)) return "size";

  const e = error instanceof BaseError ? error.walk() : error;
  const status = (e as { status?: number }).status;
  const msg = (e as { message?: string }).message ?? "";

  if (status === 413) return "size";
  if (
    /too large/i.test(msg) ||
    /request.{0,10}size/i.test(msg) ||
    /out of gas/i.test(msg) ||
    /code.{0,10}size/i.test(msg)
  ) {
    return "size";
  }

  return null;
}
