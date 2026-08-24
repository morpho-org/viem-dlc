import { BaseError, type EIP1193RequestFn, type Hex, type PublicRpcSchema } from "viem";

import type { Facet } from "../../observability.js";
import type { EIP1193Parameters } from "../../types.js";
import { isTimeoutLikeError } from "../errors.js";
import type { Tail } from "../tuples.js";

import {
  type DeploylessExfilMode,
  type DeploylessTarget,
  extractRevertData,
  wrapDeploylessFactoryCall,
} from "./codec.envelope.js";
import { arrayToCalldata, hexToArray, type ResolvedArrayFunction } from "./codec.inner.js";

type RestOfEthCallParams = Tail<EIP1193Parameters<PublicRpcSchema, "eth_call">["params"]>;

type GasModel = { constant: number; linear: number; quadratic: number };

type FactorisedFactoryCallParams = {
  target: DeploylessTarget;
  elements: readonly Hex[];
  solidity: ResolvedArrayFunction;
  batch?: {
    batchSize?: number;
    exfil?: DeploylessExfilMode;
    compress?: boolean;
    gas?: GasModel;
  };
  gasLimit?: number;
  restOfEthCallParams: RestOfEthCallParams;
  facet?: Facet;
};

type MeasureBytes = (start: number, end: number) => number;

/**
 * Packs `elements` into deployless-factory `eth_call` chunks honoring the byte budget
 * (`batch.batchSize`) and the gas budget (largest `N` with `batch.gas(N) ≤ gasLimit`),
 * fetches them in parallel, and returns per-element outputs aligned to `elements`. Either
 * budget can be unset; with neither, sends all elements in a single upstream call.
 */
export async function factorisedFactoryCall(
  requestFn: EIP1193RequestFn<PublicRpcSchema>,
  { target, elements, solidity, batch, gasLimit, restOfEthCallParams, facet }: FactorisedFactoryCallParams,
): Promise<Hex[]> {
  const exfil: DeploylessExfilMode = batch?.exfil ?? "return";
  const compress = batch?.compress ?? false;
  const wrap = (els: readonly Hex[]): Hex =>
    wrapDeploylessFactoryCall({ target, targetData: arrayToCalldata(solidity, els) }, { exfil, compress });

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

  facet?.set({ elements_fetched: elements.length, nominal_batches: ranges.length });
  // Packed size of each batch, to compare realized utilization against `batchSize`.
  // Guarded rather than `facet?.stat(...)` so unobserved calls skip re-measuring.
  if (facet) for (const [start, end] of ranges) facet.stat("batch_bytes", measureBytes(start, end));
  const splits = { count: 0, size: 0, timeout: 0, maxDepth: 0 };

  const fetchChunk = exfil === "return" ? fetchChunkReturn : fetchChunkRevert;

  const fetchRecursive = async (
    els: readonly Hex[],
    startIdx: number,
    precomputed?: Hex,
    timeoutSplitsRemaining = 1,
    depth = 0,
  ): Promise<void> => {
    if (depth > splits.maxDepth) splits.maxDepth = depth;

    const wrapped = precomputed ?? wrap(els);
    try {
      const returndata = await fetchChunk(requestFn, wrapped, restOfEthCallParams);
      const chunkOutputs = hexToArray(solidity.outputLayout, returndata);
      if (chunkOutputs.length !== els.length) {
        throw new Error(`eth_call returned ${chunkOutputs.length} output elements, expected ${els.length}`);
      }
      for (let j = 0; j < chunkOutputs.length; j++) outputs[startIdx + j] = chunkOutputs[j]!;
    } catch (e) {
      if (els.length > 1) {
        const cause = classifyBatchSizeError(e);
        if (cause === "size" || (cause === "timeout" && timeoutSplitsRemaining > 0)) {
          const nextBudget = cause === "timeout" ? timeoutSplitsRemaining - 1 : timeoutSplitsRemaining;
          splits.count += 1;
          splits[cause] += 1;
          const mid = Math.floor(els.length / 2);
          await Promise.all([
            fetchRecursive(els.slice(0, mid), startIdx, undefined, nextBudget, depth + 1),
            fetchRecursive(els.slice(mid), startIdx + mid, undefined, nextBudget, depth + 1),
          ]);
          return;
        }
      }
      throw e;
    }
  };

  try {
    await Promise.all(
      ranges.map(([start, end]) =>
        fetchRecursive(elements.slice(start, end), start, ranges.length === 1 ? getReferenceWrapped() : undefined),
      ),
    );
  } finally {
    facet?.set({
      splits_count: splits.count,
      splits_size: splits.size,
      splits_timeout: splits.timeout,
      splits_max_depth: splits.maxDepth,
    });
  }

  return outputs;
}

async function fetchChunkReturn(requestFn: EIP1193RequestFn<PublicRpcSchema>, data: Hex, rest: RestOfEthCallParams) {
  return requestFn({ method: "eth_call", params: [{ data }, ...rest] });
}

async function fetchChunkRevert(requestFn: EIP1193RequestFn<PublicRpcSchema>, data: Hex, rest: RestOfEthCallParams) {
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
 *   - Gas limit:       "out of gas" during execution
 *   - Return data size (RETURN mode): EIP-170 "code size" exceeded
 *   - Initcode size (EIP-3860): "max initcode size exceeded" — also matched by /code.*size/
 */
function classifyBatchSizeError(error: unknown): "size" | "timeout" | null {
  if (isTimeoutLikeError(error)) return "timeout";

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
