import { BaseError, type EIP1193RequestFn, type Hex, type PublicRpcSchema } from "viem";

import type { EIP1193Parameters } from "../../types.js";
import type { Tail } from "../tuples.js";

import {
  type DeploylessExfilMode,
  type DeploylessTarget,
  extractRevertData,
  wrapDeploylessFactoryCall,
} from "./codec.envelope.js";
import { arrayToCalldata, hexToArray, type ResolvedArrayFunction } from "./codec.inner.js";

type RestOfEthCallParams = Tail<EIP1193Parameters<PublicRpcSchema, "eth_call">["params"]>;

type FactorisedFactoryCallParams = {
  target: DeploylessTarget;
  elements: readonly Hex[];
  solidity: ResolvedArrayFunction;
  batch?: { batchSize: number; exfil?: DeploylessExfilMode; compress?: boolean };
  restOfEthCallParams: RestOfEthCallParams;
};

type MeasureBytes = (start: number, end: number) => number;

/**
 * Packs `elements` into one or more deployless-factory `eth_call` chunks (respecting
 * `batch.batchSize` bytes on the outgoing `data` field), fetches them in parallel, and
 * returns the per-element output slices aligned to `elements`. When `batch` is undefined,
 * sends all elements in a single upstream call.
 */
export async function factorisedFactoryCall(
  requestFn: EIP1193RequestFn<PublicRpcSchema>,
  { target, elements, solidity, batch, restOfEthCallParams }: FactorisedFactoryCallParams,
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
  const ranges = packByByteBudget(elements.length, batch?.batchSize, measureBytes);
  const outputs = new Array<Hex>(elements.length);

  const fetchChunk = exfil === "return" ? fetchChunkReturn : fetchChunkRevert;

  const fetchRecursive = async (els: readonly Hex[], startIdx: number, precomputed?: Hex): Promise<void> => {
    const wrapped = precomputed ?? wrap(els);
    try {
      const returndata = await fetchChunk(requestFn, wrapped, restOfEthCallParams);
      const chunkOutputs = hexToArray(solidity.outputLayout, returndata);
      if (chunkOutputs.length !== els.length) {
        throw new Error(`eth_call returned ${chunkOutputs.length} output elements, expected ${els.length}`);
      }
      for (let j = 0; j < chunkOutputs.length; j++) outputs[startIdx + j] = chunkOutputs[j]!;
    } catch (e) {
      if (els.length > 1 && isErrorCausedByBatchSize(e)) {
        const mid = Math.floor(els.length / 2);
        await Promise.all([
          fetchRecursive(els.slice(0, mid), startIdx),
          fetchRecursive(els.slice(mid), startIdx + mid),
        ]);
        return;
      }
      throw e;
    }
  };

  await Promise.all(
    ranges.map(([start, end]) =>
      fetchRecursive(elements.slice(start, end), start, ranges.length === 1 ? getReferenceWrapped() : undefined),
    ),
  );

  return outputs;
}

async function fetchChunkReturn(requestFn: EIP1193RequestFn<PublicRpcSchema>, data: Hex, rest: RestOfEthCallParams) {
  return requestFn({ method: "eth_call", params: [{ data }, ...rest] });
}

async function fetchChunkRevert(requestFn: EIP1193RequestFn<PublicRpcSchema>, data: Hex, rest: RestOfEthCallParams) {
  try {
    await requestFn({ method: "eth_call", params: [{ data }, ...rest] });
  } catch (e) {
    const decoded = extractRevertData(e);
    if (!decoded.ok) throw e;
    return decoded.returnData;
  }
  throw new Error("revert-mode wrapper returned without reverting");
}

type BatchRange = readonly [start: number, end: number];

/**
 * Greedy batch packer. First checks whether all remaining elements fit, then binary
 * searches for a fitting end and shrinks defensively if measurement is not perfectly
 * monotonic. Always includes at least one miss per batch, so a single oversized element
 * still makes progress.
 *
 * Returns `[[0, n]]` (a single batch) when splitting is disabled (`maxBytes` unset
 * or non-positive).
 */
function packByByteBudget(count: number, maxBytes: number | undefined, measureBytes: MeasureBytes): BatchRange[] {
  if (count === 0) return [];
  if (!maxBytes || maxBytes <= 0) return [[0, count]];

  const ranges: BatchRange[] = [];
  let start = 0;
  while (start < count) {
    if (measureBytes(start, count) <= maxBytes) {
      ranges.push([start, count]);
      break;
    }

    let end = start + 1;
    let hi = count;
    while (end < hi) {
      const mid = Math.floor((end + hi + 1) / 2);
      if (measureBytes(start, mid) <= maxBytes) {
        end = mid;
      } else {
        hi = mid - 1;
      }
    }

    while (end > start + 1 && measureBytes(start, end) > maxBytes) {
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
 * Returns `true` when the error is likely caused by the batch being too large for the RPC,
 * making it safe to retry with a smaller element slice.
 *
 * Covers three failure modes:
 *   - Calldata size:   HTTP 413; messages containing "too large" or "request size"
 *   - Gas limit:       "out of gas" during execution
 *   - Return data size (RETURN mode): EIP-170 "code size" exceeded
 *   - Initcode size (EIP-3860): "max initcode size exceeded" — also matched by /code.*size/
 */
function isErrorCausedByBatchSize(error: unknown): boolean {
  const e = error instanceof BaseError ? error.walk() : error;
  if ((e as { status?: number }).status === 413) return true;
  const msg = (e as { message?: string }).message ?? "";
  return (
    /too large/i.test(msg) || /request.{0,10}size/i.test(msg) || /out of gas/i.test(msg) || /code.{0,10}size/i.test(msg)
  );
}
