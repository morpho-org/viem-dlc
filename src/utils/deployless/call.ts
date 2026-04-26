import type { EIP1193RequestFn, Hex, PublicRpcSchema } from "viem";

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
  batch?: { batchSize: number; exfil?: DeploylessExfilMode };
  restOfEthCallParams: RestOfEthCallParams;
};

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
  const wrap = (els: readonly Hex[]): Hex =>
    wrapDeploylessFactoryCall({ target, targetData: arrayToCalldata(solidity, els) }, exfil);

  // Per-element byte contribution: static layouts contribute a constant `layout.size`;
  // dynamic layouts contribute one offset word plus the already-padded element bytes.
  // Both are multiples of 32, so the outer `bytes` wrapper padding for `targetData` stays
  // invariant and `overhead = referenceBytes - sum(perElementBytes)` is an exact per-batch
  // constant.
  const elementByteCost =
    solidity.inputLayout.mode === "static"
      ? () => (solidity.inputLayout as { mode: "static"; size: number }).size
      : (e: Hex) => 32 + (e.length - 2) / 2;
  const perElementBytes = elements.map(elementByteCost);
  const referenceWrapped = wrap(elements);
  const referenceBytes = (referenceWrapped.length - 2) / 2;
  const overheadBytes = referenceBytes - perElementBytes.reduce((a, b) => a + b, 0);

  const ranges = packByCalldataBytes(perElementBytes, overheadBytes, batch?.batchSize);
  const outputs = new Array<Hex>(elements.length);

  const fetchChunk = exfil === "return" ? fetchChunkReturn : fetchChunkRevert;

  await Promise.all(
    ranges.map(async ([start, end]) => {
      const chunkWrapped = ranges.length === 1 ? referenceWrapped : wrap(elements.slice(start, end));
      const returndata = await fetchChunk(requestFn, chunkWrapped, restOfEthCallParams);

      const chunkOutputs = hexToArray(solidity.outputLayout, returndata);
      if (chunkOutputs.length !== end - start) {
        throw new Error(`eth_call returned ${chunkOutputs.length} output elements, expected ${end - start}`);
      }
      for (let j = 0; j < chunkOutputs.length; j++) {
        outputs[start + j] = chunkOutputs[j]!;
      }
    }),
  );

  return outputs;
}

async function fetchChunkReturn(
  requestFn: EIP1193RequestFn<PublicRpcSchema>,
  data: Hex,
  rest: RestOfEthCallParams,
): Promise<Hex> {
  return requestFn({ method: "eth_call", params: [{ data }, ...rest] });
}

async function fetchChunkRevert(
  requestFn: EIP1193RequestFn<PublicRpcSchema>,
  data: Hex,
  rest: RestOfEthCallParams,
): Promise<Hex> {
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
 * Exact greedy batch packer. Walks `perMissBytes` left-to-right, grouping consecutive
 * misses into batches whose total wire bytes (`overheadBytes + sum of element bytes`)
 * stay within `maxBytes`. Always includes at least one miss per batch, so a single
 * oversized element still makes progress.
 *
 * Callers derive `overheadBytes` and `perMissBytes` from the actual wire format, so
 * unlike a proportional heuristic this never produces a batch larger than `maxBytes`
 * (unless a single miss already exceeds it).
 *
 * Returns `[[0, n]]` (a single batch) when splitting is disabled (`maxBytes` unset
 * or non-positive).
 */
function packByCalldataBytes(
  perMissBytes: readonly number[],
  overheadBytes: number,
  maxBytes: number | undefined,
): BatchRange[] {
  const n = perMissBytes.length;
  if (n === 0) return [];
  if (!maxBytes || maxBytes <= 0) return [[0, n]];

  const ranges: BatchRange[] = [];
  let i = 0;
  while (i < n) {
    let batchBytes = overheadBytes + perMissBytes[i]!;
    let j = i + 1;
    while (j < n && batchBytes + perMissBytes[j]! <= maxBytes) {
      batchBytes += perMissBytes[j]!;
      j++;
    }
    ranges.push([i, j]);
    i = j;
  }
  return ranges;
}
