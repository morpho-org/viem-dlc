import type { RpcLog } from "viem";

import type { NdjsonMapLazy } from "../../../internal/ndjson-map-lazy.js";
import type { BlockRange } from "../../../types.js";
import { isInBlockRange, mergeBlockRanges, sortRpcLogs } from "../../../utils/blocks.js";
import { max, min } from "../../../utils/math.js";
import type { OnLogsResponse } from "../../logs-divider/types.js";
import { keychain } from "../keychain.js";

import type { CachedChunk, CachedLogs } from "./types.js";

export interface SinkConfig {
  chainId: number;
  /** Cache entry size in blocks. Responses are accumulated until each bin is complete. */
  binSize: number;
  /** NdjsonMapLazy instance to write to */
  ndjson: NdjsonMapLazy<CachedChunk>;
}

interface BinAccumulator {
  logs: RpcLog[];
  fetchedAt: number;
  fetchedAtBlock: bigint;
  coveredRanges: BlockRange[];
  alignedRange: BlockRange;
}

/**
 * Check if covered ranges span the full bin [binStart, binEnd]
 */
function isBinComplete(ranges: BlockRange[], binStart: bigint, binEnd: bigint): boolean {
  const merged = mergeBlockRanges(ranges);
  return merged.length === 1 && merged[0]!.fromBlock <= binStart && merged[0]!.toBlock >= binEnd;
}

/**
 * Creates a callback that accumulates logs responses and writes complete bins to cache.
 * Used internally by `cache` as the `onLogsResponse` handler for `logsDivider`.
 *
 * The accumulator pattern handles responses of any size relative to binSize:
 * - Responses smaller than binSize (due to splitting or halving) are accumulated
 * - Responses spanning multiple bins are distributed across them
 * - Only complete bins (fully covered ranges) are written to cache
 *
 * Cache writes are fire-and-forget to avoid blocking the response path.
 *
 * @internal
 */
export function createSink({ chainId, binSize, ndjson }: SinkConfig): OnLogsResponse {
  const binSizeBigInt = BigInt(binSize);

  // Map from entry key -> accumulator
  const accumulators = new Map<string, BinAccumulator>();

  return ({ logs, fromBlock, toBlock, fetchedAt, fetchedAtBlock }) => {
    // A response may span multiple bins - iterate over each affected bin
    let binStart = (fromBlock / binSizeBigInt) * binSizeBigInt;

    while (binStart <= toBlock) {
      const binEnd = binStart + binSizeBigInt - 1n;

      const alignedRange = { fromBlock: binStart, toBlock: binEnd };
      const entryKey = keychain.entryKey(chainId, "eth_getLogs", alignedRange);

      // Get or create accumulator for this bin
      let acc = accumulators.get(entryKey.data);
      if (!acc) {
        acc = {
          logs: [],
          fetchedAt,
          fetchedAtBlock,
          coveredRanges: [],
          alignedRange,
        };
        accumulators.set(entryKey.data, acc);
      }

      // Add logs that fall within this bin's overlap
      const binLogs = logs.filter(isInBlockRange(alignedRange));
      for (const log of binLogs) acc.logs.push(log); // NOTE: avoiding `...binLogs` spread due to engine arg limits
      acc.coveredRanges.push({
        fromBlock: max(binStart, fromBlock),
        toBlock: min(binEnd, toBlock),
      });
      acc.fetchedAt = Math.max(acc.fetchedAt, fetchedAt);
      acc.fetchedAtBlock = max(acc.fetchedAtBlock, fetchedAtBlock);

      // A bin is "complete" when covered ranges span [binStart, effectiveBinEnd].
      // For the last bin at the chain tip, the aligned boundary may exceed the
      // latest block, so we cap at `fetchedAtBlock`.
      const effectiveBinEnd = min(binEnd, acc.fetchedAtBlock);
      if (isBinComplete(acc.coveredRanges, binStart, effectiveBinEnd)) {
        // Sort logs within the bin for guaranteed ordering
        acc.logs.sort(sortRpcLogs);

        // Write metadata and logs as a batch to guarantee they're flushed together
        ndjson.upsert([
          {
            key: entryKey.metadata,
            value: {
              __type: "metadata" as const,
              fetchedAt: acc.fetchedAt,
              fetchedAtBlock: acc.fetchedAtBlock,
              alignedRange: acc.alignedRange,
              fetchedRange: { fromBlock: binStart, toBlock: effectiveBinEnd },
            } satisfies CachedChunk,
          },
          {
            key: entryKey.data,
            value: acc.logs as CachedLogs satisfies CachedChunk,
          },
        ]);
        accumulators.delete(entryKey.data);
      }

      binStart += binSizeBigInt;
    }
  };
}
