import { hexToBigInt, type RpcLog } from "viem";

import type { BlockRange, Cache } from "../../types.js";
import { isInBlockRange, mergeBlockRanges } from "../../utils/blocks.js";
import { max, min } from "../../utils/math.js";
import type { LogsResponse, OnLogsResponse } from "../logs-divider/types.js";

import type { CachedChunk } from "./types.js";
import { computeCacheKey } from "./utils.js";

export interface SinkConfig {
  chainId: number;
  /** Cache entry size in blocks. Responses are accumulated until each bin is complete. */
  binSize: number;
  /** Cache instance to write to */
  cache: Cache<CachedChunk>;
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
 * Returns a unique key for a log entry.
 * Used to deduplicate logs from concurrent requests hitting the same bin.
 */
function getLogKey(log: RpcLog): string {
  return `${log.blockHash}:${log.logIndex}`;
}

/**
 * Creates a callback that accumulates logs responses and writes complete bins to cache.
 * Used internally by `logsCache` as the `onLogsResponse` handler for `logsDivider`.
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
export function createSink(config: SinkConfig): OnLogsResponse {
  const { chainId, binSize, cache } = config;
  const binSizeBigInt = BigInt(binSize);

  // Map from cache key -> accumulator
  const accumulators = new Map<string, BinAccumulator>();

  return (response: LogsResponse) => {
    const { logs, filter, fromBlock, toBlock, fetchedAtBlock, fetchedAt } = response;

    // Collect completed bins for batched write
    const completedBins: { key: string; value: CachedChunk }[] = [];

    // A response may span multiple bins - iterate over each affected bin
    let binStart = (fromBlock / binSizeBigInt) * binSizeBigInt;

    while (binStart <= toBlock) {
      const binEnd = binStart + binSizeBigInt - 1n;

      const key = computeCacheKey({
        chainId,
        address: filter.address,
        topics: filter.topics,
        fromBlock: binStart,
        toBlock: binEnd,
      });

      // Get or create accumulator for this bin
      let acc = accumulators.get(key);
      if (!acc) {
        acc = {
          logs: [],
          fetchedAt,
          fetchedAtBlock,
          coveredRanges: [],
          alignedRange: { fromBlock: binStart, toBlock: binEnd },
        };
        accumulators.set(key, acc);
      }

      // Add logs that fall within this bin's overlap
      const binLogs = logs.filter(isInBlockRange({ fromBlock: binStart, toBlock: binEnd }));
      acc.logs.push(...binLogs);
      acc.coveredRanges.push({
        // It's important to use actual `filter.toBlock` because planned `toBlock` may have been > latestBlock,
        // and therefore not actually covered.
        fromBlock: max(binStart, hexToBigInt(filter.fromBlock)),
        toBlock: min(binEnd, hexToBigInt(filter.toBlock)),
      });
      acc.fetchedAt = Math.max(acc.fetchedAt, fetchedAt);
      acc.fetchedAtBlock = max(acc.fetchedAtBlock, fetchedAtBlock);

      // Check if bin is complete (covered ranges span the full bin)
      if (isBinComplete(acc.coveredRanges, binStart, binEnd)) {
        // Deduplicate logs (concurrent requests for the same bin can produce duplicates)
        const seen = new Set<string>();
        // NOTE: We can skip sorting here because sorting is done in `handlers.ts`
        const uniqueLogs = acc.logs.filter((log) => {
          const logKey = getLogKey(log);
          if (seen.has(logKey)) return false;
          seen.add(logKey);
          return true;
        });

        completedBins.push({
          key,
          value: {
            logs: uniqueLogs,
            fetchedAt: acc.fetchedAt,
            fetchedAtBlock: acc.fetchedAtBlock,
            alignedRange: acc.alignedRange,
            // NOTE: Currently we only store completed bins, so fetchedRange === alignedRange
            fetchedRange: acc.alignedRange,
          },
        });
        accumulators.delete(key);
      }

      binStart += binSizeBigInt;
    }

    // Batch write all completed bins (fire-and-forget)
    if (completedBins.length > 0) {
      cache.write(completedBins).catch((err) => {
        console.error("[logsCache] Cache write failed:", err);
      });
    }
  };
}
