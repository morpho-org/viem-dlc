import { hexToNumber, type RpcLog } from "viem";

import type { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import type { BlockRange } from "../../../types.js";
import { isInBlockRange, mergeBlockRanges } from "../../../utils/blocks.js";
import { max, min } from "../../../utils/math.js";
import type { OnLogsResponse } from "../../logs-divider/types.js";
import { keychain } from "../keychain.js";

import type { CachedChunk, CachedLogs } from "./types.js";

export interface SinkConfig {
  chainId: number;
  /** Cache entry size in blocks. Responses are accumulated until each bin is complete. */
  binSize: number;
  /** LazyNdjsonMap instance to write to */
  ndjson: LazyNdjsonMap<CachedChunk>;
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
export function createSink({ chainId, binSize, ndjson }: SinkConfig): OnLogsResponse {
  const binSizeBigInt = BigInt(binSize);

  // Map from entry key -> accumulator
  const accumulators = new Map<string, BinAccumulator>();

  return ({ logs, fromBlock, toBlock, fetchedAt, fetchedAtBlock }) => {
    // A response may span multiple bins - iterate over each affected bin
    let binStart = (fromBlock / binSizeBigInt) * binSizeBigInt;

    while (binStart <= toBlock) {
      const binEnd = binStart + binSizeBigInt - 1n;

      const range = { fromBlock: binStart, toBlock: binEnd };
      const entryKey = keychain.entryKey(chainId, "eth_getLogs", range);

      // Get or create accumulator for this bin
      let acc = accumulators.get(entryKey.data);
      if (!acc) {
        acc = {
          logs: [],
          fetchedAt,
          fetchedAtBlock,
          coveredRanges: [],
          alignedRange: { fromBlock: binStart, toBlock: binEnd },
        };
        accumulators.set(entryKey.data, acc);
      }

      // Add logs that fall within this bin's overlap
      const binLogs = logs.filter(isInBlockRange({ fromBlock: binStart, toBlock: binEnd }));
      for (const log of binLogs) acc.logs.push(log); // NOTE: avoiding `...binLogs` spread due to engine arg limits
      acc.coveredRanges.push({
        fromBlock: max(binStart, fromBlock),
        toBlock: min(binEnd, toBlock),
      });
      acc.fetchedAt = Math.max(acc.fetchedAt, fetchedAt);
      acc.fetchedAtBlock = max(acc.fetchedAtBlock, fetchedAtBlock);

      // Check if bin is complete (covered ranges span the full bin)
      if (isBinComplete(acc.coveredRanges, binStart, binEnd)) {
        // Sort logs within the bin for guaranteed ordering
        acc.logs.sort((a, b) => {
          const blockDiff = hexToNumber(a.blockNumber!) - hexToNumber(b.blockNumber!);
          return blockDiff !== 0 ? blockDiff : hexToNumber(a.logIndex!) - hexToNumber(b.logIndex!);
        });

        // Write metadata and logs as a batch to guarantee they're flushed together
        ndjson.upsert([
          {
            key: entryKey.metadata,
            value: {
              __type: "metadata" as const,
              fetchedAt: acc.fetchedAt,
              fetchedAtBlock: acc.fetchedAtBlock,
              alignedRange: acc.alignedRange,
              // NOTE: Currently we only store completed bins, so fetchedRange === alignedRange
              fetchedRange: acc.alignedRange,
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
