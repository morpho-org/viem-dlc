import { hexToBigInt, hexToNumber, type RpcLog, toHex } from "viem";

import type { BlockRange, Cache, EIP1193PublicRequestFn, EthGetLogsParams } from "../../types.js";
import { divideBlockRange, isInBlockRange, mergeBlockRanges, resolveBlockNumber } from "../../utils/blocks.js";
import { min } from "../../utils/math.js";

import { CacheKeyIncludes, type CachedChunk, type InvalidationStrategy } from "./types.js";
import { computeCacheKey } from "./utils.js";

/**
 * Check if cached data is valid for a chunk.
 * Returns the cached logs if valid, null otherwise.
 */
function tryUseCachedRange(
  cached: CachedChunk,
  desired: BlockRange,
  totalChunks: number,
  invalidationStrategy: InvalidationStrategy,
): RpcLog[] | null {
  // Check if cached data covers the fetch range.
  // NOTE: Currently this is extra defensive since the logs sink only writes complete bins.
  if (cached.fetchedRange.toBlock < desired.toBlock) {
    return null;
  }

  // Check probabilistic invalidation
  // blocksAgo is computed from the chain tip at fetch time, not now,
  // as that's what indicates reorg likelihood
  const probability = invalidationStrategy({
    confirmations: Number(cached.fetchedAtBlock - cached.fetchedRange.toBlock),
    cacheAgeMs: Date.now() - cached.fetchedAt,
    totalChunks,
  });
  if (Math.random() < probability) {
    return null;
  }

  return cached.logs;
}

export async function handleGetLogs(
  requestFn: EIP1193PublicRequestFn,
  chainId: number,
  filter: EthGetLogsParams,
  {
    binSize,
    invalidationStrategy,
    cache,
    cacheKeyIncludes,
  }: {
    binSize: number;
    invalidationStrategy: InvalidationStrategy;
    cache: Cache<CachedChunk>;
    cacheKeyIncludes?: CacheKeyIncludes;
  },
): Promise<RpcLog[]> {
  // blockHash queries are not cached - pass through
  if (filter.blockHash) {
    return requestFn({ method: "eth_getLogs", params: [filter] }, { dedupe: true });
  }

  const latestBlockNumber = hexToBigInt(await requestFn({ method: "eth_blockNumber" }, { dedupe: true }));

  // Resolve block tags to numbers
  const fromBlock = resolveBlockNumber(filter.fromBlock ?? "earliest", latestBlockNumber);
  const toBlock = min(resolveBlockNumber(filter.toBlock ?? "latest", latestBlockNumber), latestBlockNumber);

  if (fromBlock > toBlock) {
    return [];
  }
  // TODO: handle the above + case where they're above latest, maybe throw errors, both here and in divider.
  // TODO: also maybe update divideBlockRange to allow only alinging fromBlock to help avoid this in divider

  const originalRange =
    cacheKeyIncludes === CacheKeyIncludes.BlockRange ? { fromBlock, toBlock } : undefined;

  // Generate bin-aligned ranges and try to read from cache
  const ranges = divideBlockRange({ fromBlock, toBlock }, binSize, binSize);
  const cacheKeys = ranges.map((range) =>
    computeCacheKey({
      chainId,
      address: filter.address,
      topics: filter.topics,
      fromBlock: range.fromBlock,
      toBlock: range.toBlock,
      originalRange,
    }),
  );
  const cachedValues = await cache.read(cacheKeys);

  // Partition into cached (valid) vs needs-fetch
  const allLogs: RpcLog[] = [];
  const gaps: BlockRange[] = [];

  for (let i = 0; i < ranges.length; i++) {
    const range = ranges[i]!;
    const cached = cachedValues[i];
    const validLogs = cached ? tryUseCachedRange(cached, range, ranges.length, invalidationStrategy) : null;

    if (validLogs !== null) {
      for (const log of validLogs) allLogs.push(log);
    } else {
      gaps.push(range);
    }
  }

  // Fetch missing chunks - process gaps sequentially
  // (Inner transport handles concurrency for each gap range)
  if (gaps.length > 0) {
    const rangesToFetch = mergeBlockRanges(gaps);

    let logs: RpcLog[][];
    try {
      logs = await Promise.all(
        rangesToFetch.map((range) =>
          requestFn(
            {
              method: "eth_getLogs",
              params: [
                {
                  address: filter.address,
                  topics: filter.topics,
                  fromBlock: toHex(range.fromBlock),
                  toBlock: toHex(range.toBlock),
                  // TODO: (@haydenshively future-work) There's probably a way to pass this through without
                  // polluting filter args
                  latestBlock: toHex(latestBlockNumber), // extra param that logsDivider understands
                  ...(originalRange && {
                    originalFromBlock: toHex(originalRange.fromBlock),
                    originalToBlock: toHex(originalRange.toBlock),
                  }),
                },
              ],
            },
            { dedupe: true },
          ),
        ),
      );
    } catch (error) {
      const context = `[logsCache] Gap fetch failed for ${rangesToFetch.length} range(s): ${rangesToFetch.map((r) => `[${r.fromBlock}n, ${r.toBlock}n]`).join(", ")}`;
      if (error instanceof Error) {
        error.message = `${context} ${error.message}`;
        throw error;
      }
      throw new Error(`${context} ${String(error)}`);
    }

    for (const chunk of logs) {
      for (const log of chunk) {
        allLogs.push(log);
      }
    }
  }

  return allLogs
    .filter(isInBlockRange({ fromBlock, toBlock }))
    .sort((a, b) => hexToNumber(a.blockNumber!) - hexToNumber(b.blockNumber!));
}
