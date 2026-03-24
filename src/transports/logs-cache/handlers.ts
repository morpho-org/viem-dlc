import { type EIP1193RequestFn, hexToBigInt, hexToNumber, type RpcLog, toHex } from "viem";

import type { BlockRange, RpcSignature, Store } from "../../types.js";
import { divideBlockRange, isInBlockRange, mergeBlockRanges, resolveBlockNumber } from "../../utils/blocks.js";
import { min } from "../../utils/math.js";
import type { LogsDividerRpcSchema } from "../logs-divider/schema.js";

import { keychain } from "./keychain.js";
import type { LogsCacheRpcSchema } from "./schema.js";
import { createSink } from "./sink.js";
import type { CachedChunk, InvalidationStrategy } from "./types.js";

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
  requestFn: EIP1193RequestFn<LogsDividerRpcSchema>,
  chainId: number,
  [filter]: RpcSignature<LogsCacheRpcSchema, "eth_getLogs">["Parameters"],
  blobKey: string,
  {
    binSize,
    invalidationStrategy,
    store,
  }: {
    binSize: number;
    invalidationStrategy: InvalidationStrategy;
    store: Store;
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

  const cache = await store.get(blobKey);

  // Generate bin-aligned ranges and try to read from cache
  const ranges = divideBlockRange({ fromBlock, toBlock }, binSize, binSize);

  // Partition into cached (valid) vs needs-fetch
  const allLogs: RpcLog[] = [];
  const gaps: BlockRange[] = [];

  for (let i = 0; i < ranges.length; i++) {
    const range = ranges[i]!;
    const cached = cache?.[keychain.entryKey(chainId, "eth_getLogs", range)];
    const validLogs = cached ? tryUseCachedRange(cached, range, ranges.length, invalidationStrategy) : null;

    if (validLogs !== null) {
      for (const log of validLogs) allLogs.push(log); // NOTE: avoiding `...validLogs` spread due to engine arg limits
    } else {
      gaps.push(range);
    }
  }

  // Fetch missing chunks - process gaps sequentially
  // (Inner transport handles concurrency for each gap range)
  if (gaps.length > 0) {
    const rangesToFetch = mergeBlockRanges(gaps);

    const sinkConfig = { chainId, binSize, store };
    const sinkContext = { blobKey };
    const sink = createSink(sinkConfig, sinkContext);

    try {
      const fetches = await Promise.all(
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
                },
                undefined,
                {
                  latestBlock: toHex(latestBlockNumber),
                  onLogsResponse: sink,
                },
              ],
            },
            { dedupe: true },
          ),
        ),
      );

      // NOTE: Individual `.push(item)` is safer than `.push(...arr)` for large `arr` because
      // the spread turns each item into a function arg, which can hit engine limits. Nested
      // for loop is safer than `.flat()` because that would create a copy.
      for (const logs of fetches) {
        for (const log of logs) {
          allLogs.push(log);
        }
      }
    } catch (error) {
      const context = `[logsCache] Gap fetch failed for ${rangesToFetch.length} range(s): ${rangesToFetch.map((r) => `[${r.fromBlock}n, ${r.toBlock}n]`).join(", ")}`;
      if (error instanceof Error) {
        error.message = `${context} ${error.message}`;
        throw error;
      }
      throw new Error(`${context} ${String(error)}`);
    }
  }

  return allLogs
    .filter(isInBlockRange({ fromBlock, toBlock }))
    .sort((a, b) => hexToNumber(a.blockNumber!) - hexToNumber(b.blockNumber!));
}
