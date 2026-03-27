import { type EIP1193RequestFn, hexToBigInt, hexToNumber, type RpcLog, toHex } from "viem";

import { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import type { BlockRange, RpcSignature, Store } from "../../../types.js";
import { divideBlockRange, isInBlockRange, mergeBlockRanges, resolveBlockNumber } from "../../../utils/blocks.js";
import { parse, stringify } from "../../../utils/json.js";
import { min } from "../../../utils/math.js";
import type { LogsDividerRpcSchema } from "../../logs-divider/schema.js";
import type { LogsResponse } from "../../logs-divider/types.js";
import { keychain } from "../keychain.js";
import type { LogsCacheRpcSchema } from "../schema.js";
import type { InvalidationStrategy } from "../types.js";

import { createSink } from "./sink.js";
import type { CachedChunk } from "./types.js";

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
  [filter, options]: RpcSignature<LogsCacheRpcSchema, "eth_getLogs">["Parameters"],
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
  const reduce = options?.reduce;

  // blockHash queries are not cached - pass through
  if (filter.blockHash) {
    throw new Error(`[logsCache] eth_getLogs blockHash queries are not supported.`);
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

  // Create LazyNdjsonMap streaming wrapper around store data. Thanks to mutex, we own buffers here.
  let buffers = (await store.get(blobKey)) ?? [];
  const ndjson = new LazyNdjsonMap<CachedChunk>(
    { toJson: stringify, fromJson: parse },
    { autoFlushThresholdBytes: 1 << 24 }, // 16 MB
    {
      get: () => buffers,
      set: (value) => {
        buffers = value;
        void store.set(blobKey, value);
      },
    },
  );

  // Generate bin-aligned ranges and try to read from cache
  const ranges = divideBlockRange({ fromBlock, toBlock }, binSize, binSize);
  const isRequestedLog = isInBlockRange({ fromBlock, toBlock });
  const allLogs: RpcLog[] = [];
  let reduced: RpcLog[] = [];

  const consumeLogs = (logs: RpcLog[]) => {
    for (const log of logs) {
      if (!isRequestedLog(log)) continue;

      if (reduce) {
        // TODO: Readonly or .freeze to protect logs from mutation
        reduced = reduce(reduced, log);
      } else {
        allLogs.push(log);
      }
    }
  };

  // Match requested bins against the blob, collecting cache misses as gaps.
  const desiredRanges = new Map<string, BlockRange>(
    ranges.map((range) => [keychain.entryKey(chainId, "eth_getLogs", range), range]),
  );
  const gaps: BlockRange[] = [];

  for await (const record of ndjson.records()) {
    if (desiredRanges.size === 0) break;

    const desired = desiredRanges.get(record.key);
    if (!desired) continue;

    desiredRanges.delete(record.key);

    const validLogs = tryUseCachedRange(record.value, desired, ranges.length, invalidationStrategy);
    if (validLogs !== null) {
      consumeLogs(validLogs);
    } else {
      gaps.push(desired);
    }
  }

  for (const range of desiredRanges.values()) {
    gaps.push(range);
  }

  // Fetch missing chunks - process gaps sequentially
  // (Inner transport handles concurrency for each gap range)
  if (gaps.length > 0) {
    const rangesToFetch = mergeBlockRanges(gaps);

    const sink = createSink({ chainId, binSize, ndjson });
    const onLogsResponse = (response: LogsResponse) => {
      sink(response);
      consumeLogs(response.logs);
    };

    try {
      await Promise.all(
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
                  onLogsResponse,
                  onLogsResponseOnly: true,
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
    } finally {
      await ndjson.flush(); // TODO: decide whether to await this
    }
  }

  if (reduce) {
    return reduced;
  }

  // TODO: bigint-native sort comparator
  return allLogs.sort((a, b) => hexToNumber(a.blockNumber!) - hexToNumber(b.blockNumber!));
}
