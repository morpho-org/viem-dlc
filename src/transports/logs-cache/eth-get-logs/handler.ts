import { type EIP1193RequestFn, hexToBigInt, hexToNumber, type RpcLog, toHex } from "viem";

import { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import type { Entry } from "../../../internal/ndjson-map.js";
import type { BlockRange, RpcSignature, Store } from "../../../types.js";
import { divideBlockRange, isInBlockRange, mergeBlockRanges, resolveBlockNumber } from "../../../utils/blocks.js";
import { parse, stringify } from "../../../utils/json.js";
import { min } from "../../../utils/math.js";
import type { LogsDividerRpcSchema } from "../../logs-divider/schema.js";
import { keychain } from "../keychain.js";
import type { LogsCacheRpcSchema } from "../schema.js";
import type { InvalidationStrategy } from "../types.js";

import { createSink } from "./sink.js";
import type { CachedChunk, CachedLogs, CachedMetadata } from "./types.js";

/** Returns true if the cached range should be re-fetched. */
function shouldFetchRange(
  cached: CachedMetadata,
  desired: BlockRange,
  totalChunks: number,
  invalidationStrategy: InvalidationStrategy,
) {
  // Check if cached data covers the fetch range.
  // NOTE: Currently this is extra defensive since the logs sink only writes complete bins.
  if (cached.fetchedRange.toBlock < desired.toBlock) {
    return true;
  }

  // Check probabilistic invalidation
  // blocksAgo is computed from the chain tip at fetch time, not now,
  // as that's what indicates reorg likelihood
  const probability = invalidationStrategy({
    confirmations: Number(cached.fetchedAtBlock - cached.fetchedRange.toBlock),
    cacheAgeMs: Date.now() - cached.fetchedAt,
    totalChunks,
  });

  return Math.random() < probability;
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

  // TODO: could put this in a promise.all with `store.get`
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

  // Match requested bins against the blob, collecting cache misses as gaps.
  const desiredRanges = new Map<string, BlockRange>(
    ranges.map((range) => [keychain.entryKey(chainId, "eth_getLogs", range).metadata, range]),
  );
  const gaps: BlockRange[] = [];

  for await (const record of ndjson.records()) {
    // Stop if we found all `desiredRanges` or if key's prefix indicates it's something other than metadata
    if (desiredRanges.size === 0 || !record.key.startsWith("0:")) break;

    const desired = desiredRanges.get(record.key);
    if (!desired) continue;

    desiredRanges.delete(record.key);

    if (
      record.value.__type === "metadata" &&
      shouldFetchRange(record.value, desired, ranges.length, invalidationStrategy)
    ) {
      gaps.push(desired);
    }
  }

  for (const range of desiredRanges.values()) {
    gaps.push(range);
  }

  // Fetch missing chunks
  // (Inner transport handles concurrency for each gap range)
  if (gaps.length > 0) {
    const rangesToFetch = mergeBlockRanges(gaps);

    const sink = createSink({ chainId, binSize, ndjson });

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
                  onLogsResponse: sink,
                  onLogsResponseOnly: true,
                },
              ],
            },
            { dedupe: true },
          ),
        ),
      );
    } catch (error) {
      await ndjson.flush().catch(() => {});
      const context = `[logsCache] Gap fetch failed for ${rangesToFetch.length} range(s): ${rangesToFetch.map((r) => `[${r.fromBlock}n, ${r.toBlock}n]`).join(", ")}`;
      if (error instanceof Error) {
        error.message = `${context} ${error.message}`;
        throw error;
      }
      throw new Error(`${context} ${String(error)}`);
    }
  }

  // Fold callback shared by both the flush+fold path (gaps) and the pure-read path (no gaps).
  // Skips metadata entries (0: prefix) and processes logs entries (1: prefix).
  const isRequestedLog = isInBlockRange({ fromBlock, toBlock });
  const processEntry = (acc: RpcLog[], entry: Entry<CachedChunk>): RpcLog[] => {
    if (!entry.key.startsWith("1:")) return acc;
    const logs = entry.value as CachedLogs;
    for (const log of logs) {
      if (!isRequestedLog(log)) continue;
      if (reduce) {
        acc = reduce(acc, log);
      } else {
        acc.push(log);
      }
    }
    return acc;
  };

  // Flush pending writes (if any) + fold through all entries in a single decompression pass.
  // When there are no pending writes (no gaps, or empty responses), this degenerates to a pure read.
  const result = await ndjson.flushAndFold(processEntry, [] as RpcLog[]);
  return reduce ? result : result.sort((a, b) => hexToNumber(a.blockNumber!) - hexToNumber(b.blockNumber!));
}
