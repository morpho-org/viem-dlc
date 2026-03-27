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

  // Optimistically kickoff `latestBlockNumber` and `buffers` promises in parallel
  const preflight = [requestFn({ method: "eth_blockNumber" }, { dedupe: true }), store.get(blobKey)] as const;

  // Resolve block tags to numbers
  const latestBlockNumber = hexToBigInt(await preflight[0]);
  const fromBlock = resolveBlockNumber(filter.fromBlock ?? "earliest", latestBlockNumber);
  const toBlock = min(resolveBlockNumber(filter.toBlock ?? "latest", latestBlockNumber), latestBlockNumber);

  if (fromBlock > toBlock) {
    return [];
  }
  // TODO: handle the above + case where they're above latest, maybe throw errors, both here and in divider.
  // TODO: also maybe update divideBlockRange to allow only aligning fromBlock to help avoid this in divider

  // Create LazyNdjsonMap streaming wrapper around data from the store. Thanks to mutex, we own buffers here.
  let buffers = (await preflight[1]) ?? [];
  const ndjson = new LazyNdjsonMap<CachedChunk>(
    { toJson: stringify, fromJson: parse },
    { autoFlushThresholdBytes: 1 << 26 }, // 64MB (flushing too often strains CPU, flushing too late strains memory)
    {
      get: () => buffers,
      set: (value) => {
        buffers = value;
        void store.set(blobKey, value);
      },
    },
  );

  // Generate bin-aligned ranges and try to read from cache
  // const ranges = divideBlockRange({ fromBlock, toBlock }, binSize, binSize);

  // // Match requested bins against the blob, collecting cache misses as gaps.
  // const desiredRanges = new Map<string, BlockRange>(
  //   ranges.map((range) => [keychain.entryKey(chainId, "eth_getLogs", range).metadata, range]),
  // );

  const expectedMetadataRanges = new Map<string, BlockRange>();
  const expectedDataKeys = new Set<string>();

  // Generate bin-aligned ranges and populate expectation maps
  for (const range of divideBlockRange({ fromBlock, toBlock }, binSize, binSize)) {
    const ek = keychain.entryKey(chainId, "eth_getLogs", range);
    expectedMetadataRanges.set(ek.metadata, range);
    expectedDataKeys.add(ek.data);
  }

  // Determine which ranges are stale and/or missing
  const gaps: BlockRange[] = [];

  for await (const record of ndjson.records()) {
    // Stop if we found all ranges *or* if key's prefix indicates we've passed all metadata
    if (expectedMetadataRanges.size === 0 || !record.key.startsWith("0:")) break;

    const range = expectedMetadataRanges.get(record.key);
    if (!range) continue;
    expectedMetadataRanges.delete(record.key);

    if (
      record.value.__type === "metadata" &&
      shouldFetchRange(record.value, range, expectedDataKeys.size, invalidationStrategy)
    ) {
      gaps.push(range);
    }
  }

  for (const range of expectedMetadataRanges.values()) {
    gaps.push(range);
  }

  // Start fetching all gaps. `logsDivider` and `rateLimiter` handle splitting, concurrency, and rate limits.
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
    expectedDataKeys.delete(entry.key);

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

  if (expectedDataKeys.size > 0) {
    // This should not happen. If it does, data stack atomicity is broken or we got hit with bit flips.
    console.warn(`[logsCache] eth_getLogs handler detected missing keys in data blob: ${expectedDataKeys}`)
  }

  return reduce ? result : result.sort((a, b) => hexToNumber(a.blockNumber!) - hexToNumber(b.blockNumber!));
}
