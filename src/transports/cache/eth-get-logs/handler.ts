import { hexToBigInt, type RpcLog, toHex } from "viem";

import { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import type { Entry } from "../../../internal/ndjson-map.js";
import type { BlockRange, EIP1193Parameters } from "../../../types.js";
import { divideBlockRange, extractRangeFromFilter, isInBlockRange, mergeBlockRanges } from "../../../utils/blocks.js";
import { parse, stringify } from "../../../utils/json.js";
import { keychain } from "../keychain.js";
import type { CacheRpcSchema } from "../schema.js";
import type { HandlerContext, InvalidationStrategy } from "../types.js";

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

export async function handleEthGetLogs(
  { binSize, invalidationStrategy, store, coalesce, requestFn, chainId }: HandlerContext,
  req: EIP1193Parameters<CacheRpcSchema, "eth_getLogs">,
): Promise<RpcLog[]> {
  const blobKey = keychain.blobKey(chainId, req);

  return coalesce(blobKey, req, async ({ params: [filter, options] }, collectFollowers) => {
    /*//////////////////////////////////////////////////////////////
                               LEADER OPS
    //////////////////////////////////////////////////////////////*/

    // blockHash queries are not cached - pass through
    if (filter.blockHash) {
      throw new Error(`[cache] eth_getLogs blockHash queries are not supported.`);
    }

    // Optimistically kickoff `latestBlockNumber` and `buffers` promises in parallel
    const preflight = [requestFn({ method: "eth_blockNumber" }, { dedupe: true }), store.get(blobKey)] as const;

    // Resolve block tags to numbers
    const latestBlockNumber = hexToBigInt(await preflight[0]);
    const requestedRange = extractRangeFromFilter(filter, latestBlockNumber);

    if (requestedRange.fromBlock > requestedRange.toBlock) {
      return { leader: { action: "resolve", result: [] } };
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

    const expectedMetadataRanges = new Map<string, BlockRange>();
    const expectedDataKeys = new Set<string>();

    // Generate bin-aligned ranges and populate expectation maps
    for (const range of divideBlockRange(requestedRange, binSize, binSize)) {
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
    // viem also provides request deduplication at each layer, and at this point we've already normalized it.
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
        const context = `[cache] Gap fetch failed for ${rangesToFetch.length} range(s): ${rangesToFetch.map((r) => `[${r.fromBlock}n, ${r.toBlock}n]`).join(", ")}`;
        if (error instanceof Error) {
          error.message = `${context} ${error.message}`;
          throw error;
        }
        throw new Error(`${context} ${String(error)}`);
      }
    }

    /*//////////////////////////////////////////////////////////////
                                FAN OUT
    //////////////////////////////////////////////////////////////*/

    // Collect followers whose filter matches the leader's (only reduce may differ).
    // Matching followers share this decompression pass; non-matching ones are deferred.
    // Leader is prepended at index 0; matching followers follow.
    const collected = collectFollowers();
    const filterJson = JSON.stringify(filter);
    const participants = collected
      .filter((f) => JSON.stringify(f.args.params[0]) === filterJson)
      .map((f) => ({ slot: f.slot, reduce: f.args.params[1]?.reduce }));
    participants.unshift({ slot: -1, reduce: options?.reduce });

    // Each reduce is isolated so a throwing reducer only kills its own participant.
    const failed = new Map<number, unknown>();

    // Single decompression pass applies all reducers.
    const isRequestedLog = isInBlockRange(requestedRange);
    const processEntry = (accs: RpcLog[][], entry: Entry<CachedChunk>): RpcLog[][] => {
      if (!entry.key.startsWith("1:")) return accs;
      expectedDataKeys.delete(entry.key);

      const logs = entry.value as CachedLogs;
      for (const log of logs) {
        if (!isRequestedLog(log)) continue;
        for (let i = 0; i < participants.length; i++) {
          if (failed.has(i)) continue;
          try {
            const reduce = participants[i]!.reduce;
            if (reduce) {
              accs[i] = reduce(accs[i]!, log);
            } else {
              accs[i]!.push(log);
            }
          } catch (e) {
            failed.set(i, e);
          }
        }
      }
      return accs;
    };

    const accs = await ndjson.flushAndFold(
      processEntry,
      participants.map(() => [] as RpcLog[]),
    );

    if (expectedDataKeys.size > 0) {
      console.warn(`[cache] eth_getLogs handler detected missing keys in data blob: ${expectedDataKeys}`);
    }

    const outcomes = participants.map((p, i) =>
      failed.has(i)
        ? { slot: p.slot, action: "reject" as const, error: failed.get(i) }
        : { slot: p.slot, action: "resolve" as const, result: accs[i]! },
    );

    return {
      leader: outcomes[0]!,
      followers: outcomes.slice(1),
    };
  });
}
