import { hexToBigInt, type RpcLog, toHex } from "viem";

import { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import type { LazyEntry } from "../../../internal/ndjson-map.js";
import type { BlockRange, EIP1193Parameters } from "../../../types.js";
import { divideBlockRange, extractRangeFromFilter, isInBlockRange, mergeBlockRanges } from "../../../utils/blocks.js";
import { tryCatch } from "../../../utils/errors.js";
import { parse, stringify } from "../../../utils/json.js";
import { keychain } from "../keychain.js";
import type { CacheSchema } from "../schema.js";
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
  // Check if cached data covers the desired range.
  // `desired` is aligned, so `cached.alignedRange` should always pass this check,
  // but `cached.fetchedRange` may not if this bin happened to be at chain tip last time
  // it was updated.
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
  req: EIP1193Parameters<CacheSchema, "eth_getLogs">,
): Promise<RpcLog[]> {
  const blobKey = keychain.blobKey(chainId, req);

  return coalesce(blobKey, req, async (args, collectFollowers) => {
    /*//////////////////////////////////////////////////////////////
                               LEADER OPS
    //////////////////////////////////////////////////////////////*/
    const [filter] = args.params;

    // blockHash queries are not cached - pass through
    if (filter.blockHash) {
      throw new Error(`[cache] eth_getLogs blockHash queries are not supported.`);
    }

    // Optimistically kickoff `latestBlockNumber` and `buffers` promises in parallel
    const preflight = [requestFn({ method: "eth_blockNumber" }), store.get(blobKey)] as const;

    // Resolve block tags to numbers
    const latestBlockNumber = hexToBigInt(await preflight[0]);
    const requestedRange = extractRangeFromFilter(filter, latestBlockNumber);

    if (requestedRange.fromBlock > requestedRange.toBlock) {
      return { leader: { action: "resolve", result: [] } };
    }
    // TODO: handle the above + case where they're above latest, maybe throw errors, both here and in divider.
    // TODO: also maybe update divideBlockRange to allow only aligning fromBlock to help avoid this in divider

    const expectedMetadataRanges = new Map<string, BlockRange>();
    const expectedDataKeys = new Set<string>();

    // Generate bin-aligned ranges and populate expectation maps
    for (const range of divideBlockRange(requestedRange, binSize, binSize)) {
      const ek = keychain.entryKey(chainId, "eth_getLogs", range);
      expectedMetadataRanges.set(ek.metadata, range);
      expectedDataKeys.add(ek.data);
    }

    // Create LazyNdjsonMap streaming wrapper around data from the store. Thanks to mutex, we own buffers here.
    let buffers = (await preflight[1]) ?? [];
    const ndjson = new LazyNdjsonMap<CachedChunk>(
      { toJson: stringify, fromJson: parse },
      {
        get: () => buffers,
        set: (value) => {
          buffers = value;
          void store.set(blobKey, value);
        },
      },
      { debounceMs: 500, maxDelayMs: 2_500 },
    );

    // Determine which ranges are stale and/or missing
    const gaps: BlockRange[] = [];

    await ndjson.scan((record) => {
      // Stop if we found all ranges *or* if key's prefix indicates we've passed all metadata
      if (expectedMetadataRanges.size === 0 || !record.key.startsWith("0:")) return false;

      const range = expectedMetadataRanges.get(record.key);
      if (!range) return;
      expectedMetadataRanges.delete(record.key);

      if (
        record.value.__type !== "metadata" ||
        shouldFetchRange(record.value, range, expectedDataKeys.size, invalidationStrategy)
      ) {
        gaps.push(range);
      }
    });

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
            requestFn({
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
            }),
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

    // Collect followers whose filter matches the leader's (only search/reduce may differ).
    // Matching followers share this decompression pass; non-matching ones are deferred.
    // Leader is prepended at index 0; matching followers follow.
    const leader = { slot: -1, args };
    const followers = collectFollowers();

    const leaderFilterJson = JSON.stringify(filter);
    const participants = [leader, ...followers]
      .filter((f) => JSON.stringify(f.args.params[0]) === leaderFilterJson)
      .map((f) => {
        const search = tryCatch(() =>
          f.args.params[1]?.search ? new RegExp(f.args.params[1].search, "i") : undefined,
        );
        return {
          slot: f.slot,
          reduce: f.args.params[1]?.reduce,
          search: search.result,
          error: search.error,
        };
      });

    // Single decompression pass applies all reducers.
    const isRequestedLog = isInBlockRange(requestedRange);
    const processEntry = (accs: RpcLog[][], entry: LazyEntry<CachedChunk>): RpcLog[][] => {
      if (!entry.key.startsWith("1:")) return accs;
      expectedDataKeys.delete(entry.key);

      // Test each participant's search pattern against the raw NDJSON once per entry.
      // Skip JSON parsing entirely if no participant matches.
      const matched: number[] = [];
      participants.forEach((p, i) => {
        if (p.error || (p.search && !p.search.test(entry.rawValue))) return;
        matched.push(i);
      });
      if (matched.length === 0) return accs;

      const logs = entry.value as CachedLogs;
      for (const log of logs) {
        if (!isRequestedLog(log)) continue;
        for (const i of matched) {
          if (participants[i]!.error) continue;
          try {
            const reduce = participants[i]!.reduce;
            if (reduce) {
              accs[i] = reduce(accs[i]!, log);
            } else {
              accs[i]!.push(log);
            }
          } catch (error) {
            participants[i]!.error = error;
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
      console.warn(`[cache] eth_getLogs handler detected missing keys in data blob: ${[...expectedDataKeys]}`);
    }

    const outcomes = participants.map((p, i) =>
      p.error
        ? { slot: p.slot, action: "reject" as const, error: p.error }
        : { slot: p.slot, action: "resolve" as const, result: accs[i]! },
    );

    return {
      leader: outcomes[0]!,
      followers: outcomes.slice(1),
    };
  });
}
