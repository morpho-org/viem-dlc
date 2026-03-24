import { custom, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import type { EIP1193Parameters, EIP1193RequestOptions } from "../../types.js";
import { cyrb64Hash } from "../../utils/hash.js";
import { withDedupe } from "../../utils/with-dedupe.js";
import { createKeyedMutex } from "../../utils/with-keyed-mutex.js";
import { type LogsDividerConfig, logsDivider } from "../logs-divider/index.js";
import type { LogsSieveConfig } from "../logs-sieve/types.js";
import type { RateLimiterConfig } from "../rate-limiter/index.js";

import { handleGetLogs } from "./handlers.js";
import { keychain } from "./keychain.js";
import { normalize } from "./normalization.js";
import type { CachedMethod, LogsCacheRpcSchema } from "./schema.js";
import type { InvalidationStrategy, LogsCacheConfig } from "./types.js";

export type * from "./schema.js";
export type * from "./types.js";

/**
 * @param alphaAge Exponential growth rate w.r.t cache entry age (in time). @default 1/8
 * @param maxAgeDays Cache entries older than this are always invalidated. @default 31
 * @param alphaBlocks Exponential growth rate w.r.t cache entry age (in blocks). @default 1/64
 * @param scaleBlocks Scaling factor on block-based exponential (to tune probability of
 * invalidating entry at chain tip). @default 7/8
 */
export function createExponentialInvalidation(
  alphaAge = 1 / 8,
  maxAgeDays = 31,
  alphaBlocks = 1 / 64,
  scaleBlocks = 7 / 8,
): InvalidationStrategy {
  return ({ confirmations, cacheAgeMs }) => {
    const msPerDay = 24 * 60 * 60 * 1000;
    const cacheAgeDays = cacheAgeMs / msPerDay;

    const eAge = Math.exp(-alphaAge * maxAgeDays);
    const cAge = 1 / (1 - eAge);
    const zAge = cAge * (Math.exp(alphaAge * (cacheAgeDays - maxAgeDays)) - eAge);

    const zBlocks = scaleBlocks * Math.exp(-alphaBlocks * confirmations);

    return Math.max(0, Math.min(zAge + zBlocks, 1));
  };
}

/**
 * @param minAgeMs Cache entries younger than this are never invalidated. @default 5_000
 * @param maxAgeDays Cache entries older than this are always invalidated. @default 31
 * @param numHotBlocks Cache entries that were within this many blocks of chain tip (when fetched)
 * are always invalidated (as long as `cacheAgeMs >= minCacheAgeMs`). @default 128
 * @param avgInvalidationsPerRequest The average number of chunks to invalidate per request.
 * If < 1, e.g. 0.01, interpret as "Invalidate 1 old entry every 1/0.01=100 requests". @default 0.001
 */
export function createSimpleInvalidation(
  minAgeMs = 5_000,
  maxAgeDays = 31,
  numHotBlocks = 128,
  avgInvalidationsPerRequest = 0.001,
): InvalidationStrategy {
  return ({ confirmations, cacheAgeMs, totalChunks }) => {
    if (cacheAgeMs < minAgeMs) return 0;

    if (confirmations < numHotBlocks) return 1;

    if (avgInvalidationsPerRequest === 0) return 0;

    const msPerDay = 24 * 60 * 60 * 1000;
    const cacheAgeDays = cacheAgeMs / msPerDay;

    const initialValue = Math.min(1, avgInvalidationsPerRequest / totalChunks);
    const alpha = -Math.log(initialValue) / maxAgeDays;

    return Math.min(1, initialValue * Math.exp(alpha * cacheAgeDays));
  };
}

/**
 * Creates an all-in-one caching transport for eth_getLogs calls.
 *
 * Internally composes three layers:
 * - **rateLimiter**: Controls RPC request rate (token bucket + concurrency limit + priority queue)
 * - **logsDivider**: Splits large requests, retries with range halving on failure
 * - **cache**: Reads from cache, fetches gaps, writes complete bins via accumulator
 *
 * The `binSize` determines cache entry granularity. Requests are aligned to bin boundaries
 * to maximize cache hits. Smaller bins allow finer-grained invalidation but increase
 * storage overhead.
 *
 * **Configuration Considerations**
 * - `alignTo % binSize === 0`: Required so that fetches eventually cover all bins (otherwise accumulators dangle)
 * - `alignTo = binSize`: Recommended so that requested ranges aren't extended more than is necessary for cache
 * - `maxBlockRange`: Can be any value. Smaller values mean more accumulation before cache writes;
 *   larger values may hit RPC limits and trigger halving.
 *
 * @example
 * const transport = logsCache(
 *   http(rpcUrl),
 *   [
 *     { binSize: 10_000, store: new LruStore(), invalidationStrategy: createSimpleInvalidation() },
 *     { maxBlockRange: 100_000 },
 *     { maxRequestsPerSecond: 10, maxConcurrentRequests: 5 }
 *   ]
 * )
 *
 * const client = createPublicClient({ chain: mainnet, transport })
 */
export function logsCache(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<PublicRpcSchema>>,
  [{ binSize, store, invalidationStrategy }, logsDividerConfig, ...otherConfigs]: [
    LogsCacheConfig,
    Omit<LogsDividerConfig, "alignTo">,
    RateLimiterConfig,
    LogsSieveConfig,
  ],
  // biome-ignore lint/suspicious/noExplicitAny: this `any` matches the underlying viem type's default
): Transport<"custom", Record<string, any>, EIP1193RequestFn<LogsCacheRpcSchema>> {
  return (params) => {
    if (params.chain === undefined) {
      throw new Error("You must pass a chain to the logsCache transport.");
    }
    const chainId = params.chain.id;

    const transport = logsDivider(baseTransportFn, [{ ...logsDividerConfig, alignTo: binSize }, ...otherConfigs])(
      params,
    );

    const { withKeyedMutex } = createKeyedMutex();

    // TODO: I think `options` are always undefined because of viem internals. document? what to do with them if real?
    const request = (args: EIP1193Parameters<LogsCacheRpcSchema>, options?: EIP1193RequestOptions) => {
      args = normalize(args);
      // TODO: compare args against allowlist

      // Compute hash of normalized request args, for use as dedupe id
      const requestHash = cyrb64Hash(JSON.stringify(args));

      // Dedupe all requests
      return withDedupe(
        () => {
          switch (args.method) {
            case "eth_call": {
              const blobKey = keychain.blobKey(chainId, args);
              const run = () =>
                transport.request(
                  { method: args.method, params: [args.params[0], args.params[1], args.params[2], args.params[3]] },
                  options,
                );

              return blobKey ? withKeyedMutex(blobKey, run) : run();
            }
            case "eth_getLogs": {
              const blobKey = keychain.blobKey(chainId, args);
              const run = () =>
                handleGetLogs(transport.request, chainId, args.params, blobKey, {
                  binSize,
                  invalidationStrategy,
                  store,
                });

              return withKeyedMutex(blobKey, run);
            }
            default: {
              // Assert that all `CachedMethod` are handled explicitly
              const _: never = args.method as Extract<typeof args.method, CachedMethod>;
              return transport.request(args, options);
            }
          }
        },
        { enabled: true, id: requestHash },
      );
    };

    // TODO: have a better way of creating the transport so we can apply custom name and other props.
    return custom({ request })(params);
  };
}
