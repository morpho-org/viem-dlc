import { custom, type EIP1193Parameters, type PublicRpcSchema, type Transport } from "viem";

import type { EIP1193PublicRequestFn, EIP1193RequestOptions } from "../../types.js";
import { createTokenBucket, withRateLimit } from "../../utils/with-rate-limit.js";

export interface RateLimiterConfig {
  /** Rate at which requests are sent after burst is depleted. @default 10 */
  maxRequestsPerSecond?: number;
  /** Max requests that can be sent when starting from idle. @default 5 */
  maxBurstRequests?: number;
}

/**
 * Creates a transport wrapper that rate-limits all RPC requests using a token bucket.
 *
 * Features:
 * - Token bucket rate limiting with burst capacity
 * - FIFO ordering guaranteed (requests processed in order received)
 * - Configurable requests per second and concurrent request limit
 *
 * The token bucket starts full, allowing an initial burst of up to `maxBurstRequests`.
 * After the burst, requests are rate-limited to `maxRequestsPerSecond`. Requests that
 * arrive when no tokens are available are queued and processed in FIFO order.
 *
 * @example
 * // Basic usage
 * const client = createPublicClient({
 *   chain: mainnet,
 *   transport: rateLimiter(
 *     http('https://eth-mainnet.example.com'),
 *     {
 *       maxRequestsPerSecond: 10,
 *       maxBurstRequests: 5
 *     }
 *   )
 * })
 *
 * @example
 * // Composed with other transports
 * const transport = logsCache(
 *   logsDivider(
 *     rateLimiter(http(url), { maxRequestsPerSecond: 10 }),
 *     { maxBlockRange: 10_000 }
 *   ),
 *   cacheConfig
 * )
 */
export function rateLimiter(
  // biome-ignore lint/suspicious/noExplicitAny: this `any` matches the underlying viem type's default
  baseTransportFn: Transport<string, Record<string, any>, EIP1193PublicRequestFn>,
  { maxRequestsPerSecond = 10, maxBurstRequests = 5 }: RateLimiterConfig,
): Transport {
  if (maxRequestsPerSecond <= 0) {
    throw new Error(`maxRequestsPerSecond must be positive, got ${maxRequestsPerSecond}`);
  }
  if (maxBurstRequests < 1) {
    throw new Error(`maxConcurrency must be at least 1, got ${maxBurstRequests}`);
  }

  return (params) => {
    const baseTransport = baseTransportFn(params);
    const bucket = createTokenBucket(maxBurstRequests, maxRequestsPerSecond);

    const request = (args: EIP1193Parameters<PublicRpcSchema>, options?: EIP1193RequestOptions) => {
      return withRateLimit(() => baseTransport.request(args, options), {
        bucket,
      });
    };

    return custom({ request })(params);
  };
}
