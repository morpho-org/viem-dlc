import { createTransport, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import type { EIP1193Parameters } from "../../types.js";
import { hash } from "../../utils/hash.js";
import { createDedupe } from "../../utils/with-dedupe.js";
import { createRateLimit } from "../../utils/with-rate-limit.js";

import { type RateLimiterSchema, stripAdditionalParameters } from "./schema.js";
import type { RateLimiterConfig } from "./types.js";

export type * from "./schema.js";
export type * from "./types.js";

export const key = "viem-dlc-rate-limiter" as const;

/**
 * Creates a transport wrapper that rate-limits all RPC requests using a token bucket.
 *
 * Features:
 * - Token bucket rate limiting with burst capacity
 * - Concurrency limiting
 * - Priority scheduling (lower numeric priority runs first)
 * - FIFO ordering within same priority
 *
 * The token bucket starts full, allowing an initial burst of up to `maxBurstRequests`.
 * After the burst, requests are rate-limited to `maxRequestsPerSecond`. Requests that
 * arrive when no tokens are available are queued and processed by priority, then FIFO.
 *
 * @example
 * const client = createPublicClient({
 *   chain: mainnet,
 *   transport: rateLimiter(
 *     http('https://eth-mainnet.example.com'),
 *     [{ maxRequestsPerSecond: 10, maxBurstRequests: 5, maxConcurrentRequests: 3 }]
 *   )
 * })
 */
export function rateLimiter(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<PublicRpcSchema>>,
  [{ maxRequestsPerSecond = 20, maxBurstRequests = 1, maxConcurrentRequests = Infinity, dedupe = false }]: [
    RateLimiterConfig,
  ],
): Transport<typeof key, unknown, EIP1193RequestFn<RateLimiterSchema>> {
  return (params) => {
    const transport = baseTransportFn(params);
    const { withRateLimit } = createRateLimit(maxBurstRequests, maxRequestsPerSecond, maxConcurrentRequests);
    const { withDedupe } = createDedupe();

    const request = (req: EIP1193Parameters<RateLimiterSchema>) => {
      const [baseReq, additional] = stripAdditionalParameters(req);
      const inner = () => withRateLimit(() => transport.request(baseReq), { priority: additional?.[0].priority });

      return dedupe ? withDedupe(inner, { key: hash(baseReq) }) : inner();
    };

    return createTransport({
      key,
      name: "[viem-dlc] rate-limiter",
      request: request as EIP1193RequestFn,
      retryCount: params.retryCount,
      timeout: params.timeout,
      type: key,
    });
  };
}
