import { createTransport, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import { createFacetId, getObservability, observe } from "../../observability.js";
import type { EIP1193Parameters } from "../../types.js";
import { isRevertExpected } from "../../utils/deployless/codec.envelope.js";
import { hash } from "../../utils/hash.js";
import { createDedupe } from "../../utils/with-dedupe.js";
import { createRateLimit } from "../../utils/with-rate-limit.js";

import { type RateLimiterSchema, stripAdditionalParameters } from "./schema.js";
import type { RateLimiterConfig } from "./types.js";

export type * from "./schema.js";
export type * from "./types.js";

export const rateLimiterTransportKey = "viem-dlc-rate-limiter" as const;

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
): Transport<typeof rateLimiterTransportKey, unknown, EIP1193RequestFn<RateLimiterSchema>> {
  const facetId = createFacetId(rateLimiterTransportKey);

  return (params) => {
    const transport = baseTransportFn(params);
    const { withRateLimit } = createRateLimit(maxBurstRequests, maxRequestsPerSecond, maxConcurrentRequests);
    const { withDedupe } = createDedupe();

    const request = (req: EIP1193Parameters<RateLimiterSchema>) => {
      const [baseReq, additional] = stripAdditionalParameters(req);
      // Captured here rather than looked up inside `onAdmitted`, so the sample lands on
      // this call's slot no matter which job's completion drained the queue. Crossed once
      // per chunk under a divider fan-out, so `stat` summarizes the whole call's waiting.
      const facet = getObservability()?.facet(facetId);
      const inner = () =>
        withRateLimit(() => transport.request(baseReq, isRevertExpected(baseReq) ? { retryCount: 0 } : undefined), {
          priority: additional?.[0].priority,
          onAdmitted: (waitMs) => facet?.stat("queue_wait_ms", waitMs),
        });

      return dedupe ? withDedupe(inner, { key: hash(baseReq) }) : inner();
    };

    return createTransport({
      key: rateLimiterTransportKey,
      name: "[viem-dlc] rate-limiter",
      request: observe(request, facetId, params.chain?.id) as EIP1193RequestFn,
      retryCount: 0,
      type: rateLimiterTransportKey,
    });
  };
}
