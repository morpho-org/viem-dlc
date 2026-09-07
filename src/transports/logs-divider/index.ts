import { createTransport, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import { createFacetId, observe } from "../../observability.js";
import type { EIP1193Parameters } from "../../types.js";
import { isRevertExpected } from "../../utils/deployless/codec.envelope.js";
import { logsEnricher } from "../logs-enricher/index.js";
import type { LogsEnricherConfig } from "../logs-enricher/types.js";
import { type LogsSieveConfig, logsSieve } from "../logs-sieve/index.js";
import { type RateLimiterConfig, rateLimiter } from "../rate-limiter/index.js";

import { handleEthGetLogs } from "./handlers.js";
import { type LogsDividerSchema, logsDividerTransportKey } from "./schema.js";
import type { LogsDividerConfig } from "./types.js";

export type * from "./schema.js";
export type * from "./types.js";
export { logsDividerTransportKey };

/**
 * Creates a transport wrapper that divides large eth_getLogs requests into smaller chunks.
 *
 * Internally composes a `rateLimiter` transport for rate and concurrency limiting.
 *
 * Features:
 * - Divides requests exceeding maxBlockRange into smaller chunks
 * - Automatic retry with range halving on "range too large" errors
 * - Optional chunk alignment for cache optimization
 * - Optional logs response callback for progressive updates
 * - Priority-based scheduling (chunks processed roughly in order)
 *
 * @example
 * // Basic usage
 * const client = createPublicClient({
 *   chain: mainnet,
 *   transport: logsDivider(
 *     http('https://eth-mainnet.example.com'),
 *     [{ maxBlockRange: 100_000 }, { maxRequestsPerSecond: 10 }]
 *   )
 * })
 *
 * @example
 * // With alignment and progressive callback
 * const transport = logsDivider(
 *   http(url),
 *   [
 *     { maxBlockRange: 100_000, alignTo: 10_000 },
 *     { maxRequestsPerSecond: 10, maxConcurrentRequests: 5 }
 *   ]
 * )
 * // onLogsResponse is passed per-request, not in config:
 * const logs = await client.request({
 *   method: 'eth_getLogs',
 *   params: [filter, undefined, {
 *     onLogsResponse: ({ logs, fromBlock, toBlock }) => {
 *       console.log(`Fetched ${logs.length} logs from ${fromBlock}-${toBlock}`)
 *     }
 *   }]
 * })
 */
export function logsDivider(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<PublicRpcSchema>>,
  [logsDividerConfig, logsEnricherConfig, logsSieveConfig, rateLimiterConfig]: [
    LogsDividerConfig,
    LogsEnricherConfig,
    LogsSieveConfig,
    RateLimiterConfig,
  ],
): Transport<typeof logsDividerTransportKey, Record<string, never>, EIP1193RequestFn<LogsDividerSchema>> {
  if (Number.isNaN(logsDividerConfig.maxBlockRange) || logsDividerConfig.maxBlockRange < 1) {
    throw new Error(`[logsDivider] maxBlockRange must be >= 1 (got ${logsDividerConfig.maxBlockRange})`);
  }

  const facetId = createFacetId(logsDividerTransportKey);

  return (params) => {
    const transport = logsEnricher(logsSieve(rateLimiter(baseTransportFn, [rateLimiterConfig]), [logsSieveConfig]), [
      logsEnricherConfig,
    ])(params);

    const request = (req: EIP1193Parameters<LogsDividerSchema>) => {
      if (req.method !== "eth_getLogs") {
        return transport.request(req, isRevertExpected(req) ? { retryCount: 0 } : undefined);
      }

      return handleEthGetLogs(transport.request, req.params, logsDividerConfig, facetId);
    };

    return createTransport({
      key: logsDividerTransportKey,
      name: "[viem-dlc] logs-divider",
      request: observe(request, facetId) as EIP1193RequestFn,
      retryCount: 0,
      type: logsDividerTransportKey,
    });
  };
}
