import type { PublicRpcSchema, Transport } from "viem";
import { custom, type EIP1193RequestFn } from "viem";

import type { EIP1193Parameters, EIP1193RequestOptions } from "../../types.js";
import { type RateLimiterConfig, rateLimiter } from "../rate-limiter/index.js";

import { handleGetLogs } from "./handlers.js";
import type { LogsDividerRpcSchema } from "./schema.js";
import type { LogsDividerConfig } from "./types.js";

export type * from "./schema.js";
export type * from "./types.js";

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
  [{ maxBlockRange, maxLogBytes, alignTo }, rateLimiterConfig]: [LogsDividerConfig, RateLimiterConfig],
  // biome-ignore lint/suspicious/noExplicitAny: this `any` matches the underlying viem type's default
): Transport<"custom", Record<string, any>, EIP1193RequestFn<LogsDividerRpcSchema>> {
  if (Number.isNaN(maxBlockRange) || maxBlockRange < 1) {
    throw new Error(`[logsDivider] maxBlockRange must be > 1 (got ${maxBlockRange})`);
  }
  if (maxLogBytes !== undefined && (!Number.isSafeInteger(maxLogBytes) || maxLogBytes < 1)) {
    throw new Error(`[logsDivider] maxLogBytes must be undefined or a safe integer > 1 (got ${maxLogBytes})`);
  }

  return (params) => {
    const transport = rateLimiter(baseTransportFn, [rateLimiterConfig])(params);

    const request = (args: EIP1193Parameters<LogsDividerRpcSchema>, options?: EIP1193RequestOptions) => {
      if (args.method !== "eth_getLogs") {
        return transport.request(args, options);
      }

      // TODO: (@haydenshively future-work) `handleGetLogs` could respect `options`
      return handleGetLogs(transport.request, args.params, {
        maxBlockRange,
        maxLogBytes,
        alignTo,
      });
    };

    return custom({ request })(params);
  };
}
