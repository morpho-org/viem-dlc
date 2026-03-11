import type { PublicRpcSchema, Transport } from "viem";
import { custom, type EIP1193Parameters, type EIP1193RequestFn } from "viem";

import type { EIP1193RequestOptions } from "../../types.js";

import { handleGetLogs } from "./handlers.js";
import type { LogsDividerRpcSchema } from "./schema.js";
import type { LogsDividerConfig } from "./types.js";

export type * from "./schema.js";
export type * from "./types.js";

/**
 * Creates a transport wrapper that divides large eth_getLogs requests into
 * smaller chunks with concurrency control.
 *
 * Features:
 * - Divides requests exceeding maxBlockRange into smaller chunks
 * - Concurrent request processing with maxConcurrentChunks limit
 * - Automatic retry with range halving on "range too large" errors
 * - Optional chunk alignment for cache optimization
 * - Optional logs response callback for progressive updates
 *
 * @example
 * // Basic usage
 * const client = createPublicClient({
 *   chain: mainnet,
 *   transport: logsDivider(
 *     http('https://eth-mainnet.example.com'),
 *     { maxBlockRange: 100_000 }
 *   )
 * })
 *
 * @example
 * // With alignment for cache optimization
 * const transport = logsCache(
 *   logsDivider(http(url), {
 *     maxBlockRange: 100_000,
 *     alignTo: 10_000,  // Chunks aligned to 10k boundaries
 *     onLogsResponse: ({ logs, fromBlock, toBlock }) => {
 *       console.log(`Fetched ${logs.length} logs from ${fromBlock}-${toBlock}`)
 *     }
 *   }),
 *   cacheConfig
 * )
 */
export function logsDivider(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<PublicRpcSchema>>,
  { maxBlockRange, maxConcurrentChunks = 5, alignTo }: LogsDividerConfig,
  // biome-ignore lint/suspicious/noExplicitAny: this `any` matches the underlying viem type's default
): Transport<"custom", Record<string, any>, EIP1193RequestFn<LogsDividerRpcSchema>> {
  if (maxBlockRange < 1) {
    throw new Error(`maxBlockRange must be at least 1, got ${maxBlockRange}`);
  }

  return (params) => {
    const baseTransport = baseTransportFn(params);

    const request = (args: EIP1193Parameters<LogsDividerRpcSchema>, options?: EIP1193RequestOptions) => {
      if (args.method !== "eth_getLogs") {
        return baseTransport.request(args, options);
      }

      // TODO: (@haydenshively future-work) `handleGetLogs` could respect `options`
      return handleGetLogs(baseTransport.request, args.params, {
        maxBlockRange,
        maxConcurrentChunks,
        alignTo,
      });
    };

    return custom({ request })(params);
  };
}
