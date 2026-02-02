import { custom, type EIP1193Parameters, type PublicRpcSchema, type Transport } from "viem";

import type { EIP1193PublicRequestFn, EIP1193RequestOptions } from "../../types.js";

import { handleGetLogs } from "./handlers.js";
import type { LogsDividerConfig } from "./types.js";

export type {
  LogsDividerConfig,
  LogsResponse,
  OnLogsResponse,
} from "./types.js";

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
  // biome-ignore lint/suspicious/noExplicitAny: this `any` matches the underlying viem type's default
  baseTransportFn: Transport<string, Record<string, any>, EIP1193PublicRequestFn>,
  { maxBlockRange, maxConcurrentChunks = 5, alignTo, onLogsResponse = () => {} }: LogsDividerConfig,
): Transport {
  if (maxBlockRange < 1) {
    throw new Error(`maxBlockRange must be at least 1, got ${maxBlockRange}`);
  }

  return (params) => {
    const baseTransport = baseTransportFn(params);

    const request = (args: EIP1193Parameters<PublicRpcSchema>, options?: EIP1193RequestOptions) => {
      if (args.method !== "eth_getLogs") {
        return baseTransport.request(args, options);
      }

      // TODO: (@haydenshively future-work) `handleGetLogs` could respect `options`
      return handleGetLogs(baseTransport.request, args.params[0], {
        maxBlockRange,
        maxConcurrentChunks,
        alignTo,
        onLogsResponse,
      });
    };

    return custom({ request })(params);
  };
}
