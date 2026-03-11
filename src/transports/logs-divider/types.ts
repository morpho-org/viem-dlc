import type { RpcLog } from "viem";

/** Data provided to the response callback */
export interface LogsResponse {
  /** Logs returned by the sub-request */
  logs: RpcLog[];
  /** filter.fromBlock for the sub-request */
  fromBlock: bigint;
  /** filter.toBlock for the sub-request */
  toBlock: bigint;
  /** Latest block (chain tip) when main request started */
  fetchedAtBlock: bigint;
  /** Unix timestamp (ms) when logs response was received */
  fetchedAt: number;
}

/** Callback to be invoked for each successful sub-request (including retried chunks) */
export type OnLogsResponse = (response: LogsResponse) => void;

export interface LogsDividerConfig {
  /** Maximum blocks per single RPC request. Use `Infinity` for unconstrained RPCs. */
  maxBlockRange: number;
  /** Max concurrent `eth_getLogs` requests. @default 5 */
  maxConcurrentChunks?: number;
  /**
   * The maximum valid log size. Logs larger than this are considered spam and SILENTLY IGNORED
   * in both the `onLogsResponse` callback and the final response.
   */
  maxLogBytes?: number;
  /**
   * Optional alignment boundary for chunk boundaries. When set, chunks are aligned to
   * multiples of this value, which may extend beyond the original range. Extra logs
   * are filtered out before the final return. Useful for cache hit optimization.
   */
  alignTo?: number;
}
