import type { RpcLog } from "viem";

import type { BlockRange, Store } from "../../types.js";
import type { LogsDividerConfig } from "../logs-divider/types.js";
import type { RateLimiterConfig } from "../rate-limiter/index.js";

export interface CachedChunk {
  logs: RpcLog[];
  /** Unix timestamp (ms) when this chunk was fetched */
  fetchedAt: number;
  /** Block number of the chain tip when this chunk was fetched */
  fetchedAtBlock: bigint;
  /** The aligned range this cache entry represents */
  alignedRange: BlockRange;
  /** The actual range that was fetched (may be smaller if fetch.to was capped) */
  fetchedRange: BlockRange;
}

export interface AlignedChunk {
  /** The portion of the chunk that was actually requested */
  requested: BlockRange;
  /** The aligned chunk boundaries (for cache keys and invalidation) */
  aligned: BlockRange;
  /** The boundaries to actually fetch (aligned.to may be capped at currentBlock) */
  fetch: BlockRange;
}

export interface InvalidationContext {
  /** How many blocks ago this chunk's aligned.to was from the chain tip when fetched */
  confirmations: number;
  /** Milliseconds since this chunk was last fetched */
  cacheAgeMs: number;
  /** Total number of chunks in the current request */
  totalChunks: number;
}

/** Returns probability [0,1] that a cached chunk should be refetched */
export type InvalidationStrategy = (context: InvalidationContext) => number;

export type LogsCacheConfig = {
  store: Store;
  /**
   * Cache alignment boundary. Chunks are aligned to multiples of this value.
   * Smaller values allow finer-grained invalidation.
   */
  binSize: number;
  /** Returns the probability [0,1] that a cached chunk should be refetched. */
  invalidationStrategy: InvalidationStrategy;

  logsDividerConfig: Omit<LogsDividerConfig, "alignTo" | "onLogsResponse">;
  rateLimiterConfig: RateLimiterConfig;
};
