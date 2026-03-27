import type { Store } from "../../types.js";

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

export interface LogsCacheConfig {
  store: Store;
  /**
   * Cache alignment boundary. Chunks are aligned to multiples of this value.
   * Smaller values allow finer-grained invalidation.
   */
  binSize: number;
  /** Returns the probability [0,1] that a cached chunk should be refetched. */
  invalidationStrategy: InvalidationStrategy;
}
