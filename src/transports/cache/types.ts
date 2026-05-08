import type { EIP1193RequestFn } from "viem";

import type { Store } from "../../types.js";
import type { createCoalescingMutex } from "../../utils/coalescing-mutex.js";
import type { LogsDividerSchema } from "../logs-divider/schema.js";

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

export interface CacheConfig {
  store: Store;
  /**
   * Cache alignment boundary. Chunks are aligned to multiples of this value.
   * Smaller values allow finer-grained invalidation.
   */
  binSize: number;
  /** Returns the probability [0,1] that a cached chunk should be refetched. */
  invalidationStrategy: InvalidationStrategy;
  /**
   * RPC `eth_call` gas cap. Combined with `policy().batch.estimatedGasPerItem` to chunk
   * deployless calls under the cap; also exposed via the transport's `value`.
   */
  gasLimit: number;
}

export type HandlerContext = CacheConfig & {
  chainId: number;
  requestFn: EIP1193RequestFn<LogsDividerSchema>;
  coalesce: ReturnType<typeof createCoalescingMutex>["coalesce"];
};
