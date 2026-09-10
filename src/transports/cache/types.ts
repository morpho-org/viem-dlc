import type { EIP1193RequestFn } from "viem";

import type { ChainDefinition } from "../../chains/index.js";
import type { FacetId } from "../../observability.js";
import type { Store } from "../../types.js";
import type { createCoalescingMutex } from "../../utils/coalescing-mutex.js";
import type { DeliveryMemo } from "../../utils/deployless/call.js";
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
   * The provider's `eth_call` gas cap. It bounds the opening wave's bytes, and with
   * `policy().batch.gas` its elements; every later chunk is sized from what the pages report, so a
   * value too low costs a round trip, never a result. On a chain whose nodes give an unspecified
   * `gas` a fixed default below the cap (see `chains`), it is also sent as every chunk's `gas`, and
   * there a value above the cap is rejected by the node (Monad: `gas limit too high`) and fails the
   * request — state the cap the provider documents. Elsewhere nothing is sent and
   * `gas_limit_observed` on the wide event is the cap the provider granted.
   */
  gasLimit?: number;
}

export type HandlerContext = CacheConfig & {
  chainId: number;
  chain: ChainDefinition;
  delivery: DeliveryMemo;
  requestFn: EIP1193RequestFn<LogsDividerSchema>;
  coalesce: ReturnType<typeof createCoalescingMutex>["coalesce"];
  /** Owning transport's facet identity; see {@link FacetId}. */
  facetId: FacetId;
};
