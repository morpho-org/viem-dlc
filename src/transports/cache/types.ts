import type { EIP1193RequestFn } from "viem";

import type { FacetId } from "../../observability.js";
import type { Store } from "../../types.js";
import type { createCoalescingMutex } from "../../utils/coalescing-mutex.js";
import type { Provider } from "../../utils/deployless/call.js";
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
   * The provider's `eth_call` gas cap. It bounds the opening chunks, and with `policy().batch.gas`
   * sizes them element by element; every later chunk is sized from what the pages report, so a
   * value too low costs a round trip, never a result. `gas_limit_observed` on the wide event is
   * what the node granted.
   *
   * Most nodes run an `eth_call` that leaves `gas` unspecified in the whole cap, and nothing is
   * sent. Monad's run it in a fixed default instead (see `chains`), so there the value is sent as
   * every chunk's `gas`, and one above the provider's cap fails the request with
   * `gas limit too high`: state the cap the provider documents.
   */
  gasLimit?: number;
}

export type HandlerContext = Omit<CacheConfig, "gasLimit"> & {
  chainId: number;
  /** The node this transport instance talks to, with {@link CacheConfig.gasLimit} resolved. */
  provider: Provider;
  requestFn: EIP1193RequestFn<LogsDividerSchema>;
  coalesce: ReturnType<typeof createCoalescingMutex>["coalesce"];
  /** Owning transport's facet identity; see {@link FacetId}. */
  facetId: FacetId;
};
