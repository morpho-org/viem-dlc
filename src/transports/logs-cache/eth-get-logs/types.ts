import type { RpcLog } from "viem";

import type { BlockRange } from "../../../types.js";

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
