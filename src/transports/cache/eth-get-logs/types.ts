import type { RpcLog } from "viem";

import type { BlockRange } from "../../../types.js";

export type CachedMetadata = {
  __type: "metadata";
  /** Unix timestamp (ms) when this chunk was fetched */
  fetchedAt: number;
  /** Block number of the chain tip when this chunk was fetched */
  fetchedAtBlock: bigint;
  /** The aligned range this cache entry represents */
  alignedRange: BlockRange;
  /** The actual range that was fetched (may be smaller if fetch.to was capped) */
  fetchedRange: BlockRange;
};

export type CachedLogs = RpcLog[] & { __type?: undefined };

export type CachedChunk = CachedMetadata | CachedLogs;
