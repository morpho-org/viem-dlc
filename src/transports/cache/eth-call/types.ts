import type { Hex } from "viem";

export type CachedEthCallEntry = {
  /** The return data from the sub-call */
  output: Hex;
  /** Unix timestamp (ms) when this entry was fetched */
  fetchedAt: number;
};
