import type { Hex } from "viem";

export type CachedEthCallEntry = {
  /** The return data from the sub-call */
  returnData: Hex;
  /** Whether the sub-call succeeded (relevant for multicall3 allowFailure) */
  success: boolean;
  /** Unix timestamp (ms) when this entry was fetched */
  fetchedAt: number;
};
