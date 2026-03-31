import type { StateOverride } from "viem";
import { toHex } from "viem";

import { ETH_CALL_CACHE_POLICY_ADDRESS } from "../transports/cache/eth-call/state-override.js";

/**
 * Returns a StateOverride entry encoding the cache policy. Pass in `stateOverride` array.
 *
 * Caches `eth_call` results per sub-call, with Multicall3 `aggregate3` detection.
 *
 * Assumptions:
 * - Cached callees are pure view functions whose return value depends only on
 *   `to`, `data`, `block`, and state/block overrides. Fields like `from`, `value`,
 *   and `gas` are excluded from cache identity because they have different semantics
 *   in direct vs. multicall contexts (`msg.sender` is the Multicall3 contract for
 *   sub-calls, not the original `from`).
 * - Re-fetching only missed sub-calls changes the aggregate3 execution context
 *   (e.g. remaining gas budget), so gas-sensitive sub-call behavior may diverge
 *   from a full-batch call. This is acceptable for typical view-function workloads.
 *
 * @param blobKey Specifies which entry of the `Store` holds these calls. Blob is extended by new results, not replaced.
 * @param ttl Maximum age (ms) of a cached entry before it is considered stale and re-fetched.
 */
export function cachePolicy(blobKey: string, ttl: number): StateOverride[number] {
  return {
    address: ETH_CALL_CACHE_POLICY_ADDRESS,
    code: toHex(JSON.stringify({ blobKey, ttl })),
  };
}
