import type { AbiFunction, StateOverride } from "viem";
import { toHex } from "viem";

import { ETH_CALL_CACHE_POLICY_ADDRESS } from "../transports/cache/eth-call/state-override.js";

/**
 * Returns a StateOverride entry encoding the cache policy. Pass in `stateOverride` array.
 *
 * Caches per-element results of a single-input, single-output array function invoked via
 * viem's deployless-factory pattern (`call({ factory, factoryData, data: ... })`).
 *
 * The handler decodes the outer dynamic array structurally (without instantiating element
 * values), hashes each raw element against `(targetTo, factory, factoryData, selector, element)`
 * to derive a cache key, fetches misses by re-packing a subset of elements into a new
 * deployless-factory call, and merges fresh results with cached ones. Element bytes round-
 * trip through the cache untouched.
 *
 * Restrictions on `abi`:
 * - Must be a `function` fragment with exactly one input and one output.
 * - Both the input and output types must be dynamic arrays (`T[]`).
 * - Element types may be static (uint/int/bool/address/bytesN, tuples, fixed-size arrays)
 *   or dynamic (string, bytes, nested arrays, dynamic tuples).
 *
 * @param blobKey Identifies the cache blob. The user chooses this; it should encode any
 *   state context (block, target contract identity, overrides) that would invalidate
 *   results across requests. Two requests with the same `blobKey` share a blob.
 * @param ttl Maximum age (ms) of a cached entry before it is considered stale and re-fetched.
 * @param opts.delta XFetch early-refresh scale (ms). On each freshness check, the handler
 *   samples `u ~ Uniform(0, 1]` and treats the entry as stale once
 *   `age - delta * ln(u) >= ttl`. Since `ln(u) <= 0`, the effective expiry is always `<= ttl`
 *   but may fire up to several `delta` earlier, with probability rising as `age` approaches
 *   `ttl`. Desynchronizes refreshes across many keys populated together, avoiding stampedes.
 *   Based on the XFetch algorithm from Vattani et al., "Optimal Probabilistic Cache Stampede
 *   Prevention" (2015), assuming constant recompute cost. Defaults to 0 (disabled).
 * @param opts.batchSize Maximum bytes of the `eth_call` `data` field when fetching misses.
 *   Misses are greedy-packed into chunks under this limit and fetched in parallel.
 *   Defaults to no splitting.
 * @param opts.abi The function fragment describing the cached callee.
 */
export function cachePolicy(
  blobKey: string,
  ttl: number,
  opts: { delta?: number; batchSize?: number; abi: AbiFunction },
): StateOverride[number] {
  const delta = opts.delta ?? 0;
  if (delta < 0) {
    throw new Error(`[cache] cachePolicy: delta (${delta}) must be >= 0`);
  }
  return {
    address: ETH_CALL_CACHE_POLICY_ADDRESS,
    code: toHex(JSON.stringify({ blobKey, ttl, delta, batchSize: opts.batchSize, abi: opts.abi })),
  };
}
