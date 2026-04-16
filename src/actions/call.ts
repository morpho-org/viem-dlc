import type { StateOverride } from "viem";
import { toHex } from "viem";

import { ETH_CALL_POLICY_ADDRESS, type EthCallPolicy } from "../transports/state-overrides.js";

/**
 * Returns a StateOverride entry encoding the `eth_call` policy. Pass it in the
 * `stateOverride` array.
 *
 * Caches per-element results of a single-input, single-output array function invoked via
 * viem's deployless-factory pattern (`call({ factory, factoryData, to, data, ... })`).
 *
 * The handler decodes the outer dynamic array structurally (without instantiating element
 * values), hashes each raw element against `(targetTo, factory, factoryData, selector, element)`
 * to derive a cache key, fetches misses by re-packing a subset of elements into a new
 * deployless-factory call, and merges fresh results with cached ones. Element bytes round-
 * trip through the cache untouched.
 *
 * Requirements:
 * - The callee must be elementwise: for an input array `[x0, ..., xn]`, it must return
 *   an output array `[y0, ..., yn]` with the same length and order, where each `yi`
 *   depends only on `xi` plus shared chain state (`block`, state overrides, etc.), not
 *   on other input elements, their multiplicity, or their order.
 * - The request must not depend on tx envelope fields outside `data` (`from`, `gas`,
 *   `value`, etc.). Those fields are intentionally excluded from cache identity.
 *
 * ABI restrictions:
 * - Must be a `function` fragment with exactly one input and one output.
 * - Both the input and output types must be dynamic arrays (`T[]`).
 * - Element types may be static (uint/int/bool/address/bytesN, tuples, fixed-size arrays)
 *   or dynamic (string, bytes, nested arrays, dynamic tuples).
 *
 * @param opts.batchSize Maximum bytes of the `eth_call` `data` field when fetching misses.
 *   Misses are greedy-packed into chunks under this limit and fetched in parallel.
 *   Defaults to no splitting.
 * @param opts.cache Optional cache config. If omitted, caching is disabled but `batchSize`
 *   is still honored.
 * @param opts.cache.blobKey Identifies the backing cache blob. Requests with the same
 *   `blobKey` share storage; different `blobKey`s are isolated into different blobs.
 * @param opts.cache.ttl Maximum age (ms) of a cached entry before it is considered stale
 *   and re-fetched.
 * @param opts.cache.delta XFetch early-refresh scale (ms). On each freshness check, the
 *   handler samples `u ~ Uniform(0, 1]` and treats the entry as stale once
 *   `age - delta * ln(u) >= ttl`. Since `ln(u) <= 0`, the effective expiry is always `<= ttl`
 *   but may fire up to several `delta` earlier, with probability rising as `age` approaches
 *   `ttl`. Desynchronizes refreshes across many keys populated together, avoiding stampedes.
 *   Based on the XFetch algorithm from Vattani et al., "Optimal Probabilistic Cache Stampede
 *   Prevention" (2015), assuming constant recompute cost. Defaults to 0 (disabled).
 * @param opts.abi The function fragment describing the cached callee.
 */
export function policy(opts: EthCallPolicy): StateOverride[number] {
  return {
    address: ETH_CALL_POLICY_ADDRESS,
    code: toHex(JSON.stringify(opts)),
  };
}
