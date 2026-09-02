import type { StateOverride } from "viem";
import { toHex } from "viem";

import { ETH_CALL_POLICY_ADDRESS, type EthCallPolicy } from "../transports/state-overrides.js";

/**
 * EIP-3860 initcode cap. Deployless calldata rides inside initcode, so this is the natural
 * `batch.batchSize`.
 */
export const MAX_INITCODE_SIZE = 49_152;

/**
 * Returns a StateOverride entry encoding the `eth_call` policy. Pass it in the
 * `stateOverride` array.
 *
 * Marks a paginated lens read — calldata encoded against the array-shaped fragment
 * `f(T[]) returns (U[] results, uint256[] skipped)` derived by `paginatedAbi` from the lens's
 * per-item function `f(T) returns (U)` — invoked via viem's deployless-factory pattern
 * (`call({ factory, factoryData, to, data, ... })`) for special handling by the `deployless` or
 * `cache` transport: the input is re-packed into chunks under byte budgets, the envelope calls the
 * per-item function once per element and pages, elements gas could not resolve are retried alone,
 * and the pages aggregate into one over the caller's whole input so the response keeps the shape
 * `abi` declares. With `cache`, raw element bytes are additionally keyed by
 * `(targetTo, factory, factoryData, selector, element)` and cached per element. `readLens` attaches
 * this for you; the README's "Paginated lenses" section has the lens contract. No gas is
 * configured anywhere.
 *
 * Requirements:
 * - Elementwise: each served value, and each decline (a per-item revert), depends only on its own
 *   element plus shared chain state (`block`, state overrides, etc.), not on other elements, their
 *   multiplicity, their order, or the gas the frame had.
 * - The request must not depend on tx envelope fields outside `data` (`from`, `gas`,
 *   `value`, etc.). Those fields are intentionally excluded from cache identity.
 *
 * @param opts.abi The array-shaped fragment, from `paginatedAbi`: exactly one dynamic-array input
 *   `T[]` and exactly the outputs `(U[] results, uint256[] skipped)`. The per-item selector the
 *   envelope calls is derived from it, so build it from the contract's real ABI.
 * @param opts.maxItemBytes Required when `T` is dynamic: the most padded ABI tail bytes (length
 *   word plus padded data) one input element may occupy. Larger inputs are declined client-side,
 *   with no request made.
 * @param opts.maxResultBytes Required when `U` is dynamic: the same bound for one result element.
 *   A result exceeding it, fresh or cached, is a protocol error.
 * @param opts.batch Optional batching config. Omit to send all elements in a single upstream
 *   `eth_call`, still under the fixed allocation budget.
 * @param opts.batch.batchSize Maximum bytes of the `eth_call` `data` field per chunk. Input
 *   elements are greedy-packed under this limit and fetched in parallel; {@link MAX_INITCODE_SIZE}
 *   is the usual value.
 * @param opts.batch.compress Whether to FastLZ-compress calldata on the wire, so more elements
 *   fit under the initcode cap per chunk at the cost of encoding time and decompression gas. An
 *   over-packed chunk pages and costs one more round trip, never a bisection.
 * @param opts.cache Optional cache config. Honored by the `cache` transport only; if omitted,
 *   or when used with `deployless`, `batch` is still honored without caching.
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
 */
export function policy(opts: EthCallPolicy): StateOverride[number] {
  return {
    address: ETH_CALL_POLICY_ADDRESS,
    code: toHex(JSON.stringify(opts)),
  };
}
