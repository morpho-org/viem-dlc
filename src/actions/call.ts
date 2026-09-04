import type { StateOverride } from "viem";
import { toHex } from "viem";

import { ETH_CALL_POLICY_ADDRESS, type EthCallPolicy } from "../transports/state-overrides.js";

/**
 * EIP-3860 initcode cap. Deployless calldata rides inside initcode, so this is the natural
 * `batch.batchSize`.
 */
export const MAX_INITCODE_SIZE = 49_152;

/**
 * Returns a StateOverride entry encoding the `eth_call` policy. Pass it in the `stateOverride`
 * array; `readLens` does so for you.
 *
 * Marks a deployless `eth_call` (`call({ factory, factoryData, to, data, ... })`) over a paginated
 * lens for the `deployless` or `cache` transport: elements are re-packed into chunks, the envelope
 * calls the per-item function once per element and pages, and the pages aggregate back into the
 * shape `abi` declares. With `cache`, element bytes are keyed by
 * `(targetTo, factory, factoryData, selector, element)` and cached individually.
 *
 * Requirements:
 * - Elementwise: each value, and each decline (a per-item revert), depends only on its own element
 *   plus shared chain state, never on other elements, their order or multiplicity, or gas.
 * - The request must not depend on tx envelope fields outside `data` (`from`, `gas`,
 *   `value`, etc.). Those fields are intentionally excluded from cache identity.
 *
 * @param opts.abi The array-shaped fragment from `arrayifiedAbi`, built from the contract's real
 *   ABI: the per-item selector the envelope calls is derived from it.
 * @param opts.batch Optional batching config. Omit to send all elements in one upstream
 *   `eth_call`.
 * @param opts.batch.batchSize Maximum bytes of the `eth_call` `data` field per chunk; elements
 *   are greedy-packed under it and fetched in parallel. {@link MAX_INITCODE_SIZE} is the usual value.
 * @param opts.batch.compress FastLZ-compress calldata on the wire so more elements fit per chunk,
 *   at the cost of encoding time and decompression gas.
 * @param opts.batch.pageSizeHint Elements per chunk in the opening wave; later waves size themselves
 *   from what the pages report. Too high costs one continuation wave, too low costs extra parallel
 *   requests. Read `page_size_suggested` off the wide event to set it.
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
