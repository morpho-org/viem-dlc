import type { StateOverride } from "viem";
import { toHex } from "viem";

import { ETH_CALL_POLICY_ADDRESS, type EthCallPolicy } from "../transports/state-overrides.js";

/**
 * Returns a StateOverride entry encoding the `eth_call` policy. Pass it in the
 * `stateOverride` array.
 *
 * Marks a single-input, single-output array function invoked via viem's deployless-factory
 * pattern (`call({ factory, factoryData, to, data, ... })`) for special handling by the
 * `deployless` or `cache` transport.
 *
 * Both transports decode the outer dynamic array structurally (without instantiating element
 * values) and can re-pack subsets of elements into new deployless-factory calls to honor
 * `batchSize`. When used with `cache`, raw element bytes are additionally keyed by
 * `(targetTo, factory, factoryData, selector, element)` and cached per element.
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
 * @param opts.batch Optional batching config. Omit to send all elements in a single
 *   upstream `eth_call`. When set, chunks honor both `batchSize` and `gas`.
 * @param opts.batch.batchSize Maximum bytes of the `eth_call` `data` field per chunk.
 *   Input elements are greedy-packed under this limit and fetched in parallel. Omit to
 *   skip byte-budget enforcement (useful when the gas budget is the only relevant cap).
 * @param opts.batch.exfil Outer wrapper mode. Defaults to `'return'` (viem's stock RETURN-mode
 *   wrapper). Set to `'revert'` to exfiltrate via `REVERT` instead, which lifts the EIP-170
 *   24_576 bytes returndata cap at the cost of relying on the RPC preserving revert data.
 * @param opts.batch.compress Whether to use FastLZ (LZ77) compression on the wire (to RPC).
 *   EIP-3860 limits initcode to 49_152 bytes. For deployless reads, that constrains calldata,
 *   so compression can help squeeze more entities into the request at the cost of extra
 *   pre-request encoding time.
 * @param opts.batch.gas Polynomial gas-cost model `G(N) = constant + linear·N + quadratic·N²`
 *   for the lens. Combined with the transport's `gasLimit`, the chunker picks the largest
 *   per-chunk item count `N` such that `G(N) ≤ gasLimit` and enforces it alongside `batchSize`.
 *   Omit to skip gas-budget enforcement. No internal safety factor is applied — pad your
 *   estimate if you want headroom.
 *
 *   Each coefficient is a property of the lens, not of any specific RPC's gas cap:
 *   - `constant`: fixed per-call overhead (selector dispatch, decoder, encoder). Pass 0 if
 *     negligible.
 *   - `linear`: asymptotic per-item rate. The dominant term for typical lenses; for many
 *     lenses this is the only non-zero term.
 *   - `quadratic`: per-item² cost — typically memory expansion (`memWords² / 512`). Pass 0
 *     if memory expansion is negligible at the chunk sizes you'll see.
 *
 *   Calibrate by measuring `G(N)` at a few values of `N` and fitting; or measure `N_max` at
 *   one cap via binary search and back out coefficients with knowledge of the lens shape.
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
 * @param opts.abi The function fragment describing the marked callee.
 */
export function policy(opts: EthCallPolicy): StateOverride[number] {
  return {
    address: ETH_CALL_POLICY_ADDRESS,
    code: toHex(JSON.stringify(opts)),
  };
}
