import type { Account, CallParameters, Chain, Client, Hex, Transport } from "viem";
import { call } from "viem/actions";

import { findDeploylessPartialResult } from "../utils/deployless/errors.js";

export type Call2ReturnType = {
  /**
   * ABI-encoded `U[]` holding every element that was served, in input order, with the
   * {@link Call2ReturnType.missing} indices omitted. `undefined` exactly when viem's `call`
   * would return it — an empty response, which a paged read does not produce.
   */
  data: Hex | undefined;
  /**
   * Ascending indices into the input array that could not be served — the lens declined them,
   * or a single-element retry ran the frame out of gas. Empty on full success, so callers only
   * ever handle one shape.
   */
  missing: number[];
};

/**
 * Settled counterpart to viem's `call` for paged deployless reads: where `call` throws if any
 * element is unservable, this returns the elements that were served alongside the indices that
 * were not.
 *
 * Requires a client whose transport is `deployless()` or `cache()`, and a request marked with
 * `policy({ paged: true, ... })`; against anything else it degenerates to `call` with an empty
 * `missing`. Transport, protocol, and ordinary lens-revert errors still throw — only
 * unservable *elements* are settled.
 *
 * @example
 * const { data, missing } = await call2(client, {
 *   factory, factoryData, to,
 *   data: encodeFunctionData({ abi: [pageAbi], functionName: 'page', args: [inputs] }),
 *   stateOverride: [policy({ abi: pageAbi, paged: true })],
 * })
 * const [served] = decodeAbiParameters([{ type: 'uint256[]' }], data)
 */
export async function call2<chain extends Chain | undefined>(
  client: Client<Transport, chain, Account | undefined>,
  parameters: CallParameters<chain>,
): Promise<Call2ReturnType> {
  try {
    const { data } = await call(client, parameters);
    return { data, missing: [] };
  } catch (error) {
    const partial = findDeploylessPartialResult(error);
    if (!partial) throw error;
    return { data: partial.data, missing: [...partial.missing] };
  }
}
