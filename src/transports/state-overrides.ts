import type { AbiFunction, Address, RpcStateOverride } from "viem";
import { fromHex, getAddress, keccak256, toHex } from "viem";

import { omit } from "../utils/omit.js";

export const ETH_CALL_POLICY_ADDRESS: Address = getAddress(`0x${keccak256(toHex("viem-dlc-policy")).slice(26)}`);

const ETH_CALL_POLICY_ADDRESS_LOWER = ETH_CALL_POLICY_ADDRESS.toLowerCase() as Address;

export type EthCallPolicy = {
  /** The array-shaped fragment of a paginated lens, from `arrayifiedAbi`: `f(T[]) returns (U[] results, uint256[] skipped)`. */
  abi: AbiFunction;
  /**
   * Upper bound on one input element's ABI-encoded tail (length word plus padded data).
   * Required when `T` is dynamic.
   */
  maxItemBytes?: number;
  /** As {@link EthCallPolicy.maxItemBytes}, for one result element `U`. */
  maxResultBytes?: number;
  batch?: {
    batchSize?: number;
    compress?: boolean;
  };
  cache?: {
    blobKey: string;
    ttl: number;
    delta?: number;
  };
};

export function extractEthCallPolicy(stateOverride: RpcStateOverride | undefined) {
  if (!stateOverride) return null;

  const entry = stateOverride[ETH_CALL_POLICY_ADDRESS_LOWER] ?? stateOverride[ETH_CALL_POLICY_ADDRESS];
  if (!entry?.code) return null;

  const rest: typeof stateOverride = omit(stateOverride, [ETH_CALL_POLICY_ADDRESS_LOWER, ETH_CALL_POLICY_ADDRESS]);

  return {
    policy: JSON.parse(fromHex(entry.code, "string")) as EthCallPolicy,
    stateOverride: Object.keys(rest).length > 0 ? rest : undefined,
  };
}
