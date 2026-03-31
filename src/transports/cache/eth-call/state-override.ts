import type { Address, RpcStateOverride } from "viem";
import { fromHex, getAddress, keccak256, toHex } from "viem";

import { omit } from "../../../utils/omit.js";

export const ETH_CALL_CACHE_POLICY_ADDRESS: Address = getAddress(
  `0x${keccak256(toHex("viem-dlc-cache-policy")).slice(26)}`,
);

const ETH_CALL_CACHE_POLICY_ADDRESS_LOWER = ETH_CALL_CACHE_POLICY_ADDRESS.toLowerCase() as Address;

export function extractEthCallCachePolicy(stateOverride: RpcStateOverride | undefined): {
  policy: { blobKey: string; ttl: number };
  /** The stateOverride with the sentinel entry removed. Undefined if it was the only entry. */
  cleanStateOverride: RpcStateOverride | undefined;
} | null {
  if (!stateOverride) return null;

  const entry = stateOverride[ETH_CALL_CACHE_POLICY_ADDRESS_LOWER] ?? stateOverride[ETH_CALL_CACHE_POLICY_ADDRESS];
  if (!entry?.code) return null;

  const rest: typeof stateOverride = omit(stateOverride, [
    ETH_CALL_CACHE_POLICY_ADDRESS_LOWER,
    ETH_CALL_CACHE_POLICY_ADDRESS,
  ]);

  return {
    policy: JSON.parse(fromHex(entry.code, "string")),
    cleanStateOverride: Object.keys(rest).length > 0 ? rest : undefined,
  };
}
