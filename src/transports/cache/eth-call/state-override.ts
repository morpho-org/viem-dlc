import type { AbiFunction, Address, RpcStateOverride } from "viem";
import { fromHex, getAddress, keccak256, toHex } from "viem";

import { omit } from "../../../utils/omit.js";

import { resolveArrayFunction } from "./array-codec.js";

export const ETH_CALL_CACHE_POLICY_ADDRESS: Address = getAddress(
  `0x${keccak256(toHex("viem-dlc-cache-policy")).slice(26)}`,
);

const ETH_CALL_CACHE_POLICY_ADDRESS_LOWER = ETH_CALL_CACHE_POLICY_ADDRESS.toLowerCase() as Address;

export function extractEthCallCachePolicy(stateOverride: RpcStateOverride | undefined) {
  if (!stateOverride) return null;

  const entry = stateOverride[ETH_CALL_CACHE_POLICY_ADDRESS_LOWER] ?? stateOverride[ETH_CALL_CACHE_POLICY_ADDRESS];
  if (!entry?.code) return null;

  const rest: typeof stateOverride = omit(stateOverride, [
    ETH_CALL_CACHE_POLICY_ADDRESS_LOWER,
    ETH_CALL_CACHE_POLICY_ADDRESS,
  ]);

  const policy = JSON.parse(fromHex(entry.code, "string")) as {
    blobKey: string;
    ttl: number;
    batchSize?: number;
    abi: AbiFunction;
  };

  return {
    policy,
    resolved: resolveArrayFunction(policy.abi),
    cleanStateOverride: Object.keys(rest).length > 0 ? rest : undefined,
  };
}
