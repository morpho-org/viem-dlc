import type { AbiFunction, Address, RpcStateOverride } from "viem";
import { fromHex, getAddress, keccak256, toHex } from "viem";

import type { DeploylessExfilMode } from "../utils/deployless/codec.envelope.js";
import { omit } from "../utils/omit.js";

export const ETH_CALL_POLICY_ADDRESS: Address = getAddress(`0x${keccak256(toHex("viem-dlc-policy")).slice(26)}`);

const ETH_CALL_POLICY_ADDRESS_LOWER = ETH_CALL_POLICY_ADDRESS.toLowerCase() as Address;

export type EthCallPolicy = {
  abi: AbiFunction;
  batch?: { batchSize: number; exfil?: DeploylessExfilMode };
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
