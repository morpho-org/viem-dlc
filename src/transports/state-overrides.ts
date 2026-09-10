import type { AbiFunction, Address, Hex, PublicRpcSchema, RpcStateOverride } from "viem";
import { fromHex, getAddress, keccak256, toHex } from "viem";

import type { EIP1193Parameters } from "../types.js";
import type { BatchOptions } from "../utils/deployless/call.js";
import {
  type DeploylessTarget,
  type RestOfEthCallParams,
  unwrapDeploylessFactoryCall,
} from "../utils/deployless/codec.envelope.js";
import {
  calldataToArray,
  pageToHex,
  type ResolvedArrayFunction,
  resolveArrayFunction,
} from "../utils/deployless/codec.inner.js";
import { omit } from "../utils/omit.js";

export const ETH_CALL_POLICY_ADDRESS: Address = getAddress(`0x${keccak256(toHex("viem-dlc-policy")).slice(26)}`);

const ETH_CALL_POLICY_ADDRESS_LOWER = ETH_CALL_POLICY_ADDRESS.toLowerCase() as Address;

export type EthCallPolicy = {
  /** The array-shaped fragment `arrayifiedAbi` derives: `f(T[]) returns (U[] results, uint256[] skipped)`. */
  abi: AbiFunction;
  batch?: BatchOptions;
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

/** A deployless `eth_call` over a paginated lens, as {@link parseMarkedEthCall} reads it off the request. */
export type MarkedEthCall = {
  policy: EthCallPolicy;
  target: DeploylessTarget;
  lens: ResolvedArrayFunction;
  /** The caller's input array, one raw element per entry. */
  elements: readonly Hex[];
  /** The block selector, the caller's state override less the sentinel, and the block overrides; nothing trailing undefined. */
  rest: RestOfEthCallParams;
};

/**
 * Recognises a request by the `policy(...)` sentinel in its state override: `null` for any other
 * request, which the transport forwards unchanged; throws when a marked request cannot be served.
 * The transaction may set nothing but `data`, since the response is aggregated from many requests
 * and cached without regard to the rest of it.
 */
export function parseMarkedEthCall(req: EIP1193Parameters<PublicRpcSchema, "eth_call">): MarkedEthCall | null {
  const [txn, block, stateOverride, ...blockOverrides] = req.params;
  const extracted = extractEthCallPolicy(stateOverride);
  if (!extracted) return null;
  if (txn.data === undefined) throw new Error("[viem-dlc] eth_call with policy requires `data`");
  const extras = Object.keys(txn).filter((k) => k !== "data" && txn[k as keyof typeof txn] !== undefined);
  if (extras.length > 0) {
    throw new Error(
      `[viem-dlc] eth_call with policy: tx object may only set \`data\` (found extras: ${extras.join(", ")})`,
    );
  }
  // Nodes reject a trailing `undefined` param, so the tail is trimmed rather than passed through.
  const rest: unknown[] = [block, extracted.stateOverride ?? (blockOverrides[0] ? {} : undefined), ...blockOverrides];
  while (rest.length > 0 && rest[rest.length - 1] === undefined) rest.pop();

  const { target, targetData } = unwrapDeploylessFactoryCall(txn.data);
  const lens = resolveArrayFunction(extracted.policy.abi);
  return {
    policy: extracted.policy,
    target,
    lens,
    elements: calldataToArray(lens, targetData),
    rest: rest as unknown as RestOfEthCallParams,
  };
}

/**
 * The caller-facing `(U[] results, uint256[] skipped)` page over the whole input: `outputs` is
 * sparse exactly at `skipped`, which indexes the caller's input array.
 */
export function aggregatedPage(
  lens: ResolvedArrayFunction,
  outputs: readonly (Hex | undefined)[],
  skipped: readonly number[],
): Hex {
  return pageToHex(lens.outputLayout, { results: outputs.filter((o) => o !== undefined), skipped });
}
