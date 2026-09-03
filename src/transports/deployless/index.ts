import { createTransport, type EIP1193RequestFn, type Hex, type PublicRpcSchema, type Transport } from "viem";

import { createFacetId, type FacetId, getObservability, observe } from "../../observability.js";
import type { EIP1193Parameters, SafelyExtendedRpcSchema } from "../../types.js";
import { factorisedFactoryCall } from "../../utils/deployless/call.js";
import { unwrapDeploylessFactoryCall } from "../../utils/deployless/codec.envelope.js";
import { calldataToArray, pageToHex, resolveArrayFunction } from "../../utils/deployless/codec.inner.js";
import { extractEthCallPolicy } from "../state-overrides.js";

type Base = SafelyExtendedRpcSchema<PublicRpcSchema>;

export const deploylessTransportKey = "viem-dlc-deployless" as const;

/**
 * Creates a thin transport wrapper that chunks marked deployless `eth_call`s under the wire and
 * allocation byte budgets and aggregates the pages. No gas cap is configured: the lens adapts to
 * whatever frame each node grants.
 *
 * Requests are only intercepted when they carry the `policy(...)` sentinel in `stateOverride`.
 * All other requests are forwarded unchanged.
 */
export function deployless<T extends Base>(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<T>>,
): Transport<typeof deploylessTransportKey, Record<string, never>, EIP1193RequestFn<T>> {
  const facetId = createFacetId(deploylessTransportKey);

  return (params) => {
    const requestFn = baseTransportFn(params).request;

    const request = (args: EIP1193Parameters<T>) => {
      if (args.method !== "eth_call") {
        return requestFn(args);
      }

      return handleEthCall(requestFn, args as EIP1193Parameters<PublicRpcSchema, "eth_call">, facetId);
    };

    return createTransport(
      {
        key: deploylessTransportKey,
        name: "[viem-dlc] deployless",
        request: observe(request, facetId) as EIP1193RequestFn,
        retryCount: 0,
        type: deploylessTransportKey,
      },
      {},
    );
  };
}

async function handleEthCall(
  requestFn: EIP1193RequestFn<Base>,
  req: EIP1193Parameters<PublicRpcSchema, "eth_call">,
  facetId: FacetId,
) {
  const extracted = extractEthCallPolicy(req.params[2]);
  if (!extracted) {
    return requestFn(req);
  }

  const [txn, ...restOfEthCallParams] = req.params;
  if (txn.data === undefined) {
    throw new Error("[deployless] eth_call with policy requires `data`");
  }
  {
    const txnKeys = Object.keys(txn).filter((k) => txn[k as keyof typeof txn] !== undefined);
    // `txn.data` must be the only field on `txn`
    if (txnKeys.length > 1) {
      const extras = txnKeys.filter((k) => k !== "data");
      throw new Error(
        `[deployless] eth_call with policy: tx object may only set \`data\` (found extras: ${extras.join(", ")})`,
      );
    }
  }
  // `stateOverride` must be overwritten with the cleaned/extracted version.
  // trailing undefined args must be removed for RPC compatibility.
  if (restOfEthCallParams.length >= 2) {
    restOfEthCallParams[1] = extracted.stateOverride ?? (restOfEthCallParams[2] ? {} : undefined);
    const lastDefinedParamIdx = restOfEthCallParams.reduce((acc, x, i) => (x === undefined ? acc : i), -1);
    restOfEthCallParams.splice(lastDefinedParamIdx + 1);
  }

  const { target, targetData } = unwrapDeploylessFactoryCall(txn.data);
  const solidity = resolveArrayFunction(extracted.policy.abi);
  const inputElements = calldataToArray(solidity, targetData);

  const facet = getObservability()?.facet(facetId).sub("eth_call");
  facet?.set({ input_elements: inputElements.length });

  if (inputElements.length === 0) {
    return pageToHex(solidity.outputLayout, { results: [], skipped: [] });
  }

  const { outputs, missing } = await factorisedFactoryCall(requestFn, {
    target,
    elements: inputElements,
    solidity,
    batch: extracted.policy.batch,
    restOfEthCallParams,
    facet,
  });
  // The chunked calls aggregate into a single page over the caller's whole input, so the response
  // keeps the `(U[] results, uint256[] skipped)` shape the ABI promises.
  return pageToHex(solidity.outputLayout, { results: definedOnly(outputs), skipped: missing });
}

/** Drops the holes an unservable element leaves, preserving input order. */
function definedOnly(outputs: readonly (Hex | undefined)[]): Hex[] {
  return outputs.filter((o) => o !== undefined);
}
