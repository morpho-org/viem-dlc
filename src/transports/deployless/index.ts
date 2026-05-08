import { createTransport, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import type { EIP1193Parameters, SafelyExtendedRpcSchema } from "../../types.js";
import { factorisedFactoryCall } from "../../utils/deployless/call.js";
import { unwrapDeploylessFactoryCall } from "../../utils/deployless/codec.envelope.js";
import { arrayToHex, calldataToArray, resolveArrayFunction } from "../../utils/deployless/codec.inner.js";
import { extractEthCallPolicy } from "../state-overrides.js";

type Base = SafelyExtendedRpcSchema<PublicRpcSchema>;

export const deploylessTransportKey = "viem-dlc-deployless" as const;

export interface DeploylessConfig {
  /**
   * RPC `eth_call` gas cap. Combined with `policy().batch.gas` to chunk deployless calls
   * under the cap; also exposed via the transport's `value`.
   */
  gasLimit: number;
}

/**
 * Creates a thin transport wrapper that chunks marked deployless `eth_call`s under both the
 * `batchSize` byte budget and the `gas`/`gasLimit` gas budget.
 *
 * Requests are only intercepted when they carry the `policy(...)` sentinel in `stateOverride`.
 * All other requests are forwarded unchanged.
 */
export function deployless<T extends Base>(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<T>>,
  { gasLimit }: DeploylessConfig,
): Transport<typeof deploylessTransportKey, { gasLimit: number }, EIP1193RequestFn<T>> {
  return (params) => {
    const requestFn = baseTransportFn(params).request;

    const request = (args: EIP1193Parameters<T>) => {
      if (args.method !== "eth_call") {
        return requestFn(args);
      }

      return handleEthCall(requestFn, args as EIP1193Parameters<PublicRpcSchema, "eth_call">, gasLimit);
    };

    return createTransport(
      {
        key: deploylessTransportKey,
        name: "[viem-dlc] deployless",
        request: request as EIP1193RequestFn,
        retryCount: 0,
        type: deploylessTransportKey,
      },
      { gasLimit },
    );
  };
}

async function handleEthCall(
  requestFn: EIP1193RequestFn<Base>,
  req: EIP1193Parameters<PublicRpcSchema, "eth_call">,
  gasLimit: number,
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

  if (inputElements.length === 0) {
    return arrayToHex(solidity.outputLayout, []);
  }

  const outputs = await factorisedFactoryCall(requestFn, {
    target,
    elements: inputElements,
    solidity,
    batch: extracted.policy.batch,
    gasLimit,
    restOfEthCallParams,
  });
  return arrayToHex(solidity.outputLayout, outputs);
}
