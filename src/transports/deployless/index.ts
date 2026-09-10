import { createTransport, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import { chainDefinition } from "../../chains/index.js";
import { createFacetId, type FacetId, getObservability, observe } from "../../observability.js";
import type { EIP1193Parameters, SafelyExtendedRpcSchema } from "../../types.js";
import { factorisedFactoryCall, type Provider, provider } from "../../utils/deployless/call.js";
import { aggregatedPage, parseMarkedEthCall } from "../state-overrides.js";

type Base = SafelyExtendedRpcSchema<PublicRpcSchema>;

export const deploylessTransportKey = "viem-dlc-deployless" as const;

export type DeploylessConfig = {
  /**
   * The provider's `eth_call` gas cap. It bounds the opening wave's bytes, and with
   * `policy().batch.gas` its elements; every later chunk is sized from what the pages report, so a
   * value too low costs a round trip, never a result. On a chain whose nodes give an unspecified
   * `gas` a fixed default below the cap (see `chains`), it is also sent as every chunk's `gas`, and
   * there a value above the cap is rejected by the node (Monad: `gas limit too high`) and fails the
   * request — state the cap the provider documents. Elsewhere nothing is sent and
   * `gas_limit_observed` on the wide event is the cap the provider granted.
   */
  gasLimit?: number;
};

/**
 * Creates a thin transport wrapper that chunks marked deployless `eth_call`s under the wire byte
 * budget and aggregates the pages. The lens adapts to whatever frame each node grants;
 * {@link DeploylessConfig.gasLimit} lets the opening wave anticipate it, and is sent to nodes that
 * would otherwise run the call in a smaller default frame.
 *
 * Requests are only intercepted when they carry the `policy(...)` sentinel in `stateOverride`.
 * All other requests are forwarded unchanged.
 */
export function deployless<T extends Base>(
  baseTransportFn: Transport<string, unknown, EIP1193RequestFn<T>>,
  { gasLimit }: DeploylessConfig = {},
): Transport<typeof deploylessTransportKey, DeploylessConfig, EIP1193RequestFn<T>> {
  const facetId = createFacetId(deploylessTransportKey);

  return (params) => {
    const requestFn = baseTransportFn(params).request;
    const node = provider(chainDefinition(params.chain?.id), gasLimit);

    const request = (args: EIP1193Parameters<T>) => {
      if (args.method !== "eth_call") {
        return requestFn(args);
      }

      return handleEthCall(requestFn, args as EIP1193Parameters<PublicRpcSchema, "eth_call">, node, facetId);
    };

    return createTransport(
      {
        key: deploylessTransportKey,
        name: "[viem-dlc] deployless",
        request: observe(request, facetId, params.chain?.id) as EIP1193RequestFn,
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
  node: Provider,
  facetId: FacetId,
) {
  const marked = parseMarkedEthCall(req);
  if (!marked) {
    return requestFn(req);
  }
  const { policy, target, lens, elements, rest } = marked;

  const facet = getObservability()?.facet(facetId).sub("eth_call");
  facet?.set({ input_elements: elements.length });

  if (elements.length === 0) {
    return aggregatedPage(lens, [], []);
  }

  const { outputs, missing } = await factorisedFactoryCall(requestFn, {
    target,
    elements,
    solidity: lens,
    batch: policy.batch,
    provider: node,
    restOfEthCallParams: rest,
    facet,
  });
  return aggregatedPage(lens, outputs, missing);
}
