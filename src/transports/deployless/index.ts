import { createTransport, type EIP1193RequestFn, type PublicRpcSchema, type Transport } from "viem";

import { createFacetId, type FacetId, getObservability, observe } from "../../observability.js";
import type { EIP1193Parameters, SafelyExtendedRpcSchema } from "../../types.js";
import { factorisedFactoryCall, type Provider, providerOf } from "../../utils/deployless/call.js";
import { type RestOfEthCallParams, unwrapDeploylessFactoryCall } from "../../utils/deployless/codec.envelope.js";
import { calldataToArray, pageToAbi, resolveArrayFunction } from "../../utils/deployless/codec.inner.js";
import { extractEthCallPolicy } from "../state-overrides.js";

type Base = SafelyExtendedRpcSchema<PublicRpcSchema>;

export const deploylessTransportKey = "viem-dlc-deployless" as const;

export type DeploylessConfig = {
  /**
   * The provider's `eth_call` gas cap. It bounds the opening chunks, and with `policy().batch.gas`
   * sizes them element by element; every later chunk is sized from what the pages report, so a
   * value too low costs a round trip, never a result. `gas_limit_observed` on the wide event is
   * what the node granted.
   *
   * Most nodes run an `eth_call` that leaves `gas` unspecified in the whole cap, and nothing is
   * sent. Monad's run it in a fixed default instead (see `chains`), so there the value is sent as
   * every chunk's `gas`, and one above the provider's cap fails the request with
   * `gas limit too high`: state the cap the provider documents.   *
   * Stating it requires the client's chain to carry `viemDlc` facts, since whether the value is sent
   * as each chunk's `gas` is a fact of the chain; a chain carrying none throws when the client is
   * built.
   */
  gasLimit?: number;
  /**
   * The largest request the provider accepts, in bytes of a chunk's `eth_call` `data`; elements are
   * greedy-packed under it and fetched in parallel. State what the provider documents, often a few
   * megabytes. An initcode-delivered chunk is also bound by the chain's initcode limit, which the
   * chain states and which is usually far the smaller of the two.
   */
  batchSize?: number;
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
  { gasLimit, batchSize }: DeploylessConfig = {},
): Transport<typeof deploylessTransportKey, DeploylessConfig, EIP1193RequestFn<T>> {
  const facetId = createFacetId(deploylessTransportKey);

  return (params) => {
    const requestFn = baseTransportFn(params).request;
    const provider = providerOf(params.chain, { gasLimit, batchSize });

    const request = (args: EIP1193Parameters<T>) => {
      if (args.method !== "eth_call") {
        return requestFn(args);
      }

      return handleEthCall(requestFn, args as EIP1193Parameters<PublicRpcSchema, "eth_call">, provider, facetId);
    };

    return createTransport(
      {
        key: deploylessTransportKey,
        name: "[viem-dlc] deployless",
        request: observe(request, facetId, params.chain?.id) as EIP1193RequestFn,
        retryCount: 0,
        type: deploylessTransportKey,
      },
      { gasLimit, batchSize },
    );
  };
}

async function handleEthCall(
  requestFn: EIP1193RequestFn<Base>,
  req: EIP1193Parameters<PublicRpcSchema, "eth_call">,
  provider: Provider,
  facetId: FacetId,
) {
  const [txn, block, stateOverride, ...blockOverrides] = req.params;
  const extracted = extractEthCallPolicy(stateOverride);
  if (!extracted) {
    return requestFn(req);
  }
  const { policy } = extracted;
  if (txn.data === undefined) throw new Error("[deployless] eth_call with policy requires `data`");
  const extras = Object.keys(txn).filter((k) => k !== "data" && txn[k as keyof typeof txn] !== undefined);
  if (extras.length > 0) {
    throw new Error(
      `[deployless] eth_call with policy: tx object may only set \`data\` (found extras: ${extras.join(", ")})`,
    );
  }
  // Nodes reject a trailing `undefined` param, so the tail is trimmed rather than passed through.
  const trimmed: unknown[] = [
    block,
    extracted.stateOverride ?? (blockOverrides[0] ? {} : undefined),
    ...blockOverrides,
  ];
  while (trimmed.length > 0 && trimmed[trimmed.length - 1] === undefined) trimmed.pop();
  const rest = trimmed as unknown as RestOfEthCallParams;

  const { target, targetData } = unwrapDeploylessFactoryCall(txn.data);
  const lens = resolveArrayFunction(policy.abi);
  const elements = calldataToArray(lens, targetData);

  const facet = getObservability()?.facet(facetId).sub("eth_call");
  facet?.set({ input_elements: elements.length });

  if (elements.length === 0) {
    return pageToAbi(lens.outputLayout, { results: [], skipped: [] });
  }

  const { outputs, missing } = await factorisedFactoryCall(requestFn, {
    target,
    elements,
    lens,
    batch: policy.batch,
    provider,
    restOfEthCallParams: rest,
    facet,
  });
  return pageToAbi(lens.outputLayout, { results: outputs.filter((o) => o !== undefined), skipped: missing });
}
