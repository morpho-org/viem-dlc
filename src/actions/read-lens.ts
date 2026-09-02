import type {
  Abi,
  AbiFunction,
  AbiParameter,
  Address,
  BlockTag,
  Client,
  ContractFunctionArgs,
  ContractFunctionName,
  ContractFunctionReturnType,
  Hex,
  StateOverride,
} from "viem";
import { decodeFunctionResult, encodeFunctionData } from "viem";
import { call } from "viem/actions";

import type { EthCallPolicy } from "../transports/state-overrides.js";
import { arrayifiedAbi } from "../utils/deployless/codec.inner.js";

import { policy } from "./call.js";

type ViewMutability = "pure" | "view";

/** Names of the `view`/`pure` functions in `abi` taking one parameter and returning one value. */
export type LensFunctionName<abi extends Abi> = ContractFunctionName<abi, ViewMutability> &
  (Abi extends abi
    ? string
    : Extract<
        abi[number],
        {
          type: "function";
          stateMutability: ViewMutability;
          inputs: readonly [AbiParameter];
          outputs: readonly [AbiParameter];
        }
      >["name"]);

type ItemInput<abi extends Abi, functionName extends LensFunctionName<abi>> =
  ContractFunctionArgs<abi, ViewMutability, functionName> extends readonly [infer input, ...unknown[]]
    ? input
    : unknown;

export type ReadLensParameters<abi extends Abi, functionName extends LensFunctionName<abi>> = Pick<
  EthCallPolicy,
  "maxItemBytes" | "maxResultBytes" | "batch" | "cache"
> & {
  abi: abi;
  /** The per-item function: one parameter in, one value out. */
  functionName: functionName;
  /** One entry per element; each becomes its own per-item call. */
  args: readonly ItemInput<abi, functionName>[];
  /** Deployless-factory descriptor, as `lens.with(...)` produces. */
  address: Address;
  factory: Address;
  factoryData: Hex;
  blockNumber?: bigint;
  blockTag?: BlockTag;
  /** Appended to the `policy` entry the action adds. */
  stateOverride?: StateOverride;
};

export type ReadLensReturnType<abi extends Abi, functionName extends LensFunctionName<abi>> = {
  /** In input order, dense: one per element not in `skipped`. */
  results: ContractFunctionReturnType<abi, ViewMutability, functionName>[];
  /** Ascending indices into `args` the lens declined, that were declined for size, or that gas could not resolve. */
  skipped: number[];
};

/**
 * Reads a paginated lens: calls the per-item function `functionName` once per element of `args`
 * through the `deployless` or `cache` transport, which packs the elements into chunks, pages,
 * retries what gas could not resolve, and aggregates. The array-shaped fragment the wire carries
 * ({@link arrayifiedAbi}) is derived here from the contract's real ABI, so it never has to be
 * written by hand and the per-item selector cannot drift from what the lens implements.
 *
 * A partial result is a successful response — check `skipped` if you need every element.
 */
export function readLens<const abi extends Abi, functionName extends LensFunctionName<abi>>(
  client: Client,
  parameters: ReadLensParameters<abi, functionName>,
): Promise<ReadLensReturnType<abi, functionName>>;
export async function readLens(
  client: Client,
  {
    abi,
    functionName,
    args,
    address,
    factory,
    factoryData,
    blockNumber,
    blockTag,
    stateOverride,
    ...policyOpts
  }: ReadLensParameters<Abi, string>,
): Promise<ReadLensReturnType<Abi, string>> {
  const candidates = abi.filter((f): f is AbiFunction => f.type === "function" && f.name === functionName);
  if (candidates.length !== 1) {
    throw new Error(
      `readLens: ${candidates.length === 0 ? "no" : "more than one"} function named ${functionName} in the lens abi`,
    );
  }
  const item = candidates[0]!;
  if (item.stateMutability !== "view" && item.stateMutability !== "pure") {
    throw new Error(`readLens: ${functionName} must be view or pure`);
  }
  const fragment = arrayifiedAbi(item);
  const paginated: readonly [typeof fragment] = [fragment];
  const block: { blockNumber: bigint } | { blockTag?: BlockTag } =
    blockNumber !== undefined ? { blockNumber } : { blockTag };

  const { data } = await call(client, {
    to: address,
    factory,
    factoryData,
    data: encodeFunctionData({ abi: paginated, functionName, args: [args] }),
    stateOverride: [...(stateOverride ?? []), policy({ abi: fragment, ...policyOpts })],
    ...block,
  });
  if (data === undefined) throw new Error("readLens: empty response");

  const page = decodeFunctionResult({ abi: paginated, functionName, data });
  if (!Array.isArray(page) || page.length !== 2 || !Array.isArray(page[0]) || !Array.isArray(page[1])) {
    throw new Error("readLens: response is not a (results, skipped) page");
  }
  return { results: [...page[0]], skipped: page[1].map(Number) };
}
