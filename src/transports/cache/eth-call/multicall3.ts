import type { Address, Hex } from "viem";
import {
  decodeFunctionData,
  decodeFunctionResult,
  encodeFunctionData,
  encodeFunctionResult,
  multicall3Abi,
} from "viem";

export const MULTICALL3_ADDRESS: Address = "0xca11bde05977b3631167028862be2a173976ca11";

const AGGREGATE3_SELECTOR = "0x82ad56cb";

export type Call3 = {
  target: Address;
  allowFailure: boolean;
  callData: Hex;
};

export type Result = {
  success: boolean;
  returnData: Hex;
};

/** Returns true if the call targets Multicall3's `aggregate3`. */
export function isMulticall3(to: Address | undefined, data: Hex | undefined): boolean {
  return (
    to?.toLowerCase() === MULTICALL3_ADDRESS &&
    data !== undefined &&
    data.slice(0, 10).toLowerCase() === AGGREGATE3_SELECTOR
  );
}

/** Decodes `aggregate3` calldata into its constituent sub-calls. */
export function decodeAggregate3(data: Hex): Call3[] {
  const { args } = decodeFunctionData({ abi: multicall3Abi, data });
  return args[0] as Call3[];
}

/** Encodes sub-calls into `aggregate3` calldata. */
export function encodeAggregate3(calls: readonly Call3[]): Hex {
  return encodeFunctionData({
    abi: multicall3Abi,
    functionName: "aggregate3",
    args: [calls],
  });
}

/** Decodes an `aggregate3` return value into per-call results. */
export function decodeAggregate3Result(data: Hex): Result[] {
  return decodeFunctionResult({
    abi: multicall3Abi,
    functionName: "aggregate3",
    data,
  }) as Result[];
}

/** Encodes per-call results into the `aggregate3` return format. */
export function encodeAggregate3Result(results: readonly Result[]): Hex {
  return encodeFunctionResult({
    abi: multicall3Abi,
    functionName: "aggregate3",
    result: results,
  });
}
