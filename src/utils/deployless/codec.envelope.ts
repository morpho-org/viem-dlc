import type { Address, Hex } from "viem";
import { decodeAbiParameters, deploylessCallViaFactoryBytecode, encodeAbiParameters, parseAbiParameters } from "viem";

/** Viem's factory wrapper bytecode, lowercased to match normalized request hex. */
const FACTORY_BYTECODE = deploylessCallViaFactoryBytecode.toLowerCase() as Hex;

const DEPLOYLESS_CONSTRUCTOR_PARAMS = parseAbiParameters("address, bytes, address, bytes");

/** Addresses & data describing the deployless target — invariant across a batch. */
export type DeploylessTarget = {
  address: Address;
  factory: Address;
  factoryData: Hex;
};

/** A deployless factory call: its {@link DeploylessTarget} plus the per-call `targetData` bytes. */
export type DeploylessFactoryCall = {
  target: DeploylessTarget;
  targetData: Hex;
};

/**
 * Reverses {@link wrapDeploylessFactoryCall}. Given the `data` field from a viem-produced
 * deployless factory `eth_call`, returns the target (address/factory/factoryData) and the
 * target calldata.
 */
export function unwrapDeploylessFactoryCall(data: Hex): DeploylessFactoryCall {
  if (!data.startsWith(FACTORY_BYTECODE)) {
    throw new Error("eth_call data is not a deployless factory wrapper");
  }
  const argsHex = `0x${data.slice(FACTORY_BYTECODE.length)}` as Hex;
  const [address, targetData, factory, factoryData] = decodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, argsHex);
  return { target: { address, factory, factoryData }, targetData };
}

/**
 * Mirrors viem's internal `toDeploylessCallViaFactoryData`. Rebuilds a deployless factory
 * `eth_call` payload from its constituent parts.
 */
export function wrapDeploylessFactoryCall({ target, targetData }: DeploylessFactoryCall): Hex {
  const args = encodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, [
    target.address,
    targetData,
    target.factory,
    target.factoryData,
  ]);
  return `${FACTORY_BYTECODE}${args.slice(2)}` as Hex;
}
