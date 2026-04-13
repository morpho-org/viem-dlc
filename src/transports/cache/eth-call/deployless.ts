import type { Address, Hex } from "viem";
import { decodeAbiParameters, deploylessCallViaFactoryBytecode, encodeAbiParameters, parseAbiParameters } from "viem";

/** Viem's factory wrapper bytecode, lowercased to match normalized request hex. */
const FACTORY_BYTECODE = deploylessCallViaFactoryBytecode.toLowerCase() as Hex;

const DEPLOYLESS_CONSTRUCTOR_PARAMS = parseAbiParameters("address, bytes, address, bytes");

export type DeploylessFactoryParts = {
  targetTo: Address;
  targetData: Hex;
  factory: Address;
  factoryData: Hex;
};

/**
 * Reverses {@link wrapDeploylessFactoryCall}. Given the `data` field from a viem-produced
 * deployless factory `eth_call`, returns the original target `to`, target calldata, factory
 * address, and factory data.
 */
export function unwrapDeploylessFactoryCall(data: Hex): DeploylessFactoryParts {
  if (!data.startsWith(FACTORY_BYTECODE)) {
    throw new Error("[cache] eth_call data is not a deployless factory wrapper");
  }
  const argsHex = `0x${data.slice(FACTORY_BYTECODE.length)}` as Hex;
  const [targetTo, targetData, factory, factoryData] = decodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, argsHex);
  return { targetTo, targetData, factory, factoryData };
}

/**
 * Mirrors viem's internal `toDeploylessCallViaFactoryData`. Rebuilds a deployless factory
 * `eth_call` payload from its constituent parts.
 */
export function wrapDeploylessFactoryCall({ targetTo, targetData, factory, factoryData }: DeploylessFactoryParts): Hex {
  const args = encodeAbiParameters(DEPLOYLESS_CONSTRUCTOR_PARAMS, [targetTo, targetData, factory, factoryData]);
  return `${FACTORY_BYTECODE}${args.slice(2)}` as Hex;
}
