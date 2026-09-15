import type { Hex } from "viem";

import {
  type DeploylessFactoryCall,
  encodeEnvelopeArgs,
  FACTORY_BYTECODE_REVERT,
} from "../../src/utils/deployless/codec.envelope.js";

/** The initcode-delivered payload: the envelope followed by its argument tuple. */
export function wrapDeploylessFactoryCall(
  call: DeploylessFactoryCall,
  { config }: { compress?: boolean; config: bigint },
): Hex {
  return `${FACTORY_BYTECODE_REVERT}${encodeEnvelopeArgs(call, config).slice(2)}` as Hex;
}
