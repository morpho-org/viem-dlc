// A Node script with a lens written inline, run by ../adapters.test.ts through the Node hook and
// scanned by the check command.
import { contract, internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

import { SCALE } from "./constants.ts";

const scaled = internal(pure(uint256).returns(uint256), (x) => x * SCALE);

export const ScaleLens = contract("ScaleLens", {
  scale: pure(uint256).returns(uint256),
}).implementation({
  scale: (x) => scaled(x),
});

console.log(
  JSON.stringify({
    name: ScaleLens.name,
    functions: ScaleLens.abi.map((item) => item.name),
    bytecode: ScaleLens.bytecode,
  }),
);
