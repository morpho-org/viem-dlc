// A lens tsc accepts and solc rejects, for the check command's error format: an arithmetic result is a
// plain bigint to tsc, and a uint256 to solc. Nothing imports this module.
import { contract, pure, uint128, uint256 } from "@morpho-org/viem-dlc/solidity";

export const BrokenLens = contract("BrokenLens", {
  narrow: pure(uint256).returns(uint128),
}).implementation({
  narrow: (x) => x + 1n,
});
