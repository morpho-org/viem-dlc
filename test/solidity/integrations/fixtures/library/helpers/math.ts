import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

import { WAD } from "./units.js";

export type Rounding = "down" | "up";

// `as`, `!` and the parameter annotations exist only in the TypeScript.
export const mulWad = internal(pure(uint256, uint256).returns(uint256), (x: uint256, y: uint256) => {
  const product = uint256((x as uint256) * y!);
  return product / WAD;
});
