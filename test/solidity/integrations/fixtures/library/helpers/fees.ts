import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

import { BPS } from "./units.js";

export const feeOf = internal(pure(uint256, uint256).returns(uint256), (amount, feeBps) => (amount * feeBps) / BPS);
