import { Struct, uint256 } from "@morpho-org/viem-dlc/solidity";

export class Quote extends Struct({ gross: uint256, fee: uint256, net: uint256 }) {}
