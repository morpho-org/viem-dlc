// The lens every fixture project builds. Its helper comes through the host's "@/" alias and its
// constant through a relative import, so both have to be resolved by the host's own resolver.
import { contract, pure, Struct, uint256 } from "@morpho-org/viem-dlc/solidity";

import { WAD } from "./lib/constants";
import { mulDivDown } from "@/lib/math";

class Quote extends Struct({ gross: uint256, fee: uint256, net: uint256 }) {}

export const QuoteLens = contract("QuoteLens", {
  quote: pure(uint256, uint256, uint256).returns(Quote),
}).implementation({
  quote(assets, price, feeRate) {
    const lensBodyQuote = new Quote();
    lensBodyQuote.gross = mulDivDown(assets, price, WAD);
    lensBodyQuote.fee = mulDivDown(lensBodyQuote.gross, feeRate, WAD);
    lensBodyQuote.net = lensBodyQuote.gross - lensBodyQuote.fee;
    return lensBodyQuote;
  },
});
