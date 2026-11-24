// A lens whose helpers, struct and constants all come from the library named "helpers", which the
// test resolves to the library's TypeScript source or to one of its builds.
import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { feeOf, mulWad, Quote } from "helpers";

export const FeeLens = contract("FeeLens", {
  quote: pure(uint256, uint256, uint256).returns(Quote),
}).implementation({
  quote(assets, price, feeBps) {
    const o = new Quote();
    o.gross = mulWad(assets, price);
    o.fee = feeOf(o.gross, feeBps);
    o.net = o.gross - o.fee;
    return o;
  },
});
