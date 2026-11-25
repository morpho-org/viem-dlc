// Several return values: a function that returns [a, b] is checked against the pair its signature
// declares, with no annotation.

import {
  address,
  bool,
  contract,
  internal,
  library,
  pure,
  require,
  uint128,
  uint256,
} from "@morpho-org/viem-dlc/solidity";

// An expression body, a block body, and values of different types.
export const pair = internal(pure(uint256).returns(uint256, uint256), (x) => [x, x + 1n]);
export const ordered = internal(pure(uint256, uint256).returns(uint256, uint256), (a, b) => {
  if (a < b) return [a, b];
  return [b, a];
});
export const mixed = internal(pure(uint256, address).returns(uint128, bool, address), (x, account) => [
  uint128(x),
  x > 1n,
  account,
]);
// One value and no value are as they were.
export const one = internal(pure(uint256).returns(uint256), (x) => x + 1n);
export const none = internal(pure(uint256), (x) => {
  require(x > 0n);
});

// @ts-expect-error a pair is not three values
export const tooMany = internal(pure(uint256).returns(uint256, uint256), (x) => [x, x, x]);
// @ts-expect-error a pair is not one value
export const tooFew = internal(pure(uint256).returns(uint256, uint256), (x) => x);
// @ts-expect-error the second value is an address, not an integer
export const wrongType = internal(pure(uint256).returns(uint256, address), (x) => [x, x]);
// @ts-expect-error one value is not written in brackets
export const bracketed = internal(pure(uint256).returns(uint256), (x) => [x]);

// The same in a library and in a contract.
export const Pairs = library("Pairs", { split: pure(uint256).returns(uint256, uint256) }).implementation({
  split: (x) => [x / 2n, x % 2n],
});
export const PairLens = contract("PairLens", {
  pair: pure(uint256).returns(uint256, uint256),
  mixed: pure(uint256, address).returns(uint128, bool, address),
}).implementation({
  pair(x) {
    // Each value has the type the signature declares for it.
    const [low, high] = pair(x);
    const [half, rest] = Pairs.split(low);
    const sum: uint256 = half + rest + high;
    // @ts-expect-error the helper returns two values
    const [, , third] = ordered(x, sum);
    return [sum, third ?? high];
  },
  mixed: (x, account) => mixed(x, account),
});
