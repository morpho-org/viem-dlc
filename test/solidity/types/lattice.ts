// The width lattice: tsc accepts every widening and refuses a direct narrowing, as solc does. Where tsc
// cannot follow solc it is deliberately loose, and solc catches the rest at build time.

import * as solidity from "@morpho-org/viem-dlc/solidity";
import {
  type address,
  block,
  type bytes,
  type bytes4,
  type bytes32,
  int16,
  int256,
  internal,
  pure,
  Struct,
  uint8,
  uint64,
  uint128,
  uint256,
  view,
} from "@morpho-org/viem-dlc/solidity";

class Widths extends Struct({
  u8: uint8,
  u64: uint64,
  u128: uint128,
  u256: uint256,
  i16: int16,
  i256: int256,
  account: solidity.address,
  word: solidity.bytes32,
  selector: solidity.bytes4,
  data: solidity.bytes,
  text: solidity.string,
  flag: solidity.bool,
}) {}

export const widening = internal(view(Widths).returns(uint256), (w) => {
  // Every widening is accepted.
  w.u64 = w.u8;
  w.u128 = w.u64;
  w.u256 = w.u128;
  w.u256 = w.u8;
  // A signed type takes every narrower unsigned one, and no unsigned type takes a signed one.
  w.i16 = w.u8;
  w.i256 = w.u128;
  w.i256 = w.i16;
  // @ts-expect-error int16 does not convert to uint256
  w.u256 = w.i16;
  // @ts-expect-error uint256 does not convert to int256
  w.i256 = w.u256;
  // @ts-expect-error uint64 does not convert to int16
  w.i16 = w.u64;

  // A direct narrowing is refused, and a conversion names it.
  // @ts-expect-error uint256 does not convert to uint128
  w.u128 = w.u256;
  // @ts-expect-error uint128 does not convert to uint64
  w.u64 = w.u128;
  // @ts-expect-error uint64 does not convert to uint8
  w.u8 = w.u64;
  // @ts-expect-error block.timestamp is a uint256
  w.u64 = block.timestamp;
  w.u64 = uint64(block.timestamp);
  w.u8 = uint8(w.u256);

  // A literal goes anywhere. So does an arithmetic result, which is a plain bigint to tsc: solc refuses
  // this one at build time, reported at this line.
  w.u8 = 255n;
  w.u128 = w.u256 - 1n;

  // Arithmetic and comparison across widths need no conversion.
  const sum = uint256(w.u8 + w.u128 * w.u256);
  return w.u64 < w.u256 ? sum : w.u8;
});

export const opaque = internal(pure(Widths), (w) => {
  // An address, a bytes32, a bytes4 and a bytes are four different things.
  // @ts-expect-error a bytes32 is not an address
  w.account = w.word;
  // @ts-expect-error an address is not a bytes32
  w.word = w.account;
  // @ts-expect-error a bytes32 is not a bytes4
  w.selector = w.word;
  // @ts-expect-error a bytes4 is not a bytes32
  w.word = w.selector;
  // @ts-expect-error a bytes is not a bytes32
  w.word = w.data;
  // @ts-expect-error a bytes4 is not a bytes
  w.data = w.selector;
  // @ts-expect-error an integer is not an address
  w.account = w.u256;
  // @ts-expect-error an address is not an integer
  w.u256 = w.account;
  // @ts-expect-error a string is not a bytes
  w.data = w.text;
  // @ts-expect-error an integer is not a bool
  w.flag = w.u8;

  // None of them does arithmetic.
  // @ts-expect-error an address cannot be added to
  const next = w.account + 1n;
  // @ts-expect-error a bytes32 cannot be compared with an integer
  const zero = w.word === 0n;

  // A conversion goes between them, spelled as in Solidity: an address and a bytes32 meet at the integers.
  w.account = solidity.address(solidity.uint160(uint256(w.word)));
  w.word = solidity.bytes32(uint256(solidity.uint160(w.account)));
  // @ts-expect-error a bytes32 does not convert to an address directly
  w.account = solidity.address(w.word);
  // @ts-expect-error an address does not convert to a bytes32 directly
  w.word = solidity.bytes32(w.account);
  // @ts-expect-error an address converts to uint160 and to no other integer
  w.u256 = uint256(w.account);
  w.account = solidity.address(0);
  w.u256 = uint256(w.word);
  return [next, zero] as unknown as undefined;
});

// The four opaque types are distinct as types too.
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
export const distinct: [
  Same<address, bytes32>,
  Same<bytes32, bytes4>,
  Same<bytes4, bytes>,
  Same<bytes, address>,
  Same<address, address>,
] = [false, false, false, false, true];

// A helper's arguments are checked against its signature.
const scale = internal(pure(uint128, solidity.address).returns(uint256), (amount, _account) => amount * 2n);
export const calls = internal(pure(Widths).returns(uint256), (w) => {
  // @ts-expect-error the arguments are swapped
  scale(w.account, w.u128);
  // @ts-expect-error a uint256 is not a uint128
  scale(w.u256, w.account);
  // @ts-expect-error a bytes32 is not an address
  scale(w.u64, w.word);
  return scale(w.u64, w.account);
});
