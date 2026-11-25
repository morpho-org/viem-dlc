// Conversions to the fixed-size bytes types: tsc accepts the ones solc accepts, and refuses the rest.

import {
  address,
  bool,
  bytes,
  bytes4,
  bytes32,
  internal,
  pure,
  Struct,
  string,
  uint32,
  uint256,
} from "@morpho-org/viem-dlc/solidity";

class Values extends Struct({
  selector: bytes4,
  word: bytes32,
  data: bytes,
  account: address,
  flag: bool,
  text: string,
  small: uint32,
  large: uint256,
}) {}

export const fixedBytes = internal(pure(Values), (v) => {
  // From the other fixed-size type, which cuts or pads, and from the type itself.
  v.selector = bytes4(v.word);
  v.word = bytes32(v.selector);
  v.selector = bytes4(v.selector);
  v.word = bytes32(v.word);
  // From bytes, from an integer and from a literal.
  v.selector = bytes4(v.data);
  v.word = bytes32(v.data);
  v.selector = bytes4(v.small);
  v.word = bytes32(v.large);
  v.selector = bytes4("0x313ce567");
  v.selector = bytes4(0x313ce567);
  v.word = bytes32(0);

  // @ts-expect-error an address does not convert to a bytes4
  v.selector = bytes4(v.account);
  // @ts-expect-error a bool does not convert to a bytes4
  v.selector = bytes4(v.flag);
  // @ts-expect-error a string does not convert to a bytes32
  v.word = bytes32(v.text);
  // @ts-expect-error a struct does not convert to a bytes4
  v.selector = bytes4(v);

  // The conversion gives the type it names.
  // @ts-expect-error a bytes4 is not a bytes32
  v.word = bytes4(v.word);
  // @ts-expect-error a bytes32 is not a bytes4
  v.selector = bytes32(v.selector);
});
