// A block of assembly as a value: tsc accepts every block the transform prints as one, with the type the
// printed local has.

import {
  address,
  assembly,
  bool,
  bytes32,
  internal,
  pure,
  Struct,
  string,
  uint8,
  uint256,
} from "@morpho-org/viem-dlc/solidity";
import { add, lt, mload, mstore } from "@morpho-org/viem-dlc/solidity/yul";

class Pair extends Struct({ low: uint256, high: uint256 }) {}

export const values = internal(pure(uint256).returns(uint256), (x) => {
  // A comparison is a word, in an expression body and after a return.
  const atLeast: uint256 = assembly("memory-safe", () => x >= 3n);
  const below: uint256 = assembly("memory-safe", () => {
    return x < 3n;
  });
  const sum: uint256 = assembly("memory-safe", () => add(x, 1n));

  // The type named first is the type of the value: any elementary type.
  const small: uint8 = assembly(uint8, "memory-safe", () => add(x, 1n));
  const flag: boolean = assembly(bool, "memory-safe", () => lt(x, 3n));
  const account: address = assembly(address, "memory-safe", () => x);
  const word: bytes32 = assembly(bytes32, "memory-safe", () => mload(0n));
  const text: string = assembly(string, "memory-safe", () => mload(0x40n));
  // @ts-expect-error a block with no type named is a uint256, which is not a uint8
  const narrowed: uint8 = assembly("memory-safe", () => add(x, 1n));
  // @ts-expect-error a struct is not the type of a block's value
  const pair = assembly(Pair, "memory-safe", () => mload(0x40n));

  // A block with no return is a statement, and not a value.
  assembly("memory-safe", () => {
    mstore(0n, x);
  });
  // @ts-expect-error a block with no return has no value
  const missing: uint256 = assembly("memory-safe", () => {
    mstore(0n, x);
  });

  return [atLeast, below, sum, small, flag, account, word, text, narrowed, pair, missing].length > 0 ? sum : below;
});
