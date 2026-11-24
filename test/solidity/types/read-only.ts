// A lens can only read: a state-changing interface function cannot be declared, and no opcode that
// writes state exists to be imported.
import * as solidity from "@morpho-org/viem-dlc/solidity";
import { address, bool, iface, pure, uint256, view } from "@morpho-org/viem-dlc/solidity";
import * as yul from "@morpho-org/viem-dlc/solidity/yul";

/** `true` when the module exports none of the names, and an error naming the ones it does export. */
type Absent<Module, Names extends string> = [Extract<keyof Module, Names>] extends [never]
  ? true
  : { exported: Extract<keyof Module, Names> };

// Every opcode that writes state, calls with the right to write it, creates a contract or logs.
type StateWriting =
  | "sstore"
  | "tstore"
  | "call"
  | "callcode"
  | "delegatecall"
  | "create"
  | "create2"
  | "selfdestruct"
  | "log0"
  | "log1"
  | "log2"
  | "log3"
  | "log4";
export const noStateWritingOpcode: Absent<typeof yul, StateWriting> = true;
export const noStateChangingBuilder: Absent<typeof solidity, "nonpayable" | "payable"> = true;

// The check above can fail: it does for names that are exported.
// @ts-expect-error mload and staticcall are exported
export const readsAreExported: Absent<typeof yul, "mload" | "staticcall"> = true;
export const present = [yul.mload, yul.staticcall];

// @ts-expect-error sstore is not exported
export const sstore = yul.sstore;
// @ts-expect-error there is no nonpayable builder
export const nonpayable = solidity.nonpayable;

// Reads are declared with view and pure, and nothing else is a signature.
export const IERC20 = iface({
  balanceOf: view(address).returns(uint256),
  decimals: pure().returns(uint256),
});
export const IWritable = iface({
  // @ts-expect-error "nonpayable" is not a mutability a lens can declare
  transfer: { inputs: [address, uint256], outputs: [bool], mutability: "nonpayable" },
  // @ts-expect-error "payable" is not a mutability a lens can declare
  deposit: { inputs: [], outputs: [], mutability: "payable" },
});

// An interface handle has the declared functions and no others.
export const reads = (token: ReturnType<typeof IERC20>, holder: Parameters<typeof IERC20>[0]) => {
  // @ts-expect-error the interface declares no transfer
  token.transfer(holder, 1n);
  return token.balanceOf(IERC20(holder) as never);
};
