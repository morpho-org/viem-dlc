/**
 * `@morpho-org/viem-dlc/solidity/yul`: the opcodes a typed-Yul block calls.
 *
 * An `assembly("memory-safe", () => { … })` block is compiled to Yul and means what Yul means, wrapping
 * arithmetic and right-to-left argument evaluation included. These are stubs: they give the editor types
 * and throw when called. No opcode that writes state (`sstore`, `tstore`, `call`, `delegatecall`, `create`,
 * `log0`…, `selfdestruct`) is exported, so a lens cannot be written to change state.
 */
import type { Yul } from "./types.js";

const opcode = <K extends keyof Yul>(name: K): Yul[K] =>
  ((): never => {
    throw new Error(
      `@morpho-org/viem-dlc/solidity/yul: ${name}(…) is only meaningful inside an assembly block of a lens body. Bodies are compiled at build time and never run as JavaScript.`,
    );
  }) as Yul[K];

export const add = opcode("add");
export const sub = opcode("sub");
export const mul = opcode("mul");
export const div = opcode("div");
export const sdiv = opcode("sdiv");
export const mod = opcode("mod");
export const smod = opcode("smod");
export const exp = opcode("exp");
export const signextend = opcode("signextend");
export const addmod = opcode("addmod");
export const mulmod = opcode("mulmod");
export const lt = opcode("lt");
export const gt = opcode("gt");
export const slt = opcode("slt");
export const sgt = opcode("sgt");
export const eq = opcode("eq");
export const iszero = opcode("iszero");
export const and = opcode("and");
export const or = opcode("or");
export const xor = opcode("xor");
export const not = opcode("not");
export const byte = opcode("byte");
export const shl = opcode("shl");
export const shr = opcode("shr");
export const sar = opcode("sar");
export const keccak256 = opcode("keccak256");
export const mload = opcode("mload");
export const mstore = opcode("mstore");
export const mstore8 = opcode("mstore8");
export const mcopy = opcode("mcopy");
export const calldataload = opcode("calldataload");
export const calldatasize = opcode("calldatasize");
export const calldatacopy = opcode("calldatacopy");
export const returndatasize = opcode("returndatasize");
export const returndatacopy = opcode("returndatacopy");
export const staticcall = opcode("staticcall");
export const extcodesize = opcode("extcodesize");
export const balance = opcode("balance");
export const selfbalance = opcode("selfbalance");
export const address = opcode("address");
export const caller = opcode("caller");
export const timestamp = opcode("timestamp");
export const number = opcode("number");
export const chainid = opcode("chainid");
export const gas = opcode("gas");
export const pop = opcode("pop");
