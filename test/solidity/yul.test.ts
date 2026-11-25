/**
 * Typed Yul: one fixture pair per row of the block's table. Each pair is a TypeScript snippet and the
 * Solidity a person wrote for it, with the assembly block written by hand; see ./helpers/harness.ts for
 * what is compared.
 */
import { decodeFunctionResult, stringToHex } from "viem";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { YUL_OPCODES } from "../../src/solidity/build/print-yul.js";

import { type Evm, requireAnvil, startAnvil } from "./helpers/anvil.js";
import {
  buildSnippet,
  compilePair,
  expectSameAst,
  expectSameExecution,
  type FixtureCall,
  type FixturePair,
} from "./helpers/harness.js";

const MAX_UINT256 = 2n ** 256n - 1n;

const pairs = {
  "const and let are Yul locals": {
    ts: `
      import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
      import { add, mul } from "@morpho-org/viem-dlc/solidity/yul";

      export const Locals = contract("Locals", {
        weigh: pure(uint256, uint256).returns(uint256),
      }).implementation({
        weigh(a, b) {
          const weight = assembly("memory-safe", () => {
            const doubled = mul(a, 2n);
            let total = add(doubled, b);
            return total;
          });
          return weight;
        },
      });
    `,
    solidity: `
      contract Locals {
          function weigh(uint256 a, uint256 b) external pure returns (uint256) {
              uint256 weight;
              assembly ("memory-safe") {
                  let doubled := mul(a, 2)
                  let total := add(doubled, b)
                  weight := total
              }
              return weight;
          }
      }
    `,
    calls: [
      { functionName: "weigh", args: [3n, 4n] },
      // Yul wraps where Solidity would revert.
      { functionName: "weigh", args: [MAX_UINT256, 5n] },
    ],
  },
  "assignment to a Yul local and to a Solidity local declared outside the block": {
    ts: `
      import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
      import { lt, sub } from "@morpho-org/viem-dlc/solidity/yul";

      export const Assign = contract("Assign", {
        gap: pure(uint256, uint256).returns(uint256),
      }).implementation({
        gap(a, b) {
          let wrapped = uint256(a % 2n + b % 2n);
          let difference = 0n;
          assembly("memory-safe", () => {
            let x = a;
            x = sub(x, b);
            difference = x;
            wrapped = lt(a, b);
          });
          return difference ^ wrapped;
        },
      });
    `,
    solidity: `
      contract Assign {
          function gap(uint256 a, uint256 b) external pure returns (uint256) {
              uint256 wrapped = uint256(a % 2 + b % 2);
              uint256 difference = 0;
              assembly ("memory-safe") {
                  let x := a
                  x := sub(x, b)
                  difference := x
                  wrapped := lt(a, b)
              }
              return difference ^ wrapped;
          }
      }
    `,
    calls: [
      { functionName: "gap", args: [9n, 4n] },
      { functionName: "gap", args: [4n, 9n] },
      { functionName: "gap", args: [0n, 1n] },
    ],
  },
  "every opcode, with its arguments in Yul's order": {
    ts: `
      import { assembly, contract, uint256, view } from "@morpho-org/viem-dlc/solidity";
      import {
        add, addmod, address, and, balance, byte, calldatacopy, calldataload, calldatasize, caller, chainid, div, eq,
        exp, extcodesize, gas, gt, iszero, keccak256, lt, mcopy, mload, mod, mstore, mstore8, mul, mulmod, not, number,
        or, pop, returndatacopy, returndatasize, sar, sdiv, selfbalance, sgt, shl, shr, signextend, slt, smod,
        staticcall, sub, timestamp, xor,
      } from "@morpho-org/viem-dlc/solidity/yul";

      export const Opcodes = contract("Opcodes", {
        mix: view(uint256, uint256, uint256).returns(uint256),
      }).implementation({
        mix(a, b, m) {
          const mixed = assembly("memory-safe", () => {
            const p = mload(0x40);
            let acc = add(a, b);
            acc = xor(acc, sub(a, b));
            acc = xor(acc, mul(a, b));
            acc = xor(acc, div(a, b));
            acc = xor(acc, sdiv(a, b));
            acc = xor(acc, mod(a, b));
            acc = xor(acc, smod(a, b));
            acc = xor(acc, exp(a, b));
            acc = xor(acc, signextend(0n, a));
            acc = xor(acc, addmod(a, b, m));
            acc = xor(acc, mulmod(a, b, m));
            acc = xor(acc, shl(1n, lt(a, b)));
            acc = xor(acc, shl(2n, gt(a, b)));
            acc = xor(acc, shl(3n, slt(a, b)));
            acc = xor(acc, shl(4n, sgt(a, b)));
            acc = xor(acc, shl(5n, eq(a, b)));
            acc = xor(acc, shl(6n, iszero(a)));
            acc = xor(acc, and(a, b));
            acc = xor(acc, or(a, b));
            acc = xor(acc, not(a));
            acc = xor(acc, byte(31n, a));
            acc = xor(acc, shr(3n, a));
            acc = xor(acc, sar(3n, a));
            mstore(p, acc);
            mstore8(add(p, 0x20), a);
            mcopy(add(p, 0x40), p, 0x21);
            calldatacopy(add(p, 0x80), 0x04, sub(calldatasize(), 0x04));
            acc = keccak256(p, add(0x7c, calldatasize()));
            acc = xor(acc, calldataload(0x24));
            pop(staticcall(gas(), address(), 0x00, 0x00, 0x00, 0x00));
            returndatacopy(p, 0x00, returndatasize());
            acc = xor(acc, returndatasize());
            acc = xor(acc, iszero(extcodesize(address())));
            acc = xor(acc, balance(caller()));
            acc = xor(acc, selfbalance());
            acc = xor(acc, shl(7n, gt(timestamp(), 0n)));
            acc = xor(acc, shl(8n, lt(number(), chainid())));
            return acc;
          });
          return mixed;
        },
      });
    `,
    solidity: `
      contract Opcodes {
          function mix(uint256 a, uint256 b, uint256 m) external view returns (uint256) {
              uint256 mixed;
              assembly ("memory-safe") {
                  let p := mload(0x40)
                  let acc := add(a, b)
                  acc := xor(acc, sub(a, b))
                  acc := xor(acc, mul(a, b))
                  acc := xor(acc, div(a, b))
                  acc := xor(acc, sdiv(a, b))
                  acc := xor(acc, mod(a, b))
                  acc := xor(acc, smod(a, b))
                  acc := xor(acc, exp(a, b))
                  acc := xor(acc, signextend(0, a))
                  acc := xor(acc, addmod(a, b, m))
                  acc := xor(acc, mulmod(a, b, m))
                  acc := xor(acc, shl(1, lt(a, b)))
                  acc := xor(acc, shl(2, gt(a, b)))
                  acc := xor(acc, shl(3, slt(a, b)))
                  acc := xor(acc, shl(4, sgt(a, b)))
                  acc := xor(acc, shl(5, eq(a, b)))
                  acc := xor(acc, shl(6, iszero(a)))
                  acc := xor(acc, and(a, b))
                  acc := xor(acc, or(a, b))
                  acc := xor(acc, not(a))
                  acc := xor(acc, byte(31, a))
                  acc := xor(acc, shr(3, a))
                  acc := xor(acc, sar(3, a))
                  mstore(p, acc)
                  mstore8(add(p, 0x20), a)
                  mcopy(add(p, 0x40), p, 0x21)
                  calldatacopy(add(p, 0x80), 0x04, sub(calldatasize(), 0x04))
                  acc := keccak256(p, add(0x7c, calldatasize()))
                  acc := xor(acc, calldataload(0x24))
                  pop(staticcall(gas(), address(), 0x00, 0x00, 0x00, 0x00))
                  returndatacopy(p, 0x00, returndatasize())
                  acc := xor(acc, returndatasize())
                  acc := xor(acc, iszero(extcodesize(address())))
                  acc := xor(acc, balance(caller()))
                  acc := xor(acc, selfbalance())
                  acc := xor(acc, shl(7, gt(timestamp(), 0)))
                  acc := xor(acc, shl(8, lt(number(), chainid())))
                  mixed := acc
              }
              return mixed;
          }
      }
    `,
    calls: [
      { functionName: "mix", args: [7n, 3n, 5n] },
      // Division and modulus by zero are zero in Yul.
      { functionName: "mix", args: [MAX_UINT256, 0n, 0n] },
      { functionName: "mix", args: [2n ** 255n, MAX_UINT256, 97n] },
    ],
  },
  "comparisons are lt, gt, eq and iszero of them": {
    ts: `
      import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
      import { add, or, shl, shr } from "@morpho-org/viem-dlc/solidity/yul";

      export const Compare = contract("Compare", {
        bits: pure(uint256, uint256).returns(uint256),
      }).implementation({
        bits(a, b) {
          const set = assembly("memory-safe", () => {
            let acc = 0n;
            acc = or(shl(1n, acc), a < b);
            acc = or(shl(1n, acc), a > b);
            acc = or(shl(1n, acc), a <= b);
            acc = or(shl(1n, acc), a >= b);
            acc = or(shl(1n, acc), a == b);
            acc = or(shl(1n, acc), a != b);
            acc = or(shl(1n, acc), a === b);
            acc = or(shl(1n, acc), a !== b);
            acc = or(shl(1n, acc), (a < b) == (b > a));
            if (a >= 100n) {
              acc = add(acc, 0x1000n);
            }
            while (acc > 0x1000n) {
              acc = shr(1n, acc);
            }
            return acc;
          });
          return set;
        },
      });
    `,
    solidity: `
      contract Compare {
          function bits(uint256 a, uint256 b) external pure returns (uint256) {
              uint256 set;
              assembly ("memory-safe") {
                  let acc := 0
                  acc := or(shl(1, acc), lt(a, b))
                  acc := or(shl(1, acc), gt(a, b))
                  acc := or(shl(1, acc), iszero(gt(a, b)))
                  acc := or(shl(1, acc), iszero(lt(a, b)))
                  acc := or(shl(1, acc), eq(a, b))
                  acc := or(shl(1, acc), iszero(eq(a, b)))
                  acc := or(shl(1, acc), eq(a, b))
                  acc := or(shl(1, acc), iszero(eq(a, b)))
                  acc := or(shl(1, acc), eq(lt(a, b), gt(b, a)))
                  if iszero(lt(a, 100)) { acc := add(acc, 0x1000) }
                  for {} gt(acc, 0x1000) {} { acc := shr(1, acc) }
                  set := acc
              }
              return set;
          }
      }
    `,
    calls: [
      { functionName: "bits", args: [1n, 2n] },
      { functionName: "bits", args: [2n, 1n] },
      { functionName: "bits", args: [2n, 2n] },
      { functionName: "bits", args: [200n, 100n] },
      // Unsigned: the largest word is not below zero.
      { functionName: "bits", args: [MAX_UINT256, 0n] },
    ],
  },
  "if is if, and if with else is a switch": {
    ts: `
      import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
      import { add, gt, lt } from "@morpho-org/viem-dlc/solidity/yul";

      export const Branches = contract("Branches", {
        grade: pure(uint256, uint256).returns(uint256),
      }).implementation({
        grade(a, b) {
          let out = 0n;
          assembly("memory-safe", () => {
            if (gt(a, b)) {
              out = 1n;
            }
            if (a == b) {
              out = 2n;
            } else {
              out = add(out, 4n);
            }
            if (lt(a, 10n)) out = add(out, 8n);
            else if (lt(a, 20n)) out = add(out, 16n);
            else out = add(out, 32n);
          });
          return out;
        },
      });
    `,
    solidity: `
      contract Branches {
          function grade(uint256 a, uint256 b) external pure returns (uint256) {
              uint256 out = 0;
              assembly ("memory-safe") {
                  if gt(a, b) { out := 1 }
                  switch eq(a, b)
                  case 0 { out := add(out, 4) }
                  default { out := 2 }
                  switch lt(a, 10)
                  case 0 {
                      switch lt(a, 20)
                      case 0 { out := add(out, 32) }
                      default { out := add(out, 16) }
                  }
                  default { out := add(out, 8) }
              }
              return out;
          }
      }
    `,
    calls: [
      { functionName: "grade", args: [5n, 3n] },
      { functionName: "grade", args: [5n, 5n] },
      { functionName: "grade", args: [15n, 20n] },
      { functionName: "grade", args: [25n, 1n] },
    ],
  },
  "while and for are Yul's for, with break and continue": {
    ts: `
      import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
      import { add, eq, gt, lt, mload, mstore, shr, sub } from "@morpho-org/viem-dlc/solidity/yul";

      export const Loops = contract("Loops", {
        walk: pure(uint256).returns(uint256),
      }).implementation({
        walk(n) {
          const total = assembly("memory-safe", () => {
            let acc = 0n;
            for (let i = 0n; lt(i, n); i = add(i, 1n)) {
              if (eq(i, 3n)) {
                continue;
              }
              if (gt(i, 10n)) {
                break;
              }
              acc = add(acc, i);
            }
            let k = n;
            while (gt(k, 100n)) {
              k = shr(1n, k);
            }
            for (; lt(k, 5n); ) k = add(k, 2n);
            for (let low = 0n, high = k; low < high; mstore(0x00, low)) {
              low = add(low, 1n);
              high = sub(high, 1n);
            };
            for (acc = add(acc, 1n); gt(acc, 20n); acc = shr(1n, acc)) {}
            return add(acc, add(k, mload(0x00)));
          });
          return total;
        },
      });
    `,
    solidity: `
      contract Loops {
          function walk(uint256 n) external pure returns (uint256) {
              uint256 total;
              assembly ("memory-safe") {
                  let acc := 0
                  for { let i := 0 } lt(i, n) { i := add(i, 1) } {
                      if eq(i, 3) { continue }
                      if gt(i, 10) { break }
                      acc := add(acc, i)
                  }
                  let k := n
                  for {} gt(k, 100) {} { k := shr(1, k) }
                  for {} lt(k, 5) {} { k := add(k, 2) }
                  for { let low := 0 let high := k } lt(low, high) { mstore(0x00, low) } {
                      low := add(low, 1)
                      high := sub(high, 1)
                  }
                  for { acc := add(acc, 1) } gt(acc, 20) { acc := shr(1, acc) } {}
                  total := add(acc, add(k, mload(0x00)))
              }
              return total;
          }
      }
    `,
    calls: [
      { functionName: "walk", args: [0n] },
      { functionName: "walk", args: [3n] },
      { functionName: "walk", args: [8n] },
      { functionName: "walk", args: [50n] },
      { functionName: "walk", args: [1000n] },
    ],
  },
  "arrow functions are Yul functions": {
    ts: `
      import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
      import { gt, lt, mload, mstore, mul, xor } from "@morpho-org/viem-dlc/solidity/yul";

      export const Functions = contract("Functions", {
        bound: pure(uint256, uint256, uint256).returns(uint256),
      }).implementation({
        bound(a, b, c) {
          const lowest = assembly("memory-safe", () => {
            const min = (x, y) => xor(x, mul(xor(x, y), lt(y, x)));
            const clamp = (x: bigint, low: bigint, high: bigint) => {
              let v = x;
              if (lt(v, low)) {
                v = low;
              }
              if (gt(v, high)) {
                v = high;
              }
              return v;
            };
            const store = (p: bigint, v: bigint) => {
              mstore(p, v);
            };
            store(0x00n, clamp(a, b, c));
            return min(mload(0x00), c);
          });
          return lowest;
        },
      });
    `,
    solidity: `
      contract Functions {
          function bound(uint256 a, uint256 b, uint256 c) external pure returns (uint256) {
              uint256 lowest;
              assembly ("memory-safe") {
                  function min(x, y) -> r {
                      r := xor(x, mul(xor(x, y), lt(y, x)))
                  }
                  function clamp(x, low, high) -> r {
                      let v := x
                      if lt(v, low) { v := low }
                      if gt(v, high) { v := high }
                      r := v
                  }
                  function store(p, v) {
                      mstore(p, v)
                  }
                  store(0x00, clamp(a, b, c))
                  lowest := min(mload(0x00), c)
              }
              return lowest;
          }
      }
    `,
    calls: [
      { functionName: "bound", args: [5n, 1n, 9n] },
      { functionName: "bound", args: [0n, 1n, 9n] },
      { functionName: "bound", args: [50n, 1n, 9n] },
      { functionName: "bound", args: [5n, 9n, 1n] },
    ],
  },
  "a block that ends in return is a value: a uint256, or the type named first": {
    ts: `
      import { assembly, bytes, bytes32, contract, pure, string, uint256 } from "@morpho-org/viem-dlc/solidity";
      import { add, keccak256, mload, mstore } from "@morpho-org/viem-dlc/solidity/yul";

      export const Values = contract("Values", {
        free: pure().returns(uint256),
        pair: pure(uint256, uint256).returns(bytes32),
        text: pure(bytes32, uint256).returns(string),
        raw: pure(bytes32, uint256).returns(bytes),
      }).implementation({
        free() {
          const pointer = assembly("memory-safe", () => mload(0x40));
          return pointer;
        },
        pair(a, b) {
          const id = assembly(bytes32, "memory-safe", () => {
            mstore(0x00, a);
            mstore(0x20, b);
            return keccak256(0x00, 0x40);
          });
          return id;
        },
        text(word, length) {
          const made = assembly(string, "memory-safe", () => {
            const p = mload(0x40);
            mstore(0x40, add(p, 0x40));
            mstore(p, length);
            mstore(add(p, 0x20), word);
            return p;
          });
          return made;
        },
        raw(word, length) {
          const made = assembly(bytes, "memory-safe", function () {
            const p = mload(0x40);
            mstore(0x40, add(p, 0x40));
            mstore(p, length);
            mstore(add(p, 0x20), word);
            return p;
          });
          return made;
        },
      });
    `,
    solidity: `
      contract Values {
          function free() external pure returns (uint256) {
              uint256 pointer;
              assembly ("memory-safe") {
                  pointer := mload(0x40)
              }
              return pointer;
          }

          function pair(uint256 a, uint256 b) external pure returns (bytes32) {
              bytes32 id;
              assembly ("memory-safe") {
                  mstore(0x00, a)
                  mstore(0x20, b)
                  id := keccak256(0x00, 0x40)
              }
              return id;
          }

          function text(bytes32 word, uint256 length) external pure returns (string memory) {
              string memory made;
              assembly ("memory-safe") {
                  let p := mload(0x40)
                  mstore(0x40, add(p, 0x40))
                  mstore(p, length)
                  mstore(add(p, 0x20), word)
                  made := p
              }
              return made;
          }

          function raw(bytes32 word, uint256 length) external pure returns (bytes memory) {
              bytes memory made;
              assembly ("memory-safe") {
                  let p := mload(0x40)
                  mstore(0x40, add(p, 0x40))
                  mstore(p, length)
                  mstore(add(p, 0x20), word)
                  made := p
              }
              return made;
          }
      }
    `,
    calls: [
      { functionName: "free" },
      { functionName: "pair", args: [1n, 2n] },
      { functionName: "text", args: [stringToHex("made of words", { size: 32 }), 13n] },
      { functionName: "text", args: [stringToHex("", { size: 32 }), 0n] },
      { functionName: "raw", args: [stringToHex("made of words", { size: 32 }), 4n] },
    ],
  },
  "a literal is respelled, and a constant is the single literal it names": {
    ts: {
      "lens.ts": `
        import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
        import { add, and, or, shl } from "@morpho-org/viem-dlc/solidity/yul";

        import { DECIMALS_SELECTOR } from "./selectors.js";

        const SHIFT = 224n;
        const MASK = 0xffn;
        const STEP = 60_000n;

        export const Literals = contract("Literals", {
          probe: pure(uint256).returns(uint256),
        }).implementation({
          probe(a) {
            const word = assembly("memory-safe", () => {
              let acc = shl(SHIFT, DECIMALS_SELECTOR);
              acc = or(acc, and(a, MASK));
              acc = add(acc, STEP);
              acc = add(acc, 0X1Fn);
              acc = add(acc, 1_000);
              return acc;
            });
            return word;
          },
        });
      `,
      "selectors.ts": `
        export const DECIMALS_SELECTOR = 0x313ce567n;
      `,
    },
    solidity: `
      contract Literals {
          function probe(uint256 a) external pure returns (uint256) {
              uint256 word;
              assembly ("memory-safe") {
                  let acc := shl(224, 0x313ce567)
                  acc := or(acc, and(a, 0xff))
                  acc := add(acc, 60000)
                  acc := add(acc, 0x1F)
                  acc := add(acc, 1000)
                  word := acc
              }
              return word;
          }
      }
    `,
    calls: [
      { functionName: "probe", args: [0n] },
      { functionName: "probe", args: [0x1234n] },
    ],
  },
  "an opcode keeps its name under an alias or a namespace, and TypeScript-only syntax is erased": {
    ts: `
      import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
      import * as yul from "@morpho-org/viem-dlc/solidity/yul";
      import { add as plus, mload as load } from "@morpho-org/viem-dlc/solidity/yul";

      export const Names = contract("Names", {
        blend: pure(uint256, uint256).returns(uint256),
      }).implementation({
        blend(a, b) {
          const out = assembly("memory-safe", () => {
            const base: bigint = (a as bigint)!;
            const next = plus((base), yul.mul(b satisfies bigint, 2n));
            yul.mstore(0x00, next);
            return load(0x00);
          });
          return out;
        },
      });
    `,
    solidity: `
      contract Names {
          function blend(uint256 a, uint256 b) external pure returns (uint256) {
              uint256 out;
              assembly ("memory-safe") {
                  let base := a
                  let next := add(base, mul(b, 2))
                  mstore(0x00, next)
                  out := mload(0x00)
              }
              return out;
          }
      }
    `,
    calls: [
      { functionName: "blend", args: [1n, 2n] },
      { functionName: "blend", args: [MAX_UINT256, 1n] },
    ],
  },
} satisfies Record<string, FixturePair & { calls: readonly FixtureCall[] }>;

describe("typed Yul", () => {
  describe("the printed block is the handwritten block", () => {
    for (const [name, pair] of Object.entries(pairs))
      it(name, async () => {
        expectSameAst(await compilePair(pair));
      });
  });

  describe.skipIf(!requireAnvil())("both execute to the same bytes", () => {
    let evm: Evm;
    beforeAll(async () => {
      evm = await startAnvil();
    });
    afterAll(() => evm?.stop());

    for (const [name, pair] of Object.entries(pairs))
      it(name, async () => {
        const outcomes = await expectSameExecution(evm, await compilePair(pair), pair.calls);
        // No block here reverts: Yul wraps, and divides by zero to zero.
        expect(outcomes.map((outcome) => outcome.status)).toEqual(pair.calls.map(() => "success"));
      });

    it("the meaning is Yul's: arithmetic wraps", async () => {
      const [, wrapped] = await expectSameExecution(
        evm,
        await compilePair(pairs["const and let are Yul locals"]),
        pairs["const and let are Yul locals"].calls,
      );
      // MAX * 2 + 5 wraps to 3.
      expect(BigInt(wrapped?.data ?? "0x")).toBe(3n);
    });

    it("a block of a named type gives that type", async () => {
      const pair = await compilePair(
        pairs["a block that ends in return is a value: a uint256, or the type named first"],
      );
      const [free, , text, , raw] = await expectSameExecution(
        evm,
        pair,
        pairs["a block that ends in return is a value: a uint256, or the type named first"].calls,
      );
      const decode = (functionName: string, data: `0x${string}` | undefined) =>
        decodeFunctionResult({ abi: pair.handwritten.abi, functionName, data: data ?? "0x" });
      expect(decode("free", free?.data)).toBe(0x80n);
      expect(decode("text", text?.data)).toBe("made of words");
      expect(decode("raw", raw?.data)).toBe(stringToHex("made"));
    });
  });
});

describe("the harness tells two blocks apart", () => {
  const ts = (difference: string) => `
    import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
    import { sub } from "@morpho-org/viem-dlc/solidity/yul";

    export const Gap = contract("Gap", { gap: pure(uint256, uint256).returns(uint256) }).implementation({
      gap(a, b) {
        const difference = assembly("memory-safe", () => ${difference});
        return difference;
      },
    });
  `;
  const solidity = `
    contract Gap {
        function gap(uint256 a, uint256 b) external pure returns (uint256) {
            uint256 difference;
            assembly ("memory-safe") {
                difference := sub(a, b)
            }
            return difference;
        }
    }
  `;

  it("by the order of an opcode's arguments", async () => {
    expectSameAst(await compilePair({ ts: ts("sub(a, b)"), solidity }));
    const swapped = await compilePair({ ts: ts("sub(b, a)"), solidity });
    expect(() => expectSameAst(swapped)).toThrow();
  });

  it("by the opcode", async () => {
    const other = await compilePair({ ts: ts("a > b"), solidity });
    expect(() => expectSameAst(other)).toThrow();
  });
});

describe("the opcodes", () => {
  // Every opcode that writes state, calls with the right to write it, creates a contract, logs, or ends the call.
  const STATE_WRITING = [
    "sstore",
    "tstore",
    "call",
    "callcode",
    "delegatecall",
    "create",
    "create2",
    "selfdestruct",
    "log0",
    "log1",
    "log2",
    "log3",
    "log4",
  ];

  it("are the ones the yul entry exports, and none writes state", async () => {
    const exported = Object.keys(await import("../../src/solidity/yul.js")).sort();
    expect([...YUL_OPCODES].sort()).toEqual(exported);
    expect(exported.filter((name) => STATE_WRITING.includes(name))).toEqual([]);
  });

  it("each have a fixture pair", () => {
    const pair = pairs["every opcode, with its arguments in Yul's order"];
    for (const name of YUL_OPCODES) {
      expect(pair.ts, name).toMatch(new RegExp(`[^a-z]${name}\\(`));
      expect(pair.solidity, name).toMatch(new RegExp(`[^a-z]${name}\\(`));
    }
  });
});

describe("what the Yul printer prints", () => {
  it("is the block as written: nothing is evaluated, reordered or added", async () => {
    const built = await buildSnippet(pairs["a literal is respelled, and a constant is the single literal it names"].ts);
    expect(built.lenses[0]?.printed.solidity).toContain(`        uint256 word;
        assembly ("memory-safe") {
            let acc := shl(224, 0x313ce567)
            acc := or(acc, and(a, 0xff))
            acc := add(acc, 60000)
            acc := add(acc, 0x1F)
            acc := add(acc, 1000)
            word := acc
        }
        return word;`);
  });

  it("is laid out one statement to a line", async () => {
    const built = await buildSnippet(pairs["while and for are Yul's for, with break and continue"].ts);
    expect(built.lenses[0]?.printed.solidity).toContain(`        assembly ("memory-safe") {
            let acc := 0
            for { let i := 0 } lt(i, n) { i := add(i, 1) } {
                if eq(i, 3) {
                    continue
                }
                if gt(i, 10) {
                    break
                }
                acc := add(acc, i)
            }
            let k := n
            for {} gt(k, 100) {} {
                k := shr(1, k)
            }
            for {} lt(k, 5) {} {
                k := add(k, 2)
            }
            for { let low := 0 let high := k } lt(low, high) { mstore(0x00, low) } {
                low := add(low, 1)
                high := sub(high, 1)
            }
            for { acc := add(acc, 1) } gt(acc, 20) { acc := shr(1, acc) } {
            }
            total := add(acc, add(k, mload(0x00)))
        }`);
  });
});
