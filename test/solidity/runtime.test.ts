/**
 * The runtime entry as production sees it: stubs that do nothing, and the compiled-lens wrapper.
 *
 * This file names the package only through its source path, so the build integration leaves it alone:
 * the `contract(…)` below runs uncompiled, as it would in a tool with no integration.
 */
import { getContractAddress, parseAbi } from "viem";
import { describe, expect, it } from "vitest";

import * as solidity from "../../src/solidity/index.js";
import * as yul from "../../src/solidity/yul.js";

const { address, compiledLens, contract, iface, immutable, internal, library, pure, Struct, uint256, view } = solidity;

describe("declarations", () => {
  it("are inert: declaring a lens module's parts runs nothing and throws nothing", () => {
    class Position extends Struct({ owner: address, shares: uint256 }) {}
    const IPriceFeed = iface({ latest: view().returns(uint256) });
    const FEED = immutable(IPriceFeed);
    const double = internal(pure(uint256).returns(uint256), (x) => x * 2n);
    const Lib = library("Lib", { f: pure(uint256).returns(uint256) }).implementation({ f: (x) => double(x) });
    expect([Position, IPriceFeed, FEED, double, Lib].every((value) => value !== undefined)).toBe(true);
    expect(view(uint256).returns(Position)).toMatchObject({
      mutability: "view",
      inputs: [uint256],
      outputs: [Position],
    });
  });

  it("throw when something that only means something in a body is run", () => {
    const double = internal(pure(uint256).returns(uint256), (x) => x * 2n);
    for (const run of [
      () => double(1n),
      () => uint256(1n),
      () => solidity.keccak256("0x"),
      () => solidity.abi.encode(1n),
      () => solidity.block.timestamp,
      () => solidity.gasleft(),
      () => solidity.require(true),
      () => solidity.revert(),
      () => new (Struct({ a: uint256 }))(),
      () => yul.mload(0),
    ])
      expect(run).toThrow(/only meaningful inside/);
  });
});

describe("a lens no build integration compiled", () => {
  const Lens = contract("Uncompiled", { f: pure(uint256).returns(uint256) }).implementation({ f: (x) => x });

  it("throws on first use and names the integrations", () => {
    expect(Lens.name).toBe("Uncompiled");
    for (const use of [() => Lens.abi, () => Lens.bytecode, () => Lens.with()]) {
      expect(use).toThrow(/the lens Uncompiled was not compiled/);
      expect(use).toThrow(/solidity\/vite.*solidity\/next.*solidity\/register/);
    }
  });
});

describe("compiledLens", () => {
  const abi = parseAbi(["constructor(address feed, uint256 scale)", "function f(uint256) view returns (uint256)"]);
  const lens = compiledLens({ name: "L", abi, bytecode: "0x600a600c", immutables: ["FEED", "SCALE"] });

  it("exposes the name, the ABI and the bytecode", () => {
    expect(lens).toMatchObject({ name: "L", abi, bytecode: "0x600a600c" });
  });

  it("with() gives viem's deployless descriptor, constructor arguments in declared order", () => {
    const feed = "0x00000000000000000000000000000000000000aa";
    const call = (lens.with as (immutables: object) => ReturnType<typeof lens.with>)({ SCALE: 7n, FEED: feed });
    const initcode = `0x600a600c${"aa".padStart(64, "0")}${"7".padStart(64, "0")}` as const;
    expect(call).toEqual({
      abi,
      factory: "0x4e59b44847b379578588920cA78FbF26c0B4956C",
      factoryData: `0x${"0".repeat(40)}51A1E51A1E51A1E51A1E51A1${initcode.slice(2)}`,
      address: getContractAddress({
        opcode: "CREATE2",
        from: "0x4e59b44847b379578588920cA78FbF26c0B4956C",
        salt: `0x${"0".repeat(40)}51A1E51A1E51A1E51A1E51A1`,
        bytecode: initcode,
      }),
    });
  });

  it("with() refuses a missing immutable", () => {
    expect(() =>
      (lens.with as (immutables: object) => unknown)({ FEED: "0x00000000000000000000000000000000000000aa" }),
    ).toThrow(/missing "SCALE"/);
  });

  it("a lens without immutables is its bytecode", () => {
    const plain = compiledLens({ name: "P", abi: [], bytecode: "0x00", immutables: [] });
    expect(plain.with().factoryData.endsWith("51A1E51A1E51A1E51A1E51A100")).toBe(true);
  });
});

describe("the yul entry", () => {
  it("exports no opcode that writes state", () => {
    const writers = [
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
    expect(Object.keys(yul).filter((name) => writers.includes(name))).toEqual([]);
  });
});
