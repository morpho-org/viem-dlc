/**
 * Made-up contracts for a lens to call: a token, a price feed, and an account whose every call reverts.
 * They are handwritten Solidity, compiled with the repository's pinned compiler and deployed on the test
 * node, so a fixture pair's calls leave the lens and come back with real return or revert data.
 */
import { type Address, encodeAbiParameters, type Hex } from "viem";

import type { Evm } from "./anvil.js";
import { type CompiledPair, compileSolidity } from "./harness.js";

const MOCKS = `
// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;

contract MockToken {
    function balanceOf(address holder) external pure returns (uint256) {
        return uint256(uint160(holder)) % 1000;
    }

    function totalSupply() external pure returns (uint256) {
        return 1_000_000e6;
    }

    function decimals() external pure returns (uint8) {
        return 6;
    }

    function symbol() external pure returns (string memory) {
        return "TKN";
    }

    function allowance(address owner, address spender) external pure returns (uint256) {
        return uint256(uint160(owner)) % 7 + uint256(uint160(spender)) % 5;
    }
}

contract MockFeed {
    function price(address token) external pure returns (uint256) {
        return uint256(uint160(token)) % 1_000_000 + 1;
    }

    function latest(address token) external pure returns (uint256, uint64) {
        return (uint256(uint160(token)) % 1_000_000 + 1, 1_700_000_000);
    }

    function validate(address holder) external pure {
        require(holder != address(0), "no holder");
    }

    function paused() external pure returns (bool) {
        return false;
    }

    function describe(bytes calldata tag) external pure returns (bytes memory) {
        return abi.encodePacked("feed:", tag);
    }

    function gasLeft() external view returns (uint256) {
        return gasleft();
    }

    // Never returns: it runs until the gas it was given is gone.
    function spin() external pure returns (uint256 i) {
        while (true) i++;
    }
}

contract MockBroken {
    fallback() external {
        revert("broken");
    }
}
`;

export interface Mocks {
  token: Address;
  feed: Address;
  /** A contract whose every call reverts with Error("broken"). */
  broken: Address;
  /** An address with no code. */
  empty: Address;
}

export async function deployMocks(evm: Evm): Promise<Mocks> {
  const deploy = (name: string) => evm.deploy(compileSolidity(MOCKS, name).bytecode);
  return {
    token: await deploy("MockToken"),
    feed: await deploy("MockFeed"),
    broken: await deploy("MockBroken"),
    empty: "0x00000000000000000000000000000000000000e0",
  };
}

/** A compiled pair with the same constructor arguments appended to both sides' creation bytecode. */
export function withConstructorArgs(pair: CompiledPair, args: readonly unknown[]): CompiledPair {
  const inputs = pair.handwritten.abi.find((item) => item.type === "constructor")?.inputs ?? [];
  const encoded = encodeAbiParameters(inputs, args).slice(2);
  const append = (bytecode: Hex): Hex => `${bytecode}${encoded}`;
  return {
    ...pair,
    ported: { ...pair.ported, bytecode: append(pair.ported.bytecode) },
    handwritten: { ...pair.handwritten, bytecode: append(pair.handwritten.bytecode) },
  };
}
