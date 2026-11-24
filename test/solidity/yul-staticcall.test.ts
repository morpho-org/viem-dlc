/**
 * Typed Yul on a realistic block: a memory-safe call that reads a token's balance and decodes the
 * returned word only when a whole word came back. Executed on Anvil against made-up tokens that answer
 * well, answer nothing, answer too little, and revert.
 */
import { type Address, decodeFunctionResult, encodeDeployData, type Hex } from "viem";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "./helpers/anvil.js";
import {
  type CompiledPair,
  compilePair,
  compileSolidity,
  expectSameAst,
  expectSameExecution,
  type FixturePair,
} from "./helpers/harness.js";

/** The lens, with the two operands of `and` in the order given. */
const lens = (first: string, second: string): FixturePair => ({
  ts: `
    import { address, assembly, bool, contract, internal, Struct, uint256, view } from "@morpho-org/viem-dlc/solidity";
    import { and, gas, gt, mload, mstore, returndatasize, staticcall } from "@morpho-org/viem-dlc/solidity/yul";

    // balanceOf(address), left-aligned in a word.
    const BALANCE_OF = 0x70a0823100000000000000000000000000000000000000000000000000000000n;

    class Balance extends Struct({ known: bool, amount: uint256 }) {}

    const tryBalanceOf = internal(view(address, address).returns(Balance), (token, holder) => {
      const o = new Balance();
      let ok = 0n;
      let amount = 0n;
      assembly("memory-safe", () => {
        mstore(0x00, BALANCE_OF);
        mstore(0x04, holder);
        ok = and(${first}, ${second});
        if (ok) {
          amount = mload(0x00);
        }
      });
      o.known = ok != 0n;
      o.amount = amount;
      return o;
    });

    export const BalanceLens = contract("BalanceLens", {
      balanceOf: view(address, address).returns(Balance),
    }).implementation({
      balanceOf: (token, holder) => tryBalanceOf(token, holder),
    });
  `,
  solidity: `
    struct Balance {
        bool known;
        uint256 amount;
    }

    contract BalanceLens {
        function balanceOf(address token, address holder) external view returns (Balance memory) {
            return tryBalanceOf(token, holder);
        }

        function tryBalanceOf(address token, address holder) internal view returns (Balance memory) {
            Balance memory o;
            uint256 ok = 0;
            uint256 amount = 0;
            assembly ("memory-safe") {
                mstore(0x00, 0x70a0823100000000000000000000000000000000000000000000000000000000)
                mstore(0x04, holder)
                ok := and(${first}, ${second})
                if ok { amount := mload(0x00) }
            }
            o.known = ok != 0;
            o.amount = amount;
            return o;
        }
    }
  `,
});

const SIZE = "gt(returndatasize(), 0x1f)";
const CALL = "staticcall(gas(), token, 0x00, 0x24, 0x00, 0x20)";
/** Yul evaluates arguments right to left, so the call runs first and the size read is its own. */
const sizeAfterCall = lens(SIZE, CALL);
/** The same two operands the other way round: the size is read before the call. */
const sizeBeforeCall = lens(CALL, SIZE);

const TOKENS = `// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;

contract Token {
    mapping(address => uint256) public balanceOf;

    constructor(address holder, uint256 amount) {
        balanceOf[holder] = amount;
    }
}

/// Answers every call with nothing.
contract SilentToken {
    fallback() external {}
}

/// Answers every call with one byte.
contract ShortToken {
    fallback() external {
        assembly {
            mstore(0x00, 0xff)
            return(0x1f, 0x01)
        }
    }
}

/// Answers every call with two words.
contract WideToken {
    fallback() external {
        assembly {
            mstore(0x00, 7)
            mstore(0x20, 9)
            return(0x00, 0x40)
        }
    }
}

/// Reverts with more than a word of data.
contract RevertingToken {
    fallback() external {
        revert("no balance to report");
    }
}
`;

const HOLDER: Address = "0x00000000000000000000000000000000000a11ce";
const STRANGER: Address = "0x0000000000000000000000000000000000000b0b";
const NO_CODE: Address = "0x000000000000000000000000000000000000dEaD";

describe("typed Yul: a memory-safe staticcall that decodes a word only when one came back", () => {
  it("prints the handwritten block, with the arguments in the order written", async () => {
    const pair = await compilePair(sizeAfterCall);
    expectSameAst(pair);
    expect(pair.printed).toContain(`        assembly ("memory-safe") {
            mstore(0x00, 0x70a0823100000000000000000000000000000000000000000000000000000000)
            mstore(0x04, holder)
            ok := and(gt(returndatasize(), 0x1f), staticcall(gas(), token, 0x00, 0x24, 0x00, 0x20))
            if ok {
                amount := mload(0x00)
            }
        }`);
    expectSameAst(await compilePair(sizeBeforeCall));
  });

  describe.skipIf(!requireAnvil())("on Anvil, against made-up tokens", () => {
    let evm: Evm;
    const tokens: Record<"token" | "silent" | "short" | "wide" | "reverting", Address> = {
      token: NO_CODE,
      silent: NO_CODE,
      short: NO_CODE,
      wide: NO_CODE,
      reverting: NO_CODE,
    };

    beforeAll(async () => {
      evm = await startAnvil();
      const token = compileSolidity(TOKENS, "Token");
      tokens.token = await evm.deploy(
        encodeDeployData({ abi: token.abi, bytecode: token.bytecode, args: [HOLDER, 1234n] }),
      );
      tokens.silent = await evm.deploy(compileSolidity(TOKENS, "SilentToken").bytecode);
      tokens.short = await evm.deploy(compileSolidity(TOKENS, "ShortToken").bytecode);
      tokens.wide = await evm.deploy(compileSolidity(TOKENS, "WideToken").bytecode);
      tokens.reverting = await evm.deploy(compileSolidity(TOKENS, "RevertingToken").bytecode);
    });
    afterAll(() => evm?.stop());

    /** Both sides of the pair on each target, and what the lens returned. */
    const read = async (pair: CompiledPair, targets: readonly (readonly [token: Address, holder: Address])[]) => {
      const outcomes = await expectSameExecution(
        evm,
        pair,
        targets.map((args) => ({ functionName: "balanceOf", args })),
      );
      return outcomes.map((outcome) => {
        expect(outcome.status).toBe("success");
        return decodeFunctionResult({
          abi: pair.handwritten.abi,
          functionName: "balanceOf",
          data: outcome.data as Hex,
        });
      });
    };

    it("decodes the balance a token returns, and nothing when no whole word came back", async () => {
      const balances = await read(await compilePair(sizeAfterCall), [
        [tokens.token, HOLDER],
        [tokens.token, STRANGER],
        [tokens.wide, HOLDER],
        [tokens.silent, HOLDER],
        [tokens.short, HOLDER],
        [tokens.reverting, HOLDER],
        [NO_CODE, HOLDER],
      ]);
      expect(balances).toEqual([
        { known: true, amount: 1234n },
        { known: true, amount: 0n },
        // More than a word came back: the first word is the answer.
        { known: true, amount: 7n },
        // The call succeeded and returned nothing.
        { known: false, amount: 0n },
        // The call succeeded and returned one byte.
        { known: false, amount: 0n },
        // The call failed, with more than a word of revert data.
        { known: false, amount: 0n },
        // An address with no code: the call succeeds and returns nothing.
        { known: false, amount: 0n },
      ]);
    });

    it("means what Yul means: with the operands swapped, the size is read before the call", async () => {
      const balances = await read(await compilePair(sizeBeforeCall), [
        [tokens.token, HOLDER],
        [tokens.wide, HOLDER],
      ]);
      // Nothing has returned yet when the size is read, so nothing is ever decoded.
      expect(balances).toEqual([
        { known: false, amount: 0n },
        { known: false, amount: 0n },
      ]);
    });
  });
});
