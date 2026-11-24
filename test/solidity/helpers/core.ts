/**
 * Fixture pairs that are both compared and executed, for the core*.test.ts files.
 *
 * A pair here carries its calls. Each pair gets two tests: the printed Solidity has the AST of the
 * handwritten Solidity, and both give the same bytes on every call. A call says whether it reverts, so
 * a vector that is meant to reach a revert is known to reach it.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "./anvil.js";
import {
  type CompiledPair,
  compilePair,
  expectSameAst,
  expectSameExecution,
  type FixtureCall,
  type FixturePair,
} from "./harness.js";

export interface ExecutedCall extends FixtureCall {
  /** True when the call is meant to revert. Both sides must then revert with the same bytes. */
  reverts?: boolean;
}

export interface ExecutedPair extends FixturePair {
  calls: readonly ExecutedCall[];
}

/** The usual imports of a snippet, so a fixture shows only what it is about. */
export const IMPORTS = `import {
  address, array, bool, bytes, bytes4, bytes32, contract, int256, internal, pure, require, revert, string, Struct,
  uint8, uint64, uint128, uint256, view,
} from "@morpho-org/viem-dlc/solidity";`;

export const MAX_UINT256 = 2n ** 256n - 1n;

/** One Anvil for the tests of a file. Undefined where Anvil is not installed and the run is not CI. */
export function useEvm(): () => Evm {
  let evm: Evm | undefined;
  beforeAll(async () => {
    if (requireAnvil()) evm = await startAnvil();
  });
  afterAll(() => evm?.stop());
  return () => {
    if (!evm) throw new Error("anvil is not running");
    return evm;
  };
}

/** Registers the two tests of every pair. */
export function describePairs(title: string, pairs: Readonly<Record<string, ExecutedPair>>): void {
  describe(title, () => {
    const evm = useEvm();
    const compiled = new Map<string, Promise<CompiledPair>>();
    const compile = (name: string, pair: ExecutedPair): Promise<CompiledPair> => {
      const known = compiled.get(name) ?? compilePair(pair);
      compiled.set(name, known);
      return known;
    };

    for (const [name, pair] of Object.entries(pairs)) {
      it(`${name}: the printed Solidity is the handwritten Solidity`, async () => {
        expectSameAst(await compile(name, pair));
      });

      it.skipIf(!requireAnvil())(`${name}: both execute to the same bytes`, async () => {
        const outcomes = await expectSameExecution(evm(), await compile(name, pair), pair.calls);
        expect(outcomes.map((outcome) => outcome.status)).toEqual(
          pair.calls.map((call) => (call.reverts ? "revert" : "success")),
        );
      });
    }
  });
}
