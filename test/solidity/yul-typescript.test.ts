/**
 * Typed Yul as TypeScript writes it: what is only TypeScript is erased inside a block as it is outside
 * one, and what solc refuses about a name is reported at that name.
 */
import { beforeAll, describe, expect, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS } from "./helpers/core.js";
import { emitJavaScript } from "./helpers/emit.js";
import { buildSnippet, expectRejection, positionIn } from "./helpers/harness.js";

const HEAD = `${IMPORTS}
import { assembly } from "@morpho-org/viem-dlc/solidity";
import { add, gt, lt, mload, mstore, mul } from "@morpho-org/viem-dlc/solidity/yul";
`;

const erased = `${HEAD}
export const ErasedLens = contract("ErasedLens", {
  tripled: pure(uint256).returns(uint256),
  stored: pure(uint256).returns(uint256),
}).implementation({
  tripled(x) {
    // A this parameter and types declared in the block, in a function, in a branch and after the return.
    const value = assembly("memory-safe", function (this: void) {
      type Word = bigint;
      interface Unused {
        word: Word;
      }
      const n: Word = 3n;
      const triple = (y: Word): Word => {
        type Inner = Word;
        return mul(y, 3n);
        type AfterInner = Inner;
      };
      let sum: Word = add(triple(x), n);
      if (gt(x, 100n)) {
        type Branch = Word;
        sum = add(sum, 1n);
      }
      return sum;
      type After = Unused;
    });
    return value;
  },
  stored(x) {
    // A block that is a statement: the type declared last does not make it a value, or hide that it is not.
    assembly("memory-safe", function (this: void) {
      mstore(0n, x);
      type Last = bigint;
    });
    const word = assembly("memory-safe", () => mload(0n));
    return word;
  },
});
`;

const pairs: Record<string, ExecutedPair> = {
  "a block that declares types and takes a this parameter": {
    ts: erased,
    solidity: `
      contract ErasedLens {
          function tripled(uint256 x) external pure returns (uint256) {
              uint256 value;
              assembly ("memory-safe") {
                  let n := 3
                  function triple(y) -> r {
                      r := mul(y, 3)
                  }
                  let sum := add(triple(x), n)
                  if gt(x, 100) {
                      sum := add(sum, 1)
                  }
                  value := sum
              }
              return value;
          }

          function stored(uint256 x) external pure returns (uint256) {
              assembly ("memory-safe") {
                  mstore(0, x)
              }
              uint256 word;
              assembly ("memory-safe") {
                  word := mload(0)
              }
              return word;
          }
      }
    `,
    calls: [
      { functionName: "tripled", args: [0n] },
      { functionName: "tripled", args: [3n] },
      { functionName: "tripled", args: [101n] },
      { functionName: "stored", args: [7n] },
    ],
  },
};

describePairs("typed Yul as TypeScript writes it", pairs);

describe("typed Yul: what is only TypeScript", () => {
  let javascript: string;
  beforeAll(() => {
    javascript = emitJavaScript({ lens: erased }).lens;
  });

  it("is gone from the JavaScript a compiler makes of the block", () => {
    expect(erased).toMatch(/this: void|type Word|interface Unused|: Word/);
    expect(javascript).not.toMatch(/this: void|type \w+ =|interface Unused|: Word/);
    expect(javascript).toContain("function ()");
  });

  it("prints the same Solidity from the TypeScript and from that JavaScript", async () => {
    const fromTypescript = await buildSnippet({ "lens.ts": erased });
    const fromJavascript = await buildSnippet({ "lens.js": javascript });
    expect(fromTypescript.lenses[0]?.printed.solidity).toBe(fromJavascript.lenses[0]?.printed.solidity);
    expect(fromTypescript.lenses[0]?.artifact.bytecode).toBe(fromJavascript.lenses[0]?.artifact.bytecode);
  });

  it("does not make a parameter of a block's callback acceptable", async () => {
    const code = `${HEAD}
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    const value = assembly("memory-safe", function (this: void, y: bigint) {
      return add(x, y);
    });
    return value;
  },
});
`;
    await expectRejection(code, {
      message: "the callback of an assembly block takes no parameters",
      ...positionIn(code, "y: bigint"),
    });
  });

  it("does not make a return that is not the last statement acceptable", async () => {
    const code = `${HEAD}
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    assembly("memory-safe", () => {
      return x;
      type After = bigint;
      mstore(0n, x);
    });
    return x;
  },
});
`;
    await expectRejection(code, {
      message: "return word is the last statement of an assembly block",
      ...positionIn(code, "return x;"),
    });
  });
});

describe("typed Yul: what solc refuses about a name, at that name", () => {
  const SHADOWS = "This declaration shadows a declaration outside the inline assembly block.";
  const lens = (block: string) => `${HEAD}
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    const value = assembly("memory-safe", () => {
${block}
    });
    return value;
  },
});
`;

  it("the second local of one declaration", async () => {
    const code = lens("      const first = 1n,\n        x = 2n;\n      return first;");
    await expectRejection(code, { message: SHADOWS, ...positionIn(code, "x = 2n"), source: "solc" });
  });

  it("the first local of one declaration", async () => {
    const code = lens("      const x = 1n,\n        second = 2n;\n      return second;");
    await expectRejection(code, { message: SHADOWS, ...positionIn(code, "x = 1n"), source: "solc" });
  });

  it("a parameter of a function", async () => {
    const code = lens("      const pick = (y, mod) => y;\n      return pick(1n, 2n);");
    await expectRejection(code, {
      message: 'Cannot use builtin function name "mod" as identifier name.',
      ...positionIn(code, "mod) =>"),
      source: "solc",
    });
  });

  it("the name of a function", async () => {
    const code = lens("      const one = 1n,\n        sub = () => 2n;\n      return one;");
    await expectRejection(code, {
      message: 'Cannot use builtin function name "sub" as identifier name.',
      ...positionIn(code, "sub = () =>"),
      source: "solc",
    });
  });
});
