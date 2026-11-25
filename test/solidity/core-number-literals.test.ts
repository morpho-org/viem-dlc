/**
 * Integer literals that JavaScript and Solidity could read differently.
 *
 * A Number literal is a 64-bit float in JavaScript: above the safe integers it does not hold the value
 * it spells, and a compiler that lowers the module to older JavaScript prints the value it holds. So a
 * Number literal that is not a safe integer is refused in a body and in an assembly block, and asked
 * for as a bigint.
 */
import { beforeAll, describe, expect, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS } from "./helpers/core.js";
import { emitJavaScript } from "./helpers/emit.js";
import { buildSnippet, expectRejection, positionIn } from "./helpers/harness.js";

const UNSAFE = "is not a safe integer";

const pairs: Record<string, ExecutedPair> = {
  "the largest safe integer as a Number, and bigints above it": {
    ts: `${IMPORTS}
      import { add } from "@morpho-org/viem-dlc/solidity/yul";
      import { assembly } from "@morpho-org/viem-dlc/solidity";
      export const SafeLens = contract("SafeLens", {
        largestSafe: pure().returns(uint256, uint256),
        above: pure().returns(uint256, uint256),
        word: pure(uint256).returns(uint256),
      }).implementation({
        largestSafe: () => [9007199254740991, 0x1fffffffffffff],
        above: () => [9_007_199_254_740_993n, 0x20000000000001n],
        word(x) {
          const sum = assembly("memory-safe", () => add(add(x, 9007199254740991), 9007199254740993n));
          return sum;
        },
      });
    `,
    solidity: `
      contract SafeLens {
          function largestSafe() external pure returns (uint256, uint256) {
              return (9007199254740991, 0x1fffffffffffff);
          }

          function above() external pure returns (uint256, uint256) {
              return (9007199254740993, 0x20000000000001);
          }

          function word(uint256 x) external pure returns (uint256) {
              uint256 sum;
              assembly ("memory-safe") {
                  sum := add(add(x, 9007199254740991), 9007199254740993)
              }
              return sum;
          }
      }
    `,
    calls: [{ functionName: "largestSafe" }, { functionName: "above" }, { functionName: "word", args: [1n] }],
  },
};

describePairs("integer literals JavaScript and Solidity could read differently", pairs);

describe("a Number literal that is not a safe integer", () => {
  const lens = (literal: string) => `${IMPORTS}
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    return x + ${literal};
  },
});
`;

  for (const [name, literal] of [
    ["one above the largest safe integer", "9007199254740992"],
    ["an integer JavaScript rounds", "9007199254740993"],
    ["written with separators", "9_007_199_254_740_993"],
    ["written in hexadecimal", "0x20000000000001"],
    ["2 ** 256 - 1 written out", "115792089237316195423570985008687907853269984665640564039457584007913129639935"],
    ["a fraction", "1.5"],
    ["a fraction below one", ".5"],
    ["an exponent too large to be exact", "1e21"],
  ] as const)
    it(`is refused in a body: ${name}`, async () => {
      const code = lens(literal);
      const diagnostic = await expectRejection(code, { message: UNSAFE, ...positionIn(code, literal) });
      expect(diagnostic.message).toContain("n suffix");
    });

  it("is refused in an assembly block", async () => {
    const code = `${IMPORTS}
import { assembly } from "@morpho-org/viem-dlc/solidity";
import { add } from "@morpho-org/viem-dlc/solidity/yul";
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    const sum = assembly("memory-safe", () => add(x, 9007199254740993));
    return sum;
  },
});
`;
    const diagnostic = await expectRejection(code, { message: UNSAFE, ...positionIn(code, "9007199254740993") });
    expect(diagnostic.message).toContain("n suffix");
  });

  it("is refused in a helper another module declares, at the helper's literal", async () => {
    const helper = `import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
export const read = internal(pure().returns(uint256), () => 9007199254740993);
`;
    await expectRejection(
      {
        "lens.ts": `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { read } from "./helper.js";
export const Lens = contract("Lens", { f: pure().returns(uint256) }).implementation({ f: () => read() });
`,
        "helper.ts": helper,
      },
      { message: UNSAFE, file: "helper.ts", ...positionIn(helper, "9007199254740993") },
    );
  });
});

describe("a helper as tsc emits it for ES2020", () => {
  const lens = `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { read } from "./helper.js";
export const Lens = contract("Lens", { f: pure().returns(uint256) }).implementation({ f: () => read() });
`;
  const helper = (literal: string) => `import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
export const read = internal(pure().returns(uint256), () => ${literal});
`;
  const sources = { number: helper("9_007_199_254_740_993"), bigint: helper("9_007_199_254_740_993n") };
  let emitted: typeof sources;
  beforeAll(() => {
    emitted = emitJavaScript(sources, "es2020");
  });

  it("spells the value JavaScript holds for a Number, which is not the value the source spells", () => {
    expect(sources.number).toContain("9_007_199_254_740_993)");
    expect(emitted.number).toContain("9007199254740992)");
  });

  it("is refused for the Number, from the source and from the emitted JavaScript alike", async () => {
    await expectRejection(
      { "lens.ts": lens, "helper.ts": sources.number },
      { message: UNSAFE, file: "helper.ts", ...positionIn(sources.number, "9_007_199_254_740_993") },
    );
    await expectRejection(
      { "lens.ts": lens, "helper.js": emitted.number },
      { message: UNSAFE, file: "helper.js", ...positionIn(emitted.number, "9007199254740992") },
    );
  });

  it("is the same Solidity and the same bytecode for the bigint", async () => {
    expect(emitted.bigint).toContain("9007199254740993n)");
    const fromSource = await buildSnippet({ "lens.ts": lens, "helper.ts": sources.bigint });
    const fromEmitted = await buildSnippet({ "lens.ts": lens, "helper.js": emitted.bigint });
    expect(fromSource.lenses[0]?.printed.solidity).toContain("return 9007199254740993;");
    expect(fromEmitted.lenses[0]?.printed.solidity).toBe(fromSource.lenses[0]?.printed.solidity);
    expect(fromEmitted.lenses[0]?.artifact.bytecode).toBe(fromSource.lenses[0]?.artifact.bytecode);
  });
});
