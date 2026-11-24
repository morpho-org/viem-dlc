/**
 * The fixture-pair harness on a few constructs. Each pair is a TypeScript snippet and the Solidity a
 * person wrote for it; see ./helpers/harness.ts for what is compared.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "./helpers/anvil.js";
import {
  buildSnippet,
  compilePair,
  expectRejection,
  expectSameAst,
  expectSameExecution,
  type FixturePair,
  positionIn,
} from "./helpers/harness.js";

const WAD = 10n ** 18n;
const MAX_UINT256 = 2n ** 256n - 1n;

const pairs = {
  "helpers, a module constant and arithmetic": {
    ts: `
      import { contract, internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

      const WAD = 10n ** 18n;
      const HALF_WAD = WAD - 5n * 10n ** 17n;

      const mulDivDown = internal(pure(uint256, uint256, uint256).returns(uint256), (x, y, d) => (x * y) / d);
      const mulDivUp = internal(pure(uint256, uint256, uint256).returns(uint256), (x, y, d) => (x * y + (d - 1n)) / d);

      export const MathLens = contract("MathLens", {
        wMulDown: pure(uint256, uint256).returns(uint256),
        wDivUp: pure(uint256, uint256).returns(uint256),
        halve: pure(uint256).returns(uint256),
      }).implementation({
        wMulDown: (x, y) => mulDivDown(x, y, WAD),
        wDivUp: (x, y) => mulDivUp(x, WAD, y),
        halve(x) {
          const half = mulDivDown(x, HALF_WAD, WAD);
          return half;
        },
      });
    `,
    solidity: `
      contract MathLens {
          function wMulDown(uint256 x, uint256 y) external pure returns (uint256) {
              return mulDivDown(x, y, 10 ** 18);
          }

          function wDivUp(uint256 x, uint256 y) external pure returns (uint256) {
              return mulDivUp(x, 10 ** 18, y);
          }

          function halve(uint256 x) external pure returns (uint256) {
              uint256 half = mulDivDown(x, 10 ** 18 - 5 * 10 ** 17, 10 ** 18);
              return half;
          }

          function mulDivDown(uint256 x, uint256 y, uint256 d) internal pure returns (uint256) {
              return x * y / d;
          }

          function mulDivUp(uint256 x, uint256 y, uint256 d) internal pure returns (uint256) {
              return (x * y + (d - 1)) / d;
          }
      }
    `,
    calls: [
      { functionName: "wMulDown", args: [3n * WAD, 2n * WAD] },
      { functionName: "wMulDown", args: [MAX_UINT256, 2n] },
      { functionName: "wDivUp", args: [1n, 3n * WAD] },
      { functionName: "wDivUp", args: [1n, 0n] },
      { functionName: "halve", args: [7n] },
    ],
  },
  "a struct, locals with copied types, if and else": {
    ts: `
      import { bool, contract, Struct, uint128, uint256, view } from "@morpho-org/viem-dlc/solidity";

      class Range extends Struct({ low: uint128, high: uint128 }) {}
      class Clamped extends Struct({ value: uint256, inside: bool, width: uint128 }) {}

      export const ClampLens = contract("ClampLens", {
        clamp: view(Range, uint256).returns(Clamped),
      }).implementation({
        clamp(range, x) {
          const o = new Clamped();
          const low = range.low;
          let high = range.high;
          const ordered = low <= high;
          if (!ordered) {
            high = low;
          }
          o.width = high - low;
          if (x < low) o.value = low;
          else if (x > high) o.value = high;
          else {
            o.value = x;
            o.inside = ordered && (x != low || x == high);
          }
          return o;
        },
      });
    `,
    solidity: `
      struct Range {
          uint128 low;
          uint128 high;
      }

      struct Clamped {
          uint256 value;
          bool inside;
          uint128 width;
      }

      contract ClampLens {
          function clamp(Range calldata range, uint256 x) external view returns (Clamped memory) {
              Clamped memory o;
              uint128 low = range.low;
              uint128 high = range.high;
              bool ordered = low <= high;
              if (!ordered) {
                  high = low;
              }
              o.width = high - low;
              if (x < low) o.value = low;
              else if (x > high) o.value = high;
              else {
                  o.value = x;
                  o.inside = ordered && (x != low || x == high);
              }
              return o;
          }
      }
    `,
    calls: [
      { functionName: "clamp", args: [{ low: 10n, high: 20n }, 15n] },
      { functionName: "clamp", args: [{ low: 10n, high: 20n }, 5n] },
      { functionName: "clamp", args: [{ low: 10n, high: 20n }, 25n] },
      { functionName: "clamp", args: [{ low: 10n, high: 20n }, 10n] },
      { functionName: "clamp", args: [{ low: 20n, high: 10n }, 15n] },
    ],
  },
  "require, revert, a conversion and a conditional": {
    ts: `
      import { contract, pure, require, revert, uint8, uint256 } from "@morpho-org/viem-dlc/solidity";

      const MAX_DECIMALS = 36n;

      export const ScaleLens = contract("ScaleLens", {
        scale: pure(uint256, uint256, uint256).returns(uint256),
        narrow: pure(uint256).returns(uint8),
      }).implementation({
        scale(amount, from, to) {
          require(from <= MAX_DECIMALS, "from");
          require(to <= MAX_DECIMALS);
          if (amount == 0n) revert("nothing to scale");
          const factor = uint256(10n ** (from > to ? from - to : to - from));
          return from > to ? amount / factor : amount * factor;
        },
        narrow(x) {
          if (x > 0xffn) revert();
          return uint8(x);
        },
      });
    `,
    solidity: `
      contract ScaleLens {
          function scale(uint256 amount, uint256 from, uint256 to) external pure returns (uint256) {
              require(from <= 36, "from");
              require(to <= 36);
              if (amount == 0) revert("nothing to scale");
              uint256 factor = uint256(10 ** (from > to ? from - to : to - from));
              return from > to ? amount / factor : amount * factor;
          }

          function narrow(uint256 x) external pure returns (uint8) {
              if (x > 0xff) revert();
              return uint8(x);
          }
      }
    `,
    calls: [
      { functionName: "scale", args: [1_000_000n, 6n, 18n] },
      { functionName: "scale", args: [10n ** 18n, 18n, 6n] },
      { functionName: "scale", args: [1n, 37n, 6n] },
      { functionName: "scale", args: [1n, 6n, 37n] },
      { functionName: "scale", args: [0n, 6n, 18n] },
      { functionName: "scale", args: [MAX_UINT256, 0n, 1n] },
      { functionName: "narrow", args: [255n] },
      { functionName: "narrow", args: [256n] },
    ],
  },
} satisfies Record<string, FixturePair & { calls: unknown }>;

describe("fixture pairs", () => {
  describe("the printed Solidity is the handwritten Solidity", () => {
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
        // Every pair's vectors reach a return, and the ones with a revert reach it.
        expect(outcomes.some((outcome) => outcome.status === "success")).toBe(true);
        if (name !== "a struct, locals with copied types, if and else")
          expect(outcomes.some((outcome) => outcome.status === "revert")).toBe(true);
      });

    it("reverts carry Solidity's own data", async () => {
      const outcomes = await expectSameExecution(
        evm,
        await compilePair(pairs["require, revert, a conversion and a conditional"]),
        [
          { functionName: "scale", args: [1n, 37n, 6n] },
          { functionName: "scale", args: [MAX_UINT256, 0n, 1n] },
          { functionName: "narrow", args: [256n] },
        ],
      );
      // Error("from"), Panic(0x11) for the overflowing product, and an empty revert.
      expect(outcomes.map((outcome) => outcome.data.slice(0, 10))).toEqual(["0x08c379a0", "0x4e487b71", "0x"]);
    });
  });
});

describe("the harness tells two different programs apart", () => {
  const ts = `
    import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
    export const Lens = contract("Lens", { f: pure(uint256, uint256, uint256).returns(uint256) }).implementation({
      f: (x, y, d) => (x * y) / d,
    });
  `;
  const solidity = (body: string) => `
    contract Lens {
        function f(uint256 x, uint256 y, uint256 d) external pure returns (uint256) {
            return ${body};
        }
    }
  `;

  it("by the tree: parentheses that change the tree are a different AST", async () => {
    expectSameAst(await compilePair({ ts, solidity: solidity("x * y / d") }));
    expectSameAst(await compilePair({ ts, solidity: solidity("((x * y)) / d") }));
    const regrouped = await compilePair({ ts, solidity: solidity("x * (y / d)") });
    expect(() => expectSameAst(regrouped)).toThrow();
    const retyped = await compilePair({ ts, solidity: solidity("uint256(x * y) / d") });
    expect(() => expectSameAst(retyped)).toThrow();
  });

  it.skipIf(!requireAnvil())("by the bytes: a different result is a failure", async () => {
    const evm = await startAnvil();
    try {
      const regrouped = await compilePair({ ts, solidity: solidity("x * (y / d)") });
      await expectSameExecution(evm, regrouped, [{ functionName: "f", args: [4n, 6n, 2n] }]);
      await expect(expectSameExecution(evm, regrouped, [{ functionName: "f", args: [3n, 3n, 2n] }])).rejects.toThrow();
    } finally {
      evm.stop();
    }
  });
});

describe("what the transform prints", () => {
  it("is a literal expression for a module constant, in place and unevaluated", async () => {
    const built = await buildSnippet(pairs["helpers, a module constant and arithmetic"].ts);
    const solidity = built.lenses[0]?.printed.solidity;
    expect(solidity).toContain("mulDivDown(x, ((10 ** 18) - (5 * (10 ** 17))), (10 ** 18))");
    expect(solidity).not.toContain("500000000000000000");
  });

  it("resolves a helper and a constant through another module, and reports the file", async () => {
    const built = await buildSnippet({
      "lens.ts": `
        import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
        import * as math from "./math.js";
        import { wMulDown as mul } from "./index.js";

        export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
          f: (x) => mul(x, math.WAD),
        });
      `,
      "index.ts": `export * from "./math.js";`,
      "math.ts": `
        import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
        export const WAD = 10n ** 18n;
        export const wMulDown = internal(pure(uint256, uint256).returns(uint256), (x, y) => (x * y) / WAD);
      `,
    });
    expect(built.lenses[0]?.printed.solidity).toContain("return wMulDown(x, (10 ** 18));");
    expect(built.dependencies.map((file) => file.split("/").at(-1)).sort()).toEqual([
      "index.ts",
      "math.ts",
      "solidity.config.json",
    ]);
  });
});

describe("rejections", () => {
  const lens = (
    body: string,
    head = "",
  ) => `import { contract, pure, uint128, uint256 } from "@morpho-org/viem-dlc/solidity";
${head}
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    ${body}
  },
});
`;

  it("a constant a lens reads cannot use /, at the operator", async () => {
    const code = lens("return x * RATE;", "const RATE = 10n ** 18n / 3n;");
    await expectRejection(code, { message: "cannot use /", ...positionIn(code, "/ 3n"), source: "transform" });
  });

  it("a constant a lens reads cannot call anything", async () => {
    const code = lens("return x * RATE;", "const RATE = BigInt(process.env.RATE ?? 1);");
    await expectRejection(code, { message: "a call", ...positionIn(code, "BigInt(process") });
  });

  it("a lens can only read a module-level const", async () => {
    const code = lens("return x * rate;", "let rate = 2n;");
    await expectRejection(code, { message: "declared with let", ...positionIn(code, "rate = 2n") });
  });

  it("a name that is not at module level", async () => {
    const code = lens("return x * missing;");
    await expectRejection(code, { message: '"missing" is not a module-level name', ...positionIn(code, "missing") });
  });

  it("a local whose initializer has no type in the source", async () => {
    const code = lens("const y = x * 2n;\n    return y;");
    await expectRejection(code, { message: "name the type with a conversion", ...positionIn(code, "x * 2n") });
  });

  it("a construct with no Solidity form", async () => {
    const code = lens("for (const y of [x]) return y;\n    return x;");
    await expectRejection(code, { message: "cannot contain this statement", ...positionIn(code, "for (") });
  });

  it("an error from solc, at the TypeScript it was printed from", async () => {
    const code = lens("const narrow = uint128(1n);\n    narrow = x;\n    return narrow;");
    const diagnostic = await expectRejection(code, {
      message: "Type uint256 is not implicitly convertible to expected type uint128.",
      ...positionIn(code, "x;"),
      source: "solc",
    });
    expect(diagnostic.lens).toBe("Lens");
  });
});
