/**
 * The rest of the printer's own rules: every operator and assignment, statements with nothing to
 * return, constants read through other modules, and what a body may not contain.
 */
import { describe, expect, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS, MAX_UINT256 } from "./helpers/core.js";
import { buildSnippet, expectRejection, positionIn } from "./helpers/harness.js";

const MAX_INT256 = 2n ** 255n - 1n;

const pairs: Record<string, ExecutedPair> = {
  "every operator, parenthesized as the tree is": {
    ts: `${IMPORTS}
      export const OperatorLens = contract("OperatorLens", {
        arithmetic: pure(uint256, uint256).returns(uint256),
        bitwise: pure(uint256, uint256).returns(uint256),
        signed: pure(int256, int256).returns(int256),
        precedence: pure(uint256, uint256, uint256).returns(uint256, uint256, bool),
      }).implementation({
        arithmetic: (x, y) => x + y - (x % y) * (x / y) + 2n ** y,
        bitwise: (x, y) => ((x << y) | (x >> y)) ^ (x & ~y),
        signed: (x, y) => -x + y * -1n - (x % y),
        // Parentheses are printed where the tree nests an operator, whatever the source wrote.
        precedence(a, b, c) {
          return [a + b * c, (a + b) * c, a < b == b < c || !(a > c) && a != c];
        },
      });
    `,
    solidity: `
      contract OperatorLens {
          function arithmetic(uint256 x, uint256 y) external pure returns (uint256) {
              return x + y - (x % y) * (x / y) + 2 ** y;
          }

          function bitwise(uint256 x, uint256 y) external pure returns (uint256) {
              return ((x << y) | (x >> y)) ^ (x & ~y);
          }

          function signed(int256 x, int256 y) external pure returns (int256) {
              return -x + y * -1 - (x % y);
          }

          function precedence(uint256 a, uint256 b, uint256 c) external pure returns (uint256, uint256, bool) {
              return (a + (b * c), (a + b) * c, (((a < b) == (b < c)) || ((!(a > c)) && (a != c))));
          }
      }
    `,
    calls: [
      { functionName: "arithmetic", args: [17n, 5n] },
      { functionName: "arithmetic", args: [17n, 0n], reverts: true },
      { functionName: "arithmetic", args: [3n, 256n], reverts: true },
      { functionName: "bitwise", args: [0xf0f0n, 4n] },
      { functionName: "bitwise", args: [MAX_UINT256, 255n] },
      { functionName: "bitwise", args: [1n, 300n] },
      { functionName: "signed", args: [-7n, 3n] },
      { functionName: "signed", args: [MAX_INT256, -1n] },
      { functionName: "signed", args: [-MAX_INT256 - 1n, 1n], reverts: true },
      { functionName: "precedence", args: [1n, 2n, 3n] },
      { functionName: "precedence", args: [3n, 2n, 1n] },
      { functionName: "precedence", args: [2n, 2n, 2n] },
    ],
  },
  "every assignment, and statements that return nothing": {
    ts: `${IMPORTS}
      class Counter extends Struct({ count: uint256, limit: uint256 }) {}

      // A helper with nothing to return: an expression body, a bare return, and a body that ends.
      const bump = internal(pure(Counter), (c) => c.count++);
      const bumpBelow = internal(pure(Counter), (c) => {
        if (c.count >= c.limit) return;
        bump(c);
      });

      export const AssignLens = contract("AssignLens", {
        compound: pure(uint256, uint256).returns(uint256),
        bits: pure(uint256, uint256).returns(uint256),
        steps: pure(uint256).returns(uint256, uint256, uint256),
        bumped: pure(uint256, uint256).returns(Counter),
      }).implementation({
        compound(x, y) {
          let z = x;
          z += y;
          z -= 1n;
          z *= y;
          z /= 2n;
          z %= 1000n;
          return z;
        },
        bits(x, y) {
          let z = x;
          z <<= 2n;
          z >>= 1n;
          z |= y;
          z &= 0xffffn;
          z ^= x;
          return z;
        },
        // Before and after, as a statement and as a value.
        steps(x) {
          let before = x;
          let later = x;
          ++before;
          later++;
          --before;
          later--;
          const taken = uint256(before++ + ++later);
          return [before, later, taken];
        },
        bumped(count, limit) {
          const c = new Counter();
          c.count = count;
          c.limit = limit;
          bumpBelow(c);
          bumpBelow(c);
          return c;
        },
      });
    `,
    solidity: `
      struct Counter {
          uint256 count;
          uint256 limit;
      }

      contract AssignLens {
          function compound(uint256 x, uint256 y) external pure returns (uint256) {
              uint256 z = x;
              z += y;
              z -= 1;
              z *= y;
              z /= 2;
              z %= 1000;
              return z;
          }

          function bits(uint256 x, uint256 y) external pure returns (uint256) {
              uint256 z = x;
              z <<= 2;
              z >>= 1;
              z |= y;
              z &= 0xffff;
              z ^= x;
              return z;
          }

          function steps(uint256 x) external pure returns (uint256, uint256, uint256) {
              uint256 before = x;
              uint256 later = x;
              ++before;
              later++;
              --before;
              later--;
              uint256 taken = uint256(before++ + ++later);
              return (before, later, taken);
          }

          function bumped(uint256 count, uint256 limit) external pure returns (Counter memory) {
              Counter memory c;
              c.count = count;
              c.limit = limit;
              bumpBelow(c);
              bumpBelow(c);
              return c;
          }

          function bump(Counter memory c) internal pure {
              c.count++;
          }

          function bumpBelow(Counter memory c) internal pure {
              if (c.count >= c.limit) return;
              bump(c);
          }
      }
    `,
    calls: [
      { functionName: "compound", args: [10n, 7n] },
      { functionName: "compound", args: [0n, 0n], reverts: true },
      { functionName: "compound", args: [MAX_UINT256, 1n], reverts: true },
      { functionName: "bits", args: [0x1234n, 0xff00n] },
      { functionName: "bits", args: [MAX_UINT256, 0n] },
      { functionName: "steps", args: [5n] },
      { functionName: "steps", args: [MAX_UINT256], reverts: true },
      { functionName: "bumped", args: [0n, 1n] },
      { functionName: "bumped", args: [0n, 5n] },
      { functionName: "bumped", args: [5n, 5n] },
    ],
  },
  "constants read through other modules, negative and inverted": {
    ts: {
      "lens.ts": `${IMPORTS}
        import * as units from "./units.js";
        import { LIMIT, SHIFTED as MOVED } from "./units.js";

        export const ConstantLens = contract("ConstantLens", {
          scaled: pure(uint256).returns(uint256, uint256),
          masked: pure(int256).returns(int256),
          offset: pure(int256).returns(int256),
        }).implementation({
          // A local declared from a constant is a uint256, however the constant is reached.
          scaled(x) {
            const scale = units.SCALE;
            const limit = LIMIT;
            return [x * scale, limit];
          },
          masked: (x) => (x & units.MASK) | MOVED,
          offset: (x) => x + units.FLOOR,
        });
      `,
      "units.ts": `
        const BASE = 10n;
        export const SCALE = BASE ** 6n;
        export const LIMIT = SCALE * 1_000n - 1n;
        export const MASK = ~0xffn;
        export const SHIFTED = (1n << 4n) % 7n;
        export const FLOOR = -(2n ** 8n) + (5n & 3n | 8n ^ 1n) - (16n >> 2n);
      `,
    },
    solidity: `
      contract ConstantLens {
          function scaled(uint256 x) external pure returns (uint256, uint256) {
              uint256 scale = 10 ** 6;
              uint256 limit = 10 ** 6 * 1000 - 1;
              return (x * scale, limit);
          }

          function masked(int256 x) external pure returns (int256) {
              return (x & ~0xff) | ((1 << 4) % 7);
          }

          function offset(int256 x) external pure returns (int256) {
              return x + (-(2 ** 8) + (((5 & 3) | (8 ^ 1))) - (16 >> 2));
          }
      }
    `,
    calls: [
      { functionName: "scaled", args: [3n] },
      { functionName: "scaled", args: [MAX_UINT256], reverts: true },
      { functionName: "masked", args: [0xabcdn] },
      { functionName: "offset", args: [0n] },
      { functionName: "offset", args: [1000n] },
    ],
  },
};

describePairs("the printer's own rules", pairs);

describe("the printer's own rules: what is printed", () => {
  it("is a constant's own expression, in place and in parentheses, from the module that declares it", async () => {
    const built = await buildSnippet(pairs["constants read through other modules, negative and inverted"]?.ts ?? "");
    const solidity = built.lenses[0]?.printed.solidity;
    expect(solidity).toContain("uint256 scale = ((10) ** 6);");
    expect(solidity).toContain("uint256 limit = ((((10) ** 6) * 1000) - 1);");
    expect(solidity).toContain("return (x & (~0xff)) | ((1 << 4) % 7);");
  });
});

describe("the printer's own rules: rejections", () => {
  const lens = (body: string, parameters = "x") => `${IMPORTS}
class Pair extends Struct({ low: uint128, high: uint128 }) {}
const half = internal(pure(uint256).returns(uint256), (x) => x / 2n);
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(${parameters}) {
    ${body}
  },
});
`;

  it("an implementation takes the parameters its signature declares", async () => {
    const code = lens("return x + y;", "x, y");
    await expectRejection(code, {
      message: '"f" declares 1 parameters and its implementation takes 2',
      ...positionIn(code, "(x, y) {"),
    });
  });

  it("a parameter is a plain name, not a pattern", async () => {
    const code = lens("return low;", "{ low }");
    await expectRejection(code, { message: "a parameter must be a plain name", ...positionIn(code, "{ low }") });
  });

  it("a parameter has no default", async () => {
    const code = lens("return x;", "x = 1n");
    await expectRejection(code, { message: "a parameter must be a plain name", ...positionIn(code, "x = 1n") });
  });

  for (const [operator, expression] of [
    ["+", "+x"],
    ["typeof", "typeof x"],
    ["void", "void x"],
    [">>>", "x >>> 1n"],
    ["in", "x in half"],
    ["instanceof", "x instanceof Pair"],
    ["??", "x ?? 1n"],
    ["**=", "x **= 2n"],
    [">>>=", "x >>>= 2n"],
    ["&&=", "x &&= 2n"],
    ["||=", "x ||= 2n"],
    ["??=", "x ??= 2n"],
  ] as const)
    it(`a lens has no operator ${operator}`, async () => {
      const code = lens(`return half(${expression});`);
      await expectRejection(code, { message: `a lens has no operator ${operator}`, ...positionIn(code, expression) });
    });

  for (const [what, type, expression] of [
    ["a comma between expressions", "SequenceExpression", "x++, x"],
    ["an object", "ObjectExpression", "{ x }"],
    ["a function", "ArrowFunctionExpression", "() => x"],
    ["an optional read", "ChainExpression", "x?.low"],
    ["a class", "ClassExpression", "class {}"],
  ] as const)
    it(`${what} has no Solidity form`, async () => {
      const code = lens(`return half((${expression}));`);
      await expectRejection(code, {
        message: `a lens body cannot contain this expression (${type})`,
        ...positionIn(code, expression),
      });
    });

  for (const [what, type, statement] of [
    ["throw", "ThrowStatement", "throw x;"],
    ["switch", "SwitchStatement", "switch (x) { default: }"],
    ["a function declared in a body", "FunctionDeclaration", "function inner() {}"],
    ["a class declared in a body", "ClassDeclaration", "class Inner {}"],
  ] as const)
    it(`${what} has no Solidity form`, async () => {
      const code = lens(`${statement}\n    return x;`);
      await expectRejection(code, {
        message: `a lens body cannot contain this statement (${type})`,
        ...positionIn(code, statement),
      });
    });

  it("an enum declared in a body is not a name a lens can use", async () => {
    const code = lens("enum Side { Low, High }\n    return x;");
    await expectRejection(code, { message: '"Side" is not a module-level name', ...positionIn(code, "Side") });
  });

  it("a statement assigns or calls", async () => {
    const code = lens("x + 1n;\n    return x;");
    await expectRejection(code, { message: "this expression is not a statement", ...positionIn(code, "x + 1n") });
  });

  it("an expression body that returns nothing assigns or calls", async () => {
    const code = `${IMPORTS}
const noop = internal(pure(uint256), (x) => x + 1n);
export const Lens = contract("Lens", { f: pure(uint256) }).implementation({ f: (x) => noop(x) });
`;
    await expectRejection(code, { message: "this expression is not a statement", ...positionIn(code, "x + 1n") });
  });

  it("a call does not spread its arguments", async () => {
    const code = lens("return half(...[x]);");
    await expectRejection(code, { message: "a call cannot spread its arguments", ...positionIn(code, "...[x]") });
  });

  it("only what is declared can be called", async () => {
    const code = lens("return x(1n);");
    await expectRejection(code, {
      message: "a lens body can call helpers, interface functions, library functions and the package's built-ins",
      ...positionIn(code, "x(1n)"),
    });
  });

  for (const [what, initializer] of [
    ["null", "null"],
    ["a call of a local", "x(1n)"],
    ["a call of a struct class", "Pair(x)"],
    ["the operator ??", "x ?? 1n"],
    ["a class", 'class { "constructor"(private z: bigint) {} }'],
    ["a function", "(y: bigint) => y"],
  ] as const)
    it(`${what} has no type to copy`, async () => {
      const code = lens(`const y = ${initializer};\n    return x;`);
      await expectRejection(code, { message: "name the type with a conversion", ...positionIn(code, initializer) });
    });

  it("an object pattern that gathers the rest is not a local", async () => {
    const code = lens("const { low, ...rest } = x;\n    return low;");
    await expectRejection(code, { message: "a local must be a plain name", ...positionIn(code, "{ low, ...rest }") });
  });
});
