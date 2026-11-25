/**
 * Several return values, as Solidity tuples: `return [a, b]`, `const [a, , c] = f(x)` and
 * `[r.a, r.b] = f(x)`.
 */
import { describe, it } from "vitest";

import { describePairs, type ExecutedPair, IMPORTS, MAX_UINT256 } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

const pairs: Record<string, ExecutedPair> = {
  "returning, declaring and assigning several values": {
    ts: `${IMPORTS}
      class Range extends Struct({ low: uint256, high: uint256, width: uint128 }) {}

      // An expression body that returns several values.
      const divMod = internal(pure(uint256, uint256).returns(uint256, uint256), (x, y) => [x / y, x % y]);
      const ordered = internal(pure(uint256, uint256).returns(uint256, uint256), (a, b) => {
        if (a < b) return [a, b];
        return [b, a];
      });
      // Values of different types, one of them a reference type.
      const describeAmount = internal(pure(uint256).returns(uint128, bool, string), (x) => [
        uint128(x),
        x > 1000n,
        "amount",
      ]);

      export const TupleLens = contract("TupleLens", {
        divMod: pure(uint256, uint256).returns(uint256, uint256),
        quotient: pure(uint256, uint256).returns(uint256),
        remainder: pure(uint256, uint256).returns(uint256),
        range: pure(uint256, uint256).returns(Range),
        label: pure(uint256).returns(string, bool),
        rotate: pure([uint256]).returns([uint256]),
      }).implementation({
        // Each local takes the type the helper declares for that value.
        divMod(x, y) {
          const [q, r] = divMod(x, y);
          const quotient = q;
          return [quotient, r];
        },
        // A value that is not used is a hole, in any position.
        quotient(x, y) {
          const [q, ,] = divMod(x, y);
          return q;
        },
        remainder(x, y) {
          const [, r] = divMod(x, y);
          return r;
        },
        // Assigning to fields, with a hole, and to a local declared before.
        range(a, b) {
          const o = new Range();
          [o.low, o.high] = ordered(a, b);
          let high = 0n;
          [, high] = ordered(a, b);
          // A hole in the last place takes its own comma: [a, ,] has two places, and [a, , ,] three.
          [o.width, , ,] = describeAmount(high - o.low);
          return o;
        },
        label(x) {
          const [, large, text] = describeAmount(x);
          return [text, large];
        },
        // Assigning to elements of an array.
        rotate(xs) {
          const out = array(uint256, 2n);
          [out[1], out[0]] = ordered(xs[0]!, xs[1]!);
          return out;
        },
      });
    `,
    solidity: `
      struct Range {
          uint256 low;
          uint256 high;
          uint128 width;
      }

      contract TupleLens {
          function divMod(uint256 x, uint256 y) external pure returns (uint256, uint256) {
              (uint256 q, uint256 r) = divMod_2(x, y);
              uint256 quotient = q;
              return (quotient, r);
          }

          function quotient(uint256 x, uint256 y) external pure returns (uint256) {
              (uint256 q, ) = divMod_2(x, y);
              return q;
          }

          function remainder(uint256 x, uint256 y) external pure returns (uint256) {
              (, uint256 r) = divMod_2(x, y);
              return r;
          }

          function range(uint256 a, uint256 b) external pure returns (Range memory) {
              Range memory o;
              (o.low, o.high) = ordered(a, b);
              uint256 high = 0;
              (, high) = ordered(a, b);
              (o.width, , ) = describeAmount(high - o.low);
              return o;
          }

          function label(uint256 x) external pure returns (string memory, bool) {
              (, bool large, string memory text) = describeAmount(x);
              return (text, large);
          }

          function rotate(uint256[] calldata xs) external pure returns (uint256[] memory) {
              uint256[] memory out = new uint256[](2);
              (out[1], out[0]) = ordered(xs[0], xs[1]);
              return out;
          }

          function divMod_2(uint256 x, uint256 y) internal pure returns (uint256, uint256) {
              return (x / y, x % y);
          }

          function ordered(uint256 a, uint256 b) internal pure returns (uint256, uint256) {
              if (a < b) return (a, b);
              return (b, a);
          }

          function describeAmount(uint256 x) internal pure returns (uint128, bool, string memory) {
              return (uint128(x), x > 1000, "amount");
          }
      }
    `,
    calls: [
      { functionName: "divMod", args: [17n, 5n] },
      { functionName: "divMod", args: [17n, 0n], reverts: true },
      { functionName: "quotient", args: [MAX_UINT256, 3n] },
      { functionName: "remainder", args: [MAX_UINT256, 7n] },
      { functionName: "range", args: [9n, 4n] },
      { functionName: "range", args: [4n, 9n] },
      { functionName: "range", args: [0n, MAX_UINT256] },
      { functionName: "label", args: [1000n] },
      { functionName: "label", args: [1001n] },
      { functionName: "rotate", args: [[3n, 8n]] },
      { functionName: "rotate", args: [[8n, 3n]] },
      { functionName: "rotate", args: [[8n]], reverts: true },
    ],
  },
};

describePairs("several return values", pairs);

describe("several return values: rejections", () => {
  const lens = (body: string, signature = "pure(uint256).returns(uint256, uint256)") => `${IMPORTS}
const divMod = internal(pure(uint256, uint256).returns(uint256, uint256), (x, y) => [x / y, x % y]);
const check = internal(pure(uint256), (x) => {
  require(x > 0n);
});
export const Lens = contract("Lens", { f: ${signature} }).implementation({
  f(x) {
    ${body}
  },
});
`;

  it("one value is returned without the brackets", async () => {
    const code = lens("return [x];");
    await expectRejection(code, {
      message: "one value is returned without the brackets",
      ...positionIn(code, "[x]"),
    });
  });

  it("a returned value cannot be left out", async () => {
    const code = lens("return [x, , x];");
    await expectRejection(code, {
      message: "every value of return [a, b] is an expression",
      ...positionIn(code, "[x, , x]"),
    });
  });

  it("a returned value cannot be a spread", async () => {
    const code = lens("return [x, ...[x]];");
    await expectRejection(code, {
      message: "every value of return [a, b] is an expression",
      ...positionIn(code, "...[x]"),
    });
  });

  it("several values outside a return have no Solidity form", async () => {
    const code = lens("let a = 0n;\n    let b = 1n;\n    [a, b] = [b, a];\n    return [a, b];");
    await expectRejection(code, {
      message: "several values are assigned from one call whose return types are declared",
      ...positionIn(code, "[b, a]"),
    });
  });

  it("a pattern does not take the one value of a call", async () => {
    const code = `${IMPORTS}
const word = internal(pure().returns(string), () => "abc");
export const Lens = contract("Lens", { f: pure().returns(string) }).implementation({
  f() {
    const [letter] = word();
    return letter;
  },
});
`;
    await expectRejection(code, {
      message: "a pattern takes the values of a call that returns several, and this call returns 1",
      ...positionIn(code, "word();"),
    });
  });

  it("a pattern is not assigned the one value of a call", async () => {
    const code = `${IMPORTS}
const word = internal(pure().returns(string), () => "abc");
export const Lens = contract("Lens", { f: pure().returns(string) }).implementation({
  f() {
    let letter = "";
    [letter] = word();
    return letter;
  },
});
`;
    await expectRejection(code, {
      message: "a pattern takes the values of a call that returns several, and this call returns 1",
      ...positionIn(code, "word();"),
    });
  });

  it("a pattern is not assigned a value that is not a call", async () => {
    const code = `${IMPORTS}
export const Lens = contract("Lens", { f: pure().returns(string) }).implementation({
  f() {
    let letter = "";
    [letter] = "abc";
    return letter;
  },
});
`;
    await expectRejection(code, {
      message: "several values are assigned from one call whose return types are declared",
      ...positionIn(code, '"abc";'),
    });
  });

  it("a pattern is not assigned from a call whose return types are not in the source", async () => {
    const code = lens("let q = 0n;\n    let r = 0n;\n    [q, r] = require(x > 0n);\n    return [q, r];");
    await expectRejection(code, {
      message: "several values are assigned from one call whose return types are declared",
      ...positionIn(code, "require(x > 0n);\n    return"),
    });
  });

  it("an assigned pattern has one place for each value the call returns", async () => {
    const more = lens("let q = 0n;\n    [q, q, q] = divMod(x, 2n);\n    return [q, q];");
    await expectRejection(more, {
      message: "this pattern has 3 places and the call returns 2 values",
      ...positionIn(more, "[q, q, q]"),
    });
  });

  it("several locals are declared from a call", async () => {
    const code = lens("const [a, b] = x;\n    return [a, b];");
    await expectRejection(code, {
      message: "several locals are declared from one call whose return types are declared",
      ...positionIn(code, "x;"),
    });
  });

  it("several locals are declared from a call whose return types are in the source", async () => {
    const code = lens("const [a, b] = require(x > 0n);\n    return [a, b];");
    await expectRejection(code, {
      message: "several locals are declared from one call whose return types are declared",
      ...positionIn(code, "require(x > 0n);\n    return"),
    });
  });

  it("a pattern has one place for each value the call returns", async () => {
    const code = lens("const [q] = divMod(x, 2n);\n    return [q, q];");
    await expectRejection(code, {
      message: "this pattern has 1 places and the call returns 2 values",
      ...positionIn(code, "[q]"),
    });
  });

  it("a pattern does not have more places than the call returns values", async () => {
    const code = lens("const [q, r, s] = divMod(x, 2n);\n    return [q, r + s];");
    await expectRejection(code, {
      message: "this pattern has 3 places and the call returns 2 values",
      ...positionIn(code, "[q, r, s]"),
    });
  });

  it("a place of a pattern is a plain name", async () => {
    const code = lens("const [q, [r]] = divMod(x, 2n);\n    return [q, r];");
    await expectRejection(code, { message: "a local must be a plain name", ...positionIn(code, "[r]") });
  });

  it("a place of a pattern has no default", async () => {
    const code = lens("const [q, r = 1n] = divMod(x, 2n);\n    return [q, r];");
    await expectRejection(code, { message: "a local must be a plain name", ...positionIn(code, "r = 1n") });
  });

  it("a local is not declared from the fields of an object", async () => {
    const code = lens("const { q } = divMod(x, 2n);\n    return [q, q];");
    await expectRejection(code, { message: "a local must be a plain name", ...positionIn(code, "{ q }") });
  });

  it("one local does not take several values", async () => {
    const code = lens("const q = divMod(x, 2n);\n    return [q, q];");
    await expectRejection(code, {
      message: "a local takes one value and this call returns 2",
      ...positionIn(code, "divMod(x, 2n)"),
    });
  });

  it("a local does not take the value of a call that returns none", async () => {
    const code = lens("const q = check(x);\n    return [q, q];");
    await expectRejection(code, {
      message: "a local takes one value and this call returns 0",
      ...positionIn(code, "check(x)"),
    });
  });

  it("a target of an assignment cannot gather the rest", async () => {
    const code = lens("let q = 0n;\n    let r = 0n;\n    [q, ...r] = divMod(x, 2n);\n    return [q, q];");
    await expectRejection(code, {
      message: "cannot contain this expression (RestElement)",
      ...positionIn(code, "...r"),
    });
  });

  it("an expression body that returns nothing cannot be several values", async () => {
    const code = `${IMPORTS}
const pair = internal(pure(uint256), (x) => [x, x]);
export const Lens = contract("Lens", { f: pure(uint256) }).implementation({
  f(x) {
    pair(x);
  },
});
`;
    await expectRejection(code, { message: "this expression is not a statement", ...positionIn(code, "[x, x]") });
  });

  it("solc refuses a pattern that takes no value, at the TypeScript position", async () => {
    const code = lens("const [, ,] = divMod(x, 2n);\n    return [x, x];");
    await expectRejection(code, {
      message: "Expected primary expression.",
      ...positionIn(code, "const [, ,]"),
      source: "solc",
    });
  });

  it("the targets of an assignment are counted before solc sees them", async () => {
    const code = lens("let q = 0n;\n    [q] = divMod(x, 2n);\n    return [q, q];");
    await expectRejection(code, {
      message: "this pattern has 1 places and the call returns 2 values",
      ...positionIn(code, "[q]"),
      source: "transform",
    });
  });

  it("solc counts the returned values, at the TypeScript position", async () => {
    const code = lens("return [x, x, x];");
    await expectRejection(code, {
      message: "Different number of arguments in return statement than in returns declaration.",
      ...positionIn(code, "return [x, x, x]"),
      source: "solc",
    });
  });
});
