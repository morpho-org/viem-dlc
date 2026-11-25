/**
 * Typed Yul: the names in a block. A function inside a block names its value in Yul, where TypeScript
 * names nothing, and that name is one the block does not use. A name that is called is a function declared
 * in the block, or an opcode.
 */
import { describe, expect, it } from "vitest";

import { describePairs } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

/** A lens whose one function has this body. */
const lens = (body: string) => `import { assembly, contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { add, add as plus, mload, mstore } from "@morpho-org/viem-dlc/solidity/yul";
export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({
  f(x) {
    ${body}
  },
});
`;

/** The handwritten contract, with this body. */
const handwritten = (body: string) => `
  contract Lens {
      function f(uint256 x) external pure returns (uint256) {
          ${body}
      }
  }
`;

const calls = [0n, 3n, 6n].map((x) => ({ functionName: "f", args: [x] }));

describePairs("the name a function inside a block gives its value", {
  "beside a parameter named r": {
    ts: lens(`const value = assembly("memory-safe", () => {
      const identity = (r) => r;
      return identity(x);
    });
    return value;`),
    solidity: handwritten(`uint256 value;
          assembly ("memory-safe") {
              function identity(r) -> r_ {
                  r_ := r
              }
              value := identity(x)
          }
          return value;`),
    calls,
  },
  "beside a Solidity local named r": {
    ts: lens(`const r = assembly("memory-safe", () => {
      const twice = (a) => add(a, a);
      return twice(x);
    });
    return r;`),
    solidity: handwritten(`uint256 r;
          assembly ("memory-safe") {
              function twice(a) -> r_ {
                  r_ := add(a, a)
              }
              r := twice(x)
          }
          return r;`),
    calls,
  },
  "beside Yul locals named r and r_": {
    ts: lens(`const value = assembly("memory-safe", () => {
      const r = add(x, 1n);
      const twice = (r_) => add(r_, r_);
      return twice(r);
    });
    return value;`),
    solidity: handwritten(`uint256 value;
          assembly ("memory-safe") {
              let r := add(x, 1)
              function twice(r_) -> r__ {
                  r__ := add(r_, r_)
              }
              value := twice(r)
          }
          return value;`),
    calls,
  },
  "inside a function that gives a value itself": {
    ts: lens(`const value = assembly("memory-safe", () => {
      const outer = (a) => {
        const inner = (b) => add(b, 1n);
        return inner(inner(a));
      };
      const other = (a) => add(a, 2n);
      return outer(other(x));
    });
    return value;`),
    solidity: handwritten(`uint256 value;
          assembly ("memory-safe") {
              function outer(a) -> r {
                  function inner(b) -> r_ {
                      r_ := add(b, 1)
                  }
                  r := inner(inner(a))
              }
              function other(a) -> r {
                  r := add(a, 2)
              }
              value := outer(other(x))
          }
          return value;`),
    calls,
  },
});

describe("a function inside a block does not read a local of the block, whatever the local is named", () => {
  // Yul gives a function none of the locals around it. The name the function's value is given must not
  // stand in for one of them.
  for (const name of ["r", "t"])
    it(`refuses a function that reads the later local ${name}, from solc`, async () => {
      const code = lens(`const value = assembly("memory-safe", () => {
      const get = () => ${name};
      const ${name} = x;
      return get();
    });
    return value;`);
      const diagnostic = await expectRejection(code, {
        message: `Identifier "${name}" not found.`,
        ...positionIn(code, `${name};\n      const`),
        source: "solc",
      });
      expect(diagnostic.code).toBe("8198");
    });

  it("refuses a function that reads the parameter r of the function around it, from solc", async () => {
    const code = lens(`const value = assembly("memory-safe", () => {
      const save = (r) => {
        const get = () => r;
        mstore(0, get());
      };
      save(x);
      return mload(0);
    });
    return value;`);
    const diagnostic = await expectRejection(code, {
      message: 'Identifier "r" not found.',
      ...positionIn(code, "r;\n        mstore"),
      source: "solc",
    });
    expect(diagnostic.code).toBe("8198");
  });
});

const CALL =
  "inside an assembly block a call is to an opcode imported from @morpho-org/viem-dlc/solidity/yul, or to a function declared in the block";

describe("a name that is called inside a block is a function declared in the block", () => {
  const cases: readonly (readonly [name: string, block: string, at: string])[] = [
    ["a Yul local that is a word", "const word = x;\n      return word(1n);", "word(1n)"],
    ["a Solidity local", "return x(1n);", "x(1n)"],
    ["a parameter of a function", "const apply = (g) => g(1n);\n      return apply(x);", "g(1n)"],
    [
      "a word that is nearer than the function of its name",
      "const twice = (a) => add(a, a);\n      const use = (twice) => twice(1n);\n      return use(twice(x));",
      "twice(1n)",
    ],
  ];
  for (const [name, block, at] of cases)
    it(`refuses a call of ${name}`, async () => {
      const code = lens(`const value = assembly("memory-safe", () => {\n      ${block}\n    });\n    return value;`);
      await expectRejection(code, { message: CALL, ...positionIn(code, at), source: "transform" });
    });
});

describePairs("what a block can call", {
  "a function declared further down, from a nested block and from another function, and an opcode under another name": {
    ts: lens(`const value = assembly("memory-safe", () => {
      let total = 0n;
      for (let i = 0n; twice(i) < x; i = plus(i, 1n)) {
        total = thrice(total);
      }
      const thrice = (a) => plus(twice(a), plus(a, 1n));
      const twice = (a) => add(a, a);
      return total;
    });
    return value;`),
    solidity: handwritten(`uint256 value;
          assembly ("memory-safe") {
              let total := 0
              for { let i := 0 } lt(twice(i), x) { i := add(i, 1) } {
                  total := thrice(total)
              }
              function thrice(a) -> r {
                  r := add(twice(a), add(a, 1))
              }
              function twice(a) -> r {
                  r := add(a, a)
              }
              value := total
          }
          return value;`),
    calls,
  },
});
