/**
 * Typed Yul: the names in a block. A function inside a block names its value in Yul, where TypeScript
 * names nothing, and that name is one the block does not use.
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
