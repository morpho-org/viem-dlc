/**
 * Scopes: a name means its nearest binding. The transform reads module-level bindings, so a nearer one,
 * a parameter or a local of the code around a lens, is never taken for the module-level name it shares.
 */
import path from "node:path";

import { describe, expect, it } from "vitest";

import { build, find, parseModule } from "../../src/solidity/build/index.js";

import { describePairs } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

const PACKAGE = "@morpho-org/viem-dlc/solidity";
const FILE = path.join(import.meta.dirname, "__fixtures__", "lens.ts");

/** The lenses the transform finds in a module that imports nothing but the package. */
const lensesOf = async (code: string) =>
  (await build(code, FILE, { resolve: () => undefined, addDependency: () => {} }, { cache: false }))?.lenses.map(
    (lens) => lens.resolved.name,
  ) ?? null;

/** [what is refused, the module, where the error points, the message] */
type Case = readonly [name: string, code: string, at: string, message: string];

const refuses = (cases: readonly Case[]) => {
  for (const [name, code, at, message] of cases)
    it(`refuses ${name}`, async () => {
      await expectRejection(code, { message, ...positionIn(code, at), source: "transform" });
    });
};

describe("a name that a nearer binding hides is not the package's contract", () => {
  it("leaves a function whose parameter is named contract as it is", async () => {
    const code = `import { contract } from "${PACKAGE}";
export function wrap(contract) {
  return contract("NotALens", {}).implementation({});
}
`;
    expect(find(parseModule(FILE, code))).toEqual([]);
    expect(await lensesOf(code)).toBeNull();
  });

  it("leaves a local, a caught value and a namespace parameter of the same name as they are", async () => {
    const code = `import * as sol from "${PACKAGE}";
import { contract, pure, uint256 } from "${PACKAGE}";
export function local(make) {
  const contract = make;
  return contract("A", {}).implementation({});
}
export function hoisted() {
  if (true) {
    var contract = () => ({ implementation: () => 1 });
  }
  return contract("B", {}).implementation({});
}
export function caught() {
  try {
    throw undefined;
  } catch (contract) {
    return contract("C", {}).implementation({});
  }
}
export const viaNamespace = (sol) => sol.contract("D", {}).implementation({});
export const Real = contract("Real", { f: pure().returns(uint256) }).implementation({ f: () => 1n });
`;
    expect(await lensesOf(code)).toEqual(["Real"]);
  });
});

describe("a lens cannot capture a binding of the code around it", () => {
  const CAPTURE = (name: string) =>
    `"${name}" is a parameter or a local of the code around this lens, which a lens cannot capture`;
  refuses([
    [
      "a parameter of the enclosing function, where a module constant has the same name",
      `import { contract, pure, uint256 } from "${PACKAGE}";
const limit = 3n;
export function make(limit = 9n) {
  return contract("Lens", { run: pure().returns(uint256) }).implementation({
    run() { return limit; },
  });
}
`,
      "limit; }",
      CAPTURE("limit"),
    ],
    [
      "a parameter of the enclosing function, where no module-level name is the same",
      `import { contract, pure, uint256 } from "${PACKAGE}";
export function make(limit = 9n) {
  return contract("Lens", { run: pure().returns(uint256) }).implementation({
    run() { return limit; },
  });
}
`,
      "limit; }",
      CAPTURE("limit"),
    ],
    [
      "a local of an enclosing block, named in a signature",
      `import { contract, pure, uint256 } from "${PACKAGE}";
export function make(wide) {
  {
    const uint256 = wide;
    return contract("Lens", { run: pure().returns(uint256) }).implementation({ run: () => 1n });
  }
}
`,
      "uint256) }).implementation",
      CAPTURE("uint256"),
    ],
    [
      "a parameter of the enclosing function, listed as an immutable",
      `import { contract, immutable, uint256, view } from "${PACKAGE}";
const total = immutable(uint256);
export const make = (total) =>
  contract("Lens", { run: view().returns(uint256) }, { immutables: { total } }).implementation({ run: () => 1n });
`,
      "total } }",
      CAPTURE("total"),
    ],
  ]);
});

describePairs("a parameter and a local hide a module-level name inside a lens, as in Solidity", {
  "a parameter and a block's local named like a module constant": {
    ts: `
      import { contract, pure, uint256 } from "${PACKAGE}";
      const RATE = 17n;
      export const Lens = contract("Lens", { run: pure(uint256).returns(uint256) }).implementation({
        run(RATE) {
          let n = RATE;
          {
            const RATE = 8n;
            n += RATE;
          }
          return n + RATE;
        },
      });
    `,
    solidity: `
      contract Lens {
          function run(uint256 RATE) external pure returns (uint256) {
              uint256 n = RATE;
              {
                  uint256 RATE = 8;
                  n += RATE;
              }
              return n + RATE;
          }
      }
    `,
    calls: [{ functionName: "run", args: [5n] }],
  },
  "a lens made inside a function, from module-level names": {
    ts: `
      import { contract, pure, uint256 } from "${PACKAGE}";
      const LIMIT = 3n;
      export function make(limit = 9n) {
        return contract("Lens", { run: pure(uint256).returns(uint256) }).implementation({
          run(limit) { return limit + LIMIT; },
        });
      }
    `,
    solidity: `
      contract Lens {
          function run(uint256 limit) external pure returns (uint256) {
              return limit + 3;
          }
      }
    `,
    calls: [{ functionName: "run", args: [5n] }],
  },
});
