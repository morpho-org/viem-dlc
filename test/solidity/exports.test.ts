/**
 * Exports: a name a lens imports is followed through re-exports as a JavaScript engine links it. An
 * `export *` gives a name only when it is one binding, never gives `default`, and an export a module
 * writes itself comes before any star.
 */
import { describe, it } from "vitest";

import { describePairs } from "./helpers/core.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

const PACKAGE = "@morpho-org/viem-dlc/solidity";
const IMPORTS = `import { contract, pure, uint256 } from "${PACKAGE}";`;
const reads = (imports: string, value: string) => `${IMPORTS}
${imports}
export const Lens = contract("Lens", { run: pure().returns(uint256) }).implementation({
  run() { return ${value}; },
});
`;
const RUN = `
  contract Lens {
      function run() external pure returns (uint256) {
          return VALUE;
      }
  }
`;

describe("export * follows the rules of a JavaScript module", () => {
  it("refuses a name two export * declarations give from different declarations", async () => {
    const lens = reads('import { X } from "./barrel.js";', "X");
    await expectRejection(
      {
        "lens.ts": lens,
        "barrel.ts": 'export * from "./left.js";\nexport * from "./right.js";\n',
        "left.ts": "export const X = 11n;\n",
        "right.ts": "export const X = 22n;\n",
      },
      {
        message: /^"X" is exported by more than one export \* of .*barrel\.ts, from different declarations/,
        ...positionIn(lens, "X }"),
        source: "transform",
      },
    );
  });

  it("refuses the same name read off a namespace", async () => {
    const lens = reads('import * as barrel from "./barrel.js";', "barrel.X");
    await expectRejection(
      {
        "lens.ts": lens,
        "barrel.ts": 'export * from "./left.js";\nexport * from "./right.js";\n',
        "left.ts": "export const X = 11n;\n",
        "right.ts": "export const X = 22n;\n",
      },
      { message: "from different declarations", ...positionIn(lens, "barrel.X"), source: "transform" },
    );
  });

  it("refuses a default import through export *, which passes no default on", async () => {
    const lens = reads('import N from "./barrel.js";', "N");
    await expectRejection(
      {
        "lens.ts": lens,
        "barrel.ts": 'export * from "./provider.js";\n',
        "provider.ts": "const N = 17n;\nexport { N as default };\n",
      },
      {
        message: /^"default" is not exported by .*barrel\.ts: export \* does not pass a default export on/,
        ...positionIn(lens, "N from"),
        source: "transform",
      },
    );
  });
});

describePairs("names that export * does give", {
  "one binding reached through two export * declarations": {
    ts: {
      "lens.ts": reads('import * as top from "./top.js";', "top.X"),
      "top.ts": 'export * from "./one.js";\nexport * from "./two.js";\nexport * from "./provider.js";\n',
      "one.ts": 'export { X } from "./provider.js";\n',
      "two.ts": 'export * from "./provider.js";\n',
      "provider.ts": "export const X = 33n;\n",
    },
    solidity: RUN.replace("VALUE", "33"),
    calls: [{ functionName: "run" }],
  },
  "an export written by name, which comes before a star of the same name": {
    ts: {
      "lens.ts": reads('import { X } from "./barrel.js";', "X"),
      "barrel.ts": 'export * from "./left.js";\nexport { X } from "./right.js";\n',
      "left.ts": "export const X = 11n;\n",
      "right.ts": "export const X = 22n;\n",
    },
    solidity: RUN.replace("VALUE", "22"),
    calls: [{ functionName: "run" }],
  },
  "the package's names through two modules of export *, and through two ways to it": {
    ts: {
      "lens.ts": `import { contract } from "${PACKAGE}";
        import * as dsl from "./top.js";
        export const Lens = contract("Lens", { run: dsl.pure().returns(dsl.uint256) }).implementation({
          run() { return dsl.uint256(31n); },
        });
      `,
      "top.ts": 'export * from "./middle.js";\nexport * from "./other.js";\n',
      "middle.ts": `export * from "${PACKAGE}";\n`,
      "other.ts": `export * from "${PACKAGE}";\n`,
    },
    solidity: RUN.replace("VALUE", "uint256(31)"),
    calls: [{ functionName: "run" }],
  },
});

describe("a default export", () => {
  it("refuses one that is computed, where it is imported", async () => {
    const lens = reads('import N from "./provider.js";', "N");
    await expectRejection(
      { "lens.ts": lens, "provider.ts": "const N = 17n;\nexport default N * 1n;\n" },
      {
        message: "a default export that is an expression cannot be used by a lens; export it under a name",
        ...positionIn(lens, "N from"),
        source: "transform",
      },
    );
  });
});

describePairs("a default export that is a name", {
  "export default N": {
    ts: {
      "lens.ts": reads('import N from "./provider.js";', "N"),
      "provider.ts": "const N = 17n;\nexport default N;\n",
    },
    solidity: RUN.replace("VALUE", "17"),
    calls: [{ functionName: "run" }],
  },
  "export { N as default }": {
    ts: {
      "lens.ts": reads('import N from "./provider.js";', "N"),
      "provider.ts": "const N = 17n;\nexport { N as default };\n",
    },
    solidity: RUN.replace("VALUE", "17"),
    calls: [{ functionName: "run" }],
  },
  "a default export read off a namespace, written with a type": {
    ts: {
      "lens.ts": reads('import * as provider from "./provider.js";', "provider.default"),
      "provider.ts": "const N = 17n;\nexport default N as bigint;\n",
    },
    solidity: RUN.replace("VALUE", "17"),
    calls: [{ functionName: "run" }],
  },
});
