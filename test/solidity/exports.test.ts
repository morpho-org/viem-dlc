/**
 * Exports: a name a lens imports is followed through re-exports. An export a module writes itself is
 * followed directly and comes before any star. An `export *` is followed only where it is the one
 * `export *` of its module that offers the name, and never gives `default`. Two that offer it are refused,
 * where a JavaScript engine would link them to one binding too: the build compares no bindings.
 */
import { describe, it } from "vitest";

import { describePairs } from "./helpers/core.js";
import { expectRejection, positionIn, type Snippet } from "./helpers/harness.js";

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
const BARREL = 'export * from "./left.js";\nexport * from "./right.js";\n';
const TWICE = (name: string, file = "barrel") =>
  new RegExp(
    `^"${name}" is offered by more than one export \\* of .*${file}\\.ts: from "\\./left\\.js" and from "\\./right\\.js"\\. `,
  );

describe("a name that more than one export * offers is refused", () => {
  /** [what offers the name twice, the import, the value read, the other modules, where the error points] */
  const cases: readonly (readonly [name: string, imports: string, value: string, modules: Snippet, at: string])[] = [
    [
      "two declarations",
      'import { X } from "./barrel.js";',
      "X",
      { "barrel.ts": BARREL, "left.ts": "export const X = 11n;\n", "right.ts": "export const X = 22n;\n" },
      "X }",
    ],
    [
      "two declarations, read off a namespace",
      'import * as barrel from "./barrel.js";',
      "barrel.X",
      { "barrel.ts": BARREL, "left.ts": "export const X = 11n;\n", "right.ts": "export const X = 22n;\n" },
      "barrel.X",
    ],
    [
      "the namespaces of two modules",
      'import { X } from "./barrel.js";',
      "X.V",
      {
        "barrel.ts": BARREL,
        "left.ts": 'import * as X from "./small.js";\nexport { X };\n',
        "right.ts": 'import * as X from "./large.js";\nexport { X };\n',
        "small.ts": "export const V = 11n;\n",
        "large.ts": "export const V = 22n;\n",
      },
      "X }",
    ],
    [
      "one declaration, by its name in both",
      'import { X } from "./barrel.js";',
      "X",
      {
        "barrel.ts": BARREL,
        "left.ts": 'export { X } from "./provider.js";\n',
        "right.ts": 'export * from "./provider.js";\n',
        "provider.ts": "export const X = 33n;\n",
      },
      "X }",
    ],
    [
      "one declaration, by its name and as a copy of the default export",
      'import { X } from "./barrel.js";',
      "X",
      {
        "barrel.ts": BARREL,
        "left.ts": 'export { default as X } from "./provider.js";\n',
        "right.ts": 'export { X } from "./provider.js";\n',
        "provider.ts": "export const X = 23n;\nexport default X;\n",
      },
      "X }",
    ],
    [
      "the namespace of one module, which each exports by import",
      'import { X } from "./barrel.js";',
      "X.V",
      {
        "barrel.ts": BARREL,
        "left.ts": 'import * as X from "./provider.js";\nexport { X };\n',
        "right.ts": 'import * as X from "./provider.js";\nexport { X };\n',
        "provider.ts": "export const V = 23n;\n",
      },
      "X }",
    ],
    [
      "the namespace of one module, which each exports directly",
      'import { X } from "./barrel.js";',
      "X.V",
      {
        "barrel.ts": BARREL,
        "left.ts": 'export * as X from "./provider.js";\n',
        "right.ts": 'export * as X from "./provider.js";\n',
        "provider.ts": "export const V = 23n;\n",
      },
      "X }",
    ],
    [
      "a declaration and a name of the package only JavaScript uses",
      'import { compiledLens } from "./barrel.js";',
      "compiledLens",
      {
        "barrel.ts": BARREL,
        "left.ts": `export * from "${PACKAGE}";\n`,
        "right.ts": "export const compiledLens = 23n;\n",
      },
      "compiledLens }",
    ],
    [
      "a declaration and a name declared by destructuring",
      'import { X } from "./barrel.js";',
      "X",
      {
        "barrel.ts": BARREL,
        "left.ts": "export const { X } = { X: 11n };\n",
        "right.ts": "export const X = 22n;\n",
      },
      "X }",
    ],
    [
      "a declaration and an enum",
      'import { X } from "./barrel.js";',
      "X",
      { "barrel.ts": BARREL, "left.ts": "export enum X { A }\n", "right.ts": "export const X = 22n;\n" },
      "X }",
    ],
    [
      "a declaration, and a module that names it back from the module asked",
      'import { X } from "./barrel.js";',
      "X",
      {
        "barrel.ts": BARREL,
        "left.ts": 'export { X } from "./barrel.js";\n',
        "right.ts": 'export { X } from "./provider.js";\n',
        "provider.ts": "export const X = 23n;\n",
      },
      "X }",
    ],
  ];
  for (const [name, imports, value, modules, at] of cases)
    it(`refuses ${name}`, async () => {
      const lens = reads(imports, value);
      await expectRejection(
        { "lens.ts": lens, ...(modules as Record<string, string>) },
        { message: TWICE(/\{ (\w+) \}/.exec(imports)?.[1] ?? "X"), ...positionIn(lens, at), source: "transform" },
      );
    });

  it("refuses the package's names through two modules that each export * of it", async () => {
    const lens = `import { contract } from "${PACKAGE}";
import * as dsl from "./barrel.js";
export const Lens = contract("Lens", { run: dsl.pure().returns(dsl.uint256) }).implementation({
  run() { return dsl.uint256(31n); },
});
`;
    await expectRejection(
      {
        "lens.ts": lens,
        "barrel.ts": BARREL,
        "left.ts": `export * from "${PACKAGE}";\n`,
        "right.ts": `export * from "${PACKAGE}";\n`,
      },
      { message: TWICE("pure"), ...positionIn(lens, "dsl.pure"), source: "transform" },
    );
  });

  it("refuses it one module down, where the two export * declarations are", async () => {
    const lens = reads('import { X } from "./top.js";', "X");
    await expectRejection(
      {
        "lens.ts": lens,
        "top.ts": 'export * from "./barrel.js";\n',
        "barrel.ts": BARREL,
        "left.ts": "export const X = 11n;\n",
        "right.ts": "export const X = 22n;\n",
      },
      { message: TWICE("X"), ...positionIn(lens, "X }"), source: "transform" },
    );
  });
});

describe("what export * does not give", () => {
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

  it("refuses a name that only a circle of export * leads to", async () => {
    const lens = reads('import { X } from "./barrel.js";', "X");
    await expectRejection(
      { "lens.ts": lens, "barrel.ts": 'export * from "./left.js";\n', "left.ts": 'export * from "./barrel.js";\n' },
      { message: /^"X" is not exported by .*barrel\.ts$/, ...positionIn(lens, "X }"), source: "transform" },
    );
  });

  it("refuses a name the one export * that offers it names back from the module asked", async () => {
    const lens = reads('import { X } from "./barrel.js";', "X");
    await expectRejection(
      { "lens.ts": lens, "barrel.ts": 'export * from "./left.js";\n', "left.ts": 'export { X } from "./barrel.js";\n' },
      {
        message: /^"X" is exported by .*barrel\.ts in terms of itself$/,
        ...positionIn(lens, "X }"),
        source: "transform",
      },
    );
  });

  it("refuses a name of the package only JavaScript uses, where one export * offers it", async () => {
    const lens = reads('import { compiledLens } from "./barrel.js";', "compiledLens");
    await expectRejection(
      { "lens.ts": lens, "barrel.ts": `export * from "${PACKAGE}";\n` },
      {
        message: '"compiledLens" is not something a lens can use',
        ...positionIn(lens, "compiledLens }"),
        source: "transform",
      },
    );
  });
});

describePairs("names that export * does give", {
  "a name one export * offers, beside one that offers other names": {
    ts: {
      "lens.ts": reads('import { X } from "./barrel.js";', "X"),
      "barrel.ts": BARREL,
      "left.ts": "export const X = 11n;\n",
      "right.ts": "export const Y = 22n;\nexport default 5n;\n",
    },
    solidity: RUN.replace("VALUE", "11"),
    calls: [{ functionName: "run" }],
  },
  "a name through two modules of export *, one after the other": {
    ts: {
      "lens.ts": reads('import * as top from "./top.js";', "top.X"),
      "top.ts": 'export * from "./middle.js";\n',
      "middle.ts": 'export * from "./provider.js";\n',
      "provider.ts": "export const X = 33n;\n",
    },
    solidity: RUN.replace("VALUE", "33"),
    calls: [{ functionName: "run" }],
  },
  "a name one export * offers, beside one that leads back to the module asked": {
    ts: {
      "lens.ts": reads('import { X } from "./barrel.js";', "X"),
      "barrel.ts": BARREL,
      "left.ts": 'export * from "./barrel.js";\n',
      "right.ts": 'export { X } from "./provider.js";\n',
      "provider.ts": "export const X = 23n;\n",
    },
    solidity: RUN.replace("VALUE", "23"),
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
  "an export written by name, which comes before two stars of the same name": {
    ts: {
      "lens.ts": reads('import { X } from "./barrel.js";', "X"),
      "barrel.ts": `export { X } from "./provider.js";\n${BARREL}`,
      "left.ts": "export const X = 57n;\n",
      "right.ts": "export const X = 61n;\n",
      "provider.ts": "export const X = 23n;\n",
    },
    solidity: RUN.replace("VALUE", "23"),
    calls: [{ functionName: "run" }],
  },
  "the package's names through two modules of export *, one after the other": {
    ts: {
      "lens.ts": `import { contract } from "${PACKAGE}";
        import * as dsl from "./top.js";
        import { add } from "./yul.js";
        export const Lens = contract("Lens", { run: dsl.pure().returns(dsl.uint256) }).implementation({
          run() {
            const sum = dsl.assembly("memory-safe", () => add(30n, 1n));
            return dsl.uint256(sum);
          },
        });
      `,
      "top.ts": 'export * from "./middle.js";\n',
      "middle.ts": `export * from "${PACKAGE}";\n`,
      "yul.ts": `export * from "${PACKAGE}/yul";\n`,
    },
    solidity: `
      contract Lens {
          function run() external pure returns (uint256) {
              uint256 sum;
              assembly ("memory-safe") {
                  sum := add(30, 1)
              }
              return uint256(sum);
          }
      }
    `,
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
