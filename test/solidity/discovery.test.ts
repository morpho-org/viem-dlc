/**
 * Discovery: every use of the package's contract is a lens, or is refused. A use the transform would pass
 * over is one that reaches production uncompiled, and throws on first use.
 */
import path from "node:path";

import { describe, expect, it } from "vitest";

import { build, find, LensError, parseModule, transformSync } from "../../src/solidity/build/index.js";

import { emitJavaScript } from "./helpers/emit.js";
import { expectRejection, positionIn } from "./helpers/harness.js";

const PACKAGE = "@morpho-org/viem-dlc/solidity";
const FILE = path.join(import.meta.dirname, "__fixtures__", "lens.ts");
const HOST = { resolve: () => undefined, addDependency: () => {} };
const SHAPE = "contract is written in one expression with its implementation";

/** [what is refused, the module, where the error points, the module's file when it is not lens.ts] */
type Case = readonly [name: string, code: string, at: string, file?: string];

describe("a use of contract that is not a lens is refused where it is written", () => {
  const cases: readonly Case[] = [
    [
      "a declaration and its implementation in two statements",
      `import { contract, pure, uint256 } from "${PACKAGE}";
const spec = contract("Split", { read: pure().returns(uint256) });
export const Split = spec.implementation({ read() { return 1n; } });
`,
      'contract("Split"',
    ],
    [
      "a declaration and its implementation in two statements, off a namespace",
      `import * as sol from "${PACKAGE}";
const spec = sol.contract("Split", { read: sol.pure().returns(sol.uint256) });
export const Split = spec.implementation({ read() { return 1n; } });
`,
      'sol.contract("Split"',
    ],
    [
      "a declaration that is not followed by its implementation",
      `import { contract, pure, uint256 } from "${PACKAGE}";
export const name = contract("Bare", { read: pure().returns(uint256) }).name;
`,
      'contract("Bare"',
    ],
    [
      "contract under another name",
      `import { contract, pure, uint256 } from "${PACKAGE}";
const make = contract;
export const Aliased = make("Aliased", { read: pure().returns(uint256) }).implementation({ read: () => 1n });
`,
      "contract;",
    ],
    [
      "contract passed to a function",
      `import { contract } from "${PACKAGE}";
export const made = [1, 2].map(contract);
`,
      "contract);",
    ],
    [
      "contract kept in an object",
      `import { contract } from "${PACKAGE}";
export const tools = { contract };
`,
      "contract };",
    ],
    [
      "contract exported, for another module to make a lens with",
      `import { contract } from "${PACKAGE}";
export { contract };
`,
      "contract };",
    ],
    [
      "contract exported under another name",
      `import { contract } from "${PACKAGE}";
export { contract as make };
`,
      "contract as make",
    ],
    [
      "contract exported straight from the package",
      `export { pure, contract } from "${PACKAGE}";
`,
      "contract }",
    ],
    [
      "contract exported straight from the package under another name",
      `export { contract as make } from "${PACKAGE}";
`,
      "contract as make",
    ],
    [
      "contract as the default export",
      `import { contract } from "${PACKAGE}";
export default contract;
`,
      "contract;",
    ],
  ];
  for (const [name, code, at] of cases)
    it(`refuses ${name}`, async () => {
      await expectRejection(code, { message: SHAPE, ...positionIn(code, at), source: "transform" });
      // The same refusal where no host awaits: what the check command and the Node hook run.
      expect(() => transformSync(code, FILE, HOST, { cache: false })).toThrow(LensError);
    });
});

describe("an export * of the package is refused, so contract is only ever imported from the package", () => {
  const STAR = `export * from "${PACKAGE}" passes contract on to every module that imports from this one`;
  const stars = [
    ["every name of the package", `export * from "${PACKAGE}";\n`],
    ["the package as a namespace", `export * as dsl from "${PACKAGE}";\n`],
  ] as const;

  it.each(stars)("refuses a module that exports %s", async (_what, barrel) => {
    await expectRejection(barrel, { message: STAR, line: 1, column: 1, source: "transform" });
    expect(() => transformSync(barrel, FILE, HOST, { cache: false })).toThrow(LensError);
  });

  it.each(stars)("refuses it where a lens reads a name through a module that exports %s", async (_what, barrel) => {
    const lens = `import { contract, pure } from "${PACKAGE}";
import * as names from "./barrel.js";
export const Lens = contract("Lens", { run: pure().returns(names.uint256) }).implementation({ run: () => 3n });
`;
    await expectRejection(
      { "lens.ts": lens, "barrel.ts": barrel },
      { message: STAR, line: 1, column: 1, file: "barrel.ts", source: "transform" },
    );
  });

  it("leaves a module that exports the package's types alone", async () => {
    const code = `export type * from "${PACKAGE}";\n`;
    expect(await build(code, FILE, HOST, { cache: false })).toBeNull();
  });
});

describe("the package's namespace is only read as namespace.name, so no contract leaves it unseen", () => {
  const NAMESPACE = "a namespace import of the package is only read as namespace.name";
  const cases: readonly Case[] = [
    [
      "contract read by a computed name, and implemented in another statement",
      `import * as s from "${PACKAGE}";
const spec = s["contract"]("Split", { read: s.pure().returns(s.uint256) });
export const Split = spec.implementation({ read: () => 3n });
`,
      's["contract"]',
    ],
    [
      "contract read by a computed name, in one expression with its implementation",
      `import * as s from "${PACKAGE}";
export const Computed = s["contract"]("Computed", { read: s.pure().returns(s.uint256) }).implementation({ read: () => 3n });
`,
      's["contract"]',
    ],
    [
      "a name read by a name that is computed",
      `import * as s from "${PACKAGE}";
const name = "contract";
export const made = s[name];
`,
      "s[name]",
    ],
    [
      "contract taken out by destructuring",
      `import * as s from "${PACKAGE}";
const { contract: make } = s;
export const Lens = make("Lens", { read: s.pure().returns(s.uint256) }).implementation({ read: () => 3n });
`,
      "s;",
    ],
    [
      "the namespace under another name",
      `import * as s from "${PACKAGE}";
const dsl = s;
export const Lens = dsl.contract("Lens", { read: s.pure().returns(s.uint256) }).implementation({ read: () => 3n });
`,
      "s;",
    ],
    [
      "the namespace passed to a function",
      `import * as s from "${PACKAGE}";
export const names = Object.keys(s);
`,
      "s);",
    ],
    [
      "the namespace exported",
      `import * as s from "${PACKAGE}";
export { s };
`,
      "s };",
    ],
    [
      "the namespace as the element of a tag",
      `import * as s from "${PACKAGE}";
export const element = <s.contract />;
`,
      "s.contract />",
      "lens.tsx",
    ],
  ];
  for (const [name, code, at, file = "lens.ts"] of cases)
    it(`refuses ${name}`, async () => {
      await expectRejection({ [file]: code }, { message: NAMESPACE, ...positionIn(code, at), source: "transform" });
      expect(() => transformSync(code, path.join(path.dirname(FILE), file), HOST, { cache: false })).toThrow(LensError);
    });
});

describe("what only shares the name is left as it is", () => {
  it("builds the lens beside a property, a key and a type named contract", async () => {
    const code = `import { contract, pure, uint256 } from "${PACKAGE}";
import * as sol from "${PACKAGE}";
export type { contract as Contract } from "${PACKAGE}";
interface Options { contract: string; sol: typeof sol; made: (typeof sol)["contract"] }
type Factory = typeof contract;
const options = { contract: "by key", nested: { contract: () => "a method" } };
export const viaProperty = options.nested.contract();
export const viaOther = sol.pure;
export function annotated(make: Factory, given: Options): string {
  return given.contract;
}
export const Real = contract("Real", { f: pure().returns(uint256) }).implementation({ f: () => 1n });
`;
    const built = await build(code, FILE, HOST, { cache: false });
    expect(built?.lenses.map((lens) => lens.resolved.name)).toEqual(["Real"]);
  });
});

describe("a declaration of the same name that a compiler erases does not hide the package's contract", () => {
  const lensesOf = async (code: string, file = FILE) =>
    (await build(code, file, HOST, { cache: false }))?.lenses.map((lens) => lens.resolved.name) ?? null;
  const BODY = '("Lens", { run: pure().returns(uint256) }).implementation({ run: () => 29n })';

  it.each([
    [
      "a type alias named like contract",
      `import { contract as make, pure, uint256 } from "${PACKAGE}";
type make = typeof make;
export const Lens = make${BODY};
`,
    ],
    [
      "an interface named like contract",
      `import { contract as make, pure, uint256 } from "${PACKAGE}";
interface make { tag: string }
export const Lens = make${BODY};
`,
    ],
    [
      "a namespace declared for its types, named like contract",
      `import { contract as make, pure, uint256 } from "${PACKAGE}";
declare namespace make { type Tag = string }
export const Lens = make${BODY};
`,
    ],
    [
      "a type alias named like the namespace contract is read off",
      `import * as dsl from "${PACKAGE}";
type dsl = typeof dsl;
export const Lens = dsl.contract("Lens", { run: dsl.pure().returns(dsl.uint256) }).implementation({ run: () => 29n });
`,
    ],
  ])("finds the lens beside %s, in the module and in the JavaScript a compiler makes of it", async (_what, code) => {
    expect(find(parseModule(FILE, code)).map((site) => site.name)).toEqual(["Lens"]);
    expect(await lensesOf(code)).toEqual(["Lens"]);
    const { lens } = emitJavaScript({ lens: code });
    expect(await lensesOf(lens, FILE.replace(/\.ts$/, ".js"))).toEqual(["Lens"]);
  });

  // A namespace is a value when it holds one, and a compiler erases it when it holds only types. Which
  // of the two a use names is not read off the tree, so the use is refused rather than passed over.
  it.each([
    [
      "contract",
      `import { contract as make, pure, uint256 } from "${PACKAGE}";
namespace make { export type Tag = string }
export const Lens = make${BODY};
`,
      'make("Lens"',
    ],
    [
      "the namespace contract is read off",
      `import * as dsl from "${PACKAGE}";
namespace dsl { export type Tag = string }
export const Lens = dsl.contract("Lens", { run: dsl.pure().returns(dsl.uint256) }).implementation({ run: () => 29n });
`,
      'dsl.contract("Lens"',
    ],
  ])("refuses a use of %s under a name a namespace of the module declares again", async (_what, code, at) => {
    await expectRejection(code, {
      message: "is imported from the package and declared again in this module",
      ...positionIn(code, at),
      source: "transform",
    });
    expect(() => transformSync(code, FILE, HOST, { cache: false })).toThrow(LensError);
  });
});
