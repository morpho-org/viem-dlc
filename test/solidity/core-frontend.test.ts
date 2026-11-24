/**
 * The front end: parsing a module, finding its lenses, and resolving every name a lens uses to a
 * module-level declaration. These run the three stages directly, without printing, so they hold
 * whatever the printer does with the result.
 */
import path from "node:path";

import { describe, expect, it } from "vitest";

import type { ModuleLoader, ResolvedLens, SourceModule } from "../../src/solidity/build/index.js";
import { find, LensError, lookupPosition, parseModule, resolve } from "../../src/solidity/build/index.js";
import { mapSolcDiagnostics } from "../../src/solidity/build/solc-errors.js";

import { IMPORTS } from "./helpers/core.js";
import { buildSnippet, expectRejection, positionIn, type Snippet } from "./helpers/harness.js";

const PACKAGE = "@morpho-org/viem-dlc/solidity";
const ROOT = "/fixtures";

/** Parses, finds and resolves the one lens of a snippet, with modules that exist only in memory. */
function resolveSnippet(snippet: Snippet): ResolvedLens {
  const files = new Map(
    Object.entries(typeof snippet === "string" ? { "lens.ts": snippet } : snippet).map(([name, code]) => [
      path.join(ROOT, name),
      code,
    ]),
  );
  const parsed = new Map<string, SourceModule>();
  const loader: ModuleLoader = {
    resolve(specifier, importer) {
      const base = path.resolve(path.dirname(importer), specifier);
      return [base, base.replace(/\.js$/, ".ts")].find((candidate) => files.has(candidate));
    },
    load(file) {
      const module = parsed.get(file) ?? parseModule(file, files.get(file) ?? "");
      parsed.set(file, module);
      return module;
    },
  };
  const module = loader.load(
    path.join(ROOT, Object.keys(typeof snippet === "string" ? { "lens.ts": "" } : snippet)[0] ?? ""),
  );
  const [site] = find(module);
  if (!site) throw new Error("the snippet holds no lens");
  return resolve(site, module, loader);
}

/** The declarations of a resolved lens, as kind and name. */
const declared = (lens: ResolvedLens) =>
  lens.declarations.map((declaration) => `${declaration.kind} ${declaration.name}`);

/** What the names of a resolved lens resolved to, as text. */
const referenced = (lens: ResolvedLens) =>
  new Set(
    [...lens.references.values()].map((reference) =>
      reference.kind === "builtin"
        ? `${reference.module}:${reference.name}`
        : `${reference.declaration.kind} ${reference.declaration.name}`,
    ),
  );

describe("parse", () => {
  const module = parseModule(
    "/fixtures/module.ts",
    `
import fallback from "./fallback.js";
import * as everything from "./everything.js";
import { one, two as second, "string name" as third, type OnlyAType } from "./names.js";
import type { AlsoAType } from "./names.js";

const plain = 1n;
let reassigned = 2n, another = 3n;
const { left, right } = everything;
declare const ambient: bigint;
declare function ambientFunction(): void;
declare class AmbientClass {}
function local() {}
class LocalClass {}
type Alias = bigint;

export const exportedConstant = 4n;
export function exportedFunction() {}
export class ExportedClass {}
export { plain, reassigned as renamed, one, missing };
export { type Alias };
export type { OnlyAType };
export { inner, inner as outer, type InnerType } from "./inner.js";
export type { Hidden } from "./inner.js";
export * from "./star.js";
export * as grouped from "./grouped.js";
export type * from "./types.js";
export default class DefaultClass {}
`,
  );

  it("records a module-level binding for each value a module declares or imports", () => {
    expect(Object.fromEntries([...module.bindings].map(([name, binding]) => [name, binding.kind]))).toEqual({
      fallback: "import",
      everything: "import",
      one: "import",
      second: "import",
      third: "import",
      plain: "variable",
      reassigned: "variable",
      another: "variable",
      local: "function",
      LocalClass: "class",
      exportedConstant: "variable",
      exportedFunction: "function",
      ExportedClass: "class",
      DefaultClass: "class",
    });
  });

  it("records what an import names on the other side", () => {
    const imported = (name: string) => {
      const binding = module.bindings.get(name);
      return binding?.kind === "import" ? [binding.source, binding.imported] : undefined;
    };
    expect(imported("fallback")).toEqual(["./fallback.js", "default"]);
    expect(imported("everything")).toEqual(["./everything.js", "*"]);
    expect(imported("one")).toEqual(["./names.js", "one"]);
    expect(imported("second")).toEqual(["./names.js", "two"]);
    expect(imported("third")).toEqual(["./names.js", "string name"]);
  });

  it("records how a variable is declared, and whether its statement declares it alone", () => {
    const variable = (name: string) => {
      const binding = module.bindings.get(name);
      return binding?.kind === "variable" ? [binding.declarationKind, binding.single, binding.exported] : undefined;
    };
    expect(variable("plain")).toEqual(["const", true, true]);
    expect(variable("reassigned")).toEqual(["let", false, true]);
    expect(variable("another")).toEqual(["let", false, false]);
    expect(variable("exportedConstant")).toEqual(["const", true, true]);
  });

  it("records each export as a local, a re-export or a default, and leaves types out", () => {
    expect(Object.fromEntries(module.exports)).toEqual({
      exportedConstant: { kind: "local", local: "exportedConstant" },
      exportedFunction: { kind: "local", local: "exportedFunction" },
      ExportedClass: { kind: "local", local: "ExportedClass" },
      plain: { kind: "local", local: "plain" },
      renamed: { kind: "local", local: "reassigned" },
      one: { kind: "local", local: "one" },
      missing: { kind: "local", local: "missing" },
      inner: { kind: "reexport", source: "./inner.js", imported: "inner" },
      outer: { kind: "reexport", source: "./inner.js", imported: "inner" },
      grouped: { kind: "reexport", source: "./grouped.js", imported: "*" },
      default: { kind: "local", local: "DefaultClass" },
    });
    expect(module.starExports).toEqual(["./star.js"]);
  });

  it("records a default export that is a function, an expression or only a type", () => {
    const defaultOf = (code: string) => parseModule("/fixtures/default.ts", code).exports.get("default");
    expect(defaultOf("export default function named() {}")).toEqual({ kind: "local", local: "named" });
    expect(defaultOf("export default function () {}")).toBeUndefined();
    expect(defaultOf("export default 10n ** 18n;")).toMatchObject({ kind: "default" });
    expect(defaultOf("export default interface Shape { side: bigint }")).toBeUndefined();
  });

  it("reports a module that does not parse at the line and column of the error", () => {
    const code = "const fine = 1n;\nconst broken = (;\n";
    let error: unknown;
    try {
      parseModule("/fixtures/broken.ts", code);
    } catch (cause) {
      error = cause;
    }
    expect(error).toBeInstanceOf(LensError);
    expect((error as LensError).diagnostics).toEqual([
      expect.objectContaining({
        file: "/fixtures/broken.ts",
        line: 2,
        column: 17,
        severity: "error",
        source: "transform",
        message: expect.stringContaining("cannot parse this module"),
      }),
    ]);
  });
});

describe("find", () => {
  const body = `{ f: pure(uint256).returns(uint256) }).implementation({ f: (x) => x })`;

  it("finds a lens at any depth, and one made through a namespace import of the package", async () => {
    const built = await buildSnippet(`
      import * as sol from "${PACKAGE}";
      import { contract, pure, uint256 } from "${PACKAGE}";

      export const lenses = {
        nested: [contract("Nested", ${body}],
        viaNamespace: sol.contract("ViaNamespace", { f: sol.pure(sol.uint256).returns(sol.uint256) }).implementation({
          f: (x) => sol.uint256(x),
        }),
      };
    `);
    expect(built.lenses.map((lens) => lens.resolved.name)).toEqual(["Nested", "ViaNamespace"]);
    expect(built.lenses[1]?.printed.solidity).toContain("return uint256(x);");
  });

  it("takes only the package's own contract for a lens", () => {
    const module = parseModule(
      "/fixtures/lens.ts",
      `
        import * as sol from "${PACKAGE}";
        import { pure, uint256 } from "${PACKAGE}";
        import * as other from "./other.js";
        import { contract } from "./other.js";

        const local = { contract };
        // A contract from another module, by name and off a namespace.
        contract("A", ${body};
        other.contract("B", ${body};
        // Off an object that is not a namespace import, and off one that is not a name.
        local.contract("C", ${body};
        local.nested.contract("D", ${body};
        // Something else off the package's namespace, and a computed name.
        sol.library("E", ${body};
        sol["contract"]("F", ${body};
        // A call of a call, and .implementation on something that is not a call.
        local.contract()("G", ${body};
        local.implementation({});
        local[sol.contract("H", {}).name].implementation({});
      `,
    );
    expect(find(module)).toEqual([]);
  });

  const rejections: [string, string, string, string][] = [
    [
      "a name that is not a string literal",
      `contract(name, ${body}`,
      "the first argument of contract(…) must be a string literal",
      "name,",
    ],
    [
      "no arguments",
      `contract().implementation({})`,
      "the first argument of contract(…) must be a string literal",
      "contract()",
    ],
    [
      "a name that is not an identifier",
      `contract("My Lens", ${body}`,
      '"My Lens" is not a Solidity identifier',
      '"My Lens"',
    ],
    [
      "signatures that are not an object literal",
      `contract("Lens", signatures).implementation({})`,
      "the second argument of contract(…) must be an object literal",
      "signatures)",
    ],
    [
      "no signatures",
      `contract("Lens").implementation({})`,
      "the second argument of contract(…) must be an object literal",
      'contract("Lens")',
    ],
    [
      "options that are not an object literal",
      `contract("Lens", {}, options).implementation({})`,
      "the third argument of contract(…) must be an object literal",
      "options)",
    ],
    [
      "an implementation that is not an object literal",
      `contract("Lens", {}).implementation(functions)`,
      ".implementation(…) takes an object literal written in place",
      "functions)",
    ],
    [
      "no implementation",
      `contract("Lens", {}).implementation()`,
      ".implementation(…) takes an object literal written in place",
      'contract("Lens", {})',
    ],
  ];
  for (const [what, expression, message, needle] of rejections)
    it(`refuses ${what}`, async () => {
      const code = `import { contract, pure, uint256 } from "${PACKAGE}";\nexport const Lens = ${expression};\n`;
      await expectRejection(code, { message, ...positionIn(code, needle), source: "transform" });
    });
});

describe("resolve: names", () => {
  it("follows imports, re-exports, star exports and namespaces to the declaration", () => {
    const lens = resolveSnippet({
      "lens.ts": `${IMPORTS}
        import { SCALE as UNIT, half } from "./index.js";
        import * as all from "./index.js";
        import { mload } from "./dsl.js";
        import { uint256 as word } from "./dsl.js";

        // A namespace inside a namespace is read where a constant is declared.
        const DOUBLE = all.units.SCALE * 2n;

        export const Lens = contract("Lens", { f: pure(word).returns(uint256) }).implementation({
          f: (x) => half(x) * UNIT + DOUBLE + all.LIMIT + mload(x),
        });
      `,
      "index.ts": `
        export { SCALE } from "./units.js";
        export * as units from "./units.js";
        export * from "${PACKAGE}";
        export * from "./loop.js";
        export * from "./helpers.js";
      `,
      // Two modules that export each other's names, so a search has to stop.
      "loop.ts": `export * from "./index.js"; export * from "./units.js";`,
      "units.ts": `export const SCALE = 10n ** 6n; export const LIMIT = SCALE * 3n;`,
      "helpers.ts": `
        import { internal, pure, uint256 } from "${PACKAGE}";
        export const half = internal(pure(uint256).returns(uint256), (x) => x / 2n);
      `,
      "dsl.ts": `export * from "${PACKAGE}"; export * from "${PACKAGE}/yul";`,
    });
    // The module of the helper is reached first, then the module of the constants, then the lens's own.
    expect(declared(lens)).toEqual(["helper half", "constant SCALE", "constant LIMIT", "constant DOUBLE"]);
    expect(referenced(lens)).toEqual(
      new Set(["solidity:uint256", "helper half", "constant SCALE", "constant LIMIT", "constant DOUBLE", "yul:mload"]),
    );
    expect(lens.files.map((file) => path.basename(file)).sort()).toEqual([
      "dsl.ts",
      "helpers.ts",
      "index.ts",
      "lens.ts",
      "loop.ts",
      "units.ts",
    ]);
  });

  it("reads the package's exports off a namespace import, the opcodes included", () => {
    const lens = resolveSnippet(`
      import * as sol from "${PACKAGE}";
      import * as yul from "${PACKAGE}/yul";
      import { add } from "${PACKAGE}/yul";
      import { contract } from "${PACKAGE}";

      export const Lens = contract("Lens", { f: sol.pure(sol.uint256).returns(sol.uint256) }).implementation({
        f: (x) => sol.uint256(yul.mload(add(x, BigInt(0)))),
      });
    `);
    expect(referenced(lens)).toEqual(new Set(["solidity:uint256", "yul:mload", "yul:add", "global:BigInt"]));
  });

  it("orders declarations by module, other modules first, and renames two that share a name", () => {
    const lens = resolveSnippet({
      "lens.ts": `${IMPORTS}
        import { Pair, first } from "./b.js";
        import { Pair as Other, second } from "./a.js";

        class Lens_ extends Struct({ x: uint256 }) {}
        const f = internal(pure(uint256).returns(uint256), (x) => x);

        export const Lens = contract("Lens", { f: pure(Pair, Other, Lens_).returns(uint256) }).implementation({
          f: (p, q, r) => f(first(p) + second(q)),
        });
      `,
      "a.ts": `${IMPORTS}
        export class Pair extends Struct({ a: uint256 }) {}
        export const second = internal(pure(Pair).returns(uint256), (p) => p.a);
      `,
      "b.ts": `${IMPORTS}
        export class Pair extends Struct({ b: uint256 }) {}
        export const first = internal(pure(Pair).returns(uint256), (p) => p.b);
      `,
    });
    expect(declared(lens)).toEqual([
      "struct Pair",
      "helper first",
      "struct Pair_2",
      "helper second",
      "struct Lens_",
      "helper f_2",
    ]);
  });

  it("resolves interfaces, immutables, libraries and the names inside try and catch", () => {
    const lens = resolveSnippet(`${IMPORTS}
      import { iface, immutable, library } from "${PACKAGE}";

      const IFeed = iface({ latest: view(address).returns(uint256), feeds: view().returns([address]) });
      const FEED = immutable(IFeed);
      const DECIMALS = immutable(uint8);
      const SCALE = 10n ** 6n;
      const scale = internal(pure(uint256).returns(uint256), (x) => x * SCALE);
      const Maths = library("Maths", { scaled: pure(uint256).returns(uint256), twice: pure(uint256).returns(uint256) })
        .implementation({ scaled: (x) => scale(x), twice: (x) => scale(x) * 2n });

      export const Lens = contract(
        "Lens",
        { price: view(address, IFeed).returns(uint256) },
        { immutables: { FEED, decimals: DECIMALS } },
      ).implementation({
        price: function named(token, feed) {
          let price = 0n;
          try {
            price = feed.latest(token);
          } catch (reason) {
            const fallback = FEED.latest(token);
            price = fallback;
          }
          try {
            price = Maths.scaled(price);
          } catch {
            const shift = (y) => y + named.length;
            price = shift(price);
          }
          return price;
        },
      });
    `);
    expect(declared(lens)).toEqual([
      "interface IFeed",
      "immutable FEED",
      "immutable DECIMALS",
      "constant SCALE",
      "helper scale",
      "library Maths",
    ]);
    expect(lens.immutables.map(({ key, declaration }) => [key, declaration.name])).toEqual([
      ["FEED", "FEED"],
      ["decimals", "DECIMALS"],
    ]);
    const library = lens.declarations.find((declaration) => declaration.kind === "library");
    expect(library?.kind === "library" && library.functions.map((fn) => fn.name)).toEqual(["scaled", "twice"]);
    expect(library?.dependencies.map((dependency) => dependency.name)).toEqual(["scale"]);
    const iface = lens.declarations.find((declaration) => declaration.kind === "interface");
    expect(iface?.kind === "interface" && iface.functions.map((fn) => fn.signature.outputs)).toEqual([
      [{ kind: "elementary", name: "uint256" }],
      [{ kind: "array", element: { kind: "elementary", name: "address" } }],
    ]);
    expect(lens.entries[0]?.signature.inputs[1]).toMatchObject({ kind: "interface" });
    // The names of the function itself, of a catch parameter and of a nested function are locals.
    expect(referenced(lens)).toEqual(
      new Set([
        "solidity:address",
        "solidity:uint256",
        "solidity:uint8",
        "interface IFeed",
        "immutable FEED",
        "immutable DECIMALS",
        "constant SCALE",
        "helper scale",
        "library Maths",
      ]),
    );
  });
});

describe("resolve: rejections", () => {
  const lens = (body: string, head = "", signature = "pure(uint256).returns(uint256)") => `${IMPORTS}
${head}
export const Lens = contract("Lens", { f: ${signature} }).implementation({
  f(x) {
    ${body}
  },
});
`;

  // Each case: what is wrong, the module-level text, the body, the message, and where the error is.
  const names: [string, string, string, string, string][] = [
    [
      "an import that resolves to nothing",
      'import { gone } from "./gone.js";',
      "return gone(x);",
      'cannot resolve "./gone.js"',
      "gone }",
    ],
    [
      "a package export that is not for a lens",
      `import { compiledLens } from "${PACKAGE}";`,
      "return compiledLens(x);",
      '"compiledLens" is not something a lens can use',
      "compiledLens }",
    ],
    [
      "a namespace import used as a value",
      `import * as sol from "${PACKAGE}";`,
      "return x + sol;",
      "a namespace import can only be used as namespace.name",
      "sol;",
    ],
    [
      "a module-level let",
      "let rate = 2n;",
      "return x * rate;",
      "a lens can only read a module-level const",
      "rate = 2n",
    ],
    [
      "a plain function",
      "function plain(y: bigint) { return y; }",
      "return plain(x);",
      '"plain" is a plain function; wrap it in internal(…)',
      "function plain",
    ],
    [
      "a class that extends nothing",
      "class Loose {}",
      "const o = new Loose();\n    return x;",
      'class "Loose" does not extend Struct({ … })',
      "class Loose",
    ],
    [
      "a class that extends something else",
      "class Loose extends Array {}",
      "const o = new Loose();\n    return x;",
      'class "Loose" does not extend Struct({ … })',
      "class Loose",
    ],
    [
      "a struct class with members",
      "class Busy extends Struct({ a: uint256 }) { extra = 1n; }",
      "const o = new Busy();\n    return x;",
      "a struct class has no members",
      "{ extra",
    ],
    [
      "struct fields that are not an object literal",
      "const fields = { a: uint256 };\nclass Indirect extends Struct(fields) {}",
      "const o = new Indirect();\n    return x;",
      "the fields of a struct must be an object literal written in place",
      "fields) {}",
    ],
    [
      "a struct with no fields object",
      "class Bare extends Struct() {}",
      "const o = new Bare();\n    return x;",
      "the fields of a struct must be an object literal written in place",
      "Struct() {}",
    ],
    [
      "a struct field under a computed key",
      'const key = "a";\nclass Keyed extends Struct({ [key]: uint256 }) {}',
      "const o = new Keyed();\n    return x;",
      "the fields of a struct can only have plain keys",
      "[key]: uint256",
    ],
    [
      "a struct field that is a method",
      "class Method extends Struct({ a() {} }) {}",
      "const o = new Method();\n    return x;",
      "the fields of a struct can only have plain keys",
      "a() {}",
    ],
    [
      "a struct that spreads other fields",
      "const more = {};\nclass Spread extends Struct({ ...more }) {}",
      "const o = new Spread();\n    return x;",
      "the fields of a struct can only have plain keys",
      "...more",
    ],
    [
      "a struct field that is not a type",
      "class Odd extends Struct({ a: 1n }) {}",
      "const o = new Odd();\n    return x;",
      "this is not a type",
      "1n }",
    ],
    [
      "a struct field typed by a helper",
      "const h = internal(pure(), () => {});\nclass Odd extends Struct({ a: h }) {}",
      "const o = new Odd();\n    return x;",
      "this is not a type",
      "h }",
    ],
    [
      "a struct field typed by a built-in that is not a type",
      "class Odd extends Struct({ a: require }) {}",
      "const o = new Odd();\n    return x;",
      "this is not a type",
      "require }",
    ],
    [
      "a struct field typed by the member of a call",
      "class Odd extends Struct({ a: pure().returns }) {}",
      "const o = new Odd();\n    return x;",
      "this is not a type",
      "pure().returns }",
    ],
    [
      "a struct field typed by the member of a constant",
      "const ONE = 1n;\nclass Odd extends Struct({ a: ONE.type }) {}",
      "const o = new Odd();\n    return x;",
      "this is not a type",
      "ONE.type",
    ],
    [
      "an array type with two element types",
      "class Odd extends Struct({ a: [uint256, uint8] }) {}",
      "const o = new Odd();\n    return x;",
      "an array type is written [T], with one element type",
      "[uint256, uint8]",
    ],
    [
      "an array type with no element type",
      "class Odd extends Struct({ a: [] }) {}",
      "const o = new Odd();\n    return x;",
      "an array type is written [T], with one element type",
      "[] }",
    ],
    [
      "an array type that spreads",
      "class Odd extends Struct({ a: [...uint256] }) {}",
      "const o = new Odd();\n    return x;",
      "an array type is written [T], with one element type",
      "[...uint256]",
    ],
    [
      "a helper with no signature",
      "const h = internal();",
      "return h(x);",
      "a signature is written view(…) or pure(…)",
      "internal()",
    ],
    [
      "a helper whose signature is not a call",
      "const h = internal(uint256, (y) => y);",
      "return h(x);",
      "a signature is written view(…) or pure(…)",
      "uint256, (y)",
    ],
    [
      "a helper whose signature has no builder",
      "const h = internal(pure.returns(uint256), (y) => y);",
      "return h(x);",
      "a signature is written view(…) or pure(…)",
      "pure.returns",
    ],
    [
      "a helper whose signature is built by something else",
      "const h = internal(internal(uint256), (y) => y);",
      "return h(x);",
      "a signature is written view(…) or pure(…)",
      "internal(uint256)",
    ],
    [
      "a helper with no function",
      "const h = internal(pure(uint256).returns(uint256));",
      "return h(x);",
      "the second argument of internal(…) must be a function written in place",
      "internal(pure",
    ],
    [
      "a helper whose function is a name",
      "const g = (y: bigint) => y;\nconst h = internal(pure(uint256).returns(uint256), g);",
      "return h(x);",
      "the second argument of internal(…) must be a function written in place",
      "g);",
    ],
    [
      "an async helper",
      "const h = internal(pure(uint256).returns(uint256), async (y) => y);",
      "return h(x);",
      "a lens function cannot be async or a generator",
      "async (y)",
    ],
    [
      "a generator helper",
      "const h = internal(pure(uint256).returns(uint256), function* (y) { yield y; });",
      "return h(x);",
      "a lens function cannot be async or a generator",
      "function* (y)",
    ],
    [
      "a constant that is null",
      "const NOTHING = null;",
      "return x + NOTHING;",
      "this literal cannot be part of a constant a lens reads",
      "null;",
    ],
    [
      "a constant that names a helper",
      "const h = internal(pure(), () => {});\nconst ALIAS = h;",
      "return x + ALIAS;",
      "this name cannot be part of a constant a lens reads",
      "h;",
    ],
    [
      "a constant that names a type",
      "const ALIAS = uint256;",
      "return x + ALIAS;",
      "this name cannot be part of a constant a lens reads",
      "uint256;",
    ],
    [
      "a constant with a logical not",
      "const NOT = !true;",
      "return NOT ? x : 0n;",
      "the operator ! cannot be part of a constant a lens reads",
      "!true",
    ],
    [
      "a constant with a comparison",
      "const LESS = 1n < 2n;",
      "return LESS ? x : 0n;",
      "the operator < cannot be part of a constant a lens reads",
      "1n < 2n",
    ],
    [
      "a constant with a conditional",
      "const PICK = true ? 1n : 2n;",
      "return x + PICK;",
      "this expression cannot be part of a constant a lens reads",
      "true ? 1n",
    ],
    [
      "a constant that calls a method",
      "const ONE = 1n;\nconst TEXT = ONE.toString();",
      "return x + TEXT;",
      "a call (a hash or any other computed value is written inside a body, where solc computes it) cannot be part",
      "ONE.toString()",
    ],
    [
      "a namespace inside a namespace, used as a value in a body",
      'import * as all from "./lens.js";\nexport * as again from "./lens.js";',
      "return x + all.again;",
      "a namespace import can only be used as namespace.name",
      "all.again",
    ],
    [
      "a constant defined in terms of itself",
      "const A = B + 1n;\nconst B = A + 1n;",
      "return x + A;",
      '"A" is defined in terms of itself',
      "A = B + 1n",
    ],
    [
      "a lens inside a lens",
      'const Inner = contract("Inner", { g: pure() }).implementation({ g() {} });',
      "return x + Inner;",
      "a lens cannot be used inside another lens",
      "Inner = contract",
    ],
  ];
  for (const [what, head, body, message, needle] of names)
    it(`refuses ${what}`, async () => {
      const code = lens(body, head);
      await expectRejection(code, { message, ...positionIn(code, needle), source: "transform" });
    });

  // The same for the shape of a lens: the signatures, the implementation and the options.
  const shapes: [string, string, string, string][] = [
    [
      "a signature under a computed key",
      'contract("Lens", { ["f"]: pure() }).implementation({ f() {} })',
      "the declaration of contract Lens can only have signatures under plain keys",
      '["f"]: pure()',
    ],
    [
      "a signature written as a method",
      'contract("Lens", { f() {} }).implementation({ f() {} })',
      "the declaration of contract Lens can only have signatures under plain keys",
      "f() {} }).",
    ],
    [
      "signatures that spread",
      'contract("Lens", { ...more }).implementation({ f() {} })',
      "the declaration of contract Lens can only have signatures under plain keys",
      "...more }",
    ],
    [
      "an implementation under a computed key",
      'contract("Lens", { f: pure() }).implementation({ ["f"]() {} })',
      "the implementation of contract Lens can only have plain functions under plain keys",
      '["f"]() {}',
    ],
    [
      "an implementation that is a getter",
      'contract("Lens", { f: pure() }).implementation({ get f() { return 1n; } })',
      "the implementation of contract Lens can only have plain functions under plain keys",
      "get f()",
    ],
    [
      "an implementation that spreads",
      'contract("Lens", { f: pure() }).implementation({ ...more })',
      "the implementation of contract Lens can only have plain functions under plain keys",
      "...more }",
    ],
    [
      "an implementation that is not a function",
      'contract("Lens", { f: pure() }).implementation({ f: 1n })',
      'the implementation of "f" must be a function written in place',
      "1n }",
    ],
    [
      "an entry point declared and not implemented",
      'contract("Lens", { f: pure(), g: pure() }).implementation({ f() {} })',
      'contract Lens declares "g" and does not implement it',
      "g: pure()",
    ],
    [
      "an entry point implemented and not declared",
      'contract("Lens", { f: pure() }).implementation({ f() {}, g() {} })',
      'contract Lens implements "g" and does not declare its signature',
      "g() {}",
    ],
    [
      "an option contract does not have",
      'contract("Lens", { f: pure() }, { salt: 1n }).implementation({ f() {} })',
      'contract(…) has no option "salt"',
      "1n }",
    ],
    [
      "options under a computed key",
      'contract("Lens", { f: pure() }, { ["immutables"]: {} }).implementation({ f() {} })',
      "the options of contract(…) can only have plain keys",
      '["immutables"]',
    ],
    [
      "immutables that are not an object literal",
      'contract("Lens", { f: pure() }, { immutables: more }).implementation({ f() {} })',
      "the immutables of a contract must be an object literal written in place",
      "more }",
    ],
    [
      "an immutable that is not declared with immutable(…)",
      'contract("Lens", { f: pure() }, { immutables: { ONE } }).implementation({ f() {} })',
      '"ONE" must name a module-level immutable(…)',
      "ONE }",
    ],
    [
      "an immutable that is not a name",
      'contract("Lens", { f: pure() }, { immutables: { ONE: 1n } }).implementation({ f() {} })',
      '"ONE" must name a module-level immutable(…)',
      "1n } }",
    ],
  ];
  for (const [what, expression, message, needle] of shapes)
    it(`refuses ${what}`, async () => {
      const code = `${IMPORTS}\nconst more = {};\nconst ONE = 1n;\nexport const Lens = ${expression};\n`;
      await expectRejection(code, { message, ...positionIn(code, needle), source: "transform" });
    });

  // And for the other declarations a lens can reach.
  const others: [string, string, string, string][] = [
    [
      "an interface whose functions are not an object literal",
      "const I = iface(more);",
      "the functions of an interface must be an object literal written in place",
      "more);",
    ],
    ["an immutable with no type", "const I = immutable();", "this is not a type", "immutable()"],
    [
      "a library with no name",
      "const I = library().implementation({});",
      "the first argument of library(…) must be a string literal: its name",
      "library()",
    ],
    [
      "a library whose name is not a string",
      "const I = library(more, {}).implementation({});",
      "the first argument of library(…) must be a string literal: its name",
      "more, {}",
    ],
    [
      "a library whose functions are not an object literal",
      'const I = library("L", more).implementation({});',
      "library(name, { … }).implementation({ … }) takes object literals written in place",
      'library("L", more)',
    ],
    [
      "a library with no implementation",
      'const I = library("L", {}).implementation();',
      "library(name, { … }).implementation({ … }) takes object literals written in place",
      'library("L", {})',
    ],
    [
      "a library function declared and not implemented",
      'const I = library("L", { g: pure() }).implementation({});',
      'library L declares "g" and does not implement it',
      "g: pure()",
    ],
    [
      "a constant made by .implementation on a name",
      "const I = ONE.implementation({});",
      "a call (a hash or any other computed value",
      "ONE.implementation",
    ],
    [
      "a constant made by .implementation on a call that is not library(…)",
      "const I = ONE.toString().implementation({});",
      "a call (a hash or any other computed value",
      "ONE.toString()",
    ],
    [
      "a constant made by a computed call",
      'const I = ONE["implementation"]({});',
      "a call (a hash or any other computed value",
      'ONE["implementation"]',
    ],
    [
      "a constant made by a call of another member",
      "const I = ONE.valueOf();",
      "a call (a hash or any other computed value",
      "ONE.valueOf()",
    ],
  ];
  for (const [what, declaration, message, needle] of others)
    it(`refuses ${what}`, async () => {
      const code = `${IMPORTS}
import { iface, immutable, library } from "${PACKAGE}";
const more = {};
const ONE = 1n;
${declaration}
export const Lens = contract("Lens", { f: pure().returns(uint256) }).implementation({ f: () => uint256(I) });
`;
      await expectRejection(code, { message, ...positionIn(code, needle), source: "transform" });
    });

  it("refuses a name another module does not export, where it is imported", async () => {
    const code = lens("return x * MISSING;", 'import { MISSING } from "./units.js";');
    await expectRejection(
      { "lens.ts": code, "units.ts": 'export * from "./more.js";', "more.ts": 'export * from "./units.js";' },
      { message: '"MISSING" is not exported by', ...positionIn(code, "MISSING }") },
    );
  });

  it("refuses a default export that is an expression, where it is imported", async () => {
    const code = lens("return x * SCALE;", 'import SCALE from "./units.js";');
    await expectRejection(
      { "lens.ts": code, "units.ts": "export default 10n ** 6n;" },
      { message: "a default export that is an expression cannot be used by a lens", ...positionIn(code, "SCALE from") },
    );
  });

  it("refuses two modules that re-export a name from each other", async () => {
    const code = lens("return x * SCALE;", 'import { SCALE } from "./a.js";');
    await expectRejection(
      { "lens.ts": code, "a.ts": 'export { SCALE } from "./b.js";', "b.ts": 'export { SCALE } from "./a.js";' },
      { message: '"SCALE" is exported by', ...positionIn(code, "SCALE }") },
    );
  });

  it("refuses two modules that import a name from each other", async () => {
    const code = lens("return x * SCALE;", 'import { SCALE } from "./a.js";');
    const b = 'import { SCALE } from "./a.js";\nexport { SCALE };';
    await expectRejection(
      { "lens.ts": code, "a.ts": 'import { SCALE } from "./b.js";\nexport { SCALE };', "b.ts": b },
      { message: "in terms of itself", file: "b.ts", ...positionIn(b, "SCALE }") },
    );
  });

  it("reports an error in another module at that module", async () => {
    const units = "export const RATE = 10n ** 18n / 3n;\n";
    await expectRejection(
      { "lens.ts": lens("return x * RATE;", 'import { RATE } from "./units.js";'), "units.ts": units },
      { message: "a constant a lens reads cannot use /", file: "units.ts", ...positionIn(units, "/ 3n") },
    );
  });
});

describe("solc's errors, pointed at the TypeScript", () => {
  const entry = (start: number, end: number, line: number) => ({
    start,
    end,
    file: "/fixtures/lens.ts",
    line,
    column: 1,
  });
  // An outer node, two nodes inside it, and a node of the same range as the outer one recorded before it.
  const map = { entries: [entry(10, 20, 3), entry(20, 30, 4), entry(0, 40, 2), entry(0, 40, 1), entry(50, 60, 5)] };

  it("the innermost node that covers the whole range gives the position", () => {
    expect(lookupPosition(map, 12, 18)?.line).toBe(3);
    expect(lookupPosition(map, 20, 30)?.line).toBe(4);
    expect(lookupPosition(map, 15, 25)?.line).toBe(2);
    expect(lookupPosition(map, 0, 40)?.line).toBe(2);
  });

  it("a range no node covers whole is placed by its start, and one outside every node is not placed", () => {
    expect(lookupPosition(map, 55, 70)?.line).toBe(5);
    expect(lookupPosition(map, 35, 55)?.line).toBe(2);
    expect(lookupPosition(map, 42, 48)).toBeUndefined();
    expect(lookupPosition(map, 60, 61)).toBeUndefined();
  });

  it("an error with no location is reported against the lens, and a warning stays a warning", () => {
    const lens = resolveSnippet(`${IMPORTS}

      export const Lens = contract("Lens", { f: pure(uint256).returns(uint256) }).implementation({ f: (x) => x });
    `);
    const printed = { solidity: "contract Lens {}", map: { entries: [] } };
    const at = { file: "/fixtures/lens.ts", line: 6, column: 27, source: "solc", lens: "Lens" };
    expect(
      mapSolcDiagnostics(
        [
          { severity: "error", type: "CompilerError", message: "no location", formattedMessage: "" },
          {
            severity: "error",
            type: "TypeError",
            message: "outside",
            formattedMessage: "",
            errorCode: "1234",
            range: { start: 90, end: 95 },
          },
          { severity: "warning", type: "Warning", message: "careful", formattedMessage: "", errorCode: "2072" },
          { severity: "info", type: "Info", message: "note", formattedMessage: "" },
        ],
        printed,
        lens,
      ),
    ).toEqual([
      { ...at, severity: "error", message: "no location" },
      { ...at, severity: "error", message: "outside", code: "1234" },
      { ...at, severity: "warning", message: "careful", code: "2072" },
      { ...at, severity: "warning", message: "note" },
    ]);
  });
});
