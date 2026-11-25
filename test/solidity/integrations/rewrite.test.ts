/**
 * The rewrite: what a lens module is once an integration has transformed it. The lens is its compiled
 * form, what only its body used is gone, and every other line is where it was.
 */
import path from "node:path";

import { parseSync } from "oxc-parser";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { parseModule, rewrite, transform } from "../../../src/solidity/build/index.js";
import { positionIn } from "../helpers/harness.js";

import { bin, Project } from "./helpers/project.js";

const DIR = path.join(import.meta.dirname, "..", "__fixtures__");

/** Transforms the first of several modules held in memory, as an integration would. */
const transformSnippet = async (files: Readonly<Record<string, string>>) => {
  const modules = new Map(Object.entries(files).map(([name, code]) => [path.join(DIR, name), code]));
  const [entry] = modules.keys();
  const dependencies: string[] = [];
  const result = await transform(
    modules.get(entry as string) as string,
    entry as string,
    {
      resolve: (specifier, importer) => {
        const file = path.resolve(path.dirname(importer), specifier).replace(/\.js$/, ".ts");
        return modules.has(file) ? file : undefined;
      },
      addDependency: (file) => dependencies.push(path.basename(file)),
      readFile: (file) => modules.get(file) as string,
    },
    { cache: false },
  );
  return { result, dependencies };
};

const source = `import fs from "node:fs";
import {
  contract,
  internal,
  pure,
  Struct,
  uint256,
} from "@morpho-org/viem-dlc/solidity"
import { half } from "./math.js";
import { LABEL, WAD } from "./units.js";

const SHARED = 2n;

class Pair extends Struct({ low: uint256, high: uint256 }) {}

const double = internal(pure(uint256).returns(uint256), (x) => x * SHARED);

export const triple = internal(
  pure(uint256).returns(uint256),
  (x) => x * 3n,
);

export const PairLens = contract("PairLens", {
  pair: pure(uint256).returns(Pair),
}).implementation({
  pair(x) {
    const o = new Pair();
    o.low = half(double(x));
    o.high = triple(x) * WAD;
    return o;
  },
});

export const label = \`\${LABEL} \${SHARED} \${fs.constants.F_OK}\`;
export const marker = "after the lens";
`;

const modules = {
  "main.ts": source,
  "math.ts": `
    import { internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
    export const half = internal(pure(uint256).returns(uint256), (x) => x / 2n);
  `,
  "units.ts": `
    export const WAD = 10n ** 18n;
    export const LABEL = "wad";
  `,
};

describe("the rewritten module", () => {
  it("holds the compiled lens in place of the lens, and nothing its body alone used", async () => {
    const { result } = await transformSnippet(modules);
    if (!result) throw new Error("the module holds a lens");
    const { code, lenses } = result;
    const [lens] = lenses;

    // Still a module the host can parse, importing the runtime wrapper beside the package's other names.
    expect(parseSync("main.ts", code).errors).toEqual([]);
    expect(code).toContain(';import { compiledLens as __viemDlcCompiledLens } from "@morpho-org/viem-dlc/solidity";');
    const compiled = /\/\*#__PURE__\*\/ __viemDlcCompiledLens\((\{.*\})\)/.exec(code);
    expect(JSON.parse(compiled?.[1] ?? "null")).toEqual({
      name: "PairLens",
      abi: lens?.artifact.abi,
      bytecode: lens?.artifact.bytecode,
      immutables: [],
    });

    // Gone: the body, the helper and the struct only the body used, and the imports only they used.
    expect(code).not.toMatch(
      /\.implementation\(|new Pair|class Pair|const double|math\.js|\bhalf\b|\bWAD\b|\bStruct\b/,
    );
    // Kept: what the module exports or still uses.
    expect(code).toContain("export const triple = internal(");
    expect(code).toContain("const SHARED = 2n;");
    expect(code).toContain('import { LABEL } from "./units.js";');
    expect(code).toContain('import fs from "node:fs";');
    expect(code).toMatch(/import \{\s*internal,\s*pure,\s*uint256,?\s*\} from "@morpho-org\/viem-dlc\/solidity"/);
  });

  it("keeps every remaining line on its line", async () => {
    const { result } = await transformSnippet(modules);
    const code = result?.code ?? "";
    expect(code.split("\n")).toHaveLength(source.split("\n").length);
    for (const kept of [
      "const SHARED",
      "export const triple",
      "export const PairLens",
      "export const label",
      "export const marker",
    ])
      expect(positionIn(code, kept).line, kept).toBe(positionIn(source, kept).line);
  });

  it("reports every file the lens was built from, and not the module itself", async () => {
    const { dependencies } = await transformSnippet(modules);
    expect(dependencies.sort()).toEqual(["math.ts", "solidity.config.json", "units.ts"]);
  });

  it("replaces each lens of a module, with one import of the wrapper", async () => {
    const two = `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

export const First = contract("First", { run: pure(uint256).returns(uint256) }).implementation({ run: (x) => x + 1n });
export const lenses = {
  second: contract("Second", { run: pure(uint256).returns(uint256) }).implementation({ run: (x) => x + 2n }),
};
`;
    const { result } = await transformSnippet({ "main.ts": two });
    const code = result?.code ?? "";
    expect(parseSync("main.ts", code).errors).toEqual([]);
    expect(code.match(/compiledLens as /g)).toHaveLength(1);
    expect([...code.matchAll(/__viemDlcCompiledLens\(\{"name":"(\w+)"/g)].map((match) => match[1])).toEqual([
      "First",
      "Second",
    ]);
    expect(result?.lenses[0]?.artifact.bytecode).not.toBe(result?.lenses[1]?.artifact.bytecode);
  });

  it("names its wrapper import apart from anything the module already says", async () => {
    const taken = `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

export const __viemDlcCompiledLens = "taken";
export const Lens = contract("Lens", { run: pure(uint256).returns(uint256) }).implementation({ run: (x) => x });
`;
    const { result } = await transformSnippet({ "main.ts": taken });
    expect(parseSync("main.ts", result?.code ?? "").errors).toEqual([]);
    expect(result?.code).toContain("compiledLens as __viemDlcCompiledLens_ }");
    expect(result?.code).toContain("/*#__PURE__*/ __viemDlcCompiledLens_({");
  });

  it("leaves a module without a lens to the host", async () => {
    const mentions = 'import { uint256 } from "@morpho-org/viem-dlc/solidity";\nexport const word = uint256;\n';
    expect((await transformSnippet({ "main.ts": mentions })).result).toBeNull();
    expect((await transformSnippet({ "main.ts": "export const one = 1;\n" })).result).toBeNull();
  });

  it("is the module as it was written when no lens was built from it", () => {
    expect(rewrite(parseModule(path.join(DIR, "main.ts"), source), [])).toBe(source);
  });

  it("puts its wrapper import beside a namespace import the lens is written through", async () => {
    const viaNamespace = `import * as sol from "@morpho-org/viem-dlc/solidity";

export const NamespaceLens = sol
  .contract("NamespaceLens", { one: sol.pure().returns(sol.uint256) })
  .implementation({ one: () => 1n });
export const word = sol.uint256;
`;
    const code = (await transformSnippet({ "main.ts": viaNamespace })).result?.code ?? "";
    expect(parseSync("main.ts", code).errors).toEqual([]);
    expect(code.split("\n")).toHaveLength(viaNamespace.split("\n").length);
    // On the line of the namespace import, which stays: the module still reads a name off it.
    expect(code.split("\n")[0]).toMatch(
      /^import \* as sol from "@morpho-org\/viem-dlc\/solidity";+import \{ compiledLens as __viemDlcCompiledLens \} from "@morpho-org\/viem-dlc\/solidity";$/,
    );
    expect(code).toContain("export const NamespaceLens = /*#__PURE__*/ __viemDlcCompiledLens({");
    expect(code).not.toContain(".implementation(");
    expect(code).toContain("export const word = sol.uint256;");
  });
});

/** A module with one lens whose only helper is `scale`, and whatever else the module says. */
const scaled = (
  rest: string,
  declaration = "const scale = internal(pure(uint256).returns(uint256), (x) => x * 123456789n);",
) =>
  `import { contract, internal, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

${declaration}

export const ScaleLens = contract("ScaleLens", { read: pure(uint256).returns(uint256) }).implementation({
  read: (x) => scale(x),
});

${rest}
`;

describe("what the rewritten module keeps, by binding", () => {
  const rewritten = async (source: string) => {
    const code = (await transformSnippet({ "main.ts": source })).result?.code ?? "";
    expect(parseSync("main.ts", code).errors).toEqual([]);
    expect(code.split("\n")).toHaveLength(source.split("\n").length);
    return code;
  };
  const HELPER = /123456789n|\binternal\b/;

  it.each([
    ["a property key", "export const markers = { scale: 3 };"],
    ["a property read", "export const read = (markers: { scale: number }) => markers.scale;"],
    [
      "a method and a field of a class",
      "export class Ruler {\n  scale = 3;\n  static scale() {\n    return 4;\n  }\n}",
    ],
    ["a parameter", "export function identity(scale: bigint) {\n  return scale;\n}"],
    ["a parameter of an arrow function", "export const identity = (scale: bigint) => scale;"],
    ["a destructured parameter", "export const pick = ({ scale }: { scale: bigint }) => scale;"],
    ["a local of a function", "export function three() {\n  const scale = 3n;\n  return scale;\n}"],
    ["a local of a block", "export const sizes: bigint[] = [];\n{\n  const scale = 3n;\n  sizes.push(scale);\n}"],
    ["a loop variable", "export const sizes: bigint[] = [];\nfor (const scale of [3n]) sizes.push(scale);"],
    [
      "a caught error",
      "export const attempt = () => {\n  try {\n    return 1;\n  } catch (scale) {\n    return scale;\n  }\n};",
    ],
    ["a label", "export const first = () => {\n  scale: for (;;) break scale;\n};"],
    [
      "a local of a switch",
      "export const pick = (on: number) => {\n  switch (on) {\n    default:\n      const scale = 3n;\n      return scale;\n  }\n};",
    ],
    [
      "a function declared in a block",
      "export const sizes: bigint[] = [];\n{\n  function scale() {\n    return 3n;\n  }\n  sizes.push(scale());\n}",
    ],
    ["a var declared in a block", "export function three() {\n  {\n    var scale = 3n;\n  }\n  return scale;\n}"],
    [
      "a value a namespace exports",
      "export namespace Ruler {\n  export const scale = 3n;\n  export const twice = scale * 2n;\n}",
    ],
    [
      "a value an ambient module declares and lists as an export",
      'declare module "ruler" {\n  const scale: bigint;\n  export { scale };\n}',
    ],
    ["a name another module exports", 'export { scale as ruler } from "./ruler.js";'],
    ["the far side of an export alias", "const ruler = 3;\nexport { ruler as scale };"],
    ["a parameter in a type", "export type Ruler = (scale: bigint) => bigint;"],
  ])("%s spelled like a helper does not keep the helper", async (_what, rest) => {
    const code = await rewritten(scaled(rest));
    expect(code).not.toMatch(HELPER);
    // The rest of the module is as it was written.
    for (const line of rest.split("\n")) expect(code).toContain(line);
  });

  it.each([
    ["a shorthand property", "export const markers = { scale };"],
    ["a property value", "export const markers = { ruler: scale };"],
    ["a computed key", "export const markers = { [String(scale)]: 3 };"],
    ["a use inside a function", "export function ruler() {\n  return scale;\n}"],
    [
      "a use inside a function that declares the name deeper",
      "export function ruler(deep: boolean) {\n  if (deep) {\n    const scale = 3n;\n    return scale;\n  }\n  return scale;\n}",
    ],
    ["a default of a parameter", "export const ruler = (helper = scale) => helper;"],
    [
      "a default of a parameter, beside a var of the function's body",
      "export function ruler(helper = scale) {\n  var scale = 3n;\n  return [helper, scale];\n}",
    ],
    [
      "the discriminant of a switch, beside a const of one of its cases",
      "export const pick = () => {\n  switch (scale) {\n    default:\n      const scale = 3n;\n      return scale;\n  }\n};",
    ],
    [
      "a function, beside a var of a static block of a class",
      "export function ruler() {\n  class Inner {\n    static {\n      var scale = 3n;\n    }\n  }\n  return [Inner, scale];\n}",
    ],
    ["an export under another name", "export { scale as ruler };"],
    ["a use in a type", "export type Ruler = typeof scale;"],
  ])("%s that reads the helper keeps it", async (_what, rest) => {
    const code = await rewritten(scaled(rest));
    expect(code).toContain("const scale = internal(pure(uint256).returns(uint256), (x) => x * 123456789n);");
    for (const line of rest.split("\n")) expect(code).toContain(line);
  });

  it.each([
    [
      "first",
      "const scale = internal(pure(uint256).returns(uint256), (x) => x * 123456789n), appValue = 1n;",
      "const appValue = 1n;",
    ],
    [
      "last",
      "const appValue = 1n, scale = internal(pure(uint256).returns(uint256), (x) => x * 123456789n);",
      "const appValue = 1n;",
    ],
    [
      "between two others, on lines of its own",
      "const appValue = 1n,\n  scale = internal(pure(uint256).returns(uint256), (x) => x * 123456789n),\n  otherValue = 2n;",
      "const appValue = 1n,\n  otherValue = 2n;",
    ],
  ])(
    "a helper declared %s in a statement goes, and the values declared beside it stay",
    async (_where, declaration, kept) => {
      const rest = 'export { appValue };\nexport const marker = "after the lens";';
      const source = scaled(rest, declaration);
      const code = await rewritten(source);
      expect(code).not.toMatch(HELPER);
      expect(code.replace(/\s+/g, " ")).toContain(kept.replace(/\s+/g, " "));
      expect(code).toContain("export { appValue };");
      expect(positionIn(code, "export const marker").line).toBe(positionIn(source, "export const marker").line);
    },
  );

  it("two helpers declared in one statement go with the statement", async () => {
    const declaration =
      "const scale = internal(pure(uint256).returns(uint256), (x) => twice(x) * 123456789n),\n  twice = internal(pure(uint256).returns(uint256), (x) => x * 2n);";
    const code = await rewritten(scaled('export const marker = "after the lens";', declaration));
    expect(code).not.toMatch(HELPER);
    expect(code).not.toMatch(/\bconst\s*;|\bscale\b|\btwice\b/);
  });
});

describe("the rewritten module, bundled as a library whose runtime is external", { timeout: 120_000 }, () => {
  let project: Project;
  beforeAll(() => {
    project = new Project("app");
  });
  afterAll(() => project?.remove());

  it("ships no helper body for a name the module also uses as a property key, and keeps the module's exports", async () => {
    project.write(
      "library/index.ts",
      scaled(
        "export const markers = { scale: 3 };",
        "const scale = internal(pure(uint256).returns(uint256), (x) => x * 123456789n), appValue = 1n;\nexport { appValue };",
      ),
    );
    project.write(
      "vite.library.config.ts",
      `import { solidity } from "@morpho-org/viem-dlc/solidity/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [solidity()],
  build: {
    minify: false,
    outDir: "dist/library",
    lib: { entry: "library/index.ts", formats: ["es"], fileName: "index" },
    rollupOptions: { external: ["viem", "@morpho-org/viem-dlc/solidity"] },
  },
});
`,
    );
    const run = await project.run([bin("vite"), "build", "--config", "vite.library.config.ts"]);
    expect(run.ok, run.output).toBe(true);
    const bundle = project.read("dist/library/index.js");

    // With the runtime external, the bundler cannot know a call to `internal` is free of effects: what
    // the rewrite leaves is what ships.
    expect(bundle).not.toMatch(/123456789n|\binternal\(|\bpure\(/);
    expect(bundle).toMatch(/import \{ compiledLens \} from "@morpho-org\/viem-dlc\/solidity"/);
    expect(bundle).toContain("const markers = { scale: 3 }");
    expect(bundle).toContain("const appValue = 1n");
    expect(bundle).toMatch(/export \{\s*ScaleLens,\s*appValue,\s*markers\s*\}/);
    expect(bundle).toMatch(/"bytecode": ?"0x60[0-9a-f]+"/);
  });
});

describe("the rewritten module, run", () => {
  let project: Project;
  beforeAll(() => {
    project = new Project("app");
  });
  afterAll(() => project?.remove());

  it("fails on the line the source has it on", async () => {
    // The lens, its helper import and its constant import all come before the line that throws.
    const script = `${project.read("src/lens.ts")}
const reached: string = QuoteLens.name;
throw new Error(\`reached \${reached}\`);
`;
    project.write("src/throws.ts", script);
    const run = await project.run(["--import", "@morpho-org/viem-dlc/solidity/register", "src/throws.ts"]);
    expect(run.ok).toBe(false);
    expect(run.output).toContain("Error: reached QuoteLens");
    expect(run.output).toContain(`src/throws.ts:${positionIn(script, "throw new Error").line}`);
  });

  it("still has a constant its own code reads where a nearer binding of the name is not yet in scope", async () => {
    // The lens reads the constant too, so the rewrite has to tell the module's own read of it from the
    // case's binding of the same name. What is run is the module's function, not the lens.
    project.write(
      "src/tiers.ts",
      `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";

const rate = 3n;
export const RateLens = contract("RateLens", { read: pure().returns(uint256) }).implementation({ read: () => rate });

export function tier() {
  switch (rate) {
    case 3n:
      return "low";
    default:
      const rate = "high";
      return rate;
  }
}

console.log("tier", tier(), "of", RateLens.name);
`,
    );
    const run = await project.run(["--import", "@morpho-org/viem-dlc/solidity/register", "src/tiers.ts"]);
    expect(run.ok, run.output).toBe(true);
    expect(run.output).toContain("tier low of RateLens");
  });
});
