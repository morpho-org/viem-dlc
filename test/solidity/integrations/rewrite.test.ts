/**
 * The rewrite: what a lens module is once an integration has transformed it. The lens is its compiled
 * form, what only its body used is gone, and every other line is where it was.
 */
import path from "node:path";

import { parseSync } from "oxc-parser";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { transform } from "../../../src/solidity/build/index.js";
import { positionIn } from "../helpers/harness.js";

import { Project } from "./helpers/project.js";

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
});
