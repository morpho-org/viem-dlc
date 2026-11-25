/**
 * Invariants of the package as a whole, which no single integration owns. The ones each integration
 * has to hold are in `integrations/helpers/host-suite.ts`.
 *
 * Runs the built package: `pnpm test` builds first.
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { resolveSync } from "../../src/solidity/build/adapters/node-resolve.js";
import { build } from "../../src/solidity/build/index.js";

import { bin, installPackage, Project, ROOT } from "./integrations/helpers/project.js";

describe("the packed package, installed without its optional peers", () => {
  let dir: string;

  beforeAll(() => {
    // Outside the repository, so nothing is found by walking up: node_modules holds the tarball's files,
    // and viem and valibot, which the package's manifest requires. solc, the parser and the resolver are
    // not there.
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "viem-dlc-consumer-"));
    installPackage(dir);
    for (const name of ["viem", "valibot"])
      fs.symlinkSync(fs.realpathSync(path.join(ROOT, "node_modules", name)), path.join(dir, "node_modules", name));
    fs.writeFileSync(path.join(dir, "package.json"), '{ "name": "consumer", "private": true, "type": "module" }\n');
  });
  afterAll(() => fs.rmSync(dir, { recursive: true, force: true }));

  /** Runs a module's source in the consumer directory and returns the JSON it prints. */
  const evaluate = (source: string): unknown => {
    const result = spawnSync(process.execPath, ["--input-type=module", "--eval", source], {
      cwd: dir,
      encoding: "utf8",
    });
    expect(result.status, result.stderr).toBe(0);
    return JSON.parse(result.stdout);
  };

  it("has no compiler, parser, resolver or scope analyzer to find", () => {
    const found = evaluate(`
      import { createRequire } from "node:module";
      const require = createRequire(process.cwd() + "/");
      const resolves = (name) => { try { require.resolve(name); return true; } catch { return false; } };
      console.log(JSON.stringify(Object.fromEntries(["viem", "solc", "oxc-parser", "oxc-resolver", "@typescript-eslint/scope-manager"].map((name) => [name, resolves(name)]))));
    `);
    expect(found).toEqual({
      viem: true,
      solc: false,
      "oxc-parser": false,
      "oxc-resolver": false,
      "@typescript-eslint/scope-manager": false,
    });
  });

  it("imports the runtime entries, and a compiled lens works", () => {
    const lens = evaluate(`
      const { compiledLens, uint256 } = await import("@morpho-org/viem-dlc/solidity");
      const { mload } = await import("@morpho-org/viem-dlc/solidity/yul");
      const lens = compiledLens({ name: "Compiled", abi: [], bytecode: "0x00", immutables: [] });
      console.log(JSON.stringify({ stubs: [typeof uint256, typeof mload], name: lens.name, call: Object.keys(lens.with()) }));
    `);
    expect(lens).toEqual({
      stubs: ["function", "function"],
      name: "Compiled",
      call: ["abi", "address", "factory", "factoryData"],
    });
  });

  it("loads a Next configuration, as next start does where nothing is built", () => {
    const cacheKey = evaluate(`
      const { withSolidity } = await import("@morpho-org/viem-dlc/solidity/next");
      console.log(JSON.stringify(withSolidity({}).turbopack.rules["*.{ts,tsx}"].loaders[0].options.cacheKey));
    `);
    expect(cacheKey).toMatch(/^\d+\.\d+\.\d+[^+]*\+absent\+absent\+absent$/);
  });

  it("cannot import the build half, which is what needs them", () => {
    const failure = evaluate(`
      const failed = (specifier) => import(specifier).then(() => "imported", (error) => error.code + ": " + error.message);
      console.log(JSON.stringify([
        await failed("@morpho-org/viem-dlc/solidity/build"),
        await failed("@morpho-org/viem-dlc/solidity/vite"),
      ]));
    `);
    const missing = /^ERR_MODULE_NOT_FOUND: Cannot find package '(?:oxc-parser|@typescript-eslint\/scope-manager)'/;
    expect(failure).toEqual([expect.stringMatching(missing), expect.stringMatching(missing)]);
  });
});

/** Every fixture whose lens reads helpers or constants from other modules, and how tsc emits those. */
const HELPER_FIXTURES = [
  {
    name: "the helper library",
    template: "library",
    // The library as its own project emits it, file by file.
    tsc: ["--project", "helpers/tsconfig.json"],
    helpers: "helpers",
    modules: ["fees", "index", "math", "types", "units"],
    // The lens names the library as a package would be named.
    alias: { helpers: "helpers/index.ts" } as Record<string, string>,
    typescriptOnly: /\bas uint256\b|y!|\bsatisfies\b|: bigint|\(x: uint256|export type/,
    helper: "function mulWad(uint256 x, uint256 y) internal pure returns (uint256)",
  },
  {
    name: "the application's own modules",
    template: "app",
    // Emitted on their own: the project's configuration emits nothing.
    tsc: [
      ..."--ignoreConfig --target ES2022 --module ESNext --moduleResolution Bundler --types node --skipLibCheck".split(
        " ",
      ),
      ..."--rootDir src/lib --outDir built/tsc src/lib/constants.ts src/lib/math.ts".split(" "),
    ],
    helpers: "src/lib",
    modules: ["constants", "math"],
    alias: {} as Record<string, string>,
    typescriptOnly: undefined,
    helper: "function mulDivDown(uint256 x, uint256 y, uint256 d) internal pure returns (uint256)",
  },
] as const;

describe.each(HELPER_FIXTURES)("$name, built to JavaScript", (fixture) => {
  let project: Project;

  beforeAll(() => {
    project = new Project(fixture.template);
    // The modules as tsc emits them, file by file: built/tsc/*.js.
    const emitted = spawnSync(process.execPath, [bin("typescript", "tsc"), ...fixture.tsc], {
      cwd: project.dir,
      encoding: "utf8",
    });
    expect(emitted.status, emitted.stdout + emitted.stderr).toBe(0);
  });
  afterAll(() => project?.remove());

  /** Builds the fixture lens with its helper modules read as TypeScript source, or as tsc emitted them. */
  const buildAgainst = async (form: "source" | "built") => {
    const lensFile = project.file("src/lens.ts");
    const built = await build(
      fs.readFileSync(lensFile, "utf8"),
      lensFile,
      {
        resolve(specifier, importer) {
          const alias = fixture.alias[specifier];
          const file = alias ? project.file(alias) : resolveSync(specifier, importer);
          if (form === "source" || file === undefined) return file;
          const module = path.relative(project.file(fixture.helpers), file);
          return module.startsWith("..") ? file : project.file(`built/tsc/${module.replace(/\.ts$/, ".js")}`);
        },
        addDependency: () => {},
      },
      { cache: false },
    );
    const [lens] = built?.lenses ?? [];
    if (!built || !lens) throw new Error("the fixture holds no lens");
    return {
      solidity: lens.printed.solidity,
      bytecode: lens.artifact.bytecode,
      read: built.dependencies.map((file) => path.relative(project.dir, file)).sort(),
    };
  };

  it("gives the same Solidity as the TypeScript source", async () => {
    const fromSource = await buildAgainst("source");
    const fromBuilt = await buildAgainst("built");

    // Each was read from its own files.
    const { modules, helpers, typescriptOnly } = fixture;
    expect(fromSource.read).toEqual([...modules.map((name) => `${helpers}/${name}.ts`), "solidity.config.json"].sort());
    expect(fromBuilt.read).toEqual([...modules.map((name) => `built/tsc/${name}.js`), "solidity.config.json"].sort());
    // Where the source has what only TypeScript has, the built files have none of it.
    if (typescriptOnly) {
      expect(modules.map((name) => project.read(`${helpers}/${name}.ts`)).join("\n")).toMatch(typescriptOnly);
      for (const name of modules)
        expect(project.read(`built/tsc/${name}.js`).replace(/\/\/.*/g, "")).not.toMatch(typescriptOnly);
    }

    expect(fromBuilt.solidity).toBe(fromSource.solidity);
    expect(fromBuilt.bytecode).toBe(fromSource.bytecode);
    expect(fromSource.solidity).toContain(fixture.helper);
  });
});

describe("the fixture modules that log when they are imported", () => {
  let project: Project;
  beforeAll(() => {
    project = new Project("app");
  });
  afterAll(() => project?.remove());

  // The control for every "runs no application module" check: the log is written when one does run.
  it("do log when something imports them", async () => {
    expect(project.importLog()).toBe("");
    const run = await project.run([
      "--input-type=module",
      "--eval",
      'await import("./src/lib/math.ts"); await import("./src/lib/constants.ts");',
    ]);
    expect(run.ok, run.output).toBe(true);
    expect(project.importLog()).toBe("src/lib/math.ts\nsrc/lib/constants.ts\n");
  });
});
