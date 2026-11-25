/**
 * The two integrations that run without a bundler: the `--import` hook, and the check command.
 * Runs the built package: `pnpm test` builds first.
 */
import path from "node:path";

import { type Hex, toFunctionSelector } from "viem";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { build } from "../../../src/solidity/build/index.js";
import { type Evm, requireAnvil, startAnvil } from "../helpers/anvil.js";
import { positionIn } from "../helpers/harness.js";

import { describeHost } from "./helpers/host-suite.js";
import { lastPrintedBytecode, Project, waitFor } from "./helpers/project.js";
import {
  expectHostResolution,
  REFUSED,
  REPORT,
  REPORTED_NAMES,
  type Reported,
  writeRefusedImport,
  writeResolutionFixture,
} from "./helpers/resolution.js";

const HOOK = ["--import", "@morpho-org/viem-dlc/solidity/register", "script.ts"];
const CLI = "node_modules/@morpho-org/viem-dlc/dist/solidity/build/adapters/cli.js";

describeHost({
  name: "the Node hook",
  template: "app",
  run: (project) => project.run(HOOK),
  served: (_project, run) => lastPrintedBytecode(run.output),
  async watch(project) {
    const watcher = project.start(["--watch", ...HOOK]);
    return { served: () => lastPrintedBytecode(watcher.output()), output: watcher.output, stop: watcher.stop };
  },
});

describeHost({
  name: "the check command",
  template: "app",
  run: (project) => project.run([project.file(CLI), "check", "--print"]),
  // The command serves no bytecode: it prints each lens's size and, with --print, its Solidity.
  served(_project, run) {
    const printed = /^ok {2}QuoteLens {2}src\/lens\.ts {2}(\d+) bytes\n([\s\S]*?)\n\d+ files scanned/m.exec(run.output);
    return printed ? `${printed[1]} bytes\n${printed[2]}` : undefined;
  },
  expected: (lens) => `${lens.bytecode.length / 2 - 1} bytes\n${lens.solidity}`,
});

describe("the Node hook and the check command, beyond the shared suite", { timeout: 120_000 }, () => {
  let project: Project;
  let evm: Evm | undefined;
  beforeAll(async () => {
    project = new Project("app");
    if (requireAnvil()) evm = await startAnvil();
  }, 60_000);
  afterAll(() => {
    evm?.stop();
    project?.remove();
  });

  // What Node itself loads for an import depends on the process: its conditions, and the files that exist.
  // A lens reads an import that names one file whatever the conditions, and no other.
  describe.each([{ flags: [] }, { flags: ["--conditions=fixture"] }])("under node $flags", ({ flags }) => {
    it.skipIf(!requireAnvil())("a lens reads the module the script imports", async () => {
      project.restore();
      writeResolutionFixture(project, "resolved");
      project.write(
        "resolved/script.ts",
        `import { ${REPORTED_NAMES} } from "./lens.ts";\n\nconsole.log(${REPORT});\n`,
      );

      const hook = await project.run([
        ...flags,
        "--import",
        "@morpho-org/viem-dlc/solidity/register",
        "resolved/script.ts",
      ]);
      expect(hook.ok, hook.output).toBe(true);
      const reported = JSON.parse(hook.output) as Reported;

      // The check command, in a process run the same way, prints the Solidity of the same lens.
      const check = await project.run([...flags, project.file(CLI), "check", "--print", "resolved"]);
      expect(check.ok, check.output).toBe(true);
      const printed = /^ok {2}ResolvedLens {2}\S+ {2}\d+ bytes\n([\s\S]*?)\n\d+ files scanned/m.exec(check.output)?.[1];
      expect(printed, check.output).toBeDefined();

      await expectHostResolution(evm as Evm, reported, { printed });
    });

    it.each(REFUSED)("$name is refused at the import, by the hook and by the check command", async (refused) => {
      project.restore();
      const line = writeRefusedImport(project, "refused/lens.ts", refused);
      project.write(
        "refused/script.ts",
        'import { RATE, RefusedLens } from "./lens.ts";\n\nconsole.log(RefusedLens.bytecode, RATE);\n',
      );
      const hook = await project.run([
        ...flags,
        "--import",
        "@morpho-org/viem-dlc/solidity/register",
        "refused/script.ts",
      ]);
      expect(hook.ok, hook.output).toBe(false);
      expect(hook.output).toContain(`LensError: ${project.file(line)}`);

      const check = await project.run([...flags, project.file(CLI), "check", "refused"]);
      expect(check.ok, check.output).toBe(false);
      expect(check.output).toContain(`${line}\n`);
      expect(check.output).toContain("0 lenses compiled, 1 files failed");
    });
  });

  // Node runs the newest resolve hook first. One registered before the build's is asked with the import
  // itself; one registered after it sees only the build's own question, so the build cannot follow it.
  describe("a resolve hook of the application's, which sends one import of the lens module to another file", () => {
    const LENS = `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { RATE } from "./units.js";

export { RATE };

export const HookedLens = contract("HookedLens", { rate: pure().returns(uint256) }).implementation({
  rate: () => RATE,
});
`;
    const redirect = `specifier === "./units.js" && context.parentURL?.endsWith("/hooked/lens.ts") ? "./selected.js" : specifier`;
    const REGISTER = "@morpho-org/viem-dlc/solidity/register";
    const writeHooked = () => {
      project.restore();
      project.write("hooked/lens.ts", LENS);
      project.write("hooked/units.js", "export const RATE = 17n;\n");
      project.write("hooked/selected.js", "export const RATE = 29n;\n");
      project.write(
        "hooked/script.ts",
        'import { HookedLens, RATE } from "./lens.ts";\n\nconsole.log(JSON.stringify({ bytecode: HookedLens.bytecode, imported: String(RATE) }));\n',
      );
      project.write(
        "hooked/alias.mjs",
        `import { registerHooks } from "node:module";

registerHooks({ resolve: (specifier, context, nextResolve) => nextResolve(${redirect}, context) });
`,
      );
      project.write(
        "hooked/alias-off-thread.mjs",
        'import { register } from "node:module";\n\nregister("./alias-off-thread-hooks.mjs", import.meta.url);\n',
      );
      project.write(
        "hooked/alias-off-thread-hooks.mjs",
        `export const resolve = (specifier, context, nextResolve) => nextResolve(${redirect}, context);\n`,
      );
    };
    /** The lens the script printed returns, when executed, the value the script's own import gave: 29. */
    const expectFollowed = async (imports: readonly string[]) => {
      const run = await project.run([...imports.flatMap((module) => ["--import", module]), "hooked/script.ts"]);
      expect(run.ok, run.output).toBe(true);
      const reported = JSON.parse(run.output.slice(run.output.lastIndexOf("{"))) as { bytecode: Hex; imported: string };
      expect(reported.imported).toBe("29");
      const lens = await (evm as Evm).deploy(reported.bytecode);
      const returned = await (evm as Evm).call(lens, toFunctionSelector("rate()"));
      expect(returned.status).toBe("success");
      expect(String(BigInt(returned.data))).toBe(reported.imported);
    };

    it.skipIf(!requireAnvil())("registered before the build's hook, is followed", async () => {
      writeHooked();
      await expectFollowed(["./hooked/alias.mjs", REGISTER]);
    });

    it("registered after the build's hook, is refused at the import, naming the order to use", async () => {
      writeHooked();
      const run = await project.run(["--import", REGISTER, "--import", "./hooked/alias.mjs", "hooked/script.ts"]);
      expect(run.ok, run.output).toBe(false);
      const { line, column } = positionIn(LENS, '"./units.js"');
      expect(run.output).toContain(
        `LensError: ${project.file("hooked/lens.ts")}:${line}:${column} - error: cannot tell which file "./units.js" names: a resolve hook was registered with registerHooks after @morpho-org/viem-dlc/solidity/register, and can send this import to another file than a lens would read. Register @morpho-org/viem-dlc/solidity/register last: put its --import after the others.`,
      );
    });

    it.skipIf(!requireAnvil())("added with module.register, is followed in either order", async () => {
      writeHooked();
      await expectFollowed(["./hooked/alias-off-thread.mjs", REGISTER]);
      await expectFollowed([REGISTER, "./hooked/alias-off-thread.mjs"]);
    });
  });

  // A string names the package whatever escapes spell it, and Node imports the package either way.
  it("a lens whose module spells the package's specifier with an escape is compiled, by the hook and by the check command", async () => {
    project.restore();
    const lens = (specifier: string) => `import { contract, pure, uint256 } from "${specifier}";

export const SpelledLens = contract("SpelledLens", { rate: pure().returns(uint256) }).implementation({ rate: () => 17n });
`;
    project.write("spelled/plain.ts", lens("@morpho-org/viem-dlc/solidity"));
    project.write("spelled/escaped.ts", lens("@morpho-org/viem-dlc/solidi\\u0074y"));
    expect(project.read("spelled/escaped.ts")).not.toContain("@morpho-org/viem-dlc/solidity");
    project.write(
      "spelled/script.ts",
      `import { SpelledLens as escaped } from "./escaped.ts";
import { SpelledLens as plain } from "./plain.ts";

console.log(JSON.stringify({ plain: plain.bytecode, escaped: escaped.bytecode }));
`,
    );

    const hook = await project.run(["--import", "@morpho-org/viem-dlc/solidity/register", "spelled/script.ts"]);
    expect(hook.ok, hook.output).toBe(true);
    const served = JSON.parse(hook.output) as { plain: string; escaped: string };
    expect(served.plain).toMatch(/^0x60[0-9a-f]+$/);
    expect(served.escaped).toBe(served.plain);

    const check = await project.run([project.file(CLI), "check", "spelled"]);
    expect(check.ok, check.output).toBe(true);
    expect(check.output).toContain("2 lenses compiled, 0 files failed");
  });

  // A contract taken from a dynamic import is one the build does not see. The import is an error where
  // it is written, not a lens that throws on first use.
  it("a dynamic import of the package is refused where it is written, by the hook and by the check command", async () => {
    project.restore();
    const lens = `const { contract, pure, uint256 } = await import("@morpho-org/viem-dlc/solidity");

export const DynamicLens = contract("DynamicLens", { rate: pure().returns(uint256) }).implementation({ rate: () => 17n });
`;
    project.write("dynamic/lens.ts", lens);
    project.write(
      "dynamic/script.ts",
      'import { DynamicLens } from "./lens.ts";\n\nconsole.log(DynamicLens.bytecode);\n',
    );
    const { line, column } = positionIn(lens, 'import("');
    const refusal = `dynamic/lens.ts:${line}:${column} - error: this loads the package when the module runs`;

    const hook = await project.run(["--import", "@morpho-org/viem-dlc/solidity/register", "dynamic/script.ts"]);
    expect(hook.ok, hook.output).toBe(false);
    expect(hook.output).toContain(`LensError: ${project.file(refusal)}`);

    const check = await project.run([project.file(CLI), "check", "dynamic"]);
    expect(check.ok, check.output).toBe(false);
    expect(check.output).toContain(refusal);
    expect(check.output).toContain("0 lenses compiled, 1 files failed");
  });

  // Node loads no package.json a script's import is resolved through, so only the build can report it.
  it("an edit to only the package.json an import is resolved through restarts node --watch, which then serves the other file", async () => {
    project.restore();
    const manifest = (target: string) =>
      `${JSON.stringify({ name: "fixture", private: true, type: "module", imports: { "#unit": `./mapped/${target}.ts` } }, null, 2)}\n`;
    project.write("package.json", manifest("a"));
    project.write("mapped/a.ts", "export const UNIT = 17n;\n");
    project.write("mapped/b.ts", "export const UNIT = 29n;\n");
    const lens = `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { UNIT } from "#unit";

export const MappedLens = contract("MappedLens", { unit: pure().returns(uint256) }).implementation({ unit: () => UNIT });
`;
    project.write("mapped/lens.ts", lens);
    project.write(
      "mapped/script.ts",
      'import { MappedLens } from "./lens.ts";\n\nconsole.log(JSON.stringify({ bytecode: MappedLens.bytecode }));\n',
    );
    /** The lens built with "#unit" resolved by hand. */
    const builtWith = async (target: string) => {
      const built = await build(
        lens,
        project.file("mapped/lens.ts"),
        { resolve: () => project.file(`mapped/${target}.ts`), addDependency: () => {} },
        { cache: false },
      );
      return built?.lenses[0]?.artifact.bytecode;
    };
    const fromA = await builtWith("a");
    project.write("package.json", manifest("b"));
    const fromB = await builtWith("b");
    expect(fromB).not.toBe(fromA);
    project.write("package.json", manifest("a"));

    const watcher = project.start([
      "--watch",
      "--import",
      "@morpho-org/viem-dlc/solidity/register",
      "mapped/script.ts",
    ]);
    const serves = (want: string | undefined, when: string) =>
      waitFor(
        () => (lastPrintedBytecode(watcher.output()) === want ? want : undefined),
        `node --watch to serve the lens ${when}`,
        watcher.output,
      );
    try {
      await serves(fromA, "as first built");
      project.write("package.json", manifest("b"));
      await serves(fromB, "after the edit to package.json");
    } finally {
      await watcher.stop();
    }
  });

  describe("an alias of tsconfig.json, which only the build reads", () => {
    const ALIASED = `import { contract, pure, uint256 } from "@morpho-org/viem-dlc/solidity";
import { UNIT } from "@units";

export const AliasLens = contract("AliasLens", { read: pure().returns(uint256) }).implementation({ read: () => UNIT });
`;
    // With a comment and a trailing comma, as tsc accepts them.
    const tsconfig = (target: string) => `{
  // The alias is all this project configures.
  "compilerOptions": { "paths": { "@units": ["./${target}"], }, },
}
`;
    /** Builds the lens twice in one process, with an edit to one configuration file in between. */
    const BUILD_TWICE = `import fs from "node:fs";
import path from "node:path";

import { transformSync } from "@morpho-org/viem-dlc/solidity/build";

import { resolveSync } from "../node_modules/@morpho-org/viem-dlc/dist/solidity/build/adapters/node-resolve.js";

const [edited, text] = process.argv.slice(2);
const file = path.resolve("aliased/lens.ts");
const once = () => {
  const dependencies = [];
  const addDependency = (dependency) => dependencies.push(path.relative(process.cwd(), dependency));
  const result = transformSync(
    fs.readFileSync(file, "utf8"),
    file,
    { resolve: (specifier, importer) => resolveSync(specifier, importer, addDependency), addDependency },
    { cache: false },
  );
  return { returns: /return ([^;]+);/.exec(result.lenses[0].printed.solidity)[1], dependencies: dependencies.sort() };
};
const before = once();
fs.writeFileSync(edited, text);
console.log(JSON.stringify([before, once()]));
`;
    const writeAliased = () => {
      project.write("aliased/lens.ts", ALIASED);
      project.write("aliased/a.ts", "export const UNIT = 17n;\n");
      project.write("aliased/b.ts", "export const UNIT = 29n;\n");
      project.write("aliased/build-twice.mjs", BUILD_TWICE);
    };

    it.each([
      {
        where: "the nearest tsconfig.json",
        files: { "aliased/tsconfig.json": tsconfig("a.ts") },
        edit: ["aliased/tsconfig.json", tsconfig("b.ts")] as const,
      },
      {
        where: "a file it extends",
        files: {
          "aliased/tsconfig.json": '{ "extends": "./tsconfig.base" }\n',
          "aliased/tsconfig.base.json": tsconfig("a.ts"),
        },
        edit: ["aliased/tsconfig.base.json", tsconfig("b.ts")] as const,
      },
    ])(
      "an edit to only $where reaches the next build of the same process, and the file is a dependency",
      async ({ files, edit }) => {
        writeAliased();
        for (const [file, content] of Object.entries(files)) project.write(file, content);
        const run = await project.run(["aliased/build-twice.mjs", ...edit]);
        expect(run.ok, run.output).toBe(true);
        const dependencies = (helper: string) => [helper, ...Object.keys(files), "solidity.config.json"].sort();
        expect(JSON.parse(run.output)).toEqual([
          { returns: "(17)", dependencies: dependencies("aliased/a.ts") },
          { returns: "(29)", dependencies: dependencies("aliased/b.ts") },
        ]);
      },
    );

    it("an edit to only tsconfig.json restarts node --watch, which then serves the other helper", async () => {
      project.restore();
      // The fixture lens imports its helper as "@/lib/math", which only the project's tsconfig.json maps.
      project.write("other/lib/math.ts", project.read("src/lib/math.ts").replace("/ d;", "/ d + 7n;"));
      /** The lens built with "@/lib/math" resolved by hand, to the helper under the given directory. */
      const builtWith = async (directory: string) => {
        const file = project.file("src/lens.ts");
        const built = await build(
          project.read("src/lens.ts"),
          file,
          {
            resolve: (specifier, importer) =>
              specifier === "@/lib/math"
                ? project.file(`${directory}/lib/math.ts`)
                : `${path.resolve(path.dirname(importer), specifier)}.ts`,
            addDependency: () => {},
          },
          { cache: false },
        );
        return built?.lenses[0]?.artifact.bytecode;
      };
      const [fromSrc, fromOther] = [await builtWith("src"), await builtWith("other")];
      expect(fromOther).not.toBe(fromSrc);

      const watcher = project.start(["--watch", ...HOOK]);
      const serves = (want: string | undefined, when: string) =>
        waitFor(
          () => (lastPrintedBytecode(watcher.output()) === want ? want : undefined),
          `node --watch to serve the lens ${when}`,
          watcher.output,
        );
      try {
        await serves(fromSrc, "as first built");
        project.edit("tsconfig.json", '"@/*": ["./src/*"]', '"@/*": ["./other/*"]');
        await serves(fromOther, "after the alias edit");
      } finally {
        await watcher.stop();
      }
    });
  });
});
