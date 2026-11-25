/**
 * The two integrations that run without a bundler: the `--import` hook, and the check command.
 * Runs the built package: `pnpm test` builds first.
 */
import path from "node:path";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { build } from "../../../src/solidity/build/index.js";
import { type Evm, requireAnvil, startAnvil } from "../helpers/anvil.js";

import { describeHost } from "./helpers/host-suite.js";
import { lastPrintedBytecode, Project, waitFor } from "./helpers/project.js";
import { expectHostResolution, REPORT, type Reported, VALUES, writeResolutionFixture } from "./helpers/resolution.js";

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
  describe.each([
    { flags: [], conditional: VALUES["fixture-units, by default"] },
    { flags: ["--conditions=fixture"], conditional: VALUES["fixture-units, under the condition 'fixture'"] },
  ])("under node $flags", ({ flags, conditional }) => {
    it.skipIf(!requireAnvil())("a lens reads the module the script imports", async () => {
      writeResolutionFixture(project, "resolved");
      project.write(
        "resolved/script.ts",
        `import { DUAL, ResolvedLens, SCALE, UNIT } from "./lens.ts";\n\nconsole.log(${REPORT});\n`,
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

      await expectHostResolution(evm as Evm, reported, conditional, { printed });
    });
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
