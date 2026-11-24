/**
 * The invariants every build integration has to hold, as one suite run against each host.
 *
 * A host is described by how to run it once, how to read the bytecode it then serves, and how to keep
 * it running in watch mode. The suite builds one fixture project and takes it through:
 *
 * - the first run: the lens compiles, the output holds no compiler, no parser and no lens source, no
 *   application module ran, and the working tree is as it was;
 * - solc hidden: a warm cache builds, a cold one fails and names the lens;
 * - staleness: an edit to only a helper file, or to only `solidity.config.json`, changes the bytecode
 *   served, in a second cold run and in watch mode;
 * - every kind of solc diagnostic, at its TypeScript file, line and column.
 */
import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { DIAGNOSTIC_CASES, expectedLine } from "./cases.js";
import { type BuiltFixtureLens, IGNORED, Project, type Run, waitFor } from "./project.js";

export interface Watch {
  /** The bytecode the host serves now, or undefined while it has none to give. */
  served(): string | undefined | Promise<string | undefined>;
  output(): string;
  stop(): Promise<void>;
}

export interface Host {
  name: string;
  template: "app" | "next";
  /** One cold run: a new process on the project as it stands. */
  run(project: Project): Promise<Run>;
  /** What the host serves after that run: the lens's bytecode, unless `expected` says otherwise. */
  served(project: Project, run: Run): string | undefined;
  /** What `served` gives for a lens, when that is not its bytecode. */
  expected?(lens: BuiltFixtureLens): string;
  /** The directories holding the host's output, and the caches inside them, when it writes any. */
  output?: { directories: readonly string[]; caches?: readonly string[] };
  /** The host in watch mode, when it has one. */
  watch?(project: Project): Promise<Watch>;
}

/** What must not reach production: the compiler, the parser, and the source of a lens. */
const COMPILER = /\bsolc\b|soljson/;
const PARSER = /oxc[-_]parser|oxc[-_]resolver|\bparseSync\b/;
const LENS_SOURCE = /mulDivDown|lensHelperProduct|lensBodyQuote/;

const HELPER = "src/lib/math.ts";
const CONFIG = "solidity.config.json";

export function describeHost(host: Host): void {
  describe(host.name, { timeout: 240_000 }, () => {
    let project: Project;
    let first: Run;
    let lens: BuiltFixtureLens;

    const expected = async () => {
      const built = await project.expectedLens();
      return { lens: built, served: host.expected ? host.expected(built) : built.bytecode };
    };
    /** Runs the host and expects it to serve the lens of the sources as they stand. */
    const runAndExpectCurrent = async (): Promise<string> => {
      const want = await expected();
      const run = await host.run(project);
      expect(run.ok, run.output).toBe(true);
      expect(host.served(project, run), run.output).toBe(want.served);
      return want.served;
    };

    beforeAll(async () => {
      project = new Project(host.template);
      lens = await project.expectedLens();
      first = await host.run(project);
    });
    afterAll(() => project?.remove());

    describe("the first run", () => {
      it("compiles the lens, its imports resolved through the host's alias", () => {
        expect(first.ok, first.output).toBe(true);
        expect(project.read("src/lens.ts")).toContain('from "@/lib/math"');
        expect(host.served(project, first), first.output).toBe(host.expected ? host.expected(lens) : lens.bytecode);
        expect(project.cachedLenses()).toHaveLength(1);
      });

      const { output } = host;
      it.skipIf(!output)("writes output with no compiler, no parser and no lens source", () => {
        if (!output) return;
        const search = (pattern: RegExp) => project.search(pattern, output.directories, output.caches);
        // The search finds what it looks for: the sources hold all three, and the output holds the lens.
        expect(project.search(LENS_SOURCE, ["src"])).not.toEqual([]);
        expect(project.search(COMPILER, ["node_modules/@morpho-org"])).not.toEqual([]);
        expect(project.search(PARSER, ["node_modules/@morpho-org"])).not.toEqual([]);
        expect(search(/0x60[0-9a-f]{64}/)).not.toEqual([]);
        expect(search(COMPILER)).toEqual([]);
        expect(search(PARSER)).toEqual([]);
        expect(search(LENS_SOURCE)).toEqual([]);
      });

      it("runs no application module", () => {
        expect(project.importLog()).toBe("");
      });

      it("leaves the working tree as it was, outside build output and the cache", () => {
        expect(project.changedFiles()).toEqual([]);
        expect(IGNORED).toEqual(expect.arrayContaining(project.ignoredEntries()));
      });
    });

    describe("with solc hidden", () => {
      afterAll(() => project.showSolc());

      it("a warm cache builds", async () => {
        project.hideSolc();
        await runAndExpectCurrent();
      });

      it("a cold cache fails the build, naming the lens", async () => {
        project.hideSolc();
        project.clean();
        expect(project.cachedLenses()).toEqual([]);
        const run = await host.run(project);
        expect(run.ok, run.output).toBe(false);
        expect(run.output).toMatch(
          /src\/lens\.ts:\d+:\d+ - error: the lens QuoteLens needs compiling: solc is not installed/,
        );
      });
    });

    describe("an edit changes the bytecode served", () => {
      it("in a second cold run, after editing only a helper file", async () => {
        const before = await runAndExpectCurrent();
        project.edit(HELPER, "return lensHelperProduct / d;", "return lensHelperProduct / d + 1n;");
        expect(await runAndExpectCurrent()).not.toBe(before);
      });

      it("in a second cold run, after editing only solidity.config.json", async () => {
        const before = await runAndExpectCurrent();
        project.edit(CONFIG, '"enabled": true', '"enabled": false');
        expect(await runAndExpectCurrent()).not.toBe(before);
      });

      const { watch: startWatch } = host;
      it.skipIf(!startWatch)(
        "in watch mode, after editing only a helper file, then only solidity.config.json",
        async () => {
          if (!startWatch) return;
          project.restore();
          const watch = await startWatch(project);
          try {
            const serves = async (want: string, after: string) =>
              waitFor(
                async () => ((await watch.served()) === want ? want : undefined),
                `the host to serve the lens ${after}`,
                watch.output,
              );
            const initial = (await expected()).served;
            await serves(initial, "as first built");

            project.edit(HELPER, "return lensHelperProduct / d;", "return lensHelperProduct / d + 2n;");
            const afterHelper = (await expected()).served;
            expect(afterHelper).not.toBe(initial);
            await serves(afterHelper, "after the helper edit");

            project.edit(CONFIG, '"enabled": true', '"enabled": false');
            const afterConfig = (await expected()).served;
            expect(afterConfig).not.toBe(afterHelper);
            await serves(afterConfig, "after the configuration edit");
          } finally {
            await watch.stop();
          }
        },
      );
    });

    describe("reports what solc reports, at the TypeScript position", () => {
      for (const diagnosticCase of DIAGNOSTIC_CASES) {
        it(diagnosticCase.kind, async () => {
          project.restore();
          for (const [file, content] of Object.entries(diagnosticCase.files)) project.write(file, content);
          const run = await host.run(project);
          expect(run.ok, run.output).toBe(diagnosticCase.severity === "warning");
          expect(run.output).toContain(expectedLine(diagnosticCase, project.read(diagnosticCase.file)));
        });
      }
    });

    it("has still run no application module, after every run above", () => {
      expect(project.importLog()).toBe("");
      expect(fs.existsSync(project.file("imported.log"))).toBe(false);
    });
  });
}
