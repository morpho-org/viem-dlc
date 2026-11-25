/**
 * The plugin in Vite: a production build, and the dev server as its watch mode.
 * Runs the built package: `pnpm test` builds first.
 */
import fs from "node:fs";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "../helpers/anvil.js";
import { positionIn } from "../helpers/harness.js";

import { describeHost, LENS_SOURCE } from "./helpers/host-suite.js";
import { bin, bytecodeIn, freePort, Project, waitFor } from "./helpers/project.js";
import {
  expectHostResolution,
  REFUSED,
  REPORT,
  REPORTED_NAMES,
  type Reported,
  writeRefusedImport,
  writeResolutionFixture,
} from "./helpers/resolution.js";
import { expectNoLensSourceInMaps } from "./helpers/source-maps.js";

describeHost({
  name: "Vite",
  template: "app",
  run: (project) => project.run([bin("vite"), "build"]),
  served: (project) => project.bytecodeUnder("dist"),
  output: { directories: ["dist"] },
  async watch(project) {
    const port = await freePort();
    const server = project.start([bin("vite"), "--host", "127.0.0.1", "--port", String(port), "--strictPort"]);
    return {
      // The module as the dev server hands it to a browser.
      async served() {
        try {
          return bytecodeIn(await (await fetch(`http://127.0.0.1:${port}/src/lens.ts`)).text());
        } catch {
          return undefined;
        }
      },
      output: server.output,
      stop: server.stop,
    };
  },
});

describe("a Vite build, beyond the shared suite", { timeout: 120_000 }, () => {
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

  describe("with source maps", () => {
    it.each([
      { sourcemap: "true", kind: "in files the scripts name", expected: { external: 1, inline: 0, linked: 1 } },
      { sourcemap: "inline", kind: "inline in the scripts", expected: { external: 0, inline: 1, linked: 0 } },
      { sourcemap: "hidden", kind: "in files no script names", expected: { external: 1, inline: 0, linked: 0 } },
    ])("$kind, no map holds a lens body", async ({ sourcemap, expected }) => {
      project.clean();
      const run = await project.run([bin("vite"), "build", "--sourcemap", sourcemap]);
      expect(run.ok, run.output).toBe(true);
      expect(project.bytecodeUnder("dist")).toBe((await project.expectedLens()).bytecode);
      const found = expectNoLensSourceInMaps(project, "dist");
      expect({ external: found.external.length, inline: found.inline.length, linked: found.linked.length }).toEqual(
        expected,
      );
    });

    // The source a map embeds for a module is what the module's loader returned. A module asked for with
    // a query or a fragment is another module of the same file, which Vite's own loader reads as written.
    it.each([
      { asked: "a query", suffix: "?revision=2" },
      { asked: "a fragment", suffix: "#second" },
    ])("a lens module asked for with $asked stops the build at the lens, and no map is written", async ({ suffix }) => {
      project.restore();
      project.clean();
      project.write(
        "versioned/entry.ts",
        `export { QuoteLens as plain } from "../src/lens.ts";\nexport { QuoteLens as versioned } from "../src/lens.ts${suffix}";\n`,
      );
      project.write(
        "vite.versioned.config.ts",
        `import { fileURLToPath } from "node:url";

import { solidity } from "@morpho-org/viem-dlc/solidity/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [solidity()],
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  build: {
    minify: false,
    sourcemap: true,
    outDir: "dist/versioned",
    lib: { entry: "versioned/entry.ts", formats: ["es"], fileName: "entry" },
    rollupOptions: { external: ["viem", "@morpho-org/viem-dlc/solidity"] },
  },
});
`,
      );
      const run = await project.run([bin("vite"), "build", "--config", "vite.versioned.config.ts"]);
      expect(run.ok, run.output).toBe(false);
      const { line, column } = positionIn(project.read("src/lens.ts"), 'contract("QuoteLens"');
      expect(run.output).toContain(
        `${project.file("src/lens.ts")}:${line}:${column} - error: the solidity plugin did not load`,
      );
      // Nothing was written that holds the lens: no script, and no map with the module as written.
      expect(
        fs.existsSync(project.file("dist/versioned")) ? project.search(LENS_SOURCE, ["dist/versioned"]) : [],
      ).toEqual([]);
    });
  });

  // No map is shipped from the dev server, which serves each module of a file as the plugin compiled it.
  it("the dev server compiles a lens module asked for with a query", async () => {
    project.restore();
    const port = await freePort();
    const server = project.start([bin("vite"), "--host", "127.0.0.1", "--port", String(port), "--strictPort"]);
    try {
      const served = await waitFor(
        async () => {
          try {
            return bytecodeIn(await (await fetch(`http://127.0.0.1:${port}/src/lens.ts?revision=2`)).text());
          } catch {
            return undefined;
          }
        },
        "the dev server to serve the lens",
        server.output,
      );
      expect(served).toBe((await project.expectedLens()).bytecode);
    } finally {
      await server.stop();
    }
  });

  /** A library build of one entry, under a condition of the application's own. */
  const buildEntry = (entry: string) => {
    project.write(
      "vite.resolved.config.ts",
      `import { solidity } from "@morpho-org/viem-dlc/solidity/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [solidity()],
  resolve: { conditions: ["fixture"] },
  build: {
    minify: false,
    outDir: "dist/resolved",
    lib: { entry: "${entry}", formats: ["es"], fileName: "entry" },
    rollupOptions: { external: ["viem", "@morpho-org/viem-dlc/solidity"] },
  },
});
`,
    );
    return project.run([bin("vite"), "build", "--config", "vite.resolved.config.ts"]);
  };

  // The plugin asks Vite's own resolver, and the build takes its answer for an import that names one
  // file whatever the conditions.
  it.skipIf(!requireAnvil())("a lens reads the module Vite bundles", async () => {
    project.restore();
    writeResolutionFixture(project, "resolved");
    project.write("resolved/entry.ts", `import { ${REPORTED_NAMES} } from "./lens";\n\nconsole.log(${REPORT});\n`);
    const built = await buildEntry("resolved/entry.ts");
    expect(built.ok, built.output).toBe(true);
    const run = await project.run(["dist/resolved/entry.js"]);
    expect(run.ok, run.output).toBe(true);
    await expectHostResolution(evm as Evm, JSON.parse(run.output) as Reported);
  });

  it.each(REFUSED)("$name is refused at the import", async (refused) => {
    project.restore();
    const line = writeRefusedImport(project, "refused/lens.ts", refused);
    project.write(
      "refused/entry.ts",
      'import { RATE, RefusedLens } from "./lens";\n\nconsole.log(RefusedLens.bytecode, RATE);\n',
    );
    const built = await buildEntry("refused/entry.ts");
    expect(built.ok, built.output).toBe(false);
    expect(built.output).toContain(project.file(line));
  });
});
