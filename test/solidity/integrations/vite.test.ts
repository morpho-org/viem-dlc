/**
 * The plugin in Vite: a production build, and the dev server as its watch mode.
 * Runs the built package: `pnpm test` builds first.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { type Evm, requireAnvil, startAnvil } from "../helpers/anvil.js";

import { describeHost } from "./helpers/host-suite.js";
import { bin, bytecodeIn, freePort, Project } from "./helpers/project.js";
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
