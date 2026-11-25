/**
 * The plugin in tsdown, which builds with Rolldown: a library build, and watch mode.
 * Runs the built package: `pnpm test` builds first.
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { describe, expect, it } from "vitest";

import { describeHost } from "./helpers/host-suite.js";
import { bin, Project } from "./helpers/project.js";
import { expectNoLensSourceInMaps } from "./helpers/source-maps.js";

describeHost({
  name: "tsdown",
  template: "app",
  run: (project) => project.run([bin("tsdown")]),
  served: (project) => project.bytecodeUnder("dist"),
  output: { directories: ["dist"] },
  async watch(project) {
    const watcher = project.start([bin("tsdown"), "--watch"]);
    return { served: () => project.bytecodeUnder("dist"), output: watcher.output, stop: watcher.stop };
  },
});

describe("a tsdown bundle, beyond the shared suite", { timeout: 120_000 }, () => {
  it("with a source map, the map holds no lens body", async () => {
    const project = new Project("app");
    try {
      const built = await project.run([bin("tsdown"), "--sourcemap"]);
      expect(built.ok, built.output).toBe(true);
      expect(project.bytecodeUnder("dist", ["dist/index.mjs.map"])).toBe((await project.expectedLens()).bytecode);
      expect(expectNoLensSourceInMaps(project, "dist")).toEqual({
        external: ["dist/index.mjs.map"],
        inline: [],
        linked: ["dist/index.mjs"],
      });
    } finally {
      project.remove();
    }
  });

  it("runs where nothing is installed: no compiler, no parser, no package", async () => {
    const project = new Project("app");
    // Outside the repository, so nothing is found by walking up.
    const elsewhere = fs.mkdtempSync(path.join(os.tmpdir(), "viem-dlc-bundle-"));
    try {
      const built = await project.run([bin("tsdown")]);
      expect(built.ok, built.output).toBe(true);
      fs.cpSync(project.file("dist"), elsewhere, { recursive: true });
      const run = spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "--eval",
          'const { QuoteLens } = await import("./index.mjs"); console.log(JSON.stringify({ bytecode: QuoteLens.bytecode, call: Object.keys(QuoteLens.with()) }));',
        ],
        { cwd: elsewhere, encoding: "utf8" },
      );
      expect(run.status, run.stderr).toBe(0);
      expect(JSON.parse(run.stdout)).toEqual({
        bytecode: (await project.expectedLens()).bytecode,
        call: ["abi", "address", "factory", "factoryData"],
      });
    } finally {
      project.remove();
      fs.rmSync(elsewhere, { recursive: true, force: true });
    }
  });
});
