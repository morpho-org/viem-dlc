/**
 * The adapters that run outside a bundler, exercised as a consumer runs them: from the built package.
 * `pnpm test` builds first; run `pnpm build` before running this file on its own.
 */
import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const dist = (file: string) => path.join(ROOT, "dist/solidity/build/adapters", file);
const fixture = (file: string) => path.join(ROOT, "test/solidity/adapters", file);

const run = (args: string[]) => spawnSync(process.execPath, args, { cwd: ROOT, encoding: "utf8" });

describe("adapters outside a bundler", () => {
  it("are built", () => {
    for (const file of ["register.js", "cli.js", "vite.js", "next.js", "next-loader.cjs"])
      expect(fs.existsSync(dist(file)), `${file} is missing: run pnpm build`).toBe(true);
  });

  it("the Node hook compiles a script's inline lens", () => {
    const output = execFileSync(process.execPath, ["--import", dist("register.js"), fixture("script.ts")], {
      cwd: ROOT,
      encoding: "utf8",
    });
    const printed = JSON.parse(output) as { name: string; functions: string[]; bytecode: string };
    expect(printed).toMatchObject({ name: "ScaleLens", functions: ["scale"] });
    expect(printed.bytecode).toMatch(/^0x60[0-9a-f]+$/);
  });

  it("without the hook, the lens throws on first use and names the integrations", () => {
    const result = run([fixture("script.ts")]);
    expect(result.status).not.toBe(0);
    expect(result.stderr).toMatch(/the lens ScaleLens was not compiled/);
    expect(result.stderr).toMatch(/solidity\/register/);
  });

  it("the check command compiles a tree, prints Solidity, and reports errors in tsc's format", () => {
    const result = run([dist("cli.js"), "check", "--print", "test/solidity/adapters"]);
    expect(result.stdout).toMatch(/^ok {2}ScaleLens {2}test\/solidity\/adapters\/script\.ts {2}\d+ bytes$/m);
    expect(result.stdout).toContain("return x * (10 ** 6);");
    expect(result.stderr).toContain(
      "test/solidity/adapters/broken.ts:8:18 - error: Return argument type uint256 is not implicitly convertible to expected type (type of first return variable) uint128.",
    );
    expect(result.stdout).toMatch(/3 files scanned, 1 lenses compiled, 1 files failed/);
    expect(result.status).toBe(1);
  });
});
