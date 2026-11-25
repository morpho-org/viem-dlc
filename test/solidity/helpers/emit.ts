/**
 * A module as a compiler leaves it: the JavaScript the repository's own tsc emits for a TypeScript
 * source, so a test reads what a published package holds and not a transcription of it.
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { bin } from "../integrations/helpers/project.js";

/** Emits each `name.ts` as JavaScript for `target`, without type checking, and returns it under `name.js`. */
export function emitJavaScript<Name extends string>(
  sources: Readonly<Record<Name, string>>,
  target = "es2022",
): Record<Name, string> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "viem-dlc-emit-"));
  try {
    const names = Object.keys(sources) as Name[];
    for (const name of names) fs.writeFileSync(path.join(dir, `${name}.ts`), sources[name]);
    const tsc = spawnSync(
      process.execPath,
      [
        bin("typescript", "tsc"),
        ...["--target", target, "--module", "esnext", "--moduleResolution", "bundler", "--noCheck"],
        ...["--outDir", "out", ...names.map((name) => `${name}.ts`)],
      ],
      { cwd: dir, encoding: "utf8" },
    );
    if (tsc.status !== 0) throw new Error(`tsc failed:\n${tsc.stdout}${tsc.stderr}`);
    return Object.fromEntries(
      names.map((name) => [name, fs.readFileSync(path.join(dir, "out", `${name}.js`), "utf8")]),
    ) as Record<Name, string>;
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}
