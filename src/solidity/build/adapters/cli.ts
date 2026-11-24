#!/usr/bin/env node
/**
 * `viem-dlc-solidity check [--print] [dir…]`: compiles every lens under the given directories (the
 * current one by default) without a bundler and without running anything, and prints each error in
 * tsc's format. `--print` also prints the Solidity of every lens, for debugging.
 *
 * Optional: a build or a test run already compiles each lens it loads. The command makes the same
 * failure faster to reach.
 */
import fs from "node:fs";
import path from "node:path";

import { formatDiagnostic, LensError } from "../errors.js";
import { mentionsPackage } from "../find.js";
import { buildSync } from "../transform.js";

import { resolveSync } from "./node-resolve.js";

const SKIPPED = new Set(["node_modules", "dist", "out", "build", "coverage", ".next", ".turbo", ".git"]);

const scan = (dir: string, files: string[]): string[] => {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (!SKIPPED.has(entry.name)) scan(full, files);
    } else if (/\.[cm]?[jt]sx?$/.test(entry.name) && !/\.d\.[cm]?ts$/.test(entry.name)) files.push(full);
  }
  return files;
};

const main = (argv: readonly string[]): number => {
  if (argv[0] !== "check") {
    console.error("usage: viem-dlc-solidity check [--print] [dir…]");
    return 2;
  }
  const print = argv.includes("--print");
  const roots = argv.slice(1).filter((argument) => !argument.startsWith("--"));
  const files = (roots.length ? roots : ["."]).flatMap((root) => scan(path.resolve(root), []));
  const relative = (file: string) => path.relative(process.cwd(), file) || file;
  let lenses = 0;
  let failed = 0;
  for (const file of files) {
    const code = fs.readFileSync(file, "utf8");
    if (!mentionsPackage(code)) continue;
    try {
      const built = buildSync(code, file, {
        resolve: resolveSync,
        addDependency: () => {},
        warn: (diagnostic) => console.warn(formatDiagnostic({ ...diagnostic, file: relative(diagnostic.file) })),
      });
      for (const lens of built?.lenses ?? []) {
        lenses++;
        console.log(`ok  ${lens.resolved.name}  ${relative(file)}  ${lens.artifact.bytecode.length / 2 - 1} bytes`);
        if (print) console.log(lens.printed.solidity);
      }
    } catch (cause) {
      if (!(cause instanceof LensError)) throw cause;
      failed++;
      for (const diagnostic of cause.diagnostics)
        console.error(formatDiagnostic({ ...diagnostic, file: relative(diagnostic.file) }));
    }
  }
  console.log(`${files.length} files scanned, ${lenses} lenses compiled, ${failed} files failed`);
  return failed ? 1 : 0;
};

process.exitCode = main(process.argv.slice(2));
