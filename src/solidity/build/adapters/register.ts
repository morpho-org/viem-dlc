/**
 * `node --import @morpho-org/viem-dlc/solidity/register script.ts`: the integration for Node scripts.
 * It registers a synchronous `load` hook that runs the same transform on every first-party module that
 * mentions the package.
 */
import { registerHooks } from "node:module";
import { fileURLToPath } from "node:url";

import { formatDiagnostic } from "../errors.js";
import { mentionsPackage } from "../find.js";
import { transformSync } from "../transform.js";

import { resolveSync } from "./node-resolve.js";

/**
 * A script runs once, except under `node --watch`, which restarts it when a file it loaded changes.
 * The rewrite removes the imports of the files a lens was compiled from, so Node never loads them, and
 * Node loads no `tsconfig.json` at all: each is reported the way Node's own loaders report a file to
 * the watching parent process, as soon as it is read, so that a build that fails is still watched.
 */
const reported = new Set<string>();
const addDependency = (file: string): void => {
  if (reported.has(file) || !process.env.WATCH_REPORT_DEPENDENCIES || !process.send) return;
  reported.add(file);
  process.send({ "watch:require": [file] });
};

registerHooks({
  load(url, context, nextLoad) {
    const loaded = nextLoad(url, context);
    if (!url.startsWith("file:") || !loaded.source) return loaded;
    // The file says whether a module is first-party source. A query or a fragment is no part of the file:
    // Node makes another module of the same file for each, and each is transformed as the file is.
    const file = fileURLToPath(url);
    if (/[\\/]node_modules[\\/]/.test(file) || !/\.[cm]?[jt]sx?$/.test(file)) return loaded;
    // Text, or bytes in whichever container the hooks before this one hand them on in.
    const code = typeof loaded.source === "string" ? loaded.source : new TextDecoder().decode(loaded.source);
    if (!mentionsPackage(code)) return loaded;
    const result = transformSync(code, file, {
      resolve: (specifier, importer) => resolveSync(specifier, importer, addDependency),
      addDependency,
      warn: (diagnostic) => console.warn(formatDiagnostic(diagnostic)),
    });
    return result ? { ...loaded, source: result.code } : loaded;
  },
});
