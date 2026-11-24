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

registerHooks({
  load(url, context, nextLoad) {
    const loaded = nextLoad(url, context);
    if (!url.startsWith("file:") || url.includes("/node_modules/") || !/\.[cm]?[jt]sx?$/.test(url) || !loaded.source)
      return loaded;
    const code = loaded.source.toString();
    if (!mentionsPackage(code)) return loaded;
    const result = transformSync(code, fileURLToPath(url), {
      resolve: resolveSync,
      // A script runs once, except under `node --watch`, which restarts it when a file it loaded changes.
      // The rewrite removes the imports of the files a lens was compiled from, so Node never loads them:
      // they are reported the way Node's own loaders report a file to the watching parent process.
      addDependency(file) {
        if (process.env.WATCH_REPORT_DEPENDENCIES && process.send) process.send({ "watch:require": [file] });
      },
      warn: (diagnostic) => console.warn(formatDiagnostic(diagnostic)),
    });
    return result ? { ...loaded, source: result.code } : loaded;
  },
});
