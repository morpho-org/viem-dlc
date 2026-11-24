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
      // A script runs once: there is nothing to rerun when a dependency changes.
      addDependency: () => {},
      warn: (diagnostic) => console.warn(formatDiagnostic(diagnostic)),
    });
    return result ? { ...loaded, source: result.code } : loaded;
  },
});
