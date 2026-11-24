/**
 * A synchronous resolver for hosts that have none: the Node hook and the check command. It resolves as
 * the application's bundler would, with the nearest `tsconfig.json` for `paths` aliases.
 *
 * `oxc-resolver` is an optional peer dependency, needed only by these two adapters.
 */
import fs from "node:fs";
import path from "node:path";

import { ResolverFactory } from "oxc-resolver";

const resolvers = new Map<string | undefined, ResolverFactory>();

const nearestTsconfig = (from: string): string | undefined => {
  for (let dir = from; ; dir = path.dirname(dir)) {
    const candidate = path.join(dir, "tsconfig.json");
    if (fs.existsSync(candidate)) return candidate;
    if (dir === path.dirname(dir)) return undefined;
  }
};

export function resolveSync(specifier: string, importer: string): string | undefined {
  const dir = path.dirname(importer);
  const tsconfig = nearestTsconfig(dir);
  let resolver = resolvers.get(tsconfig);
  if (!resolver) {
    resolver = new ResolverFactory({
      conditionNames: ["import", "node", "default"],
      extensions: [".ts", ".tsx", ".mts", ".js", ".mjs", ".jsx"],
      extensionAlias: { ".js": [".ts", ".tsx", ".js"], ".mjs": [".mts", ".mjs"] },
      ...(tsconfig ? { tsconfig: { configFile: tsconfig, references: "auto" as const } } : {}),
    });
    resolvers.set(tsconfig, resolver);
  }
  return resolver.sync(dir, specifier).path;
}
