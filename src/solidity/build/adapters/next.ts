/**
 * `@morpho-org/viem-dlc/solidity/next`: the integration for Next with Turbopack.
 *
 * ```ts
 * import { withSolidity } from "@morpho-org/viem-dlc/solidity/next";
 * export default withSolidity(nextConfig);
 * ```
 *
 * It adds one loader rule. The rule applies to first-party `.ts` and `.tsx` files whose text mentions
 * the package, so every other file skips the loader without leaving Turbopack. The rule's option
 * carries the versions that decide the output, because Turbopack's persistent cache keys a loader's
 * result on its options and not on the loader's own code.
 *
 * Next with webpack is not supported: no rule is added for it, and a webpack build stops at once rather
 * than serve lenses that were never compiled.
 */
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const versionIn = (manifest: string | URL): string =>
  (JSON.parse(readFileSync(manifest, "utf8")) as { version: string }).version;

/** `next start` also loads the Next configuration, on machines where the build-time compiler is absent. */
const versionOf = (name: string): string => {
  try {
    return versionIn(require.resolve(`${name}/package.json`));
  } catch {
    return "absent";
  }
};

interface NextConfigLike {
  turbopack?: { rules?: Record<string, unknown>; [key: string]: unknown };
  [key: string]: unknown;
}

const GLOB = "*.{ts,tsx}";

/** A loader in Turbopack's shorthand, where a rule is only the list of its loaders. */
const isLoader = (item: unknown): boolean =>
  typeof item === "string" || (typeof item === "object" && item !== null && "loader" in item);

export function withSolidity<C extends NextConfigLike>(config: C = {} as C): C {
  const rule = {
    condition: { all: [{ not: "foreign" }, { content: /@morpho-org\/viem-dlc\/solidity/ }] },
    loaders: [
      {
        loader: "@morpho-org/viem-dlc/solidity/next/loader",
        options: {
          cacheKey: [
            // This package's own manifest, which its `exports` do not expose to `require.resolve`.
            versionIn(new URL("../../../../package.json", import.meta.url)),
            versionOf("solc"),
            versionOf("oxc-parser"),
            versionOf("@typescript-eslint/scope-manager"),
          ].join("+"),
        },
      },
    ],
  };
  // Turbopack runs every rule that matches a file, in order. A rule the application already has under
  // the same glob is kept, after this one: the lens is compiled from the module as written, and the
  // application's loaders then see the module without it, as a bundler would hand it to them.
  const existing = config.turbopack?.rules?.[GLOB];
  const existingRules = Array.isArray(existing) && existing.every(isLoader) ? [{ loaders: existing }] : existing;
  return {
    ...config,
    turbopack: {
      ...config.turbopack,
      rules: {
        ...config.turbopack?.rules,
        [GLOB]: existingRules === undefined ? rule : [rule, ...[existingRules].flat()],
      },
    },
    webpack() {
      throw new Error(
        "@morpho-org/viem-dlc/solidity/next compiles lenses with Turbopack only, and this build runs webpack. Build without --webpack.",
      );
    },
  };
}
