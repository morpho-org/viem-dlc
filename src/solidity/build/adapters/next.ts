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
 * Not yet exercised by a test in this repository.
 */
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

/** `next start` also loads the Next configuration, on machines where the build-time compiler is absent. */
const versionOf = (name: string): string => {
  try {
    return (JSON.parse(readFileSync(require.resolve(`${name}/package.json`), "utf8")) as { version: string }).version;
  } catch {
    return "absent";
  }
};

interface NextConfigLike {
  turbopack?: { rules?: Record<string, unknown>; [key: string]: unknown };
  [key: string]: unknown;
}

export function withSolidity<C extends NextConfigLike>(config: C = {} as C): C {
  return {
    ...config,
    turbopack: {
      ...config.turbopack,
      rules: {
        ...config.turbopack?.rules,
        "*.{ts,tsx}": {
          condition: { all: [{ not: "foreign" }, { content: /@morpho-org\/viem-dlc\/solidity/ }] },
          loaders: [
            {
              loader: "@morpho-org/viem-dlc/solidity/next/loader",
              options: {
                cacheKey: [versionOf("@morpho-org/viem-dlc"), versionOf("solc"), versionOf("oxc-parser")].join("+"),
              },
            },
          ],
        },
      },
    },
  };
}
