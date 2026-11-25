import { fileURLToPath } from "node:url";

import { defineConfig } from "vitest/config";

import { solidity } from "./src/solidity/build/adapters/vite.js";

const source = (path: string) => fileURLToPath(new URL(path, import.meta.url));

export default defineConfig({
  // Compiles the lenses written inline in test/solidity, the way a consumer's test run does.
  plugins: [solidity()],
  resolve: {
    // Lens modules import the package by name, as a consumer does; here the name is the source.
    alias: [
      { find: /^@morpho-org\/viem-dlc\/solidity$/, replacement: source("./src/solidity/index.ts") },
      { find: /^@morpho-org\/viem-dlc\/solidity\/yul$/, replacement: source("./src/solidity/yul.ts") },
    ],
  },
  test: {
    globals: true,
    environment: "node",
    // Pinned rather than left to the default glob, which would reach into `playground/` -- its own
    // pnpm project, whose dependencies this runner cannot resolve.
    include: ["test/**/*.test.ts"],
    // `pnpm test` measures how much of the transform the tests reach, and fails below the threshold.
    coverage: {
      provider: "v8",
      include: ["src/solidity/build/**"],
      exclude: [
        // An adapter runs inside its tool: the Vite plugin in the process that starts the workers, the
        // register hook and the command in a child Node process, Next in its own build. The integration
        // tests drive each one there, where this measure does not reach.
        "src/solidity/build/adapters/**",
      ],
      reporter: ["text"],
      // A line that no input reaches is excluded where it stands, by a `v8 ignore` comment that says
      // why. What is left below 100 can be reached, and has no test in this process yet.
      thresholds: { branches: 98.8 },
    },
  },
  benchmark: {
    include: ["test/bench/**/*.bench.ts"],
  },
});
