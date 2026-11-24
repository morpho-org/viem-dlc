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
  },
  benchmark: {
    include: ["test/bench/**/*.bench.ts"],
  },
});
