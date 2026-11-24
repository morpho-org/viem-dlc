// One configuration for the Vite build, the Vite dev server and Vitest.
import { fileURLToPath } from "node:url";

import { solidity } from "@morpho-org/viem-dlc/solidity/vite";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [solidity()],
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  test: {
    include: ["src/**/*.spec.ts"],
    // Vitest's cache of transformed modules on disk: the one place a second run could find an old lens.
    experimental: { fsModuleCache: true },
  },
});
