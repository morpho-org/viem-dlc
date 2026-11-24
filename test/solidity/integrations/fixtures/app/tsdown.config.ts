import { fileURLToPath } from "node:url";

import { solidity } from "@morpho-org/viem-dlc/solidity/vite";
import { defineConfig } from "tsdown";

export default defineConfig({
  entry: ["src/index.ts"],
  plugins: [solidity()],
  alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
  dts: false,
});
