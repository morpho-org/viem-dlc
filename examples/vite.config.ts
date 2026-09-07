import { fileURLToPath } from "node:url";

import soltag from "soltag/vite";
import { defineConfig } from "vite";

const src = fileURLToPath(new URL("../src/", import.meta.url));

export default defineConfig({
  plugins: [soltag({ solc: { optimizer: { enabled: true, runs: 200 } } })],
  resolve: {
    alias: [
      { find: /^@morpho-org\/viem-dlc$/, replacement: `${src}index.ts` },
      { find: /^@morpho-org\/viem-dlc\/stores\/(upstash|vercel)$/, replacement: `${src}stores/$1.ts` },
      { find: /^@morpho-org\/viem-dlc\/(.+)$/, replacement: `${src}$1/index.ts` },
    ],
  },
});
