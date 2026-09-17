/**
 * Node-side config for one-off scripts: `pnpm script <file>`. Resolves the package to `src/`, with
 * none of the browser shims `playground/vite.config.ts` aliases in. It carries no Solidity plugin --
 * run a probe that needs `sol()` through `playground/vite.config.ts`, which has soltag installed.
 */
import { fileURLToPath } from "node:url";

import { defineConfig } from "vite";

const src = fileURLToPath(new URL("./src/", import.meta.url));

export default defineConfig({
  resolve: {
    alias: [
      { find: /^@morpho-org\/viem-dlc$/, replacement: `${src}index.ts` },
      { find: /^@morpho-org\/viem-dlc\/stores\/(upstash|vercel)$/, replacement: `${src}stores/$1.ts` },
      { find: /^@morpho-org\/viem-dlc\/(.+)$/, replacement: `${src}$1/index.ts` },
    ],
  },
});
