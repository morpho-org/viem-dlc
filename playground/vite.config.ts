import { fileURLToPath } from "node:url";

import soltag from "soltag/vite";
import { defineConfig } from "vite";

const src = fileURLToPath(new URL("../src/", import.meta.url));
const asyncHooks = fileURLToPath(new URL("./src/shim/async-hooks.ts", import.meta.url));
const srcl = fileURLToPath(new URL("./vendor/srcl/", import.meta.url));

export default defineConfig({
  // GitHub Pages serves a project site under /<repo>/.
  base: "/viem-dlc/",
  // esbuild transforms JSX directly; @vitejs/plugin-react would only add Fast Refresh.
  esbuild: { jsx: "automatic", jsxImportSource: "react" },
  build: {
    rollupOptions: {
      input: {
        index: fileURLToPath(new URL("./index.html", import.meta.url)),
        selftest: fileURLToPath(new URL("./selftest.html", import.meta.url)),
      },
    },
  },
  plugins: [soltag({ solc: { optimizer: { enabled: true, runs: 200 } } })],
  resolve: {
    alias: [
      { find: /^node:async_hooks$/, replacement: asyncHooks },
      // SRCL is vendored unmodified; its own import aliases are honoured rather than rewritten.
      { find: /^@components\/(.+)$/, replacement: `${srcl}components/$1` },
      { find: /^@common\/(.+)$/, replacement: `${srcl}common/$1` },
      { find: /^@morpho-org\/viem-dlc$/, replacement: `${src}index.ts` },
      { find: /^@morpho-org\/viem-dlc\/(.+)$/, replacement: `${src}$1/index.ts` },
    ],
  },
});
