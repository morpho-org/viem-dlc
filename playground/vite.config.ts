import { fileURLToPath } from "node:url";

import soltag from "soltag/vite";
import { defineConfig } from "vite";

const src = fileURLToPath(new URL("../src/", import.meta.url));
const asyncHooks = fileURLToPath(new URL("./src/shim/async-hooks.ts", import.meta.url));
const srcl = fileURLToPath(new URL("./vendor/srcl/", import.meta.url));
const shim = fileURLToPath(new URL("./src/shim/", import.meta.url));

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
      // The Node surface `src/` reaches for. Each entry is a place the page diverges from Node, so
      // the list is kept short and explicit rather than delegated to a polyfill plugin.
      // `stream/promises` must precede `stream`.
      { find: /^(node:)?stream\/promises$/, replacement: `${shim}stream-promises.ts` },
      { find: /^(node:)?zlib$/, replacement: `${shim}zlib-gzip.ts` },
      { find: /^(node:)?stream$/, replacement: `${shim}stream.ts` },
      { find: /^(node:)?string_decoder$/, replacement: "string_decoder" },
      // `stream-browserify` builds on readable-stream, which reaches for these.
      { find: /^(node:)?events$/, replacement: "events" },
      { find: /^(node:)?process$/, replacement: "process" },
      { find: /^(node:)?buffer$/, replacement: "buffer" },
      // Bypass the stores barrel: it re-exports NodeFsStore and CompressedStore, which would drag
      // `fs/promises`, `path` and `crypto` in. These are still the real store modules.
      {
        find: /^@morpho-org\/viem-dlc\/stores\/(lru|memory|hierarchical|ttl|throttled)$/,
        replacement: `${src}stores/$1.ts`,
      },
      // SRCL is vendored unmodified; its own import aliases are honoured rather than rewritten.
      { find: /^@components\/(.+)$/, replacement: `${srcl}components/$1` },
      { find: /^@common\/(.+)$/, replacement: `${srcl}common/$1` },
      { find: /^@morpho-org\/viem-dlc$/, replacement: `${src}index.ts` },
      { find: /^@morpho-org\/viem-dlc\/(.+)$/, replacement: `${src}$1/index.ts` },
    ],
  },
});
