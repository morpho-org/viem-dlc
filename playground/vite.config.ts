import { fileURLToPath } from "node:url";

import soltag from "soltag/vite";
import { defineConfig } from "vite";

const src = fileURLToPath(new URL("../src/", import.meta.url));
const asyncHooks = fileURLToPath(new URL("./src/shim/async-hooks.ts", import.meta.url));
const shim = fileURLToPath(new URL("./src/shim/", import.meta.url));

export default defineConfig({
  // GitHub Pages serves a project site under /<repo>/.
  base: "/viem-dlc/",
  // This directory is its own pnpm project, so Vite would otherwise treat it as the workspace root
  // and deny the reads that reach above it: `../src` and the `?raw` README behind the About page.
  server: { fs: { allow: [".."] } },
  // esbuild transforms JSX directly; @vitejs/plugin-react would only add Fast Refresh.
  esbuild: { jsx: "automatic", jsxImportSource: "react" },
  build: {
    rollupOptions: {
      // Radix ships "use client" for RSC hosts; this page has no server component to mark.
      onwarn(warning, defaultHandler) {
        if (warning.code !== "MODULE_LEVEL_DIRECTIVE") defaultHandler(warning);
      },
      input: {
        index: fileURLToPath(new URL("./index.html", import.meta.url)),
        selftest: fileURLToPath(new URL("./selftest.html", import.meta.url)),
      },
    },
  },
  plugins: [soltag({ solc: { optimizer: { enabled: true, runs: 200 } } })],
  resolve: {
    // `../src` is outside this pnpm project, so a bare specifier it imports would resolve against
    // the root's node_modules. viem lives in both and would be bundled twice; the shims below live
    // only here, and resolving them from the root fails outright. Every name the aliases leave bare
    // belongs in this list. viem is pinned to the same exact version in both manifests.
    dedupe: ["viem", "string_decoder", "events", "process", "buffer"],
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
      { find: /^@morpho-org\/viem-dlc$/, replacement: `${src}index.ts` },
      { find: /^@morpho-org\/viem-dlc\/(.+)$/, replacement: `${src}$1/index.ts` },
    ],
  },
});
