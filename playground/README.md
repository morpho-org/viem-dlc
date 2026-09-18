# Playground

The library's tutorials: a sidebar of pages, each prose interleaved with runnable steps whose source
is editable in place and whose result is the wide event the call emitted. It imports `src/` directly,
so the page is the library at this commit rather than a published version.

Routing is hash-based (`#/about`, `#/eth-call`, …) because GitHub Pages serves a static tree with no SPA
fallback — a real path would 404 on reload or on a shared link. The first item mirrors the root
`README.md`; the rest are tutorials.

```sh
pnpm install    # in this directory — the playground installs separately from the library
pnpm dev        # dev server
pnpm build      # static bundle into dist/
pnpm typecheck  # checks this directory, and the tutorial .js files, against ../src
```

## Its own pnpm project

This directory has its own `package.json`, `pnpm-lock.yaml` and `pnpm-workspace.yaml`, and is
deliberately not a member of the root workspace. React, Radix, CodeMirror and `solc` are the tutorial
site's dependencies, not the library's, and a contributor working on `src/` should not install them.
Run `pnpm install` here as well as at the root, and run the scripts above from this directory —
the library's `package.json` deliberately carries none of them.

The boundary moves dependencies, not resolution — the Vite aliases and the tsconfig `paths` still
point at `../src/*.ts`, so the page remains the library at this commit. Three consequences follow
from being its own project, each load-bearing:

- `pnpm-workspace.yaml` restates the root's install settings rather than inheriting them — pnpm reads
  them from the directory holding the lockfile. Drift between the two files is a bug. `.pnpmfile.cjs`
  has no counterpart: soltag is installed here and nowhere else, so this is the repo's only pnpmfile.
- `server.fs.allow` in `vite.config.ts` is what lets the dev server read above this directory. Vite
  treats a directory containing `pnpm-workspace.yaml` as a workspace root; without the setting,
  `../src` and the About page's `?raw` README both 403.
- `resolve.dedupe` covers every bare specifier the aliases leave alone. `../src` sits outside this
  project, so a bare import it makes would otherwise resolve against the root's `node_modules` —
  bundling `viem` twice, and failing outright for the shims, which live only here.

A page is a `Tutorial` (`src/tutorials/`): page-scoped controls, then an ordered list of sections.
A section is prose, a runnable step, or both. Each step owns its editors, its Run button and its
result, so two steps hold two outcomes at once and a comparison between them is a comparison of the
transports rather than of the inputs — `prepare` runs once per page, memoized against the endpoint,
and every step sees the same corpus.

Prose lives beside the code it introduces, in `tutorials/<id>/*.md`, rendered with `marked`. Scripts
are real files under the same directory, so a comparison like `multicall` against `readLens` is two
programs rather than a flag. They receive a **rate-limited, request-counting `http` transport** and
compose the transport under test over it themselves — the composition is the thing these tutorials
exist to show, so it stays in the editable source rather than in the harness.

Under the page sits a session-long event feed: every wide event the transports emit and every `log`
a script writes, interleaved, newest last. The per-step table is the record of one run; the feed is
the view of a run happening. Expand a row for its full field set.

## How editing works

`tutorials/eth-call/vault-snapshot.sol` is the single source for the lens. `solFile` splices it into
`sol()` at build time for the fast path, and the same file is `?raw`-imported to seed the editor — so
the Solidity a visitor reads is provably the Solidity that ran. Edit it and the page compiles in your browser
instead, via `solc` in a worker.

The compiler is `node_modules/solc/soljson.js` (8.9 MB, pinned to the version the build uses) copied
into `public/` by `scripts/copy-soljson.mjs`. It is fetched on the first in-browser compile and
never before, so a visitor who only reads or switches tabs never downloads it. Because the version
and the standard-JSON input match soltag's own, browser output is byte-identical to build output —
`selftest.html` asserts exactly that.

Scripts are `.js` with JSDoc types, which keeps them inside `pnpm typecheck` (so they
cannot rot against `src/`) while staying directly evaluable: `evaluateTab` strips the import block
and rebinds those names as function parameters against a small registry in `src/tab.ts`. A
specifier that is not in the registry fails loudly rather than yielding `undefined`.

## Interface

[Radix Themes](https://www.radix-ui.com/themes) supplies the components. It is configured down to a
sober palette — gray accent, sand grays, square corners, and the mono stack promoted to
`--default-font-family` — so the page reads as a technical document rather than as a themed widget
set, while every control behaves the way a visitor already expects.

Tokens are imported modularly in `main.tsx`: the bundled `styles.css` carries all 26 colour scales,
of which seven are used. `appearance` follows `prefers-color-scheme`, and both light and dark resolve
from the same token set.

Editors are CodeMirror 6 — `@replit/codemirror-lang-solidity` for the lens,
`@codemirror/lang-javascript` for the script — with token colours mapped onto Radix scales in
`style.css`, so they follow the appearance with everything else. The Solidity grammar tags value
types (`uint256`, `address`) as keywords and leaves user identifiers untagged, so that pane is
deliberately flatter than the JavaScript one.

## Running the cache transport in a browser

`src/internal/compressed-lines-blob.ts` is reached unconditionally from both cache handlers and is
built on Node streams and streaming zstd. `src/shim/zlib-gzip.ts` supplies the `zlib` surface it
imports, backed by the browser's `CompressionStream`/`DecompressionStream` with **gzip**.

That substitution is deliberate and narrow. No published JS or WASM zstd exposes a push-driven
compressor — they all take a complete buffer — so using one would force the write path to
materialise the whole uncompressed blob, which is exactly what the blob module's backpressured
pipeline exists to avoid. `CompressionStream` is a genuine streaming transform, so the memory
behaviour is preserved and only the codec changes:

```
                 peak heap growth    for 52.2 MB of data
node zstd        0.2 MB              0.32 MB compressed
gzip shim        0.3 MB              0.59 MB compressed
```

Reproduce with `scripts/codec-memory.ts` (it prints the command). Consequences, both bounded:
`blob_bytes_written` on the wide event is a gzip figure, and a blob written here is unreadable by
Node — which never comes up, because the browser Store is in-memory and per-session. Nothing in
`src/` inspects the format; `isZstdFailure` keys on error *codes*, which the shim reproduces.

The principled version is an injectable codec on `NdjsonMap`/`LazyNdjsonMap`, at which point the
shim becomes a supported configuration rather than an alias.

The other Node surfaces are aliased explicitly in `vite.config.ts` rather than via a polyfill plugin,
so the list of divergences stays short and enumerable: `stream` (plus a `Readable.from` the browser
build omits), `stream/promises` (no such subpath exists in `node-stdlib-browser`, and Node's error
propagation had to be reproduced — `readable-stream` reports `ERR_STREAM_PREMATURE_CLOSE` to the
consumer and keeps the real error for the callback), `string_decoder`, `events`, `process`, `buffer`.

`installNodeGlobals()` is a function, not a side-effecting import: the package declares
`"sideEffects": false`, so Rollup drops a bare `import "./globals.js"` and the globals never appear.

## Notes

- **No server.** Calls go from the browser straight to the RPC endpoint, which works because the
  public Base and Robinhood endpoints send `access-control-allow-origin: *`. A private endpoint
  without CORS headers fails, and the UI says so rather than surfacing a bare fetch error.
- Public endpoints rate-limit hard, and Base's caps a single `eth_getLogs` at 2 000 blocks, answered
  as HTTP 413 with `-32614`. The `eth_getLogs` tutorial's first step discovers that cap live rather
  than asserting it, so it reports whatever your endpoint allows.
- `getLogs2`'s `search` pays in proportion to how rare the target is: over 100 000 blocks in 50 bins
  it skips parsing for a borrower in one bin (121 logs parsed of 7 327) and saves almost nothing for
  one in 33. `reduce` bounds memory rather than time, and `test/bench` is where that is asserted.
- The `eth_call` tutorial reads Morpho Vault V2 on Base, fetched from `api.morpho.org` with a pinned
  snapshot as fallback. Every vault and every gas figure is real. `grief` is the one synthetic input:
  a loop in the lens that burns gas on one element, standing in for cost a curator controls. The page
  says so.
- Base's public endpoint grants 600,000,000 gas for an `eth_call` and throttles hard. The tutorial's
  defaults are set so its numbers reproduce there; a dedicated endpoint makes the request-count
  contrasts cleaner.
- solc hashes the source into the trailing CBOR metadata, so even a comment-only edit changes the
  bytecode and the counterfactual CREATE2 address. Harmless for a lens that is never deployed.
- `src/shim/async-hooks.ts` stands in for `node:async_hooks`, without which the library's
  observability is a silent no-op in browsers. It tracks one active scope, which holds because the
  UI runs one scenario at a time.
- `NodeFsStore` and `CompressedStore` are Node-bound (`fs/promises`, `zlib`) and cannot appear here.

## Deploying

`.github/workflows/pages.yml` publishes `playground/dist` to GitHub Pages on push to `main`. It
needs Pages enabled once for this repository, with **Settings → Pages → Source: GitHub Actions**.
That is a repository-level setting; it creates a project site at `https://<org>.github.io/viem-dlc/`
and affects nothing outside this repo.
