# Playground

An interactive counterpart to `examples/`: a sidebar of feature pages, each with editable source and
the wide event rendered as the result. It imports `src/` directly, so the page is the library at this
commit rather than a published version.

Routing is hash-based (`#/about`, `#/cache`, …) because GitHub Pages serves a static tree with no SPA
fallback — a real path would 404 on reload or on a shared link. The first item mirrors the root
`README.md`; the rest are examples.

```sh
pnpm playground          # dev server
pnpm playground:build    # static bundle into playground/dist
```

Each page is an `Example` (`src/examples/`): controls, one or more scripts, an optional lens, and an
optional `prepare` that runs first. Scripts are real files under `tabs/`, so a comparison like
`initcode` vs `override` is two programs rather than a flag. They receive a **rate-limited,
request-counting `http` transport** and compose the transport under test over it themselves — the
composition is the thing these examples exist to show, so it stays in the editable source rather than
in the harness.

## How editing works

`tabs/positions.sol` is the single source for the lens. `solFile` splices it into `sol()` at build
time for the fast path, and the same file is `?raw`-imported to seed the editor — so the Solidity a
visitor reads is provably the Solidity that ran. Edit it and the page compiles in your browser
instead, via `solc` in a worker.

The compiler is `node_modules/solc/soljson.js` (8.9 MB, pinned to the version the build uses) copied
into `public/` by `scripts/copy-soljson.mjs`. It is fetched on the first in-browser compile and
never before, so a visitor who only reads or switches tabs never downloads it. Because the version
and the standard-JSON input match soltag's own, browser output is byte-identical to build output —
`selftest.html` asserts exactly that.

Scripts are `.js` with JSDoc types, which keeps them inside `pnpm typecheck:playground` (so they
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
  as HTTP 413. The defaults are set for that; raise them against an endpoint that allows more.
- The deployless page's default 2 000 elements shows the initcode/override contrast clearly but wants
  a permissive endpoint; a few hundred fit in one chunk either way.
- `getLogs2`'s three strategies are within noise over a few thousand logs, and what `reduce` and
  `search` save first is peak memory rather than time. `test/bench` is where that is asserted.
- Elements are the discovered `(market, borrower)` pairs repeated up to the requested count. Every
  element is a real position, so packing and gas figures are real; only input distinctness is
  synthetic.
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
