# Playground

An interactive counterpart to `examples/`: the same `deployless` + `readLens` read, with the lens
and the script both editable and the wide event rendered as the result. It imports `src/` directly,
so the page is the library at this commit rather than a published version.

```sh
pnpm playground          # dev server
pnpm playground:build    # static bundle into playground/dist
```

Tabs select a script — `initcode` (the default delivery) and `override` (the envelope placed by
state override, no byte cap). Each tab is a real file under `tabs/`, so the comparison is two
programs rather than a flag, and there is no toggle to locate in the source.

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

## Editors

CodeMirror 6, with `@replit/codemirror-lang-solidity` for the lens and `@codemirror/lang-javascript`
for the script. Token colours are CSS variables (`--syn-*`), so both palettes live in `style.css`
beside the rest of the theme rather than in the editor config.

The Solidity grammar tags value types (`uint256`, `address`) as keywords and leaves user identifiers
untagged, so that pane is deliberately flatter than the JavaScript one. Separating types would mean
decorating them ourselves.

## Notes

- **No server.** Calls go from the browser straight to the RPC endpoint, which works because the
  public Base and Robinhood endpoints send `access-control-allow-origin: *`. A private endpoint
  without CORS headers fails, and the UI says so rather than surfacing a bare fetch error.
- Public endpoints rate-limit hard. The default 2 000 elements shows the initcode/override contrast
  clearly but wants a permissive endpoint; a few hundred fit in one chunk either way.
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
