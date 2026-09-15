# Playground

An interactive counterpart to `examples/`: the same `deployless` + `readLens` read, with the knobs
exposed as controls and the wide event rendered as the result. It imports `src/` directly, so the
code on the page is the library at this commit rather than a published version.

```sh
pnpm playground          # dev server
pnpm playground:build    # static bundle into playground/dist
```

The controls generate the program they run — the source pane is derived from the same config object
the run uses, so the two cannot disagree.

## Notes

- **No server.** Every call goes from the browser to the RPC endpoint, which works because the
  public Base and Robinhood endpoints send `access-control-allow-origin: *`. A private endpoint that
  omits CORS headers will fail, and the UI says so rather than surfacing a bare fetch error.
- **No solc in the bundle.** `soltag/vite` compiles the lens at build time, so the page ships
  bytecode. Adding a Solidity editor would mean lazy-loading `soljson.js`.
- Elements are the discovered `(market, borrower)` pairs repeated up to the requested count. Every
  element is a real position, so packing and gas figures are real; only input distinctness is
  synthetic.
- `src/shim/async-hooks.ts` stands in for `node:async_hooks`, without which the library's
  observability is a silent no-op in browsers. It tracks one active scope, which holds because the
  UI runs one scenario at a time.
- `NodeFsStore` and `CompressedStore` are Node-bound (`fs/promises`, `zlib`) and cannot appear here.

## Deploying

`.github/workflows/pages.yml` publishes `playground/dist` to GitHub Pages on push to `main`. It
needs Pages enabled once for this repository, with **Settings → Pages → Source: GitHub Actions**.
That is a repository-level setting; it creates a project site at `https://<org>.github.io/viem-dlc/`
and affects nothing outside this repo.
