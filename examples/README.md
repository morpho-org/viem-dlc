# Examples

One file per feature, in the order the [README](../README.md) introduces them. Each is fully
self-contained — copy one file and run it — so the transport composition being demonstrated is
visible in the file rather than hidden in a helper.

Morpho Blue on Base is the running domain example (`06-paged-lens-blue` reads Blue on Robinhood Chain, `07-paged-lens-midnight` reads Midnight). The lens contracts used by the deployless
examples are written inline with [`soltag`](https://github.com/haydenshively/soltag) and compiled
on the fly.

## Running

```sh
export RPC_URL=https://base-mainnet.g.alchemy.com/v2/KEY   # any Base RPC
pnpm example examples/01-logs-divider.ts
```

`pnpm example` is `vite-node` with `examples/vite.config.ts`, which compiles the inline Solidity and
resolves `@morpho-org/viem-dlc/*` to `src/`. Full-history examples (`02`, `03`) make a few hundred
`eth_getLogs` calls on their first run and need a provider with real rate limits; public endpoints
will throttle them. Everything else queries a recent window and finishes in seconds.

| Env | Used by | |
| --- | --- | --- |
| `RPC_URL` | all but `06-paged-lens-blue` | Base JSON-RPC endpoint |
| `ROBINHOOD_RPC_URL` | `06-paged-lens-blue` | Robinhood Chain endpoint; defaults to the chain's public RPC |
| `RPC_URL_FALLBACK` | `08` | second provider; defaults to `RPC_URL` |
| `UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN` | `09` | attaches an Upstash tier when set |
| `BLOB_READ_WRITE_TOKEN` | `09` | attaches a Vercel Blob tier when set |

`pnpm typecheck:examples` generates soltag's ABI types (`.soltag/`, gitignored) and typechecks the
directory. Add `"plugins": [{ "name": "soltag/plugin" }]` support in your editor by using the
workspace TypeScript version for inline Solidity diagnostics.

## Index

| File | Feature | README |
| --- | --- | --- |
| `01-logs-divider.ts` | chunked `eth_getLogs` with alignment, retry, and `onLogsResponse` progress | [`logsDivider`](../README.md#logsdivider) |
| `02-cache-logs.ts` | the all-in-one `cache` transport; cold vs. warm, LRU over disk | [`cache`](../README.md#cache) |
| `03-search-and-reduce.ts` | narrowing a shared cache to one account: filter vs. `reduce` vs. `search` + `reduce` | [`getLogs2`](../README.md#getlogs2) |
| `04-deployless-batching.ts` | `deployless` + `policy({ batch })` through an inline lens | [`deployless`](../README.md#deployless) |
| `05-deployless-cache.ts` | per-element `eth_call` caching with `blobKey`, `ttl`, `delta` | [`policy`](../README.md#eth_call-policy) |
| `06-paged-lens-blue.ts` | a Morpho Blue borrower-health paged lens on Robinhood Chain (accrued debt vs. oracle-priced capacity); candidates from the GraphQL API, re-read on-chain; un-created markets land in `skipped` | [Paged lenses](../README.md#paged-lenses) |
| `07-paged-lens-midnight.ts` | the same shape for Midnight on Base, with liquidatability (maturity, lock); candidates from the liquidation-candidates API | [Paged lenses](../README.md#paged-lenses) |
| `08-failover.ts` | two providers with their own limits sharing one store; custom `shouldThrow` | [`failover`](../README.md#failover) |
| `09-stores.ts` | `HierarchicalStore` / `TtlStore` / `CompressedStore` / `NodeFsStore`, optional remote tiers | [Stores](../README.md#stores) |
| `10-observability.ts` | `withLogging` with a real LogLayer; one wide event per call | [Observability](../README.md#observability-optional) |
| `11-deployment-block.ts` | `getDeploymentBlockNumber` | [`getDeploymentBlockNumber`](../README.md#getdeploymentblocknumber) |

`NodeFsStore` and `CompressedStore` are Node/Bun-only; everything else runs wherever viem does.
