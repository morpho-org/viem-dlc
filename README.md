# @morpho-org/viem-dlc

A collection of flexible [viem](https://viem.sh) extensions with a focus on intelligent caching.
Provides composable transport wrappers for optimized `eth_getLogs` and deployless `eth_call`
handling with caching, rate limiting, automatic request splitting, and oversized-log filtering.

## Installation

```bash
pnpm add @morpho-org/viem-dlc
```

Also available on the [GitHub Package Registry](https://npm.pkg.github.com).

## Tutorials

Four interactive tutorials — `eth_getLogs`, `eth_call`, transport composition, and observability —
live in [`playground/`](./playground/README.md). Each is prose interleaved with runnable, editable
steps that call this package against a live endpoint, so every figure they quote is one you can
reproduce.

## Choosing a half

The library has two independent halves, and they answer different questions. Neither is a general
answer to "how do I read chain data"; each has a scale past which something else is the right tool.

| What you need | Use | Holds up to | What replaces it past that |
| --- | --- | --- | --- |
| History and discovery — events over a block range | the `cache` transport | roughly 10 MB of zstd-compressed logs per query in serverless memory (about eight months of Morpho Vault V2 history), or roughly 100 MB on a developer machine (every Morpho Blue borrow event ever emitted) | an indexer surfacing raw events |
| The freshest current state — the same question about many subjects, right now | a batched lens read: `readLens`, or `deployless` with `policy` | tens of thousands of elements before a provider's gas budget binds | an indexer accumulating derived state |

The rule of thumb: **index for history and discovery, read a lens for the freshest current state.**
Most systems want both — discover the subject list from logs, then read its live state through a
lens.

### When a lens fits

A lens fits when you ask the same question about many subjects and each answer stands alone. That
independence is a hard requirement rather than a preference — see the
[semantic requirement](#eth_call-policy) on `policy`. It fits best when you want the current block
rather than history, and when the answer is derived from several storage reads rather than sitting
in one field.

**Against a `multicall`, almost always.** Whatever the provider's gas cap and rate limits, a lens
sends less calldata, decodes faster, and splits under pressure instead of failing. A low gas cap
means more calls to answer the same list; it does not mean `multicall` would have done better,
because a `multicall` aggregates into one call under one gas budget. One item that burns unbounded
gas fails the whole batch, and every retry fails the same way — so anyone who can get an address
into your input list can trigger that deliberately. This library bisects the offender out instead,
which costs a few extra round trips rather than the query.

**Against an indexer that accumulates derived state, usually — until the budget binds.** Such an
indexer has to be deployed, backfilled, and kept correct across reorgs, and what it serves is always
at least one block stale. A lens reads live chain state with nothing to stand up, and freshness is
guaranteed by construction. The provider's gas cap and rate limit are what eventually bite; past
that point there is a real trade, and the usual split is derived state from an indexer plus a lens
for the fast-moving inputs.

### When caching logs fits

Caching does an indexer's job up to a point. For history and discovery the two are interchangeable
until the query outgrows its host, at the sizes in the table above. Past that, indexing is required
rather than preferred.

It is also the fastest way to start, because there is nothing to deploy and nothing to backfill.
That is what makes it the right choice for experimentation and for standing something up quickly,
and it is worth knowing which of those two properties you are relying on: a cache that has quietly
become the thing you index with will hit its ceiling as a surprise.

## Observability (optional)

This library can emit structured events through a logger you provide. The expected
shape is a structural subset of [`loglayer`](https://www.npmjs.com/package/loglayer)
— a `LogLayer` instance satisfies it directly — but `loglayer` is **not** a declared
peer dependency, so it isn't installed transitively and isn't required to typecheck.
Pass any value matching the exported `Logger` interface (`child`, `withContext`,
`withMetadata`, `withError`, `info`, `warn`, `error`, `metadataOnly`).

```bash
pnpm add loglayer    # only if you want to use it as the logger
```

If you don't call `withLogging`, the library emits nothing and the dep is irrelevant.

```ts
import { withLogging } from '@morpho-org/viem-dlc'

await withLogging(() => client.request({ method: 'eth_getLogs', params: [filter] }), {
  logger,           // anything satisfying the `Logger` interface, such as a LogLayer instance
  service: 'indexer', // extra opts become context fields on every event
})
```

Each outermost `client.request` made inside a `withLogging` scope emits a single
`"concluded"` wide event. Transports contribute flat, queryable fields under their
key — `viem-dlc-failover.succeeded_index`, `viem-dlc-logs-divider.logs_fetched` —
and layers crossed many times per call, such as the divider once per chunk,
accumulate totals there, such as `viem-dlc-logs-sieve.logs_dropped`. If a call crosses
several *instances* of the same transport — say, one cache per failover branch — later
instances are suffixed `.1`, `.2`, ... in first-touch order, which is stable for a
given composition. Every layer also stamps a per-instance `crossings` count, so the
event records which transports the call traversed and how many times each. Call-level
fields are `call_id`, `chain_id` (when the client has a chain), `duration_ms`, and `status`
(`"ok"` or `"error"`). Failed calls
emit at `error` level with the error attached via `withError`, so hosts that forward
`withError` entries to an error reporter such as Sentry capture them automatically.

## Transports

### `deployless`

Splits a deployless `eth_call` into as many calls as it takes. It intercepts only calls carrying
the `policy(...)` sentinel in `stateOverride`: it re-packs the marked input array into one or more
deployless-factory calls under a wire byte budget — the chain's initcode limit, and the transport's
`batchSize` where stated — and aggregates the pages that come back. It forwards everything else
unchanged.
No gas figure is load-bearing: the envelope calls the lens's per-item function once per element
in its own frame and reports how far it got, so a chunk adapts to whatever gas the node grants —
see [Paginated lenses](#paginated-lenses). An optional `gasLimit` lets the opening wave
anticipate the grant, and rides as each chunk's `gas` on chains that need it (see
[Chains](#chains)). A chunk's elements ride inside the envelope's initcode by default, so the
chain's initcode limit bounds it; with `batch.envelope: 'override'` the envelope is placed by state
override instead and only the frame's gas and the provider's request size bound a chunk. Most
callers reach it through [`readLens`](#readlens) rather than building the call by hand.

```ts
import { createPublicClient, defineChain, encodeFunctionData, http, parseAbiItem } from 'viem'
import { mainnet } from 'viem/chains'
import { call } from 'viem/actions'
import { deployless } from '@morpho-org/viem-dlc/transports'
import { arrayifiedAbi, policy } from '@morpho-org/viem-dlc/actions'
import { chainConfig, ethereumFacts } from '@morpho-org/viem-dlc/chains'

// The lens implements `positionOf((bytes32,address)) view returns ((uint256,uint128,uint128))`;
// the array-shaped fragment the wire carries is derived from it.
const positionsAbi = arrayifiedAbi(
  parseAbiItem('function positionOf((bytes32 id, address user) input) view returns ((uint256,uint128,uint128))')
)

// The chunk rides as initcode, which the chain's limit bounds — see [Chains](#chains).
const chain = defineChain({ ...mainnet, ...chainConfig }).extend({ viemDlc: ethereumFacts })

const client = createPublicClient({ chain, transport: deployless(http(rpcUrl)) })

const result = await call(client, {
  factory,
  factoryData,
  to,
  data: encodeFunctionData({ abi: [positionsAbi], functionName: 'positionOf', args: [inputs] }),
  stateOverride: [policy({ abi: positionsAbi, batch: { compress: true } })],
})
```

If `policy.cache` is present, `deployless(...)` ignores it and stays in split-only mode.
Use `cache(...)` when you want the same marked calls to populate and read from a backing store.

Both transports take two optional figures about the provider they were pointed at.

`gasLimit` is its `eth_call` gas cap: `deployless(http(rpcUrl), { gasLimit: 50_000_000 })`, or
`gasLimit` beside `binSize` in the `cache` config. Together with the policy's `batch.gas` it sizes
the opening wave. Every later chunk is sized from what the pages report, so a value that is too low
costs a round trip, never a result.

Some chains run an `eth_call` that leaves `gas` unspecified in a fixed default below the cap — Monad
grants 8.1M, and promotes only on out-of-gas, which a paging envelope never is. On those chains the
transports also send `gasLimit` as every chunk's `gas`, and a value above the provider's cap makes
the node reject the request. State the cap the provider documents. Whether a chain behaves this way
is a fact the chain carries; [Chains](#chains) records it.

`batchSize` is the largest request the provider accepts, in bytes of a chunk's `eth_call` `data`.
State what the provider documents, often a few megabytes.

Behind `failover`, each branch states its own.

With observability enabled, batching reports `elements_requested` / `elements_fetched`,
`nominal_batches` and `batch_bytes` (sizes of the initial packing against the wire budget;
halved and continued chunks are not resampled), and `splits_*` for chunks halved after an
error: `splits_size` (413 / initcode-size errors) and `splits_timeout`. The envelope's prologue
grows with the chunk only by the copy of its bytes, which the packer prices, so a frame that dies
without reporting is a constructor too heavy for the node's cap, surfaced as an error rather than
halved. Pagination is normal rather than a failure and gets its own
fields: `pages_continued` (responses that stopped early; the elements they did not reach are pooled
and re-packed together), `flushes` (the requests those pooled elements were re-packed into, split
into `flushes_full`, `flushes_drain` and `flushes_eager` by what released them — see
`batch.continuations`), `continuation_depth_max` (the longest chain of pages behind pages),
`page_adjudicated` (elements per page, as a stat — a lens yielding ~1 per page is
pathological), `pages_all_skipped` (pages whose every element reverted — a per-item selector the
lens does not implement is one cause), `attempts_unresolved` (elements a frame's gas could not
resolve, whether the per-item frame died or the envelope refused to start it), `pages_escalated`
(singleton retries those cost), and, matching the response's
`skipped` array: `elements_missing` in total, of which `elements_declined_oversize` could not fit
a chunk alone under the byte budget and `elements_unresolved` were gas-terminal even alone —
the subset another provider with a higher cap might still serve.

Every page also reports what its attempts cost, and the request pools it: `frame_gas` (the gas a
frame had for attempts, on the smallest frame seen), `fixed_gas` (what a frame spent before its
first attempt — prologue, lens deploy and reserve — less the copy of the chunk's own bytes, so the
figure does not depend on how large the observed pages were), `item_gas_avg` / `item_gas_stddev` /
`item_gas_max` per attempt, and `gas_limit_observed`, the `eth_call` gas cap the provider actually
granted, read back from the frame, the prologue and the calldata's intrinsic gas. Every chunk after
the opening wave is packed from these; they are also the numbers the opening wave takes as
configuration: `fixed_gas`, `item_gas_avg` and `item_gas_stddev` go to the policy's
`batch.gas`, and `gas_limit_observed` to each transport's `gasLimit` (stamped as `gas_limit` when it
applied). Take them over a representative window; a `gas_limit` above `gas_limit_observed` is a
cap the provider has since lowered. Costs depend on which items share a frame: grouping related
elements warms storage they share and lowers `item_gas_avg`, shuffling makes the rate uniform
across chunks; results align to `args` in either order. A full cache hit or an empty input makes
no upstream call and carries none of these fields.

Delivery has its own fields: `chunks_override` and `chunks_initcode` (requests sent in each
delivery), `override_fallbacks` (ranges re-fetched as initcode, split into
`override_fallbacks_unsupported`, `override_fallbacks_unproven` and `override_fallbacks_exhausted`
by what the failed chunk proved). A provider that never honours overrides shows one unsupported
fallback per request; turn the option off for it.

### `cache`

Caches `eth_getLogs` and `eth_call` through one transport. It composes five layers: oversized-log
filtering (`logsSieve`), log enrichment (`logsEnricher`), rate limiting (`rateLimiter`), request
splitting (`logsDivider`), and caching. It needs a `chain` on the client, so that it can namespace
cache keys by chain ID.

```ts
import { createPublicClient, http } from 'viem'
import { mainnet } from 'viem/chains'
import { cache, createSimpleInvalidation } from '@morpho-org/viem-dlc/transports/cache'
import { LruStore } from '@morpho-org/viem-dlc/stores'

const transport = cache(http(rpcUrl), [
  {
    binSize: 10_000,
    store: new LruStore({ maxBytes: 100_000_000 }),
    invalidationStrategy: createSimpleInvalidation(),
  },
  {
    maxBlockRange: 100_000,
  },
  {
    retryCount: 3,
    retryDelay: 1_000,
    blockTimestamp: false,
  },
  {
    maxBytes: 8_192,
  },
  {
    maxRequestsPerSecond: 10,
    maxBurstRequests: 5,
    maxConcurrentRequests: 5,
  },
])

const client = createPublicClient({ chain: mainnet, transport })
```

`binSize` sets the granularity of a cache entry. The transport aligns requests to bin boundaries
so that overlapping windows hit the same entries, and it sets the `logsDivider` config's `alignTo`
from `binSize` for you. Smaller bins invalidate more precisely and cost more entries; larger bins
do the opposite.

The package ships two invalidation strategies:

- `createSimpleInvalidation(minAgeMs?, maxAgeDays?, numHotBlocks?, avgInvalidationsPerRequest?)` — entries near the chain tip are always refetched; older entries are probabilistically invalidated based on age.
- `createExponentialInvalidation(alphaAge?, maxAgeDays?, alphaBlocks?, scaleBlocks?)` — exponential model with separate time and block-age factors.

### `failover`

Dispatches a request across several RPC providers, each with its own limits. Each branch is a
fully-built per-provider stack carrying its own `maxBlockRange` and, optionally, its own `gasLimit`.
Deployless lenses adapt to each node's grant on their own, and the cap sizes the opening wave.

`failover` builds each branch once, at composition time, so stateful inner transports — coalescing
mutexes, rate-limiter token buckets — persist across requests instead of being rebuilt per call.
viem's stock `fallback` rebuilds the active branch on every request, which disables those features.

```ts
import { createPublicClient, http } from 'viem'
import { mainnet } from 'viem/chains'
import { failover } from '@morpho-org/viem-dlc/transports'
import { cache, createSimpleInvalidation } from '@morpho-org/viem-dlc/transports/cache'
import { LruStore } from '@morpho-org/viem-dlc/stores'

const store = new LruStore({ maxBytes: 100_000_000 })
const sharedConfig = { binSize: 10_000, store, invalidationStrategy: createSimpleInvalidation() }

const transport = failover([
  cache(http(rpcUrlA), [sharedConfig, { maxBlockRange: 100_000 }]),
  cache(http(rpcUrlB), [sharedConfig, { maxBlockRange: 10_000 }]),
])

const client = createPublicClient({ chain: mainnet, transport })
```

Each branch's `logsDivider` chunks requests at its own `maxBlockRange`, so neither provider
is sized for the lowest common denominator. The shared `Store` means partial fetches from
branch A persist in cache and are visible to branch B on fallover, making recovery cheap.

`failover` only sees errors that escape per-branch halving (`logsDivider` range-halving and
`deployless` halving run inside each branch first). By default, contract reverts and
user-rejection errors propagate immediately instead of triggering fallover — pass a custom
`shouldThrow` to override:

```ts
import { defaultShouldThrow, failover } from '@morpho-org/viem-dlc/transports'

failover([branchA, branchB], {
  shouldThrow: (err) =>
    defaultShouldThrow(err) ||
    [401, 402, 403].includes((err as { status?: number })?.status ?? 0),
})
```

### `logsDivider`

Splits a large `eth_getLogs` request into smaller chunks, retrying failures and aligning
boundaries on request. It composes `rateLimiter` for rate and concurrency limits, `logsEnricher`
for enrichment, and `logsSieve` for oversized-log filtering.

```ts
import { createPublicClient, http } from 'viem'
import { logsDivider } from '@morpho-org/viem-dlc/transports'

const transport = logsDivider(http(rpcUrl), [
  {
    maxBlockRange: 100_000,
    alignTo: 10_000,
  },
  {
    retryCount: 3,
    retryDelay: 1_000,
    blockTimestamp: false,
  },
  {
    maxBytes: 8_192,
  },
  {
    maxRequestsPerSecond: 10,
    maxConcurrentRequests: 5,
  },
])

const client = createPublicClient({ transport })

const logs = await client.request({
  method: 'eth_getLogs',
  params: [
    filter,
    undefined,
    {
      onLogsResponse: ({ logs, fromBlock, toBlock }) => {
        /* progressive updates */
      },
    },
  ],
})
```

### `logsEnricher`

Enriches `eth_getLogs` responses with data that standard RPCs omit. Today it populates
`blockTimestamp` by fetching block headers. It drops logs whose block was reorged away, without
reporting them.

```ts
import { createPublicClient, http } from 'viem'
import { logsEnricher } from '@morpho-org/viem-dlc/transports'

const transport = logsEnricher(http(rpcUrl), [{
  retryCount: 3,
  retryDelay: 1_000,
  blockTimestamp: true,
}])

const client = createPublicClient({ transport })
```

### `logsSieve`

Filters `eth_getLogs` responses by estimated UTF-8 payload size. It drops any `RpcLog` whose
serialized size exceeds `maxBytes`, without reporting it. `logsDivider(...)` and `cache(...)` already
compose this transport by default; use `logsSieve(...)` directly when filtering is all you need.

```ts
import { createPublicClient, http } from 'viem'
import { logsSieve } from '@morpho-org/viem-dlc/transports'

const transport = logsSieve(http(rpcUrl), [{ maxBytes: 8_192 }])

const client = createPublicClient({ transport })
```

### `rateLimiter`

Limits request rate with a token bucket, bounds concurrency, and schedules by priority. With
observability enabled it reports `queue_wait_ms` (admission wait, summarized over
every crossing in the call), which separates time spent queued behind your own limits
from time spent waiting on the upstream RPC:

```ts
import { createPublicClient, http } from 'viem'
import { rateLimiter } from '@morpho-org/viem-dlc/transports'

const transport = rateLimiter(http(rpcUrl), [
  {
    maxRequestsPerSecond: 10,
    maxBurstRequests: 5,
    maxConcurrentRequests: 3,
  },
])

const client = createPublicClient({ transport })

await client.request({
  method: 'eth_getLogs',
  params: [
    filter,
    {
      __rateLimiter: true,
      priority: 0,
    },
  ],
})
```

## Chains

Two things this package needs are facts of the chain rather than of the caller, so the chain the
client is built with carries them, on viem's `extendSchema`:

- **`ethCall.gasWhenUnspecified`** — the frame a request that leaves `gas` unspecified runs in:
  the provider's whole cap (`providerCap`, geth), or a fixed default below it (`fixedDefault`;
  Monad grants 8.1M and promotes only on out-of-gas, which a paging envelope never is). The
  transports send `gasLimit` as each chunk's `gas` on a `fixedDefault` chain and nothing elsewhere,
  so `gas_limit_observed` reads the true cap wherever a node would reveal it.
- **`ethCall.gasAboveCap`** — what the node does with a `gas` above the provider's cap: `clamped`
  (geth) or `rejected` (Monad, `gas limit too high`).
- **`maxInitcodeSize`** — the largest initcode the chain's nodes accept. An initcode-delivered
  chunk's bytes *are* the initcode, so this bounds one; EIP-3860's 49 152 nearly everywhere, 262 144
  on Monad.

Nothing is assumed. `ethereumFacts` and `monadFacts` are the two this package has probed; attach one
of them, or state your own for a chain neither describes, and nothing needs upstreaming here:

```ts
import { defineChain } from 'viem'
import { monad } from 'viem/chains'
import { chainConfig, monadFacts } from '@morpho-org/viem-dlc/chains'

const chain = defineChain({ ...monad, ...chainConfig }).extend({ viemDlc: monadFacts })

const client = createPublicClient({ chain, transport: deployless(http(rpcUrl)) })
```

A fact that is missing when it is needed throws, naming the chain: stating a `gasLimit` needs the
`eth_call` behaviour and fails when the client is built, and packing a chunk as initcode needs the
initcode limit and fails on the first read. An override-delivered read with no `gasLimit` needs
neither, so it works on a chain carrying nothing — which means a provider that stops honouring state
overrides can surface the error later than the code that caused it.

This needs viem 2.43 or newer, where `extendSchema` arrives.

## Stores

Key-value stores. Each implements the `Store` interface:

```ts
interface Store {
  get(key: string): MaybePromise<Buffer[] | null>
  set(key: string, value: Buffer[]): MaybePromise<void>
  delete(key: string): MaybePromise<void>
  flush(): MaybePromise<void>
}
```

| Store | Import | Description |
| --- | --- | --- |
| `LruStore` | `@morpho-org/viem-dlc/stores` | LRU cache with configurable byte-size limit |
| `TtlStore` | `@morpho-org/viem-dlc/stores` | Wraps any store with an absolute per-entry TTL — bounds how long a warm tier may diverge from a fresher source behind it |
| `MemoryStore` | `@morpho-org/viem-dlc/stores` | Simple in-memory Map (prefer `LruStore`) |
| `HierarchicalStore` | `@morpho-org/viem-dlc/stores` | Layered stores — reads fall through, writes fan out |
| `ThrottledStore` | `@morpho-org/viem-dlc/stores` | Rate-limits writes, coalescing bursts per key under a max staleness timeout |
| `CompressedStore` | `@morpho-org/viem-dlc/stores` | Transparent zstd compression (Node/Bun only) |
| `NodeFsStore` | `@morpho-org/viem-dlc/stores` | One file per key on the local filesystem, atomic writes (Node/Bun only) |
| `UpstashStore` | `@morpho-org/viem-dlc/stores/upstash` | Upstash Redis with automatic value sharding and atomic writes |
| `VercelStore` | `@morpho-org/viem-dlc/stores/vercel` | Vercel Blob; `createOptimizedVercelStore` returns the same pre-composed stack as Upstash's |

### Composing stores

Stores nest, because anything that wraps a store is itself a store. `createOptimizedUpstashStore`,
exported from `@morpho-org/viem-dlc/stores/upstash`, returns a pre-composed stack:

```
LruStore (fast, in-process)
  └─ ThrottledStore (coalesces writes)
       └─ UpstashStore (durable, remote)
```

```ts
import { createOptimizedUpstashStore } from '@morpho-org/viem-dlc/stores/upstash'

const store = createOptimizedUpstashStore({
  maxRequestBytes: 1_000_000,
  maxWritesPerSecond: 300,
})
```

`TtlStore` wraps any store to cap how long its entries stay warm. Fronting a shared remote with a
TTL-bounded in-memory tier keeps reads fast while ensuring a cross-instance write is masked for at
most `ttlMs` — after which the read falls through to the authoritative remote (a plain `LruStore`
front would pin the stale copy for the whole process lifetime):

```ts
import { HierarchicalStore, LruStore, TtlStore } from '@morpho-org/viem-dlc/stores'

const store = new HierarchicalStore(
  [new TtlStore(new LruStore({ maxBytes: 100_000_000 }), { ttlMs: 60_000 }), remote],
  { populateOnMiss: true },
)
```

## Actions

### `getLogs2`

Replaces viem's `getLogs`, adding cache-layer `search` pre-filtering and streaming `reduce`. It
needs a client whose transport uses the `cache()` wrapper — that is, one whose `rpcSchema` is
`CacheSchema`.

`search` is a regex matched against raw NDJSON before parsing — use hex-encoded values
(address fragments, topic prefixes) to skip expensive `JSON.parse` calls on irrelevant batches.
`reduce` folds over decoded logs in order, keeping memory proportional to the accumulator
rather than the full result set.

```ts
import { parseAbiItem } from 'viem'
import { getLogs2 } from '@morpho-org/viem-dlc/actions'

const logs = await getLogs2(client, {
  address: '0x...',
  event: parseAbiItem('event Transfer(address indexed, address indexed, uint256)'),
  fromBlock: 18_000_000n,
  toBlock: 19_000_000n,
  search: 'deadbeef',
  reduce: (acc, log) => {
    acc.push(log) // log.args is already decoded
    return acc
  },
})
```

### `readLens`

Reads a [paginated lens](#paginated-lenses): the lens's per-item function, called once per element
of `args` through the `deployless` or `cache` transport. Takes the same deployless-factory
descriptor as viem's `readContract` (`abi`, `address`, `factory`, `factoryData`) plus the `policy`
options; returns `{ results, skipped }`, with `results` typed from the per-item function's return
type and `skipped` the indices into `args` that were not served.

```ts
import { readLens } from '@morpho-org/viem-dlc/actions'

const { results, skipped } = await readLens(client, {
  ...healthLens.with(MORPHO),          // abi, address, factory, factoryData
  functionName: 'healthOf',            // f(T) returns (U), one parameter, one value
  args: inputs,                        // T[]
  batch: { compress: true },
  cache: { blobKey: 'blue-health', ttl: 60_000 },
})
```

A partial result is a **successful response**: `skipped` merges elements the lens declined
(its per-item call reverted), elements declined client-side for size, and elements
that ran out of gas even when retried alone. Check it if you need every element.

### `eth_call` `policy`

The lower-level marker `readLens` attaches for you: a `stateOverride` entry that tells the
`deployless` or `cache` transport to treat a deployless `eth_call` as a paginated lens read. The
call is encoded against the array-shaped fragment `f(T[]) returns (U[] results, uint256[] skipped)`,
which never exists on-chain; `arrayifiedAbi` derives it from the per-item function. Use it when you
want plain `readContract`/`call` instead of `readLens`. Element bytes round-trip through the cache
untouched, so tuples, nested arrays, and other complex element types are supported.

```ts
policy(opts: {
  abi: AbiFunction              // arrayifiedAbi(itemFragment)
  batch?: {
    compress?: boolean
    gas?: { fixed: number; item: { avg: number; stddev?: number } }
    continuations?: 'fill' | 'eager'
    envelope?: 'initcode' | 'override'
  }
  cache?: {
    blobKey: string
    ttl: number
    delta?: number
  }
})
```

- **`opts.abi`** — the array-shaped fragment from `arrayifiedAbi`. Build it from the per-item
  fragment in the contract's real ABI: the transport derives the per-item selector from it, and a
  selector the lens does not implement fails as a page that skips every element.
- **`opts.batch`** — optional batching config. What bounds a chunk's bytes is not here: the chain
  states its initcode limit and the transport states the provider's `batchSize`.
- **`opts.batch.compress`** — FastLZ-compress calldata on the wire, so more elements fit per
  chunk at the cost of encoding time and decompression gas. The envelope decompresses element by
  element as it attempts them, so a highly compressible chunk pages like any other and costs
  nothing before its first element.
- **`opts.batch.gas`** — the lens's cost, in the units the wide event reports it: `fixed` from
  `fixed_gas`, what a frame spends before its first attempt less the copy of the chunk's own bytes,
  `item.avg` and `item.stddev` from `item_gas_avg` and `item_gas_stddev`. Together with the
  transport's `gasLimit` it sizes the
  opening wave: a chunk is as many elements as fit the cap after the calldata's intrinsic gas and
  `fixed`, with the same headroom for the spread that continuations keep. Every later chunk is
  sized from what the pages report, so this only matters until the first attempt has been costed:
  understating the cost costs one continuation, overstating it costs extra parallel requests. A property of the
  lens, so one value serves every provider and chain.
- **`opts.batch.continuations`** — when the elements a page did not reach are re-sent. `fill`
  (the default) pools them across pages: a full page's worth goes at once, the remainder once no
  earlier chunk that could still add to it is in flight, so small tails from many pages travel
  together. `eager` sends every tail as soon as its page lands: more requests, no waiting.
  Anything else reads as `fill`.
- **`opts.batch.envelope`** — how a chunk reaches the node. `initcode` (the default) creates the
  envelope with the elements trailing it, bounded by the chain's initcode cap. `override` calls the
  envelope at a fixed address placed by `eth_call`'s state-override parameter, so the frame's gas is
  the only bound; a provider that does not honour overrides is detected on the opening wave and the
  range re-fetched as initcode, at the cost of one wasted wave per request, which
  `override_fallbacks_unsupported` reports. It pays only when bytes bind, which the wide event says: `(gas_limit_observed − fixed_gas) /
  item_gas_avg` well above `elements_requested / nominal_batches`.
- **`opts.cache`** — optional cache config, honored by `cache(...)` only. If omitted,
  or when used with `deployless(...)`, `batch` is still honored without caching.
- **`opts.cache.blobKey`** — identifies the backing store blob. Requests with the same
  `blobKey` share storage; different `blobKey`s are isolated into different blobs.
- **`opts.cache.ttl`** — maximum age in milliseconds before a cached entry is
  considered stale.
- **Semantic requirement** — the per-item function must be elementwise: each served value, and
  each decline, depends only on its own element plus shared chain state, never on other elements,
  their multiplicity, their order, or the gas the frame happened to have.
- **`opts.cache.delta`** — XFetch early-refresh scale in milliseconds. On each
  freshness check the handler samples `u ~ Uniform(0, 1]` and treats the entry as
  stale once `age - delta * ln(u) >= ttl`, so entries may refresh up to several
  `delta` before `ttl` but never later. Desynchronizes refreshes across many keys
  populated together, avoiding stampedes. Based on Vattani et al., "Optimal
  Probabilistic Cache Stampede Prevention" (2015), assuming constant recompute
  cost. Defaults to 0 (disabled).

The cache derives keys from `(targetTo, factory, factoryData, selector, inputElement)`,
so repeat elements collapse into a single blob entry and novel elements are appended to
the blob on the next fetch. The handler rejects every tx envelope field besides `data`,
including `from`, `gas` and `value`.

#### Paginated lenses

A lens is one `view` function over one element. That is the whole contract:

```solidity
contract BlueHealthLens {
  IMorpho immutable morpho;
  constructor(IMorpho _morpho) { morpho = _morpho; }

  function healthOf(Input calldata x) external view returns (Health memory) {
    Market memory m = morpho.market(x.id);
    require(m.lastUpdate != 0);          // permanent condition → skipped
    return _health(x.id, m, x.borrower);
  }
}
```

The envelope — the bytecode this package puts in front of every deployless call — reads the element
array, calls the per-item function **once per element in its own frame** with all remaining gas,
and deposits each result straight into the response. Per element, exactly one of three things
happens. The call **returns**: the result is kept. It **reverts** (any reason, any data): the
index goes to `skipped` — so keep per-item reverts to conditions that are permanent, since a
broken dependency is skipped the same way, and the revert reason is not surfaced. It **runs out
of gas**: EIP-150 guarantees the envelope keeps 1/64 of what it forwarded, which is enough to
report that in-band and stop; the transport retries the element once on its own, where it holds
the largest frame a node can grant, and only if it dies there too does it land in the caller's
`skipped`. Before each attempt the envelope also checks that the frame can pay for the memory the
attempt would touch and still report an outcome, priced from the fee schedule; below that, it
stops (or, for element 0, reports it unresolved without attempting).
So a frame never dies mid-page, and the prologue grows with the chunk only by the copy of its
bytes, which the packer prices; a constructor too heavy for a node's cap is reported as an error
rather than halved.

No element type needs a number from the author: static sizes come from the ABI, dynamic inputs
carry their length on the wire, and dynamic results carry theirs in returndata. The envelope
refuses an ill-formed result (`MalformedResult`, surfaced as a protocol error rather than halved).

**Shared work goes in the constructor.** Item frames share no memory, but the counterfactual
deploy runs the constructor inside the same `eth_call`: immutables hold value types, and storage
written in the constructor is readable from every per-item call. EIP-2929 warmth is per
transaction, so the first element to touch a market's storage warms it for all later elements in
the chunk. The constructor runs once per chunk in the prologue, so keep it bounded: one that
exhausts the node's cap fails the request outright.

What the envelope cannot enforce, and the lens must still honor: **skips are deterministic** (a
revert means invalid input or a permanently failing element, never something more gas would pass)
and **values are batching-invariant** (neither a served value nor a decline may depend on
position, batch composition, or `gasleft()`).

**What the caller sees.** `readLens` returns `{ results, skipped }` typed from the per-item
function. Through plain viem, the chunked calls aggregate into one page over the whole input, in
the shape `arrayifiedAbi` declares, so `readContract` and `decodeFunctionResult` work too:

```ts
const pageAbi = arrayifiedAbi(getAbiItem({ abi: healthLens.abi, name: 'healthOf' }))

const [results, skipped] = await readContract(client, {
  ...healthLens.with(MORPHO),
  abi: [pageAbi],                       // after the spread: the real ABI has no array function
  functionName: 'healthOf',
  args: [inputs],
  stateOverride: [policy({ abi: pageAbi })],
})
```

`skipped` is rebased onto the caller's input, and expands across deduplicated inputs, so its
indices always address the array you passed. Elements that ran out of gas even alone depend on
the node's `eth_call` gas cap, so a provider with a higher cap might serve them;
`elements_unresolved` counts them in the wide event.

### `getDeploymentBlockNumber`

Finds the block a contract was deployed at, by binary search over `getCode`.

```ts
import { createPublicClient, http } from 'viem'
import { mainnet } from 'viem/chains'
import { getDeploymentBlockNumber } from '@morpho-org/viem-dlc/actions'

const client = createPublicClient({ chain: mainnet, transport: http() })

const block = await getDeploymentBlockNumber(client, {
  address: '0x...',
})
```

## Utilities

Exported from `@morpho-org/viem-dlc/utils`:

- `divideBlockRange` / `mergeBlockRanges` / `halveBlockRange` — block range manipulation
- `resolveBlockNumber` / `extractRangeFromFilter` / `isInBlockRange` — block number helpers
- `classifyBlockRangeError` — classify RPC errors as range-related, timeout-like, or neither
- `createCoalescingMutex` — per-resource leader/follower batching
- `createTokenBucket` / `createRateLimit` — rate limiting primitives
- `cyrb64Hash` — fast string hashing
- `stringify` / `parse` / `estimateUtf8Bytes` — JSON serialization with bigint support
- `pick` / `omit` — object helpers
- `measureUtf8Bytes` / `shardString` — string utilities

The envelope's codecs are exported separately, from `@morpho-org/viem-dlc/utils/deployless`, for
building fixtures and mocks: everything needed to read a chunk off a request the transports sent and
to answer it the way a node running the envelope would.

- `unwrapDeploylessFactoryCall` / `encodeEnvelopeArgs` / `envelopeConfig` / `deliveryParams` — the
  outbound request, both deliveries
- `decodeEnvelopeRevert`, `FACTORY_BYTECODE_REVERT`, `ENVELOPE_ADDRESS`, `OK_SENTINEL` and the other
  sentinels — what the envelope reverts with, and what identifies it
- `arrayifiedAbi` / `resolveArrayFunction` / `abiToArray` / `arrayToAbi` — the caller's array
- `arrayToWire` / `wireToArray` / `streamToPage` / `pageToStream` / `pageToAbi` — the envelope's wire
  and the page it reverts

The packer, the cost model and the FastLZ codec stay internal: their shapes follow the
implementation rather than the wire.
