# @morpho-org/viem-dlc

A collection of flexible [viem](https://viem.sh) extensions with a focus on intelligent caching.
Provides composable transport wrappers for optimized `eth_getLogs` and deployless `eth_call`
handling with caching, rate limiting, automatic request splitting, and oversized-log filtering.

## Installation

```bash
pnpm add @morpho-org/viem-dlc
```

Also available on the [GitHub Package Registry](https://npm.pkg.github.com).

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
  logger,           // anything satisfying the `Logger` interface, e.g. a LogLayer instance
  service: 'indexer', // extra opts become context fields on every event
})
```

Each outermost `client.request` made inside a `withLogging` scope emits a single
`"concluded"` wide event. Transports contribute flat, queryable fields under their
key — e.g. `viem-dlc-failover.succeeded_index`, `viem-dlc-logs-divider.logs_fetched` —
and layers crossed many times per call (e.g. once per chunk under the divider)
accumulate totals there (e.g. `viem-dlc-logs-sieve.logs_dropped`). If a call crosses
several *instances* of the same transport — say, one cache per failover branch — later
instances are suffixed `.1`, `.2`, ... in first-touch order, which is stable for a
given composition. Every layer also stamps a per-instance `crossings` count, so the
event records which transports the call traversed and how many times each. Call-level
fields are `call_id`, `duration_ms`, and `status` (`"ok"` or `"error"`). Failed calls
emit at `error` level with the error attached via `withError`, so hosts that forward
`withError` entries to an error reporter (e.g. Sentry) capture them automatically.

## Transports

### `deployless`

Thin transport wrapper for deployless `eth_call` splitting. It only intercepts calls carrying
the `policy(...)` sentinel in `stateOverride`, re-packs the marked input array into one or more
deployless-factory calls under a wire byte budget (`batch.batchSize`), aggregates the pages that
come back, and forwards everything else unchanged.
There is no gas configuration: the envelope calls the lens's per-item function once per element
in its own frame and reports how far it got, so a chunk adapts to whatever gas the node grants —
see [Paginated lenses](#paginated-lenses). Most callers reach it through
[`readLens`](#readlens) rather than building the call by hand.

```ts
import { createPublicClient, encodeFunctionData, http, parseAbiItem } from 'viem'
import { call } from 'viem/actions'
import { deployless } from '@morpho-org/viem-dlc/transports'
import { arrayifiedAbi, policy } from '@morpho-org/viem-dlc/actions'

// The lens implements `positionOf((bytes32,address)) view returns ((uint256,uint128,uint128))`;
// the array-shaped fragment the wire carries is derived from it.
const positionsAbi = arrayifiedAbi(
  parseAbiItem('function positionOf((bytes32 id, address user) input) view returns ((uint256,uint128,uint128))')
)

const client = createPublicClient({
  transport: deployless(http(rpcUrl)),
})

const result = await call(client, {
  factory,
  factoryData,
  to,
  data: encodeFunctionData({ abi: [positionsAbi], functionName: 'positionOf', args: [inputs] }),
  stateOverride: [policy({ abi: positionsAbi, batch: { batchSize: 1 << 15 } })],
})
```

If `policy.cache` is present, `deployless(...)` ignores it and still behaves as split-only mode.
Use `cache(...)` when you want the same marked calls to populate and read from a backing store.

With observability enabled, batching reports `elements_requested` / `elements_fetched`,
`nominal_batches` and `batch_bytes` (sizes of the initial packing against the wire budget;
halved and continued chunks are not resampled), and `splits_*` for chunks halved after an
error: `splits_size` (413 / initcode-size errors) and `splits_timeout`. Nothing in the envelope's
prologue grows with the chunk, so a frame that dies without reporting is a constructor too heavy
for the node's cap, surfaced as an error rather than halved. Pagination is normal rather than a
failure and gets its own
fields: `pages_continued` (responses that stopped early, each repacked into one or more requests),
`pages_waves`, `page_adjudicated` (elements per page, as a stat — a lens yielding ~1 per page is
pathological), `pages_all_skipped` (pages whose every element reverted — a per-item selector the
lens does not implement is one cause), `attempts_unresolved` (elements a frame's gas could not
resolve, whether the per-item frame died or the envelope refused to start it), `pages_escalated`
(singleton retries those cost), and, matching the response's
`skipped` array: `elements_missing` in total, of which `elements_declined_oversize` could not fit
a chunk alone under `batch.batchSize` and `elements_unresolved` were gas-terminal even alone —
the subset another provider with a higher cap might still serve.

Every page also reports what its attempts cost, and the request pools it: `frame_gas` (the gas a
frame had for attempts, on the smallest frame seen), `item_gas_avg` / `item_gas_stddev` /
`item_gas_max` per attempt, and `page_size_suggested`, the chunk size the transport would open
with given those numbers — the value for `batch.pageSizeHint`, which is stamped as `page_size_hint` when
set. To pick a hint, run without one under observability and take the median suggestion over a
representative window; a hint far from the suggestion is stale, and `pages_continued` at zero
with the suggestion above the hint means it undershoots. To keep learning once a hint is set,
scale it on a sampled fraction of requests, e.g. `(Math.random() < 0.1 ? 2 : 1) * hint`. Costs
depend on which items share a frame: grouping related elements warms storage they share and
lowers `item_gas_avg`, shuffling makes the rate uniform across chunks; results align to `args` in
either order. A full cache hit or an empty input makes no upstream call and carries none of
these fields.

### `cache`

All-in-one caching transport for `eth_getLogs` and `eth_call`. Internally composes five layers:
oversized-log filtering (`logsSieve`), log enrichment (`logsEnricher`), rate limiting (`rateLimiter`),
request splitting (`logsDivider`), and caching. Requires a `chain` on the client so it can
namespace cache keys by chain ID.

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

The `binSize` determines cache entry granularity. Requests are aligned to bin boundaries
to maximize cache hits. Smaller bins allow finer-grained invalidation but increase storage
overhead. The `logsDivider` config's `alignTo` is automatically set to `binSize`.

Two invalidation strategies are provided:

- `createSimpleInvalidation(minAgeMs?, maxAgeDays?, numHotBlocks?, avgInvalidationsPerRequest?)` — entries near the chain tip are always refetched; older entries are probabilistically invalidated based on age.
- `createExponentialInvalidation(alphaAge?, maxAgeDays?, alphaBlocks?, scaleBlocks?)` — exponential model with separate time and block-age factors.

### `failover`

Request-level fallback dispatcher for fronting multiple RPC providers with provider-specific
limits. Each branch is a fully-built per-provider stack carrying its own `maxBlockRange`; nothing
gas-related is configured per branch, since deployless lenses adapt to each node's grant on their
own. Branches are constructed once at composition time, so stateful inner transports
(coalescing mutexes, rate-limiter token buckets) persist across requests instead of being
rebuilt per call — unlike viem's stock `fallback`, which rebuilds the active branch on every
request and effectively disables those features.

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

Splits large `eth_getLogs` requests into smaller chunks with automatic retry, optional alignment,
internal rate/concurrency limiting via `rateLimiter`, log enrichment via `logsEnricher`, and
oversized-log filtering via `logsSieve`.

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

Enriches `eth_getLogs` responses with data that standard RPCs omit. Currently supports
populating `blockTimestamp` by fetching block headers. Logs whose block was reorged away
are silently dropped.

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

Filters `eth_getLogs` responses by estimated UTF-8 payload size. Any `RpcLog` whose serialized
size exceeds `maxBytes` is silently dropped. `logsDivider(...)` and `cache(...)` already
compose this transport by default; use `logsSieve(...)` directly when filtering is all you need.

```ts
import { createPublicClient, http } from 'viem'
import { logsSieve } from '@morpho-org/viem-dlc/transports'

const transport = logsSieve(http(rpcUrl), [{ maxBytes: 8_192 }])

const client = createPublicClient({ transport })
```

### `rateLimiter`

Token-bucket rate limiting with concurrency limiting and priority scheduling. When
observability is enabled it reports `queue_wait_ms` (admission wait, summarized over
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

## Stores

Key-value stores implementing the `Store` interface:

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
| `DebouncedStore` | `@morpho-org/viem-dlc/stores` | Batches writes with debounce + max staleness timeout |
| `CompressedStore` | `@morpho-org/viem-dlc/stores` | Transparent zstd compression (Node/Bun only) |
| `UpstashStore` | `@morpho-org/viem-dlc/stores/upstash` | Upstash Redis with automatic value sharding and atomic writes |

### Composing stores

Stores are designed to be layered. For example, `createOptimizedUpstashStore` (exported from
`@morpho-org/viem-dlc/stores/upstash`) returns a pre-composed stack:

```
LruStore (fast, in-process)
  └─ DebouncedStore (coalesces writes)
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

Drop-in replacement for viem's `getLogs` that adds cache-layer `search` pre-filtering and
streaming `reduce`. Requires a client whose transport uses the `cache()` wrapper (i.e. whose
`rpcSchema` is `CacheSchema`).

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
import { readLens, MAX_INITCODE_SIZE } from '@morpho-org/viem-dlc/actions'

const { results, skipped } = await readLens(client, {
  ...healthLens.with(MORPHO),          // abi, address, factory, factoryData
  functionName: 'healthOf',            // f(T) returns (U), one parameter, one value
  args: inputs,                        // T[]
  batch: { batchSize: MAX_INITCODE_SIZE, compress: true },
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
    batchSize?: number
    compress?: boolean
    pageSizeHint?: number
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
- **`opts.batch`** — optional batching config. Omit to send all elements in a single upstream
  `eth_call`.
- **`opts.batch.batchSize`** — maximum bytes of the `eth_call` `data` field per chunk; elements
  are greedy-packed under it and fetched in parallel. `MAX_INITCODE_SIZE` (EIP-3860's 49 152
  bytes) is the usual value. The cap is not tuned per lens, chain, or provider.
- **`opts.batch.compress`** — FastLZ-compress calldata on the wire, so more elements fit per
  chunk at the cost of encoding time and decompression gas. The envelope decompresses element by
  element as it attempts them, so a highly compressible chunk pages like any other and costs
  nothing before its first element.
- **`opts.batch.pageSizeHint`** — elements per chunk in the opening wave, beside the byte budget.
  Later waves size themselves from what the pages report, so this only matters before the first
  response: too high costs one continuation wave, too low costs extra parallel requests. Read
  `page_size_suggested` off the wide event of a request made without it.
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

Cache keys are derived from `(targetTo, factory, factoryData, selector, inputElement)`,
so repeat elements collapse into a single blob entry and novel elements are appended to
the blob on the next fetch. The handler rejects any tx envelope field besides `data`
(`from`, `gas`, `value`, etc.).

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

The envelope — the initcode this package sends with every deployless call — reads the element
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
So a frame never dies mid-page, and nothing in the prologue grows with the chunk; a constructor
too heavy for a node's cap is reported as an error rather than halved.

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

Finds the block at which a contract was deployed using binary search over `getCode`.

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
