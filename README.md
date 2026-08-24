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
deployless-factory calls under `batchSize`, and forwards everything else unchanged.

```ts
import { createPublicClient, encodeFunctionData, http, parseAbiItem } from 'viem'
import { call } from 'viem/actions'
import { deployless } from '@morpho-org/viem-dlc/transports'
import { policy } from '@morpho-org/viem-dlc/actions'

const positionsAbi = parseAbiItem(
  'function positions((bytes32 id, address user)[] inputs) view returns ((uint256,uint128,uint128)[])'
)

const client = createPublicClient({
  transport: deployless(http(rpcUrl)),
})

const result = await call(client, {
  factory,
  factoryData,
  to,
  data: encodeFunctionData({ abi: [positionsAbi], functionName: 'positions', args: [inputs] }),
  stateOverride: [
    policy({
      abi: positionsAbi,
      batchSize: 1 << 15,
    }),
  ],
})
```

If `policy.cache` is present, `deployless(...)` ignores it and still behaves as split-only mode.
Use `cache(...)` when you want the same marked calls to populate and read from a backing store.

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
    store: new LruStore(100_000_000),
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
limits. Each branch is a fully-built per-provider stack carrying its own `maxBlockRange` /
`gasLimit`. Branches are constructed once at composition time, so stateful inner transports
(coalescing mutexes, rate-limiter token buckets) persist across requests instead of being
rebuilt per call — unlike viem's stock `fallback`, which rebuilds the active branch on every
request and effectively disables those features.

```ts
import { createPublicClient, http } from 'viem'
import { mainnet } from 'viem/chains'
import { failover } from '@morpho-org/viem-dlc/transports'
import { cache, createSimpleInvalidation } from '@morpho-org/viem-dlc/transports/cache'
import { LruStore } from '@morpho-org/viem-dlc/stores'

const store = new LruStore(100_000_000)
const sharedConfig = { binSize: 10_000, store, invalidationStrategy: createSimpleInvalidation() }

const transport = failover([
  cache(http(rpcUrlA), [{ ...sharedConfig, gasLimit: 30_000_000 }, { maxBlockRange: 100_000 }]),
  cache(http(rpcUrlB), [{ ...sharedConfig, gasLimit: 50_000_000 }, { maxBlockRange: 10_000 }]),
])

const client = createPublicClient({ chain: mainnet, transport })
```

Each branch's `logsDivider` chunks requests at its own `maxBlockRange`, so neither provider
is sized for the lowest common denominator. The shared `Store` means partial fetches from
branch A persist in cache and are visible to branch B on fallover, making recovery cheap.

`failover` only sees errors that escape per-branch halving (`logsDivider` range-halving and
`deployless` size-bisection run inside each branch first). By default, contract reverts and
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

### `eth_call` `policy`

Creates a `stateOverride` entry that tells the `deployless` or `cache` transport how
to handle a deployless `eth_call`. Works with viem's `call` action against a contract
exposing a single dynamic-array input and a single dynamic-array output (e.g.
`balancesOf(address[]) -> uint256[]`), invoked via viem's deployless-factory pattern
(`call({ factory, factoryData, to, data, ... })`). The transports decode the outer
array structurally; when used with `cache`, element bytes round-trip through the cache
untouched, so tuples, nested arrays, and other complex element types are supported.

```ts
policy(opts: {
  abi: AbiFunction
  batchSize?: number
  cache?: {
    blobKey: string
    ttl: number
    delta?: number
  }
})
```

- **`opts.abi`** — the `AbiFunction` fragment for the callee. Must have exactly one
  input and one output, both dynamic arrays.
- **`opts.batchSize`** — maximum bytes of the `eth_call` `data` field when fetching
  chunks. Input elements are greedy-packed under this limit and fetched in parallel.
  Defaults to no splitting.
- **`opts.cache`** — optional cache config, honored by `cache(...)` only. If omitted,
  or when used with `deployless(...)`, `batchSize` is still honored without caching.
- **`opts.cache.blobKey`** — identifies the backing store blob. Requests with the same
  `blobKey` share storage; different `blobKey`s are isolated into different blobs.
- **`opts.cache.ttl`** — maximum age in milliseconds before a cached entry is
  considered stale.
- **Semantic requirement** — beyond the ABI shape, the callee must be elementwise:
  for an input array `[x0, ..., xn]`, it must return `[y0, ..., yn]` with the same
  length and order, where each `yi` depends only on `xi` plus shared chain state,
  not on other elements, their multiplicity, or their order.
- **`opts.cache.delta`** — XFetch early-refresh scale in milliseconds. On each
  freshness check the handler samples `u ~ Uniform(0, 1]` and treats the entry as
  stale once `age - delta * ln(u) >= ttl`, so entries may refresh up to several
  `delta` before `ttl` but never later. Desynchronizes refreshes across many keys
  populated together, avoiding stampedes. Based on Vattani et al., "Optimal
  Probabilistic Cache Stampede Prevention" (2015), assuming constant recompute
  cost. Defaults to 0 (disabled).

```ts
import { encodeFunctionData, parseAbiItem } from 'viem'
import { call } from 'viem/actions'
import { policy } from '@morpho-org/viem-dlc/actions'

const positionsAbi = parseAbiItem(
  'function positions((bytes32 id, address user)[] inputs) view returns ((uint256,uint128,uint128)[])'
)

const cachePolicy = policy({
  batchSize: 1 << 15,
  abi: positionsAbi,
  cache: {
    blobKey: 'morpho-positions',
    ttl: 300_000,
  },
})

const result = await call(client, {
  factory,      // deployed factory address
  factoryData,  // calldata that makes `factory` deploy the lens helper
  to,           // deterministic deployment address of the lens
  data: encodeFunctionData({ abi: [positionsAbi], functionName: 'positions', args: [inputs] }),
  stateOverride: [cachePolicy],
})
```

Cache keys are derived from `(targetTo, factory, factoryData, selector, inputElement)`,
so repeat elements collapse into a single blob entry and novel elements are appended to
the blob on the next fetch. The handler rejects any tx envelope field besides `data`
(`from`, `gas`, `value`, etc.).

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
