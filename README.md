# @morpho-org/viem-dlc

A collection of flexible [viem](https://viem.sh) extensions with a focus on intelligent caching.
Provides composable transport wrappers for optimized `eth_getLogs` and `eth_call` handling with
caching, rate limiting, automatic request splitting, and oversized-log filtering.

## Installation

```bash
pnpm add @morpho-org/viem-dlc
```

Also available on the [GitHub Package Registry](https://npm.pkg.github.com).

## Transports

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

Token-bucket rate limiting with concurrency limiting and priority scheduling:

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

### `eth_call` `cachePolicy`

Creates a `stateOverride` entry that tells the `cache` transport how to cache an `eth_call`.
Works with viem's `call` action against a contract exposing a single dynamic-array input
and a single dynamic-array output (e.g. `balancesOf(address[]) -> uint256[]`), invoked via
viem's deployless-factory pattern (`call({ factory, factoryData, to, data, ... })`). The
handler decodes the outer array structurally — element bytes round-trip through the cache
untouched, so tuples, nested arrays, and other complex element types are supported.

```ts
cachePolicy(blobKey: string, ttl: number, opts: { batchSize?: number; abi: AbiFunction })
```

- **`blobKey`** — groups cached results into a named store entry. Encode any state
  context (block, target contract identity, caller-dependent overrides) that would
  invalidate results across requests.
- **`ttl`** — maximum age in milliseconds before a cached entry is considered stale.
- **`opts.abi`** — the `AbiFunction` fragment for the callee. Must have exactly one
  input and one output, both dynamic arrays.
- **`opts.batchSize`** — maximum bytes of the `eth_call` `data` field when fetching
  misses. Misses are greedy-packed into chunks under this limit and fetched in parallel.
  Defaults to no splitting.

```ts
import { encodeFunctionData, parseAbiItem } from 'viem'
import { call } from 'viem/actions'
import { cachePolicy } from '@morpho-org/viem-dlc/actions'

const positionsAbi = parseAbiItem(
  'function positions((bytes32 id, address user)[] inputs) view returns ((uint256,uint128,uint128)[])'
)

const policy = cachePolicy('morpho-positions', 300_000, {
  batchSize: 1 << 15,
  abi: positionsAbi,
})

const result = await call(client, {
  factory,      // deployed factory address
  factoryData,  // calldata that makes `factory` deploy the lens helper
  to,           // deterministic deployment address of the lens
  data: encodeFunctionData({ abi: [positionsAbi], functionName: 'positions', args: [inputs] }),
  stateOverride: [policy],
})
```

Cache keys are derived from `(targetTo, factory, factoryData, selector, inputElement)`,
so repeat elements collapse into a single blob entry and novel elements are appended to
the blob on the next fetch. The handler rejects any tx envelope field besides `data`
(`from`, `gas`, `value`, etc.) — if results depend on caller identity, encode that into
`blobKey`.

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
- `isErrorCausedByBlockRange` — detect RPC "block range too large" errors
- `createCoalescingMutex` — per-resource leader/follower batching
- `createTokenBucket` / `createRateLimit` — rate limiting primitives
- `cyrb64Hash` — fast string hashing
- `stringify` / `parse` / `estimateUtf8Bytes` — JSON serialization with bigint support
- `pick` / `omit` — object helpers
- `measureUtf8Bytes` / `shardString` — string utilities
