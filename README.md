# @morpho-org/viem-dlc

A collection of flexible [viem](https://viem.sh) extensions with a focus on intelligent caching.
Provides composable transport wrappers for optimized `eth_getLogs` fetching with caching,
rate limiting, and automatic request splitting.

## Installation

```bash
pnpm add @morpho-org/viem-dlc
```

> Published to the GitHub Package Registry. You may need to configure your `.npmrc`:
>
> ```
> @morpho-org:registry=https://npm.pkg.github.com
> ```

## Transports

### `logsCache`

All-in-one caching transport for `eth_getLogs`. Internally composes rate limiting, request
splitting, and caching. Requires a `chain` on the client so it can namespace cache keys by
chain ID.

```ts
import { createPublicClient, http } from 'viem'
import { mainnet } from 'viem/chains'
import { logsCache, createSimpleInvalidation } from '@morpho-org/viem-dlc/transports'
import { LruStore } from '@morpho-org/viem-dlc/stores'

const transport = logsCache(http(rpcUrl), {
  binSize: 10_000,
  store: new LruStore(100_000_000),
  invalidationStrategy: createSimpleInvalidation(),
  logsDividerConfig: { maxBlockRange: 100_000 },
  rateLimiterConfig: { maxRequestsPerSecond: 10, maxBurstRequests: 5 },
})

const client = createPublicClient({ chain: mainnet, transport })
```

The `binSize` determines cache entry granularity. Requests are aligned to bin boundaries
to maximize cache hits. Smaller bins allow finer-grained invalidation but increase storage
overhead.

Two invalidation strategies are provided:

- `createSimpleInvalidation(minAgeMs?, maxAgeDays?, numHotBlocks?, avgInvalidationsPerRequest?)` — entries near the chain tip are always refetched; older entries are probabilistically invalidated based on age.
- `createExponentialInvalidation(alphaAge?, maxAgeDays?, alphaBlocks?, scaleBlocks?)` — exponential model with separate time and block-age factors.

### `logsDivider`

Splits large `eth_getLogs` requests into smaller chunks with concurrent fetching and automatic
retry with range halving on failure.

```ts
import { logsDivider } from '@morpho-org/viem-dlc/transports'

const transport = logsDivider(http(rpcUrl), {
  maxBlockRange: 100_000,
  maxConcurrentChunks: 5,
  alignTo: 10_000,
  onLogsResponse: ({ logs, fromBlock, toBlock }) => {
    /* progressive updates */
  },
})
```

### `rateLimiter`

Token bucket rate limiting with FIFO ordering:

```ts
import { rateLimiter } from '@morpho-org/viem-dlc/transports'

const transport = rateLimiter(http(rpcUrl), {
  maxRequestsPerSecond: 10,
  maxBurstRequests: 5,
})
```

## Stores

Key-value stores implementing the `Store` interface:

```ts
interface Store {
  get(key: string): MaybePromise<string | null>
  set(key: string, value: string): MaybePromise<void>
  delete(key: string): MaybePromise<void>
}
```

| Store | Import | Description |
| --- | --- | --- |
| `LruStore` | `@morpho-org/viem-dlc/stores` | LRU cache with configurable byte-size limit |
| `MemoryStore` | `@morpho-org/viem-dlc/stores` | Simple in-memory Map (prefer `LruStore`) |
| `HierarchicalStore` | `@morpho-org/viem-dlc/stores` | Layered stores — reads fall through, writes fan out |
| `DebouncedStore` | `@morpho-org/viem-dlc/stores` | Batches writes with debounce + max staleness timeout |
| `CompressedStore` | `@morpho-org/viem-dlc/stores/compressed` | Transparent gzip compression (Node/Bun only) |
| `UpstashStore` | `@morpho-org/viem-dlc/stores/upstash` | Upstash Redis with automatic value sharding and atomic writes |

### Composing stores

Stores are designed to be layered. For example, `createOptimizedUpstashStore` (exported from
`@morpho-org/viem-dlc/stores/upstash`) returns a pre-composed stack:

```
LruStore (fast, in-process)
  └─ DebouncedStore (coalesces writes)
       └─ CompressedStore (reduces payload)
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
- `resolveBlockNumber` — convert block tags to numbers
- `isErrorCausedByBlockRange` — detect RPC "block range too large" errors
- `createTokenBucket` / `withRateLimit` — rate limiting primitives
- `createKeyedMutex` / `withKeyedMutex` — per-key concurrency control
- `cyrb64Hash` — fast string hashing
- `stringify` / `parse` — JSON serialization with bigint support
