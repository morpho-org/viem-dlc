# @morpho-org/viem-dlc

Viem transport wrappers and utilities for optimized `eth_getLogs` fetching with caching, rate
limiting, and automatic request splitting.

## Transports

### `logsCache`

All-in-one caching transport for `eth_getLogs`. Internally composes rate limiting, request
splitting, and caching.

```ts
import { logsCache, createSimpleInvalidation } from '@morpho-org/viem-dlc/transports'
import { MemoryStore } from '@morpho-org/viem-dlc/stores'

const transport = logsCache(http(rpcUrl), {
  binSize: 10_000,
  store: new MemoryStore(),
  invalidationStrategy: createSimpleInvalidation(),
  maxCacheShardBytes: 1_000_000,
  logsDividerConfig: { maxBlockRange: 100_000 },
  rateLimiterConfig: { maxRequestsPerSecond: 10, maxConcurrentRequests: 5 }
})
```

### `logsDivider`

Splits large `eth_getLogs` requests into smaller chunks with automatic retry and range halving on
failure:

```ts
import { logsDivider } from '@morpho-org/viem-dlc/transports'

const transport = logsDivider(http(rpcUrl), {
  maxBlockRange: 100_000,
  maxConcurrentChunks: 5,
  alignTo: 10_000, // Optional: align chunks to boundaries
  onLogsResponse: response => {
    /* progressive updates */
  }
})
```

### `rateLimiter`

Token bucket rate limiting with FIFO ordering:

```ts
import { rateLimiter } from '@morpho-org/viem-dlc/transports'

const transport = rateLimiter(http(rpcUrl), {
  maxRequestsPerSecond: 10,
  maxConcurrentRequests: 5
})
```

## Stores

Key-value stores implementing the `Store` interface:

| Store               | Description                                                              |
| ------------------- | ------------------------------------------------------------------------ |
| `MemoryStore`       | In-memory Map-based storage                                              |
| `HierarchicalStore` | Layered stores (e.g., memory + persistent)                               |
| `DebouncedStore`    | Batches writes after inactivity period                                   |
| `CompressedStore`   | Gzip compression (import from `@morpho-org/viem-dlc/stores/compressed`) |
| `VercelStore`       | Vercel Runtime Cache (import from `@morpho-org/viem-dlc/stores/vercel`) |

## Caches

Higher-level cache abstractions built on stores:

| Cache          | Description                                                    |
| -------------- | -------------------------------------------------------------- |
| `SimpleCache`  | Direct key-value cache                                         |
| `ShardedCache` | Groups related keys into shards for efficient batch operations |

## Utilities

- `divideBlockRange` / `mergeBlockRanges` / `halveBlockRange` - Block range manipulation
- `resolveBlockNumber` - Convert block tags to numbers
- `isErrorCausedByBlockRange` - Detect RPC range errors
- `createTokenBucket` / `withRateLimit` - Rate limiting primitives
