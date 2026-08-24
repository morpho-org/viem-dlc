# @morpho-org/viem-dlc

A collection of flexible [viem](https://viem.sh) extensions with a focus on intelligent caching.
Provides composable transport wrappers for optimized `eth_getLogs` and deployless `eth_call`
handling with caching, rate limiting, automatic request splitting, and oversized-log filtering.

## Installation

```bash
pnpm add @morpho-org/viem-dlc
```

Also available on the [GitHub Package Registry](https://npm.pkg.github.com).

## Transports

### `deployless`

Thin transport wrapper for deployless `eth_call` splitting. It only intercepts calls carrying
the `policy(...)` sentinel in `stateOverride`, re-packs the marked input array into one or more
deployless-factory calls under both a byte budget (`batch.batchSize`) and a gas budget
(`batch.gas` against the transport's `gasLimit`), and forwards everything else unchanged.

```ts
import { createPublicClient, encodeFunctionData, http, parseAbiItem } from 'viem'
import { call } from 'viem/actions'
import { deployless } from '@morpho-org/viem-dlc/transports'
import { policy } from '@morpho-org/viem-dlc/actions'

const positionsAbi = parseAbiItem(
  'function positions((bytes32 id, address user)[] inputs) view returns ((uint256,uint128,uint128)[])'
)

const client = createPublicClient({
  transport: deployless(http(rpcUrl), { gasLimit: 30_000_000 }),
})

const result = await call(client, {
  factory,
  factoryData,
  to,
  data: encodeFunctionData({ abi: [positionsAbi], functionName: 'positions', args: [inputs] }),
  stateOverride: [
    policy({
      abi: positionsAbi,
      batch: {
        batchSize: 1 << 15,
        gas: { constant: 50_000, linear: 30_000, quadratic: 0 },
      },
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
    gasLimit: 30_000_000,
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
user-rejection errors propagate immediately instead of triggering fallover, as do errors that
already carry a usable payload — a paged `DeploylessPartialResultError`. Falling over on one of
those would swap a real answer for whatever the next branch returns, and lose it outright if
that branch fails for an unrelated reason. (viem's stock `fallback` does not make this
distinction, which is one more reason to prefer `failover` for paged reads.)

Payload-carrying errors are handled by `failover` itself, ahead of `shouldThrow`, and are not
overridable: this library produced them and knows their semantics better than a caller-supplied
predicate can. `shouldThrow` governs everything else — pass one to override:

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
  [new TtlStore(new LruStore(100_000_000), { ttlMs: 60_000 }), remote],
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
  paged?: boolean
  batch?: {
    batchSize?: number
    compress?: boolean
    gas?: { constant: number; linear: number; quadratic: number }
  }
  cache?: {
    blobKey: string
    ttl: number
    delta?: number
  }
})
```

- **`opts.abi`** — the `AbiFunction` fragment for the callee. Must have exactly one
  input and one output, both dynamic arrays — or, with `paged`, two outputs.
- **`opts.paged`** — marks `abi` as a *paged* lens returning
  `(U[] results, uint256[] skipped)` instead of a bare `U[]`. See
  [Paged lenses](#paged-lenses) below for what that buys and the contract it requires.
- **`opts.batch`** — optional batching config. Omit to send all elements in a single
  upstream `eth_call`. When set, chunks honor `batchSize` and `gas` together — either
  budget can be left unset.
- **`opts.batch.batchSize`** — maximum bytes of the `eth_call` `data` field per chunk.
  Input elements are greedy-packed under this limit and fetched in parallel. Omit to
  skip byte-budget enforcement.
- **`opts.batch.compress`** — FastLZ-compress calldata on the wire. Helps fit more
  elements per chunk under EIP-3860's 49_152-byte initcode cap, at the cost of extra
  pre-request encoding time.
- **`opts.batch.gas`** — polynomial gas-cost model
  `G(N) = constant + linear·N + quadratic·N²` for the lens. Combined with the
  transport's `gasLimit`, the chunker picks the largest per-chunk `N` such that
  `G(N) ≤ gasLimit`. No internal safety factor, and `gasLimit` is a client-side budget only —
  it is never sent as a `gas` field, so the node's own cap is what actually stops execution.
  Overshooting `G(N)` while staying under that cap simply burns more gas than budgeted.
  When the node's cap *is* hit, a drained lens frame is recovered rather than lost: the
  wrapper reports it as `OOG_SENTINEL` revert data, which the chunker classifies as a size
  error and bisects on, down to a single element if need be. Two caveats — the sentinel only
  covers the lens frame, so running out of gas inside the wrapper itself (FLZ decompression,
  or memory expansion while copying returndata) falls back to matching the provider's error
  text; and a lens that burns >98.4% of its frame and *then* reverts with empty data is
  reported as out-of-gas too, which costs a full bisect to singletons (`2N-1` requests)
  before the original error resurfaces. `linear` is typically the dominant term; `quadratic`
  captures memory expansion (`memWords² / 512`); pass `0` for any unused term.
- **`opts.cache`** — optional cache config, honored by `cache(...)` only. If omitted,
  or when used with `deployless(...)`, `batch` is still honored without caching.
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
  abi: positionsAbi,
  batch: {
    batchSize: 1 << 15,
    gas: { constant: 50_000, linear: 30_000, quadratic: 0 },
  },
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

#### Paged lenses

A paged lens may stop before consuming its whole input. It walks its input in index order, in a
single pass, and stops once — having attempted `i = results.length + skipped.length` elements.
`results` covers that prefix minus `skipped`; `skipped` holds ascending indices, relative to this
call's input, that it looked at and declined.

Position is what separates the two outcomes: `[i, N)` was never attempted and is **retried**,
while an index in `skipped` is **permanent**. So an over-packed chunk costs one extra round trip
rather than a bisection, and `batch.gas` only has to be a rough opening guess.

```solidity
function page(T[] input) view returns (U[] results, uint256[] skipped) {
    for (uint256 i = 0; i < input.length; i++) {
        if (i > 0 && gasleft() < reserve + estimate) break;    // tail: retryable
        if (!isValid(input[i])) { skipped.push(i); continue; } // deterministic: permanent
        results.push(one(input[i]));
    }
}
```

The contract your lens must honor:

- **Index order, single pass.** Otherwise "before `i`" no longer implies "declined".
- **Attempt at least one element.** `(results=[], skipped=[])` is a protocol violation, not a
  retryable state — it is what would let a lens stall a range forever, so it throws. A lens that
  cannot afford element 0 must attempt it and let the frame die; the envelope reports that as an
  out-of-gas and the transport marks the element unservable. This is what makes termination
  finite without a retry counter or a gas-escalation ladder.
- **Skips are deterministic.** A skip means invalid input or a reverting element — something
  that declines identically next time. Running out of gas is never a skip; when gas runs out, stop.
- **Values are batching-invariant.** Neither a served value nor a decline may depend on position,
  batch composition, or `gasleft()`. Only the stopping boundary may.

Per-element gas capping is optional and usually overkill — it matters only when an element may be
unbounded, where it turns a dead frame into a stop that preserves the prefix. A capping lens must
leave element 0 uncapped (or retrying a range headed by that element returns `([], [])`), and must
tell a capped out-of-gas from a plain `revert(0, 0)` — both yield empty returndata — by treating
"consumed ≈ the cap **and** empty returndata" as gas-driven. Same heuristic, and same imprecision,
as the envelope's own 63/64 check.

One asymmetry worth knowing: an element reported unservable via out-of-gas is unservable *under
this node's gas cap*, and a provider with a higher cap could serve it. A skip, by contrast, is a
property of the element.

### `call2`

Settled counterpart to viem's `call` for paged deployless reads. Where `call` throws if
any element is unservable, `call2` returns the elements that *were* served alongside the
indices that were not.

```ts
import { decodeAbiParameters, parseAbiItem } from 'viem'
import { call2, policy } from '@morpho-org/viem-dlc/actions'

const pageAbi = parseAbiItem(
  'function page(address[] input) view returns (uint256[] results, uint256[] skipped)'
)

const { data, missing } = await call2(client, {
  factory,
  factoryData,
  to,
  data: encodeFunctionData({ abi: [pageAbi], functionName: 'page', args: [inputs] }),
  stateOverride: [policy({ abi: pageAbi, paged: true })],
})

const [served] = decodeAbiParameters([{ type: 'uint256[]' }], data)
// `served` is the complement of `missing`, in input order.
```

`(U[] results, uint256[] skipped)` is the lens→transport format only. Both `call` and `call2`
reassemble the pages and hand back a dense `U[]`, so paging never reaches the caller — which
also means you decode with `decodeAbiParameters([resultsType], data)`, not
`decodeFunctionResult` against the two-output fragment.

- **`data`** — ABI-encoded `U[]` of every element that was served, in input order, with
  the `missing` indices omitted. Decoding is left to the caller so the transport's
  byte-level hot path stays intact.
- **`missing`** — ascending indices into the input array that could not be served: the
  lens declined them, or a single-element retry ran the frame out of gas. Empty on full
  success, so there is only one shape to handle.

There is one transport path, always settled internally, surfaced two ways. `call` stays
strict — it throws a `DeploylessPartialResultError` carrying the same `data`/`missing`,
which `call2` simply catches. Use `call` when a dense array is a precondition and `call2`
when partial coverage is acceptable; `call`'s dense contract is why these are two actions
rather than one that sometimes returns sparse results.

Only unservable *elements* are settled. Transport errors, protocol violations, and
ordinary lens reverts still throw from both. Results fetched before the failure are
already committed to the cache either way, and `failover` treats a partial result as an
answer rather than a branch failure, so it will not re-run the whole request against the
next provider.

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
