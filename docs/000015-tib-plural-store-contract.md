---
kind: tib
version: 0.0.15
related:
  - 000016-tib-upstash-store.md
---

# TIB — Make `Store` a plural (mget/mset) contract

Outcome: `Store` is defined solely in terms of `mget`/`mset`/`mdelete`, batching is real all the way
down to Upstash, and the singular methods are **gone** — every caller passes an array, length 1
included. With only four production callsites there is no case for keeping a parallel singular
surface; one contract is easier to implement against and impossible to drift.

## Intent

**Difficulty: the interface change is easy; `UpstashStore` is a rewrite.** Six of the nine stores are
a mechanical loop and production callsites are 4 lines, so the contract migration itself is maybe half
a day plus a test sweep. But doing the batching *for real* — which is the entire point — means
reworking `UpstashStore`'s read scheduler and write path (~2–3 days with the tests it has never had),
plus batched dispatch in `ThrottledStore` and per-key fall-through in `HierarchicalStore`. If either
wrapper loops into singular-style upstream calls, the new interface is plural in name only.

Sequencing (see [the `UpstashStore` rewrite](./000016-tib-upstash-store.md) for why): land the
contract + the simple stores + `HierarchicalStore` first, then `UpstashStore`'s hardened list as
its own PR, then the storage-format redesign as a third. Do not block the interface migration on
the storage rewrite.

As of `7031079` that sequencing is partly done from the far end: `UpstashStore` already exposes
native `mget`/`mset`/`mdelete` with singular adapters over them
(`src/stores/upstash.ts:164,268,389,424`). `Store` itself (`src/types.ts:160-165`) is unchanged, so
nothing here has landed at the contract level.

## Context

`Store` (`src/types.ts:145-165`) is singular-only: `get(key)`, `set(key, value)`, `delete(key)`,
`flush()`. That shape fits how *this* library uses it — the cache transport keeps one large NDJSON
blob per `blobKey`, so every production callsite touches exactly one key per request.

Third-party consumers want the store implementations as general-purpose caches (e.g. server-side
Next.js caching), where a single render touches many small keys. Against that workload the singular
contract forces `Promise.all(keys.map(store.get))`, which becomes N HTTP round trips to Upstash
instead of one batched command. The plural form is strictly more expressive; the singular case is an
array of length 1.

## Design

### 1. The contract — `src/types.ts`

```ts
export interface Store {
  mget(keys: readonly string[]): MaybePromise<(Buffer[] | null)[]>;
  mset(entries: readonly (readonly [key: string, value: Buffer[]])[]): MaybePromise<void>;
  mdelete(keys: readonly string[]): MaybePromise<void>;
  flush(): MaybePromise<void>;
}
```

Extend the existing TSDoc with the guarantees the plural form adds, since none are inferable from the
signature:

- `mget` returns an array positionally aligned with `keys` and of the same length; a miss is `null`.
- Atomicity is **per key**, not across the batch. `mset` may land some entries and drop others (it is
  still best-effort and non-throwing); it must never partially persist a single `value`.
- Duplicate keys within one call are permitted; last write wins in `mset`.
- Empty input is valid and must not issue any I/O.

### 2. No singular surface

No base class, no free-function helpers, no `get`/`set`/`delete` anywhere. Stores keep
`implements Store`, and configs stay typed as `Store` (`src/transports/cache/types.ts:20`,
`index.ts:120`).

**Sync-ness must still be preserved.** `MemoryStore`, `LruStore`, and `TtlStore` are synchronous
today and several tests depend on it (`test/transports/cache/eth-call/handler.test.ts:258` reads
without `await`; `test/stores/throttled.test.ts:332` asserts the write returns `undefined`
synchronously). The plural methods on those stores must return the array/`undefined` directly, not a
promise — so `TtlStore` keeps branching on thenable-ness rather than becoming `async`. Hoist the
existing `isThenable` helper out of `src/stores/ttl.ts:14` into `src/utils/` now that more than one
store needs it.

### 3. Per-store work

**Trivial loops** — `MemoryStore`, `LruStore`, `CompressedStore` (already `@deprecated`; consider
deleting rather than porting), `VercelStore` (`Promise.all` over the existing `_get`/`_set`; the Blob
SDK has no batch API), `NodeFsStore` (`Promise.all` over `readFile`/`_set`, one `inFlight.track` for
the whole batch).

**`TtlStore`** — stamp each value on `mset`, `unwrap` each element on `mget`. Keep the sync/async
passthrough by branching once on the array result instead of per element.

**`HierarchicalStore`** (`src/stores/hierarchical.ts`) — the one real refactor of the decorators.
`mget` walks tiers with a shrinking residual key list: query tier `i` with the keys still missing,
record hits into the positionally-aligned result, and continue with the remainder. When
`populateOnMiss`, backfill tiers `0..i-1` with a single `mset` of just the keys that hit at tier `i`
(fire-and-forget, as today). `mset`/`mdelete`/`flush` stay `Promise.all` fan-outs with no `catch` —
`test/stores/hierarchical.test.ts:68` deliberately asserts that a throwing child rejects.

**`ThrottledStore`** (`src/stores/throttled.ts`) — `mget` passes straight through (unthrottled), so
read batching reaches Upstash for free. Writes need more than a loop: `createOptimizedUpstashStore`
puts this store directly in front of `UpstashStore`, so if `mset` explodes into per-key jobs that each
call upstream `mset([one])`, the migration is plural in name only and the write path still costs one
HTTP request per key.

Keep everything that is per-key today — coalescing, versioning, staleness gating, one active job per
key, flush boundaries. Change only the **dispatch**: after the rate limiter admits keys, drain the
admitted set on a microtask (or a small bounded window, well under `maxStalenessMs`) and issue one
`upstream.mset(admittedSets)` / `upstream.mdelete(admittedDeletes)`. Error routing to `onWriteError`
and flush-boundary resolution then fan back out per key from the batch result.

One trap: `maxConcurrent` must count *upstream batch calls*, while rate-limit tokens keep counting
logical key writes. Conflating them silently multiplies the permitted write rate.

**`UpstashStore`** (`src/stores/upstash.ts`) — where the payoff is, and the only genuinely tricky
code. What follows is the **stage-1 hardening** of the existing list layout. A separate
exploration (Sol and Fable, independently) concluded the list layout should eventually be replaced
outright; see [the `UpstashStore` rewrite](./000016-tib-upstash-store.md) for the converged design
and the staging rationale. Stage 1 deliberately keeps the current format so the plural contract
can land without waiting on a storage migration.

*`mget`* — generalize `SMART_READ_SCRIPT` to take `KEYS[1..N]`, returning per key a status plus
`{listLen, element}`, keeping the `LLEN` + first `LINDEX` in one atomic invocation as today. This
alone delivers the headline win: N small keys resolve in one round trip instead of N.

Budget the request against the exact serialized `EVALSHA_RO` body via
`measureUtf8Bytes(JSON.stringify([...command]))`, and pack as many keys per round as fit. Keep
`maxRequestBytes` as the **value** budget its TSDoc already documents (caller leaves ~1kb headroom)
rather than redefining it as a strict body bound — reserve the envelope out of the *round* by packing
fewer keys, never out of the element, and guarantee each round admits at least one element whole.
Redefining it would orphan every already-written max-size element, since today's writer sizes shards
to exactly `maxRequestBytes - WriteId.LENGTH`.

Load the script with `SCRIPT LOAD`, cache the SHA, and call `EVALSHA_RO`. Avoid
`createScript().exec()` — its hidden `NOSCRIPT` fallback issues an unbudgeted `EVAL`.

Retry state becomes **per unique key**, not per batch. The scheduler works on unique keys and fans
each terminal result back to every input position that named it (which is how duplicate keys are
handled). `LLEN == 0` is a terminal miss costing no retry; a missing shard, a cross-shard `WriteId`
mismatch, or a transport failure re-attempts *only that key*. Completed keys are never re-read
because a sibling failed. Rename `maxRetries` to `maxAttempts` — the current loop performs two
attempts, not two retries.

Keys with `listLen > 1` keep the existing sequential `LINDEX` walk. **Deliberately not in stage 1:**
the byte-budgeted multi-round paging scheduler with deferred-work rotation, and any form of
intra-element slicing. Both are significant complexity that the stage-3 layout deletes outright —
`GETRANGE` on an immutable generation string gives reader-sized paging as a first-class command, with
no `sha1hex` page proofs needed. See [the `UpstashStore` rewrite](./000016-tib-upstash-store.md).

*`mset`* — one atomic Lua script per key, pipelined. Not `multi()` (cannot nest in a pipeline, and
falls back to one request per key), and not one giant script spanning all keys (needless cross-key
failure domain). Exec with `pipeline.exec({ keepErrors: true })` so one bad command reports in its own
slot instead of rejecting the batch.

Use the tmp-key + `RENAME` path even for single-shard values — it costs Redis ops but no extra HTTP
request, and it preserves the old value if the replacement fails to materialize:

```lua
-- KEYS[1] = destination, KEYS[2] = tmp key
-- ARGV[1] = packed shard, ARGV[2] = TTL seconds or 0 for persistent
redis.call("UNLINK", KEYS[2])
redis.call("RPUSH", KEYS[2], ARGV[1])
redis.call("RENAME", KEYS[2], KEYS[1])
local ttl = tonumber(ARGV[2])
if ttl > 0 then redis.call("EXPIRE", KEYS[1], ttl) else redis.call("PERSIST", KEYS[1]) end
return 1
```

Multi-shard values keep their three stages but each becomes a script: init, **idempotent append**
(compare `LLEN` against the expected index, and treat `len == expected + 1` with a matching `LINDEX`
as an already-applied retry — see the latent bug below), and a **validated finalize** that re-checks
the list length and every shard's `WriteId` prefix before `RENAME`. Refresh the tmp key's safety TTL
on each append. Shard sizing can no longer be a global `maxRequestBytes - WriteId.LENGTH`; size it
per key against the exact serialized command, then greedily pack command descriptors into pipelines
under both the request and response bounds.

*`mdelete`* — one `UNLINK` with as many keys as fit the serialized body, chunked greedily; atomic,
idempotent, duplicate-tolerant, tiny response. A pipeline of single-key `UNLINK`s buys per-key error
attribution that `mdelete` has no way to report, so it is not worth the bytes.

*Framing* — `WriteId` packing, the `<16 hex>|<base64>` element format, and the legacy all-zero unpack
path are all unchanged. On read, validate base64 strictly before decoding: `Buffer.from(s, "base64")`
silently accepts malformed input.

## Scope & files

### Callsites

Four lines, both inside `coalesce` leaders:

- `src/transports/cache/eth-call/handler.ts:102,109`
- `src/transports/cache/eth-get-logs/handler.ts:61,91`

`store.get(blobKey)` → `store.mget([blobKey]).then(r => r[0])` (or `(await store.mget([k]))[0]`; note
`:61` keeps the read unawaited in the `preflight` tuple), and `void store.set(k, v)` →
`void store.mset([[k, v]])`.

## Verification

### Tests and docs

Dropping the singular surface moves the mechanical churn here: every `store.get/set/delete` in the
suites becomes plural. It is a find-and-replace with one wrinkle — reads now index the result
(`store.mget([k])[0]` on sync stores, `(await store.mget([k]))[0]` on async ones). Files to sweep:

- `test/stores/{memory,lru,ttl,hierarchical,throttled,compressed}.test.ts` — `throttled.test.ts` (637
  lines) is the largest, and its `vi.spyOn(underlying, "set"/"delete")` assertions become spies on
  `mset`/`mdelete` with array arguments.
- `test/transports/cache/eth-call/handler.test.ts` (incl. its `populateStore` at `:159-177`),
  `test/transports/cache/eth-get-logs/_helpers.ts:67-77`, and
  `test/transports/cache/eth-get-logs/handler.test.ts`.
- The hand-written `Store` fakes — `hierarchical.test.ts:56-65,78-86`, `ttl.test.ts:9-30,205-214` —
  must be reshaped to the plural interface. The `rechunkingStore()` fake (returns one buffer per byte)
  is the one that exercises `TtlStore.unwrap`'s fallback path; keep that behavior.

New coverage to add:

- `mget`/`mset` in `test/stores/hierarchical.test.ts`: partial hits split across tiers, result
  positional alignment, backfill covering only the hit subset, duplicate keys, empty input.
- `mget`/`mset` in the `lru`/`memory`/`ttl` suites (alignment, mixed hit/miss, per-entry TTL expiry),
  plus an explicit assertion that sync stores return synchronously.
- `test/stores/throttled.test.ts` gains batched-dispatch cases: N coalesced keys reach upstream as one
  `mset`, a per-key error from the batch still routes to `onWriteError` for that key alone, and
  `maxConcurrent` counts batch calls while tokens count keys.
- `test/stores/upstash.test.ts` is new ground — there are **no** tests for `UpstashStore`,
  `VercelStore`, or `NodeFsStore` today. Build a fake requester (pipeline + `evalsha_ro` stubs) and
  cover: many small hits in one round; mixed hits/misses/duplicates with exact positional alignment;
  empty input and empty value; request/response size boundaries; a near-cap element; rewrite-between-
  rounds motivating retry for only that key; `WriteId` mismatch across shards; legacy single- and
  multi-shard reads; strict base64 rejection; partial pipeline errors under `keepErrors`; whole-
  pipeline transport failure staying non-throwing; `NOSCRIPT` load-and-retry; and the two latent bugs
  above (replayed append must not duplicate, tmp-key expiry must not publish a truncated list).

  A handful of semantics cannot be established against a mock — Lua `false`/JSON-null conversion,
  `redis.sha1hex`, `EVALSHA_RO`, `RENAME`'s TTL behavior, pipeline ordering, `keepErrors`. Those want
  a small opt-in integration suite against a real Upstash instance, skipped when credentials are
  absent.

README `Store` section (`README.md:270-291`) documents the plural contract. While there: that table is
stale — it lists a `DebouncedStore` that no longer exists (it is `ThrottledStore`) and omits
`NodeFsStore`/`VercelStore`.

### Running the migration

1. `pnpm build` (or `tsc --noEmit`) — the interface change is the compiler's job; every unported
   store surfaces here.
2. `pnpm test` — the swept suites must pass with **behavior assertions unchanged**; only the call
   shape moves. A test whose expected *values* change is a signal the port altered semantics.
3. New store tests above, especially `HierarchicalStore` alignment/backfill.
4. Manual Upstash check against a real instance: write ~50 small keys via `mset`, read them back with
   one `mget`, and confirm from the Upstash console/request count that it is a handful of round trips
   rather than ~50. Round-trip a >`maxRequestBytes` value in the same batch to exercise the shard
   fallback and the response budget.

## Derivation

### Latent bugs in the current `UpstashStore` (found during the deep dive)

Worth fixing as part of this work, since the migration rewrites these paths anyway:

1. **Duplicated shards on transport retry.** `_set`'s multi-shard loop issues a bare
   `redis.rpush(opKey, ...)` (`upstash.ts:729`). The Upstash client retries failed requests
   automatically, so an executed-but-response-lost `RPUSH` appends the same shard twice. The
   idempotent-append script above fixes it.
2. **Truncated publish on freeze.** The tmp key gets a 60s safety TTL (`upstash.ts:724`) while the
   append loop is unbounded. A serverless freeze past 60s can expire the tmp key mid-write, after
   which `RENAME` publishes a short list. The validated finalize fixes it.
3. **`maxRequestBytes` is not actually a request bound.** It budgets only the value, ignoring the
   command name, key, script SHA, and JSON envelope — the constructor accepts anything
   `> WriteId.LENGTH` (17), which cannot carry a single `EVALSHA`. Per the deferral note above, the
   fix is *not* to redefine it as a body bound (that orphans existing max-size elements). Raise the
   constructor floor to a computed minimum, keep the documented value-budget semantics, and account
   for the envelope when deciding how many keys share a round.
4. **Permissive base64 decode** on the read path, as above.
