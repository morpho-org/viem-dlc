---
kind: tib
version: 0.0.16
related:
  - 000016-tib-upstash-store.md
---

# TIB — Make `Store` a plural (mget/mset) contract

Outcome: `Store` is defined solely in terms of `mget`/`mset`/`mdelete`, reads batch all the way down
to Upstash, and the singular methods are **gone** — every caller passes an array, length 1 included.
With only four production callsites there is no case for keeping a parallel singular surface; one
contract is easier to implement against and impossible to drift. Writes stay per-key through the
throttle, deliberately; the Derivation says why.

## Intent

**The expensive half is already done.** This was first written when `UpstashStore` was a rewrite and
the open question was which of three PRs went first. `7031079` answered it from the far end: that
store is plural-native, does its own byte-budgeted multi-round packing, and arrived with 695 lines
of unit tests plus an env-gated integration suite. What is left is the contract itself — four stores
that are a mechanical loop, four production callsites, one real refactor (`HierarchicalStore.mget`),
and one store left deliberately unimproved (`ThrottledStore`). Half a day plus a test sweep.

`Store` (`src/types.ts:160-165`) is still singular, so nothing here has landed at the contract
level. `UpstashStore` bridges the gap with three `@deprecated` one-liners over its plural methods
(`src/stores/upstash.ts:423-436`). Deleting those is the concrete deletion this migration unlocks.

The invariant to protect through the port: **only the call shape moves.** No store's observable
behavior — sync-ness, error routing, flush semantics, positional results — changes because of it. A
test whose expected *values* change is a signal the port altered semantics.

## Context

`Store` (`src/types.ts:145-165`) is singular-only: `get(key)`, `set(key, value)`, `delete(key)`,
`flush()`. That shape fits how *this* library uses it — the cache transport keeps one large NDJSON
blob per `blobKey`, so every production callsite touches exactly one key per request.

Third-party consumers want the store implementations as general-purpose caches (e.g. server-side
Next.js caching), where a single render touches many small keys. Against that workload the singular
contract forces `Promise.all(keys.map(store.get))`, which becomes N HTTP round trips to Upstash
instead of one batched command. The plural form is strictly more expressive; the singular case is an
array of length 1.

`UpstashStore` already delivers that batching to anyone holding one directly. It does not reach
`createOptimizedUpstashStore` users, because `HierarchicalStore` and `ThrottledStore` sit in front
of it and dispatch per key.

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

The `m` prefix follows Redis (`MGET`/`MSET`) rather than overloading `get` with an array, so a
callsite cannot silently mean the other shape. Extend the existing TSDoc (`src/types.ts:145-159`)
with the guarantees the plural form adds, since none are inferable from the signature:

- `mget` returns an array positionally aligned with `keys` and of the same length; a miss is `null`.
- Atomicity is **per key**, not across the batch. `mset` may land some entries and drop others (it
  is still best-effort and non-throwing); it must never partially persist a single `value`.
- Duplicate keys within one call are permitted; in `mset`, last write wins. `mdelete` stays
  idempotent per key, as `delete` is today.
- Empty input is valid and must not issue any I/O.
- **A batch is not a transport unit.** A store may satisfy one call in several round trips, and a
  key that fails every attempt reports `null` — indistinguishable from a miss, by design.

The last two bullets describe what `UpstashStore` already does, so the contract is documenting
shipped behavior rather than imposing new work: `mget` packs keys into as many pipelines as the byte
budget requires (`upstash.ts:194,233`) and returns `null` for keys that exhaust `READ_ATTEMPTS`
(`:167-181`); `_mset` logs per-key write failures and drops values over the record cap (`:287,307`).

### 2. No singular surface

No base class, no free-function helpers, no `get`/`set`/`delete` anywhere. Stores keep `implements
Store`, and configs stay typed as `Store` (`src/transports/cache/types.ts:21`,
`src/transports/cache/index.ts:120`). Delete `UpstashStore`'s three deprecated adapters
(`upstash.ts:423-436`) and the test that pins them (`test/stores/upstash.test.ts:593-599`).

**Sync-ness must still be preserved.** `MemoryStore`, `LruStore`, and `TtlStore` are synchronous
today and several tests depend on it (`test/transports/cache/eth-call/handler.test.ts:260` reads
without `await`; `test/stores/throttled.test.ts:332-334` asserts the write returns `undefined`
synchronously). The plural methods on those stores must return the array/`undefined` directly, not a
promise — so `TtlStore` keeps branching on thenable-ness rather than becoming `async`. Hoist the
existing `isThenable` helper out of `src/stores/ttl.ts:14` into `src/utils/` now that more than one
store needs it.

### 3. Per-store work

**Trivial loops** — `MemoryStore`, `LruStore`, `VercelStore` (`Promise.all` over the existing
`_get`/`_set`; the Blob SDK has no batch API), `NodeFsStore` (`Promise.all` over `readFile`/`_set`,
one `inFlight.track` for the whole batch).

**`CompressedStore`** — delete it rather than porting. It is already `@deprecated`
(`src/stores/compressed.ts:16-20`) on the grounds that compression moved outside the `Store` stack,
so a port would be work spent on a class whose own docblock says not to use it. Drop the export
(`src/stores/index.ts:2`) and `test/stores/compressed.test.ts` with it.

**`TtlStore`** — stamp each value on `mset`, `unwrap` each element on `mget`. Keep the sync/async
passthrough by branching once on the array result instead of per element. `unwrap` itself
(`src/stores/ttl.ts:83-113`) is per-value and unchanged.

**`HierarchicalStore`** (`src/stores/hierarchical.ts`) — the one real refactor of the decorators.
`mget` walks tiers with a shrinking residual key list: query tier `i` with the keys still missing,
record hits into the positionally-aligned result, and continue with the remainder. When
`populateOnMiss`, backfill with **one `mset` per destination tier**, carrying the union of hits from
every tier below it — not one per (destination, source) pair, which is what a naive read of
"backfill tiers `0..i-1` with the keys that hit at tier `i`" produces. Keep it fire-and-forget, as
today (`hierarchical.ts:26`). `mset`/`mdelete`/`flush` stay `Promise.all` fan-outs with no `catch` —
`test/stores/hierarchical.test.ts:55-70` deliberately asserts that a throwing child rejects.

**`ThrottledStore`** (`src/stores/throttled.ts`) — the minimal port, and it is deliberately
unsatisfying. `mget` passes straight through, unthrottled, so read batching reaches `UpstashStore`
intact. `mset`/`mdelete` keep every piece of today's machinery — per-key coalescing, versioning,
staleness gating, one active job per key, flush boundaries, `onWriteError` routing — and each
admitted key still dispatches its own single-entry call upstream. The 10s timeout stays per key
because the unit of work stays per key.

So the write path through `createOptimizedUpstashStore` costs exactly what it costs today: one
upstream call per key. **Do not try to fix that here.** Batching the dispatch requires deciding what
a token buys — a key or a call — and neither answer is available from inside this store. See the
Derivation.

**`UpstashStore`** (`src/stores/upstash.ts`) — **nothing to do.** `mget`/`mset`/`mdelete`/`flush`
are already the native surface (`:164,268,389,411`); see [the `UpstashStore`
rewrite](./000016-tib-upstash-store.md) for the design. The only edits are deleting the deprecated
singular adapters per §2.

## Scope & files

Four lines across two handlers, both inside `coalesce` leaders:

- `src/transports/cache/eth-call/handler.ts:111,120`
- `src/transports/cache/eth-get-logs/handler.ts:64,94`

`store.get(blobKey)` → `store.mget([blobKey]).then(r => r[0])` (or `(await store.mget([k]))[0]`;
note `eth-get-logs:64` keeps the read unawaited in the `preflight` tuple), and
`void store.set(k, v)` → `void store.mset([[k, v]])`.

## Verification

### Tests and docs

Dropping the singular surface moves the mechanical churn here: every `store.get/set/delete` in the
suites becomes plural. It is a find-and-replace with one wrinkle — reads now index the result
(`store.mget([k])[0]` on sync stores, `(await store.mget([k]))[0]` on async ones). Files to sweep:

- `test/stores/{memory,lru,ttl,hierarchical,throttled}.test.ts` — `throttled.test.ts` (637 lines) is
  the largest, and its 26 `vi.spyOn(underlying, "set"/"delete")` assertions become spies on
  `mset`/`mdelete` with array arguments.
- `test/transports/cache/eth-call/handler.test.ts` (incl. its `populateStore` at `:162-181`),
  `test/transports/cache/eth-get-logs/_helpers.ts:67-77`, and
  `test/transports/cache/eth-get-logs/handler.test.ts`.
- The hand-written `Store` fakes — `hierarchical.test.ts:56-65,78-86`, `ttl.test.ts:9-30,205-214` —
  must be reshaped to the plural interface. The `rechunkingStore()` fake (`ttl.test.ts:20`, returns
  one buffer per byte) is the one that exercises `TtlStore.unwrap`'s fallback path; keep that
  behavior.
- `test/stores/upstash.test.ts` needs **no port** — it already drives the plural methods in 61
  places. Delete only `"exposes get/set/delete over the plural methods"` (`:593-599`). That the
  largest store suite is untouched is the evidence this migration is small.

New coverage to add:

- `mget`/`mset` in `test/stores/hierarchical.test.ts`: partial hits split across tiers, result
  positional alignment, backfill covering only the hit subset and issuing one `mset` per destination
  tier, duplicate keys, empty input.
- `mget`/`mset` in the `lru`/`memory`/`ttl` suites (alignment, mixed hit/miss, per-entry TTL
  expiry), plus an explicit assertion that sync stores return synchronously.
- `test/stores/throttled.test.ts` gains shape cases only: a multi-key `mget` reaches upstream as one
  unthrottled call, and a multi-entry `mset` still coalesces and dispatches per key. Pin the
  per-key dispatch explicitly — it is a decision, not an oversight, and a later reader will
  otherwise "fix" it.

README `Store` section (`README.md:312-334`) documents the plural contract — the interface block at
`:316-323` and the table below it. While there: that table is stale — it lists a `DebouncedStore`
that no longer exists (it is `ThrottledStore`) and omits `NodeFsStore`/`VercelStore`. Drop the
`CompressedStore` row along with the class.

### Running the migration

1. `pnpm build` (or `tsc --noEmit`) — the interface change is the compiler's job; every unported
   store surfaces here.
2. `pnpm test` — the swept suites must pass with **behavior assertions unchanged**; only the call
   shape moves.
3. New store tests above, especially `HierarchicalStore` alignment/backfill.
4. Manual check against a real Upstash instance: seed ~50 small keys, then read them back with one
   `mget` through `createOptimizedUpstashStore` and confirm from the request count that it is a
   handful of round trips rather than ~50. This is the end-to-end proof that `HierarchicalStore` and
   `ThrottledStore` stopped dispatching reads per key. The write path is expected to still show ~50
   calls; that is the deferred work, not a regression.

## Derivation

### Declined: batching the throttled write path

Reads batch end to end because `ThrottledStore` never touches them. Writes do not, and no
arrangement of the current pieces makes them.

The obvious move — buffer the keys the limiter admits, flush them as one `mset` on a microtask —
cannot produce a batch larger than `maxWritesBurst`. `drainQueue` (`src/utils/with-rate-limit.ts`)
admits greedily while tokens remain, so a resting bucket releases exactly its burst in one
synchronous pass and those continuations share a microtask window. Every key after that waits on
`scheduleDrain`'s `setTimeout`, arriving in its own turn of the event loop, hence its own batch.
Under the shipped constants — `maxWritesBurst: 3`, `maxWritesPerSecond: 300`, `maxConcurrent:
Infinity` (`upstash.ts:442-445,455`), so admission is purely token-bound — fifty keys become one
batch of three and forty-seven batches of one. Replacing the microtask with a bounded time window
does not help: admission is throttled at 300/sec however long dispatch waits.

Making the **batch** the unit of admission does work mechanically: buffer first, submit the
assembled batch as one job consuming one token. But it only relocates the guess. A token would then
buy an upstream call, and nothing in `ThrottledStore` knows what a call costs — `UpstashStore`
spends a number of commands per write that depends on shard count, so a batch of fifty small values
is nowhere near fifty times the cost of a batch of one. Picking "one token per call" is the same
unfounded estimate as "one token per key", moved.

That is the actual defect, and the singular contract merely hid it: **throttling is meaningless
without knowing what the underlying store does with a batch.** The store knows; the throttle has no
way to ask. Fixing it is a `Store` contract change, tracked as
[APPS-1302](https://linear.app/morpho-labs/issue/APPS-1302). This migration's only obligation is to
not entrench the guess — no new limiter knobs, no per-batch tuning option, and no dispatch scheme
that would have to be unwound once admission is defined properly.

### The sequencing resolved in the opposite order

The original plan was three PRs: contract + simple stores + `HierarchicalStore` first, then a
stage-1 hardening of `UpstashStore`'s existing list layout, then a storage-format redesign. What
happened instead is that the storage redesign landed first and whole (`7031079`), which deleted the
stage-1 plan outright. Everything it specified — generalizing `SMART_READ_SCRIPT` to `KEYS[1..N]`,
`EVALSHA_RO` reads, an idempotent-append script, a validated finalize, per-key retry state, renaming
`maxRetries` — is either shipped in a different form or moot. Reads use no script at all now; the
byte-budgeted multi-round scheduler that stage 1 deliberately excluded is exactly what shipped.

### Latent bugs in the pre-`7031079` `UpstashStore`

Four found during the deep dive, all resolved by the rewrite: duplicated shards when the client
auto-retried a bare `rpush` (fixed by staging behind a per-writer key and a verifying publish), a
once-set 60s staging TTL against an unbounded append loop (fixed by refreshing it on every push), a
permissive base64 decode on the read path, and `maxRequestBytes` not actually bounding the request.

Only the last is worth remembering here, because it resolved *against* this document's own
recommendation. The advice was explicitly **not** to redefine `maxRequestBytes` as a body bound,
since that would orphan already-written max-size elements. The rewrite redefined it anyway and added
`shardBytes` as the element bound (`upstash.ts:38-50`) — sound because the storage format changed
wholesale, so there was nothing left to orphan. The reasoning held only while the layout was being
preserved.
