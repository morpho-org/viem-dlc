---
kind: tib
version: 0.0.16
landed:
  - 7031079
related:
  - 000016-tib-plural-store-contract.md
---

# TIB — `UpstashStore`: single-format LIST rewrite

Every value is a Redis LIST of ≤ 64 KiB shards; small writes are one pipelineable `EVALSHA` per
key; large writes stage at a per-writer uuid key and publish through a verifying script. The
>75 MB raw case — which base64-expands past the 100 MB Free/PAYG record cap — is out of scope by
owner decision.

## Intent

**Recovery invariant (the property to protect — not write success rate):** *no key lacking a TTL is
ever unreachable.* Live values sit at the caller's key, reachable by name, with the caller's TTL or
deliberately persistent. Staging keys (`tmp:<key>:<uuid>`) are unreachable once their writer dies,
but **always** carry a 60 s TTL refreshed on every push, so they self-heal in every configuration.
Write failures are acceptable (best-effort cache); an untracked immortal key is not.

## Context

The current implementation (`src/stores/upstash.ts`) stores values as a list and publishes
multi-request writes via a tmp key + `RENAME` (`_set`, lines 154-191). Two live bugs force the
rewrite:

1. **Replay duplication** — `upstash.ts:176` pushes shards with a bare `this.redis.rpush(opKey, …)`
   and never checks the returned length. The Upstash client auto-retries on network error, so a
   lost response replays the push and the tmp list gains a duplicate shard *with the same
   writeId* — the read-side wid check (`upstash.ts:119-127`) cannot see it, and `RENAME` publishes
   a corrupt value.
2. **Tmp expiry mid-write** — `upstash.ts:171` sets a 60 s safety TTL on the tmp key once and never
   refreshes it. A write slower than 60 s (or a serverless freeze) lets the tmp key expire
   mid-write; the next `rpush` silently recreates it holding only the later shards, and `RENAME`
   (`upstash.ts:181`) publishes a truncated list.

## Design

### Storage format

One format, no exceptions: `key` holds a LIST of shards, each complete stored element ≤
`S = 64 KiB` (option `shardBytes`; sizing rationale in the addendum). Sharding reuses
`shardString` (`src/utils/strings.ts:27`; base64 is 1 byte/char).

```
shard 0   :  wid|0|k|<base64 slice>   wid = 16 hex (randomBytes), i = element index, k = intended
shard i>0 :  wid|i|<base64 slice>     count; i and k are decimal, both zero-padded to D digits
```

Every element carries its own index. That is the **read-time** integrity mechanism (publication
proves only shape — see read path): within one wid, index ⇒ bytes is a function, since the writer
emits exactly one `si` per `i`, every replay instance of it is byte-identical, and a restart takes a
fresh wid. So wid-uniformity plus `i` contiguous over `0..k-1` pins every position to its unique
original shard. A structural proof at `D + 1` bytes/element, replacing the deleted digest's
probabilistic one at O(bytes) of read-time hashing.

`S` includes the prefix. Let `D = decimalDigits(k)`, computed monotonically (digits only grow:
recompute `k` at the smaller capacity until `D` stops increasing, which terminates). Continuation
payload capacity is `S - 18 - D`; shard-0 payload capacity is `S - 19 - 2D`. The small
element cap — not the request budget — keeps batched reads bounded a priori (see read path). Drop
values when the sum of their complete stored element sizes exceeds the 100 MB record cap.

### TTL: absolute deadlines, computed once

Per `mset` entry with a TTL: `deadlineMs = Date.now() + ttlSec * 1000`, computed **once, before any
request is built**, and passed as `ARGV` to the scripts, which apply it via `PEXPIREAT`. A client
auto-retry replaying any request re-asserts the same absolute deadline, so replays can never extend
an entry's life. No TTL ⇒ persistent (`PERSIST`, or simply no expiry on a freshly created key).
Staging's 60 s TTL stays *relative* and refreshed per push — it is a writer-liveness bound, not an
entry lifetime.

### Scripts

Two scripts, both `#!lua flags=allow-key-locking`, every touched key declared in `KEYS`, both O(1)
in key count. SHAs are **build-time constants** in the source; use raw `evalsha` / `scriptLoad`
only. `createScript()` is **banned**: its own doc comment confirms it silently falls back to `EVAL`
(shipping the script source per command — unbudgeted inside a packed pipeline) and its `sha1` is
deprecated and set asynchronously.

**`WRITE_DIRECT`** — a whole small value, atomically, in one pipelineable command:

```lua
#!lua flags=allow-key-locking
-- KEYS[1]=key  ARGV[1]=deadlineMs ('' = persist)  ARGV[2..]=shards

local deadline = nil
if ARGV[1] ~= '' then
  deadline = tonumber(ARGV[1])
  if not deadline or deadline ~= math.floor(deadline) then
    return -3
  end

  local now = redis.call('TIME')
  local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

  if deadline <= now_ms then
    return 0
  end
end

redis.call('UNLINK', KEYS[1])
redis.call('RPUSH', KEYS[1], unpack(ARGV, 2))

if deadline then
  redis.call('PEXPIREAT', KEYS[1], deadline)
end

return 1
```

Replay-idempotent (re-executes to the identical state and absolute deadline). A value already dead
on arrival returns `0` without touching the old value; `-3` is an internal-argument bug. `unpack`
argument count is bounded by request limit / `S`, far under Lua's ~8000 limit.

**`PUBLISH`** — verify staging, stamp the deadline on *staging*, then rename:

```lua
#!lua flags=allow-key-locking
-- KEYS[1]=tmp, KEYS[2]=live
-- ARGV[1]=exact shard 0, ARGV[2]=k, ARGV[3]=deadlineMs or ''

local expected_head = ARGV[1]
local k = tonumber(ARGV[2])

if not k or k < 1 or k ~= math.floor(k) then
  return -3
end

local deadline = nil
if ARGV[3] ~= '' then
  deadline = tonumber(ARGV[3])
  if not deadline or deadline ~= math.floor(deadline) then
    return -3
  end

  local now = redis.call('TIME')
  local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

  if deadline <= now_ms then
    redis.call('UNLINK', KEYS[1])
    return 0
  end
end

local staging_head = redis.call('LINDEX', KEYS[1], 0)
if staging_head == expected_head
    and redis.call('LLEN', KEYS[1]) == k then
  if deadline then
    redis.call('PEXPIREAT', KEYS[1], deadline)
  else
    redis.call('PERSIST', KEYS[1])
  end

  redis.call('RENAME', KEYS[1], KEYS[2])
  return 1
end

local live_head = redis.call('LINDEX', KEYS[2], 0)
if live_head == expected_head
    and redis.call('LLEN', KEYS[2]) == k then
  return 2
end

if live_head then
  return -2
end

return -1
```

The exact shard-0 equality plus `LLEN == k` is the publication **shape** proof — it rejects the
single-fault cases (duplication without expiry exceeds `k`; expiry/recreation from a later batch
cannot reproduce shard 0) but not every combined fault; content is proved read-side. The deadline
is checked against server time before mutation, then stamped on staging so `RENAME` carries it to
live.

Return contract and client action:

- `1` published now; `2` exact publication already live: success.
- `0` deadline already passed; `-1` invalid/absent staging with no live value; `-2` a different live
  value exists; `-3` invalid internal arguments: terminal failure (`-3` also logs a bug).
- Before any `PUBLISH`, a staging transport failure may restart once with a fresh uuid/wid.
  `NOSCRIPT` proves non-execution, so load and reissue only that failed slot. An ambiguous transport
  failure during `PUBLISH` may retry the same script with the same staging key and expected head.
  Once `PUBLISH` has been issued, never restage the whole write; only `1` and `2` are success.
- A **thrown** script error (as opposed to a return code) is a terminal failure, swallowed — `mset`
  must not throw (`src/types.ts:146-147`). Whether Upstash's engine can lazily expire staging
  between `LINDEX` and `RENAME` is unprobed; this rule makes it moot.

**NOSCRIPT recovery, per slot:** pipelines run `exec({ keepErrors: true })`; on `NOSCRIPT` slots,
`scriptLoad` once, then reissue **only the failed subset**. `evalsha`/`evalshaRo`/`scriptLoad` all
exist on the Pipeline type (verified: `@upstash/redis` `error-8y4qG0W2.d.ts:3057-3063,3423`).

### Write path — `mset(entries)`

Coalesce duplicate keys (last wins), encode, and shard under the complete-element bound `S`. Route
by exact UTF-8 byte measurement of the serialized `EVALSHA` request body; there is no request
reserve constant:

- **Direct (one request):** one `WRITE_DIRECT` evalsha carrying all shards. Greedy-pack many keys'
  calls into `pipeline()` requests whose exact serialized bodies are ≤ `maxRequestBytes`; N small
  writes are **one HTTP request**.
- A request that cannot be split under `maxRequestBytes` (one shard plus its key and framing, or
  the `SCRIPT LOAD` body) is not special-cased: it is sent anyway and the provider's rejection
  surfaces through the same per-key error logging as any other failed command. The option's doc
  says to keep `maxRequestBytes` comfortably above `S`; no constructor constraint pretends to
  guarantee the fit.
- **Staged (direct command exceeds `maxRequestBytes`):** per-writer staging at
  `tmp:<key>:<uuid>` (`randomUUID()`), so
  concurrent writers never share a staging list.
  1. `multi()`: `RPUSH tmp shard0..` · `EXPIRE tmp 60`, exactly request-sized.
  2. Repeat for subsequent batches: `multi()`: `RPUSH tmp …` · `EXPIRE tmp 60`.
  3. `PUBLISH` evalsha and apply its return contract above.
  A pre-`PUBLISH` restart re-encodes the value (fresh wid) as well as taking a fresh uuid.

No push-time length check: exact shard-0 equality plus `LLEN == k` subsumes what one would catch,
and the residue it would not — delayed replays recombining to exactly `k` — is closed read-side by
index contiguity.

Every successful read is one complete writer's value with its original absolute deadline. The last
Redis execution wins; invocation and completion order are not guaranteed.

### Read path — `mget(keys)`

Request and response limits are treated differently: request bodies are **measured exactly**
(overshoot is a hard reject of the whole pipeline); responses are **provisioned** — each read
request asks for at most `elementsPerResponse = floor(maxResponseBytes / S)` list elements. JSON
framing and provider envelope are deliberately not modeled: they are deterministic and small
(~0.02% at 64 KiB shards), so `maxResponseBytes` is documented as "the provider limit minus
headroom" rather than derived from it.

- **Stage 1:** dedupe; pipeline one `LRANGE key 0 0` per key, at most `elementsPerResponse` per
  request. Empty ⇒ miss. Parse the shard-0 header; `k == 1` completes the common case in this
  single round trip.
  No `LLEN` cross-check: every *published* list satisfies `LLEN == k` by construction —
  `WRITE_DIRECT` writes exactly k elements in one atomic command, and `PUBLISH` verifies
  `LLEN == k` before `RENAME`. Nothing else mutates a live list's elements (both write routes are
  atomic full replacements, `mdelete` unlinks, lists have no per-element expiry); a delayed
  post-publish push recreates only the TTL'd uuid key. Publication is *not* by itself proof that
  the elements are the right ones — delayed batch replays can recombine to exactly `k` behind a
  genuine shard 0 — so the read-side integrity signal is the header's `k` plus the stage-2 wid and
  index-contiguity checks below.
- **Stage 2** (multi-shard keys only): paged `LRANGE key i j` with `elementsPerResponse` elements
  per page, pages of several keys packed under the same element cap, sequential; every shard's wid
  must match shard 0's **and its index must continue the contiguous `0..k-1` run** (`D`, the index
  field width, comes from shard 0's `k`), and each page
  must have the expected shape — an index gap or repeat (the signature of delayed batch replays
  recombining after a staging expiry), a wid mismatch, or a short/empty page (e.g. a mid-read
  replacement by a smaller value) ⇒ the list was atomically replaced ⇒ retryable miss with bounded
  retries, then null (semantics as the current `get`, `upstash.ts:137-152`).
- Reassemble and decode base64.
- Fan results back to input positions (duplicates allowed, misses null).

Replaces `SMART_READ_SCRIPT` (`upstash.ts:49-56`) — stage 1 serves the same purpose, and the
`k`/wid checks cover the non-atomicity between `LRANGE` and `LLEN`.

### Delete and flush

`mdelete(keys)`: one pipeline of `UNLINK key`. In-flight staged writes are *not* aborted (their tmp
keys are unreachable by design): a writer that began before the delete may publish afterwards and
resurrect the value — accepted for a best-effort cache with versioned keys, window bounded by one
write's duration. `flush()`: unchanged in-flight-barrier semantics (`src/utils/in-flight.ts`, used
at `upstash.ts:195/206/217`).

## Scope & files

- **Rewrite `src/stores/upstash.ts`:** plural-native `UpstashStore` (`mget`/`mset`/`mdelete`/
  `flush`) and per-slot NOSCRIPT recovery. The shard codec, the two scripts + SHA constants, and
  the request framing/packing helpers live in `src/stores/upstash.internal.ts` — a sibling module
  outside the package `exports` map, so tests can import them without making them public API. Keep
  `UpstashStoreOptions` (add `shardBytes?`, default 64 KiB, and `maxResponseBytes?`, default 10 MB)
  and `createOptimizedUpstashStore` (lines 227-249; its rate constants and timeouts are deferred,
  owner-owned — see below). Delete `WriteId` (lines 23-46) and `SMART_READ_SCRIPT` (lines 49-56).
- **`maxRequestBytes` changes meaning** — breaking for existing callers. Today it budgets only the
  *value* (the constructor accepts anything `> WriteId.LENGTH`, which cannot carry a single
  `EVALSHA`); here it bounds the **exact serialized request body**, and `shardBytes` takes over as
  the element bound. Safe only because the storage format changes wholesale, so no already-written
  element is orphaned by the redefinition. Document both in the option TSDoc; a value tuned against
  the old meaning is now too small by roughly one key plus framing.
- **Singular adapters:** `Store` (`src/types.ts:160-165`) is still singular until
  [the plural-store contract](./000016-tib-plural-store-contract.md) lands — implement
  `get`/`set`/`delete` as deprecated one-liners over the plural methods
  (`get(k) { return this.mget([k]).then((r) => r[0]); }`).
- **New `test/stores/upstash.test.ts`** (+ helper): a scripted fake of the command subset
  (RPUSH/LRANGE/LLEN/LINDEX/UNLINK/EXPIRE/PEXPIREAT/PERSIST/RENAME/TIME/EVALSHA/SCRIPT LOAD +
  pipeline/multi with `keepErrors`) that can inject replays, mid-step expiry, and NOSCRIPT.
  Optional env-gated integration test against a real Upstash database.
- No other repo files change. `shardString` is reused as-is; the store no longer depends on
  `cyrb64`.

### Order

1. Fake-Upstash harness + header codec + shard packing (pure functions, tested first).
2. Direct writes + stage-1 reads + `mdelete` + singular adapters + NOSCRIPT recovery — a complete,
   correct store for values ≤ one request.
3. Staged writes + `PUBLISH` script + stage-2 reads.
4. Env-gated integration test; delete dead code.

## Verification

1. **Publish-script table test:** staging {absent, ours-complete, ours-short, ours-with-duplicate,
   continuation-at-head, already-published} × live {absent, ours, foreign} × deadline
   {future, past, persistent} ⇒ exact return code; only code `1` mutates live.
2. **Staging adversary:** inject replayed/delayed pushes and expiry+recreation at every request
   boundary; assert `PUBLISH` accepts only exact-head, exact-count staging and readers observe only
   old-or-one-complete values. Include the **combined fault** that defeats exact-head + `LLEN == k`
   alone: several delayed instances of one batch landing after a staging expiry, recombining to
   exactly `k` behind a genuine shard 0 (e.g. `s0,s1,s0,s1,s4,s5`) — publication accepts it, and
   only the reader's index-contiguity check rejects it.
3. **NOSCRIPT recovery:** evict scripts mid-pipeline; assert per-slot detection, one reload,
   reissue of only the failed subset, and that no request ever contains script source (the
   `createScript` ban, enforced by asserting request sizes).
4. **Budget properties:** fuzz sizes ±3 bytes around `S`, both HTTP limits, base64 padding, and
   record cap; assert complete elements ≤ `S`, exact request bodies ≤ `maxRequestBytes`, at most
   `floor(maxResponseBytes / S)` elements requested per read, correct routing, and over-cap drop.
5. **`mget` matrix:** duplicates, misses, empty values, 1-shard, multi-shard, mid-read replacement
   (wid skew or a short/empty stage-2 page ⇒ bounded retry ⇒ null); positional alignment
   throughout.
6. **Deadline replay:** replay every deadline-carrying request; assert the deadline never moves.
7. **Freeze/thaw:** pause a staged writer > 60 s at each step; no truncated publish, old value and
   its TTL untouched, tmp gone afterward — in the persistent configuration specifically.
8. **Keyspace audit:** randomized workload with injected crashes ⇒ every key is either a caller key
   or a `tmp:*` key with a live TTL (the recovery invariant, checked mechanically).

### Verified against a live Upstash database

Probed 2026-08-25–26 on the deployment in `.env`; re-check if the plan or tier changes.

- `allow-key-locking` **is honored** — an undeclared-key access is rejected outright
  (`ERR Dynamic keys are not allowed in Lua scripts when 'allow-key-locking' flag is set`), so the
  lock is narrowed to `KEYS`; it is contention control, not a correctness dependency.
- `TIME` works in bare Lua and under `allow-key-locking`; millisecond server-deadline checks behave
  as specified.
- `PUBLISH` returned `1/2/0/-1/-2/-3` for publish/replay/DOA/absent/foreign/bad-argument cases;
  exact-head validation rejected the continuation-at-head cancellation list without touching live
  (*not* the combined recombination behind a genuine shard 0, which publication accepts by design).
- `RENAME` carries the source TTL to the destination, and `PEXPIREAT` on a missing key returns 0
  without erroring; deadline and persistent publish branches leave the expected live TTL.
- `exec({ keepErrors: true })` surfaces `NOSCRIPT` in its own slot while siblings still execute.
- `unpack` with 112 args and `WRITE_DIRECT` end-to-end both behave as specified.

## Open risks

- **ThrottledStore's timeout is non-aborting** (`withTimeout` rejects but the write continues): a
  staged write exceeding the write timeout is reported failed yet may still publish later — a
  complete value (same class as an allowed replay), but `flush()` can resolve before actual
  persistence and `onWriteError` over-reports. The deferred worst-case-size derivation mitigates;
  true cancellation is out of scope.
- **Resurrection after delete** (staged writer publishes past a concurrent `mdelete`): accepted,
  window ≤ one write duration.
- **`wid` is 64 bits** (16 hex from `randomBytes`), and with the digest gone it is the sole
  cross-generation tear detector. A false match needs a collision *and* a read spanning both
  generations. Accepted, not engineered around: widening it costs bytes on every element.
- **Response framing is unmodeled by choice:** a `maxResponseBytes` set at the provider's exact
  limit will overshoot by the JSON framing of a full page, and the overshoot is systematic for one
  size class. Documented headroom is the mitigation; request bodies need none because their
  serialized bytes are measured exactly.
- **Batching reaches callers only after [the plural-store
  contract](./000016-tib-plural-store-contract.md):** until then
  `HierarchicalStore` (`src/stores/hierarchical.ts:18-33`) and `ThrottledStore` dispatch per key,
  so plural batching benefits direct `UpstashStore` users immediately and
  `createOptimizedUpstashStore` users once the contract migration lands.

## Notes

### Sizing `S`
`S` is a complete-element bound and a batch-width knob: reads request `floor(maxResponseBytes / S)`
elements per HTTP round trip. 64 KiB satisfies four soft constraints:

- it batches roughly 150 shard-zero elements under a 10 MB response cap;
- the ~21-byte continuation prefix is ~0.03% overhead per shard;
- `unpack` arguments in `WRITE_DIRECT` stay far below Lua's ~8000 limit;
- a 30 MB value is 640 list elements rather than thousands.

Tune it per workload: raise for few-keys-many-blobs deployments, lower for hundreds of small keys
per batch. The bottom end is bounded by the `unpack` limit; the top end by how many keys stage 1
must fit per response.

### Ops accounting

Both `RPUSH` and `EVALSHA`'s shard list are **variadic**, which decouples two counts: **element
count scales with `bytes/S`; billed command count scales with `bytes/request-budget`.** Sharding
finer never costs commands — only requests do. An `EVALSHA` is billed as itself *plus* every command
it executes (measured), and `MULTI`/`EXEC` are billed too, so `WRITE_DIRECT` costs 5 with a TTL
(`EVALSHA`+`TIME`+`UNLINK`+`RPUSH`+`PEXPIREAT`) or 3 persistent, against today's 5.

| operation             | today                      | this plan                       |
| --------------------- | -------------------------- | ------------------------------- |
| write 50 small keys   | 250 commands / 50 requests | 250 commands / **1 request**    |
| read 50 small keys    | 150 commands / 50 requests | **50 commands** / **1 request** |
| write one 30 MB value | ~12 commands / 5 requests  | ~26 commands / 6 requests       |
| read one 30 MB value  | ~6 commands / 5 requests   | ~6 commands / 6 requests (wash) |

The wins split: **reads** get ~3× cheaper in commands because Lua leaves the read path entirely;
**writes** win on round trips, not commands. Large staged writes cost *more* commands than today
(each batch is `MULTI`+`RPUSH`+`EXPIRE`+`EXEC` rather than one bare `RPUSH`, plus `PUBLISH`'s six)
— the price of closing the splice. Commands-per-write barely moved, so **do not raise
`maxWritesPerSecond`** on the strength of this plan.

## Derivation

### Declined: every layout that is not one LIST at the caller's key

Generation-scoped chunk keys (`<key>:<wid>:<i>`), hash-chained continuations, and a two-tier
manifest key pointing at chunk keys were each explored and each lost to the list. One disqualifier
covers all three: they name the pieces, and a named piece can outlive whatever points at it. A
crash between writing chunks and publishing the manifest — or between publishing a new manifest and
unlinking the old chunks — strands keys that no caller can reach and no TTL will collect, which is
exactly the recovery invariant this design exists to protect. Keeping the pieces as *elements* of
one key means the only two key classes are the caller's own and `tmp:<key>:<uuid>`, and the latter
always carries a refreshed 60 s TTL. Paging, atomic replacement, and self-healing all fall out of
that choice rather than being engineered on top of it.

### Declined: a per-value digest

An earlier revision carried a `cyrb64` digest in shard 0 to detect cross-generation tears.
Per-element indexing subsumed it: within one wid, index ⇒ bytes is a function, so contiguity over
`0..k-1` is a *structural* proof where the digest was only a probabilistic one — and it costs
`D + 1` bytes per element against O(bytes) of read-time hashing. The digest went.
`src/utils/hash.ts` stays: `eth-call/handler.ts:203` still uses `cyrb64Hash` for leader matching,
so "the store no longer depends on `cyrb64`" is not an instruction to delete the util.

### Declined: `BUSY` writer lease

Declined, not deferred. Per-writer uuid staging removes writer-writer staging interference
entirely; what remains is last-Redis-execution-wins at the live key, which the contract permits.
`ThrottledStore` already serializes writers per key in-process (`pending`/`active`,
`src/stores/throttled.ts:48-50`), and versioned caller keys make cross-process same-key writes
rare. A lease would add a new key class and liveness edge cases for zero correctness gain.

### Kept under the subtraction pass

Once the design was correct, a pass asked what could be dropped. One thing could (a claim that
`RPUSH` return lengths were retained as a write-path metric — nothing consumed them). Three could
not, and the reasons are the guardrail against the next attempt:

- **The `TIME`/DOA check before any mutation.** Redis Lua gives isolation, *not* rollback: a script
  that errors midway leaves its earlier mutations applied. Validating the deadline against server
  time before the first write is what keeps a bad deadline from being a partial write.
- **Read retry on skew.** It reads like belt-and-braces, but a list replaced mid-read is normal
  operation rather than a fault, and turning one into a miss costs a re-fetch of up to 30 MB.
- **The live-key fallback (codes `2`/`-2`).** A wash on complexity; kept because it distinguishes an
  already-published replay from a foreign value, which the caller acts on differently.

The framing that makes the rest of the design legible: **exact shard-0 equality and `LLEN == k` are
availability guards, not integrity guards.** They stop a routine single-fault write from replacing a
good value with a persistent miss. Integrity is the index's job, read-side. Neither subsumes the
other, which is why proving content read-side does not make the publication checks redundant.

### Combined faults, not single faults

Twice during this design a mechanism was removed on reasoning that enumerated one fault at a time,
and twice it was wrong; the second attempt produced a *served hit with wrong bytes*, strictly worse
than the miss it set out to fix. The rule that fell out: any proposal to drop a write-path check
must be tested against faults in combination — several delayed instances of one batch landing
*after* a staging expiry — not against each fault alone. Verification #2 pins the specific
counterexample; this is the general form of it.

### Deferred (owner-owned)

Being handled as separate ThrottledStore work; this plan points at it rather than specifying:

- Re-derive `createOptimizedUpstashStore`'s rate constants from the final measured command mix.
- Making the hardcoded 10 s `withTimeout` (`src/stores/throttled.ts:116`) a `writeTimeoutMs`
  option, derived together with `maxStalenessMs` from the worst-case staged write
  (staging requests plus publish × a per-request allowance).
