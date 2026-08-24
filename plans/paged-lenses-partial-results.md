# Paged lenses, partial results, and dropping RETURN mode

## Context

Batch chunk sizing is driven by a caller-supplied polynomial `G(N)`. When a chunk overshoots,
the current remedy is bisection — and bisection is the wrong primitive for this workload. It
costs `~2·log₂N` requests across `log₂N` sequential waves, pays the offending item's full gas
burn at every level, and then **still fails the whole request**: `call.ts:135` rethrows once a
chunk is down to one element, and `handler.ts:150-155` only commits cache entries after the
entire factorised call succeeds, so every good result the recursion fetched is discarded.

Per-item gas is not predictable and cannot be learned — cost varies without any storage key
changing — so no amount of modelling fixes this. The fix is to stop predicting and let work
report how far it got.

**A lens that pages itself does that.** It loops internally, checks `gasleft()` against costs
only it knows, stops early, and returns what it finished. The caller keeps every completed item
and re-requests the remainder, so overshooting stops being catastrophic and the polynomial only
has to be a rough opening guess.

Critically, a paged lens **is just a lens**: `target` is the paged contract, `targetData` is the
encoded array, the response is ABI data. The Yul envelopes stay byte-for-byte identical and
fully shape-agnostic. All new logic is TypeScript. We reached this after rejecting a generic
in-EVM dispatcher envelope, which would pay the lens prologue `N×` or `log N×`, require dynamic
array offset-table surgery in Yul, and add four more hand-pasted bytecode constants.

**What paging does and does not cover.** It eliminates aggregate-overrun bisection *for a lens
that reserves enough gas* for loop exit, ABI encoding, and return — a bad reserve can still let
individually-bounded items exhaust the frame. It does not by itself fix a *single unbounded item*:
an uncapped lens will attempt it, die, and lose the prefix it had already built. That case still
resolves through `OOG_SENTINEL` plus bisection, exactly as today.

Separately, RETURN mode goes away: EIP-170 caps its response at 24_576 bytes, and code deposit
costs 200 gas/byte on top (~4.9M gas for a full-size response). REVERT mode has neither limit.

## Scope

Three phases, independently shippable, in order. Phase 1 is mechanical and unblocks nothing —
land it first to shrink the surface the later phases touch.

---

## Phase 1 — Delete RETURN mode

Removes the `exfil` axis entirely (both RETURN envelopes, not just the uncompressed one) — the
EIP-170 argument applies to both, and collapsing the axis is where the branch savings come from.

- Delete `ReturnEnvelope.yul`, `ReturnEnvelopeCompressed.yul`, their two constants in
  `codec.envelope.ts`, and their `build:*` scripts in `package.json`.
- Drop `exfil` from `EthCallPolicy.batch` (`state-overrides.ts:15`) and from the `batch` type in
  `call.ts:26`. Breaking, but pre-1.0.
- `call.ts:106` — delete the `fetchChunk` indirection and `fetchChunkReturn`; `fetchChunkRevert`
  becomes the only path.
- `wrapDeploylessFactoryCall` (`codec.envelope.ts:159`) picks on `compress` alone.
- `isRevertExpected` (`codec.envelope.ts:186`) now matches every wrapper we emit — simplify its
  body and doc, but **keep it**: four transports call it to suppress retries of intentional
  success reverts (`rate-limiter/index.ts:53`, `logs-divider/index.ts:80`, `logs-sieve`,
  `logs-enricher`).
- Update the size-classifier doc at `call.ts:248` — the "Return data size (RETURN mode)" line
  goes, but keep the `/code.*size/` match, which also catches EIP-3860 initcode failures.
- Update the README `batch` surface and the `exfil` param doc at `actions/call.ts:38`.

**Keep `FACTORY_BYTECODE_RETURN_VIEM`.** It is not part of RETURN mode. It is the *inbound*
format: callers reach us through viem's own `call({ factory, factoryData, ... })`, which encodes
with `deploylessCallViaFactoryBytecode` (`viem/_esm/actions/public/call.js:308`). Removing it
breaks the documented entry point in `README.md:39`. Only outbound RETURN support is going away.

`compress` stays — it earns its keep against EIP-3860's initcode cap.

**Do not** document REVERT mode as universally supported. `codec.envelope.ts:135` already warns
that providers may truncate revert data; deleting the alternative does not make that untrue.

Tests: `deployless.test.ts` and `handler.test.ts` are heavily parameterised on `exfil`; collapse
the `describe.each`/`it.each` to REVERT-only and delete the RETURN-specific assertions
(`deployless.test.ts:386`, `:395`, `:420-445`).

**Note:** `ReturnEnvelope.yul` is currently untracked. Commit the branch before deleting if you
want it recoverable.

## Phase 2 — Paged lens protocol

Opt-in via `policy({ paged: true })`. Zero Yul changes.

**Lens shape** — one array in, results plus an exception list out:

```solidity
function page(T[] input) view returns (U[] results, uint256[] skipped)
```

The lens walks `0..N` in index order and stops once, having attempted
`i = results.length + skipped.length` items. `results` is positional over the attempted prefix
minus `skipped`; `skipped` holds indices (relative to *this call's* input) it declined.
`i` is derivable and needs no field of its own.

Indexing the exceptions rather than the successes is the efficiency choice: a dense
`uint256[] indices` naming every served item costs 32 bytes per item (32KB at N=1000, dominating
the response), while skips are rare by construction. `skipped == []` degenerates to pure prefix
semantics.

**Position encodes intent**, which is what lets one shape carry both outcomes:

- `[i..N]` — never attempted, gas ran out. **Retryable.**
- indices in `skipped` — the lens looked and declined. **Permanent.**

### Lens contract (`paged: true` docs must state all of this)

1. **Index order, single pass.** Out-of-order or multi-pass processing silently breaks the
   "before `i` means declined" inference.
2. **Attempt at least one item.** `(results=[], skipped=[])` is a **protocol violation**, not a
   retryable state. A lens that cannot afford item 0 must attempt it and let the frame die —
   the envelope reports that as `OOG_SENTINEL`. This is what makes termination finite without
   any retry bound or gas-escalation ladder (and there is no headroom to escalate into anyway:
   the node's own `eth_call` cap is the ceiling).
3. **Skips are deterministic.** A skip means invalid input, a reverting item — something that
   will decline identically next time. Gas is never a reason to skip; when gas runs out, stop.
4. **Values are batching-invariant.** A served value and a decline decision must not depend on
   position, batch composition, or `gasleft()`. Only the *stopping boundary* may depend on gas.

The common lens needs no per-item capping — check `gasleft()`, `break` when short:

```solidity
for (i = 0; i < input.length; i++) {
    if (i > 0 && gasleft() < reserve + estimate) break;   // tail: retryable
    if (!isValid(input[i])) { skipped.push(i); continue; } // deterministic: permanent
    results.push(one(input[i]));
}
```

**Per-item capping is optional and usually overkill.** It matters only when items may be
unbounded: without it, one such item kills the frame and discards the prefix, costing a
bisection; with it, that item becomes a stop that preserves the prefix.

A capping lens must obey one extra rule, or it violates clause 2:

> **Cap only when `i > 0`. At index 0, attempt uncapped.**

Otherwise the retry of a range headed by the unbounded item caps it again, breaks again, and
returns `([], [])` — a protocol violation instead of the "unservable" answer the caller needs.
Capping exists to protect a prefix; at index 0 there is no prefix to protect, so let the item
kill the frame and let the envelope report `OOG_SENTINEL`. This also makes capped and uncapped
lenses behave identically at index 0, which is where termination is decided.

A capping lens must also distinguish a capped OOG from a plain `revert(0,0)` — both yield empty
returndata — by measuring `gasleft()` around the call and treating "consumed ≈ the cap **and**
empty returndata" as gas-driven, hence `break` rather than `skipped.push(i)`. Same heuristic, and
same caveat, as the envelope's 63/64 check.

### Codec work — this is real, not reuse

`resolveArrayFunction` (`codec.inner.ts:35`) demands exactly one input and one output, both
dynamic arrays. A paged resolver must accept the two-output form and check output 1 is exactly
`uint256[]`.

**`hexToArray`/`arrayToHex` cannot be reused as-is for a two-array tuple.** At
`codec.inner.ts:109` the last dynamic element's end is `innerBytes` — the end of the whole buffer
— so in `(U[], uint256[])` the final `U` element absorbs the skipped array. `arrayToHex:124`
hardcodes a `0x20` single-parameter outer offset and cannot emit two dynamic outputs. Static `U`
works only by accident, via fixed stride.

Write a two-array tuple codec that still keeps `U` as raw slices: read both top-level offsets,
bound the first array at the second offset, instantiate only `skipped`, and emit a two-array
head. Test `string[]`, nested dynamic tuples, empty arrays, malformed offsets, and non-empty
skips.

### Response validation (protocol errors, never retried as gas failures)

For a range of length `N ≥ 1`, with `i = results.length + skipped.length`:

- **`1 ≤ i ≤ N`** — the lower bound is clause 2 as a runtime check, and it is load-bearing:
  without it `([], [])` satisfies every other rule and a lens can stall any range forever. It
  applies to **every** page, not just singletons.
- `skipped` strictly increasing and duplicate-free; every skipped index `< i`.
- `results.length == i - skipped.length`.

Violations throw immediately as protocol errors — never retried as gas failures.

Semantic violations — out-of-order execution, wrong-order values, gas failures mislabelled as
skips — are undetectable from the tuple and remain trusted-lens obligations.

### Client loop (`factorisedFactoryCall`, `call.ts:44`)

1. Partition with `G(N)` as today, deliberately over-packed, and fire all ranges in parallel —
   unchanged happy path, one wave.
2. Accumulate a sparse index→result map. Anything neither served nor skipped is remaining.
3. Fill the remainder in a second parallel wave, sized from the realized rate measured in wave 1.
4. Repeat until covered. Send disjoint ranges so total calldata stays ~`N` rather than the
   `N·(k+1)/2` a sequential continuation would re-send.

Measurement is within-request only — observed on the batch being served, used immediately,
never persisted.

**Termination.** A singleton call resolves to exactly one of:

| returns | meaning | action |
|---|---|---|
| `results=[x]` | served | done |
| `results=[], skipped=[0]` | declined | **terminal** — unservable |
| reverts `OOG_SENTINEL` | frame died; too big for one call | **terminal** — unservable |
| `results=[], skipped=[]` | — | **protocol violation** — throw |

Every branch terminates, so no retry counter and no escalation ladder are needed. Ordinary lens
reverts, malformed tuples, and transport errors propagate as they do today.

`OOG_SENTINEL` is load-bearing here, not vestigial: clause 2 routes "cannot serve item 0"
exclusively through a dead frame, and most lenses will not cap per-item, so an unbounded item
lands here too. Nothing else can report a frame that died.

**Also in this phase:** commit cache entries per range as they land. `factorisedFactoryCall`
currently returns only after every range finishes (`call.ts:139`) and the handler does one bulk
upsert afterward (`handler.ts:141-155`), so this needs an explicit awaited callback out of
`factorisedFactoryCall`. Completed entries must be flushed *before* a terminal error escapes —
without it, one unservable element still discards every sibling result and paging buys nothing.

(Steal the shape from the `eth_getLogs` handler, which already does exactly this: `createSink`
in `eth-get-logs/sink.ts:49` is threaded down through the request params as `onLogsResponse`
(`eth-get-logs/handler.ts:125-143`), and the catch block flushes before rethrowing with context
(`:149`). Explore during impl.)

**Settle the whole wave before failing.** `Promise.all` (`call.ts:139`) rejects on the first
branch failure while siblings are still in flight, so a terminal error built at that moment has a
non-final accumulator and may escape before sibling cache writes land. The paged path needs
`allSettled` semantics per wave: await every launched range, flush successes, *then* construct
the terminal error. Precedence must be deterministic — a transport or protocol error outranks an
unservable element, so infrastructure failures are never masked by a settled partial result.

## Phase 3 — Surfacing

Phase 2 produces a settled accumulator internally; Phase 3 only exposes it.

There is **one transport path**, always settled internally — no `settled` knob, no second wire
format, no variant to pick. It surfaces two ways:

**Path A — native `call()` stays strict.** No API change. All indices covered → dense `U[]` hex,
exactly as today. Any index unservable → throw a structured error carrying the accumulator, with
successes already flushed to cache.

**Path B — `call2`** is a thin client-side wrapper over that same path: invoke it, catch the
structured error, return its accumulator as a settled result. Two actions are warranted because
`call()` must keep its dense contract — one action that sometimes returned dense and sometimes
sparse would be a typing problem and a silent-corruption risk.

Because the error is an in-process JS object rather than an RPC payload, it can carry raw `Hex[]`
element slices directly. Nothing needs ABI-encoding for this boundary — which is what removes the
schema extension, the suffix-stripping, and the degenerate-input matrix that an on-the-wire
`settled` flag would have required.

**The error must be built to survive the stack.** Two wrapping boundaries and one retry layer sit
between the handler and the caller:

- `createTransport` wraps a plain `Error` as `UnknownRpcError`
  (`viem/utils/buildRequest.ts:262`). The error **must extend viem's `BaseError`** to pass through
  intact.
- viem's `call()` catches and wraps in `CallExecutionError`
  (`viem/actions/public/call.ts:339`), so the original survives only in the cause chain. `call2`
  must locate it with `error.walk(predicate)` and a branded type guard — bare `walk()` returns the
  *deepest* cause (`viem/errors/base.ts:78`), which is not necessarily ours.
- **`failover` currently falls over on it.** `defaultShouldThrow` (`failover/index.ts:77`) returns
  `false` for any error without a numeric `code`, so a partial-result error would silently
  re-run the whole request against the next provider. Teach it to recognize the branded error and
  treat it as terminal. (`isRevertExpected` does not help here — it suppresses retries of the
  inner intentional REVERT, a different problem.)

Tests must go through a real `client.call()`, not just a direct transport call, or none of this is
exercised.

**Shape:**

```ts
type PagedPartialResult = { data: Hex; missing: number[] }
```

`data` ABI-encodes the successful `U` values in original-complement order; `missing` lists
uncovered original indices. Full success returns `missing: []`, so callers have one shape to
handle. A typed `U[]` is not inferable — viem's `call()` returns raw `{ data: Hex | undefined }`
— unless `call2` also takes an ABI generic; leaving decode to the caller keeps the byte-level hot
path intact.

**Dedup rebasing.** One cache miss can stand for several original indices (`handler.ts:82`), so
`missing` must expand to all of them and values must follow the complement in *original* order.

Client typing follows `getLogs2`, but must admit **both** transports — a `CacheSchema` constraint
alone would exclude standalone `deployless(...)` clients, which support paging just as well.

Note the two-array tuple codec from Phase 2 is unaffected: `(U[] results, uint256[] skipped)` is
still the **lens→transport** format. Only the transport→action encoding disappears.

---

## Verification

```
pnpm exec vitest run          # 429 passing today; expect fewer after the exfil collapse
pnpm typecheck
pnpm exec biome check .
```

Per phase:

- **1** — constants must still match their sources: for each remaining envelope, confirm
  `pnpm build:<name>` output equals the pasted constant. This stays a manual check.
- **2** — mock paged lenses at `requestFn`, no EVM needed. One case per row of the termination
  table; a short prefix drives gap-filling to completion; an index in `skipped` is *not* retried
  while the tail after `i` *is*; each response-validation rule rejects. Codec tests need dynamic
  `U` specifically — `string[]` and nested dynamic tuples — since static `U` masks the bug.
- **3** — through a real `client.call()`, not a direct transport call: strict `call()` throws and
  the branded error is still findable via `walk(predicate)` after `CallExecutionError` wrapping;
  `call2` returns the sparse result over the same fixture (same transport path, so one set of
  transport fixtures serves both); a `failover` client does *not* fall over to the next branch on
  a partial result; dedup rebasing expands one miss to several original indices.

## Explicitly omitted

- **Generic in-EVM dispatcher envelope** — `N×`/`log N×` lens prologues, dynamic-array offset
  rebuilding in Yul, four more hand-pasted constants, and a second full-memory copy if nested.
- **Cross-request learning** of any kind: per-element cost history, cohort AIMD, griefer
  quarantine. Cost varies without the key changing. Permanently-skipped items are likewise not
  persisted — every request rediscovers them.
- **Legacy-path improvements** (4-way fan-out and friends) — all lenses here are ours and will be
  paged. Bisection stays as-is for the non-paged path.
- **Per-item gas telemetry** and a chunk-level `gasUsed` trailer — paging reports progress
  directly.
- **Sending explicit `gas` on the wire.** Still worth doing on its own merits (the failure
  boundary is currently whatever `--rpc.gascap` a provider runs), but it is *not* needed for
  termination: clause 2 makes that finite, and the node cap is the ceiling regardless.
- **Retry counters / gas-escalation ladders** for no-progress responses — superseded by clause 2.
- **A `settled` request flag and a transport→action settled wire format.** One always-settled
  transport path surfaced two ways is simpler, and takes the schema extension, suffix-stripping,
  and degenerate-input matrix with it.
- **Per-skip reasons** (`bytes[]` alongside `skipped`). Addable later without changing the shape.
- **Bitmap encoding for `skipped`** — `N/8` bytes and fully general, but it costs that even when
  nothing is skipped, and is more awkward to build in Solidity.
- **Automated bytecode-reproduction test** — previously deferred; Phase 1 deletes two constants
  rather than adding any, so it stays deferred.
