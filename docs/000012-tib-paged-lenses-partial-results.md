---
kind: tib
version: 0.0.12
landed:
  - 07e49df
  - 0df02a9
---

# TIB — Paged lenses, partial results, and dropping RETURN mode

Per-item gas is not predictable and cannot be learned — cost varies without any storage key
changing — so no amount of modelling fixes this. The fix is to stop predicting and let work
report how far it got.

**A lens that pages itself does that.** It loops internally, checks `gasleft()` against costs
only it knows, stops early, and returns what it finished. The caller keeps every completed item
and re-requests the remainder, so overshooting stops being catastrophic and the polynomial only
has to be a rough opening guess.

## Intent

Critically, a paged lens **is just a lens**: `target` is the paged contract, `targetData` is the
encoded array, the response is ABI data. The Yul envelopes stay byte-for-byte identical and
fully shape-agnostic. All new logic is TypeScript.

**What paging does and does not cover.** It eliminates aggregate-overrun bisection *for a lens
that reserves enough gas* for loop exit, ABI encoding, and return — a bad reserve can still let
individually-bounded items exhaust the frame. It does not by itself fix a *single unbounded item*:
an uncapped lens will attempt it, die, and lose the prefix it had already built. That case still
resolves through `OOG_SENTINEL` plus bisection, exactly as today.

Separately, RETURN mode goes away: EIP-170 caps its response at 24_576 bytes, and code deposit
costs 200 gas/byte on top (~4.9M gas for a full-size response). REVERT mode has neither limit.

## Context

Batch chunk sizing is driven by a caller-supplied polynomial `G(N)`. When a chunk overshoots,
the current remedy is bisection — and bisection is the wrong primitive for this workload. It
costs `~2·log₂N` requests across `log₂N` sequential waves, pays the offending item's full gas
burn at every level, and then **still fails the whole request**: `call.ts:134` rethrows once a
chunk is down to one element, and `handler.ts:150-157` only commits cache entries after the
entire factorised call succeeds, so every good result the recursion fetched is discarded.

## Design

Three phases, independently shippable, in order. Phase 1 is mechanical and unblocks nothing —
land it first to shrink the surface the later phases touch.

### Phase 1 — Delete RETURN mode

Removes the `exfil` axis entirely (both RETURN envelopes, not just the uncompressed one) — the
EIP-170 argument applies to both, and collapsing the axis is where the branch savings come from.

- Delete `ReturnEnvelope.yul`, `ReturnEnvelopeCompressed.yul`, their two constants in
  `codec.envelope.ts`, and their `build:*` scripts in `package.json`.
- Drop `exfil` from `EthCallPolicy.batch` (`state-overrides.ts:15`) and from the `batch` type in
  `call.ts:25`. Breaking, but pre-1.0.
- `call.ts:105` — delete the `fetchChunk` indirection and `fetchChunkReturn`; `fetchChunkRevert`
  becomes the only path.
- `wrapDeploylessFactoryCall` (`codec.envelope.ts:125`) picks on `compress` alone.
- `isRevertExpected` (`codec.envelope.ts:154`) now matches every wrapper we emit — simplify its
  body and doc, but **keep it**: four transports call it to suppress retries of intentional
  success reverts (`rate-limiter/index.ts:53`, `logs-divider/index.ts:80`, `logs-sieve`,
  `logs-enricher`).
- Update the size-classifier doc at `call.ts:250` — the "Return data size (RETURN mode)" line
  goes, but keep the `/code.*size/` match, which also catches EIP-3860 initcode failures.
- Update the README `batch` surface and the `exfil` param doc at `actions/call.ts:38`.

**Keep `FACTORY_BYTECODE_RETURN_VIEM`.** It is not part of RETURN mode. It is the *inbound*
format: callers reach us through viem's own `call({ factory, factoryData, ... })`, which encodes
with `deploylessCallViaFactoryBytecode` (`viem/_esm/actions/public/call.js:308`). Removing it
breaks the documented entry point in `README.md:39`. Only outbound RETURN support is going away.

`compress` stays — it earns its keep against EIP-3860's initcode cap.

**Do not** document REVERT mode as universally supported. `codec.envelope.ts:94-97` already warns
that providers may truncate revert data; deleting the alternative does not make that untrue.

Tests: `deployless.test.ts` and `handler.test.ts` are heavily parameterised on `exfil`; collapse
the `describe.each`/`it.each` to REVERT-only and delete the RETURN-specific assertions
(`deployless.test.ts:385`, `:394`, `:418-428`, `:430-451`).

**Note:** `ReturnEnvelope.yul` is currently untracked. Commit the branch before deleting if you
want it recoverable.

### Phase 2 — Paged lens protocol

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

#### Lens contract (`paged: true` docs must state all of this)

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

#### Codec work — this is real, not reuse

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

#### Response validation (protocol errors, never retried as gas failures)

For a range of length `N ≥ 1`, with `i = results.length + skipped.length`:

- **`1 ≤ i ≤ N`** — the lower bound is clause 2 as a runtime check, and it is load-bearing:
  without it `([], [])` satisfies every other rule and a lens can stall any range forever. It
  applies to **every** page, not just singletons.
- `skipped` strictly increasing and duplicate-free; every skipped index `< i`.
- `results.length == i - skipped.length`.

Violations throw immediately as protocol errors — never retried as gas failures.

Semantic violations — out-of-order execution, wrong-order values, gas failures mislabelled as
skips — are undetectable from the tuple and remain trusted-lens obligations.

#### Client loop (`factorisedFactoryCall`, `call.ts:41`)

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
| `results=[], skipped=[0]` | declined | **terminal** — reported in `skipped` |
| reverts `OOG_SENTINEL` | frame died; too big for one call | **terminal** — reported in `skipped` |
| `results=[], skipped=[]` | — | **protocol violation** — throw |

Every branch terminates, so no retry counter and no escalation ladder are needed. Ordinary lens
reverts, malformed tuples, and transport errors propagate as they do today.

`OOG_SENTINEL` is load-bearing here, not vestigial: clause 2 routes "cannot serve item 0"
exclusively through a dead frame, and most lenses will not cap per-item, so an unbounded item
lands here too. Nothing else can report a frame that died.

**Also in this phase:** commit cache entries per range as they land, via an awaited `onResolved`
callback out of `factorisedFactoryCall`, rather than one bulk upsert after every range finishes.
Completed entries must be flushed before *any* error escapes — a transport failure on one chunk
must not discard the siblings that already landed. The `finally` around the miss-fetch does this.

(Steal the shape from the `eth_getLogs` handler, which already does exactly this: `createSink`
in `eth-get-logs/sink.ts:49` is threaded down through the request params as `onLogsResponse`
(`eth-get-logs/handler.ts:125-143`), and the catch block flushes before rethrowing with context
(`:149`). Explore during impl.)

**Settle the whole wave before failing.** `Promise.all` rejects on the first branch failure while
siblings are still in flight, so a detached sibling may commit after the failure escaped. Each
wave needs `allSettled` semantics: await every launched range, then surface the first rejection
by index order. Unservable elements never reject — they accumulate — so any rejection is a real
transport or protocol error and outranks them, and infrastructure failures are never masked by a
partial result.

### Phase 3 — Surfacing

The transport responds in the shape the policy's `abi` declares. For a paged lens that is the
two-output tuple, so the chunked calls aggregate into a **single page over the caller's whole
input**: `results` holds everything served, `skipped` holds every index no chunk could serve.
Same type in, same type out — a paged read stays readable through `readContract`,
`decodeFunctionResult`, contract instances, and anything else that decodes against the fragment.

```ts
const [results, skipped] = await readContract(client, {
  abi: [pageAbi], functionName: 'page', args: [inputs],
  factory, factoryData, address: to,
  stateOverride: [policy({ abi: pageAbi, paged: true })],
})
```

Unpaged lenses are untouched: no `skipped` array to report into, so an unservable element still
throws, and the response is the bare `U[]` it always was. Both transports share one
`encodeResponse` helper that branches on `solidity.paged`, including the empty-input fast path.

**A partial result is a successful response, not a throw.** Nothing needs to survive viem's error
wrapping, `failover` never sees a partial result to fall over on, and viem cannot retry a
response — so no branded error, no `BaseError` subclass, no terminal-error classification, and no
non-retryable `code`. Each of those problems disappears structurally rather than being solved.

The cost is that a caller ignoring `skipped` silently gets fewer elements than they asked for.
The abi forces the array to exist, and re-throwing is one `if` at the call site.

**Rebasing.** `skipped` is expressed against the *caller's* input, not the deduped miss list.
One cache miss can stand for several original indices (`handler.ts`), so each unservable miss
expands to all of them, and `results` follows the complement in original order — cache hits and
freshly fetched values alike. The `elements_missing` facet field is restamped after rebasing so
telemetry matches the array the caller receives.

**What `skipped` deliberately merges.** Elements the lens declined, and elements that exhausted
the frame even when retried alone. The second depends on the node's `eth_call` gas cap, so a
provider with a higher cap might serve them, whereas a decline is a property of the element.
Distinguishing them would need a shape change; documented in the README and the `policy` TSDoc
instead.

## Verification

```
pnpm exec vitest run          # 492 passing at completion
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
- **3** — through a real `client.call()` and a real `readContract()`, not a direct transport call:
  the aggregated tuple decodes against the lens fragment; `skipped` names caller indices with
  dedup expanded; the empty-input page is well-formed; and the observability fields still stamp
  on what is now a `status: "ok"` run.

## Notes

Rationale that lived in code comments during review, kept here instead. None of it is needed
to read the code; all of it is needed to avoid re-deriving a decision or "simplifying" a
guard away.

**`validatePage`'s `attempted >= 1` floor.** `([], [])` satisfies every other rule in the
contract while making no progress. Without the floor a lens can stall a range forever, and no
retry counter would fix it because the response is well-formed. This is the whole reason
clause 2 exists.

**Dynamic offset validation (`sliceArray`).** The original check only compared an offset to its
neighbour and the region end, so an offset could point back into its own offset table. A
`string[]` whose sole element carries offset 0 was accepted, and the bytes sliced out decode as
`""` — a malformed response became a fabricated value, and was then cached. The floor is
`offset >= length * 32`; alignment and strict increase come from every dynamic value occupying
at least a length word, so equal offsets are never canonical. Pre-existing in `hexToArray`,
which the unpaged path still uses, so both are fixed by the shared `sliceArray`.

**`settleAll` vs `Promise.all`.** `Promise.all` rejects while siblings are still in flight, so a
detached sibling could commit *after* the failure escaped, leaving a non-final accumulator and
racing the cache write against the throw. Every branch must settle before the first failure
surfaces.

**Non-paged path retained.** Deleting it is ~10 lines of `src`, but it is a contract migration
for every deployed lens and rewrites 22 test references currently protecting the chunking logic.
Worth doing as a standalone change once lenses have moved over, not tangled up with this one.

## Derivation

We reached this after rejecting a generic in-EVM dispatcher envelope, which would pay the lens
prologue `N×` or `log N×`, require dynamic array offset-table surgery in Yul, and add four more
hand-pasted bytecode constants.

### Explicitly omitted

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
- **A `settled` request flag, and any transport→action encoding distinct from the lens's own.**
  Responding in the abi's shape takes the schema extension, suffix-stripping, and
  degenerate-input matrix with it.
- **Per-skip reasons** (`bytes[]` alongside `skipped`). Addable later without changing the shape.
- **Bitmap encoding for `skipped`** — `N/8` bytes and fully general, but it costs that even when
  nothing is skipped, and is more awkward to build in Solidity.
- **Automated bytecode-reproduction test** — previously deferred; Phase 1 deletes two constants
  rather than adding any, so it stays deferred.
