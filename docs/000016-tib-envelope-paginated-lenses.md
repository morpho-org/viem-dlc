---
kind: tib
version: 0.0.16
related:
  - 000012-tib-paged-lenses-partial-results.md
---

# TIB — Envelope-driven pagination: dropping the gas model, the lens-side estimate, and the lens-side loop

> **Superseded in part by 000016-tib-outcome-stream.md**: the Intent's "declared byte bounds"
> (`maxItemBytes` / `maxResultBytes`); the slab layout, the staging region and its `S` term, and
> the flat `FLOOR = 64·C + B + M + S` in Design (admission is now priced per attempt from the
> memory it touches); "The response encoding: one tag, wire-only" (now an outcome stream); the
> two envelopes, `RevertEnvelope.yul` and `RevertEnvelopeCompressed.yul` (now one
> `Envelope.yul`); the note "Why the allocation budget must see the output stride"; and the
> "Death-detection imprecision" risk, which now also covers the post-call deposit path.
>
> **Superseded in part by 000016-tib-streaming-decompression.md**: `MAX_ALLOC_BYTES` and the
> allocation budget in "The client"; the corpse route in the outcome table (halving on the deploy
> out-of-gas marker or a provider's "out of gas" text) and its facets `splits_corpse` /
> `corpse_errors` (the marker is now a thrown error); and the "Cold-start latency" and
> "`MAX_ALLOC_BYTES` sizing" open risks.
>
> **Superseded in part by 000016-tib-page-telemetry.md**: the continuation rule "re-packed at the
> count the page demonstrated" (now a prediction from per-page gas telemetry); the deferred
> `itemsHint` in Derivation (shipped as `batch.pageSizeHint`; the remembered realized rate is declined
> there); and what remains of the "Cold-start latency" risk.

The gas model survives pagination for exactly one reason: the prologue isn't paginated. Work
proportional to the *sent* count — envelope decompression, calldata copy, output allocation —
runs before the first `gasleft()` check and can kill a frame with zero progress, and the
polynomial `G(N)` exists to keep chunks below that cliff. Meanwhile the lens's own
`PER_ELEMENT_ESTIMATE + RETURN_RESERVE` stopping rule is the same prediction one layer down.
This TIB removes both — and the loop that hosted them — by moving pagination into the
envelope: the initcode this package already ships calls the lens's **per-item function** once
per element in its own EVM frame, deposits each result densely into a preallocated response
slab, and reports an element that couldn't be resolved due to gas **in-band** as a sign-tagged entry in the
`skipped` array **on the wire only**. The client consumes the tag (retrying once as a
singleton), and the caller-facing response stays byte-for-byte TIB 000012's
`(results, skipped)`. A lens is now one ordinary `view` function `f(T) returns (U)`; the
array-shaped function the caller reads through is synthesized on the TypeScript side. EIP-150's
retention is the return reserve, a frame that reaches the element loop never dies without
reporting, and bisection survives only as the fallback for a prologue too large for the node's
cap.

## Intent

**No per-lens, per-chain, or per-provider gas configuration.** The client packs by bytes: the
wire side against protocol constants (EIP-3860's initcode cap), the allocation side against
ABI-derived strides. The envelope forwards everything (`gas()`, clamped by EIP-150) to each
per-item call and survives on the retained 1/64 because its cleanup is O(1) in results and
O(`nS`) in accumulated skips (exit relocates the used skip words). `batch.gas`, the
transports' `gasLimit`, and every lens-side `PER_ELEMENT_ESTIMATE` / `RETURN_RESERVE` cease to
exist — along with the calibration workflow behind them. One gas-derived constant survives:
`MAX_ALLOC_BYTES`, set once in the package against a documented minimum supported node cap,
never per deployment. The only numbers a lens author can ever be asked for are **declared byte
sizes** for dynamic element types — deterministic facts about the lens's types, verifiable
against every response, failing loudly on the first violation — never gas, which drifts with
chain state and fails silently.

**Splitting and pagination unify, and the caller's response does not change at all.** An
element that runs out of gas is reported inside a successful page — one tagged index on the
wire — retried once as a singleton, and, if it dies there too, surfaced as an ordinary `skipped`
entry, exactly as TIB 000012 Phase 3 specifies. The tag never reaches the caller; the
gas-vs-decline distinction it carries is stamped into facets instead (and remains trivially
surfaceable later, since the wire already carries the bit).

**The lens is plain Solidity.** No import, no wrapper, no library call, no loop: a lens is a
`view` function over one element, under a name of the author's choosing, on a contract that may
expose several. Everything that makes it paginated lives in the envelope and the client. The
array-shaped function the caller reads through — `f(T[]) returns (U[] results, uint256[]
skipped)` — never exists on-chain: `arrayifiedAbi` derives it from the per-item fragment for
plain `readContract` use, and the `readLens` action hides even that.

The invariant everything client-side leans on, and the envelope exists to make true:

> **Once the envelope reaches its element loop, it never leaves a dead frame — every outcome,
> including running out of gas, is reported in-band.**

The corollary does the client's work for it: a frame that *does* die of gas is a prologue death
— counterfactual deploy, decompression, or slab allocation on a cap too small for the chunk —
cured by halving into smaller prologues. There is no longer a "lens not using the library"
case: every lens is compliant by construction, because compliance is the envelope's job. A lens
defect that is *not* gas-shaped — a malformed or oversized result — is never a corpse: the
envelope reverts with a distinct error selector and the client throws a protocol error, so a bug
is never bisected into a phantom gas failure.

## Context

The model's real job is cliff-avoidance, not chunk-tuning. Everything a lens frame does before
its first per-element gas check scales with the *sent* count: the envelope's calldata copy and
decompression, the ABI decoder's bounds checks, the `new U[](n)` result allocation, and the
quadratic memory-expansion term under all of it. A chunk past that cliff dies with zero
progress, and because the death is a bare out-of-gas the client cannot tell it from any other
failure: bisection is the only recovery, and a bisection storm is the exact failure pagination
was built to eliminate. Below the cliff the polynomial merely picks an opening chunk size (TIB
000012's framing). Compression sharpens the exposure by moving the binding constraint from
bytes to gas, so a compressed lens needs the polynomial to be *right*, not merely conservative.

The number the model is solved against is itself unreliable. Production calibration
(the downstream consumer's calibration tooling) shows what "setting `gasLimit` correctly"
actually costs: providers **silently cap** — accept a high `gas` param, execute with less —
detectable only by probing with a `gasleft()`-returner initcode and flagging a >15% shortfall;
behind an aggregator the effective cap is **non-deterministic per request** (routing fans across
upstreams with different caps), forcing a K=64×S=8 saturation probe and a clamp to the
stingiest upstream (a min over the providers behind it). Even then a seam remains: caps are probed via
`eth_call` but polynomials are fitted via `eth_estimateGas`, with no guard that both methods
share a cap. The net effect is that every request budgets for the worst upstream even when a
permissive one serves it — a static number cannot do better. `gasleft()` inside the granted
frame is the one gas signal that cannot lie: the node enforces termination with the same
counter it reports.

The fleet this serves is uniform. A survey of the downstream consumer (2026-09-01) found ten lens
contracts with eleven array entrypoints, every one a plain per-element loop: no shared work
before or after the loop, no in-call memoization, no cross-element dependency, all shared state
already carried as constructor immutables. Exactly one call site constructs a `policy` — the
batch-lens helper — and its eleven callers pass a compiled contract (ABI plus deployless
descriptor), a function name, and an input array. Nothing in the fleet needs a lens-side loop, and nothing outside one
helper touches the array-shaped fragment.

TIB 000012's thesis was "stop predicting and let work report how far it got." It applied that
to the loop but left prediction alive in three places: the client's `G(N)`, the lens's per-item
estimate, and the lens's return reserve. This TIB applies it to all three — and then removes the
loop itself, since once each element is its own frame the loop has nothing lens-specific left in
it. Breaking changes are acceptable — the only consumer is ours, in another repo.

## Design

Within one EVM frame, completed work survives only by returning before the frame dies, so any
single-frame loop must *predict* the next attempt's cost. Prediction is the estimate. The only
way to delete it is to delete the single-frame assumption: make each attempt its own frame.
EIP-150 — a caller retains 1/64 of its gas across a call, unconditionally — is the
protocol-granted reserve, and it suffices *because the work funded by it is O(1)*: results are
deposited into their final positions as they arrive, so surviving a death costs a header patch
and a `REVERT`, never a tuple encode. The frame that hosts this loop is the envelope's own:
it is already the frame that calls the lens, already ours in Yul, and already pays the
prologue — hosting the loop there costs nothing a lens-side library did not, and removes the
library.

### The lens: one function per element

```solidity
contract BlueHealthLens {
  IMorpho immutable morpho;
  constructor(IMorpho _morpho) { morpho = _morpho; }

  function healthOf(Input calldata x) external view returns (Health memory) {
    Market memory m = morpho.market(x.id);
    require(m.lastUpdate != 0);          // permanent condition → skipped
    return _health(x.id, m, x.borrower);
  }
}
```

That is the whole lens. Per element, exactly one of three things happens to the call the
envelope makes: it **returns**, and the result is kept; it **reverts** — any reason, any data —
and the index goes to `skipped`; it **runs out of gas**, and the envelope, holding the retained
1/64, tags the index `~i` and stops. The lens's obligations shrink to two the envelope cannot
enforce: **skips are deterministic** (a revert means invalid input or a permanently failing
element, never something more gas would pass) and **values are batching-invariant** (neither a
value nor a decline may depend on position, batch composition, or `gasleft()`).

**Shared work goes in the constructor.** It was already the only channel — item frames share
no memory — and it is a capable one: immutables for value types, and storage writes (the
constructor is not `view`) for tables the per-item function then reads at 100 gas a slot once
warm. EIP-2929 warmth is per transaction, so the first element to touch a market's storage warms
it for every later element in the chunk; the per-frame design loses none of the caching a
single-frame loop would get. The constructor runs once per chunk, in the prologue — so its cost
must stay bounded and modest, since halving shrinks the chunk but not the constructor. A target
that already has code is refused: the envelope cannot check resident code against
`factoryData`, so the only lens it trusts is the one it watched the factory deploy.

**Per-item reverts are not surfaced.** A revert reason is discarded and the element is skipped,
whether the revert was the author's `require` or a broken oracle three calls down. This is the
price of "one bad element never kills a batch"; the alternative — treating revert-with-data as a
page failure — is declined in the Derivation.

### The envelope drives the page

The two envelopes (`RevertEnvelope.yul`, `RevertEnvelopeCompressed.yul`) keep their
constructor-args shape — `(address target, bytes targetData, address factory, bytes
factoryData, uint256 config)` — and their deploy step, and replace the single `CALL(target,
targetData)` with the element loop. `targetData` stays exactly what the client sends today: the
ABI-encoded array-shaped call `f(T[])` — selector, `0x20`, `n`, elements — so the wire format,
the client codec, and every cache key are unchanged; the envelope simply reads `n` and the
element region at the fixed offsets and never uses the array selector. `config` packs the
**per-item selector** (top 32 bits), two layout bits (input dynamic, output dynamic), and the
two element sizes (static stride, or declared maximum tail bytes for a dynamic type). The
response cap is derived on-chain from `n` and the output layout, so no separate word is needed.

Prologue, paid at full gas: deploy (unchanged), decompress (compressed variant), then allocate
the slab — header, result body at capacity, and skip scratch — plus a **disjoint per-attempt
staging region** (`4 + 32 + maxItemBytes` for a dynamic input, `4 + stride` for a static one),
placed after the constructor-argument/decompressed-input region and never overlapping the slab,
and touch the high-water mark once, so every later memory access is linear. Staging exists
because a dynamic element's tail cannot be reused in place as `selector ‖ 0x20 ‖ tail` — the
bytes before it belong to the offset table or the previous element. A dynamic *result* needs no
temporary: the call is made with an empty output buffer and the tail is `returndatacopy`'d
straight to its slot from offset `0x20`, after the head word is checked in scratch. The slab
begins with `OK_SENTINEL` in its first word: the success path is `revert(slab, 4 + used)` with **no copy at all**, which is what makes
the post-loop path O(1) regardless of payload. The loop, per item `i`:

```text
if gasleft() ≤ FLOOR:                                  // FLOOR = 64·C + B + M + S, see below
    if i == 0: skipped.push(~0); patch; revert(slab)   // refuse to light a fuse it can't report
    break
stage args: selector ‖ element (static: stride bytes; dynamic: 0x20 head ‖ tail)
g = gasleft()
ok = staticcall(gas(), target, args, out = next free result slot)
if ok:
    validate returndatasize (== stride, or head 0x20 and tail ≤ bound); committed in place at slot nR
else if returndatasize() == 0 and gasleft() ≤ g/64 + SLACK:
    skipped.push(~i); patch header; revert(slab)       // sub-frame died of gas: tagged, stop
else:
    skipped.push(i)                                    // deterministic revert: plain skip
```

- **Full forwarding, and why the ratio isn't a knob.** `staticcall(gas(), …)` forwards
  everything and lets EIP-150's clamp do the withholding — 63/64 is the one value on the dial
  that isn't a magic number. It maximizes the in-page serviceable band (an item costing up to
  ~63/64 of *remaining* is served in one call) and makes stop-on-death forced rather than
  chosen: after a death the envelope holds ~`R/64`, which funds the O(1) report and nothing
  else.
- **The floor, stated exactly.** `FLOOR = 64·C + B + M + S`: `C` is the worst post-call work on
  either path — classify, one bounded result deposit (with head-stripping for a dynamic
  output), tag write, header patch, exit relocation, `REVERT` over already-expanded memory;
  `B` is the call-site cost paid *before* EIP-150's split (call base cost); `S` is the staging
  copy of one element, also paid before the split and sized by the input stride or bound
  (3 gas per word — ~100k for a 1 MiB bound); `M` is a small margin absorbing `⌊·⌋` rounding
  and the loop's own opcodes. EIP-150 divides
  what remains *at the call*, i.e. `⌊(R − B)/64⌋`, so the floor must pre-add `B` and the
  comparison must be strict with margin — the reserve claim is an inequality that must be
  probed at the boundary, not an identity. All four are fee-schedule constants (`C` and `S`
  fold the declared bounds); none is measured lens behavior, and none
  grows with *results*: the slab prepaid the encode, so there is no rising bar. The one motion
  is skips: each skip to relocate at exit adds ~3 gas to `C`, i.e. ~200 gas of floor (`64 × 3`)
  per skip — precise, tiny, and paid only by pages that actually skip. An attempt at current
  skip count `nS` is admitted against `C(nS + 1)`: the reserve covers every existing skip **and
  the one plain or tagged skip this very attempt can append before exit**; a floor break that
  attempts nothing needs only `C(nS)`. `FLOOR` is tens of thousands of gas — pages drain
  essentially to the bottom.
- **The head never produces a corpse.** At `i == 0` below the floor, the envelope does *not*
  attempt — attempting without the reserve could kill the frame during the report, the one
  outcome the architecture forbids. It tags `~0` without attempting: the tag's meaning is
  "gas could not resolve this element in this frame," which is exactly as true of a fuse the
  envelope refused to light as of a sub-frame that died. Either way the page reports
  `attempted ≥ 1` and `([], [])` stays unreachable.
- **Death detection.** Empty revert data *and* `gasleft() ≤ g/64 + SLACK` (gas sampled at the
  call site) means the sub-frame died of gas; anything else is a deterministic revert → plain
  `skipped`. Whatever the callee does not burn is refunded on top of the retained 1/64, so only
  a per-item function that reverts empty with less than `SLACK` (32) gas left in its sub-frame
  reads as a death — in practice, an actual out-of-gas. (Far narrower than the ">98.4%" TIB
  000012 quoted for the same heuristic.)
- **Malformed results are protocol errors, not corpses.** A success whose `returndatasize`
  breaks its contract (≠ stride; > declared bound; missing/invalid offset head for a dynamic
  output) is never truncated and never silently dropped: the envelope reverts **with a distinct
  error selector** (`MalformedResult(index, returndataSize)`), which the client surfaces as a
  protocol error — a lens bug must not be halved into a phantom gas failure and eventually
  mislabeled unservable.

**The slab layout.** The layout must simultaneously support dense static results, exact
dynamic tail boundaries, accumulated skips, exit cleanup independent of results, and a
used-prefix return. The arrangement: **results grow up from the front** — sentinel, tuple head,
then the offset table, then tails deposited **densely at slot `nR`** (never at input position
`i` — `results` is dense over successes, and a mid-page skip would otherwise desynchronize
slots from lengths), dynamic tails appended at a moving cursor with their offset-table entries
written as they land, so a stopped page leaves the tails gap-free (the offset table is sized for
`n`; its unused words sit between table and tails, and the decoder reads by offset).
**Skips accumulate as single words in scratch at the slab's far end**, and at exit — death or
completion — the envelope **relocates the `nS` used skip words to immediately follow the
results cursor** (one `mcopy`) and reverts with the used prefix, never full capacity.
Relocation preserves the results-before-skipped region order the decoder expects, and its cost
is what makes exit cleanup O(`nS`) rather than strictly O(1): ~3 gas per accumulated skip,
pre-charged into the floor — flat in results, negligible per skip, and bounded by the page's own
skip count, never its capacity. The decoder is offsets-authoritative — it slices what the
offset table says, not what adjacency implies. Byte-level details are owned and tested by the
envelope, against dynamic `U` specifically.

**Element layouts, derived by the client from the fragment:**

- **Static element types** need nothing from the author: the client reads the strides from the
  ABI and packs them into `config`.
- **Dynamic element types** declare their bounds in the `policy` — `maxItemBytes` /
  `maxResultBytes`, in **padded ABI tail bytes** (length word plus padded data; heads excluded —
  the envelope stages the `0x20` input head and strips the output head itself). An item
  exceeding `maxItemBytes` is a *deterministic decline*, and the **client declines it first**:
  element sizes are known before packing, so an oversized element goes straight into `skipped`
  with no RPC made — it must never reach the envelope, where a multi-megabyte value that
  compresses under the initcode cap would OOG decompression before the loop could inspect it.
  The envelope repeats the check as defense in depth. The client additionally verifies returned
  element sizes and throws a protocol error on violation. Sizes are not gas: they don't drift
  with chain state, they're checkable per response, and a wrong one fails loudly on the first
  crossing instead of silently at load.

**What becomes structural, honestly stated.** Clauses 1-2 of the TIB 000012 lens contract
(index order, single pass; adjudicate element 0 — attempt it or tag it) are now properties of
the envelope, which no lens can bypass. Clauses 3-4 (skips are deterministic; values are
batching-invariant) cannot be enforced by any shape — a per-item function can still read
`gasleft()` or make gas-sensitive downstream calls — and remain documented obligations, stated
on `readLens` and the `policy` TSDoc.

### The response encoding: one tag, wire-only

A `skipped` entry `i` means the lens looked and declined — permanent. An entry `~i` (bitwise
NOT, i.e. `-1 - i` two's-complement) means gas could not resolve the element in this frame.
`~i` rather than `-i` because `-0 = 0` cannot tag the head; `~i` rather than an
offset-by-one sign scheme because untagged entries keep their exact TIB 000012 meaning —
ordinary skips decode unchanged. Real indices are byte-cap-bounded at a few thousand, so
tagged values (descending from `2^256 − 1`) can never collide.

Page-level validation (`validatePage`, `src/utils/deployless/call.ts`): at most one tagged
entry per page; if present it is the last entry and its decoded index equals `attempted − 1` —
the envelope stops at the first death, so a page's gas-death is always its final adjudicated
element. `attempted = results.length + skipped.length ≥ 1` as today.

**The tag dies at the decoder.** `hexToPage` reads the (at most one, always last) tagged
entry — the one place a top-bit-set skip value is legal, decoded as the 256-bit complement,
bigint-safe — and hands the client `{ results, skipped: number[], died?: number }`. No tagged
value enters aggregation, sorting, the handler's `missing: number[]`, or the caller's array:
the caller-facing `(results, skipped)` is plain indices with TIB 000012's exact semantics,
including its documented decline/unservable merge — terminal gas failures are, per the fleet's
operating assumption, unservable (an element a singleton cannot serve on one route is, outside
truly pathological corners, unservable on the others too). The gas-vs-decline split lives on
in facets (`elements_unresolved`), and surfacing it per-element later is additive: the wire
already carries the bit.

### The client: two fragments, bytes-only packing, singleton escalation, corpses halve

**Two fragments, one authored.** The lens's real ABI has only `f(T) returns (U)`. The
transport keeps working from the array-shaped fragment `f(T[]) returns (U[] results, uint256[]
skipped)` — it is what the caller's calldata is encoded with, and `policy.abi` keeps meaning
"the fragment this call is encoded with" — and derives the per-item fragment from it
(`itemFragmentOf`), whose selector goes into `config`. Both transforms accept exactly one
input parameter and exactly one output parameter and reject overloaded or otherwise ambiguous
function selection; `itemFragmentOf` removes exactly the terminal dynamic-array suffix from
those sole parameters, preserving names and tuple components, so `T[][]` becomes `T[]` and
`T[3][]` becomes `T[3]`. Within that domain the derivation is unambiguous, but it is also the
one place a wrong fragment would fail *silently*: a per-item selector the lens does not
implement makes every call revert and every element a plain skip. There is one provenance guard
and one diagnostic: `arrayifiedAbi(itemFragment)` is the only supported way to produce the
array-shaped fragment, and it takes the per-item fragment from the contract's real ABI, so
`getAbiItem({ abi: lens.abi, name: "healthOf" })` type-checks the name against the contract's typed ABI;
and a page whose every element was skipped stamps `pages_all_skipped`, which makes a missing
selector visible but cannot distinguish it from a legitimate all-decline page or a shared
downstream failure.

`arrayifiedAbi` is typed as a value-level and type-level transform (template-literal `` `${T}[]`
`` types, components carried through, outputs named `results` and `skipped`), verified against
viem's `readContract` and the downstream consumer's TypeScript to preserve struct field names through to
the decoded result. For most callers even that is invisible:

```ts
const { results, skipped } = await readLens(client, {
  ...healthLens.with(MORPHO),          // abi, address, factory, factoryData
  functionName: "healthOf",            // narrowed against the lens's real ABI
  args: inputs,                        // Input[]
  batch: { batchSize: MAX_INITCODE_SIZE, compress: true },
});
```

`readLens` is a viem action: it looks the per-item fragment up in the real ABI, synthesizes the
array-shaped one, encodes the call, attaches the `policy`, invokes `call` with the factory
descriptor, and decodes to `{ results: U[], skipped: number[] }`. Plain `readContract` remains
supported by passing `abi: [arrayifiedAbi(item)]` after the `with()` spread and the same fragment
to `policy` — the interop path the previous design led with, now the escape hatch.

**Packing.** Chunks honor byte budgets and nothing else:

- wire bytes ≤ `batchSize` (≤ `MAX_INITCODE_SIZE`, chain-enforced anyway);
- an allocation budget: per chunk, decompressed input bytes, plus `N ×` the *output* stride,
  plus `N × 96` of fixed per-element bookkeeping (result-offset word, skipped word, input
  head word), plus a fixed scratch term, the staging region (`36 + maxItemBytes` for a
  dynamic input), and — when compressing — the compressed args, which stay resident beside
  their decompressed copy, must fit under `MAX_ALLOC_BYTES` (1 MiB) — one
  constant covering envelope decompression, the element region, and the slab, set against the
  documented minimum supported node cap (10M; the quadratic term at the cap is ~2M). The output
  stride comes from the fragment for static types and from the declared `maxResultBytes`
  (padded-tail bytes) for dynamic ones — input bytes alone bound `N` but not the slab, whose
  stride is lens-specific and can dominate, and for dynamic `U` the final array carries an
  `N × 32` offset table on top of the tails.

The decompressed-side cap is also what closes the FLZ loophole (~100× worst-case expansion:
49KB wire → megabytes decompressed → quadratic memory-expansion cliff in the envelope).
`resolveArrayFunction` requires declared bounds for dynamic layouts (request-time error
otherwise). Element-size verification against those bounds happens **where hits and misses
merge** in the handler's response assembly, not only in `hexToPage` — cache hits bypass the
codec and may have been written under a policy with different bounds; a violation's error names
its source (fresh vs cache hit). The packer's always-make-progress rule also changes: an element
that alone exceeds a cap is a terminal client-side decline, never a single-element chunk.

**The outcome protocol:**

| observed | meaning | action |
|---|---|---|
| page, no tag | complete, or ran low at the floor | commit; repack tail at realized rate |
| page, `died: k`, chunk count > 1 | `k` died at in-page gas | commit prefix; **retry `k` as a singleton** — minimal prologue and slab, the strongest grant this client can construct; repack the rest of the tail at the *served* count (adjudicated minus the death), or under the byte caps alone when nothing was served — a died head must not turn the tail into singletons |
| page, `died: 0`, chunk count == 1 | the element died (or was below the floor) holding a singleton's grant | terminal: plain `skipped` entry, counted in `elements_unresolved` |
| dead frame (recognized OOG-like provider text), count > 1 | prologue death: deploy, decompression, or slab allocation on a cap too small for the chunk | halve |
| dead frame (as above), count == 1 | same, at minimum size | terminal: plain `skipped` entry |
| dead deploy (`OOG_SENTINEL`: factory call failed with empty returndata and the envelope drained to ~2/64) | the factory or the lens constructor ran out of gas | as a dead frame: halve, terminal `skipped` at count == 1 |
| `CounterfactualDeployFailed` (deploy failed, not OOG-shaped) | the lens's constructor reverted, or the factory did not deploy at `target` | protocol error — thrown, never halved |
| envelope's `MalformedResult` selector | lens bug: a result that does not fit its declared layout | protocol error — thrown, never halved |
| unrecognized provider error | unknown | propagate as a transport failure; `failover` is the recovery |

Escalation is a two-step ladder bounded per element: at most one in-chunk adjudication and
one singleton (corpse halving is accounted separately — an element inside repeatedly-dying
chunks re-executes once per surviving ancestor, as in today's bisection). The singleton, not
"head of a fresh chunk," is the escalation target: a head retry would still prepay its whole
chunk's decompression and slab — a singleton sheds all of it. Route non-determinism means
even the singleton's grant varies per request; by the fleet's operating assumption that is a
pathological corner, accepted and stamped (`elements_unresolved`) rather than retried on a
budget.

**Prologue deaths: one reported, the rest text-matched.** With the loop in the envelope, the
frame that dies of gas is the envelope's own, and it cannot report its own death; the
provider's error text (`out of gas` and friends) routes to halving, and an unrecognizable
message — a bare `execution reverted`, an empty `-32000` — propagates like any other transport
failure (and is captured in `corpse_errors` so the regex set can learn it). An upstream silently
clamped below the documented minimum supported cap degrades *when its error text cooperates* and
fails visibly when it doesn't; it never fails silently or corrupts results. The deploy step is
the one prologue death the envelope *can* report, because the factory runs in a child frame: a
failed factory call with empty returndata and the envelope drained to ~`2/64` of what it had (the
factory keeps its own 1/64 of a dying constructor's frame and hands it back, so the threshold is
two retentions, not one) is a deployment out-of-gas — the factory or the constructor — and the
envelope reverts `OOG_SENTINEL`, the heuristic it previously applied to the lens call. It is
treated as a dead frame: halving does not shrink the constructor, but it does shrink the initcode copy and
memory the envelope pays before the factory call, so a near-boundary deploy can succeed in a
smaller chunk, and at a singleton it is terminal like any other corpse. A deploy failure that
is *not* OOG-shaped — the constructor reverted, or the factory did not leave code at `target` —
reverts `CounterfactualDeployFailed` and is thrown, never halved.

**Termination.** Every page adjudicates at least one element (index 0 is attempted or
tagged); a singleton tag is terminal; halving terminates classically. No retry counters, no
depth bounds beyond the existing timeout budget.

**There is no devolved case.** A lens cannot opt out of the runner, forget a guard, or write an
inline loop that dies with its prefix: the only thing it can write is the per-item function.

**Deletions.** `GasModel`, `solveMaxItemsByGas`, the `maxItemsByGas` derivation and its
`packRange` clamp; `batch.gas` and `paged` from `EthCallPolicy`; the entire `gasLimit` config
(`DeploylessConfig`, the cache context, the handler plumbing); the unpaginated response path;
and the interim `contracts/PagedLens.sol`
with its export wiring. `deployless(http(url))` takes no numbers at all, and `failover`
branches over heterogeneous providers need no per-branch caps — the envelope adapts to whatever
frame each node grants, including silently clamped ones: `gasleft()` inside the frame observes
the true per-request grant, so a request routed to a permissive upstream serves more and one
routed to a stingy upstream serves less, with no worst-upstream pessimism and no probe.
Downstream, the entire calibration pipeline — the saturation probe, the polynomial fits, the
min-over-providers clamp, and the hand-patched limits table it feeds — retires with the
version bump.

**Facets.** The pathology signals a production incident needs, emitted at the existing
callsites (accumulate on the loop's structs, stamp in the same `finally`); wire-level counters
are stamped once, caller-facing element counts follow 000012's convention and are **restamped
after rebasing** so telemetry matches the array the caller receives (a terminal miss expands to
all its deduped indices):

| field | kind | production question it answers |
|---|---|---|
| `splits_size` | wire | did our packer over-pack the wire? (413/initcode only) |
| `splits_corpse` | wire | is the envelope prologue dying — a cap below the documented minimum, or a deploy (factory or constructor) running out of gas? |
| `corpse_errors` (push, ≤3) | wire | which provider phrasings fall through the OOG regexes and need adding? |
| `attempts_unresolved` | wire | how often did gas fail to resolve an element — sub-frame deaths plus below-floor refusals (indistinguishable by design; both wear the tag)? |
| `pages_escalated` | wire | how many singleton round-trips did gas-unresolved reports cost (a below-floor refusal escalates exactly like a death)? |
| `page_adjudicated` (stat) | wire | is the lens yielding ~1 element/page — lens pathology vs input size? |
| `pages_all_skipped` | wire | did every element of a page revert? A per-item selector the lens does not implement is one cause; a shared downstream failure is another |
| `batch_alloc_bytes` (stat) | wire | which budget bound the chunks — wire bytes or allocation? |
| `pages_continued` | wire | floor-stops only: how much input needed a second wave? |
| `elements_declined_oversize` | caller | did a wrong `maxItemBytes` drop elements with zero RPCs made? |
| `elements_missing` | caller | total caller-facing `skipped.length` (declines + unresolved + oversize) |
| `elements_unresolved` | caller | the gas-terminal subset — what a higher-cap provider might still serve |

`splits_max_depth` survives unchanged and inverts polarity: any non-zero value is pathology.
Bound-violation protocol errors carry their source (fresh vs cache hit) in the thrown error;
the error path of `observe` captures them.

### Phase order

1. **Envelopes.** Move the element loop into both Yul envelopes (config word, slab with leading
   sentinel, tag, floor, `MalformedResult`, `OOG_SENTINEL` on the deploy step), re-paste
   constants, port the Foundry sweep tests from the library to the envelopes. Delete
   `contracts/PagedLens.sol`.
2. **Client.** `arrayifiedAbi` / `itemFragmentOf` in `codec.inner.ts`; `config` packing in
   `codec.envelope.ts`; `CounterfactualDeployFailed` as a thrown protocol error in `call.ts`
   (`OOG_SENTINEL` keeps its corpse classification); `pages_all_skipped`; the `readLens` action.
3. **Consumers.** README; the "paginated" vocabulary throughout exported surfaces.

Phases 1-2 of the superseded revision — the tag-aware codec, bytes-only packing, singleton
escalation, the facet set, the deletions, and the removal of the unpaginated path — landed in
the working tree already and carry over unchanged.

**As built (2026-09-01).** Both envelopes compile under `--strict-assembly --optimize` with
`memoryguard(0x80)` and the constructor args copied to the returned base; the page loop lives in
`paginate` / `stage` / `malformed` Yul functions with a five-word memory frame (floor, scratch
pointer, elements pointer, elements end, config) so the loop body keeps under the stack limit
without a manual stash. Envelope initcode grew from 187 / 363 bytes to 956 / 1,150. Foundry
(scratch project, against a local EVM): static and dynamic layouts, interleaved skips with a
mid-page death, a head death, `MalformedResult`, deploy OOG → `OOG_SENTINEL`, and a grant sweep
of the uncompressed envelope with no failure above the first success; the compressed envelope on
FLZ-compressed calldata. Real node (Robinhood Chain, example 06 through `readLens`): 152 pairs in
one page, and 4,700 pairs (the candidate set replicated twenty-fold, ~170M gas of work in one
compressed chunk) paged to completion with every element accounted for. The client-side
`envelopeConfig` packs the word; `arrayifiedAbi` / `itemFragmentOf` / `readLens` shipped as
specified; `pages_all_skipped` stamps.

## Scope & files

- `src/utils/deployless/RevertEnvelope.yul`, `RevertEnvelopeCompressed.yul`,
  `codec.envelope.ts` — the element loop (ported from `contracts/PagedLens.sol`), `config`
  packing, slab-leading sentinel, staging region, `MalformedResult` and
  `CounterfactualDeployFailed` matchers; `OOG_SENTINEL` now emitted for an OOG-shaped deploy
  failure; re-pasted constants verified by their `build:*` scripts. Standalone Yul has no
  `via-ir` escape hatch for stack depth: `memoryguard` enables the optimizer's stack-to-memory
  mover, but only under its contract — the program touches only `[0, size)` and memory at or
  above the returned pointer. The envelopes honor it with `base := memoryguard(0x80)`: the
  constructor args are copied to `base` rather than to zero, and `[0, 0x80)` stays scratch for
  the error paths and the dynamic-result head check. Successful optimized strict-Yul
  compilation is the acceptance criterion; an explicit memory-stash pattern is the fallback.
- `src/utils/deployless/codec.inner.ts` — `arrayifiedAbi` (typed transform), `itemFragmentOf`
  (the inverse the transport uses), `ResolvedArrayFunction.itemSelector`.
- `src/utils/deployless/call.ts` — outcome table as above; `pages_all_skipped`.
- `src/actions/read-lens.ts` — new; exported from `actions`.
- `contracts/PagedLens.sol` and the `./contracts/*.sol` export — deleted.
- `README.md`, `src/actions/call.ts` TSDoc — "paginated lens", the two-fragment story,
  `readLens`, constructor guidance, the revert-reason tradeoff.
- Tests — envelope-level Foundry sweeps (scratch project, results recorded here); vitest
  cases for `arrayifiedAbi` round-trips (static tuple, nested arrays, dynamic types), the
  derived selector, `pages_all_skipped`, `CounterfactualDeployFailed` as thrown, `readLens`
  end-to-end against a mock transport.
- Downstream (not this repo) — the consumer's batch-lens helper collapses onto
  `readLens` (its single-output guard, return-type helper, and positional output↔input
  alignment all change for `(results, skipped)`; its `formatAbiItem`-derived cache blob key
  changes shape once); transport configs, per-resolver `gas:` blocks, the gas-limit table, and
  the calibration route retire. Sequence with the version bump.

Deliberately unchanged: the wire form of `targetData` and the cache key it feeds; wave
repacking at realized rate; timeout classification and its cautious split budget; the
caller-facing response shape and semantics from TIB 000012 Phase 3 (a partial result stays a
successful response, same ABI, same merge); and the handler's rebasing machinery.

## Verification

- Mock lenses at `requestFn`: one case per outcome-table row; a `died` mid-chunk element is
  retried exactly once, as a singleton; a singleton tag is terminal and surfaces as a plain
  `skipped` entry; a below-floor head produces `([], [~0])`, never a corpse; `validatePage`
  rejects a non-final tag, multiple tags, and a tag whose decoded index ≠ `attempted − 1`;
  `~0` round-trips through `hexToPage` (bigint-safe); an OOG-text corpse with `count > 1`
  halves with no singleton probe; `MalformedResult` and `CounterfactualDeployFailed` throw
  protocol errors and are never halved; `([], [])` is rejected as today; a terminal miss
  expands through dedup to all its caller indices, and `elements_unresolved`/`elements_missing`
  restamp to the expanded counts; a page skipping every element stamps `pages_all_skipped`.
- `arrayifiedAbi`: derived fragment round-trips (`itemFragmentOf(arrayifiedAbi(f)) ≡ f`) for
  static tuples, nested arrays, and dynamic types; `readLens` returns `{ results, skipped }`
  typed from the per-item fragment, and only one-parameter, one-value `view`/`pure` names are
  accepted as `functionName` (struct field names survive through the examples' typecheck; the
  vitest tree is not typechecked).
- Envelope, in Foundry (`test/forge`, `pnpm test:forge`; the tests build both envelopes from
  their Yul through the package's own scripts and fail if the pasted constants in
  `codec.envelope.ts` drift): a page with interleaved skips deposits densely; a stopped dynamic page
  decodes byte-exactly by offsets; gas sweeps of both envelopes with no failure above the first
  success; malformed paths, including a dynamic result shorter than its head word; oversize
  input declines; deploy OOG; a target with resident code is refused.
- Dynamic types: an oversized input element is declined client-side with **no RPC made** (and
  stamped `elements_declined_oversize`); a response element exceeding its declared bound →
  protocol error naming its source, enforced for fresh responses *and* cache hits written
  under different bounds; dynamic layout without declared bounds → request-time error.
- Packing: pathologically-compressible input packs under the allocation budget, not just wire
  bytes; a wide-output-stride lens packs smaller chunks than a narrow one at equal input
  stride.
- Real node: a lens end-to-end, including a replicated input large enough to force several
  pages, with every element accounted for and no corpse.
- `pnpm exec vitest run`, `pnpm typecheck`, `pnpm exec biome check .`

## Open risks

- **EVM repricing erodes margins.** `C`, `B`, `M`, `SLACK`, and the 1/64 retention are
  fee-schedule-derived; a repricing (à la EIP-2929) or an L2 with a nonstandard schedule can
  shrink them. All live in the envelopes; re-verify the boundary probes on hard forks of target
  chains.
- **Provider revert-data truncation** (carried from TIB 000012, amplified here): the response
  rides in revert data, and slab-sized payloads give a truncating provider more to truncate.
  Used-prefix returns keep typical payloads small, and `skipped` is the slab's tail, so any
  truncation cuts it: the decoder rejects a `skipped` offset past the payload, or a `skipped`
  length the bytes cannot hold, before it reads a single result.
- **Doomed burn at full forward.** A genuinely unbounded element burns ~63/64 of the frame's
  remaining gas per attempt (at most twice: in-chunk, then as a singleton) with zero yield —
  node compute and timeout exposure, not money. A lower forwarding ratio would bound the
  waste without touching correctness, at the price of shrinking the in-page band and
  reintroducing a magic constant; declined for the current fleet (see Derivation).
- **Dynamic-type coverage is bounded, not general.** Dynamic types are served only up to
  declared byte bounds; a lens with genuinely unbounded per-item output has no home in this
  design.
- **A wrong per-item fragment fails as all-skips.** The transport cannot verify that the lens
  implements the derived selector; `arrayifiedAbi`'s type-checked source and `pages_all_skipped`
  are the guards.
- **Per-item revert reasons are lost.** A broken oracle and a deliberate decline look the same
  to the caller. Tolerable for a fleet that dedups caller-side and treats `skipped` as "re-read
  individually if you care"; revisit if a lens needs to distinguish them (Derivation lists the
  alternative).
- **Constructor work is prologue work.** A lens constructor that alone exceeds a node's cap
  dies as an `OOG_SENTINEL` corpse on every chunk size down to singletons — every element
  terminal-unresolved, at bisection prices — and a constructor that merely does a lot narrows
  the room the chunk has. The guidance is "bounded and modest"; nothing enforces it.
- **Cancun required.** The exit relocation is one `mcopy`; a pre-Cancun chain fails the
  envelope's first call visibly (invalid opcode). A loop would cost ~40 gas per skip and move
  the floor by ~2.5k per skip.
- **Cold-start latency.** Bytes-only packing costs at least one extra wave versus an
  *accurate* model whenever the input needs multiple pages: wave 1 is over-packed by
  construction, so a tail always exists, and heterogeneous item costs can add further waves.
  Call counts are comparable, not lower. The deferred `itemsHint` (Derivation) recovers the
  guaranteed wave. Each gas-death adds one singleton round-trip — accepted; they are
  exceptional by construction.
- **Death-detection imprecision.** A per-item function that reverts empty with fewer than
  `SLACK` gas left in its sub-frame reads as a gas death: it escalates once and lands in
  `skipped`. Narrow, but a heuristic, and `SLACK` is a fee-schedule constant like the rest.
- **Terminality is route-sampled.** A singleton tag is terminal on the route that served it;
  under non-deterministic caps a different route might have served the element. Accepted as a
  truly pathological corner and visible in `elements_unresolved`.
- **`MAX_ALLOC_BYTES` sizing.** A memory-cost bound, not a cliff dodge: 1 MiB of envelope
  memory costs ~2.2M gas of expansion (`3w + w²/512` at `w = 32,768` words), about a fifth of
  the 10M minimum cap and noise above it. Too low only costs round trips; a cap clamped below
  the floor still degrades to halving (on the deploy-OOG marker or the provider's out-of-gas
  text) rather than a silent loss.
- **Yul stack depth.** The loop carried ~20 live values as a Solidity library and needed a
  memory stash to compile under the legacy codegen; standalone Yul has the same 16-slot reach.
  `memoryguard` enables the optimizer's stack-to-memory mover only when its memory-region
  contract holds (see Scope); the memory-stash pattern is the fallback either way.

## Notes

**Why the envelope hosts the loop.** Once each element is its own frame, the loop contains
nothing lens-specific: it reads elements at fixed offsets, stages a selector and a slice, calls,
classifies, deposits. Hosting it in the lens (the superseded `PagedLens.sol`) forced every lens
to carry a wrapper whose parameters and returns were all unused — a stub with an ABI attached —
and to import and correctly parameterize a library the client already had all the information
for. Hosting it in the envelope costs one CALL per element either way, ~300 bytes of initcode
per request against a 49,152-byte cap, and a second copy of the loop (two envelopes). It buys a
lens that is one function, a "compliant by construction" guarantee that deletes the devolved
case, and the removal of the shipped `.sol` and its resolution risk. TIB 000012 declined a
dispatcher envelope for paying the array lens's prologue N times, needing offset surgery in
Yul, and adding four constants; all three dissolved: the loop is per-item (one Solidity
dispatch per element, exactly what the library paid), the offset surgery was already written,
and the constants stay at two.

**Why two fragments, and why the policy keeps the array-shaped one.** The caller's calldata has
to be encoded against *something* viem can type, and the transport has to slice an array out of
it; the array-shaped fragment is that something, and keeping it as `policy.abi` leaves the
wire form, the client codec, and every cache key untouched. The per-item fragment is what the
chain needs and what the author actually wrote, so it is the source and the array-shaped one is
derived — never the reverse, because a derived per-item fragment is only as right as the string
it was derived from, whereas `getAbiItem` against the contract's real ABI is checked by the
compiler. `readLens` exists so that, for the fleet's actual call pattern (a compiled contract, a
function name, an input array), neither fragment is ever written by hand.

**Why any revert is a skip.** The envelope sees `ok = 0` and some returndata; it can distinguish
empty from non-empty, and nothing else about intent. Making revert-with-data a page failure
would give authors a channel (`revert()` to decline, `revert Reason()` to fail loudly) at the
cost of one broken downstream call killing the whole chunk — TIB 000012's pre-pagination
behavior, and the reason its examples wrapped only the decline condition in `try/catch`. With
the fleet dedup'ing caller-side and treating `skipped` as "unknown, re-read if you care," a
skipped element is the safer default; the alternative is reversible at the envelope with a
one-word protocol change if a lens ever needs it.

**Why full forwarding, precisely.** Correctness only requires the retained fraction to fund
the O(1) cleanup, which any ratio satisfies — so the ratio is a policy dial trading in-page
band width (∝ forwarded fraction: items up to `f·R` serve in one call) against doomed burn on
unbounded items (also ∝ `f`). `gas()` with the EIP-150 clamp is the one non-arbitrary point on
that dial, maximizes the band (killing the two-calls-per-item penalty a smaller ratio imposes
on items between `f·cap` and ~cap), and makes stop-on-death forced rather than chosen — after
a death the envelope holds ~`R/64` and physically cannot attempt more.

**Why the floor is flat, and why it is an inequality.** The superseded guard compared a
*shrinking* reserve against a *growing* bar (`encodeCost(nR)`), demanding real accounting.
Depositing each result into its final slab position prepays the encode during the page, so
death-cleanup is constant — but EIP-150 splits gas *at the call site, after* the call's own
costs, and `⌊(R − B)/64⌋` at `R` barely above `64·C` rounds below `C`. Hence
`FLOOR = 64·C + B + M`, strict, probed at the boundary on both the report path and a
near-boundary dynamic success. The claim is a margin-bearing inequality; only the *shape* of
the argument (flat, not growing) is structural.

**Why the slab leads with the sentinel.** The success path must be O(1) in the payload, and
the payload is already in place; prefixing the sentinel to the slab at allocation time means
the exit is a single `REVERT` over a region that was expanded at full gas. It also retires the
previous revision's response-cap constructor word: with the loop in the envelope, the slab is
the envelope's own memory and its bound follows from `n` and the output layout.

**Why corpses route to halving, and what they can still be.** After the floor covers the head
(tag-without-attempt), the boundary arithmetic covers near-`FLOOR` deaths, and the exit is a
bare `REVERT`, a frame can die in exactly one region: the prologue (deploy, decompression, slab
allocation) on a cap too small for the chunk — which halving cures directly, smaller chunk =
smaller prologue. The deploy step is the one prologue death that reports, because the factory
runs in a child frame the envelope survives; an OOG-shaped deploy failure is a corpse like the
others, and only a non-OOG deploy failure is carved out as thrown. What must *not* be a corpse is
a lens defect the envelope detected — hence `MalformedResult`: a deliberate revert with a recognizable
code, surfaced as a protocol error, so a bug is never bisected into a phantom gas failure and
mislabeled unservable.

**Why the singleton is the escalation, not a fresh head.** A head retry still prepays its
whole chunk's decompression and slab — potentially millions of gas of prologue the element
doesn't need. A singleton sheds all of it and is the strongest grant this client can
construct; there is no third rung on the ladder, which is what makes a singleton tag terminal
and the per-element bound exactly two adjudications. Route variance below that is accepted as
pathological (Open risks) rather than retried on a budget.

**Why the tag stays on the wire.** Mid-chunk tags are consumed by the singleton retry inside
the request; what survives to the caller is only the terminal case, and the fleet's operating
assumption is that a singleton-unservable element is unservable, full stop — so the caller
array keeps TIB 000012's exact semantics and the split rides in `elements_unresolved` instead.
The decision is reversible at zero protocol cost: the wire already carries the bit, so
surfacing it later is a client-side change only.

**Why `~i`.** `-i` cannot tag index 0 (`-0 = 0`) — and the head is the tag's most important
case. An offset-by-one sign scheme could, but it would renumber every *untagged* entry,
breaking the existing decode for ordinary skips; `~i` leaves them byte-identical. Collision is
structural: real indices are bounded at a few thousand by the byte caps, tagged values descend
from `2^256 − 1`. Decoding is the 256-bit complement (not a bit-mask), bigint-safe, and legal
at exactly one position — the last `skipped` entry — so no tagged value ever enters the plain
`number[]` world.

**Why at most one tag, always last.** The envelope stops at the first sub-frame death (it
retains only ~`R/64`, enough to report, not to continue), so a death is necessarily the final
adjudicated element. `validatePage` enforcing tag-is-last-and-equals-`attempted − 1` turns the
structural fact into a checkable protocol rule; a response violating it is malformed, never
retried.

**Why clause 2 needs no `invalid()`.** The old design converted an unaffordable head into a
dead frame because a caught head-death would otherwise produce `([], [])`. With the tag, both
an attempted-and-died head and a below-floor refusal report `([], [~0])` — attempted = 1,
protocol-valid, and semantically identical: gas could not resolve this element in this frame.

**Why the allocation budget must see the output stride.** Input bytes bound `N`, but the slab
is `N ×` the *output* stride, which the input side says nothing about — a 32-byte input with a
ten-word result blows any cap probed against a two-word one. The client knows the output
stride from the fragment for static types and from the declared bound for dynamic ones, so the
budget stays free of calibration: strides are facts, read not measured.

**Why declared sizes don't violate the zero-numbers intent.** A size bound is deterministic
(it doesn't drift with chain state), one-dimensional, and verifiable — the client checks every
response element against it and fails loudly on the first crossing. A gas polynomial is none
of those things: it drifts, it has three coefficients, and when wrong it fails silently as a
bisection storm under load.

**Why bisection is kept at all.** It is the only mechanism that makes progress against a frame
that dies without reporting — which, under the invariant, means a prologue death. Keeping it
as the corpse fallback means a too-stingy cap degrades to the current design's behavior
instead of failing outright.

**Why "paginated."** A lens is not itself a page; it is read in pages. The adjective for the
mechanism is *paginated*, and it is the word exported (`arrayifiedAbi`, the README section, the
TSDoc). Nouns keep *page* (`hexToPage`, `pages_continued`). TIB 000012's title is history and
stays.

## Derivation

The path: client-side `G(N)` → (first draft) estimate migrates into the lens as a
`gasleft()`-derived allocation bound plus the existing stopping rule → (second draft) frame per
attempt with full forwarding, a guard inequality, and dead-frame forensics (head-peel,
request-wide gate, evidence-based demotion, `invalid()` at the head) → two adversarial review
rounds (GPT-5.6 via Codex, 2026-08-31) that repeatedly punctured the guard margins and the
forensics → a clean-room exercise (same model, given intent and constraints but not the
design) that independently converged on frame-per-attempt with a shared runner, but with a ½
forwarding ratio, slab-deposited results, and in-band failure reporting → the synthesis (full
forwarding + slab + in-band sign-tagged reporting) → a third review round (2026-09-01) that
found the synthesis's three real holes — the envelope's own copy reserve, the unguarded head
(→ tag-without-attempt below the floor), and the slab layout (→ dense deposit at `nR`, cursor
tails, offsets-authoritative decode) — plus the singleton-vs-head escalation correction and the
TS representation gap → those fixes folded, with the tag demoted to a wire-only detail → a
resumed validation pass that pinned the slab's physical arrangement, narrowed the invariant,
and renamed the gas facets around "adjudicated" → **implementation as a bundled Solidity
library** (`contracts/PagedLens.sol`, Foundry-verified: gas sweeps of the runner and both
envelopes with no corpse above the first success, the death boundary swept across item costs,
malformed and oversize paths, dynamic layouts; real-node run of 4,686 pairs paging to
completion), which surfaced that the envelope needs no copy at all (CALL output buffer), that
the death blind spot is "under `SLACK`" not "98.4%", and that the loop needed a two-function
split for stack depth → **the maintainer's ergonomics objection**: the wrapper
`function page(Input[] calldata) returns (Health[] memory, uint256[] memory) { PagedLens.run(sel, 2, 2); }`
is a stub whose inputs and outputs move "magically", not Solidity → the fleet survey (eleven
entrypoints, all per-element; one `policy` call site; `arrayifiedAbi` prototyped type-safe
against the downstream consumer's viem/abitype/TS 7) → **this revision**: the loop moves into the
envelope, the lens becomes one function, the array-shaped fragment becomes derived, and
`readLens` hides the derivation → a second resumed validation pass on this revision (same
Codex session), which reserved the floor for the skip an attempt can *append* (`C(nS + 1)`),
pinned a disjoint staging region and its place in the allocation budget, constrained
`itemFragmentOf` to one input and one output, split OOG-shaped deploy failures (`OOG_SENTINEL`,
halved) from semantic ones (`CounterfactualDeployFailed`, thrown), qualified the constructor
guidance for predeployed targets, made the `memoryguard` precondition concrete, and demoted
`pages_all_skipped` to a diagnostic. The frame-per-attempt core survived every round; the guard
arithmetic, the forensics, the library, and three successive absolutist adjectives did not.

Declined:

- **The ½ forwarding ratio** (clean-room's choice): once cleanup is O(1), any ratio is
  correct, so the ratio is pure policy — and ½ halves the in-page band, making every item
  costing (cap/2, cap·63/64] pay two calls forever, while its benefit (bounding the doomed
  burn on unbounded items) prices a population the fleet doesn't have. Full forwarding is
  also the only non-arbitrary value. Revisit the dial only if unbounded-item lenses
  materialize.
- **The lens-side library** (`PagedLens.sol`, built and verified): superseded by hosting the
  loop in the envelope. Everything it proved carries over — the floor constants, the slab
  layout, the death heuristic, the `mcopy` relocation — as a Yul port; what it cost (an import,
  a wrapper stub, hand-counted strides, a shipped `.sol` with a resolution risk, and a
  "lens not using the library" corpse class) does not.
- **Keeping an empty array-function declaration in Solidity** so the compiler's ABI stays the single
  source of viem types: honest but odd (a function that returns empty arrays if called), and
  unnecessary once the fleet's one call site already synthesizes the fragment it needs.
- **Inferring static strides at runtime** in the library (input stride from calldata, output
  stride from the first success) to make tier 0 `run(selector)`: the slab's high-water mark
  needs the output stride before item 0; deferring the expansion reintroduces an estimate.
  Moot once the client packs the strides into `config`.
- **Revert-with-data as a page failure** (a decline/fail channel for authors): one broken
  downstream call would kill a whole chunk — TIB 000012's pre-pagination behavior. Any revert
  is a skip; reversible with a one-word protocol change at the envelope if ever needed.
- **A third ABI output, a custom packed stream, or a caller-facing tag** for the death
  report: the report is structurally one bit riding on one index per page, consumed inside
  the request by the singleton retry; the caller-facing split it could carry is declined
  because a singleton-unservable element is treated as unservable (route-variance corners are
  pathological), keeping 000012's response semantics untouched. The stream's other benefit —
  in-place deposit — is taken independently by the slab.
- **`-i` as the tag** (cannot tag index 0) and **offset-by-one signed indices** (renumbers
  untagged entries, breaking the existing decode for ordinary skips).
- **A `{index, tagged}` public representation**: unnecessary once the tag dies at
  `hexToPage` — pages decode to `{ results, skipped: number[], died?: number }` and no tagged
  value survives into aggregation, sorting, or rebasing.
- **Head-of-fresh-chunk as the escalation target**: still prepays a full chunk prologue and
  slab; a singleton sheds both and is the true maximum grant. Superseded by the two-rung
  ladder.
- **Head-peel, the request-wide peel gate, evidence-based demotion, and `invalid()` at the
  head** — the second draft's forensics, superseded wholesale: they existed to disambiguate
  dead frames, and in-band reporting makes compliant frames deathless instead.
- **Continuing the page after a death on the retained ~`R/64`**: occasionally serves a few
  more cheap items, but makes the one-tag-and-last protocol rule conditional and the floor
  analysis iterative. The tail is one wave away regardless.
- **A gas-estimate tier** (000012-style inline loop from `perItemGas + reserve`): no *current*
  occupant. Expensive lenses amortize the sub-call overhead (~150-500 gas against tens of
  thousands); cheap lenses cannot exhaust a frame under the byte caps on any cap the fleet has
  measured, so they never page. On a hypothetical 3-5M silently-clamped upstream a cheap lens
  pages and the overhead turns material — noted, not designed for.
- **Enforced naming / a generator-emitted entrypoint**: nothing depends on a name — the
  per-item function is looked up by the name the author gave it, and a contract may expose
  several.
- **Adaptive measurement as the primary stop** (`gasleft() < k × maxObservedItemCost`): the
  return reserve re-appears the moment an item exceeds `k ×` all predecessors. Kept only as
  the optional futile-attempt skip, where it has no correctness role.
- **Geometric sub-chunk doubling** (sub-call ranges of 1, 2, 4, …) to amortize CALL overhead:
  a dying sub-chunk loses its whole range's work. Per-item frames lose at most one item.
- **Truncating oversized dynamic results** instead of refusing them: truncated ABI data
  decodes as garbage or, worse, as a plausible value; refusal (`MalformedResult`) turns a wrong
  declared size into a loud protocol error instead.
- **Checkpointing across frames via storage or transient storage** from the per-item function:
  `staticcall` forbids both. The constructor is the one write channel, and it runs before the
  loop.
- **`itemsHint` / remembered realized rate** to recover the cold-start wave: an opening guess
  with no correctness role — overshoot costs one continuation, never a bisection — and it
  recovers the *guaranteed* extra wave, leaving extra waves only where a heterogeneous tail
  contradicts it. Deferred, not declined: add when the extra RTT demonstrably matters.
- **A `pages_floor_stopped` facet** (duplicate of `pages_continued`) and a **singleton retry
  budget** for route variance (pathological corner; stamped instead).
- **Client-side probe call** (tiny wave-0 chunk to measure rate before packing): strictly worse
  than eating the extra wave — it serializes the same discovery without attempting anything in
  parallel.
