---
kind: tib
version: 0.0.16
landed:
  - d812e07
related:
  - 000012-tib-paged-lenses-partial-results.md
---

# TIB — Paginated lenses: pagination in the envelope, with no load-bearing gas figure

A lens is one ordinary `view` function over one element. The initcode this package already ships
for deployless `eth_call` — the envelope — deploys the lens, then calls that function once per
element in its own EVM frame with all remaining gas, admitting each attempt against the fee
schedule before touching anything, and appends one record per adjudicated element to an outcome
stream it exfiltrates via `REVERT`. Compressed input is decompressed one element at a time through
a fixed history. Every page also reports what its attempts cost and what the frame had to spend, so
the client packs every chunk by one prediction: from measurement once a page has landed, and before
that from two stated figures — the provider's cap on the transport, the lens's cost on the policy —
that only ever cost a round trip when wrong. The elements pages do not reach are pooled across pages
and re-packed together. The caller-facing response is TIB 000012's `(results, skipped)`,
byte-for-byte. No gas number is configured for correctness, none is fitted, and the constants
that remain are measured against adversary fixtures or read from the protocol, with one stated
policy target for prediction headroom.

This document consolidates four earlier TIBs written against the same baseline (envelope-driven
pagination, the outcome stream, streaming decompression, page telemetry); their originals are in
git history. It describes the system as it stands.

## Intent

- **The lens is plain Solidity.** One `view`/`pure` function `f(T) returns (U)` per element,
  under a name of the author's choosing, on a contract that may expose several; no import, no
  wrapper, no loop. Everything that makes it paginated lives in the envelope and the client. The
  array-shaped fragment the caller reads through, `f(T[]) returns (U[] results, uint256[] skipped)`,
  never exists on-chain: `arrayifiedAbi` derives it, and `readLens` hides even that.
- **Once the envelope reaches its element loop, it never leaves a dead frame.** Every outcome,
  including running out of gas, is reported in-band. Nothing is touched before it is admitted, and
  nothing after a call costs more than the callee is unable to take away.
- **Envelope memory grows only with progress.** Beyond a prologue bounded by EIP-3860 and a
  fixed history, the envelope expands memory only for records it has produced and the element it is
  about to attempt. A chunk of a million elements costs nothing until they are attempted; a
  compressed chunk costs nothing until decompressed, and is decompressed only as attempted.
- **No number from the author about the lens is load-bearing.** Static sizes come from the ABI;
  dynamic sizes come from the elements and the results themselves. The only figures a caller can
  set — a provider's cap and a lens's measured cost — size chunks only until the pages replace
  them: the cap after the first page, the cost after the first costed attempt.
- **Every served attempt is an observation.** Whether or not it stopped for gas, a page reports
  the budget its frame had, what the frame spent before its first attempt, and the sum, sum of
  squares and maximum of per-attempt gas over every element but a death. Every chunk after the
  opening wave is packed from the request's own pooled observations, never from the count of one
  parent page, by the same predicate that packs the opening wave from the stated figures.
- **The value for every figure the caller can set is readable off any observed request**, in the
  units it is set in, on the wide event.
- **The caller's response does not change.** `readLens` and `readContract` receive
  `(U[] results, uint256[] skipped)`; a partial result is a successful response; cache keys and
  cached values are raw element bytes.
- **Nothing is remembered across requests.**

## Context

**The gas model's real job was cliff-avoidance.** Before this work, a lens was an array function
run in one frame, and the client sized chunks with a caller-fitted polynomial
`G(N) = constant + linear·N + quadratic·N²` against a required per-transport `gasLimit`.
Everything a frame did before its first per-element gas check scaled with the *sent* count:
calldata copy, decompression, the ABI decoder's bounds checks, the `new U[](n)` result allocation,
and the quadratic memory-expansion term under all of it. A chunk past that cliff died with zero
progress, and because the death was a bare out-of-gas the client could not tell it from any other
failure: bisection was the only recovery, and a bisection storm was the exact failure pagination
had been built to eliminate. Compression sharpened the exposure by moving the binding constraint
from bytes to gas, so a compressed lens needed the polynomial to be right, not merely conservative.

**The number the model was solved against was itself unreliable.** Providers silently cap —
accept a high `gas` parameter, execute with less — detectable only by probing; behind an
aggregator the effective cap varies per request as routing fans across upstreams, forcing a
saturation probe and a clamp to the stingiest. Even then caps were probed via `eth_call` while
polynomials were fitted via `eth_estimateGas`, with no guarantee the two shared a cap. Every
request budgeted for the worst upstream even when a permissive one served it. `gasleft()` inside
the granted frame is the one gas signal that cannot lie: the node enforces termination with the
same counter it reports.

**The fleet this serves is uniform.** A survey of the downstream consumer (2026-09-01) found ten
lens contracts with eleven array entrypoints, every one a plain per-element loop: no shared work
before or after the loop, no cross-element dependency, all shared state already carried as
constructor immutables. Exactly one call site constructed a `policy`. Nothing needed a lens-side
loop.

TIB 000012's thesis was "stop predicting and let work report how far it got." It applied that to
the loop but left prediction alive in the client's `G(N)`, the lens's per-item estimate and its
return reserve. This work applies it to all three and removes the loop itself, since once each
element is its own frame the loop has nothing lens-specific left in it. Four forcing functions
then shaped the design that stands:

- **Declared sizes and a preallocated slab** were the first envelope's remaining author-supplied
  numbers: `maxItemBytes` / `maxResultBytes` for dynamic types, so the client could project a
  response slab of `n × (out + 32)` touched before item zero. Memory expansion is quadratic, so at
  the wire's ~1,500 elements an eight-word tuple cost ~400k gas before any attempt and a 4 KiB
  dynamic bound ~73M; a `MAX_ALLOC_BYTES` budget, "a fifth of a 10M cap", hid it. The reservation
  existed so skip words at the slab's far end could never collide with results; the ABI offset
  table existed because the response was an ABI tuple. Both dissolve once the wire is ours: the
  response becomes a stream and dynamic elements carry their lengths.
- **The compressed prologue** still decompressed the whole body before the loop. FastLZ turns
  48 KiB of wire into as many bytes as the input repeats, so a 48 KiB request could exhaust any cap
  with zero progress — the last reason the allocation budget and a text match on providers'
  "out of gas" phrasing existed. Making decompression resumable, one element at a time through a
  fixed window, removes the last prologue term that scaled with anything.
- **Bytes-only packing learned the lens's rate only from pages that stopped for gas**, and that
  sample is conditionally heavy: a page stops because its items cost more than its neighbours'.
  Continuations were packed to the worst page seen, light pages revealed only "at least this
  many", and wave 1 always over-packed a multi-page input, paying one round trip by construction.
  The envelope already observed every attempt's gas; reporting aggregates per page makes a page
  that fully served as informative as one that stopped.
- **A count for the opening wave** (`batch.pageSizeHint`) shipped and was withdrawn before any
  release: a count is the quotient of the gas a provider grants, which varies by node and chain,
  and what one attempt costs, which does not, so behind a `failover` or across chains no single
  count is right. Dividing the cap by a per-item cost is the right shape and the wrong numerator:
  the loop can spend the cap less the transaction's intrinsic gas, the envelope's prologue, the
  lens's deploy and the admission reserve, and the deploy dominates — code deposit alone is 200
  gas per byte, so a 20 KB lens spends about 4M gas before its first attempt. The prologue has to
  be a stated figure, and the caller needs a way to read it.

## Design

Within one EVM frame, completed work survives only by returning before the frame dies, so any
single-frame loop must predict the next attempt's cost, and prediction is the estimate. The only
way to delete it is to delete the single-frame assumption: make each attempt its own frame.
EIP-150 — a caller retains 1/64 of its gas across a call, unconditionally — is the protocol-granted
reserve, and it suffices because the work it funds is O(1): a record write and a `REVERT` over
memory already expanded. The frame that hosts the loop is the envelope's own: it already calls the
lens, is already ours in Yul, and already pays the prologue.

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

That is the whole lens. Per element, exactly one of three things happens to the call the envelope
makes: it **returns**, and the result is kept; it **reverts** — any reason, any data — and the index
goes to `skipped`; it **runs out of gas**, and the envelope, holding the retained 1/64, records the
death and stops. The lens's obligations shrink to two the envelope cannot enforce: **skips are
deterministic** (a revert means invalid input or a permanently failing element, never something
more gas would pass) and **values are batching-invariant** (neither a value nor a decline may
depend on position, batch composition, or `gasleft()`).

**Shared work goes in the constructor**: immutables for value types, storage writes for tables the
per-item function then reads at 100 gas a slot once warm. EIP-2929 warmth is per transaction, so
the first element to touch a market's storage warms it for every later element in the chunk. The
constructor runs once per chunk, in the prologue, so its cost must stay bounded and modest. A
target that already has code is refused: the envelope cannot check resident code against
`factoryData`, so the only lens it trusts is the one it watched the factory deploy.

**Per-item reverts are not surfaced.** A revert reason is discarded and the element is skipped,
whether the revert was the author's `require` or a broken oracle three calls down. This is the
price of "one bad element never kills a batch".

### The request wire

The envelope's constructor arguments are viem's four plus a config word:
`(address target, bytes targetData, address factory, bytes factoryData, uint256 config)`.

`config` packs the per-item selector (top 32 bits), the input-dynamic, output-dynamic and
compressed bits at 223, 222 and 221, the input element stride at bit 64 and the output element
stride at bit 0 (static sizes; zero for a dynamic type).

`targetData` is `n ‖ bodyLen ‖ body`. For a static `T` the body is `n` strides, byte-identical to
the ABI array body. For a dynamic `T` it is `n` records `L ‖ E`, `E` the padded ABI tail and `L`
its length, a non-zero multiple of 32. With the compressed bit set the body arrives
FastLZ-compressed and `bodyLen` is its decompressed length, so `n` is readable without
decompressing and every length can be validated against `bodyLen`. Element bytes are identical in
both forms, so cache keys and values are the raw element bytes, and the packer's per-element wire
measure is the stride or `32 + L`.

### The envelope

**Deploy.** If `target` has code, revert `CounterfactualDeployFailed(bytes(""))`. Otherwise
`CALL(factory, factoryData)`. A factory call that failed with empty returndata and left the
envelope drained to about `2/64` of what it had — the factory keeps its own 1/64 of a dying
constructor's frame and hands it back, so the threshold is two retentions — is a deployment
out-of-gas: the envelope reverts `OOG_SENTINEL` (`bytes4(keccak256("ViemDlcOutOfGas()"))`), the
one prologue death it can report. Any other failure, or no code at `target` afterwards, is
`CounterfactualDeployFailed`. Both are thrown by the client, never halved: nothing in the prologue
grows with the chunk, so a smaller chunk cannot cure either.

**Memory.**

```
[args: n ‖ bodyLen ‖ body][F][history: 2·8192 + 320][slab: sentinel | nA | telemetry | records …]
                              ^ compressed only              ^ the only growing region
```

The frame `F`, the state between the input cursor and the output cursor:

| word | field | meaning |
|---|---|---|
| `0x00` | `target` | the lens |
| `0x20` | `n` | elements on the wire |
| `0x40` | `body` | first logical body byte (clear: in the args) |
| `0x60` | `bodyLen` | logical body length |
| `0x80` | `config` | the config word |
| `0xa0` | `ip` | compressed read cursor |
| `0xc0` | `op` | history write cursor |
| `0xe0` | `cur` | next logical byte not yet handed out (clear: a pointer into the body) |
| `0x100` | `ipEnd` | end of the compressed input |
| `0x120` | `consumed` | logical bytes handed out (compressed; clear derives it from `cur`) |
| `0x140` | `i` | the element being attempted, for `MalformedInput` |
| `0x160` | `len` | the static input stride; the attempt's byte length under the static layout |
| `0x180` | `floor` | the static layout's pre-split floor, `apre + 64·cpost [+ dwork]`, computed once |
| `0x1a0` | `selector` | the per-item selector, left-aligned |
| `0x1c0…` | history | compressed only; the slab follows |

The loop reads its invariants from `F` rather than holding them on the stack: the loop body is
close to the stack limit, and standalone Yul has no `via-ir` escape. `memoryguard(0x80)` lets the
optimizer spill to memory under its contract: the program touches only `[0, 0x80)` — scratch the
envelope owns (`0x00` holds the frame's gas on arrival, `0x20` the gas level between attempts,
`0x40` the memory cost of the high-water, and the first two also serve the error paths and the
dynamic-result head check) — and memory at or above the returned pointer.

**The loop.** `P` is one past the last record; attempt `i` is staged at `P + 0x20`, where its
record's bytes will go, so a static result lands on its own arguments and abandoned bytes lie at or
past `P`, outside the reverted prefix. Scratch `0x40` holds the memory cost of the highest byte the
frame has deliberately touched, so every admission prices expansion exactly or conservatively with
one `memcost`.

```
paginate(F, slab):
  mstore(slab, OK_SENTINEL)
  budget := usable(gas());  fixed := arrivalGas − budget         // telemetry header words
  P := slab + 0xc4;  mstore(P, 0);  expansion(P + 0x20)          // seed the high-water's cost
  outLen := outDyn ? 0 : outSize;  strided := !inDyn && !compressed
  for i in 0 ..< n:                                             // post block: account(slab)
    argsLen := strided ? admitStride(F, P, outLen) : admit(F, config, P, outLen, i)
    if argsLen == 0:                                            // refused
      if i == 0 { mstore(P, ~0); nA := 1; revert(slab, P + 0x20 − slab) }   // head, in touched memory
      break                                                     // i > 0: the prefix so far is the page
    g := gas()
    switch staticcall(gas(), target, P + 0x20, argsLen, P + 0x20, outLen)
    case 1:
      static:   malformedResult(i) unless returndatasize() == outLen;  Lout := outLen
      dynamic:  malformedResult(i) if returndatasize() < 0x40 or head ≠ 0x20 or (Lout := size − 0x20) % 32 ≠ 0
                if gas() ≤ expansion(P + 0x20 + Lout) + 3 + 3·words(Lout) + cpost:
                  mstore(P, ~i); P += 0x20; nA := i + 1; break  // unaffordable result: a death
                returndatacopy(P + 0x20, 0x20, Lout)
      mstore(P, (1 << 255) | Lout);  P += 0x20 + Lout
    default:
      if returndatasize() == 0 and gas() ≤ g/64 + 32 { mstore(P, ~i); P += 0x20; nA := i + 1; break }
      mstore(P, i);  P += 0x20                                  // deterministic revert: a decline
  if i == n { exhausted(F, config, n − 1) }                     // every element was staged
  mstore(slab + 4, nA);  revert(slab, P − slab)

admit(F, config, P, outLen, i):                                 // admitStride: the static uncompressed case,
  L, len, floor := locate element i                             //   with L, len and floor read from the frame
  touch := max(len, outLen)
  if gas() ≤ expansion(P + 0x20 + touch) + floor { return 0 }  // `expansion` raises the high-water
  mstore(P + 0x20 + touch − 0x20, 0)                            // the memory is touched before the call
  stage selector ‖ [0x20 ‖] element at P + 0x20;  return len
```

`admit` locates element `i`, prices the attempt and stages it, or returns zero having touched
nothing. Under the uncompressed static layout the attempt's length and floor are the page's, so
`admitStride` reads both from the frame and admission is one expansion price and one comparison;
`strided` is decided once. `expansion(b)` prices the growth from the high-water's cost in scratch
`0x40` to `memcost(b)` and raises it, which is sound because every caller either touches `b` or
leaves the loop. Exhaustion of the body is checked once after the loop, which `i == n` reaches only
when every element was staged (a head refusal reverts on its own); `malformedResult` checks it
first when the offending element is the last, so a codec bug outranks a lens bug.

**Static output is deposited by the call itself** into memory expanded before the split, so nothing
after the call expands memory and no copy is needed. **Dynamic output is admitted after the call**:
its size is unknown before and no bound exists, so the envelope checks, with the retained 1/64,
whether it can afford the expansion and copy for the actual `Lout`. If not, the element is reported
as a death and the client retries it alone, where the whole frame is available. A death recorded
here leaves `returndatasize() > 0`, so no heuristic can misread it.

**Death detection.** Empty revert data *and* `gas() ≤ g/64 + 32`, with `g` sampled immediately
before the call, means the sub-frame died of gas; anything else is a deterministic revert. Whatever
the callee does not burn is refunded on top of the retained 1/64, so only a per-item function that
reverts empty with under 32 gas left in its sub-frame reads as a death — in practice, an actual
out-of-gas.

**The head never produces a corpse.** At `i = 0` below the floor the envelope does not attempt:
attempting without the reserve could kill the frame during the report. It records `~0` without
attempting; the record's meaning, "gas could not resolve this element in this frame", is exactly
as true of a fuse the envelope refused to light as of a sub-frame that died. Every page adjudicates
at least one element.

**Malformed results and inputs are protocol errors, never corpses.** A success whose
`returndatasize` breaks its contract reverts `MalformedResult(index, returndataSize)`: a lens bug,
surfaced as a thrown error rather than halved into a phantom gas failure. A wire the envelope
cannot read — a misaligned or overlong length, a body that does not end where `n` says, a token
past the compressed bytes, a back-reference before the history, bytes left over after the last
record — reverts `MalformedInput(index)`: the client wrote the wire, so this is a codec bug, never
a decline. Both selectors are checked by exact revert-data length.

#### Admission

`memcost(b)` is over byte ends: `w = ⌈b/32⌉`, `3w + ⌊w²/512⌋`. `expansion(b)` is `memcost(b)` less
the high-water's cost when larger, else zero, and raises the high-water.

**Two currencies, one exchange rate.** Gas spent before the call's EIP-150 split and gas needed
after it are not the same money. EIP-150 retains 1/64 of what is available at the call, so a
pre-split cost reaches the reserve divided by 64 and a post-split cost reaches the floor
multiplied by 64. `need = expansion(touch) + apre(len) + 64·cpost [+ dwork(L)]` is that
statement: the pre-split terms are paid once and their error is 1/64 of itself, while `cpost` is
the reserve itself and every unit of it costs 64 at the floor. Consequences: `apre` and `dwork` may
be rough; `cpost` is the whole question; the tail of every page leaves `64·cpost` unused; and only
a callee that returns with nothing left can test `cpost`, because any prompt return refunds more
than the post-call path needs.

| term | currency | what |
|---|---|---|
| `expansion(touch)` | pre-split | memory to this attempt's touch, exact against the high-water |
| `apre(len)` | pre-split | the touch store, the selector and head stores, the staging copy, argument setup, the `gas()` sample, the warm call access: `200 + 3·⌈len/32⌉` |
| `dwork(L)` | pre-split, compressed only | producing and copying out `L` bytes: the worst per-byte token cost, one overshooting token, one rebase and the copy-out: `300·L + 3·⌈L/32⌉ + 9000` |
| `cpost` | post-split | the longest path from the call's return to a valid exit: classification, the head check and deposit calculation for dynamic output, one record store, the telemetry accounting, the exhaustion check after a last-element death, the header patch, `revert` over expanded memory, or the next iteration's admission and its refusal: `1400` |

The values are set in the Yul and guarded by the adversary fixtures in Verification, which are
the fixtures that fail when a constant is too low. `cpost`'s adversary is a callee that returns
with almost nothing left, so the post-call path runs on the retained 1/64 alone; `dwork`'s per-byte
term's is a 6 KiB element encoded as one-byte literals, since a pre-split shortfall reaches the
reserve at 1/64 and only a large element can move it. Every other fixture passes with the constants
far too low, because a prompt return refunds what the post-call path needs. A term without an
adversary is unguarded, whatever the sweeps say.

Proof sketch. After admission the frame holds `> need`. The pre-split spend is `≤ expansion + apre
[+ dwork]`, so the gas available to the call is `> 64·cpost` and EIP-150 retains `> cpost` however
the callee behaves. Every post-call path is within `cpost` and expands nothing, except the dynamic
deposit, which is separately admitted against the actual gas left and whose refusal is itself
within `cpost`. That deposit is the one place the guarantee is met by a check rather than by
construction. The head refusal writes one word the prologue touched; a refusal at `i > 0` writes
nothing.

**Full forwarding.** `staticcall(gas(), …)` forwards everything and lets EIP-150's clamp do the
withholding: 63/64 is the one value on the dial that is not a magic number. It maximizes the
in-page band (an item costing up to ~63/64 of what remains is served in one call) and makes
stop-on-death forced rather than chosen: after a death the envelope holds ~`R/64`, which funds the
O(1) report and nothing else.

#### Streaming decompression

FastLZ (Solady's `LibZip` variant, which `flz.ts` ports) is a byte stream of tokens: a literal run
of 1–32 bytes, or a back-reference of length 3–264 at distance 1–8,192. Decoding is sequential and
each token depends only on the previous 8,192 output bytes. The envelope is a transducer — input
records in, outcome records out, one cursor on each side, `F` the state between them. On the clear
path advancing the input cursor is a pointer add; on the compressed path it is `materialize`, and
nothing else in the loop knows the difference.

The history holds the back-reference window (8,192 bytes), a growth zone of the same size so
rebases amortize, and headroom for the most a token's 32-byte-stride writes can overshoot its
output. Its size is decided by the format, not by the chunk or the node.

```
materialize(F, len, dst):
  while op − cur < len:
    if op + 296 > histEnd:                                    // rebase, flushing first
      mcopy(dst, cur, op − cur);  dst += op − cur;  len −= op − cur
      mcopy(histStart, op − 8192, 8192);  op := histStart + 8192;  cur := op
    malformedInput(i) if the token's bytes exceed ipEnd
    match: malformedInput(i) if distance > op − histStart     // reference before the history
    emit the token; advance ip, op
  mcopy(dst, cur, len);  cur += len
```

A back-reference is copied by doubling `mcopy`: each round copies the whole periodic prefix written
so far, so source and destination never overlap and the rounds are logarithmic in the match
length. The target is a length relative to `cur`, so a rebase, which moves `cur` and `op` together,
cannot leave it stale. A token may cross a record boundary; the overshoot (≤ 263 bytes) stays
pending and is the first thing the next `materialize` hands out. Flushing before a rebase is sound
because everything pending belongs to the record being assembled. The trigger implies
`op ≥ histStart + 16,384`, so the rebase's source and destination are disjoint; after it
`op − histStart = 8,192 ≥` any distance, so the back-reference check only fires in a stream's first
8 KiB.

For a dynamic compressed element the length is in the stream, so `admit` materializes the length
word first under a fixed reserve (`dwork(0x20) + apre(0x24) + 64·cpost`) and then admits the record
against the actual `L`. Elements are copied out of the history into the record slot rather than
called from it, because the history is live: a selector written before a record, or a result
written over one, would corrupt a later back-reference. After the last element the stream must be
exhausted — `ip == ipEnd`, `cur == op`, `consumed == bodyLen` — else `MalformedInput`; a stream that
ended early would otherwise leave the record slot holding bytes the client never sent, which can be
valid ABI the lens answers plausibly and the cache stores under the original element's key.

#### The response

After the sentinel `OK_SENTINEL = bytes4(keccak256("ViemDlcPage3()")) = 0xa55835c3`:

```
nA ‖ budget ‖ fixed ‖ Σg ‖ Σg² ‖ gmax ‖ records
```

- `nA`: attempts adjudicated, ≥ 1. Derivable from the records, but the one redundancy that catches
  a truncated payload before a length-prefixed success record swallows the tail.
- `budget`: gas at the top of the loop less the reserve every admission keeps (`64·cpost`),
  saturating at zero. It is what attempts could spend, so a page that ran out stops with about zero
  of it left, and a prediction against it needs no client-side reserve.
- `fixed`: the frame's gas on arrival less `budget`: the prologue, the deploy and the reserve
  together. `gas()` is read at the envelope's first instruction and kept in scratch `0x00`.
  `fixed + budget` is exactly what the frame arrived with, whether or not the head refused.
- `Σg`, `Σg²`, `gmax`: over the per-attempt gas of every record but a death, where `g` runs from
  the top of an attempt's iteration to just before its accounting, so admission, staging,
  decompression, the call and the record write are all in it. A death is not charged: it consumed
  whatever was left, and the client already knows it died. A head refusal leaves all three zero.
  The three accumulators live in their slab header slots and are updated by `account(slab)` from
  the loop's post block, which a `break` skips.
- Records, in attempt order: success `(1 << 255) | L ‖ L bytes of raw U`, decline `i`, death `~i`
  (always last). Lengths are below 2²⁵⁴, so the top two bits classify a record (`10` success,
  `11` death, `00` decline; `01` is invalid).

The sentinel is the format version: a page in an older format is not recognised and propagates as
an ordinary revert, the only mixed-version behaviour needed. The exit is one `revert` over memory
already expanded; nothing is relocated or copied.

### The client

**Two fragments, one authored.** The lens's real ABI has only `f(T) returns (U)`. The transport
works from the array-shaped fragment `f(T[]) returns (U[] results, uint256[] skipped)`, which is
what the caller's calldata is encoded with and what `policy.abi` carries, and derives the per-item
fragment from it (`itemFragmentOf`), whose selector goes into `config`. `arrayifiedAbi` is the
inverse and the only supported way to produce the array-shaped fragment: it takes the per-item
fragment from the contract's real ABI, so the name and types are the compiler's; it requires exactly
one `view`/`pure` input and one output, appends `[]` to each, and names the outputs `results` and
`skipped`. `itemFragmentOf` removes the terminal `[]` from the sole input and the first output,
preserving tuple components and parameter names. This is the one place a wrong fragment fails
silently — a
per-item selector the lens does not implement makes every call revert and every element a plain
skip — and `pages_all_skipped` is the diagnostic.

```ts
const { results, skipped } = await readLens(client, {
  ...healthLens.with(MORPHO),          // abi, address, factory, factoryData
  functionName: "healthOf",            // narrowed against the lens's real ABI
  args: inputs,
  batch: { batchSize: MAX_INITCODE_SIZE, compress: true },
});
```

`readLens` looks the per-item fragment up in the real ABI, synthesizes the array-shaped one,
encodes the call, attaches the `policy`, invokes `call` with the factory descriptor, and decodes
to `{ results, skipped }` typed from the per-item fragment; only one-parameter, one-value
`view`/`pure` names are accepted as `functionName`. Plain `readContract` with `arrayifiedAbi` and
the same fragment in `policy` remains the escape hatch.

**The decoder is the protocol boundary.** `hexToPage` binds every record to its ordinal: `nA ≥ 1`
and `≤ remaining bytes / 32`, checked before any allocation; exactly `nA` records consuming the
payload exactly; a decline must equal `j`; a death must satisfy `~value == j` and `j == nA − 1`; a
success must have `L == outSize` for a static layout, or `L ≥ 32`, `L % 32 == 0` for a dynamic
one, with `L` bytes present. The telemetry must satisfy the relations any `served` non-negative
samples do: with `served = 0`, all three accumulators zero; otherwise `sum > 0`, `max ≤ sum`,
`sum² ≤ served·sumSquares`, `sumSquares ≤ sum·max` — necessary conditions, not a certificate. The
sum may exceed the budget: the last attempt admitted may spend into the reserve. Anything the
decoder accepts is a well-formed page. The death is consumed inside the request and never reaches
the caller's array.

**Packing and flushes.** Chunks honour the wire cap — `batch.batchSize`, for which EIP-3860's
`MAX_INITCODE_SIZE` is the natural value; without one, chunks are bounded only by what the provider
accepts — and a gas prediction. An element that alone exceeds the wire cap is declined client-side
with no request made. The greedy packer takes the longest prefix that fits, by binary search with a
linear shrink for measures that are not perfectly monotone (the compressed byte measure is one). A
chunk is a list of indices into the caller's array, ascending but not necessarily contiguous.

The prediction. One predicate decides every chunk: `k` elements over the chunk's bytes fit when,
beside the byte cap,

```
k = 1    or    intrinsic(bytes) + fixed + k·avg + z·stddev·√k ≤ cap
```

with `intrinsic` what the node deducts before the envelope's first `gas()` returns — the
transaction and creation base, calldata by byte (EIP-2028), initcode by word (EIP-3860), and that
opcode's own 2 — computed from the chunk's exact bytes: prefix sums of zero and non-zero bytes on
the clear path plus the four wrapper words whose zero bytes depend on the chunk; the wrapped chunk
itself on the compressed path. `z = PACKING_SIGMAS = 2`. A lone element always fits: the estimate
may shorten a chunk but never withhold an element, so the envelope decides what is served.

The parameters have two sources. Before any page has landed they are stated:
`deployless(http(url), { gasLimit })` and `gasLimit` in the `cache` config name the provider's
`eth_call` cap, one per transport instance; `policy({ batch: { gas: { fixed, item: { avg, stddev? }
} } })` states the lens's cost in the units the wide event reports it. With either side missing, or
any figure unusable or malformed, the opening wave packs by bytes alone. From the first page on they
are observed. Every page's telemetry is pooled over the request — `cap` the smallest
`intrinsic + fixed + budget` any page implied, `fixed` the largest, `served`, `sum`, `sumSquares`,
`max` accumulated — and the predicate runs on `cap`, `fixed`, `μ = sum / served` and
`σ² = (served·sumSquares − sum²) / served²`. While no attempt has been costed the stated item cost
fills in against the observed cap, else bytes alone. `PageGas.budget` excludes the reserve and
`fixed` includes it, so `cap − intrinsic − fixed` counts it once. The stated cap therefore sizes
the opening wave and nothing else, and the stated cost nothing after the first costed attempt, by
construction: no later chunk consults them.

Two terms are estimates. `fixed` grows with the chunk's bytes — the prologue copies the wire into
memory at 3 gas per word plus expansion, and behind a compressed body the history sits further out
— by about 1.5k gas at 4 KiB and 15k at the wire cap, so the largest `fixed` seen under-estimates a
far larger candidate by up to that much: half an element at 30k, two at 7k, and only for a chunk
much larger than any page observed. Accepted rather than modelled: modelling it exactly would put
the envelope's memory layout into the client, where a Yul change would skew a hint silently. And
`min cap` with `max fixed` may combine pages that never co-occurred, behind one URL that fronts
several backends: conservative, never over-packing on that account. On the compressed path bytes
and zeros are not monotone in the prefix, so the search yields a fitting prefix rather than
provably the longest, the heuristic the byte cap has always had.

Flushes. The elements a page did not reach are not re-sent per parent. They wait in one pending
list, and every chunk settling re-packs that list, sorted, from the pool as it stands. What is then
sent is `batch.continuations`'s choice:

- `fill`, the default: every chunk the packer builds but the last is full — the next element did
  not fit it — and goes at once. The last goes once no chunk that could still add to it is in
  flight: one of an earlier generation holding more than one element, since a singleton adjudicates
  its whole input and the full pages just dispatched belong to the next round. Small tails from many
  parents coalesce into pages, and a round's remainder never waits on the round it opens.
- `eager`: everything pending goes as soon as a page lands, packed the same way. Each tail travels
  alone or with whatever landed at the same instant; more requests, no waiting.

Anything else reads as `fill`. A death's singleton retry is dispatched directly, never pooled: the
escalation needs the whole frame. The halves of a chunk the provider refused are dispatched the same
way, so each settles on its own. The pump also runs when a page lands without a tail, since its
telemetry can tighten the parameters and make the pending list more than a page. Pages commit as
they land, before siblings finish, so a later chunk failing never discards results that arrived; on
the first failure nothing further is dispatched, the pending list is dropped, chunks in flight
settle and commit, and then the failure surfaces.

The outcome protocol:

| observed | meaning | action |
|---|---|---|
| page, no death | complete, or stopped at the floor | commit; the tail joins the pending list |
| page, death at `k`, chunk count > 1 | `k` could not be resolved in this frame | commit the prefix; retry `k` alone at once — minimal prologue, the strongest grant this client can construct; the tail behind it joins the pending list |
| page, death, chunk count == 1 | the element died, or was refused at the head, holding a singleton's grant | terminal: a plain `skipped` entry, counted in `elements_unresolved` |
| size error (HTTP 413, "too large", initcode size) | the provider refused the request's size | halve; a singleton propagates |
| timeout | possibly batch-induced, possibly a slow upstream | halve once per chunk while it holds more than one element, then propagate |
| `OOG_SENTINEL` | the factory or the lens constructor ran out of gas under this node's cap | thrown; a smaller chunk cannot cure it |
| `CounterfactualDeployFailed` | target occupied, constructor reverted, or no code at `target` | thrown |
| `MalformedResult` | a lens result that does not fit its declared layout | thrown |
| `MalformedInput` | the envelope rejected the wire | thrown (codec bug) |
| any other error | unknown | propagated; `failover` is the recovery |

Escalation is a two-step ladder bounded per element: one in-chunk adjudication and one singleton.
Every page adjudicates at least one element, a singleton death is terminal, halving terminates
classically, and every run of the pump either dispatches, leaves the pending list behind a chunk
still in flight, or settles the request; an element that fits no chunk alone — possible on the
compressed path, where it may have fit only beside its neighbours' history — is declined as
oversize rather than retried. So the request terminates without counters.

**Observability.** On every request that reached the packer, under the transport's facet:

| field | what it answers |
|---|---|
| `elements_requested`, `elements_fetched`, `nominal_batches`, `batch_bytes` (stat) | the initial packing and its utilisation against the wire cap |
| `gas_limit` | the cap the opening prediction used, when it applied |
| `pages_continued`, `page_adjudicated` (stat) | how often the lens stopped short, and whether it yields ~1 element per page |
| `flushes`, `flushes_full`, `flushes_drain`, `flushes_eager` | the requests pooled tails were re-packed into, by what released each |
| `continuation_depth_max`, `continuations` | the longest chain of pages behind pages, and the mode |
| `pages_all_skipped` | did every element of a page revert — a selector the lens does not implement is one cause |
| `attempts_unresolved`, `pages_escalated` | how often gas failed to resolve an element, and the singleton round trips that cost |
| `splits_count`, `splits_size`, `splits_timeout`, `splits_max_depth` | provider refusals; any non-zero depth is pathology |
| `elements_missing`, `elements_declined_oversize`, `elements_unresolved` | the caller-facing `skipped`, and its client-side and gas-terminal subsets |
| `frame_gas`, `fixed_gas` | the smallest budget and the largest prologue any page reported |
| `item_gas_avg`, `item_gas_stddev`, `item_gas_max` | the pooled per-attempt cost |
| `gas_limit_observed` | the smallest `intrinsic + fixed + budget`: the cap the provider actually granted |

The cache handler counts `elements_requested` and `elements_fetched` over deduplicated misses and
restamps `elements_missing`, `elements_unresolved` and `elements_declined_oversize` after rebasing,
so those match the array the caller receives; a full cache hit or an empty input never reaches the
packer and carries none of these. The recipe for the opening wave: run under observability, take `fixed_gas`,
`item_gas_avg` and `item_gas_stddev` for the policy and `gas_limit_observed` for each transport. A
`gas_limit` above `gas_limit_observed` is a cap the provider has since lowered.

**Transports.** `deployless(http(url), { gasLimit? })` intercepts only calls carrying the
`policy(...)` sentinel in `stateOverride`. `cache(http(url), [{ binSize, store, invalidationStrategy,
gasLimit? }, …])` does the same and keys element bytes by the deployless target (`targetTo`,
`factory`, `factoryData`), the array-shaped selector, the element, and the rest of the `eth_call`
parameters (block reference, remaining state overrides, block overrides), under a blob keyed by
chain and `policy.cache.blobKey`. Each documents `gasLimit` itself and exposes it on its value.
Behind `failover`, each branch states its own.

### Accounting

- Envelope initcode: 2,618 bytes, under 6% of the 49,152-byte cap.
- Per element on a trivial lens (`test/forge/Gas.t.sol`, `page(110) − page(10)`, which includes
  the fixture lens's own 969 gas and the 100-gas warm `STATICCALL`): about 1,890 gas clear and
  4,035 compressed, so the envelope's own work — admission, staging, the record write and the
  telemetry — is about 820 gas per element on the clear path; the snapshot lines are 1,318,165 and
  1,587,377 per hundred. The downstream fleet's lenses, by their previously fitted per-element
  coefficients, cost 7k–100k per element, so the overhead is 1–12% on the clear path.
- Constants: `apre(argsLen) = 200 + 3·⌈argsLen/32⌉`, `cpost = 1400`, `dwork(L) = 300·L +
  3·⌈L/32⌉ + 9000`, history `2·8192 + 320`, death slack 32, deploy-death threshold `gasBefore/32`,
  `PACKING_SIGMAS = 2` (a policy target, not a measurement). The suites run the adversaries at the
  shipped values; the calibration that set them is in Derivation.
- Intrinsic gas: `21000 + 32000 + 4·zeros + 16·nonzeros + 2·⌈bytes/32⌉ + 2`, Ethereum's schedule.

## Scope & files

- `src/utils/deployless/Envelope.yul`: `deploy`, `paginate`, `admit`, `admitStride`, `exhausted`,
  `materialize`, `account`, `expansion`, `pageFloor`, the floor constants, the error selectors.
  `pnpm build:Envelope` prints the constant.
- `src/utils/deployless/codec.envelope.ts`: the pasted constant, `OK_SENTINEL`, `OOG_SENTINEL`, the
  error selectors and their exact-length detectors, `envelopeConfig`, wrap/unwrap with the
  `n ‖ bodyLen ‖ body` framing and the compression bit, the `cause`-chain revert-data walk.
- `src/utils/deployless/codec.inner.ts`: `arrayifiedAbi`, `itemFragmentOf`, `resolveArrayFunction`,
  `arrayToWire` / `wireToArray`, `hexToPage` / `pageToWire`, `pageToHex`.
- `src/utils/deployless/call.ts`: `factorisedFactoryCall` — packing, the prediction and its two
  parameter sources, the pool, the pending list and its two modes, escalation, halving, the fields;
  `LensGas`, `ContinuationMode`.
- `src/utils/deployless/flz.ts`: the client-side FastLZ codec.
- `src/transports/deployless/index.ts`, `src/transports/cache/{index,types}.ts`,
  `src/transports/cache/eth-call/handler.ts`, `src/transports/state-overrides.ts`: the transports,
  `gasLimit`, the policy type, the handler's dedup and restamping.
- `src/actions/call.ts` (`policy`, `MAX_INITCODE_SIZE`), `src/actions/read-lens.ts`.
- `test/forge`: `Fixtures.sol` (the lenses, the wire builders, the page decoder, `Env.build` with
  its drift guard), `Envelope.t.sol`, `Gas.t.sol` and `.gas-snapshot`; `flz-compress.ts` compresses
  fixtures via ffi.
- `test/utils/deployless`, `test/transports`, `test/helpers/page.ts`: the codec, transport and
  handler suites and the telemetry mock helpers.
- `README.md`: "Paginated lenses", the `policy` reference, the transports, observability.

Deliberately unchanged from TIB 000012: the caller-facing response shape and semantics (a partial
result is a successful response; `skipped` merges declines, client-side size declines and
gas-terminal elements), the cache's keys and entries, the handler's rebasing machinery, and
timeout classification with its cautious split budget.

## Verification

**Foundry** (`test/forge`, `pnpm test:forge`, 64 tests, no forge-std). `Env.build` compiles the
Yul through the package's own script and fails if the pasted constant has drifted.

- Layouts: one case per cell of the input × output matrix, static and dynamic, clear and
  compressed; a dense static page's revert length is exactly its header plus `nA·(32 + outSize)`;
  a 32-word result at the largest `n` the wire admits pages under 10M.
- The stream: interleaved declines with a mid-page death; a head death; a revert with data followed
  by a success; short and bad-head dynamic returns as `MalformedResult`; a dynamic result the frame
  cannot afford as a death that the same element alone survives.
- The wire: body-length mismatch, misaligned, overlong and zero lengths, trailing and truncated
  records, each `MalformedInput(i)`.
- Compression: 50,000 identical 64-byte elements under a 2M grant page (the pre-streaming envelope
  was a corpse at every grant); rebases inside both `materialize`s of one dynamic element and
  across back-references that straddle one; the one-byte-literal and distance-one witness streams
  decode; tokens past `ipEnd`, exhaustion mid-record, back-references before the history, trailing
  tokens, overshoot past the body, each `MalformedInput`.
- Telemetry: the per-attempt mean tracks the frame's marginal cost within 10%; declines are
  charged; a head death and a head refusal charge nothing, and the refusal's budget saturates
  rather than wraps; a page that dies at 4 charges within 1% of a page over its first four; a
  compressed page charges more than the clear one; `fixed` exceeds the code deposit, is within 1%
  between a 3- and a 110-element page, and `fixed + budget` is under the grant.
- Deploy: an occupied target is refused; a drained deploy is `OOG_SENTINEL`.
- Sweeps and adversaries: gas sweeps in both modes with no corpse above the first served page;
  boundary sweeps in ±1,500-gas windows around each grant that adjudicates one more element, for
  every post-call path (empty death, empty revert, revert with data, static success and malformed,
  dynamic success, short and bad-head, the head refusal, compressed singletons, literal,
  distance-one and bomb streams); the adversaries that pin `cpost` (drained callee, clear,
  compressed and dynamic) and `dwork` (a 6 KiB one-byte-literal element).
- `Gas.t.sol` against `.gas-snapshot` in CI (`forge snapshot --check`), so a slower path that
  crosses no floor is a diff in review.

**Vitest** (`pnpm test`, 601 tests). The codec: `arrayifiedAbi` round-trips for static tuples,
nested arrays and dynamic types; the record decoder accepts every kind and rejects a short header,
`nA` the payload cannot hold, unbound ordinals, a non-final or repeated death, the unused namespace,
wrong-size and misaligned results, trailing bytes, and each inconsistent telemetry tuple;
`pageToWire ∘ hexToPage` round-trips. The transports and handler: one case per outcome row; a
mid-chunk death retried exactly once alone; a singleton death terminal and surfaced as a plain
skip; a previous-format page propagated as an ordinary revert; the four thrown selectors never
halved; continuations packed at the reported rate rather than the parent's count and from every
page that has landed, whichever settled first; a continuation bounded by the smallest cap and the
largest prologue seen; the stated item cost filling in while nothing has been served; a wide spread
packed more conservatively than a flat one; never below one element; the opening wave sized from
the cap and the cost, with the fixed cost, the spread and the intrinsic gas each reducing it (the
last to within one gas of the request actually sent), a cost above the cap sending every element
alone, cold-start recovery on a three-per-page lens, degradation when the cost is understated, and
bytes-only packing when either side is missing or any figure is malformed; small tails coalesced
into one chunk; a full page sent while siblings are in flight and a partial one held for the drain;
fullness re-checked when telemetry lands without a tail; a round's remainder sent beside its full
pages once nothing earlier is open; `eager` against `fill`, and an unknown mode
read as `fill`; a coalesced chunk's declines, death and tail mapped to the caller's indices; the
continuation depth counted; an input of only oversize elements settling without a request; nothing
dispatched after a failure, not even a death's escalation, and chunks in flight committed before it
surfaces; each half of a refused chunk settling on its own, and a tail waiting on the other half
under `fill`; the fields stamped,
`gas_limit_observed` reconstructing the mock's cap; dedup restamping through the cache handler.

**Real node** (performed against a live chain during the work, not in CI): a lens end-to-end
through `readLens`, including a replicated input large enough to force several pages, with every
element accounted for and no corpse.

`pnpm typecheck`, `pnpm exec biome check .`.

## Open risks

- **EVM repricing erodes margins.** `apre`, `cpost`, `dwork`, the death slack and the 1/64
  retention are fee-schedule-derived; a repricing or an L2 with a nonstandard schedule can shrink
  them. All live in the envelope; re-verify the adversaries on hard forks of target chains.
- **Floor terms are compiled-path values.** The adversaries pin them; an optimizer change that
  lengthens a path fails the adversary rather than shipping, and one that merely slows a path shows
  in the snapshot.
- **Provider revert-data truncation.** The response rides in revert data. `nA` makes every
  truncation detectable, including one on a record boundary; it is a transport failure, never
  something to bisect.
- **Dynamic response size is gas-bounded, not byte-bounded.** One dynamic result can be as large as
  the frame can copy, so a page's revert data is not derivable from its input bytes.
- **Double execution of an unaffordable dynamic result**, bounded to one per page and to frames
  within `cpost` plus the copy of exhaustion. The singleton retry gives the element the whole frame
  but is not a guarantee; a result larger than a frame can hold lands in `elements_unresolved`.
- **Response size.** A 32-byte record header per success doubles the payload for one-word results
  (+12% for an eight-word tuple). Accepted for the O(1) exit and the absence of any collision
  argument.
- **Doomed burn at full forward.** A genuinely unbounded element burns ~63/64 of the frame per
  attempt, at most twice, with zero yield: node compute and timeout exposure. A lower forwarding
  ratio would bound it at the price of a magic constant and a narrower in-page band.
- **Death detection is a heuristic.** A per-item function that reverts empty with under 32 gas left
  in its sub-frame reads as a gas death, escalates once and lands in `skipped`.
- **Terminality is route-sampled.** A singleton death is terminal on the route that served it;
  under non-deterministic caps another route might have served the element. Visible in
  `elements_unresolved`.
- **Constructor work is prologue work.** A lens constructor that alone exceeds a node's cap is a
  thrown `OOG_SENTINEL` on every chunk; one that merely does a lot narrows every page. Nothing
  enforces "bounded and modest".
- **A wrong per-item fragment fails as all-skips.** `arrayifiedAbi`'s type-checked source and
  `pages_all_skipped` are the guards.
- **Per-item revert reasons are lost.** A broken oracle and a deliberate decline look the same to
  the caller; reversible at the envelope with a one-word protocol change if a lens ever needs it.
- **Cancun required.** `mcopy` throughout; a pre-Cancun chain fails the first call visibly.
- **Yul stack depth.** The loop body is near the 16-slot reach; adding a local in `paginate`, even
  in a scoped block, has produced StackTooDeep, and a helper function is the fallback.
- **Per-token decompression cost.** `dwork`'s per-byte term is a source-level bound pinned by the
  witness streams; too low would let a nearly exhausted frame die in `materialize`. The reserve
  charges the worst per-byte cost for every byte of `L`, so a highly compressible large element
  reserves far more than producing it costs and a compression bomb's elements are refused at grants
  that could serve them; `min(per-byte·L, per-input-byte·remaining)` would tighten it by orders of
  magnitude and needs its own witness.
- **Per-element overhead.** About 820 gas of the envelope's own work per element on the clear path
  and ~3,000 compressed, where `materialize` costs roughly 21 gas per decompressed byte because the
  optimizer inlines it among the loop's live variables; `account` (~115 gas) is the largest single
  remaining item and is all real work. A leaner compressed path needs a cursor-on-stack loop or a
  word-aligned token format; cheaper telemetry needs different words. Performance questions, not
  correctness ones.
- **Composition.** `μ` depends on which items share a frame: warm storage makes related items
  cheaper together, so a grouped input reads cheaper than a shuffled one. The caller orders `args`;
  `readLens` aligns results to any order. A coalesced chunk mixes parents and shares less than the
  pages `μ` was measured on, so it costs a little more per element than predicted; the consequence
  is one continuation for that flush, whose own page pulls `μ` up.
- **`fixed` is read off the largest page seen.** A chunk far larger than any page observed pays up
  to ~15k gas more prologue than predicted, the growth of the wire copy with bytes; one continuation.
- **Route.** `budget` varies by provider; the pool takes the minimum over the request, so a request
  served by several nodes behind one URL packs to the smallest.
- **The death is still censored.** Its cost is unknown by definition, and the only element whose
  cost the pool never sees is the one that mattered most on that page.
- **`z` is a tuned constant, and `√k` assumes uncorrelated costs.** Under that assumption Cantelli
  bounds overshoot at `1/(1+z²)` per chunk; warm storage and ordering correlate costs, so the figure
  is a target the operator can read against `pages_continued`, not a bound.
- **`Σg²` is unbounded by the envelope.** `g` is below `2⁶⁴` on any node and `n` is bounded by
  initcode, which keeps it far under `2²⁵⁶`, but nothing checks.
- **The intrinsic schedule is Ethereum's.** A chain that prices calldata or initcode differently
  shifts `gas_limit_observed` by the difference; pasting it back cancels the shift except as chunk
  size varies.
- **`fixed` includes the reserve**, a Yul constant, so it moves when `cpost` does; the caller sees
  the move as a changed `fixed_gas`.
- **The compressed path's intrinsic gas is not monotone in `k`**: a longer prefix can compress to
  fewer or more zero bytes. Each element adds at least `avg`, so the total only fails to be
  monotone for an `avg` below a wobble of a few dozen gas, which the packer's linear shrink
  tolerates.
- **Thrown envelope errors replay across `failover` branches.** The four are plain `Error`s
  wrapping viem's revert, and the failover's default classifier inspects only the outer error, so
  each is retried against every provider before surfacing. For the deploy out-of-gas that is
  desirable — another cap may fit — but `MalformedResult`, `MalformedInput` and
  `CounterfactualDeployFailed` are deterministic. Low impact; in the backlog.

## Notes

**Why the envelope hosts the loop.** Once each element is its own frame, the loop contains nothing
lens-specific: it reads elements at fixed offsets, stages a selector and a slice, calls, classifies,
deposits. Hosting it in the lens forced every lens to carry a wrapper whose parameters and returns
were all unused and to import a library the client already had all the information for. Hosting it
in the envelope costs one call per element either way, a few KB of initcode against a 48 KB cap,
and buys a lens that is one function and a "compliant by construction" guarantee that deletes the
devolved case.

**Why two fragments, and why the policy keeps the array-shaped one.** The caller's calldata has to
be encoded against something viem can type, and the transport has to slice an array out of it; the
array-shaped fragment is that something, and keeping it as `policy.abi` leaves the client codec and
every cache key untouched. The per-item fragment is what the chain needs and what the author wrote,
so it is the source and the array-shaped one is derived, never the reverse: a derived per-item
fragment is only as right as the string it came from, whereas `getAbiItem` against the contract's
real ABI is checked by the compiler.

**Why any revert is a skip.** The envelope sees `ok = 0` and some returndata; it can distinguish
empty from non-empty, and nothing else about intent. Making revert-with-data a page failure would
give authors a channel at the cost of one broken downstream call killing the whole chunk. With the
fleet treating `skipped` as "unknown, re-read if you care", a skipped element is the safer default.

**Why full forwarding, precisely.** Correctness only requires the retained fraction to fund the
O(1) cleanup, which any ratio satisfies, so the ratio is a policy dial trading in-page band width
against doomed burn on unbounded items. `gas()` with the EIP-150 clamp is the one non-arbitrary
point on that dial, maximizes the band, and makes stop-on-death forced rather than chosen.

**Why the floor is per attempt, and why it is an inequality.** A flat floor against a preallocated
slab demanded declared sizes; pricing each attempt from the memory it touches needs none, and the
exit stays O(1) because `revert` charges nothing for already-expanded memory and nothing is
relocated. EIP-150 splits gas at the call site, after the call's own costs, and `⌊(R − B)/64⌋` at `R`
barely above `64·cpost` rounds below `cpost`, so the pre-split terms are added and the comparison is
strict. The claim is a margin-bearing inequality probed at the boundary; only the shape of the
argument is structural.

**Why a stream and not a bitmap in consumed bytes.** A bitmap is denser but needs an exit
relocation, a per-attempt pre-touch that includes it, and a proof that it never overtakes what the
decompressor still reads. The stream needs none of those: exit is one `revert`, nothing is
relocated, and no region is shared. Legibility of the envelope is worth more than the payload
bytes.

**Why `~i`.** `-i` cannot tag index 0, and the head is the tag's most important case. An
offset-by-one scheme could, but it would renumber every untagged entry. `~i` leaves declines
byte-identical, collision is structural (real indices are bounded by the byte caps, tagged values
descend from `2²⁵⁶ − 1`), and the death is legal at exactly one position, the last record, so no
tagged value ever enters the plain `number[]` world.

**Why at most one death, always last.** The envelope stops at the first death: it retains only
~`R/64`, enough to report, not to continue. The decoder turns that structural fact into a checkable
rule.

**Why the head refusal records `~0` without attempting.** Attempting without the reserve could kill
the frame during the report; refusing produces the same protocol-valid page as a head death, and
`([], [])` stays unreachable.

**Why stage in the record slot rather than in place.** The selector could be patched over the four
dead bytes preceding each element, saving the staging copy (~35 gas per element). That needs a proof
per layout that those bytes are dead, a masked store, a `0x20` head inside every dynamic wire
record, and a locate that cannot be shared with the compressed path, which must not patch its
history. One `paginate` is worth the copy.

**Why static output needs no post-call admission, and dynamic output is admitted after rather than
bounded before.** A static size is known before the call, so its expansion is paid at admission,
before the split, and the call deposits directly into the record. A dynamic result could be
pre-touched to a declared bound and never discarded, at the cost of an author-declared number
enforced on every fresh and cached result whose sole purpose is to make a memory projection sound.
The post-call check trades that for one possible double execution at the edge of a frame; the
intent ranks "no numbers from the author" above that corner.

**Why `MalformedInput` is a protocol error and not a decline.** A misaligned or overlong length is
the one path to an unbounded memory access, and the client wrote the wire. A decline would hide a
codec bug as a skipped element.

**Why one envelope.** In `eth_call` no calldata is billed and initcode meters at 2 gas per word,
so keeping a smaller uncompressed variant buys nothing; the loop is written once and compression is
a branch in `admit`, with one constant and one drift guard.

**Why the static uncompressed layout has its own admission.** Its attempt length and floor are the
page's, so `admitStride` reads both from the frame and skips the layout branches; measured, the
shipped loop is within 1% of a fully layout-specialised one, so no further duplication is worth its
bytes.

**Why the high-water is kept as a cost, and touched before the call.** Holding `memcost(hw)` in
scratch makes each admission one `memcost` rather than two, and keeps `hw` off a full stack.
Touching the attempt's memory before the call, though the call would expand it pre-split anyway,
keeps the death heuristic's gas sample after all memory work: otherwise a static out range wider
than the arguments would loosen `gas() ≤ g/64 + 32` by its expansion over 64.

**Why a fixed history and not a record-sized ring.** A ring sized by the chunk's largest record
puts an element-sized expansion back into the prologue and makes every attempt reserve for the
chunk's worst element. Separating "bytes future tokens may reference" (always 8,192, a format
constant) from "bytes this call needs" (the current element, admitted in the record slot) makes the
prologue fixed and the admission local. The growth zone halves the rebase count for ~8 KiB of
prologue memory once.

**Why two stages for a dynamic compressed element.** The record's admission needs `L`, and `L` is
in the stream. Producing the length word first under a small fixed reserve, and admitting the
record against the actual `L`, keeps the reserve proportional to the element rather than the chunk.

**Why a back-reference before the history is a protocol error.** Solady's compressor never emits
one; at the history's start `op − distance` would otherwise point below the arena, an address whose
expansion is an instant corpse. One comparison per match token closes the only such path.

**Why the deploy death is thrown, not halved.** Nothing in the prologue grows with the chunk: the
initcode is fixed, the args copy is bounded by EIP-3860, the history is a format constant. A
constructor that does not fit a node's cap does not fit it at any chunk size, so halving would only
re-execute it down to singletons at bisection prices.

**Why halving is kept at all.** It is the only mechanism that makes progress against a provider
that refused the request itself — a size limit or a timeout — where no page exists to learn from.
It is never used for gas.

**Why aggregates, not per-item costs.** The tail's items have not run, so per-item costs of the
served items would only be averaged anyway. Five words per page cost the same at any `n`; a median
or histogram would be selection in Yul or sixteen words for quantiles that `σ` and `max`
approximate.

**Why the header slots are the accumulators, and the gas level lives in scratch.** No frame word
is added, the history offset does not move, and the exit writes nothing extra. Scratch `0x20` for
the gas level keeps a variable off a stack the loop already fills; the variant with it on the stack
compiled 50 bytes larger and cost more per element on both paths.

**Why the reserve is subtracted in the envelope**, so that `64·cpost` stays in one place. A client
that subtracted it would carry a Yul constant. **Why the sum may exceed the budget**: admission
only requires the reserve to be present before the call, not after; rejecting `sum > budget` would
reject every page whose last callee drained its frame.

**Why one predicate with two parameter sources.** With tails re-sent per parent, a tail is a
sub-range of a chunk that fit, every term is monotone in the range, and a stated-only predicate for
the opening wave beside a count from the pool for continuations sufficed. Once tails coalesce, a
continuation can be larger than any chunk before it: a stated predicate on it would let an
overstated cost cap every flush, and a count from the pool would ignore that a larger chunk pays
more intrinsic gas — between a 4 KiB page and a 48 KiB chunk, 180k–720k depending on zero content,
six to twenty-four elements at 30k each. One predicate on the observed cap less the candidate's own
intrinsic gas fixes both, and makes "no chunk consults a stated figure once the pages have replaced
it" true by construction rather than by the sub-range argument.

**Why the pending list is re-packed on every settle.** Packing each tail at its parent's
completion would pack two tails of the same lens to different estimates, from whatever prefix of
the pool had landed. Re-packing everything pending from the whole pool means the estimate a chunk
goes out under is always the freshest, and a page whose telemetry tightens the pool can promote
what was waiting to a full page.

**Why a mode, not a timer.** A coalescing window in milliseconds would be the one latency constant
in a system whose every other figure is measured or stated by the caller, and no value suits both a
local node and a rate-limited public endpoint. `fill` and `eager` are the two ends the timer would
interpolate between, named. Singletons bypass the list because the escalation is about the frame,
not the packing.

**Why `z = 2`.** An overshoot costs one continuation, packed from more data; an undershoot costs
extra parallel requests. Under uncorrelated costs one sigma leaves a coin flip and two leaves one in
five, and the `√k` scaling makes the margin a few percent of a large chunk. Mean-only packing
overshoots about half the time on heterogeneous input, which is the case the prediction exists for.

**Why the cap lives on the transport and the cost on the policy.** The policy travels with the
request across every `failover` branch, so a per-provider figure cannot ride on it; the lens's
cost does not vary by provider, so one value serves every branch and chain.

**Why `fixed` rather than the arrival gas.** It is the knob the caller sets; the arrival gas is
recovered as `fixed + budget` whenever the client wants it.

**Why the stated deviation rather than the maximum.** The maximum errs low without a second
figure, but on a heterogeneous lens it under-packs by the ratio of the maximum to the mean, trading
one wave for many parallel requests. The mean and deviation give the opening wave exactly the
headroom continuations get, from one constant.

**Why the intrinsic gas is computed exactly.** It is cheap, the bytes are known, and it is the one
term whose approximation would bias every opening chunk the same way. It includes the 2 gas of the
`gas()` that samples the arrival, so `gas_limit_observed` is the cap itself.

**Why a singleton is never refused by the prediction.** An estimate that could withhold an element
would be load-bearing; the byte cap alone can, and that is a wire limit rather than a guess.

**Why the sentinel is the version.** A version word would cost 32 bytes and a check; changing the
sentinel costs nothing and reuses the existing "not our payload" path.

**Why "paginated."** A lens is not itself a page; it is read in pages. The adjective for the
mechanism is *paginated* and is the word exported; nouns keep *page*.

## Derivation

The path, in five phases on one baseline.

**Frame per attempt** (2026-08-31 to 09-01). The client-side `G(N)` first migrated into the lens as
a `gasleft()`-derived bound plus the existing stopping rule; then to a frame per attempt with full
forwarding, a guard inequality and dead-frame forensics (head-peel, a request-wide gate,
evidence-based demotion, `invalid()` at the head). Two adversarial review rounds punctured the guard
margins and the forensics; a clean-room exercise given the intent but not the design converged
independently on frame-per-attempt with a shared runner, a ½ forwarding ratio, slab-deposited
results and in-band failure reporting. The synthesis kept full forwarding, the slab and in-band
sign-tagged reporting; a third round found the envelope's own copy reserve, the unguarded head
(hence the refusal-without-attempt) and the slab layout. It was implemented first as a bundled
Solidity library (`PagedLens.sol`, Foundry-verified), which surfaced that the envelope needs no
copy at all, that the death blind spot is "under 32 gas" not "98.4%", and that the loop needed a
two-function split for stack depth. The maintainer's ergonomics objection — a wrapper stub whose
inputs and outputs move "magically" is not Solidity — and the fleet survey moved the loop into the
envelope, made the lens one function, derived the array-shaped fragment and added `readLens`. The
first envelope reserved a response slab from declared byte bounds, relocated skip words at exit,
kept two Yul files, and halved on provider "out of gas" text under a `MAX_ALLOC_BYTES` budget.

**Outcome stream** (2026-09-02). Two independent plans (a Claude instance and GPT-5.6 Sol) from a
shared brief converged on length-prefixed dynamic wire in both directions, exact memory-delta
admission, deleting the budget and the corpse class, and a new protocol error for a bad length.
They diverged on skip encoding (bitmap in consumed bytes vs. stream — stream chosen), on dynamic
output (pre-split touch to a bound vs. post-call admission — post-call, so the bound can go), on a
version word (folded into the sentinel), and on floor derivation (audited opcode counts vs.
boundary tests — boundary tests, but covering every post-call path). The bitmap was designed in
full before the stream was chosen. Review found death records
that did not advance `P`, a head refusal into untouched memory, an unsigned-subtraction `E`, the
head check before the size check, and a decoder that trusted record order; it moved staging into
the record slot for both paths and merged the two envelopes, which had existed to keep the
uncompressed initcode small at the price of every fix landing twice.

**Streaming decompression** (2026-09-03). Review of the same plans replaced a record-sized ring
with the fixed history, moved the pump target to a length relative to `cur`, added the token and
exhaustion checks, and dropped a chunk-wide maximum record length from the config word. The
framing that stuck: two gas currencies with exchange rate 64; constants pinned by adversary fixtures
and sweeps that are only no-corpse fuzzing; the envelope as a transducer with one cursor per side.
`MAX_ALLOC_BYTES`, the allocation measure, the corpse class and the text match were deleted;
`OOG_SENTINEL` became a thrown diagnostic.

**Page telemetry** (2026-09-03). Planning wandered from "look into the deferred items hint" to a
protocol change once the censoring argument surfaced: a remembered rate across requests, designed
first, could only learn upward by overshooting under count-only pages. Review caught that
predictions must use the usable budget (raw frame gas over-packs every continuation by `64·cpost/μ`)
and that tails must wait for the wave's pool. `cpost` rose from 1,200 to 1,400 for the post-call
accounting: the drained-callee adversary failed at 1,100 and passed at 1,200 when swept during
calibration, and 1,400 carries the margin.

**Gas** (2026-09-04). An `anvil --steps-tracing` trace with a program-counter to Yul-offset map
attributed the clear path's ~1,456 gas of envelope work per element: about 350 was control flow
from Yul function boundaries and layout branches, admission computed `memcost` twice, staging
re-checked exhaustion every element, and the compressed branch's inlined `materialize` taxed the
clear path through register spills. Fusing `prepare` and `stage` into `admit`, specialising the
static uncompressed layout, caching the high-water's cost, hoisting per-page values into the frame,
and moving exhaustion after the loop brought it to ~820. Optimizer flags moved nothing on the final
code (`--optimize-runs` is inert; FullInliner trades the clear path against the compressed one).
Sweeping `cpost` against the adversaries put the cliff in (700, 1000]; 1,400 was kept for margin.
Review of the change found two regressions, fixed: a malformed result on the last element masked
trailing wire bytes, and the death heuristic's sample preceded the call's out-range expansion.

**The opening wave** (2026-09-04). A count hint (`batch.pageSizeHint`) shipped and was withdrawn as
provider-dependent. Dividing the cap by a stated per-item cost alone was declined for the numerator;
reporting the deploy's gas alone was declined because the prologue and the reserve are small next to
the deploy but not next to one element; pasting the maximum as the item cost was declined for
heterogeneous lenses. The envelope gained the `fixed` word; review made a lone element always fit
the prediction, the clear path's intrinsic count the four chunk-dependent wrapper words, and a null
`batch.gas` degrade rather than throw.

**Coalesced continuations** (2026-09-04). Sending tails as parents settle, and coalescing them,
showed that the single `fits` predicate reused for tails rested on tails being sub-ranges: a
coalesced chunk can exceed any opening chunk, so the stated figures would bind again and the pool's
count would ignore the candidate's intrinsic gas. The predicate was unified and chunks became index
lists first; the pending list and its two modes followed. Review of the plan found the pump's
termination on empty and only-oversize input and its failure semantics unstated, the "exact"
prediction an estimate where `fixed` grows with bytes, and `page_size_suggested` a count whose
meaning would change; the first two were specified, the estimate accepted and documented, the facet
dropped. A coalescing timer was discussed and replaced by the mode. Review of the code found a
death's escalation could be dispatched after a failure and that halves settled as a pair rather
than as chunks; both now go through the same dispatch as every other chunk, and `settleAll` went.
The first draft held a remainder until nothing at all was in flight, which cost one round trip per
round against the waves; the maintainer's intent was to wait only on the previous round, so the
drain now counts open chunks by generation.

Declined along the way:

- **The ½ forwarding ratio**: once cleanup is O(1) any ratio is correct, so it is pure policy, and ½
  halves the in-page band for a population the fleet does not have.
- **The lens-side library** (`PagedLens.sol`): everything it proved carried over as a Yul port;
  what it cost — an import, a wrapper stub, a shipped `.sol`, a "lens not using the library" corpse
  class — did not. **An empty array-function declaration** kept in Solidity for viem's types, and
  **inferring strides at runtime**: moot once the client packs strides into `config`.
- **Revert-with-data as a page failure**; **a third ABI output or a caller-facing tag** for the
  death; **`-i`** and **offset-by-one** tags; **a `{index, tagged}` public representation**;
  **head-of-fresh-chunk as the escalation target**; **the second draft's forensics**; **continuing
  the page after a death** on the retained gas; **a gas-estimate tier**; **enforced naming**;
  **adaptive measurement as the primary stop**; **geometric sub-chunk doubling**; **truncating
  oversized results**; **checkpointing via storage** from a `staticcall`; **a client-side probe
  call** before packing; **a `pages_floor_stopped` facet** (a duplicate of `pages_continued`); **a
  singleton retry budget** for route variance (a pathological corner, stamped instead).
- **Keeping ABI on the wire for dynamic `U`** (its offset table is the O(`n`) touch being
  removed); **skip words in consumed decompressed input** (unsound under back-references without a
  lookahead); **`maxResultBytes` as an optional pre-touch bound**; **a `PAGE_V1` word**; **auditing
  opcode counts** from the disassembly.
- **Dropping compression** instead of streaming it; **keeping `MAX_ALLOC_BYTES` as a
  compressed-only guard**; **admitting each token separately**; **a chunk-wide maximum record
  length**; **a recursive out-of-line `materialize`** (measured ~10% cheaper; rests on inliner
  heuristics and bounds element size by the EVM stack); **input-bounded `dwork`** (helps only
  compressible single elements of tens to hundreds of KB).
- **Hand-inlining the expansion arithmetic** into `admitStride` (−20 gas for duplicated lines); **a
  linear expansion bound** in place of the exact `memcost` (−17, and it would contradict the exact
  admission); **splitting `paginate` per layout** (within 1% of the shipped loop); **lowering
  `cpost` to 1,000** (in the maintainer's hands, with the sweep as evidence).
- **A remembered rate across requests**; **per-item gas records** in the stream; **a median or
  histogram**; **`batch.shuffle`**; **a probing knob** (scaling the count hint on a sampled fraction
  of requests was the callsite's business; moot once the count hint went); **mean-only packing** as
  the default;
  **`batch.pageSizeHint`** and **a per-provider map of counts**; **`gasLimit` over a per-item cost
  alone**; **reporting the deploy's gas alone**; **`item_gas_max` as the item cost**; **a second
  `fits` predicate for continuations** (moot once one predicate packs every chunk); **a coalescing
  timer** (`coalesceMs`); **modelling `fixed` against bytes** in the client; **a concurrency cap**
  on flushes (the rate limiter shapes them); **`page_size_suggested`** (a data-independent count
  made sense against a count hint; the tunables are stamped in their own units).
