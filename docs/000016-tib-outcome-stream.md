---
kind: tib
version: 0.0.16
related:
  - 000016-tib-envelope-paginated-lenses.md
  - 000016-tib-streaming-decompression.md
---

# TIB — Outcome stream: envelope memory proportional to progress, no declared sizes

TIB 000016 moved the element loop into the envelope so that no per-element cost is paid before
an admission check. Three costs still are, all proportional to the *sent* count `n`: the slab
high-water touch that reserves `n × (output stride + 32)` before item zero, the ABI offset table
that dynamic input carries and dynamic output must be given, and — in the compressed envelope —
decompression of the whole payload. The client hides them behind `MAX_ALLOC_BYTES`, a byte budget
derived from an assumed minimum node cap, and behind heuristic bisection on provider "out of gas"
text. This TIB removes the first two by replacing the response tuple with an **outcome stream**
— one record per attempt, appended as it happens, with an O(1) exit — and by sending dynamic
elements as **length-prefixed records** instead of an offset table. Nothing past the page header
is written before the attempt that needs it; every memory expansion is priced against the fee
schedule before it is paid, or, for a dynamic result whose size is only known after the call,
admitted against the gas actually left; and the declared bounds `maxItemBytes` /
`maxResultBytes` cease to exist. The two envelopes become **one**, with compression a flag in the
config word, so the loop is written once. The uncompressed path becomes safe for any lens at any
`n` the wire admits, with no constant. The companion TIB 000016-streaming-decompression does the
same for the compressed path, whose prologue this TIB leaves alone.

## Intent

**Envelope memory grows only with progress.** Beyond a fixed prologue — the calldata copy
(bounded by EIP-3860) and two header words — the envelope expands memory only to hold the
records it has actually produced. A chunk of a million elements costs nothing until they are
attempted.

**Every expansion is admitted, from the fee schedule.** The admission check before each attempt
computes the exact memory delta the attempt can cause (`3w + w²/512` against the current
high-water) and refuses the attempt if the frame cannot afford it and the exit. A dynamic result
is admitted after the call, against the gas actually retained. A frame that reaches the loop
never dies unreported, for any input, result size, or skip pattern. In one sentence: nothing is
touched before it is admitted, and nothing after the call costs more than the callee is unable to
take away.

**No numbers from the author about the lens.** Static element sizes come from the ABI. Dynamic
element sizes come from the elements themselves: inputs carry their length on the wire, results
carry theirs in `returndatasize`. `maxItemBytes` and `maxResultBytes` are deleted from `policy`,
`readLens` and the cache handler. The only number left about the lens's traffic is
`batch.batchSize`, a wire cap that defaults to a chain constant.

**The caller's response does not change.** `readLens` and `readContract` consumers still receive
`(U[] results, uint256[] skipped)`, byte-for-byte; cache keys and cached values are the raw
element bytes they are today.

## Context

Memory expansion is quadratic. The slab reservation is `n × (out + 32)` where `out` is the
lens's output stride or declared bound, so at the wire's ~1,500 elements an eight-word tuple
costs ~400k gas of memory before item zero and a 4 KiB dynamic bound ~73M. The allocation budget
exists to pack fewer elements when the stride is wide. It is a correct guard and a wrong kind of
constant: 1 MiB is "a fifth of a 10M cap", a statement about nodes, not about the EVM or the lens.
It also forces the author to declare bounds for dynamic types so the client can project the slab,
and those bounds are then enforced as protocol errors on every fresh and cached result —
machinery whose only purpose is to make the projection sound.

The reservation is there so skip words, written at the slab's far end, can never collide with
results growing from its near end. Any design that stops reserving must put skips somewhere
else. The offset table is there because the response is an ABI tuple and ABI puts heads before
tails. Any design that stops reserving must also stop being ABI on the wire, since a table of
`n` offsets is exactly the O(`n`) touch being removed. Both follow from one decision: the wire
form is ours (it has had a single reader since TIB 000012), so it can be a stream.

## Design

### The response: one record per attempt

After the sentinel, the payload is `nA` (attempts adjudicated, ≥ 1) followed by `nA` records in
attempt order:

```
success:   (1 << 255) | L   ‖ L bytes of raw U          L = static size, or returndata tail length
decline:   i                                             the per-item call reverted
death:     ~i                                            gas could not resolve element i; always last
```

Lengths are below 2²⁵⁴, so the top two bits classify a record (`10` success, `11` death, `00`
decline; `01` is invalid) and `~i` is byte-identical to TIB 000016's tag. The **sentinel
changes** to `bytes4(keccak256("ViemDlcPage()")) = 0xf90a85b5`; it is the format version. A
response under the old sentinel is not a page and propagates as an error, the only
mixed-version behaviour needed.

The decoder (`hexToPage`) is the protocol boundary and binds every record to its ordinal:

- `nA ≥ 1`, a safe integer, `≤ remaining bytes / 32` (checked before any allocation);
- exactly `nA` records, consuming the payload exactly — no trailing bytes;
- record `j`: a decline must equal `j`; a death must satisfy `~value == j` and `j == nA − 1`;
  a success must have `L == outSize` for a static layout, or `L ≥ 32`, `L % 32 == 0`, `L < 2²⁵⁴`
  for a dynamic one, with `L` bytes present.

It returns the same `Page` as today; the ordering checks `validatePage` duplicates in `call.ts`
become structural and are deleted there. `pageToHex` — the caller-facing ABI encoder — is
untouched. `pageToWire(layout, page)` is added for tests and mocks and emits records by walking
attempted indices.

### The request: length-prefixed dynamic elements

`targetData` becomes `n ‖ bodyLen ‖ body`: two clear words, then the element body, which is
FLZ-compressed when the config word's compression bit is set. The unused ABI selector and offset
word go. Static `T` keeps the ABI body, byte-identical, and the envelope checks once that
`n × stride == bodyLen` (`n ≤ bodyLen / stride` first, so the product cannot wrap). Dynamic `T`
replaces the offset table with records:

```
L₀ ‖ E₀ ‖ L₁ ‖ E₁ ‖ …        Eᵢ = the padded ABI tail hexToArray returns today; Lᵢ its length
```

`Lᵢ` is a non-zero multiple of 32. Validation is in subtraction form so nothing can wrap: with
`remaining = bodyLen − consumed`, require `remaining ≥ 32`, `L % 32 == 0`, `L ≠ 0`,
`L ≤ remaining − 32`, and the last element must end the body. A violation is `MalformedInput(i)` (`bytes4(keccak256("MalformedInput(uint256)")) = 0xf5880484`),
a protocol error: the client wrote the wire, so this is a codec bug, never a decline. Element
bytes are identical in both forms, so cache keys and values do not move, and the packer's
per-element wire measure (`32 + L`) is unchanged.

### One envelope

`RevertEnvelope.yul` and `RevertEnvelopeCompressed.yul` become `Envelope.yul`. Bit 221 of the
config word says whether `body` is compressed; when it is, the prologue decompresses it whole
after the args, as the compressed envelope does today, and `paginate` runs over the result. The
client ships one bytecode constant, one build script, one drift guard; `unwrapDeploylessFactoryCall`
reads the flag from the config word it already decodes, `isRevertExpected` matches one prefix,
and `envelopeConfig(solidity, compress)` sets the bit. Size is not a cost here: an `eth_call`
bills no calldata, the extra initcode meters at 2 gas per word, and the whole envelope is ~1 KB
of a 48 KB wire cap.

### The loop

`paginate` is written once; `locate` reads element `i` in place (the companion TIB makes the
compressed path materialize it instead). `P` is one past the last record written; the exit
reverts `[slab, P)`.

```
paginate(target, n, config):
  slab := aligned(inputEnd);  mstore(slab, sentinel);  mstore(slab + 0x24, 0)   // touched through slab + 0x44
  P := slab + 0x24
  for i in 0 ..< n:
    L := locate(i)                        // static: stride. dynamic: read Lᵢ at cur, validate (above)
    argsLen := 4 + 0x20·inDyn + L
    end  := P + 0x20 + max(argsLen, outSize·!outDyn)
    need := E(end) + Apre(argsLen) + 64·Cpost
    if gas() ≤ need:
      if i == 0 { mstore(P, ~0); P += 0x20; nA := 1 }        // in memory the prologue touched
      break                                                   // i > 0: the prefix so far is the page
    mstore(end − 0x20, 0)                                     // the expansion, paid pre-split
    mstore(P + 0x20, selector << 224);  if inDyn { mstore(P + 0x24, 0x20) }
    mcopy(P + 0x24 + 0x20·inDyn, element, L);  cur += 0x20·inDyn + L
    g := gas()
    ok := staticcall(gas(), target, P + 0x20, argsLen, outDyn ? 0 : P + 0x20, outDyn ? 0 : outSize)
    if ok:
      if !outDyn:  malformed(i) unless returndatasize() == outSize;  Lout := outSize
      else:        malformed(i) if returndatasize() < 0x40
                   returndatacopy(0, 0, 0x20);  Lout := returndatasize() − 0x20
                   malformed(i) unless mload(0) == 0x20 && Lout % 32 == 0
                   deposit := E(P + 0x20 + Lout) + 3 + 3·words(Lout) + Cpost
                   if gas() ≤ deposit { mstore(P, ~i); P += 0x20; nA := i + 1; break }
                   returndatacopy(P + 0x20, 0x20, Lout)
      mstore(P, (1 << 255) | Lout);  P += 0x20 + Lout;  continue
    if returndatasize() == 0 && gas() ≤ g/64 + SLACK { mstore(P, ~i); P += 0x20; nA := i + 1; break }
    mstore(P, i);  P += 0x20
  nA := i unless set above
  mstore(slab + 4, nA);  revert(slab, P − slab)
```

**The record slot is the staging area.** The arguments for attempt `i` are assembled at
`P + 0x20`, where the record will go: on success the result overwrites arguments the call no
longer needs (the EVM copies the input range into the callee's calldata at the call and writes
output only after it returns, so a static result may land on its own arguments), on a skip the
next attempt reuses the slot, and abandoned bytes lie at or past `P`, outside the reverted prefix.
A callee that reverts *with data* into a static out region leaves garbage there too; it is past
`P` after the decline record and overwritten by the next attempt.

**Static output is deposited by the call itself** into memory expanded before the split, so
nothing after the call expands memory and no copy is needed.

**Dynamic output is admitted after the call.** Its size is unknown before, and no bound exists,
so the envelope checks — with the retained 1/64 — whether it can afford the expansion and copy
for the actual `Lout`. If not, the element is reported as a gas death and the client retries it
as a singleton, where the whole frame is available. The per-item call runs twice in that corner,
and only when the frame is nearly exhausted. The failure branch of this check fits in what
remains: the check only reads gas, and `Cpost` covers the death record and exit. A death
recorded here leaves `returndatasize() > 0`, so no heuristic can misread it.

### Admission

`memcost(b)` is over byte ends: `w = ⌈b/32⌉`, `3w + ⌊w²/512⌋`. `E(b) = memcost(max(b, hw))
− memcost(hw)` against the tracked high-water `hw`, which cannot underflow.

**Two currencies, one exchange rate.** Gas spent before the call's EIP-150 split and gas needed
after it are not the same money. EIP-150 retains 1/64 of what is available at the call, so a
pre-split cost reaches the reserve divided by 64 and a post-split cost reaches the floor multiplied
by 64. `need = E(end) + Apre + 64·Cpost` is that statement: the pre-split terms are paid once and
their error is 1/64 of itself, while `Cpost` is the reserve itself and every unit of it costs 64
at the floor. Consequences: `Apre` may be a rough constant; `Cpost` is the whole question; the
tail of every page leaves `64·Cpost` unused; and only a callee that returns with nothing left can
test `Cpost`, because any prompt return refunds more than the post-call path needs.

| term | currency | what |
|---|---|---|
| `E(end)` | pre-split | expansion to this attempt's touch, exact |
| `Apre(argsLen)` | pre-split | the touch store, the selector and head stores, the staging copy (`3 + 3·⌈argsLen/32⌉`), argument setup, the `gas()` sample, the 100-gas warm access |
| `Cpost` | post-split | the longest path from the call's return to a valid used-prefix `revert`: classification, the head check and deposit calculation for dynamic output, one record store, the header patch, `revert` over expanded memory, or the next iteration's admission check and its refusal exit |

The terms are symbols: their values are pinned in the Yul where they are set, by the adversary
fixtures in Verification, not in this document. The floor is no longer flat — `E` grows with
high-water — but every term is measured or schedule-derived, none is a node property, and the
exit is O(1): `revert` charges nothing for already-expanded memory and nothing is relocated.

Proof sketch. After admission the frame holds `> need`. The pre-split spend is `≤ E + Apre`, so
the gas available to the call is `> 64·Cpost` and EIP-150 retains `> Cpost` however the callee
behaves. Every post-call path is within `Cpost` and expands nothing, except the dynamic deposit,
which is separately admitted against the actual gas left and whose refusal is itself within
`Cpost`. That deposit is the one place the guarantee is met by a check rather than by
construction, and it is where a drained dynamic result becomes a death rather than a success. The
head refusal (`i = 0` below `need`) writes one word the prologue touched. A refusal at `i > 0`
writes nothing: the prefix so far is the page.

The death heuristic `returndatasize() == 0 && gas() ≤ g/64 + SLACK` is unchanged: `g` is sampled
after every input access and expansion, immediately before the call, so memory work can never be
mistaken for a per-item death.

### Client

- **`policy` / `readLens` / `EthCallPolicy`:** `maxItemBytes` and `maxResultBytes` removed;
  `ReadLensParameters`' pick list shrinks to `"batch" | "cache"`. `resolveArrayFunction` loses
  `ElementBounds`; `inputBytes` / `outputBytes` become the static stride or `undefined`; a
  dynamic layout no longer errors at request time.
- **`codec.inner.ts`:** `arrayToCalldata` emits `(L ‖ E)*` for a dynamic input layout (static
  path unchanged); `hexToPage` becomes the record decoder above; `pageToWire` added; `pageToHex`
  and `calldataToArray` unchanged.
- **`codec.envelope.ts`:** one `FACTORY_BYTECODE` replacing the two; the compression bit in
  `envelopeConfig`; the `n ‖ bodyLen ‖ body` framing in wrap/unwrap; new `OK_SENTINEL`;
  `MALFORMED_INPUT_SELECTOR` with an exact-length detector, as `MalformedResult` has; the config
  word's size fields carry the static stride only (zero for a dynamic layout).
- **`call.ts`:** the fresh-result bound check, the `maxItemBytes` pre-decline and
  `validatePage`'s ordering checks go. An element that alone exceeds the wire cap is still
  declined client-side (`elements_declined_oversize` keeps that meaning). `MalformedInput` is
  thrown beside `MalformedResult` with its own message. `MAX_ALLOC_BYTES`, the allocation measure
  and the corpse class are **untouched** here; the companion TIB deletes them whole.
- **`handler.ts`:** the cache-hit bound check goes, and with it `encodeResponse`'s `source`
  parameter.
- **Facets:** `attempts_unresolved` now also counts post-call deposit refusals — a success
  reported as unresolved — which is the observable signature of that corner.

### Compression here

The compressed path shares `paginate` by construction, so it gets the outcome stream and loses
its slab reservation in the same change. Its prologue still decompresses the whole body and the
client still packs it under the allocation budget; that is the companion TIB's subject.

## Scope & files

1. **Envelope.** `Envelope.yul` replaces both files: `paginate` as above, `locate`, `memcost`,
   `malformedInput`, today's `flzDecompress` behind the compression bit; delete the scratch
   pointer, the tag relocation, the size-bound checks and `stage`. One `build:Envelope` script,
   one pasted constant; `test/forge` builds and drift-checks one envelope.
2. **Codec.** `codec.inner.ts`: sequential dynamic body, record decoder, `pageToWire`, bounds
   removed. `codec.envelope.ts`: one constant, framing, compression bit, sentinel, selector and
   detector.
3. **Client.** `call.ts`, `handler.ts`, `state-overrides.ts`, `actions/call.ts`,
   `read-lens.ts`: the two options and their enforcement removed.
4. **Docs.** README: the `policy` bullets for the two options, the `readLens` example, the
   "Paginated lenses" contract paragraph. TIB 000016: a top-level addendum naming the sections
   this TIB supersedes — the Intent's "declared byte sizes" sentence, the slab layout and flat
   floor in Design, the staging paragraph and its `S` term, the response encoding, the
   "allocation budget must see the output stride" note, and the "Death-detection imprecision"
   risk (which now also covers the post-call path).

Deliberately unchanged: `deploy`, `OOG_SENTINEL`, the death heuristic, `pageToHex`, cache keys,
`flz.ts` and whole-body decompression in the prologue, `arrayifiedAbi` / `itemFragmentOf`, the
`readLens` shape apart from the two options, the allocation budget and corpse class.

## Verification

- **Foundry (`test/forge`).** `Fixtures.sol` gains a ~20-line record decoder and a sequential
  encoder for dynamic input. One case per cell of the input × output layout matrix (the suite
  covers three today; add dynamic-in / dynamic-out). A dense static page's revert length is
  exactly `4 + 32 + nA·(32 + outSize)`. A wide static result (32-word tuple) at the largest `n`
  the wrapped initcode admits returns a page under a 10M grant, where today's envelope is a
  corpse. Misaligned or overlong `L` → `MalformedInput(i)`. Short dynamic return →
  `MalformedResult` (existing). A dynamic result the frame cannot afford to copy → `~i` last,
  `nA = i + 1`, and the same element alone succeeds. A callee reverting with data into a static
  out region followed by a success. The gas sweep `noCorpseAboveFirstPage` in both modes,
  with the first-page grant independent of the response's size (it still varies with the wire
  bytes copied). **Boundary sweeps**: for each `Cpost` path — empty out-of-gas tag, empty revert,
  revert with data, static success, static malformed, dynamic short and bad-head malformed,
  dynamic success copied and converted to a tag, the head refusal at `i = 0` — find each grant at
  which one more element is adjudicated, then run every grant in a ±1,500 window around it and
  require a well-formed page or protocol error. These are no-corpse fuzzing over grants; they do
  not pin anything. **Adversaries pin constants.** A constant is pinned by a lens built to be the
  worst case for that term alone, such that lowering the term fails its sweep: `Cpost` by a
  callee that returns with almost nothing left (`test_adversary_drainedCallee*`), so the post-call
  path runs on the retained 1/64 alone. Every other fixture passes with `Cpost` far too low,
  because a prompt return refunds what the post-call path needs. A term without an adversary is
  unpinned, whatever the sweeps say. **Gas snapshot.** The sweeps catch a corpse, not a slower
  path: a compiler change that lengthens the post-call path by 20% without crossing `Cpost`
  shortens every page and fails nothing. `test/forge/Gas.t.sol` records the per-element cost of
  both paths and CI checks it against `.gas-snapshot`, so that regression is a diff in review.
- **Vitest.** Decoder: every record kind, `~0`, non-final death, wrong-but-in-range ordinal,
  namespace `01`, zero or misaligned dynamic length, oversized `nA`, truncation exactly on a
  record boundary. `pageToWire ∘ hexToPage` round-trips including a death. `arrayToCalldata`
  static output byte-identical to ABI; dynamic round-trips raw tails. `pageToHex ===
  encodeAbiParameters` retained. Handler: oversize declines keyed to the wire cap only; no bound
  check on hits.
- **Type level.** `ReadLensParameters` no longer accepts `maxItemBytes` / `maxResultBytes`.

## Open risks

- **Double execution of an unaffordable dynamic result.** Bounded to one per page (it ends the
  page) and to frames within `Cpost + copy` of exhaustion. The singleton retry gives the element
  the whole frame but is not a guarantee: a result larger than a frame's deposit budget lands in
  `elements_unresolved`.
- **Dynamic response size is gas-bounded, not byte-bounded.** With `maxResultBytes` gone, `n`
  bounds the record count but one dynamic result can be as large as the frame can copy, so a
  page's revert data is no longer derivable from its input bytes. Static responses stay
  byte-derived (`n × outSize`, ~96 KiB worst case at the wire cap for one-word results). `nA`
  makes every truncation detectable, including one on a record boundary; truncation is a
  transport failure, never something to bisect.
- **Response size.** A 32-byte record header per success doubles the payload for one-word
  results (+12% for an eight-word tuple). Accepted for the O(1) exit and the absence of any
  collision argument.
- **Floor terms.** `Apre` and `Cpost` are compiled-path values. The boundary sweep is the guard;
  an optimizer change that lengthens a path fails the sweep rather than shipping.
- **Sentinel change.** Anything pinning `0x1580d19d` outside this package breaks. Nothing known
  does.

## Notes

**Why a stream and not a bitmap in consumed bytes.** A bitmap (bit `i` = not served, written
into input already consumed) is denser — one bit per element versus a word — and was designed in
full. It needs an exit relocation of `⌈nA/256⌉` words, a per-attempt pre-touch that includes the
relocated bitmap, and a proof that the bitmap never overtakes what the decompressor still reads.
The stream needs none of those: exit is one `revert`, nothing is relocated, and no region is
shared. Legibility of the envelope is worth more than the payload bytes.

**Why `nA` is kept although it is derivable.** The decoder could read records until the payload
ends. `nA` is the one redundancy that catches a truncated payload before a length-prefixed
success record swallows the tail.

**Why the sentinel is the version.** A version word after the sentinel would cost 32 bytes and a
check; changing the sentinel costs nothing and reuses the existing "not our payload" path.

**Why one envelope.** The two files existed to keep the uncompressed initcode small, and "copied
verbatim" was the price: every loop fix landed twice, with two constants, two build scripts and
two drift guards. In `eth_call` the size buys nothing — no calldata is billed, initcode meters at
2 gas per word, and ~300 bytes is under 1% of the wire cap — so the loop is written once and the
compressed path is a branch in the prologue and in `locate`.

**Why stage in the record slot rather than in place.** The selector could be patched over the
four dead bytes preceding each element, saving the staging copy (~35 gas per element). That
needs a proof per layout that those bytes are dead, a masked store, a `0x20` head inside every
dynamic wire record, and a `locate` that cannot be shared with the compressed path, which must
not patch its decompression history. One `paginate` is worth the copy.

**Why static output needs no post-call admission.** Its size is known before the call, so the
expansion is paid at admission, before the split, and the call deposits directly into the record.

**Why dynamic output is admitted after rather than bounded before.** With a bound the envelope
could pre-touch `P + 0x20 + bound` and never discard a success. The cost is the bound itself: an
author-declared number, enforced on every fresh and cached result, whose sole purpose is to make
a memory projection sound. The post-call check trades that for one possible double execution at
the edge of a frame. The intent ranks "no numbers from the author" above that corner.

**Why `MalformedInput` is a protocol error and not a decline.** A misaligned or overlong length
is the one path to an unbounded memory access, and the client wrote the wire. A decline would
hide a codec bug as a skipped element.

**Why the allocation budget is not narrowed here.** The two PRs merge together; an interim
narrowing would be code written, reviewed and deleted between two commits on one branch.

## Derivation

Two independent plans were commissioned (a Claude instance and GPT-5.6 Sol, 2026-09-02) from a
shared brief, then both reviewed the drafted TIBs. They converged on: length-prefixed dynamic
wire in both directions with static ABI untouched; exact memory-delta admission; deleting the
budget and the corpse class; a new protocol error for a bad length; the compression bomb as the
decisive test. They diverged on skip encoding (bitmap in consumed bytes vs. outcome stream —
stream chosen), on dynamic-output deposit (pre-split touch to a bound vs. post-call admission —
post-call chosen so the bound can go), on a version word (folded into the sentinel), and on
floor derivation (audited opcode counts vs. boundary tests — boundary tests chosen as the lighter
drift guard, but covering every post-call path, not one fixture).

The review round found and fixed in this document: death records that did not advance `P`; a
head refusal into untouched memory; `E` as an unsigned subtraction; the head check before the
size check; conflated input/output branches; wrap-prone bounds arithmetic; a decoder that trusted
record order "by construction". It also moved staging into the record slot for both paths
(the first draft patched the selector in place in the uncompressed envelope) and removed an
interim narrowing of the allocation budget. Merging the two envelopes into one came out of the
same round: with a shared `paginate` and a `locate` branch, two files no longer had a reason.

Declined: **keeping ABI on the wire for dynamic `U`** — its offset table is the O(`n`) touch
being removed. **Skip words in consumed decompressed input** — unsound under FLZ back-references
without an 8 KiB lookahead. **Keeping `maxResultBytes` as an optional pre-touch bound** — two
deposit paths for one feature. **A `PAGE_V1` word.** **Auditing opcode counts per path** from the
disassembly with a build test.

Sequencing: PR 2 of three on `paginated-lenses` (after TIB 000016, before
000016-streaming-decompression); the three merge together.
