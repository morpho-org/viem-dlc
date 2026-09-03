---
kind: tib
version: 0.0.16
related:
  - 000016-tib-envelope-paginated-lenses.md
  - 000016-tib-outcome-stream.md
---

# TIB — Streaming decompression: the compressed path without a prologue

After TIB 000016-outcome-stream the envelope expands memory only with progress on the
uncompressed path. The compressed path still decompresses its whole body before the loop: FastLZ
turns 48 KiB of wire into as many bytes as the input repeats, and memory expansion is quadratic,
so a 48 KiB request can exhaust any node's cap with zero progress. That is the last reason `MAX_ALLOC_BYTES`
and heuristic bisection exist. This TIB makes decompression **resumable**: the decompressor keeps
its cursors across loop iterations, produces bytes only as the next element needs them, writes
them into a **fixed history** sized by the format's back-reference window, and copies each element
from there into the outcome slot the previous TIB already admits. Nothing in the prologue scales
with `n`, with the decompressed size, or with the size of any element. The allocation budget, the
corpse class, and the "out of gas" text match are deleted; the deploy sentinel remains as a thrown
diagnostic.

## Intent

**A compressed chunk costs nothing until attempted.** Prologue memory is the args copy (≤ 48
KiB by EIP-3860), a history arena whose size is a property of FastLZ alone, and the page header.
Nothing in it depends on `n`, on the decompressed size, or on the largest element.

**Decompression is admitted per element, against the element's actual length.** The reserve for
materializing element `i` is a bound from the format (every token yields at least one byte and
costs at most a schedule-derived amount) applied to `Lᵢ`, read from the stream before the record
is admitted. No chunk-wide maximum stands in for it.

**A malformed stream is a protocol error, never lens input.** A token that runs past the
compressed bytes, a record the stream cannot fill, a back-reference before the history, or bytes
left over after the last record are `MalformedInput(i)`. No byte the client did not send is ever
passed to the lens.

**No constant derived from a node, no heuristic on provider text.** `MAX_ALLOC_BYTES`, the
allocation measure, `batch_alloc_bytes`, the `"corpse"` class of `classifyChunkError`, the
`/out of gas/` match, `splits_corpse` and `corpse_errors` are deleted. Size and timeout halving
stay. `OOG_SENTINEL` stays as the deploy diagnostic and now throws a distinct error instead of
halving.

## Context

FastLZ (Solady's `LibZip` variant, which `flz.ts` ports) is a byte stream of tokens: a literal
run of 1–32 bytes (1 control byte + the bytes), or a back-reference of length 3–264 at distance
1–8,192 (2 or 3 bytes). Decoding is sequential and each token depends only on the previous 8,192
output bytes. Today `flzDecompress` runs the stream to its end in the prologue, writing every
output byte to memory; then `paginate` runs over the result in place. Compression's whole value —
more elements per 48 KiB — is exactly what makes the decompressed size unbounded by the wire.

The bound the current design leans on is the client's: the allocation measure counts decompressed
input bytes and packs under 1 MiB. It is the only client-side projection of envelope memory left,
and it exists for this path alone.

## Design

### Wire

Unchanged. 000016-outcome-stream already frames `targetData` as `n ‖ bodyLen ‖ body` with the
body FLZ-compressed under the config word's compression bit, so `n` is readable without
decompressing and `bodyLen` is available to validate every length and the stream's end. This TIB
changes only what the envelope does with those bytes. `flz.ts` and the client's wrap/unwrap are
untouched; `unwrapDeploylessFactoryCall` already returns the logical body, which is what tests read.

### Memory

```
[args: n ‖ bodyLen ‖ FLZ(body)][F][history: 2·8192 + 320][slab: sentinel | nA | records / current slot …]
                                   ^ fixed; absent when the bit is clear      ^ the only growing region
```

The history holds the back-reference window (8,192 bytes), a growth zone of the same size so
rebases amortize, and `296 = 264 + 32` bytes of headroom (rounded up to a word) for the most a
token's 32-byte-stride writes can overshoot its output. Its size is decided by the format, not by
the chunk or the node. `F` holds the frame in both modes — target, `n`, body, `bodyLen`, config
and the cursors — so the loop reads invariants from memory rather than holding them on the stack.
Elements are not kept in history for the call: each is copied out into the record slot at
`P + 0x24`, which is the memory the previous TIB already admits per attempt.

### The resumable decompressor

`F` gains `ip` (compressed read cursor), `op` (history write cursor) and `cur` (the history
position of the next byte not yet handed out). `materialize(len, dst)` produces whole tokens until
`len` bytes are pending, then copies them out:

```
materialize(F, len, dst):
  while op − cur < len:
    if op + 296 > histEnd:                                  // rebase, flushing first
      mcopy(dst, cur, op − cur);  dst += op − cur;  len −= op − cur
      mcopy(histStart, op − 8192, 8192);  op := histStart + 8192;  cur := op
    malformedInput(i) if ip ≥ ipEnd or the token's bytes (2, 3, or 1 + literal length) exceed ipEnd
    match: malformedInput(i) if distance > op − histStart   // reference before the history
    emit the token as today (32-byte stores, stride min(distance, 32));  advance ip, op
  mcopy(dst, cur, len);  cur += len
```

Token structure and the copy loop are Solady's, as now; only the loop bound, the persistent
cursors and the bounds checks change. The target is a *length relative to `cur`*, so a rebase —
which moves `cur` and `op` together — cannot leave it in stale coordinates. A token may cross a
record boundary: the overshoot (≤ 263 bytes) stays pending in history and is the first thing the
next `materialize` hands out. Flushing before a rebase is sound because everything pending
belongs to the record being assembled (`op − cur < len`). Rebase arithmetic: the trigger implies
`op ≥ histStart + 16,384`, so the 8,192-byte source and destination are disjoint; after it,
`op − histStart = 8,192 ≥` any distance, so the back-reference check only ever fires in the first
8 KiB of a stream; pending bytes are `≤ 263 < 8,192`, so at most one rebase per record of `≤
8,192` bytes and the copy-out never straddles one that the flush did not handle.

After the last element, `paginate` requires `ip == ipEnd`, `cur == op` and `consumed == bodyLen`;
otherwise `MalformedInput(n − 1)`.

### The loop, compressed `locate`

000016-outcome-stream's `paginate` is unchanged; under the compression bit `locate` differs only
in how element `i` reaches the record slot, in two stages so the admission uses the element's own
length:

```
locate(i):
  static:   L := stride
  dynamic:  if gas() ≤ D(0x20) + Apre₀ + 64·Cpost { head refusal / stop, as in paginate }
            materialize(0x20, 0x00);  L := mload(0x00)
            malformedInput(i) unless L % 32 == 0 && L ≤ bodyLen − consumed − 0x20
  consumed += 0x20·inDyn + L
  // paginate's admission follows, with D(L) added to need
  // paginate's staging becomes:  materialize(L, P + 0x24 + 0x20·inDyn)
```

`consumed` is the count of logical body bytes handed out; every comparison is in subtraction
form against `bodyLen`, so a garbage `L` cannot wrap.

Staging copies out of history rather than calling into it because history is live: a selector
written before a record, or a static result written over one, would corrupt a later
back-reference. The record slot is admitted by `E(end)` before `materialize` runs, so the copy-out
never expands memory that was not priced.

### Admission

`need = E(end) + D(L) + Apre + 64·Cpost`, with `E`, `Apre`, `Cpost` as in 000016-outcome-stream
and:

| term | what |
|---|---|
| `D(L)` | producing and copying out one element, pre-split: `tokenWork(L + 263)` + `3 + 3·⌈L/32⌉` (the copy-out) + one rebase (`3 + 3·256` for the 8 KiB `mcopy`, plus the flush) when `op + L + 559 > histEnd`, else 0 |

`tokenWork(b)` is a source-level bound on decoding `b` output bytes: every token yields at least
one byte, so it is `b ×` the worst per-token cost, whose witness is a stream of one-byte literals
(two input bytes per output byte; ~250 gas each as compiled). The overshoot term is the worst
single token, a 264-byte distance-one match. Only a large element can pin the per-byte term: a
shortfall before the split reaches the retained gas at 1/64, so for small elements the exit
reserve absorbs it, and the sweep that pins it drives a 6 KiB element encoded as literals.
Normal compressor output is not evidence for a safety bound. `D` is a reserve, not a spend: for
32-byte elements it is under 20k gas and only shortens a page already that close to its end.

### Client

- **`call.ts`:** `fits` is `measureWireBytes(start, end) ≤ wireCap`. Delete `MAX_ALLOC_BYTES`,
  `ALLOC_BYTES_*`, `measureAllocBytes`, the `batch_alloc_bytes` stat, `classifyChunkError`'s
  `"corpse"` branch and the `/out of gas/` match, `splits.corpse`, `corpse_errors`, and the
  `count === 1` corpse path. `isOutOfGasRevert` now throws `[deployless] counterfactual deployment
  (factory or constructor) ran out of gas under this node's cap`. The first wave is bounded by the
  wire cap only; continuations keep `maxItems = served`.
- **`codec.envelope.ts`:** re-pasted constant.
- **Facets:** `splits_corpse`, `corpse_errors`, `batch_alloc_bytes` removed.

## Scope & files

1. `Envelope.yul`: history layout, `materialize`, the compressed `locate`, `D`, the end-of-stream
   check; `flzDecompress` deleted. Re-paste the constant.
2. `codec.envelope.ts`: the constant.
3. `call.ts`: deletions above; `OOG_SENTINEL` as a thrown error.
4. Docs. README: the `batch.compress` paragraph ("an over-packed chunk pages") and the transport
   intro's "plus a fixed allocation budget". TIB 000016 addendum: `MAX_ALLOC_BYTES`, the corpse
   route in the outcome table and its facets, the "Cold-start latency" and "`MAX_ALLOC_BYTES`
   sizing" risks.

Deliberately unchanged: the wire form, the config word, `paginate`, `flz.ts`, `deploy`, the death
heuristic; everything caller-facing.

## Verification

- **Foundry.** The decisive case: 100,000 identical 32-byte elements (~37 KiB compressed) under a
  2M grant returns a page with `nR ≥ 1` — today's envelope is a corpse at every grant — and a
  larger grant serves more. Rebase: 2,000 compressible 32-byte elements (≥ 60 KiB through a 16 KiB
  history) decode to the right values; a variant whose repeats sit ~8 KiB apart exercises
  back-references whose source straddles the rebase; a rebase inside the first (`L`) and the
  second (record) `materialize` of one dynamic element. Hand-built streams: a literal and a match
  token each straddling a record boundary; a token running past `ipEnd`; a stream exhausting
  mid-record; a back-reference before the history; trailing compressed bytes after the last
  record; `consumed ≠ bodyLen`, a zero-length record — each `MalformedInput(i)`. A compressed singleton
  refused at its admission floor reports `~0`, not a malformed stream. The gas sweep in
  compressed mode, with the first-page grant independent of the unmaterialized bytes. Boundary
  sweeps as in 000016-outcome-stream (±1,500-gas windows around each grant that adjudicates one
  more element) over one-byte-literal and distance-one streams, the compression bomb across its
  first rebase, and a 6 KiB one-byte-literal element — the one fixture whose materialization is
  large enough to pin the per-byte term, since a pre-split shortfall reaches the retained gas at
  1/64.
- **Vitest.** The packer packs a compression bomb as one wire chunk followed by valid
  continuations; corpse tests become "deploy OOG throws a distinct error"; `MAX_ALLOC_BYTES` tests
  deleted.

## Open risks

- **Per-token cost bound.** `tokenWork` is a source-level bound pinned by the two witness
  streams. Too low would let a nearly exhausted frame die in `materialize` — a corpse. The sweep
  is the guard, and the bound should carry margin over the measured witnesses.
- **Large elements.** The record slot for a 1 MiB element is admitted like any other expansion:
  a frame that cannot afford it stops before it, or at the head reports `~0`, and the client
  retries the element alone. A single element larger than a whole frame's memory budget is
  unresolvable and lands in `elements_unresolved`; that is inherent to the element.
- **Per-element overhead.** One `materialize` (two for dynamic input), one copy-out of `L`
  bytes, and the amortized rebase replace a one-pass copy, and the loop's invariants moved into a
  memory frame to keep the decompressor off a deep stack. Measured on 64-byte static elements
  from a real compressor stream: ~8.0k gas per element against ~5.0k before this TIB, and ~2.3k
  against ~1.8k on the clear path. Per token the resumable decoder costs ~250 gas where the
  one-pass decoder cost far less, because the optimizer inlines it among the loop's live
  variables. A leaner decoder is a follow-up, not a correctness question.

## Notes

**Why a fixed history and not a record-sized ring.** The first draft sized a ring by the chunk's
largest record so every element would be contiguous inside it. That puts an element-sized
expansion back into the prologue — before any admission — and makes every attempt reserve for the
chunk's worst element, so one large late record starves every cheap one before it. Separating
"bytes future tokens may reference" (always 8,192, a format constant) from "bytes this call needs"
(the current element, already admitted in the record slot) makes the prologue fixed and the
admission local.

**Why `2·8192 + 296` and not `8192 + 296`.** A history of exactly the window would rebase on
every 8 KiB — an `mcopy` of the window each time. The growth zone halves the rebase count and
costs ~8 KiB of prologue memory once.

**Why two stages for dynamic input.** The record's admission needs `L`, and `L` is in the stream.
Producing the length word first, under a small fixed reserve, and admitting the record against
the actual `L` is what keeps the reserve proportional to the element rather than to the chunk.

**Why a back-reference before the history is a protocol error.** Solady's compressor never emits
one for a valid stream; at the history's start `op − distance` would otherwise point below the
arena — with a small args region, below zero, an address in the top of the space whose expansion
is an instant corpse. One comparison per match token closes the only such path.

**Why exhaustion is checked.** A stream that ends early would otherwise leave `materialize`
short and the record slot holding bytes the client never sent; those can be valid ABI, the lens
can return a plausible value, and the cache would store it under the original element's key.

## Derivation

From the two commissioned plans and their review round (see 000016-outcome-stream, Derivation).
The review replaced the record-sized ring with the fixed history (above), moved the pump target
to a length relative to `cur`, added the token and exhaustion checks, and dropped the chunk-wide
`R` from the config word along with its per-chunk plumbing in `wrap`.

Declined: **dropping compression** instead — it would make this TIB unnecessary and the previous
one sufficient, but compression is what lets a 48 KiB request carry the fleet's larger inputs,
and the README documents when it pays. **Keeping `MAX_ALLOC_BYTES` as a compressed-only guard**
permanently — it is the node-derived constant the stack exists to remove. **Admitting each token
separately** rather than reserving per element — exact, but a gas check in the hottest loop for
a reserve that only ever ends a page a few thousand gas early. **A chunk-wide maximum record
length in the config word** — see Notes.

Sequencing: PR 3 of three on `paginated-lenses`, after 000016-outcome-stream; the three merge
together.
