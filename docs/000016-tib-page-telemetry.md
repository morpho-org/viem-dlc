---
kind: tib
version: 0.0.16
related:
  - 000016-tib-envelope-paginated-lenses.md
  - 000016-tib-outcome-stream.md
  - 000016-tib-streaming-decompression.md
---

# TIB — Page telemetry: every page an observation, every later wave a prediction

The client of TIB 000016 packs by bytes and learns a lens's rate only from the count a page
adjudicated, and only when that page stopped for gas. That sample is conditionally heavy — a page
stops because its items cost more than its neighbours' — so continuations are packed to the
worst page seen, and wave 1 always over-packs a multi-page input, paying one extra round trip by
construction. The envelope already observes the gas of every attempt. This TIB has it report four
aggregates per page — the frame's usable budget and the sum, sum of squares and maximum of the
per-attempt cost — so that a page that fully served is as informative as one that stopped. The
client pools them over the request and packs every later wave to a prediction with a stated
exceedance bound; the caller can size the opening wave with `batch.itemsHint`; and the number to
give it is stamped on the wide event of every request. No state survives a request, and nothing
the earlier TIBs guarantee about admission changes.

## Intent

- Every page is an uncensored observation of the lens's per-attempt cost and of the budget the
  frame had for attempts, whether or not it stopped for gas.
- Waves after the first are packed from the request's own pooled observations, never from the
  count of a single parent page.
- The opening wave can be sized by the caller, and the value to give is derivable from any single
  observed request.
- Nothing is remembered across requests; no gas figure is configured; the envelope's admission
  guarantee (nothing touched before it is admitted, nothing after the call costs more than the
  callee cannot take away) is untouched.

## Context

Bytes-only packing was chosen in TIB 000016 to remove every gas number from configuration. It
left the client with one signal, `nA`, and one rule for continuations: repack the tail at the
count the parent served. That rule is biased. Among pages of comparable size, the ones that stop
for gas are the ones whose items happened to be expensive, so the demonstrated count is a draw
from the heavy tail. Light pages fully serve and reveal only "at least this many", and under a
caller hint that is every page, so the client can never learn that the hint is too low. The
000016 TIB deferred an `itemsHint` for the cold-start wave and stated the condition for adding
it: when the extra round trip demonstrably matters. This TIB adds the hint and, more usefully,
the instrument that makes it unnecessary to guess.

## Design

### The response

After the sentinel: `nA ‖ budget ‖ Σg ‖ Σg² ‖ gmax ‖ records`. Four fixed words per page.

- `budget`: gas at the top of the loop, less the reserve every admission insists on keeping
  (`64·cpost`, see `prepare`). It is what attempts could spend, so a page that ran out stops
  with about zero of it left, and a prediction against it needs no client-side reserve.
- `g` for an attempt: gas from the top of its iteration to just before its accounting, so
  admission, staging, decompression, the call and the record write are all in it. Successes and
  declines are charged. A death is not: it consumed whatever was left, and the client already
  knows it died. A head refusal leaves the three accumulators zero.
- The sentinel becomes `bytes4(keccak256("ViemDlcPage2()")) = 0x1824683e`; it is the format
  version, so a page in the older format is not recognised.

### Accounting in the envelope

`paginate` writes `budget` before the loop and stores the gas level in scratch `0x20`. The three
accumulators live in their own slab header slots and are updated by `account(slab)` from the
loop's post block, which a `break` — a death or an admission refusal — skips. Fresh memory zeroes
the slots. The accounting adds about 250 gas per element to both paths (the snapshot moved from
1,367,470 to 1,392,641 and from 1,632,345 to 1,658,019 per hundred elements) and 73 bytes of
initcode. It runs after the call, so its cost is post-split and bears on `cpost`: the drained
callee adversary now fails at 1,100 and passes at 1,200, and `cpost` is 1,400.

### The decoder

`Page` gains `gas: { budget, sum, sumSquares, max }`. `hexToPage` reads the four words and
rejects telemetry that no set of `served` non-negative samples could produce: with `served = 0`,
all three must be zero; otherwise `sum > 0`, `max ≤ sum`, `sum² ≤ served·sumSquares` and
`sumSquares ≤ sum·max`. The sum may exceed the budget: the last attempt admitted may spend into
the reserve, and does whenever the callee drains its frame.

### Packing

The request pools every page it sees into `{ budget: min, served, sum, sumSquares, max }`. A
continuation's cap is `predictItems`: the largest `k` with

```
k·μ + z·σ·√k ≤ budget        μ = sum / served,  σ² = (served·sumSquares − sum²) / served²
```

A chunk's cost is a sum of `k` attempt costs, so its deviation grows as `√k`, and the margin is
a shrinking fraction of a large chunk. `z = PACKING_SIGMAS = 2`. Tails are packed only after the
whole wave has settled, so every tail of a wave sees the same pool. Escalation singletons and
halving are unchanged. A page that dies no longer punishes the tail behind it: the tail is packed
from what the page reported about the elements it did serve.

### The hint

`batch.itemsHint` caps the opening wave's chunks at that many elements, beside the wire cap. It
is a count, not a gas figure: before the first response the client has no budget to divide by.
Anything but a positive safe integer is ignored, as a non-positive `batchSize` is. Overshoot
costs one continuation wave; undershoot costs more parallel requests; neither is a failure.

### Observability

On every request that reached the packer: `frame_gas` (the smallest budget seen), `item_gas_avg`,
`item_gas_stddev`, `item_gas_max`, `items_hint_suggested` (`predictItems` of the pool) and, when
one applied, `items_hint`. A full cache hit and an empty input never reach the packer and carry
none of these. A stale hint reads as `items_hint` far from `items_hint_suggested`; one that
undershoots shows `pages_continued` at zero with the suggestion above the hint.

## Scope & files

- `src/utils/deployless/Envelope.yul`: header words, `account`, scratch `0x20`, sentinel, `cpost`.
- `src/utils/deployless/codec.envelope.ts`: the pasted constant, `OK_SENTINEL`.
- `src/utils/deployless/codec.inner.ts`: `PageGas`, `Page.gas`, `hexToPage`, `pageToWire`.
- `src/utils/deployless/call.ts`: `itemsHint`, the pool, `predictItems`, wave-level packing, fields.
- `src/transports/state-overrides.ts`, `src/actions/call.ts`: the option and its documentation.
- `test/forge`: `Env.page`, the `OK` constants, telemetry cases, regenerated `.gas-snapshot`.
- `test/helpers/page.ts` and the page-building mocks: every mocked page reports telemetry.
- Not changed: admission, the wire format of the request, the cache's keys and entries.

## Verification

- Forge: the per-attempt mean tracks the frame's marginal cost per element within 10%; declines
  are charged; a head refusal charges nothing; a page that dies at 4 charges the same as a page
  over its first four elements; a compressed page charges more than the clear page for the same
  input. The adversaries and boundary sweeps pin `cpost` as before.
- Vitest: the decoder round-trips and rejects each inconsistent header; the hint caps the
  opening wave, recovers the cold-start wave on a three-per-page lens, degrades to bytes-only when
  it overshoots, and is ignored when unusable; continuations are packed at the reported rate rather
  than the parent's count; tails wait for the whole wave's pool; a wide spread packs more
  conservatively than a flat one with the same mean; the fields are stamped, and only the frame is
  when nothing was served.

## Open risks

- **Composition.** `μ` depends on which items share a frame: warm storage makes related items
  cheaper together, so a grouped input reads cheaper than a shuffled one. The caller orders
  `args`; `readLens` aligns results to any order.
- **Route.** `budget` is the node's cap less the prologue, so it varies by provider. The pool takes
  the minimum over the request; a request served by several nodes behind one URL packs to the
  smallest.
- **The death is still censored.** Its cost is unknown by definition, and the only element whose
  cost the pool never sees is the one that mattered most on that page.
- **`z` is a tuned constant.** Costs are neither independent nor normal, so the bound is
  Cantelli's: overshoot at most `1/(1+z²)` per chunk. It buys a probability, not a guarantee.
- **`Σg²` is unbounded by the envelope.** `g` is below `2⁶⁴` on any node and `n` is bounded by
  initcode, which keeps it far under `2²⁵⁶`, but nothing checks.

## Notes

- **Aggregates, not per-item costs.** The tail's items have not run, so per-item costs of the
  served items would only be averaged anyway. Four words per page cost the same at any `n`.
- **The header slots are the accumulators.** No frame word is added, the history offset does not
  move, and the exit writes nothing extra. Scratch `0x20` for the gas level keeps a variable off
  a stack the loop already fills; the variant with `t` on the stack compiled 50 bytes larger and
  cost more per element on both paths.
- **The reserve is subtracted in the envelope** so that `64·cpost` stays in one place. A client
  that subtracted it would carry a Yul constant.
- **The sum may exceed the budget** because admission only requires the reserve to be present
  before the call, not after. Rejecting `sum > budget` would reject every page whose last callee
  drained its frame.
- **The hint is a count** because it is used before any response exists. The wide event reports
  gas per item and the derived count side by side for a caller who reasons in gas.
- **Tails wait for the wave** because packing at parent completion would see an arbitrary
  completion-order prefix of the pool, and two tails in the same wave would be packed to
  different estimates of the same lens.
- **Why `z = 2`.** An overshoot costs one continuation, packed from more data; an undershoot costs
  extra parallel requests. One sigma leaves a coin flip under Cantelli; two leaves one in five,
  and the `√k` scaling makes the margin a few percent of a large chunk.

## Derivation

- **A remembered rate across requests** (the second half of the deferred `itemsHint`) was designed
  and declined. Under count-only pages it could only learn upward by overshooting, so it either
  ratcheted down to the gassiest page or had to periodically forget itself or grow by a tuned
  factor. With telemetry every page reports its headroom, so the argument no longer applies, but
  the memory is also unnecessary: the suggestion is on every request's wide event, and the caller
  owns the number.
- **Per-item gas records** in the stream were declined: return data proportional to `n` for no
  information the client can use before the tail has run.
- **A median or a histogram** was declined: selection is not worth writing in Yul, and a log-bucket
  histogram would be sixteen fixed words for quantiles that `σ` and `max` approximate.
- **`batch.shuffle`** was declined: `readLens` aligns results to `args` in any order, so a caller
  who wants a representative rate shuffles and a caller who wants warm state groups.
- **A probing knob** (ignore or scale the hint on a sampled fraction of requests) was declined: the
  hint is the opening item cap, so `(Math.random() < p ? 2 : 1) * hint` at the callsite is the
  knob, and `items_hint` on the wide event marks the probed requests.
- **Mean-only packing** was declined as the default: it overshoots about half the time on
  heterogeneous input, which is the case the prediction exists for.
- **Predicting against the raw frame gas** was caught in review: every admission requires the
  reserve to remain, so a page stops with about `64·cpost` unspent, and a prediction against the
  raw figure would over-pack every continuation by that much divided by `μ`.
- **Packing a tail as soon as its parent settled** was caught in review: it sees whichever pages
  happened to finish first.
