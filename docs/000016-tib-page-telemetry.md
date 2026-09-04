---
kind: tib
version: 0.0.16
related:
  - 000016-tib-envelope-paginated-lenses.md
  - 000016-tib-outcome-stream.md
  - 000016-tib-streaming-decompression.md
---

# TIB — Page telemetry: every page an observation, every wave a prediction

The client of TIB 000016 packs by bytes and learns a lens's rate only from the count a page
adjudicated, and only when that page stopped for gas. That sample is conditionally heavy — a page
stops because its items cost more than its neighbours' — so continuations are packed to the
worst page seen, and wave 1 always over-packs a multi-page input, paying one extra round trip by
construction. The envelope already observes the gas of every attempt. This TIB has it report four
aggregates per page — the frame's usable budget and the sum, sum of squares and maximum of the
per-attempt cost, and what the frame spent before its first attempt — so that a page that fully
served is as informative as one that stopped. The client pools them over the request and packs
every later wave to a prediction with a stated exceedance bound. The opening wave is packed by the
same estimator from two stated figures that live on different axes: the provider's `eth_call` cap
on the transport, and the lens's cost on the policy, in the units the wide event reports them. No
state survives a request, no gas figure is load-bearing, and nothing the earlier TIBs guarantee
about admission changes.

## Intent

- Every page is an uncensored observation of the lens's per-attempt cost and of the budget the
  frame had for attempts, whether or not it stopped for gas.
- Waves after the first are packed from the request's own pooled observations, never from the
  count of a single parent page.
- The opening wave can be sized by the caller from figures that each sit beside the thing they
  describe: the cap on the transport, where the provider is named; the cost on the policy, where
  the lens is. A composition over several providers states each cap once, and one lens cost serves
  every provider and chain.
- One estimator sizes every wave; the opening wave differs from a continuation only in where its
  mean and deviation come from. Every figure the caller can set is stamped on the wide event of
  every observed request, in the units the caller sets it in.
- The opening wave errs low by construction, since any overshoot buys back the round trip it exists
  to remove, and stays advisory: it may shorten a chunk but never withhold an element.
- Nothing is remembered across requests; the envelope's admission guarantee (nothing touched
  before it is admitted, nothing after the call costs more than the callee cannot take away) is
  untouched.

## Context

Bytes-only packing was chosen in TIB 000016 to remove every gas number from configuration. It
left the client with one signal, `nA`, and one rule for continuations: repack the tail at the
count the parent served. That rule is biased. Among pages of comparable size, the ones that stop
for gas are the ones whose items happened to be expensive, so the demonstrated count is a draw
from the heavy tail. Light pages fully serve and reveal only "at least this many", and under a
caller hint that is every page, so the client can never learn that the hint is too low. The
000016 TIB deferred an `itemsHint` for the cold-start wave and stated the condition for adding
it: when the extra round trip demonstrably matters. This TIB adds the instrument, and sizes the
opening wave from it.

A count for the opening wave was shipped first and withdrawn: a count is the quotient of the gas a
provider grants, which varies by node and chain, and what one attempt costs, which does not, so
behind a `failover` or across chains no single count is right. `gasLimit / itemGas` is the right
shape and the wrong numerator: what the loop can spend is the cap less the transaction's intrinsic
gas, the envelope's prologue, the lens's deploy and the admission reserve. The deploy dominates
and is lens-specific — code deposit alone is 200 gas per byte, so a 20 KB lens spends about 4M gas
before its first attempt — and every element of over-pack is a continuation wave. The prologue
has to be a stated figure, and the caller needs a way to read it.

## Design

### The response

After the sentinel: `nA ‖ budget ‖ fixed ‖ Σg ‖ Σg² ‖ gmax ‖ records`. Five fixed words per page.

- `budget`: gas at the top of the loop, less the reserve every admission insists on keeping
  (`64·cpost`, see `prepare`), saturating at zero: a frame that arrives with less than the reserve
  refuses at the head and reports no budget rather than a wrapped one. It is what attempts could
  spend, so a page that ran out stops with about zero of it left, and a prediction against it
  needs no client-side reserve.
- `fixed`: the frame's gas on arrival less `budget`, so the prologue, the deploy and the reserve
  together. `gas()` is read at the envelope's first instruction and kept in scratch `0x00` until
  `paginate` has `budget`. `fixed + budget` is exactly what the frame arrived with, whether or not
  the head refused.
- `g` for an attempt: gas from the top of its iteration to just before its accounting, so
  admission, staging, decompression, the call and the record write are all in it. Successes and
  declines are charged. A death is not: it consumed whatever was left, and the client already
  knows it died. A head refusal leaves the three accumulators zero.
- The sentinel becomes `bytes4(keccak256("ViemDlcPage3()")) = 0xa55835c3`; it is the format
  version, so a page in an older format is not recognised. Records begin at `slab + 0xc4`.

### Accounting in the envelope

`paginate` writes `budget` and `fixed` before the loop and stores the gas level in scratch `0x20`.
The three accumulators live in their own slab header slots and are updated by `account(slab)` from
the loop's post block, which a `break` — a death or an admission refusal — skips. Fresh memory
zeroes the slots. The telemetry adds about 255 gas per element to both paths (the snapshot moved
from 1,367,470 to 1,393,350 and from 1,632,345 to 1,658,772 per hundred elements) and 95 bytes of
initcode. It runs after the call, so its cost is post-split and bears on `cpost`: the drained
callee adversary now fails at 1,100 and passes at 1,200, and `cpost` is 1,400.

### The decoder

`Page` gains `gas: { budget, fixed, sum, sumSquares, max }`. `hexToPage` reads the five words and
rejects telemetry that violates the relations any `served` non-negative samples satisfy: with
`served = 0`, all three must be zero; otherwise `sum > 0`, `max ≤ sum`, `sum² ≤ served·sumSquares`
and `sumSquares ≤ sum·max`. Necessary conditions, not a certificate: a tuple can pass them and
still correspond to no integer samples. The sum may exceed the budget: the last attempt admitted may spend into
the reserve, and does whenever the callee drains its frame.

### Packing

The request pools every page it sees into `{ budget: min, served, sum, sumSquares, max }`. A
continuation's cap is `predictItems`: the largest `k` with

```
k·μ + z·σ·√k ≤ budget        μ = sum / served,  σ² = (served·sumSquares − sum²) / served²
```

A chunk's cost is a sum of `k` attempt costs; were they uncorrelated its deviation would be
`σ·√k`, a shrinking fraction of a large chunk. `z = PACKING_SIGMAS = 2`. Tails are packed only after the
whole wave has settled, so every tail of a wave sees the same pool. Escalation singletons and
halving are unchanged. A page that dies no longer punishes the tail behind it: the tail is packed
from what the page reported about the elements it did serve.

### The opening wave

- `deployless(http(url), { gasLimit })` and `cache(http(url), [{ ..., gasLimit }, ...])`: the
  provider's `eth_call` gas cap, one per transport instance.
- `policy({ batch: { gas: { fixed, item: { avg, stddev? } } } })`: the lens's cost, pasted from
  `fixed_gas`, `item_gas_avg` and `item_gas_stddev`.

A chunk `[start, end)` of `k` elements fits when, beside the byte cap,

```
intrinsic(bytes) + fixed + k·avg + z·stddev·√k ≤ gasLimit
```

with `intrinsic` what the node deducts before the envelope's first `gas()` returns: the transaction
and creation base, calldata by byte (EIP-2028), initcode by word (EIP-3860) and that opcode's own
2, computed from the chunk's exact bytes (prefix sums of zero and non-zero bytes on the clear path,
plus the four wrapper words whose zero bytes depend on the chunk; the compressed path measures the
wrapped chunk it already builds). The greedy packer's binary search finds `k`. A lone element
always fits the prediction: the estimate may shorten a chunk but never withhold an element, so the
envelope decides what is served. With either the cap or the cost missing, or any figure unusable
or malformed, the opening wave packs by bytes alone. Understating the cost costs one continuation
wave; overstating it costs more parallel requests; neither is a failure.

### Observability

On every request that reached the packer: `frame_gas` (the smallest budget seen), `fixed_gas` (the
largest `fixed`), `item_gas_avg`, `item_gas_stddev`, `item_gas_max`, `page_size_suggested`
(`predictItems` of the pool), `gas_limit_observed` (the smallest `intrinsic + fixed + budget`,
which is the cap the provider actually granted) and, when the prediction applied, `gas_limit`. A
full cache hit and an empty input never reach the packer and carry none of these. The recipe: run
under observability, take `fixed_gas`, `item_gas_avg` and `item_gas_stddev` for the policy and
`gas_limit_observed` for each transport; a `gas_limit` above `gas_limit_observed` is a cap the
provider has since lowered.

## Scope & files

- `src/utils/deployless/Envelope.yul`: `gas()` at entry, header words, `account`, scratch `0x00`
  and `0x20`, sentinel, `cpost`.
- `src/utils/deployless/codec.envelope.ts`: the pasted constant, `OK_SENTINEL`.
- `src/utils/deployless/codec.inner.ts`: `PageGas`, `Page.gas`, `hexToPage`, `pageToWire`.
- `src/utils/deployless/call.ts`: `LensGas`, `gasLimit`, the opening `fits`, `intrinsicGas`,
  `chunkCost` shared with `predictItems`, the pool, wave-level packing, fields.
- `src/transports/deployless/index.ts`, `src/transports/cache/{index,types}.ts`,
  `src/transports/cache/eth-call/handler.ts`: `gasLimit` threaded to the packer.
- `src/transports/state-overrides.ts`, `src/actions/call.ts`: `batch.gas` and its documentation.
- `test/forge`: `Env.page`, the `OK` constants, telemetry cases, regenerated `.gas-snapshot`.
- `test/helpers/page.ts` and the page-building mocks: every mocked page reports telemetry.
- Not changed: admission, the wire format of the request, the cache's keys and entries.

## Verification

- Forge: the per-attempt mean tracks the frame's marginal cost per element within 10%; declines
  are charged; a head refusal charges nothing; a page that dies at 4 charges the same as a page
  over its first four elements; a compressed page charges more than the clear page for the same
  input; `fixed` exceeds the lens's code deposit, is within 1% between a 3-element and a
  110-element page, and `fixed + budget` is under the grant. The adversaries and boundary sweeps
  pin `cpost` as before.
- Vitest: the decoder round-trips and rejects each inconsistent header; the opening wave is sized
  from the cap and the cost, and the fixed cost, the stated spread and the calldata's intrinsic gas
  each reduce it, the last to within one gas of the request actually sent; a cost above the cap
  sends every element alone rather than withholding it; the prediction recovers the cold-start wave
  on a three-per-page lens and degrades to bytes when the cost is understated; either side missing,
  or any unusable or malformed figure, leaves bytes-only packing; continuations are packed at the
  reported rate rather than the parent's count; tails wait for the whole wave's pool; a wide spread
  packs more conservatively than a flat one with the same mean; the fields are stamped, with
  `gas_limit_observed` reconstructing the mock's cap from the request actually sent, and only the
  frame's words when nothing was served.

## Open risks

- **Composition.** `μ` depends on which items share a frame: warm storage makes related items
  cheaper together, so a grouped input reads cheaper than a shuffled one. The caller orders
  `args`; `readLens` aligns results to any order.
- **Route.** `budget` is the node's cap less the prologue, so it varies by provider. The pool takes
  the minimum over the request; a request served by several nodes behind one URL packs to the
  smallest.
- **The death is still censored.** Its cost is unknown by definition, and the only element whose
  cost the pool never sees is the one that mattered most on that page.
- **`z` is a tuned constant, and the `√k` scaling assumes uncorrelated costs.** Under that
  assumption Cantelli bounds overshoot at `1/(1+z²)` per chunk; warm storage and ordering
  correlate costs and widen a chunk's deviation, so the figure is a target the operator can read
  against `pages_continued`, not a bound.
- **`Σg²` is unbounded by the envelope.** `g` is below `2⁶⁴` on any node and `n` is bounded by
  initcode, which keeps it far under `2²⁵⁶`, but nothing checks.
- **The intrinsic schedule is Ethereum's.** A chain that prices calldata or initcode differently
  shifts `gas_limit_observed` by the difference; pasting it back cancels the shift except as chunk
  size varies.
- **`fixed` includes the reserve**, a Yul constant, so it moves when `cpost` does. It is read from
  telemetry rather than derived by hand, so the caller sees the move as a changed `fixed_gas`.
- **The compressed path's intrinsic gas is not monotone in `k`**: a longer prefix can compress to
  fewer or more zero bytes. Each element adds at least `avg` to the prediction, so the total only
  fails to be monotone for an `avg` below a wobble of a few dozen gas, and the packer's linear
  shrink already tolerates a measure that is not perfectly monotone.

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
- **The cap lives on the transport** because the policy travels with the request across every
  `failover` branch; a per-provider figure cannot ride on it.
- **`fixed` rather than the arrival gas** because it is the knob the caller sets; the arrival gas
  is recovered as `fixed + budget` whenever the client wants it.
- **The stated deviation rather than the maximum.** The maximum errs low without a second figure,
  but on a heterogeneous lens it under-packs by the ratio of the maximum to the mean, trading one
  wave for many parallel requests. The mean and deviation give the opening wave exactly the
  headroom continuations get, from one constant.
- **The intrinsic gas is computed exactly** because it is cheap, the bytes are known, and it is
  the one term whose approximation would bias every opening chunk the same way.
- **A singleton is never refused by the prediction.** An estimate that could withhold an element
  would be load-bearing; the byte cap alone can, and that is a wire limit rather than a guess.
- **Tails wait for the wave** because packing at parent completion would see an arbitrary
  completion-order prefix of the pool, and two tails in the same wave would be packed to
  different estimates of the same lens.
- **Why `z = 2`.** An overshoot costs one continuation, packed from more data; an undershoot costs
  extra parallel requests. Under uncorrelated costs one sigma leaves a coin flip and two leaves one
  in five, and the `√k` scaling makes the margin a few percent of a large chunk.

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
  knob, and `page_size_hint` on the wide event marks the probed requests.
- **Mean-only packing** was declined as the default: it overshoots about half the time on
  heterogeneous input, which is the case the prediction exists for.
- **Predicting against the raw frame gas** was caught in review: every admission requires the
  reserve to remain, so a page stops with about `64·cpost` unspent, and a prediction against the
  raw figure would over-pack every continuation by that much divided by `μ`.
- **Packing a tail as soon as its parent settled** was caught in review: it sees whichever pages
  happened to finish first.
- **`batch.pageSizeHint`, a count for the opening wave**, shipped and was withdrawn before any
  release: provider-dependent, as the Context says. A per-provider map of counts on the policy was
  declined for the same reason, and because every change to the lens would touch every entry.
- **`gasLimit` dividing a stated per-item cost alone** was declined for the numerator: without the
  prologue the division over-packs by a lens-specific amount that is routinely dozens of elements.
  Reporting the deploy's gas alone was declined too: the prologue and the reserve are small next to
  the deploy but not next to one element, and any systematic shortfall is a second wave.
- **Pasting `item_gas_max` as the item cost** was proposed as the built-in way to err low and
  declined for heterogeneous lenses, as the Notes say.
