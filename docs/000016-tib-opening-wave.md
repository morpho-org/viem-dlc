---
kind: tib
version: 0.0.16
related:
  - 000016-tib-envelope-paginated-lenses.md
  - 000016-tib-outcome-stream.md
  - 000016-tib-page-telemetry.md
---

# TIB — Opening wave: the provider's cap and the lens's cost as separate numbers

TIB 000016-page-telemetry sized the opening wave with `batch.pageSizeHint`, a count. A count is
the quotient of two things that live on different axes: the gas a provider grants an `eth_call`,
which varies by node and by chain, and what one attempt on the lens costs, which does not. Behind a
`failover` over two providers, or for one lens deployed on several chains, no single count is
right. This TIB replaces the count with its factors: `gasLimit` on the transport, where the
provider is named, and `batch.gas` on the policy, where the lens is. The envelope reports one more
word per page, what the frame spent before its first attempt, so both lens figures and the
provider's cap can be read off any observed request. The opening wave is then packed by the same
estimator continuations already use. Nothing becomes load-bearing: a wrong figure costs a round
trip, never a result.

## Intent

- Each input to the opening wave sits beside the thing it describes: the cap on the transport,
  the cost on the policy. A composition over several providers states each cap once.
- One estimator sizes every wave. The opening wave differs from a continuation only in where its
  mean and deviation come from.
- Every figure the caller can set is stamped on the wide event of every observed request, in the
  units the caller sets it in.
- The opening wave errs low by construction, since any overshoot buys back the round trip it exists
  to remove; and it stays advisory, since continuations pack from what the pages report.

## Context

`gasLimit / itemGas` is the right shape and the wrong numerator. What the loop can spend is the
cap less everything before it: the transaction's intrinsic gas, the envelope's prologue, the
lens's deploy and the admission reserve. The deploy dominates and is lens-specific: code deposit
alone is 200 gas per byte, so a 20 KB lens spends about 4M gas before its first attempt. Against a
50M cap and 30k attempts, dividing the cap over-packs by roughly 130 elements, and every element
of over-pack is a continuation wave. The prologue has to be a stated figure, and the caller needs a
way to read it.

## Design

### The response

After the sentinel: `nA ‖ budget ‖ fixed ‖ Σg ‖ Σg² ‖ gmax ‖ records`. Five fixed words per page.

- `fixed`: the frame's gas on arrival less `budget`, so the prologue, the deploy and the reserve
  together. `gas()` is read at the envelope's first instruction and kept in scratch `0x00` until
  `paginate` has `budget`. `fixed + budget` is therefore exactly what the frame arrived with,
  whether or not the head refused.
- The sentinel becomes `bytes4(keccak256("ViemDlcPage3()")) = 0xa55835c3`. Records begin at
  `slab + 0xc4`. The initcode grows by 14 bytes and each element by about 5 gas.

### Configuration

- `deployless(http(url), { gasLimit })` and `cache(http(url), [{ ..., gasLimit }, ...])`: the
  provider's `eth_call` gas cap, one per transport instance.
- `policy({ batch: { gas: { fixed, item: { avg, stddev? } } } })`: the lens's cost, pasted from
  `fixed_gas`, `item_gas_avg` and `item_gas_stddev`. `batch.pageSizeHint` is removed.

### The opening wave

A chunk `[start, end)` of `k` elements fits when, beside the byte cap,

```
intrinsic(bytes) + fixed + k·avg + z·stddev·√k ≤ gasLimit
```

with `z = PACKING_SIGMAS` and `intrinsic` what the node deducts before the envelope runs: the
transaction and creation base, calldata by byte (EIP-2028) and initcode by word (EIP-3860),
computed from the chunk's exact bytes (prefix sums of zero and non-zero bytes on the clear path,
plus the four wrapper words whose zero bytes depend on the chunk; the compressed path measures the
wrapped chunk it already builds). The greedy packer's binary search finds `k`. A lone element
always fits the prediction: the estimate may shorten a chunk but never withhold an element, so the
envelope decides what is served. With either the cap or the cost missing or unusable, the opening
wave packs by bytes alone. Continuations are unchanged: `predictItems` over the request's pool, against the
smallest `budget` seen.

### Observability

`gas_limit` when the prediction applied; `fixed_gas`, the largest `fixed` any page reported;
`gas_limit_observed`, the smallest `intrinsic + fixed + budget` over the request's pages, which is
the cap the provider actually granted. `page_size_hint` is removed; `page_size_suggested` remains
what a continuation would be packed at. The recipe: run under observability, take `fixed_gas`,
`item_gas_avg` and `item_gas_stddev` for the policy and `gas_limit_observed` for each transport;
a `gas_limit` above `gas_limit_observed` reads as a cap the provider has since lowered.

## Scope & files

- `src/utils/deployless/Envelope.yul`: `gas()` at entry, the `fixed` word, shifted accumulators,
  sentinel.
- `src/utils/deployless/codec.envelope.ts`: the pasted constant, `OK_SENTINEL`.
- `src/utils/deployless/codec.inner.ts`: `PageGas.fixed`, the header size.
- `src/utils/deployless/call.ts`: `LensGas`, `gasLimit`, the opening `fits`, `intrinsicGas`,
  `chunkCost` shared with `predictItems`, the pooled `fixed` and cap, fields.
- `src/transports/deployless/index.ts`, `src/transports/cache/{index,types}.ts`,
  `src/transports/cache/eth-call/handler.ts`: `gasLimit` threaded to the packer.
- `src/transports/state-overrides.ts`, `src/actions/call.ts`: `batch.gas` replaces
  `batch.pageSizeHint`.
- `test/forge`: `Env.page` reads five words; the prologue is at least the code deposit, the same
  for any `n`, and with `budget` under the grant; regenerated `.gas-snapshot`.
- Not changed: admission, the request wire, continuations, the cache's keys and entries.

## Verification

- Forge: `fixed` exceeds the lens's code deposit, is within 1% between a 3-element and a
  110-element page, and `fixed + budget` is under the grant. The adversaries and sweeps pin
  `cpost` as before.
- Vitest: the opening wave is sized from the cap and the cost; the fixed cost, the stated spread
  and the calldata's intrinsic gas each reduce it, the last to within one gas of the request
  actually sent; a cost above the cap sends every element alone rather than withholding it; it
  recovers the cold-start wave on a three-per-page lens and degrades to bytes when the cost is
  understated; either side missing, or any unusable or malformed figure, leaves bytes-only
  packing; `fixed_gas` and `gas_limit_observed` are stamped, the latter reconstructing the mock's
  cap from the request actually sent.

## Open risks

- **The intrinsic schedule is Ethereum's.** A chain that prices calldata or initcode differently
  shifts `gas_limit_observed` by the difference; pasting it back cancels the shift except as
  chunk size varies.
- **`fixed` includes the reserve**, a Yul constant, so it moves when `cpost` does. It is read from
  telemetry rather than derived by hand, so the caller sees the move as a changed `fixed_gas`.
- **Composition and route** carry over from the page-telemetry TIB: `avg` depends on which
  elements share a frame, and a request served by several nodes behind one URL observes the
  smallest cap.
- **The compressed path's intrinsic gas is not monotone in `k`**: a longer prefix can compress to
  fewer or more zero bytes. Each element adds at least `avg` to the prediction, so the total only
  fails to be monotone for an `avg` below the wobble of a few dozen gas, and the packer's linear
  shrink already tolerates a measure that is not perfectly monotone.

## Notes

- **`fixed` rather than the arrival gas** because it is the knob the caller sets; the arrival gas
  is recovered as `fixed + budget` whenever the client wants it.
- **The stated deviation rather than the maximum.** The maximum errs low without a second figure,
  but on a heterogeneous lens it under-packs by the ratio of the maximum to the mean, trading one
  wave for many parallel requests. Using the mean and deviation gives the opening wave exactly the
  headroom continuations get, from one constant.
- **The intrinsic gas is computed exactly** because it is cheap, the bytes are known, and it is
  the one term whose approximation would bias every opening chunk the same way. It includes the 2
  gas of the `gas()` that samples the arrival, so `gas_limit_observed` is the cap itself.
- **A singleton is never refused by the prediction.** An estimate that could withhold an element
  would be load-bearing; the byte cap alone can, and that is a wire limit rather than a guess.
- **The cap lives on the transport** because the policy travels with the request across every
  `failover` branch; a per-provider figure cannot ride on it.

## Derivation

- **A per-provider map of counts** on the policy was declined: a count is still `f(cap, cost)`,
  and every change to the lens would touch every entry.
- **`gasLimit` alone, dividing by a stated per-item cost**, was the first proposal and was declined
  for the numerator: without the prologue the division over-packs by a lens-specific amount that is
  routinely dozens of elements.
- **Reporting the deploy's gas alone** was declined: the prologue and the reserve are small next to
  the deploy but not next to one element, and any systematic shortfall is a second wave.
- **Pasting `item_gas_max` as the item cost** was proposed as the built-in way to err low and
  declined for heterogeneous lenses, as above.
