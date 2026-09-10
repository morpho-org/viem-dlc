---
kind: tib
version: 0.0.16
related:
  - 000016-tib-paginated-lenses.md
---

# TIB — Override-delivered envelope: the frame's gas as the only chunk bound

Every chunk today is an `eth_call` that *creates* the envelope: the envelope's bytes and the chunk's
elements travel together as initcode, so a chunk can never exceed EIP-3860's 49,152 bytes, however
much gas the node's frame would have served. This TIB adds `batch.envelope: "override"`: a chunk is
delivered as a call *to* a fixed address whose code is the envelope, placed by `eth_call`'s
state-override parameter, with the same argument tuple as calldata. The envelope still deploys the
lens through the factory inside the call, so nothing about the lens changes hands. A provider that
does not honour the override is detected on the opening wave and the range is re-fetched as
initcode; only unambiguous non-support is remembered. The plan that implements this is APPS-1406.

## Intent

- With `batch.envelope: "override"`, on a provider that honours state overrides, a chunk is bounded
  by the frame's gas — through the paginated-lenses predicate, priced exactly for this delivery —
  and by the provider's request size limit; the initcode cap does not apply.
- On a provider that does not honour state overrides, a request with the option completes with the
  result it would have had without it, at the cost of one wasted opening wave per transport
  instance for a provider that says so unambiguously, and one wasted wave per request otherwise.
- One envelope bytecode, one constant, one drift guard; the two deliveries differ only in how the
  arguments reach it. The loop, the records, the telemetry words, the admission guarantee and the
  wire format are those of the paginated-lenses TIB.
- Lens semantics are untouched: the lens is deployed by the factory inside the call in both
  deliveries; a deploy that runs out of gas is terminal in both; the cache's identity — the cleaned
  request the transport derives at entry — never sees the delivery.

## Context

The initcode cap was never a design choice; it is where the elements happened to ride. It binds
exactly for the lenses that need batching most: a cheap per-item function on a large input. A
2,500-gas item on a 50M-gas frame could be served twenty thousand to a page, and a 49,152-byte
chunk carries seven hundred. Compression buys a few times that and then binds again. The
paginated-lenses TIB gave the client an exact picture of what a frame can serve and no way to send
it that many elements. Whether bytes bind for a given lens is already on the wide event: when
`(gas_limit_observed − fixed_gas) / item_gas_avg` is well above the realised elements per chunk
(`elements_requested / nominal_batches`), the frame could have served more than the cap let
through; when it is not, this TIB buys nothing for that lens.

The obvious alternative — override the *lens's* code at its address and skip the factory — is the
one this TIB rejects: the compiler's `deployedBytecode` carries placeholder zeros where immutables
go, constructors write storage the paginated-lenses TIB expressly blesses for shared tables, and
neither is visible to a code override. It would also not lift the cap, since the elements would
still ride as initcode. Placing the *envelope* by override leaves the lens's whole deployment path
alone, and it is the envelope, not the lens, that carries the elements.

`eth_call`'s third parameter is not universal: some providers reject it, some ignore it. The
package rides it for the `policy` sentinel, which the transports strip at entry, and forwards a
caller's own overrides; a caller who passes none has never exercised a provider's support. The
option is opt-in for that reason.

## Design

**One bytecode, two deliveries.** The envelope's prologue copies its arguments from wherever this
delivery put them: as initcode they trail the code and there is no calldata; as runtime code placed
by an override they are the calldata and nothing trails the code. Each copy is a no-op in the other
delivery, so there is no branch, no mode bit and no second constant; the arrival `gas()`, the
deploy, the loop and the exit are unchanged, and so is everything the factory sees.

**Where the envelope runs.** By override the chunk is a call to the address `CREATE` gives a
contract made by the zero address at nonce 0 — the address the envelope already has in creation
delivery, since an `eth_call` without `from` is sent from the zero address, whose nonce is 0 on
every chain. The factory is therefore called from the same `msg.sender` in both deliveries, and the
lens is deployed by it inside the call exactly as today. The envelope's code entry exists only in
the outbound request, never in the request context the cache keys from; a caller's own override at
that address is a protocol error.

**What the predicate learns.** The paginated-lenses predicate keeps deciding every chunk. Three
things it priced implicitly under a 49 KB cap become explicit at a megabyte, all client-side, none
in the telemetry: intrinsic gas depends on the delivery (no creation base and no initcode words by
override); the prologue's copy of the argument tuple is the chunk's cost, not the lens's, so it
leaves `fixed` and is added back per candidate; and since Prague a frame must clear EIP-7623's
floor to start at all, so a lone element above the floor is declined as oversize the way an element
above the byte cap is. A stated `gasLimit` bounds the opening wave's bytes on its own; nothing is
assumed on the caller's behalf when neither `gasLimit` nor `batchSize` is stated.

**Choosing, falling back, remembering.** A request with the option opens by override unless the
transport's memo says the provider does not honour it. Every response to an override chunk is
classified first by whether it *proves the envelope ran*: a page or any envelope-shaped revert is
handled as today; a size or timeout refusal halves by override while there is room; everything else
gets one initcode attempt for the same range, dispatched where the failed chunk stood so the
request settles the usual way. Only a call that returned instead of reverting, or a JSON-RPC
refusal of the request's shape, sets the memo; it is per transport instance, negative only, and
sticky. Correctness never depends on it, only the count of wasted requests does.

**Chain definitions.** Whether a node needs to be told the frame is a fact of the chain, not of the
transport: geth grants an unspecified `eth_call` the provider's cap, Monad a fixed default. An
internal `src/chains` table records such facts by name, and the packer sends the stated `gasLimit`
as `gas` only where the chain needs it. The table is internal until its shape settles (APPS-1398).

**Observability.** The packer's facet reports chunks sent in each delivery, fallbacks by reason,
and whether the memo was set when the request began. `fixed_gas` becomes the lens's prologue less
the copy of the chunk's own bytes, so the recipe — read `fixed_gas` off the wide event, paste it
into the policy — is the same motion with a figure that no longer depends on how large the observed
pages were.

## Scope

Changes: the envelope's prologue and its pasted constant; the codec (the address, the argument
encoder, the two request builders, the retry-defeating predicate's second shape); the packer's
pricing, classification and fallback; the `batch.envelope` option and its documentation; the
chains table; forge, vitest and an opt-in anvil suite; the README and the paginated-lenses TIB's
predicate paragraph. Deliberately unchanged: the wire format, the record format, the telemetry
words, the cache's entry keys, `readLens`'s result shape, and the lens contract. The file-level plan
is APPS-1406.

## Verification

- The one constant, run as initcode and as code placed by override with the same arguments, yields
  identical records and identical gas telemetry for static, dynamic and compressed inputs; a lens
  whose constructor sets an immutable and writes storage returns the same values both ways; a
  factory that requires the envelope's `msg.sender` deploys in both.
- On a Prague node a chunk the initcode cap refuses pages by override; the same call with the third
  parameter dropped returns `0x`.
- Under a mocked provider that ignores or rejects the override, a request with the option returns
  the result it would have had without it, and the memo is set in exactly those two cases; a
  transient failure or an exhausted halving gets one initcode attempt and leaves the memo unset.
- The copy term matches the frame's `fixed` between a small and a large page within one percent;
  a lone element above the floor is declined with no request made.
- Cache entry keys for the same request are byte-identical with and without the option, and with
  the memo set or unset.

The full matrix, with the probe figures, is in APPS-1406.

## Open risks

- **The provider's request size limit replaces the initcode cap** and is discovered by halving,
  one round trip per level; a caller who sets `batchSize` to the provider's limit skips it.
- **With no `gasLimit` and no `batchSize` the opening chunk is the whole input**, which by override
  can fail to start on a Prague node and halves until it does. The paginated-lenses TIB accepts the
  same shape one cap in.
- **Lifting the cap widens every per-byte schedule mismatch.** Intrinsic gas and the floor are
  Ethereum's on Monad too; memory is not, so the copy is priced on Ethereum's schedule as a ceiling
  that under-packs by a percent or two there and never over-packs. A chain pricing memory above
  Ethereum would flip the direction; none is known.
- **The floor constant is fork-dependent.** A fork that raises it (EIP-7976 is scheduled to) makes
  the predicate optimistic until the constant is bumped; a refused chunk halves on the node's
  message in the meantime.
- **Pre-execution and parameter-shaped errors are matched by message.** An unfamiliar phrasing
  lands in the unproven branch: one wasted initcode retry per chunk, never a wrong answer.
- **A provider that honours overrides but caps their size or count** reaches initcode delivery
  through the exhausted branch, one halving cascade later, on every request.

## Notes

- **Override the envelope, not the lens.** The envelope has no constructor arguments beyond the
  tuple it reads and still deploys the lens through the factory, so the only lens trusted is the one
  this frame watched the factory build. The batching gain lives entirely in where the elements
  ride, which is the envelope's frame.
- **Branch-free prologue** because the loop must not move; the two no-op copies cost six gas and
  the switch variant compiled larger.
- **Delivery is not paid for in gas.** Measured in one frame, `fixed` and the per-attempt sum are
  identical in both deliveries; the creation frame costs more only outside them.
- **The predicate is the paginated-lenses TIB's, priced honestly.** Intrinsic gas was always
  delivery-specific and the copy was always a function of bytes; both were invisible under a 49 KB
  cap. Moving the copy out of `fixed` also retires that TIB's accepted over-pack.
- **The copy is modelled rather than left observed** because a candidate can be far larger than
  any page seen once the cap is lifted, and a frame that dies in the copy reports no page. The error
  is one-sided: exact on Ethereum-schedule chains, an under-pack elsewhere.
- **The floor, not intrinsic gas, is the binding byte term** on a Prague node; both lines are
  needed because geth gates on one and deducts the other.
- **Proof first** because the question a failed override chunk poses is not "what went wrong" but
  "did our code run". Every envelope-shaped outcome answers yes; only the rest can mean
  non-support, and among those only two shapes mean it unambiguously.
- **No positive memo** because a page needs no memory. **A negative memo only on unambiguous
  evidence** because "the override failed and the initcode retry succeeded" proves only that the
  chunk was serviceable, not why; a 429 followed by a 200 would otherwise mark a capable provider
  unsupported for the life of the transport.
- **`OOG_SENTINEL` stays terminal** because the predicate has already paid for the bytes; halving
  cannot shrink a constructor, and would bisect a broken lens into a complete-looking response.
- **The fallback assumes no byte cap the caller did not state**, so initcode delivery does not gain
  a default it has nowhere else; `MAX_INITCODE_SIZE` is Ethereum's figure and a caller passes it.
- **The request context is derived once, at entry**, because the cache's identity is built from it
  before the packer runs.
- **`batch.envelope` names what is delivered**, not the parameter it rides, and lives beside the
  other options that say how elements go over the wire.
- **The chain record names facts, not handling**, so a third chain that behaves a third way is an
  addition rather than a rename.
- **Opt-in, for now.** The fleet's providers have not been surveyed, and whether bytes bind for a
  lens is readable off its wide event first. Flipping the default is a one-line change.

## Derivation

- **Overriding the lens's runtime code**, the idea as first stated, was quantified and declined: it
  needs the runtime code as constructed, which no client-side step can produce for a constructor
  that reads chain state, it is blind to constructor storage writes, it saves about one percent of a
  large frame, and it would not lift the cap at all.
- **Designed first against `main`, then against PR #63**, which already prices intrinsic gas
  exactly and pools an observed cap; the first draft's own byte model dissolved into three
  corrections to the existing predicate.
- **Reading elements straight from calldata** by override was declined: a second input path
  through the one place the paginated-lenses TIB worked hardest to keep singular, for a term
  dominated eighteen-fold by the floor.
- **An opening byte cap for override delivery** (256 KiB) was declined as a default no other
  delivery has; letting a stated `gasLimit` admit bytes removed the common half of the case.
- **Halving on `OOG_SENTINEL`**, **a positive memo** and **a paired-observation negative memo** were
  first drafts caught in review (see Notes); the clean-room exploration independently proposed the
  narrower memo rule.
- **`out of gas` as a pre-execution phrase** was dropped: it also describes a frame that died
  mid-way, which is a prologue death the outcome protocol already routes.
- **The copy term's extent** was corrected twice in review, to the point where the budget is
  sampled; **"a lone element always fits" applied to the floor** was caught: the singleton rule is
  about the gas prediction, the floor is a protocol bound.
- **A per-chain memory schedule** was declined for now and filed as APPS-1398, with the export of
  the chains module and the initcode-limit question.
- **A dedicated support probe**, **two envelope constants**, **persisting the memo** across
  instances and **remembering the discovered request size cap** were each declined: the opening
  wave is the probe, one bytecode serves, a memo that outlives its provider is worse than none, and
  the caller owns `batchSize`.
- **`keccak256("viem-dlc-envelope")[12:]`** was the override address in the first draft; review
  noted it changes the factory's `msg.sender`, and the creation address dissolves the difference.
- **`tryStateOverride: boolean`** was the option's first name; `batch.envelope` came from the
  clean-room exploration.
- Probe figures and incidental findings (Monad's schedule, the pre-change constant's failure by
  override, measurement artefacts) are recorded in APPS-1406.
