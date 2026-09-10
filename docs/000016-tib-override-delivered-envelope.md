---
kind: tib
version: 0.0.16
related:
  - 000016-tib-paginated-lenses.md
---

# TIB — Override-delivered envelope: the frame's gas as the only chunk bound

Today every chunk is an `eth_call` that creates the envelope. The envelope's bytes and the chunk's
elements travel together as initcode, so a chunk can never exceed EIP-3860's 49,152 bytes, no matter
how much gas the node's frame would serve. This TIB adds `batch.envelope: "override"`. With it, a
chunk is a call to a fixed address whose code is the envelope, placed there by the state-override
parameter of `eth_call`, with the same argument tuple as calldata. The envelope still deploys the
lens through the factory inside the call, so nothing about the lens changes. If a provider doesn't
honor the override, the opening wave detects it and the range is fetched again as initcode.
APPS-1406 holds the implementation plan.

## Intent

- With `batch.envelope: "override"`, on a provider that honors state overrides, two things bound a
  chunk: the frame's gas, through the paginated-lenses predicate priced exactly for this delivery,
  and the provider's request size limit. The initcode cap doesn't apply.
- On a provider that doesn't honor state overrides, a request with the option returns the same
  result it would have returned without it. The cost is one wasted opening wave per request, and
  the wide event reports it, so the caller can turn the option off for that provider.
- One envelope bytecode, one constant, one drift guard. The two deliveries differ only in how the
  arguments reach the envelope. The loop, the records, the telemetry words, the admission guarantee,
  and the wire format are the paginated-lenses TIB's.
- Lens semantics are untouched. The factory deploys the lens inside the call in both deliveries. A
  deploy that runs out of gas is terminal in both. The cache's identity, which is the cleaned request
  the transport derives at entry, never sees the delivery.

## Context

The initcode cap was never a design choice. It's where the elements happened to ride. It binds
exactly for the lenses that need batching most: a cheap per-item function on a large input. A
2,500-gas item on a 50M-gas frame could be served 20,000 to a page, and a 49,152-byte chunk carries
700. Compression buys a few times that and then binds again. The paginated-lenses TIB gave the
client an exact picture of what a frame can serve and no way to send it that many elements. The
wide event already says whether bytes bind for a lens. When
`(gas_limit_observed − fixed_gas) / item_gas_avg` is well above the realized elements per chunk
(`elements_requested / nominal_batches`), the frame could have served more than the cap let
through. When it isn't, this TIB buys nothing for that lens.

The obvious alternative is to override the lens's code at its address and skip the factory. This
TIB rejects it. The compiler's `deployedBytecode` carries placeholder zeros where immutables go,
constructors write storage that the paginated-lenses TIB expressly blesses for shared tables, and a
code override sees neither. It also wouldn't lift the cap, because the elements would still ride as
initcode. Placing the envelope by override leaves the lens's whole deployment path alone, and the
envelope, not the lens, carries the elements.

The third parameter of `eth_call` isn't universal. Some providers reject it, and some ignore it. The
package already uses it for the `policy` sentinel, which the transports strip at entry, and forwards
a caller's own overrides. A caller who passes none has never exercised a provider's support. For
that reason, the option is opt-in.

## Design

**One bytecode, two deliveries.** The envelope's prologue copies its arguments from wherever this
delivery put them. As initcode, the arguments trail the code and there's no calldata. As runtime
code placed by an override, the arguments are the calldata and nothing trails the code. Each copy is
a no-op in the other delivery, so there's no branch, no mode bit, and no second constant. The
arrival `gas()`, the deploy, the loop, and the exit are unchanged, and so is everything the factory
sees.

**Where the envelope runs.** By override, the chunk is a call to the address that `CREATE` gives a
contract made by the zero address at nonce 0. That's the address the envelope already has in
creation delivery, because an `eth_call` without `from` is sent from the zero address, whose nonce
is 0 on every chain. The factory is therefore called from the same `msg.sender` in both deliveries,
and it deploys the lens inside the call exactly as today. The envelope's code entry exists only in
the outbound request, never in the request context that the cache keys from. A caller's own
override at that address is a protocol error.

**What the predicate learns.** The paginated-lenses predicate keeps deciding every chunk. Three
things it priced implicitly under a 49 KB cap become explicit at a megabyte. All three are
client-side, and none touch the telemetry. First, intrinsic gas depends on the delivery: by override
there's no creation base and no initcode words. Second, the prologue's copy of the argument tuple
is the chunk's cost, not the lens's, so it leaves `fixed` and is added back per candidate. Third,
since Prague a frame must clear EIP-7623's floor to start at all, so a lone element above the floor
is declined as oversize, the same way an element above the byte cap is. A stated `gasLimit` bounds
the opening wave's bytes on its own. When neither `gasLimit` nor `batchSize` is stated, nothing is
assumed on the caller's behalf.

**Choosing and falling back.** A request with the option opens by override. Every response to an
override chunk is classified first by whether it proves the envelope ran. A page or any
envelope-shaped revert is handled as today. A size or timeout refusal halves by override while
there's room. Everything else gets one initcode attempt for the same range, dispatched where the
failed chunk stood, so the request settles the usual way. The fallback's reason is reported: a call
that returned instead of reverting or a JSON-RPC refusal of the request's shape says the provider
doesn't honor overrides, and anything else is unproven. Nothing is remembered between requests. The
option is the lever, and the wide event tells the caller when to pull it.

**Chain definitions.** Whether a node needs to be told the frame is a fact of the chain, not of the
transport. geth grants an unspecified `eth_call` the provider's cap, and Monad grants a fixed
default. An internal `src/chains` table records such facts by name, and the packer sends the stated
`gasLimit` as `gas` only where the chain needs it. The table stays internal until its shape settles
(APPS-1398).

**Observability.** The packer's facet reports the chunks sent in each delivery and the fallbacks by
reason. `fixed_gas` becomes the lens's
prologue less the copy of the chunk's own bytes. The recipe is the same motion as before: read
`fixed_gas` off the wide event and paste it into the policy. The figure no longer depends on how
large the observed pages were.

## Scope

Changes: the envelope's prologue and its pasted constant; the codec (the address, the argument
encoder, the two request builders, and the second shape of the retry-defeating predicate); the
packer's pricing, classification, and fallback; the `batch.envelope` option and its documentation;
the chains table; forge, vitest, and an opt-in anvil suite; the README and the paginated-lenses
TIB's predicate paragraph.

Deliberately unchanged: the wire format, the record format, the telemetry words, the cache's entry
keys, the result shape of `readLens`, and the lens contract.

APPS-1406 holds the file-level plan.

## Verification

- The one constant, run as initcode and as code placed by override with the same arguments, yields
  identical records and identical gas telemetry for static, dynamic, and compressed inputs. A lens
  whose constructor sets an immutable and writes storage returns the same values both ways. A
  factory that requires the envelope's `msg.sender` deploys in both.
- On a Prague node, a chunk the initcode cap refuses pages by override. The same call with the third
  parameter dropped returns `0x`.
- Under a mocked provider that ignores or rejects the override, a request with the option returns
  the result it would have returned without it and reports one unsupported fallback, on every
  request. A transient failure or an exhausted halving gets one initcode attempt and reports it as
  such.
- The copy term matches the frame's `fixed` between a small and a large page within 1%. A lone
  element above the floor is declined with no request made.
- Cache entry keys for the same request are byte-identical with and without the option.

APPS-1406 holds the full matrix with the probe figures.

## Open risks

- **The provider's request size limit replaces the initcode cap.** It's discovered by halving, one
  round trip per level. A caller who sets `batchSize` to the provider's limit skips the discovery.
- **With no `gasLimit` and no `batchSize`, the opening chunk is the whole input.** By override, it
  can fail to start on a Prague node and halves until it does. The paginated-lenses TIB accepts the
  same shape one cap in.
- **Lifting the cap widens every per-byte schedule mismatch.** Intrinsic gas and the floor are
  Ethereum's on Monad too. Memory isn't, so the copy is priced on Ethereum's schedule as a ceiling.
  On Monad that under-packs by a percent or two and never over-packs. A chain that prices memory
  above Ethereum would flip the direction. None is known.
- **The floor constant depends on the fork.** A fork that raises it (EIP-7976 is scheduled to) makes
  the predicate optimistic until the constant is bumped. In the meantime, a refused chunk halves on
  the node's message.
- **Pre-execution and parameter-shaped errors are matched by message.** An unfamiliar phrasing
  lands in the unproven branch. The cost is one wasted initcode retry per chunk, never a wrong
  answer.
- **A provider that honors overrides but caps their size or count** reaches initcode delivery
  through the exhausted branch, one halving cascade later, on every request.

## Notes

- **Override the envelope, not the lens.** The envelope has no constructor arguments beyond the
  tuple it reads, and it still deploys the lens through the factory. The only lens trusted is the
  one this frame watched the factory build. The batching gain lives entirely in where the elements
  ride, which is the envelope's frame.
- **Branch-free prologue,** because the loop must not move. The two no-op copies cost 6 gas, and
  the switch variant compiled larger.
- **Delivery isn't paid for in gas.** Measured in one frame, `fixed` and the per-attempt sum are
  identical in both deliveries. The creation frame costs more only outside them.
- **The predicate is the paginated-lenses TIB's, priced honestly.** Intrinsic gas was always
  delivery-specific, and the copy was always a function of bytes. Both were invisible under a 49 KB
  cap. Moving the copy out of `fixed` also retires that TIB's accepted over-pack.
- **The copy is modeled rather than left observed,** because once the cap is lifted a candidate can
  be far larger than any page seen, and a frame that dies in the copy reports no page. The error is
  one-sided: exact on Ethereum-schedule chains, an under-pack elsewhere.
- **The floor, not intrinsic gas, is the binding byte term** on a Prague node. Both lines are
  needed, because geth gates on one and deducts the other.
- **Proof first,** because the question a failed override chunk poses isn't "what went wrong" but
  "did our code run". Every envelope-shaped outcome answers yes. Only the rest can mean non-support,
  and among those only two shapes mean it unambiguously.
- **Nothing is remembered between requests.** Whether to try the override is the caller's choice
  per call site, and `override_fallbacks_unsupported` on the wide event says when a provider doesn't
  honor it. A memo would be an adaptive heuristic beside an explicit lever, and a wrong one, set by a
  429 followed by a 200, would outlive the provider it described.
- **`OOG_SENTINEL` stays terminal,** because the predicate has already paid for the bytes. Halving
  can't shrink a constructor, and it would bisect a broken lens into a response that looks complete.
- **The fallback assumes no byte cap the caller didn't state,** so initcode delivery doesn't gain a
  default it has nowhere else. `MAX_INITCODE_SIZE` is Ethereum's figure, and a caller passes it.
- **The request context is derived once, at entry,** because the cache's identity is built from it
  before the packer runs.
- **`batch.envelope` names what's delivered,** not the parameter it rides, and it lives beside the
  other options that say how elements go over the wire.
- **The chain record names facts, not handling,** so a third chain that behaves a third way is an
  addition rather than a rename.
- **Opt-in, for now.** The fleet's providers haven't been surveyed, and whether bytes bind for a
  lens is readable off its wide event first. Flipping the default is a one-line change.

## Derivation

- **Overriding the lens's runtime code,** the idea as first stated, was quantified and declined. It
  needs the runtime code as constructed, which no client-side step can produce for a constructor
  that reads chain state. It's blind to constructor storage writes. It saves about 1% of a large
  frame, and it wouldn't lift the cap at all.
- **Designed first against `main`, then against PR #63,** which already prices intrinsic gas
  exactly and pools an observed cap. The first draft's own byte model dissolved into three
  corrections to the existing predicate.
- **Reading elements straight from calldata** by override was declined. It adds a second input path
  through the one place the paginated-lenses TIB worked hardest to keep singular, for a term that the
  floor dominates eighteenfold.
- **An opening byte cap for override delivery** (256 KiB) was declined as a default no other
  delivery has. Letting a stated `gasLimit` admit bytes removed the common half of the case.
- **Halving on `OOG_SENTINEL`** was a first draft that review caught (see Notes).
- **A per-transport memo of non-support,** negative only and set on unambiguous evidence, was
  designed and implemented, then removed: it saved one wave per request on an unsupporting provider
  at the price of state across three layers, where the option and the wide event already give the
  caller the same control (see Notes).
- **`out of gas` as a pre-execution phrase** was dropped. It also describes a frame that died
  midway, which is a prologue death the outcome protocol already routes.
- **The copy term's extent** was corrected twice in review, to the point where the budget is
  sampled. **"A lone element always fits" applied to the floor** was caught too: the singleton rule
  is about the gas prediction, and the floor is a protocol bound.
- **A per-chain memory schedule** was declined for now and filed as APPS-1398, together with the
  export of the chains module and the initcode-limit question.
- **A dedicated support probe, two envelope constants, and remembering the discovered request size
  cap** were each declined. The opening wave is the probe, one bytecode serves, and the caller owns
  `batchSize`.
- **`keccak256("viem-dlc-envelope")[12:]`** was the override address in the first draft. Review
  noted that it changes the factory's `msg.sender`, and the creation address dissolves the
  difference.
- **`tryStateOverride: boolean`** was the option's first name. `batch.envelope` came from the
  clean-room exploration.
- APPS-1406 records the probe figures and incidental findings: Monad's schedule, the pre-change
  constant's failure by override, and the measurement artifacts.
