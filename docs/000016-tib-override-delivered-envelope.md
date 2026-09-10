---
kind: tib
version: 0.0.16
related:
  - 000016-tib-paginated-lenses.md
---

# TIB — Override-delivered envelope: the frame's gas as the only chunk bound

Every chunk today is an `eth_call` that *creates* the envelope: the envelope's bytes and the chunk's
elements travel together as initcode, so a chunk can never exceed EIP-3860's 49,152 bytes — about
700 static 64-byte elements uncompressed — however much gas the node's frame would have served.
This TIB adds `batch.envelope: "override"`: a chunk is delivered as a call *to* a fixed address
whose code is the envelope, placed by `eth_call`'s state-override parameter, with the same
constructor-argument tuple as calldata. The envelope still deploys the lens through the factory
inside the call, so constructors, immutables and constructor storage writes are exactly as they are
today; nothing about the lens changes hands. The paginated-lenses TIB's one predicate keeps deciding
every chunk; it learns that intrinsic gas depends on the delivery, that the prologue's copy of the
arguments is a function of the chunk's bytes rather than of the lens, and that a frame must clear
EIP-7623's floor to start. A provider that does not honour the override is detected on the opening
wave and the range is re-fetched as initcode; only unambiguous non-support is remembered.

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
one this TIB rejects, and the suspicion that motivated the exploration is correct: the compiler's
`deployedBytecode` carries placeholder zeros where immutables go, constructors write storage the
paginated-lenses TIB expressly blesses for shared tables, and neither is visible to a code
override. It would also not lift the cap: the elements would still ride as initcode. Placing the
*envelope* by override instead leaves the lens's whole deployment path alone, and it is the
envelope, not the lens, that carries the elements. The Derivation quantifies what the rejected
variant would have bought.

`eth_call`'s third parameter is not universal: some providers reject it, some ignore it. The
package rides it for the `policy` sentinel, which the transports strip at entry, and forwards a
caller's own overrides; a caller who passes none has never exercised a provider's support. The
option is opt-in for that reason.

## Design

### One bytecode, two deliveries

The envelope's prologue (`src/utils/deployless/Envelope.yul:55`) copies its arguments from wherever
this delivery put them:

```yul
let argsEnd := add(add(base, sub(codesize(), bytecodeLen)), calldatasize())
codecopy(base, bytecodeLen, sub(codesize(), bytecodeLen))
calldatacopy(base, 0, calldatasize())
```

As initcode, `CREATE` supplies no calldata and the arguments trail the code; as runtime code placed
by an override, nothing trails the code and the arguments are the calldata. Each copy is a no-op
in the other delivery. No branch, no mode bit, no second constant: the pasted
`FACTORY_BYTECODE_REVERT` is both the initcode and the override code. The arrival `gas()`, `deploy`,
`paginate` and the exit are unchanged, and so is everything the factory sees, `msg.sender`
included (below).

### The request context

At entry, each handler derives what it does today (`src/transports/deployless/index.ts:89`,
`src/transports/cache/eth-call/handler.ts:51`) — the policy removed, the caller's block selector,
state and block overrides kept — and that cleaned parameter set is immutable for the request. The
cache keys entries from it, as it does now (`src/transports/cache/keychain.ts:59`); the delivery
builders below take it as input and never return it. The envelope's override entry exists only in
outbound params, and a caller override naming `ENVELOPE_ADDRESS` is a protocol error, thrown.

### Chain definitions

Whether a node needs to be told the frame is a fact of the chain, not of the transport or the
operator, and this TIB's probing surfaced the first two such facts the package must know. They live
in `src/chains`, with named entries in viem's manner (`mainnet`, `base`, `arbitrum`, `robinhood`,
`monad`), a `chains` list and `chainDefinition(id)`, which returns geth's behaviour for a chain
without an entry or a client without a chain. The module is internal for now (Derivation):

```ts
type ChainDefinition = {
  id: number;
  name: string;
  ethCall: {
    gasWhenUnspecified: "providerCap" | "fixedDefault";   // the frame a request that leaves `gas` unspecified runs in
    gasAboveCap: "clamped" | "rejected";                    // what the node does with a `gas` above the provider's cap
  };
};
```

geth gives an unspecified request the whole cap and clamps a higher `gas`; Monad gives it a fixed
8.1M default, promoted to a larger pool only on out-of-gas — which a paging envelope never is — and
rejects a `gas` above the cap. Both transports look the client's chain up and pass the definition to
`factorisedFactoryCall`, which sends the stated `gasLimit` as every chunk's `gas` on a
`fixedDefault` chain and nothing elsewhere, so `gas_limit_observed` keeps reading the true cap
wherever a node would have revealed it. The record names facts of the chain, never the package's
handling of them, so that a later fact (the initcode limit, the memory schedule; APPS-1398) is an
addition rather than a rename.

### The override-delivered chunk

```
eth_call [{ to: ENVELOPE_ADDRESS, data: abi.encode(target, wire, factory, factoryData, config) },
          block,
          { ...callerOverrides, [ENVELOPE_ADDRESS]: { code: FACTORY_BYTECODE_REVERT } }]
```

`ENVELOPE_ADDRESS` is `0xBd770416a3345F91E4B34576cb804a576fa48EB1`: the address `CREATE` gives a
contract made by the zero address at nonce 0, which is where the envelope already runs in creation
delivery — an `eth_call` that omits `from` is sent from the zero address, whose nonce is 0 on every
chain because nobody can sign for it (confirmed on anvil and Monad; the account holds no code). So
the factory is called from the same address in both deliveries, and a factory that folds
`msg.sender` into its salt or checks it deploys the same lens at the same `target`. The lens is
deployed by the factory inside the call as today: `deploy` sees no code at `target`, calls the
factory, and checks the code appeared. `wrapDeploylessFactoryCall` (`src/utils/deployless/codec.envelope.ts:149`)
becomes two builders of outbound `params`, one per delivery, over the request context.
`isRevertExpected` (`codec.envelope.ts:171`) learns the second shape — `to` equal to
`ENVELOPE_ADDRESS` *and* the override entry present in the third parameter — so the
retry-defeating boundaries keep working without touching unrelated calls to that address.

### The predicate, priced for the delivery

The paginated-lenses TIB decides every chunk with

```
k = 1    or    intrinsic(bytes) + fixed + k·avg + z·stddev·√k ≤ cap
```

and prices `intrinsic` exactly from the chunk's bytes (`src/utils/deployless/call.ts:486`). Three
things change, all in the client, none in the telemetry:

**`intrinsic` takes the delivery.** As initcode: `21,000 + 32,000 + 4·z + 16·nz + 2·⌈b/32⌉ + 2`, as
today. By override: `21,000 + 4·z + 16·nz + 2` — no creation base, no initcode words. A page's
implied `cap = intrinsic + fixed + budget` (`call.ts:429`) uses the intrinsic of the delivery that
fetched it, so a request that changes delivery mid-way pools one cap.

**The argument copy leaves `fixed`.** Before `paginate` samples the budget, the prologue copies the
argument tuple — `a` bytes: the whole of `data` by override, `data` less the envelope's own bytes as
initcode — into memory from `base = 0x80`, lays the frame behind it and writes the slab's sentinel
word; the sample (`Envelope.yul:141`) follows that write, so memory has been expanded to one word
past the slab's start. That costs

```
copy(a) = 3·⌈a/32⌉ + memcost(0x80 + a + FRAME + 0x20)      memcost(x) = 3·w + ⌊w²/512⌋, w = ⌈x/32⌉
```

with `FRAME` the frame's words (`0x1c0`, plus the history when compressed), read off
`Envelope.yul`'s layout and pinned in forge. The telemetry words and the first record's slot are
written after the sample and land in the first attempt's cost, where their expansion depends on
position by a few gas.
Under the initcode cap that is ~15k and was carried inside `fixed` as an accepted error (the
paginated-lenses TIB's Open risks); at 1 MiB it is 2.3M and cannot be. The pool subtracts it from
every page's reported word before taking the maximum — `fixed₀ = max(fixed − copy(a sent))`, the
lens's prologue and nothing that scales with bytes beyond the few gas above — and the predicate
adds the candidate's own:
`intrinsic(d) + copy(a) + fixed₀ + …`. What remains byte-dependent after the subtraction is the
first attempt's staging write past the slab, a few tens of gas, inside the item statistics where it
belongs.

**The floor.** Since Prague a message starts only if its gas limit is at least
`21,000 + 10·z + 40·nz` (EIP-7623); geth checks it before execution and deducts only the ordinary
intrinsic from the frame. The predicate gains a second line, `floor(d) ≤ cap`. Unlike the first it
binds a lone element too: a singleton above the floor is refused by the node before anything runs,
so it is declined client-side as oversize, exactly as an element above the byte cap is, and lands in
`elements_declined_oversize`. Under the initcode cap the floor is at most ~2M; at 1 MiB of
compressed input it is 42M and is the binding term on a 50M cap, ahead of intrinsic (17M) and the
copy (2.3M).

The stated figures size the opening wave exactly as the paginated-lenses TIB says, through the same
predicate, so `gasLimit` bounds the opening wave's bytes as well as its items in either delivery.
The two byte lines need only a cap, not an item cost, so a stated `gasLimit` admits bytes on its
own: with `gasLimit` but no `batch.gas` the opening wave runs the byte lines against the stated cap
and packs items by bytes alone, where the paginated-lenses TIB packed by bytes alone outright. With
no `gasLimit` nothing is known about the frame and the opening wave packs under `batch.batchSize`;
without that too, it is the whole input, which halves on the node's pre-execution message until it
starts — the same discovery initcode delivery runs today, and no cap is assumed on the caller's
behalf.

A pre-execution refusal — an error mentioning `intrinsic gas` or `floor data gas` — is a size
failure in either delivery and halves: bytes caused it and fewer bytes cure it. A fallback into
initcode delivery packs exactly as an initcode request would have: under `batch.batchSize` when the
caller set one, and otherwise as one chunk that halves on the node's initcode-size error, as today.
No cap is assumed on the caller's behalf; `MAX_INITCODE_SIZE` is Ethereum's figure, and a caller who
wants it, or a chain's higher one, passes it.

### Choosing, falling back, remembering

`factorisedFactoryCall` (`call.ts:81`) takes a `delivery` record the transport owns: `{ unsupported:
boolean }`. A request with `envelope: "override"` opens in override delivery unless the memo is set;
a request without it never reads or writes the memo.

Every response to an override-delivered chunk is classified first by whether it *proves the
envelope ran*:

- **Proof:** a page, `MalformedResult`, `MalformedInput`, `CounterfactualDeployFailed`,
  `OOG_SENTINEL`. Handled exactly as the outcome protocol says today — `OOG_SENTINEL` stays
  terminal: the factory or constructor is what ran out, and the predicate has already paid for the
  bytes.
- **Size or timeout** (`classifyChunkError`, `call.ts:627`, plus the two pre-execution phrases):
  halve, still in override delivery, as today. When halving is exhausted — a singleton, or the
  timeout budget spent — the range gets one initcode attempt (below) before the error propagates,
  because a provider that rejects the fixed 2.6 KB entry or times out on an unknown parameter looks
  exactly like this.
- **Non-support, unambiguous:** the call returned without reverting (an ignoring provider: the call
  reached an empty account; `fetchChunk` already throws here, `call.ts:560`), or a JSON-RPC
  invalid-params error (`-32602`, or a message naming the third parameter or state overrides). Memo
  → `unsupported`.
- **Anything else:** no memo.

Every non-proof outcome ends the same way: the chunk's indices are re-packed as initcode — under
`batchSize` when stated, otherwise as one chunk — with the initcode predicate, and each piece is
`dispatch`ed as initcode at the failed chunk's generation, exactly as a chunk's halves are (`call.ts:326`), so the pending list waits for them as
it would for any chunk of that generation and a failure among them settles the request the usual
way. Each piece is attempted once; its errors propagate as today, size errors included. An element
the stated `batchSize` or the predicate's byte lines cannot carry alone is declined as oversize on
the way, which is what a request without the option would have done with it. Halves, singleton escalations and pooled tails are chunks like any other and take
the request's current delivery, so once the memo is set — during this request or before it — every
later chunk is initcode. The memo is per transport instance and sticky; correctness never depends
on it, only the count of wasted requests does, so a `failover` of providers with different answers
is served by the per-chunk rule and re-learns nothing worse than one wave.

### Observability

On the packer's facet, beside the existing fields and with the same absence on a full cache hit or
an empty input: `chunks_override` and `chunks_initcode` (requests sent in each delivery),
`override_fallbacks` (ranges re-fetched as initcode, by reason: `unsupported`, `unproven`,
`exhausted`), and `delivery_memo` (whether the memo was set when the packer began). `fixed_gas`
becomes `fixed₀` and `batch.gas.fixed` is stated in the same units, so the recipe — read
`fixed_gas` off the wide event, paste it into the policy — is the same motion with a figure that no
longer depends on how large the observed pages were. A `fixed` figure recorded before this change
includes a copy of up to ~15k and over-states by that much: one continuation's worth of
under-packing, never an over-pack. `frame_gas`, `gas_limit_observed` and the item fields keep their
meaning.

## Scope & files

- `src/chains/index.ts`: the definitions, internal; the transports resolve the client's chain and
  pass it to `factorisedFactoryCall`, which sends `gasLimit` as `gas` on a `fixedDefault` chain.
  Landed first, on its own, since PR #63 gains from it on Monad without the rest of this TIB.
- `src/utils/deployless/Envelope.yul`: the branch-free prologue; regenerated constant (2,626 bytes,
  up 8; `fixed` up 21 gas).
- `src/utils/deployless/codec.envelope.ts`: `ENVELOPE_ADDRESS`; the two outbound builders over the
  request context; `isRevertExpected`'s second shape.
- `src/utils/deployless/call.ts`: `intrinsicGas` by delivery; `copy` in the pool and the predicate;
  `floor` as the predicate's second line; the two pre-execution phrases in `classifyChunkError`; the
  `delivery` record, proof-first classification, one initcode attempt per non-proof chunk, the memo
  rule; the fallback's byte cap; the fields.
- `src/transports/state-overrides.ts`, `src/actions/call.ts`, `src/actions/read-lens.ts`: the
  `batch.envelope` option and its documentation; `MAX_INITCODE_SIZE`'s doc names it Ethereum's
  figure and the natural `batchSize` on chains that keep it.
- `src/transports/deployless/index.ts`, `src/transports/cache/eth-call/handler.ts`: the request
  context stays where it is derived and becomes immutable; the memo lives in the transport closure
  beside `gasLimit` and is passed through.
- `test/forge`: both deliveries of one bytecode; `Env` gains an override runner (`vm.etch` +
  `call`); `copy` pinned; regenerated `.gas-snapshot`.
- `test/transports`, `test/utils/deployless`: the fallback matrix and the pricing cases under a
  mocked provider.
- `README.md` (`deployless`, `policy`): the option, the instrument that says whether it will pay,
  and `batchSize` guidance per delivery. The examples live on `examples-and-bench` and take the
  same edit once this lands.
- `docs/000016-tib-paginated-lenses.md`: the predicate's statement and the `fixed`-grows-with-bytes
  risk, edited to point here.
- Not changed: the wire format, the record format, the telemetry words, `keychain.ts`, the cache's
  entries, `readLens`'s result shape, the lens contract and its requirements.

## Verification

- Forge: the regenerated constant, run as initcode and as etched code called with the same
  arguments, yields identical records and identical `Σg`, `gmax`, `nA` and `fixed` for static,
  dynamic-in, dynamic-out and compressed inputs (the probe: `fixed = 316,001` and `Σg = 940,410` in
  both deliveries at 500 elements); a lens whose constructor sets an immutable and writes a storage
  slot returns the same values in both deliveries; a factory that requires `msg.sender ==
  ENVELOPE_ADDRESS` deploys in both (the pre-change constant, etched, failed with
  `CounterfactualDeployFailed` in the probe: the option cannot ship without the Yul change); `copy(a)` matches
  the difference in `fixed` between a 5- and a 500-element page within 1% (the probe: 8,063
  predicted against 8,121 measured) on the clear and the compressed path; the per-element gas of
  both paths is within the snapshot's tolerance of today's.
- Anvil, Prague (the probe that produced this TIB, kept as a test): a 1,000-element chunk (67,833
  bytes) fails as initcode with `max initcode size exceeded` and returns `nA = 1000` by override;
  the same call with the third parameter dropped returns `0x`; a chunk whose `floor` exceeds the
  configured gas cap fails with the node's floor-data-gas error text, and that text halves.
- Vitest, mocked provider: honouring — every chunk is `to`-shaped and carries the code entry, the
  caller's own overrides are merged, `chunks_initcode` is zero, the memo is never set; ignoring
  (`0x`) and invalid-params — the opening wave falls back, the chunk is re-packed under the
  initcode cap rather than halved, the memo is set, and a second request on the same transport
  opens as initcode; transient — an override chunk fails with a 500, its initcode retry *succeeds*,
  the result is complete and the memo is still unset; exhausted — a 413 that persists to a singleton
  by override gets one initcode attempt and succeeds; `OOG_SENTINEL` by override throws as today and
  is never `unresolved`; a caller override at `ENVELOPE_ADDRESS` throws; without the option the
  memo is never read; `isRevertExpected` is true for the `to`-shape with the entry and false without
  it; a `failover` whose two providers answer differently completes with a correct result; pooled
  tails after a fallback go out as initcode.
- Vitest, pricing: `intrinsic` by override omits exactly the creation base and the initcode words,
  to within one gas of the request sent; a page's `fixed` less `copy` of its own bytes pools to the
  same `fixed₀` from a small and a large page; a candidate's `copy` grows quadratically and a
  multi-megabyte candidate is refused on it against a small cap; a candidate whose `floor` exceeds
  `cap` is refused whatever its item count, and a lone element above the floor is declined as
  oversize with no request made; a fallback's pieces enter the pump at the failed chunk's generation
  and a pending tail waits for them; `gas_limit_observed`
  agrees across deliveries for the same mock cap; a fallback re-packs under the caller's `batchSize`
  and, without one, as a single chunk that halves on the initcode-size error; `fixed_gas` from a 5- and a 500-element page agree
  within the first attempt's positional expansion.
- Cache: entry keys for the same request are byte-identical with and without the option, and with
  the memo set or unset.
- Chains: Monad's id sends `gasLimit` as `gas` on every chunk; mainnet, an unknown chain and an
  unstated `gasLimit` send none; `chainDefinition` of an unknown id is geth's behaviour.

## Open risks

- **The provider's request size limit replaces the initcode cap** and is not known in advance;
  it is discovered by size-halving, one round trip per level. A caller who sets `batchSize` to the
  provider's limit skips the discovery. No memo of the discovered size is kept (Derivation).
- **With no `gasLimit` and no `batchSize`, the opening chunk is the whole input**, which by
  override can fail to start on a Prague node and halves until it does, one round trip per level.
  The paginated-lenses TIB accepts the same shape one cap in, and a caller with a large input states
  either figure to skip it.
- **Lifting the cap widens every per-byte schedule mismatch.** The paginated-lenses TIB notes
  that a chain pricing bytes differently from Ethereum shifts the prediction "as chunk size varies";
  chunk size varied over 49 KB and now varies over megabytes. Probed on Monad mainnet (chain 143,
  `rpc.monad.xyz`, 2026-09-09): intrinsic gas is Ethereum's to the gas (4 per zero byte, 16 per
  non-zero, 2 per initcode word, 53,000 base) and the EIP-7623 floor applies (10 and 40 per byte
  via `eth_estimateGas`), so the two byte terms the predicate reads from the schedule are portable
  to the one chain in the fleet with a repriced EVM. Memory is not: expanding to 1 MiB cost 16,394
  gas there (`w/2`) against 2,195,456 on Ethereum's `3w + w²/512`. `copy` therefore prices memory
  on Ethereum's schedule as a *ceiling*: on Monad it over-charges by 2.2M at 1 MiB, an under-pack of
  1.5% of a 150M frame, never an over-pack. A chain pricing memory above Ethereum would flip the
  direction; none is known.
- **The floor constant is fork-dependent.** `floor` uses Prague's 10 per token; a fork that raises
  it (EIP-7976 is scheduled to) makes the predicate optimistic by the difference until the constant
  is bumped, and a chunk refused for it halves on the node's floor-data-gas message in the meantime.
  A chain without the floor would be under-packed to `cap / 40` bytes where `cap / 16` would start;
  Monad has it, so no fleet chain is known to.
- **Pre-execution and parameter-shaped errors are matched by message.** The phrases are geth's; an
  unfamiliar phrasing lands in the unproven branch, which costs one initcode retry per chunk and
  never sets the memo, so the failure mode is waste, not a wrong answer.
- **`copy` reads the envelope's memory layout** — `base`, the frame's words, the slab head — into
  two client constants. A layout change moves them; the forge pin catches it, and the error of a
  stale constant is a few hundred gas per chunk, not a function of bytes.
- **A provider that honours overrides but caps their size or count** reaches initcode delivery
  through the exhausted branch, one halving cascade later, on every request.

## Notes

- **Override the envelope, not the lens.** The suspicion that constructors and immutables block
  this idea is correct for a lens override and irrelevant for an envelope override: the envelope
  has no constructor arguments of its own beyond the tuple it reads, and it still deploys the lens
  through the factory, so `deploy`'s guarantee — the only lens trusted is the one this frame
  watched the factory build — holds unchanged. The batching gain lives entirely in where the
  elements ride, which is the envelope's frame.
- **Branch-free prologue** because the loop must not move. A `switch calldatasize()` variant
  measured the same per element but compiled 18 bytes larger; the two no-op copies cost six gas.
  The forge gas snapshot moved by ~3k per 100 elements on the change, which is the test harness's
  storage-held envelope growing by one word, not the loop: measured in one frame, `Σg` is identical
  and `fixed` is up 21 gas at 10 and at 110 elements.
- **Delivery does not change what the frame does.** Measured in one frame on the fixture lens at
  500 elements against this baseline's envelope, `fixed` and `Σg` are identical in both deliveries
  and the creation frame costs 110k more outside them, of which 32,000 is the `CREATE` itself.
  Override delivery is not paid for in gas.
- **The predicate is the paginated-lenses TIB's, priced honestly.** Nothing is added to the
  telemetry and no second model appears: `intrinsic` was always delivery-specific and the copy was
  always a function of bytes; both were invisible under a 49 KB cap and are not under a megabyte.
  Moving the copy out of `fixed` also retires that TIB's accepted over-pack in initcode delivery.
- **Why the copy is modelled rather than left observed, given that memory pricing is not
  portable.** Left inside `fixed`, the term is only ever as large as the largest page seen, and on
  an Ethereum-schedule chain with a high cap a candidate can be far larger: the floor admits
  3.75 MiB at 150M, whose expansion alone is 28M, and a frame that dies in the copy reports no page
  and cannot be continued. Modelled on Ethereum's schedule the error is one-sided: exact on
  Ethereum-schedule chains, and on Monad an over-charge that under-packs by a percent or two. Safe
  in the direction the earlier TIBs chose everywhere else — a wrong figure costs requests, never a
  result.
- **The floor, not intrinsic gas, is the binding byte term** on a Prague node: 40 per non-zero
  byte against a frame of tens of millions puts a 50M cap's ceiling near 1.2 MiB of compressed
  input, under most providers' body limits. Both lines are needed: geth gates on the floor and
  deducts the intrinsic, and they are different numbers.
- **Pre-execution refusals halve in both deliveries** because bytes caused them in both; under the
  initcode cap they cannot occur, so the rule changes nothing there. `out of gas` is deliberately
  not in the list: a frame that dies without a page is, in this design, a prologue death, which the
  outcome protocol already routes.
- **Proof first** because the question a failed override chunk poses is not "what went wrong" but
  "did our code run". Every envelope-shaped outcome answers yes and is handled as today; only the
  rest can mean non-support, and among those only two shapes mean it unambiguously.
- **No positive memo** because a page needs no memory: the next override attempt is already the
  right move. **A negative memo only on unambiguous evidence** because a paired observation — the
  override attempt failed, the initcode retry succeeded — proves only that the chunk was
  serviceable, not why the first attempt failed; a 429 followed by a 200 would have marked a capable
  provider unsupported for the life of the transport.
- **`OOG_SENTINEL` stays terminal** because the predicate has already paid for the bytes: the only
  gas the deploy can be short of is the constructor's, and halving does not shrink a constructor.
  Halving it would bisect a broken lens into a complete-looking response with every element
  `unresolved`.
- **The fallback assumes no byte cap the caller did not state.** PR #63 honours `batchSize` or
  has none, and `batchSize` has been optional since the option existed; a fallback that packed under
  `MAX_INITCODE_SIZE` on its own would give initcode delivery a default it does not have anywhere
  else, and one that is wrong on a chain with a higher limit. A caller who wants the Ethereum figure
  passes it, as the examples do. The price, without one, is halving from an override-sized chunk
  down to what the node accepts, one round trip per level, the same discovery PR #63 runs today.
- **The request context is derived once, at entry,** because the cache's identity is built from it
  and coalescing happens before the packer runs; a delivery builder that mutated or re-derived it
  would let `batch`, the option or the envelope entry leak into keys.
- **`batch.envelope` names what is delivered,** not the parameter it rides: a reader of
  `envelope: "override"` is not led to think the lens is overridden, and the value has room for a
  third delivery if one ever exists. It lives in `batch` beside `batchSize`, `compress`, `gas` and
  `continuations`: all say how elements go over the wire, none says what they mean, and `readLens`
  already forwards `batch` whole.
- **The chain record names facts, not handling.** `gasWhenUnspecified` and `gasAboveCap` say what
  a node does; whether to send `gasLimit` is derived from them at the one place that sends. An
  earlier draft named the field `gasFrame: "cap" | "pool"`, which described the package's response
  in Monad's vocabulary and would have needed renaming the first time a third chain behaved a third
  way.
- **Opt-in, for now.** The fleet's providers have not been surveyed for override support, and
  whether bytes bind for a lens is readable off its wide event before the option is set. Once both
  are known, flipping the default is a one-line change and a doc edit, not a design.

## Derivation

- **Overriding the lens's runtime code** — the idea as first stated — was declined after
  quantifying it. What it needs: the runtime code *as constructed*, which is not the compiler's
  `deployedBytecode` (soltag's own docs note the placeholder zeros in immutable slots) and which
  no client-side step can produce for a constructor that reads chain state. It could be fetched
  by a probe that deploys through the factory and `extcodecopy`s the result — one request, cached
  per `(factory, factoryData)` — but a constructor's storage writes are invisible to that probe and
  to any code override, and there is no cheap way to detect that a constructor made them. What it
  buys: the deploy's cost once per chunk — 251,798 gas for the 895-byte fixture lens; about 480k
  for the 2,174-byte Blue health lens (200 gas per byte of code deposit, 32,000 for `CREATE2`,
  hashing and the constructor). On a 50M frame that is about one percent, less on a larger one.
  And it would not lift the cap at all. Every byte of the batching gain comes from moving the
  elements out of initcode, which the envelope override does alone.
- **Designed first against `main`, then against PR #63** (`opening-wave`). The first draft carried
  its own byte model — a `c(bytes)` term inside the item prediction, a budget normalised at 16 per
  byte, a `page_size_suggested` computed with it — and review found it over-packed when the
  observed chunk was zero-heavy and the candidate was not, and omitted memory. PR #63 already
  prices intrinsic gas exactly and pools an observed cap, which dissolved most of that; what
  survives is the delivery-specific intrinsic, the copy term and the floor, each a correction to a
  predicate that already exists rather than a model beside it.
- **Reading elements straight from calldata** by override, so the prologue copies only the factory
  data and each element as staged, was considered when the quadratic copy surfaced and declined: it
  needs an input-source abstraction through `stage`, `materialize` and the decompressor's cursors, a
  second code path in the one place the paginated-lenses TIB worked hardest to keep singular, and
  the term it removes is dominated eighteen-fold by the floor at 1 MiB. Pricing it exactly costs
  one line.
- **An opening cap for override delivery** (a fixed byte figure when nothing is stated) was
  drafted at 256 KiB and declined: it would give override delivery a default no other delivery has,
  and the case it guards — no `gasLimit`, no `batchSize`, a large input — is one halving cascade,
  which is what the caller who states neither has accepted in initcode delivery all along. Letting
  a stated `gasLimit` admit bytes without `batch.gas` removed the more common half of the case.
- **Halving on `OOG_SENTINEL`** by override was the first draft and was caught in review: it would
  convert a terminal deploy failure into per-element `unresolved` entries.
- **A positive memo and a paired-observation negative memo** were the first draft and were caught
  in review (see Notes). The clean-room exploration independently proposed the narrower rule.
- **`out of gas` as a pre-execution phrase** was in the first draft and dropped: it also describes
  a frame that died mid-way, which is a prologue death in this design.
- **`copy` to the end of the arguments** was the revision's first formula and was caught in
  review: the prologue expands memory past the frame to the slab's head before the budget is
  sampled, so the expansion term runs to there. The constant offset changes the figure by under 1%
  at the initcode cap and by nothing that scales, but "exact" has to mean exact.
- **"A lone element always fits" applied to the floor** was caught in review: the paginated-lenses
  TIB's singleton rule is about the gas *prediction*, which may be wrong and which the envelope
  corrects; the floor is a protocol bound the node enforces before anything runs, like the byte cap,
  and an element above it is oversize.
- **The `+2` in `intrinsic`** is the paginated-lenses TIB's: the sampling `gas()`'s own cost, so
  that `intrinsic + fixed + budget` reconstructs the grant exactly. Kept as is.
- **A per-chain memory schedule** (Ethereum quadratic by default, Monad `w/2`) was considered when
  Monad's pricing came back linear and declined for now: the ceiling costs Monad at most 1.5% of a
  large frame, and a table is a second load-bearing figure to keep current. Revisit if a chain with
  memory priced *above* Ethereum's appears, which would flip the ceiling's direction. Filed as
  APPS-1398 (per-chain pricing and `eth_call` notes on viem's chain definitions), which would also
  settle the initcode-limit decision under Open risks.
- **Leaving the copy inside observed `fixed`** — the paginated-lenses TIB's position — was
  reconsidered against the Monad result and declined (see Notes): it is safe only while the
  candidate is no larger than the pages observed, which the initcode cap guaranteed and override
  delivery does not.
- **Incidental findings, Monad mainnet, 2026-09-09.** Code overrides are honoured by `eth_call`;
  intrinsic gas and the EIP-7623 floor follow Ethereum's schedule exactly; memory expansion is
  linear (`w/2`); a 60 KB initcode creation succeeds; an `eth_call` without `gas` is granted 8.1M
  and one with `gas` up to at least 150M. The 8.1M frame is handled in the paginated-lenses TIB:
  `src/chains` records per chain whether a bare `eth_call` runs in the cap or in a pool, and
  `gasLimit` rides as `gas` only on a `fixedDefault` chain. The same table is where a chain's
  initcode limit and memory schedule belong when they are needed (APPS-1398).
- **A dedicated support probe** (a small call before the opening wave) was declined: the opening
  wave is the probe, and a probe would add a serial round trip to every first request.
- **Two constants** (an initcode build and a runtime build of the envelope) were declined: a second
  drift guard and a second pasted blob for a difference of three opcodes; the no-op copies make
  one bytecode serve.
- **Persisting the memo** across transport instances or processes was declined: the cost it saves
  is one wave per instance, and a memo that outlives the provider it describes is worse than none.
- **Remembering the discovered request size cap** was deferred: the caller owns `batchSize`, and
  the wide event's `splits_size` says when it is too high.
- **Exporting the chain definitions** (`@morpho-org/viem-dlc/chains`, in viem's manner) was in
  the first cut and withdrawn: the record will grow under APPS-1398 — the initcode limit, the
  memory schedule, cold-access pricing, the floor — and its shape should settle before consumers
  depend on it. The module and its lookup are unchanged; only the export is deferred.
- **`keccak256("viem-dlc-envelope")[12:]`** was the override address in the first draft, derived
  like the policy sentinel's. Review noted it changes the factory's `msg.sender` from creation
  delivery's, which a factory that salts or checks its caller would notice. The creation address is
  a chain-independent constant — the zero address's nonce is 0 everywhere — so using it dissolves
  the difference instead of documenting it.
- **`copy` to the slab's telemetry words** was the revision's second formula and was caught in
  review: the budget is sampled right after the sentinel word, so the expansion the prologue pays
  ends one word past the slab's start and the rest is the first attempt's.
- **`tryStateOverride: boolean`** was the option's first name; `batch.envelope` was suggested by
  the clean-room exploration and adopted.
- **Incidental finding.** The shipped constant, delivered by override, fails deterministically
  with `CounterfactualDeployFailed`: it reads zero-length arguments from code, sees `target = 0`,
  and the factory call with empty data leaves no code there. The option depends on the Yul change.
- **Incidental finding.** The first gas comparison suggested override delivery cost ~270 gas more
  per element; it was the measurement, not the delivery — the creation baseline ran through the
  forge `Runner` hop and the override did not. In one frame, `Σg` is identical.
- **Incidental finding.** geth's floor check is upfront (`msg.GasLimit < floorDataGas` →
  `insufficient gas for floor data gas cost`), and only ordinary intrinsic gas is deducted from the
  runtime frame; the floor gates starting, not `gasleft()`.
