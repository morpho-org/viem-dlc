The problem was never the gas ceiling. It's that every read in a batch shares one frame, so one of
them can spend what the others needed.

`deployless` gives each element its own. It puts an envelope in front of your call; the envelope
reads the element array and invokes the lens's per-item function **once per element, in a fresh
frame**, with all the gas that's left. Exactly one of three things then happens to each element:

- It **returns**, and the value goes straight into the response.
- It **reverts**, and its index goes to `skipped`. Keep per-item reverts to permanent conditions,
  because the reason isn't surfaced.
- It **runs out of gas**. EIP-150 leaves the envelope 1/64 of what it forwarded, which is enough to
  record that and stop cleanly. The transport re-sends that element on its own, where it gets the
  largest frame the node will grant. Only if it dies there too does it reach your `skipped`.

Run it with the same `grief` that returned nothing above. You get 119 results and one entry in
`skipped`, in three requests, at every `batchSize` — there isn't one to set. A partial result is a
successful response, not an exception.

Run the **no hints** tab, then read these fields off the wide event it emits:

- `pages_continued` — the first frame stopped early, and the elements it didn't reach were re-packed.
- `pages_escalated` — the grief element was retried on its own.
- `elements_unresolved` — it didn't survive even alone, so it's in `skipped`. Another endpoint with a
  higher cap might serve it.

Note what the script doesn't contain: no batch size, no gas figure, no retry policy, no bisect. The
chunking adapts to what the node reports, so there is no number to tune and no number to get wrong.
That's the whole trade — you give up a knob you were going to set wrong anyway.

## The hints are optional, and you don't have to guess them

There are two figures you *can* state, and both come back to you on the same event. `batch.gas`
describes what the lens costs (`fixed_gas`, `item_gas_avg`, `item_gas_stddev`), and the transport's
`gasLimit` describes what the provider grants (`gas_limit_observed`). Together they size the opening
wave. Every chunk after it is sized from what the pages actually reported, so a wrong figure costs
one round trip and never a result.

The **with gas hints** tab fills all of them in from the run above. It makes no difference here, and the
event says why. Gas would pay for about `(600,000,000 − 288,653) / 103,090 ≈ 5,800` elements in one
frame, while this request sends 120 in a single batch. Gas isn't the binding constraint;
[EIP-3860](https://eips.ethereum.org/EIPS/eip-3860)'s 49,152-byte initcode cap is, at roughly 690
elements.

That ratio is the signal for the **override delivery** and **compressed calldata** tabs, the two
ways to stop bytes binding.
`envelope: 'override'` places the envelope by state override, so the initcode cap doesn't apply at
all; `compress: true` FastLZ-compresses the elements so more fit under it. Reach for either once your
corpus outgrows one chunk. At 120 vaults it already fits, so here they change the delivery and not
the request count — `chunks_override`, `chunks_initcode` and `batch_bytes.max` in each table say
which one carried the request and how close to the cap it came.
