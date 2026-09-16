The problem isn't the gas ceiling. It's that 300 reads share one frame, so one of them can spend
what the others needed.

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

Read the wide event underneath:

- `pages_continued` — the first frame stopped early, and the elements it didn't reach were re-packed.
- `pages_escalated` — the grief element was retried on its own.
- `elements_unresolved` — it didn't survive even alone, so it's in `skipped`. Another endpoint with a
  higher cap might serve it.

Note what the script doesn't contain: no batch size, no gas figure, no retry policy, no bisect. The
chunking adapts to what the node reports, so there is no number to tune and no number to get wrong.

## The hints are optional, and you don't have to guess them

There are two figures you *can* state, and both come back to you on the same event. `batch.gas`
describes what the lens costs (`fixed_gas`, `item_gas_avg`, `item_gas_stddev`), and the transport's
`gasLimit` describes what the provider grants (`gas_limit_observed`). Together they size the opening
wave. Every chunk after it is sized from what the pages actually reported, so a wrong figure costs
one round trip and never a result.

The second tab fills all of them in from the run you just did. It makes no difference here, and the
event says why. Gas would pay for about `(600,000,000 − 288,653) / 103,090 ≈ 5,800` elements in one
frame, while this request sends 120 in a single batch. Gas isn't the binding constraint; the
49,152-byte initcode cap is, at roughly 690 elements.

That same ratio is the signal for `batch.envelope: 'override'`, which places the envelope by state
override so the byte cap stops applying. Reach for it when gas would pay for far more elements than
bytes allow — which is to say, once your corpus outgrows one chunk.
