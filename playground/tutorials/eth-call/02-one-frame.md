The gas ceiling was never the problem. The problem is that every read in a batch shares one frame,
so one of them can spend what the others needed.

`deployless` gives each element a frame of its own. It puts an envelope in front of your call. The
envelope reads the element array and invokes the lens's per-item function **once per element, in a
fresh frame**, with all the gas that is left. One of three things then happens to each element:

- It **returns**, and the value goes straight into the response.
- It **reverts**, and its index goes into `skipped`. Reserve per-item reverts for permanent
  conditions, because the reason is not surfaced.
- It **runs out of gas**. EIP-150 leaves the envelope 1/64 of what it forwarded, which is enough to
  record the fact and stop cleanly. The transport then sends that element again on its own, where it
  gets the largest frame the node will grant. Only if it dies there too does it reach your `skipped`.

Run it with the same `grief` that returned nothing above. You get 119 results and one entry in
`skipped`, in three requests, at every `batchSize`, because there is no `batchSize` to set. A partial
result is a successful response rather than an exception.

Run the **no hints** tab and read these fields off the wide event it emits:

- `pages_continued`: the first frame stopped early, and the elements it did not reach were packed
  again.
- `pages_escalated`: the grief element was retried on its own.
- `elements_unresolved`: it did not survive even alone, so it is in `skipped`. An endpoint with a
  higher cap might serve it.

Notice what the script does not contain: no batch size, no gas figure, no retry policy, no
bisection. The chunking adapts to what the node reports, so there is no number to tune and none to
get wrong.

## The hints are optional, and measured rather than guessed

There are two figures you *can* state, and both come back to you on the same event. `batch.gas`
describes what the lens costs (`fixed_gas`, `item_gas_avg`, `item_gas_stddev`), and the transport's
`gasLimit` describes what the provider grants (`gas_limit_observed`). Together they size the opening
wave. Every chunk after it is sized from what the pages actually reported, so a wrong figure costs
one round trip and never a result.

The **with gas hints** tab fills all of them in from the run above. It makes no difference here, and
the event says why. Gas would pay for about `(600,000,000 − 288,653) / 103,090 ≈ 5,800` elements in
one frame, while this request sends 120 in a single batch. Gas is not the binding constraint.
[EIP-3860](https://eips.ethereum.org/EIPS/eip-3860)'s 49,152-byte initcode cap is, at roughly 690
elements.

That ratio is the signal for the **override delivery** and **compressed calldata** tabs.
`envelope: 'override'` places the envelope by state override, so the initcode cap does not apply at
all and a chunk is bounded only by the frame's gas and the provider's request-size limit. It is the
stronger of the two, and the one to use when your provider honours state overrides.
`compress: true` FastLZ-compresses the elements so that more of them fit under the initcode cap. It
is a partial workaround for providers that do not honour overrides, and its gains are smaller:
compression buys a constant factor, where override removes the cap. Neither matters until your
corpus outgrows one chunk. At 120 vaults it already fits, so here they change the delivery and not
the request count.
`chunks_override`, `chunks_initcode` and `batch_bytes.max` in each table say which delivery carried
the request and how close to the cap it came.
