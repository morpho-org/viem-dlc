Now the part the `eth_call` tutorial kept deferring.

That tutorial says `gasLimit` and `batch.gas` are optional hints you shouldn't guess, and that the
telemetry hands them to you. This is that, literally: run the lens stating nothing, read four fields
off the event, and print the configuration to paste.

| field | goes to |
| --- | --- |
| `gas_limit_observed` | the transport's `gasLimit` |
| `fixed_gas` | `batch.gas.fixed` |
| `item_gas_avg` | `batch.gas.item.avg` |
| `item_gas_stddev` | `batch.gas.item.stddev` |

These describe the lens and the provider, not this request, so take them over a representative window
rather than one run — costs depend on which elements share a frame, and grouping related elements
warms storage they share. Measure the corpus you'll actually query, at the size you'll query it. A
figure that's stale costs one round trip and never a result, which is the whole reason it's safe to
paste a number you measured last month.

The step also computes the `envelope` decision from the same event.
`(gas_limit_observed − fixed_gas) / item_gas_avg` is how many elements a frame's gas would pay for;
`elements_requested / nominal_batches` is how many actually went in each chunk. When the first is far
larger than the second, bytes are binding and `envelope: 'override'` is worth reaching for.

The comparison only means something once a request is big enough to fill a chunk. Ask for 60 elements
and they all fit in one, so the second number is 60 — a fact about your request, not about what bytes
allowed. Raise the vault count until `nominal_batches` climbs above 1 before reading it.

One caveat on `gas_limit_observed`: it's the cap the provider granted at that moment, not a contract.
A `gasLimit` above it is a cap that has since been lowered, which is worth alerting on rather than
pasting.
