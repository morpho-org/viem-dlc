Now for the part the `eth_call` tutorial kept putting off.

That tutorial says `gasLimit` and `batch.gas` are optional hints that you should not guess, because
the telemetry hands them to you. Here is that, literally. Run the lens stating nothing, read four
fields off the event, and print the configuration to paste.

| field | goes to |
| --- | --- |
| `gas_limit_observed` | the transport's `gasLimit` |
| `fixed_gas` | `batch.gas.fixed` |
| `item_gas_avg` | `batch.gas.item.avg` |
| `item_gas_stddev` | `batch.gas.item.stddev` |

These are estimates of what the lens costs and what the provider grants, but what you observe also
depends on the request that produced them. Costs move with which elements shared a frame, because
grouping related elements warms storage they share. So take the figures over a representative
window rather than from one run, on the corpus you will actually query, at the size you will query
it. A stale figure costs one round trip and never a result, so it is safe to paste a number you
measured last month.

The step also computes the `envelope` decision from the same event.
`(gas_limit_observed − fixed_gas) / item_gas_avg` is how many elements a frame's gas would pay for.
`nominal_batches` is how many chunks the request was planned as, so
`elements_requested / nominal_batches` is the average number of elements a chunk actually carried.
When the first number is far larger than the second, bytes are binding and `envelope: 'override'`
will help.

The comparison only means something once a request is big enough to fill a chunk. Ask for 60
elements and they all fit in one, so the second number is 60, which tells you about your request and
nothing about what bytes allowed. Raise the vault count until `nominal_batches` climbs above 1
before you read it.

One caveat on `gas_limit_observed`: it is the cap the provider granted at that moment, and not a
contract. A `gasLimit` above it means the cap has since been lowered, which is something to alert on
rather than to paste.
