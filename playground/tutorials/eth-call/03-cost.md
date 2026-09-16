Correctness first, cost second. Two measurements, neither chosen to flatter.

## Encoding

Multicall's unit is the call, so every element is encoded on its own: one `encodeFunctionData`, then
an `(address, bool, bytes)` tuple to carry it — an offset, an address word, a bool word, a length
word, and the padded calldata. A lens read's unit is the array, so the elements encode once against
the array-shaped fragment `arrayifiedAbi` derives from the per-item function, and each element costs
only its own packed bytes.

The first tab measures both with no network involved. For 120 vaults:

| | multicall | lens |
| --- | --- | --- |
| encode | 5.7 ms | 0.3 ms |
| wire bytes per element | 257 | 65 |

Four times the bytes, paid on every request. That ratio is also why a chunk holds four times as many
elements before it hits a size limit.

## Requests

The second tab is the code you'd write after being caught by the first section: raise `batchSize` for
the request count, and when a batch comes back empty, split it in half and retry, down to single
elements. It's a reasonable design, and it does recover most of the data.

It also can't tell why a batch failed. A gas-starved batch, a reverting element, and a throttled
request are all `status: "failure"`, so the only safe reading is "try again smaller". Every split
costs another request the node already spent time on:

| | requests | result |
| --- | --- | --- |
| multicall + bisect | 23 | 112 of 120, after 11 splits |
| `readLens` | 3 | 119 of 120 |

Read request count rather than wall clock. This page rate-limits itself to keep public endpoints
happy, so elapsed time mostly measures that limiter. Request count is what changes when you change
providers, and it's what you're billed for.
