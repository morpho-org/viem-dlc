The last two sections were about correctness: a lens read returns what the shared frame could not.
This section is about cost, and the lens read wins that comparison as well. Two measurements
follow: the bytes each encoding puts on the wire, and the requests each design needs to recover
from the same grief element.

## Encoding

Multicall's unit is the call, so every element is encoded on its own: one `encodeFunctionData`, then
an `(address, bool, bytes)` tuple to carry it, which means an offset, an address word, a bool word,
a length word, and the padded calldata. A lens read's unit is the array. The elements are encoded
once against the array-shaped fragment that `arrayifiedAbi` derives from the per-item function, and
each element costs only its own packed bytes.

The **encoding only** tab measures both with no network involved. For 120 vaults:

| | multicall | lens |
| --- | --- | --- |
| encode | 5.7 ms | 0.3 ms |
| wire bytes per element | 257 | 65 |

*Measured in this browser tab. The milliseconds are your machine's; the byte counts, and the ratio
between them, are properties of the encoding and hold anywhere.*

Four times the bytes, paid on every request. The same ratio is why a chunk holds four times as many
elements before it hits a size limit. The encoding argument and the chunking argument are the same
argument.

## Requests

The **multicall + bisect** tab is the code you would write after the first section caught you: raise
`batchSize` to bring the request count down, and when a batch comes back empty, split it in half and
retry, down to single elements. It is a reasonable design, and it recovers most of the data.

What it cannot do is tell why a batch failed. A gas-starved batch, a reverting element and a
throttled request all report `status: "failure"`, so the only safe response is to try again with a
smaller batch. Every split costs another request that the node has already spent time on, and the
splits are wasted on the 119 elements that were never the problem:

| | requests | result |
| --- | --- | --- |
| multicall + bisect | 23 | 112 of 120, after 11 splits |
| `readLens` | 3 | 119 of 120 |

Read the request count rather than the wall clock. This page rate-limits itself to be kind to public
endpoints, so elapsed time here mostly measures the limiter. Request count is what changes when you
change providers, and it is what you are billed for.
