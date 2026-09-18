The blob is broad and warm, so every request is now a question of how to read it. There are three
ways, and they solve different problems.

**Fetch everything, then filter.** `getLogs2` returns the range and you call `.filter()`. Every log
in the range is parsed, decoded and held in an array until you are done with it. Memory is O(range).
For 37 million blocks of Blue history that array is the problem, well before the network is.

**`reduce`.** The fold runs inside the transport as each bin is decoded, so only your accumulator
outlives the pass. Memory becomes O(result). The logs are still all parsed (`parsed` in the table
below does not change) and the callback costs a little time. You are buying a bound on memory, not
speed. `test/bench` asserts that bound over the full history; a browser tab is a poor place to
measure it, so this page does not try.

**`search` + `reduce`.** `search` is a regex tested against a bin's **raw NDJSON, before it is
parsed**. A bin whose text does not contain your borrower is skipped entirely: no `JSON.parse`, no
decoding, no callback.

How much the last one saves depends entirely on your data, so the step runs the same code against
the busiest borrower in the range and the rarest one:

| pass | logs parsed, of 7,327 |
| --- | --- |
| filter | 7,327 |
| reduce | 7,327 |
| search, common target — 33 of 50 bins | 5,619 |
| search, rare target — 1 of 50 bins | **121** |

Elapsed time tracks those counts, because on a warm pass parsing is nearly all of the work. In the
run these counts came from, the rare-target pass finished about 40% faster than the others. The step
prints its own times, and nothing in this table touches the network.

A borrower who appears in most bins cannot be skipped, so `search` earns nothing there and costs its
own regex. A borrower who appears in one bin means 49 bins are never decoded. **`search` pays in
proportion to how rare your target is.** It saves `1 − matched/total` of the parsing, which makes it
worth using on per-user queries over a broad cache and not worth using on aggregate queries, where
nothing is rare by construction.

Two cautions before you use it. `search` matches raw JSON, so it finds your address as an unprefixed
lowercase hex substring, and a string that happens to appear elsewhere in the record also matches.
It is a pre-filter, so `reduce` still has to check properly. And `reduce`'s accumulator is a log
array that starts empty: fold into it, return it, and let the transport hand it back.
