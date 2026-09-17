The blob is broad and warm, so every request is now a question about how to read it. There are three
answers, and they solve different problems.

**Fetch everything, then filter.** `getLogs2` returns the range and you call `.filter()`. Every log
in the range is parsed, decoded, and held in an array until you're done with it. Memory is O(range),
and for 37 million blocks of Blue history that array is the problem, not the network.

**`reduce`.** The fold runs inside the transport as each bin decodes, so nothing but your accumulator
outlives the pass. Memory becomes O(result). The logs are still all parsed — `parsed` in the table
below is unchanged — and the callback costs a little time. You're buying a memory bound, not speed.
`test/bench` asserts that bound over the full history; a browser tab is the wrong place to measure
it, so this page doesn't try.

**`search` + `reduce`.** `search` is a regex tested against a bin's **raw NDJSON, before it is
parsed**. A bin whose text doesn't contain your borrower is skipped entirely: no `JSON.parse`, no
decode, no callback.

That last one is worth being precise about, because its value depends entirely on your data. The step
runs the same code against the busiest borrower in the range and the rarest one:

| pass | logs parsed, of 7,327 |
| --- | --- |
| filter | 7,327 |
| reduce | 7,327 |
| search, common target — 33 of 50 bins | 5,619 |
| search, rare target — 1 of 50 bins | **121** |

Elapsed time tracks those counts, because on a warm pass parsing is nearly all of the work — in the
run these counts came from, the rare-target pass finished about 40% faster than the others. The step
prints its own times; nothing in this table touches the network.

A borrower in most bins can't be skipped, and `search` earns nothing but its own regex cost. A
borrower in one bin means 49 bins are never decoded. **`search` pays in proportion to how rare your
target is** — it saves you `1 − matched/total` of the parse, so it's worth reaching for on per-user
queries over a broad cache, and worth skipping on aggregate ones, where by construction nothing is
rare.

Two constraints before you reach for it. `search` matches raw JSON, so it finds your address as an
unprefixed lowercase hex substring, and a string that happens to appear elsewhere in the record
matches too — it's a pre-filter, not a predicate, which is why `reduce` still has to check properly.
And `reduce`'s accumulator is a log array that starts empty: fold into it, return it, and let the
transport hand it back.
