The transport that caches `eth_getLogs` caches `eth_call` too, and for a lens read the unit it
caches is the **element**, not the request.

Give the read a `cache` block and each element's result is stored under `blobKey` for `ttl`
milliseconds. Ask for the same vault tomorrow and it never reaches the envelope; ask for a set that
overlaps yesterday's and only the new members cost anything. `delta` jitters expiry so a blob filled
in one burst doesn't expire in one burst and hand you a thundering herd.

The step fills the store with half the corpus, re-reads that half, then asks for the whole corpus —
so the third pass is half cached and half new. Read `elements_requested` against `elements_fetched`
on each one: the gap is what the store answered, and on the third pass only the new half leaves.

Two things worth knowing. The elements are cached, not the call, so the set you ask for doesn't have
to match the set you cached — that's what makes this useful for a per-user query over a shared blob,
the same argument the `eth_getLogs` tutorial makes about `topics`. And `ttl` is your statement about
how stale an answer may be: vault totals move every block, so the five minutes below is a choice
about freshness, not a fact about the data.
