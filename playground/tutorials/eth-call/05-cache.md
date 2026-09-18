Caching is optional. For most uses `deployless` on its own is the right tool, and the previous
sections are all you need. Add a cache when the same elements are read again and again and an
answer a few minutes old is acceptable.

The transport that caches `eth_getLogs` caches `eth_call` too. For a lens read, the unit it caches
is the **element** rather than the request.

Give the read a `cache` block and each element's result is stored under `blobKey` for `ttl`
milliseconds. Ask for the same vault tomorrow and it never reaches the envelope. Ask for a set that
overlaps yesterday's and only the new members cost anything. `delta` jitters the expiry, so a blob
filled in one burst does not expire in one burst and hand you a thundering herd.

The step fills the store with half the corpus, re-reads that half, then asks for the whole corpus,
so the third pass is half cached and half new. Read `elements_requested` against `elements_fetched`
on each pass. The gap is what the store answered, and on the third pass only the new half leaves the
process.

Two points about this. First, elements are cached, not calls, so the set you ask for does not have
to match the set you cached. That is what makes it work for a per-user query over a shared blob,
which is the same argument the `eth_getLogs` tutorial makes about `topics`. Second, `ttl` is your
statement about how stale an answer may be; the chain does not supply it. Vault totals move every
block, so the five minutes below buys you one request per vault per five minutes in exchange for
showing numbers up to five minutes old. Choose it according to what your screen is for.
