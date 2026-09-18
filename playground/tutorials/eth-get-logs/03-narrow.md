Caching one query works. The trouble starts when you have users.

The cache key for `eth_getLogs` is `hash(address, topics)`. Put a borrower in `topics` and that
borrower gets a blob of their own, warm for them and useless to everyone else. The **one blob per
borrower** tab does exactly that for two borrowers. Both pay for the full range. The second pays
again for bytes the first already fetched, because the two queries differ in one topic.

Now drop the borrower from the filter. One key covers every borrower, the first request fills the
range for all of them, and the narrowing moves out of the request and into your process. The **one
blob for everyone** tab does that, over the same two borrowers and the same 100,000 blocks:

| | RPC requests, whole pass | borrower A's read | borrower B's read |
| --- | --- | --- | --- |
| one blob per borrower | 157 | 12,903 ms | 13,039 ms |
| one blob for everyone | 55 | **403 ms** | **407 ms** |

*Measured on `mainnet.base.org`, September 2026, rate-limited as this page is. The two rows are
different shapes of work, so do not read them as a 32× speedup. In the top row each borrower's read
is a cold fetch. In the bottom row both are warm reads over a range that one shared fill already
fetched. That fill's 55 requests appear in the requests column, but its elapsed time appears in
neither borrower column; the step logs it separately.*

The 55 requests buy the range once, and every borrower after the first adds no requests at all.
Per-user cost falls as `1/n`, where the one-blob-per-borrower design stays flat.

This is the trade that makes caching pay at scale. You fetch and store more than any one caller
needs, once, and every later caller reads bytes you already hold. The cost moves from requests per
user to parsing per user. Parsing is the part you control, and the next section is about it.
