Caching one query works. The trouble starts when you have users.

The cache key for `eth_getLogs` is `hash(address, topics)`. Put a borrower in `topics` and that
borrower gets a blob of their own — warm for them, useless to everybody else. The **one blob per borrower** tab does
exactly that for two borrowers. Both pay the full range; the second pays it again for bytes the first
already fetched, because the two queries disagree about a topic.

Now drop the borrower from the filter. One key covers every borrower, the first request fills the
range for all of them, and narrowing moves from the request into your process. The **one blob for everyone** tab does
that, over the same two borrowers and the same 100,000 blocks:

| | RPC requests, whole pass | borrower A's read | borrower B's read |
| --- | --- | --- | --- |
| one blob per borrower | 157 | 12,903 ms | 13,039 ms |
| one blob for everyone | 55 | **403 ms** | **407 ms** |

*Measured on `mainnet.base.org`, September 2026, rate-limited as this page is. Read the two rows as
different shapes, not as a 32× speedup: in the top row each borrower's read is a cold fetch, and in
the bottom row both are warm reads over a range one shared fill already fetched. That fill's 55
requests are in the requests column; its elapsed time is in neither borrower column — the step logs
it separately.*

So the 55 requests buy the range once, and every borrower after the first adds no request at all.
Per-user cost falls as `1/n` where the one-blob-per-user design stays flat.

This is the trade that makes caching worth it at scale. You fetch and store more than any one caller
needs, once, and every caller after that reads bytes you already hold. The cost moves from *requests
per user* to *parsing per user* — and parsing is the part you control, which is what the next section
is about.
