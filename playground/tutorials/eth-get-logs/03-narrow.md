Caching one query works. The trouble starts when you have users.

The cache key for `eth_getLogs` is `hash(address, topics)`. Put a borrower in `topics` and that
borrower gets a blob of their own — warm for them, and useless to everybody else. The first tab does
exactly that for two borrowers. Both pay the full range. The second one pays it again, for bytes the
first one already fetched, because the two queries disagree about a topic.

Now drop the borrower from the filter. One key covers every borrower, the first request fills the
range for all of them, and narrowing moves from the request into your process. The second tab does
that, over the same two borrowers and the same 100,000 blocks:

| | requests | borrower A | borrower B |
| --- | --- | --- | --- |
| one blob per borrower | 157 | 12,903 ms | 13,039 ms |
| one blob for everyone | 55 | **403 ms** | **407 ms** |

The 55 requests are the single fill. Every borrower after the first is free.

This is the trade that makes caching worth it at scale. You fetch and store more than any one
caller needs, once, and every caller after that is reading bytes you already hold. The cost moves
from *requests per user* to *parsing per user* — and parsing is the part you control.
