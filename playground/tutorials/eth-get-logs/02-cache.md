A finalized block's logs are finished. They will not change or be reordered, and nothing you do will
get a different answer tomorrow. Fetching them again spends requests on a value you already had.

So put a store in front of the divider. That is the `cache` transport: the same splitting, retrying
and enrichment as in the last section, with a store in front and rate limiting behind.

Run the step and compare the two passes. The cold pass makes one request per chunk. The warm pass
makes exactly one request, an `eth_blockNumber` to learn where the tip is now, and answers the rest
from the store.

Three settings make that work, and each is a decision about *alignment* rather than about storage.

**Bins.** `binSize` sets the granularity of a cache entry, and the divider's `alignTo` is set from it
automatically. Every chunk therefore covers whole bins, so a later request for an overlapping window
hits the same entries instead of straddling them. Smaller bins invalidate more precisely and cost
more entries; larger bins do the opposite. With unaligned chunks the store would fill up and never
hit.

**A range that ends below the tip.** This page asks for a range that stops short of the head and
lands on a bin boundary. A range that touches the tip includes a bin that is not finished yet. That
bin is invalidated on every pass, so the warm run would fetch it again and the comparison would show
nothing.

**Invalidation that knows where the tip is.** `createSimpleInvalidation()` always refetches entries
near the head, where reorgs happen, and refreshes older ones probabilistically by age.
`createExponentialInvalidation()` weighs time and block age separately. Neither needs to be told
what finality means on your chain. Both treat recent blocks as suspect and old ones as settled, and
that is the only property the cache needs.

The store here is an `LruStore`, so it lives in this tab and a reload starts cold. Outside a browser
`NodeFsStore` puts it on disk, `CompressedStore` shrinks it, `HierarchicalStore` stacks a fast tier
in front of a durable one, and `UpstashStore` or `VercelBlobStore` share it across processes.

The cache has a limit, and you should know where it is. It serves history and discovery until a
query outgrows its host: around 10 MB of compressed logs per query in serverless memory, around
100 MB on a developer machine. Past that you want an indexer that surfaces raw events. The README's
[Choosing a half](https://github.com/morpho-org/viem-dlc#choosing-a-half) section has the sizes and
the trade-off.
