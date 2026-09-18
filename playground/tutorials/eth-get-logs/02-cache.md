A finalized block's logs are finished. They won't change, they won't be reordered, and nothing you do
will produce a different answer tomorrow. Refetching them spends requests on a value you already had.

So put a store in front of the divider. That's the `cache` transport: the same splitting, retrying
and enrichment as the last section, with the store in front and rate limiting behind.

Run the step and compare the two passes. The cold pass makes one request per chunk. The warm pass
makes exactly one — `eth_blockNumber`, to find out where the tip is now — and answers the rest from
the store.

Three things make that work, and all three are decisions about *alignment* rather than storage.

**Bins.** `binSize` sets the granularity of a cache entry, and the divider's `alignTo` is set from it
automatically. Every chunk therefore covers a whole bin, so a later request for an overlapping window
hits the same entries instead of straddling them. Smaller bins invalidate more precisely and cost
more entries; larger bins do the opposite. Unaligned chunks would give you a store that fills up and
never hits.

**A range that ends below the tip.** This page asks for a range that stops short of the head and
lands on a bin boundary. That isn't tidiness — a range touching the tip has a bin that isn't finished
yet, which is invalidated on every pass, so the warm run would refetch it and the comparison would
mean nothing.

**Invalidation that knows where the tip is.** `createSimpleInvalidation()` always refetches entries
near the head, where reorgs live, and probabilistically refreshes older ones by age.
`createExponentialInvalidation()` weighs time and block-age separately. Neither has to be told what
finality means on your chain; both treat recent blocks as untrustworthy and old ones as settled,
which is the only property you actually need.

The store here is an `LruStore`, so it lives in this tab and a reload starts cold. Outside a browser
`NodeFsStore` puts it on disk, `CompressedStore` shrinks it, `HierarchicalStore` stacks a fast tier
in front of a durable one, and `UpstashStore` or `VercelBlobStore` share it across processes.

Know where this stops paying. A cache serves history and discovery until the query outgrows its
host — around 10 MB of compressed logs per query in serverless memory, around 100 MB on a developer
machine — and past that you want an indexer surfacing raw events instead. The README's
[Choosing a half](https://github.com/morpho-org/viem-dlc#choosing-a-half) section has the sizes and
the trade.
