You want every `Borrow` event Morpho Blue has ever emitted on Base. That means knowing where its
history starts, and the answer shouldn't be a number pasted from a block explorer — it's wrong for
every other contract and every other chain, and nothing tells you when it goes stale. Run the step
below: `getDeploymentBlockNumber` binary-searches `eth_getCode` and finds it in about 26 requests,
which is `log2` of the chain height and therefore what it costs on any chain, at any height.

Then subtract. That's roughly 37 million blocks of history behind one `eth_getLogs` call.

No endpoint will answer that call. Every provider caps the request somehow — by block range, by
result count, by response size, by wall clock — and the cap is different everywhere:

| Endpoint | widest `Borrow`-only range that answers |
| --- | --- |
| `mainnet.base.org` (the default above) | 2,000 blocks, then `-32614: eth_getLogs is limited to a 2,000 range` |
| a commercial endpoint | 100,000 blocks in 3.3 s, then `-32012: getLogs request exceeded max allowed range` |

*Measured September 2026. Neither error code is standardized — providers invent their own, which is
the first reason your code can't detect a cap by reading it.*

Two things follow, and the rest of this tutorial is about them. Your code can't know the cap, so it
has to discover it. And once you're making hundreds of requests to read history that will never
change again, you should be making them once.
