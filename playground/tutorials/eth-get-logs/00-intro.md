You want every `Borrow` event Morpho Blue has ever emitted on Base. Blue was deployed at block
13,977,148 and the chain is past 51,400,000, so that's about 37 million blocks of history behind one
`eth_getLogs` call.

No endpoint will answer that call. Every provider caps the request somehow — by block range, by
result count, by response size, by wall clock — and the cap is different everywhere:

| Endpoint | widest `Borrow`-only range that answers |
| --- | --- |
| `mainnet.base.org` (the default above) | 2,000 blocks, then `-32614: eth_getLogs is limited to a 2,000 range` |
| a commercial endpoint | 100,000 blocks in 3.3 s, then `-32012: getLogs request exceeded max allowed range` |

Two things follow, and the rest of this tutorial is about them. Your code can't know the cap, so it
has to discover it. And once you're making hundreds of requests to read history that will never
change again, you should be making them once.
