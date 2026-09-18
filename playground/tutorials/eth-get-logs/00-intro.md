Suppose you want every `Borrow` event that Morpho Blue has emitted on Base. The first thing you need
is the block where its history begins. You could copy that number from a block explorer, but the
copied number is right for one contract on one chain, and nothing will tell you when it stops being
right. The step below asks the chain instead. `getDeploymentBlockNumber` binary-searches
`eth_getCode` and finds the deployment block in about 26 requests. That is `log2` of the chain
height, so the cost is about the same on any chain at any height.

Subtract the deployment block from the current head and you have the range you want. On Base today
it is roughly 37 million blocks, and you would like to read it with one `eth_getLogs` call.

No endpoint will answer that call. Every provider caps the request in some way: by block range, by
result count, by response size, or by wall clock. The cap differs from one provider to the next.

| Endpoint | widest `Borrow`-only range that answers |
| --- | --- |
| `mainnet.base.org` (the default above) | 2,000 blocks, then `-32614: eth_getLogs is limited to a 2,000 range` |
| a commercial endpoint | 100,000 blocks in 3.3 s, then `-32012: getLogs request exceeded max allowed range` |

*Measured September 2026. The second endpoint is a paid plan. It is left unnamed because the lesson
is that the two numbers differ, and which vendor sets which does not matter. Neither error code is
standardized. Providers invent their own, so your code cannot learn the cap by reading one.*

The rest of this tutorial follows from two observations. Since your code cannot know the cap, it has
to discover it. And once you are making hundreds of requests to read history that will never change,
you should make them only once.
