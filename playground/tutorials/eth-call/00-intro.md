Suppose you want one number from every Morpho vault on Base. There are around 600 vaults, and
`totalAssets()` is a `view` function, so this is a read problem: several hundred `eth_call`s that you
would rather not send one at a time.

The standard answer is [Multicall3](https://github.com/mds1/multicall). You pack the calls into a
single `aggregate3`, the node runs them in one frame, and you get one response back. viem has this
built in as `multicall`, and it works.

This tutorial is about where it stops working, why you will not notice until it does, and what to
use instead. Every step runs against the endpoint in the box above, over real vaults.

One fact carries the rest of this page: `totalAssets()` does not cost the same on every vault.

| Vault | gas |
| --- | --- |
| Steakhouse Prime USDC | 1,894,687 |
| Steakhouse HY USDC | 493,626 |
| OUSD Vault V2 | 327,444 |
| Gauntlet USDC Prime | 193,280 |
| Re7 USDC | 72,837 |

*Measured by bisecting the `gas` parameter on `mainnet.base.org`, September 2026.*

That is a 26× spread across live contracts, and you set none of it. A Vault V2 allocates through
adapters, so reading one costs whatever it costs to walk its curator's allocations. The number moves
when the curator acts, not when you deploy. The cost of your read is a variable that someone else
holds.

Every step below reads through the same lens contract, shown beside the script. `grief` is the one
artificial thing on this page: a loop that burns gas on a single vault, so that you can make an
element expensive on demand instead of waiting for a curator to do it. The table above is the real
version of the same event.
