You want one number from every Morpho vault on Base. There are around 600 of them, and
`totalAssets()` is a `view` function, so this is a read problem: several hundred `eth_call`s that you
would rather not send one at a time.

The standard answer is [Multicall3](https://github.com/mds1/multicall). You pack the calls into a
single `aggregate3`, the node runs them in one frame, and you get one response. viem has it built in
as `multicall`, and it works.

This tutorial is about where it stops working, why you won't notice until it does, and what to use
instead. Every step runs against the endpoint in the box above, over real vaults.

One number to keep in view: `totalAssets()` doesn't cost the same everywhere.

| Vault | gas |
| --- | --- |
| Steakhouse Prime USDC | 1,894,687 |
| Steakhouse HY USDC | 493,626 |
| OUSD Vault V2 | 327,444 |
| Gauntlet USDC Prime | 193,280 |
| Re7 USDC | 72,837 |

A 26× spread, and none of it is yours to set. A Vault V2 allocates through adapters, so the cost of
reading one is the cost of walking whatever its curator allocated to. That number moves when they
act, not when you deploy.

Every step below reads through the same lens contract, shown beside the script. `grief` is the one
thing on this page that isn't real: a loop that burns gas on a single vault, so you can watch what
happens when one element gets expensive without waiting for a curator to make it happen. The table
above is the honest version of the same event.
