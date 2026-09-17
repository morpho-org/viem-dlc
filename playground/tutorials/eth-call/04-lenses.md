Everything so far treated the lens as a way to survive gas. It's also a place to put logic, and that
changes which problems are round-trip problems.

Take the full snapshot: a vault's total assets, the asset it holds, and that asset's decimals. The
third read depends on the second. Batching doesn't help, because the dependency is in your data, not
in your transport — you can't ask for decimals until you know the asset. In TypeScript that's three
sequential rounds however wide each one is, and latency multiplies by three.

In the lens it's three lines:

```solidity
s.totalAssets = IVaultV2(x.vault).totalAssets();
s.asset       = IVaultV2(x.vault).asset();
s.decimals    = IERC20Metadata(s.asset).decimals();
```

Both tabs return the same 120 snapshots:

| | rounds | requests | elapsed |
| --- | --- | --- | --- |
| client-side waterfall | 3 | 3 | 628 ms |
| one lens call | 1 | 1 | 273 ms |

The requests were never the expensive part. The *rounds* were, and only the lens removes them. The
gap is one round trip to Base from a browser; it scales with your distance from the node, and a
deeper dependency chain multiplies it again.

Two things make this practical rather than clever.

**The lens is never deployed.** It's created counterfactually inside the `eth_call`, from initcode
the transport assembles, so there's no deployment, no address to manage, and no governance step
between writing a query and running it. Edit the Solidity above and run it — it compiles in your
browser and the next request uses it.

**Shared work goes in the constructor.** Item frames share no memory, but the counterfactual deploy
runs the constructor in the same `eth_call`, so immutables and anything the constructor wrote to
storage are readable from every per-item call in that chunk. Warm storage carries over too: the first
element to touch a market warms it for the rest of the chunk. Keep the constructor bounded — one that
exhausts the node's cap fails the request instead of being split.

What the lens must honor in exchange: a revert means the element is permanently invalid, never that
more gas would have helped, and a served value can't depend on which other elements shared its chunk.
Both hold naturally for reads. Neither is checked for you.
