So far the lens has been a way to survive gas. It is also a place to put logic, and that changes
which problems are round-trip problems.

Take the full snapshot: a vault's total assets, the asset it holds, and that asset's decimals. The
third read depends on the second. Batching does not help, because the dependency is in your data
rather than in your transport; you cannot ask for decimals until you know the asset. In TypeScript
that is three sequential rounds, however wide each one is, and latency is multiplied by three.

In the lens it is three lines:

```solidity
s.totalAssets = IVaultV2(x.vault).totalAssets();
s.asset       = IVaultV2(x.vault).asset();
s.decimals    = IERC20Metadata(s.asset).decimals();
```

At the default 120 vaults, both tabs return the same snapshots:

| | rounds | requests | elapsed |
| --- | --- | --- | --- |
| client-side waterfall | 3 | 3 | 628 ms |
| one lens call | 1 | 1 | 273 ms |

The requests were never the expensive part. The rounds were, and only the lens removes them. The gap
is one round trip to Base from a browser. It grows with your distance from the node, and a deeper
dependency chain multiplies it again.

Two properties make this practical rather than merely clever.

**The lens is never deployed.** It is created counterfactually inside the `eth_call`, from initcode
the transport assembles, so there is no deployment, no address to manage, and no governance step
between writing a query and running it. Edit the Solidity above and run it. It compiles in your
browser and the next request uses it.

**Shared work goes in the constructor.** Item frames share no memory, but the counterfactual deploy
runs the constructor in the same `eth_call`, so immutables, and anything the constructor wrote to
storage, are readable from every per-item call in that chunk. Warm storage carries over too: the
first element to touch a market warms it for the rest of the chunk. Keep the constructor bounded.
One that exhausts the node's cap fails the request instead of being split.

In exchange, the lens must honor two rules. A revert means the element is permanently invalid, never
that more gas would have helped. And a served value must not depend on which other elements shared
its chunk. Both hold naturally for reads. Neither is checked for you.
