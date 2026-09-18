Some entries in that array are policy you chose: `binSize`, `retryCount`, `maxBytes`. The rest are
facts about **one provider**, such as its block-range ceiling and its rate limits. Point the same
code at a second provider and the second group is wrong.

That is what `failover` is for, and it is why `failover` takes complete transports rather than URLs:

```ts
failover([
  cache(http(primary),  [shared, { maxBlockRange: 100_000 }, ...tail]),
  cache(http(fallback), [shared, { maxBlockRange: 2_000 },  ...tail]),
])
```

Each branch is a full stack, built once. Its rate limiter keeps its token bucket, its coalescer
keeps its in-flight requests, and its block-range ceiling belongs to it. viem's own `fallback`
dispatches between bare transports, so it has nowhere to keep any of this. The per-provider state
you need is exactly the state it cannot hold.

**The store is shared.** If branch A fetches half a range before it dies, that work is kept. Those
bins are already in the store, and branch B starts from what is there. Failover costs you the
remainder of the range, not the whole of it.

**`shouldThrow` decides which errors end the attempt.** Return `true` and the error is thrown at
once, with no further branch tried. Return `false` and the next branch gets a turn. The default
mirrors viem's classification of non-retryable errors: reverts and user rejections throw, and
everything else falls over to the next branch. A 401 or 402 belongs on the `true` side. A rejected
key will not fare better at the next provider, and trying it there delays the error you need to see.
If the second provider answers, it also hides the fact that you are paying for a key that no longer
works.

The step below puts a dead endpoint first and the endpoint from the box above second. The read still
returns. Check `succeeded_index` in the table (`1` means the primary was skipped), along with
`branches_attempted` and `branch_errors`, which record what each branch said before it gave up.
