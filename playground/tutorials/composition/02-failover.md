Every limit in that array is a fact about **one provider**. Point the same code at a second provider
and every number in it is wrong.

That's what `failover` is for, and why it takes complete transports rather than URLs:

```ts
failover([
  cache(http(primary),  [shared, { maxBlockRange: 100_000 }, ...tail]),
  cache(http(fallback), [shared, { maxBlockRange: 2_000 },  ...tail]),
])
```

Each branch is a full stack, built once. Its rate limiter keeps its token bucket, its coalescer keeps
its in-flight requests, and its block-range ceiling belongs to it. viem's own `fallback` dispatches
between bare transports, so it has nowhere to put any of that — the per-provider state you need is
exactly the state it can't hold.

**The store is shared.** Branch A fetching half a range before it dies isn't wasted: those bins are
already in the store, and branch B starts from what's there. Failover costs you the remainder, not
the range.

**`shouldThrow` decides what's worth failing over.** The default mirrors viem's classification of
non-retryable errors. A 401 or 402 is worth adding: your key being rejected won't go better at the
next provider, and trying it there only delays the error you need to see — and, if the second
provider answers, hides the fact that you're paying for a key that no longer works.

The step below puts a dead endpoint first and the endpoint from the box above second. The read still
returns. Check `succeeded_index` in the table — `1` means the primary was skipped — along with
`branches_attempted` and `branch_errors`, which record what each branch said before it gave up.
