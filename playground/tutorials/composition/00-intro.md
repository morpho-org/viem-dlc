Every script in the other two tutorials passes the same shape into `cache(...)`: a transport, then an
array of five configs. That array isn't a bag of options. It's the stack, in order, and each entry
configures exactly one layer:

```ts
cache(http(url), [
  { binSize, store, invalidationStrategy },        // 1. cache
  { maxBlockRange },                               // 2. logsDivider
  { retryCount, retryDelay, blockTimestamp },      // 3. logsEnricher
  { maxBytes },                                    // 4. logsSieve
  { maxRequestsPerSecond, maxConcurrentRequests }, // 5. rateLimiter
])
```

A request falls down it and a response comes back up. The cache answers what it can and asks the
divider for the rest; the divider splits what's left and hands chunks to the enricher; the enricher
retries failures and attaches timestamps; the sieve drops logs too large to carry; the rate limiter
decides when each request is actually allowed to leave.

You can build the same stack by hand — `rateLimiter`, `logsSieve`, `logsEnricher` and `logsDivider`
are all exported and compose like any viem transport. `cache(...)` exists because that order is the
only one that works, and getting it wrong is silent: put the limiter above the divider and you rate-
limit one call instead of the hundred requests it becomes.

The step below changes one layer at a time so you can see what each costs.

Two of them are worth knowing before you run it. `maxRequestsPerSecond` moves elapsed time and
nothing else — the limiter decides *when* a request leaves, never whether one is needed, so it can't
show up in a request count. And `blockTimestamp` is a backfill, not a feature: it exists because most
chains omit timestamps from log results, and the enricher then fetches one block per distinct block
that carried a log. Base is an OP-stack chain and returns them already, which is why every config in
these tutorials leaves it off — and why turning it on here proves nothing. The step reports how many
logs arrived with a timestamp so you can see which case you're in.
