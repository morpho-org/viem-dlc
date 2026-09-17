The other tutorials keep pointing at a table of facts about each run — `gaps_fetched`,
`pages_escalated`, `item_gas_avg` — and telling you to read numbers back out of it. This is where
those come from, and how you get them in your own process.

One rule: **the library emits nothing until you ask.** There is no global logger, no ambient config,
and no output on any path you haven't wrapped. Wrap a call in `withLogging` and every outermost
transport call inside that scope produces exactly one event when it concludes.

One event, not one per layer. A request that crosses the cache, the divider, the enricher and the
rate limiter contributes fields from all four to the same event, each namespaced by the transport
that set them:

```
viem-dlc-cache.gaps_fetched          18
viem-dlc-logs-divider.nominal_ranges 25
viem-dlc-rate-limiter.queue_wait_ms.avg 112
status                               ok
duration_ms                          8420
```

That shape is deliberate. One row per outermost call — the read you asked for, however many RPC
requests it became — wide rather than deep, is what you can actually query later: no joins across spans, no sampling decisions, no reconstructing a call from twelve log lines.
The question you'll want to ask six months from now — *which reads escalated, and what did they have
in common?* — is a `WHERE` clause against one table, or it's an afternoon.
