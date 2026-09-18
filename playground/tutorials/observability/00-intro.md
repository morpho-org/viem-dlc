The other tutorials keep pointing at a table of facts about each run (`gaps_fetched`,
`pages_escalated`, `item_gas_avg`) and asking you to read numbers out of it. This tutorial explains
where those numbers come from and how to get them in your own process.

The rule is simple: **the library emits nothing until you ask.** There is no global logger, no
ambient configuration, and no output on any path you have not wrapped. Wrap a call in `withLogging`
and every outermost transport call inside that scope produces exactly one event when it finishes.

That is one event for the whole call, rather than one per layer. A request that crosses the cache,
the divider, the enricher and the rate limiter contributes fields from all four to the same event,
each namespaced by the transport that set it:

```
viem-dlc-cache.gaps_fetched          18
viem-dlc-logs-divider.nominal_ranges 25
viem-dlc-rate-limiter.queue_wait_ms.avg 112
status                               ok
duration_ms                          8420
```

The shape is deliberate. One row per outermost call, meaning the read you asked for, however many
RPC requests it became, is a shape you can query later. There are no joins across spans, no sampling
decisions, and no reconstructing a call from twelve log lines. The question you will want to ask six
months from now, *which reads escalated, and what did they have in common*, is a `WHERE` clause
against one table. With any other shape it is an afternoon.
