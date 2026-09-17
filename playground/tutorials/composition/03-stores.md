A `Store` is four methods:

```ts
interface Store {
  get(key: string): MaybePromise<Buffer[] | null>
  set(key: string, value: Buffer[]): MaybePromise<void>
  delete(key: string): MaybePromise<void>
  flush(): MaybePromise<void>
}
```

That's the whole contract, which is why stores nest: anything that wraps a store is a store. The step
below defines a complete one in a dozen lines — it delegates to another store and counts what passes
through — then wraps each tier so the fall-through is visible.

`HierarchicalStore` reads top to bottom and writes fan out. With `populateOnMiss`, an answer from a
lower tier is written back up, so the second identical read never reaches it.

At the default size the hot tier holds the whole range, so the warm pass hits it and the cold tier is
never asked: `hot hits` rises and `cold hits` stays at zero. **Set the hot tier to 1,000 bytes and
run it again.** Now the range doesn't fit up top, the warm pass misses hot, and `cold hits` answers
instead. That's what a working set outgrowing its fast tier looks like from the outside, and it's why
the fast tier is a cache rather than a shard: nothing is lost when it's too small, only speed.

The tiers that matter in production are the ones a browser can't run:

```ts
new HierarchicalStore([
  new TtlStore(new LruStore({ maxBytes: 100_000_000 }), { ttlMs: 60_000 }),
  new CompressedStore(new NodeFsStore({ directory: ".cache" })),
  createOptimizedUpstashStore({ maxRequestBytes: 1_000_000 }),
], { populateOnMiss: true })
```

`TtlStore` bounds how long a warm tier may keep masking a fresher remote one. `CompressedStore`
shrinks what lands on disk. `NodeFsStore` is the durable local tier, and
`createOptimizedUpstashStore` or `createOptimizedVercelStore` share one cache across processes — and
because those subpaths carry optional peer dependencies, the `stores` barrel deliberately doesn't
re-export them. `ThrottledStore` accepts writes without blocking when a tier is slower than the
traffic.

Always `await store.flush()` before exit. Debounced and remote tiers batch their writes, and flushing
is what makes them attempt those writes — it promises the attempt, not the success. A process that
exits without it loses whatever the last batch held, which on a long backfill is the expensive part.
