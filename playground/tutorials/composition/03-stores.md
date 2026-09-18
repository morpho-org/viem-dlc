A `Store` is four methods:

```ts
type MaybePromise<T> = T | Promise<T>

interface Store {
  get(key: string): MaybePromise<Buffer[] | null>
  set(key: string, value: Buffer[]): MaybePromise<void>
  delete(key: string): MaybePromise<void>
  flush(): MaybePromise<void>
}
```

That is the whole contract, and it is why stores nest: anything that wraps a store is itself a
store. The step below defines a complete one in a dozen lines. It delegates to another store and
counts what passes through. Wrapping one around each tier makes the fall-through visible.

`HierarchicalStore` reads from the top tier down, and writes fan out to every tier. With
`populateOnMiss`, an answer from a lower tier is written back up, so the second identical read never
reaches it.

At the default size the hot tier holds the whole range, so the warm pass hits it and the cold tier
is never asked: `hot hits` rises and `cold hits` stays at zero. **Set the hot tier to 1,000 bytes
and run it again.** Now the range does not fit up top, the warm pass misses the hot tier, and
`cold hits` answers instead. That is what a working set outgrowing its fast tier looks like from the
outside. It is also why the fast tier is a cache rather than a shard: when it is too small you lose
speed and nothing else.

The tiers that matter in production are the ones a browser cannot run:

```ts
new HierarchicalStore([
  new TtlStore(new LruStore({ maxBytes: 100_000_000 }), { ttlMs: 60_000 }),
  new CompressedStore(new NodeFsStore({ directory: ".cache" })),
  createOptimizedUpstashStore({ maxRequestBytes: 1_000_000 }),
], { populateOnMiss: true })
```

`TtlStore` bounds how long a warm tier may keep masking a fresher remote one. `CompressedStore`
shrinks what lands on disk. `NodeFsStore` is the durable local tier. `createOptimizedUpstashStore`
and `createOptimizedVercelStore` share one cache across processes; their subpaths carry optional
peer dependencies, so the `stores` barrel deliberately does not re-export them. `ThrottledStore`
accepts writes without blocking when a tier is slower than the traffic.

Always `await store.flush()` before exit. Debounced and remote tiers batch their writes, and
flushing is what makes them attempt those writes. It promises the attempt, not the success. A
process that exits without flushing loses whatever the last batch held, and on a long backfill that
is the expensive part.
