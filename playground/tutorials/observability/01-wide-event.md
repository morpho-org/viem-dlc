`withLogging` takes a `logger` and anything else you want stamped on every event from that scope —
`service`, a request id, a tenant. The step below passes a logger that collects instead of printing,
so you can count what arrives.

It runs the same read twice: once outside the scope, once inside. Outside, nothing. Inside, one
event carrying fields from every transport the call crossed.

**The `Logger` interface is structural.** It's `child`, `withContext`, `metadataOnly`, `info`,
`warn`, `error`, and a small builder with `withMetadata` and `withError`. A
[LogLayer](https://loglayer.dev) instance satisfies it as-is:

```ts
import { ConsoleTransport, LogLayer } from 'loglayer'

const logger = new LogLayer({ transport: new ConsoleTransport({ logger: console }) })
await withLogging(() => getLogs2(client, query), { logger, service: 'indexer' })
```

`loglayer` is **not** a dependency of this package and isn't installed on this page — it's the
shape, not the requirement. Anything matching the interface works, including the dozen lines in the
step below. If you never call `withLogging`, none of it is reachable and the cost is zero.

Scopes nest, and the innermost one wins. That's how this page works: the playground opens a scope to
draw its own table, and the script opens its own inside it to collect events for the summary.
