`withLogging` takes a `logger` and anything else you want stamped on every event from that scope —
`service`, a request id, a tenant. The step below passes a logger that collects instead of printing,
so you can count what arrives.

It runs the same read twice: once outside the scope, once inside. Outside, nothing. Inside, one event
carrying fields from every transport the call crossed.

**The `Logger` interface is structural**, and small enough to state in full:

```ts
interface Logger extends LogBuilder {
  child(): Logger
  withContext(context: Record<string, unknown>): Logger
  metadataOnly(metadata: Record<string, unknown>): void
}

interface LogBuilder {
  withMetadata(metadata: Record<string, unknown>): LogBuilder
  withError(error: unknown): LogBuilder
  info(message?: string): void
  warn(message?: string): void
  error(message?: string): void
}
```

A [LogLayer](https://loglayer.dev) instance satisfies it as-is:

```ts
import { ConsoleTransport, LogLayer } from 'loglayer'

const logger = new LogLayer({ transport: new ConsoleTransport({ logger: console }) })
await withLogging(() => getLogs2(client, query), { logger, service: 'indexer' })
```

`loglayer` is **not** a dependency of this package and isn't installed on this page — it's the shape,
not the requirement. Anything matching the interface works, including the dozen lines in the step
below, so your logging stack is your choice and not this library's. If you never call `withLogging`,
none of it is reachable and the cost is zero.

Scopes nest, and the innermost one wins. That's how this page works: the playground opens a scope to
draw its own table, and the script opens its own inside it to collect events for the summary.
