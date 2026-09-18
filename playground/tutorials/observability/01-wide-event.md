`withLogging` takes a `logger` and anything else you want stamped on every event from that scope: a
`service` name, a request id, a tenant. The step below passes a logger that collects events instead
of printing them, so you can count what arrives.

It runs the same read twice, once outside the scope and once inside. Outside, nothing arrives.
Inside, one event arrives, carrying fields from every transport the call crossed.

**The `Logger` interface is structural**, and small enough to show in full:

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

`loglayer` is **not** a dependency of this package and is not installed on this page. It is shown
because it has the right shape, and anything else with that shape works too, including the dozen
lines in the step below. Your logging stack is your choice rather than this library's. If you never
call `withLogging`, none of this is reachable and the cost is zero.

Scopes nest, and the innermost one wins. This page relies on that: the playground opens a scope to
draw its own table, and the script opens another inside it to collect events for the summary.
