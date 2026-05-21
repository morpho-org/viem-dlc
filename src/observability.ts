import { deepTransform } from "./utils/objects.js";
import { pick } from "./utils/pick.js";

/**
 * Minimal structural slice of the `loglayer` `LogLayer` interface — only the methods
 * this library calls. Defined locally so `loglayer` is a *true* optional dep: the
 * emitted `.d.ts` files reference `Logger` rather than `import("loglayer").LogLayer`,
 * so consumers who don't pass a logger don't need to install `loglayer` to typecheck.
 * A real `LogLayer` instance satisfies this structurally.
 */
export interface Logger {
  child(): Logger;
  withContext(context: Record<string, unknown>): Logger;
  withMetadata(metadata: Record<string, unknown>): Logger;
  withError(error: unknown): Logger;
  info(message?: string): void;
  warn(message?: string): void;
  error(message?: string): void;
  metadataOnly(metadata: Record<string, unknown>): void;
}

export type Observability = {
  logger?: Logger;
  call_id?: string;
  /**
   * Monotonic ordinal incremented on every `observe` boundary crossing inside this
   * call's scope. Transports can stamp it into their dotted keys (e.g.
   * `viem-dlc-logs-sieve.${counter}.logs_dropped`) so that the same transport invoked
   * N times in one outer request (e.g. a log-sieve under a divider fan-out) doesn't
   * clobber its `withContext` field N-1 times.
   */
  counter?: number;
};

/**
 * Per-operation ALS scope.
 *
 * `parentLogger` and `context` are the seed captured by `withLogging`; they're
 * used once by the outermost `observe` for a given `client.request` to derive
 * `logger` (a single `.child().withContext({ ...context, method, call_id })`).
 * Inner viem-dlc transport layers reuse the same `logger` and `call_id`, and
 * each increments `counter` so they can disambiguate their emitted keys.
 */
interface Scope {
  parentLogger: Logger;
  context: Record<string, unknown>;
  logger?: Logger;
  call_id?: string;
  counter?: number;
}

let als:
  | {
      getStore(): Scope | undefined;
      run<R>(store: Scope, fn: () => R): R;
    }
  | undefined;

// Lazy import: avoid pulling `node:async_hooks` into bundles that don't need it.
// In environments without `AsyncLocalStorage` (e.g. browsers without a polyfill),
// `withLogging` becomes a no-op and the library emits nothing.
async function loadAls() {
  if (als !== undefined) return als;
  try {
    const { AsyncLocalStorage } = await import("node:async_hooks");
    als = new AsyncLocalStorage<Scope>();
    return als;
  } catch {
    return undefined;
  }
}

export interface WithLoggingOpts {
  logger: Logger;
  /** Additional context fields stamped onto the scope's child logger. */
  [key: string]: unknown;
}

/**
 * Opens an ALS scope holding a child `Logger` instance for the duration of `fn`.
 * Every viem-dlc transport call made inside `fn` (synchronously or via awaits)
 * emits events through descendants of this child. Outside the scope, the library
 * emits nothing.
 *
 * Parallel `client.request` calls inside one `withLogging` scope each get their
 * own per-call child via `enterRequest`, fully isolated from one another.
 *
 * In environments without `AsyncLocalStorage`, this is a no-op pass-through.
 */
export async function withLogging<T>(fn: () => Promise<T> | T, opts: WithLoggingOpts): Promise<T> {
  const storage = await loadAls();
  if (!storage) return fn();

  const { logger, ...rest } = opts;

  return storage.run({ parentLogger: logger, context: rest }, fn);
}

/**
 * Inherit-or-originate primitive used by every viem-dlc transport's `request` fn.
 */
export function observe<Input, Output>(
  fn: (req: Input, observability?: Observability) => Promise<Output>,
): (req: Input) => Promise<Output> {
  return (req: Input) => {
    const scope = als?.getStore();
    if (!als || !scope) return fn(req);
    if (scope.logger) {
      // Inner traversal: bump the shared ordinal so each layer sees a fresh slot.
      scope.counter = (scope.counter ?? 0) + 1;
      return fn(req, pick(scope, ["logger", "call_id", "counter"]));
    }

    const call_id = crypto.randomUUID();
    const logger = scope.parentLogger.child().withContext({
      library: "viem-dlc",
      ...scope.context,
      req: deepTransform(req, {
        /** Trims strings longer than 100 characters, adding a trailing '...' in place of the last 3 chars. */
        transformLeaf: <T>(v: T) => (typeof v === "string" && v.length > 100 ? v.slice(0, 97).concat("...") : v) as T,
      }),
      call_id,
    });
    return als.run({ ...scope, logger, call_id, counter: 0 }, () => {
      const t0 = performance.now();
      return fn(req, { logger, call_id, counter: 0 }).finally(() =>
        logger?.withContext({ duration_ms: performance.now() - t0 }).info("concluded"),
      );
    });
  };
}
