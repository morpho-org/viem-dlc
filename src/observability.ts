import { estimateUtf8Bytes } from "./utils/json.js";
import { deepTransform } from "./utils/objects.js";

/**
 * Minimal structural slice of `loglayer`'s `LogLayer`, which satisfies it directly.
 * Declared locally so `loglayer` stays a *true* optional dep: the emitted `.d.ts`
 * refers to `Logger`, not `import("loglayer").LogLayer`, so consumers who don't use
 * it needn't install it to typecheck.
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

/** Non-enumerable brand {@link observe} stamps on errors it has already emitted. */
const observedSymbol = Symbol.for("viem-dlc.observed");

function markObserved(error: unknown): void {
  if (typeof error !== "object" || error === null) return;
  try {
    Object.defineProperty(error, observedSymbol, { value: true });
  } catch {
    // Non-extensible errors can't be branded; the rethrow still proceeds.
  }
}

/**
 * Whether `error`, or anything on its `cause` chain, was already emitted by
 * {@link observe} — so hosts that also log escaped errors can skip the duplicate.
 * The chain walk matters because viem wraps transport failures before they
 * reach the host, so the brand usually sits on a `cause`.
 */
export function isObserved(error: unknown): boolean {
  const seen = new Set<unknown>();
  try {
    for (let e = error; typeof e === "object" && e !== null && !seen.has(e); e = (e as { cause?: unknown }).cause) {
      if ((e as Record<symbol, unknown>)[observedSymbol] === true) return true;
      seen.add(e);
    }
  } catch {
    // A throwing accessor (proxy trap, hostile getter) must not escape host error handling.
  }
  return false;
}

/**
 * Accumulates one transport instance's fields on the call's wide event, under the
 * prefix its {@link FacetId} claims.
 *
 * Re-allocating a facet for the same id returns the same slot, so a layer crossed
 * many times per call (e.g. once per chunk under a divider fan-out) aggregates
 * naturally. Writes are valid at any point in the operation's lifetime, including
 * after `await`s. All facets share one byte budget per event; if it is exceeded,
 * the largest fields are dropped and named in `truncated_fields`.
 */
export interface Facet {
  /**
   * Merges fields into this facet's slot; last write per field wins. Reserve it for
   * once-per-call facts — on a layer crossed repeatedly, prefer `add`/`stat`/`push`.
   */
  set(fields: Record<string, unknown>): void;
  /** Adds `n` (default 1) to a numeric accumulator field. */
  add(field: string, n?: number): void;
  /**
   * Records a sample into a streaming summary. Emitted at conclusion as
   * `${field}.count`, `${field}.min`, `${field}.max`, and `${field}.avg`.
   */
  stat(field: string, sample: number): void;
  /**
   * Appends to a bounded array (default limit 10). Values pushed past the limit
   * are dropped and counted in `${field}_truncated`.
   */
  push(field: string, value: unknown, limit?: number): void;
  /** Returns a facet writing under `prefix` within this same slot. */
  sub(prefix: string): Facet;
}

/**
 * Identity of one transport instance, naming its fields on the wide event.
 *
 * The first id of a given key touched during a call writes bare `${key}.${field}`
 * fields; later ids sharing that key (e.g. one cache per failover branch) get
 * `${key}.1.*`, `${key}.2.*`, ... in first-touch order — stable across calls
 * because transports traverse in a fixed order for a given composition.
 *
 * Create exactly one per composition node, at transport-factory scope, and pass it
 * to both {@link observe} and {@link Observability.facet}: identity is by object
 * reference, so an id created per request would claim a fresh label every call.
 */
export interface FacetId {
  readonly key: string;
}

export function createFacetId(key: string): FacetId {
  return { key };
}

export type Observability = {
  logger: Logger;
  call_id: string;
  facet(id: FacetId): Facet;
};

/** Streaming summary accumulator backing `Facet["stat"]`. */
interface StatAcc {
  count: number;
  sum: number;
  min: number;
  max: number;
}

/** Per-call state, shared by reference across the call's ALS scope; every facet writes here. */
interface RootState {
  /** First-touch labels: facet key → (id → field prefix). */
  labels: Map<string, Map<FacetId, string>>;
  /** Flattened `${prefix}.${field}` → value. */
  fields: Record<string, unknown>;
  /** Streaming summaries, keyed by full dotted field path. */
  stats: Map<string, StatAcc>;
}

class FacetImpl implements Facet {
  constructor(
    private readonly root: RootState,
    private readonly prefix: string,
  ) {}

  set(fields: Record<string, unknown>): void {
    for (const [k, v] of Object.entries(fields)) this.root.fields[`${this.prefix}.${k}`] = v;
  }

  add(field: string, n = 1): void {
    const fk = `${this.prefix}.${field}`;
    const current = this.root.fields[fk];
    this.root.fields[fk] = (typeof current === "number" ? current : 0) + n;
  }

  stat(field: string, sample: number): void {
    const fk = `${this.prefix}.${field}`;
    let acc = this.root.stats.get(fk);
    if (!acc) {
      acc = { count: 0, sum: 0, min: Number.POSITIVE_INFINITY, max: Number.NEGATIVE_INFINITY };
      this.root.stats.set(fk, acc);
    }
    acc.count += 1;
    acc.sum += sample;
    if (sample < acc.min) acc.min = sample;
    if (sample > acc.max) acc.max = sample;
  }

  push(field: string, value: unknown, limit = 10): void {
    const fk = `${this.prefix}.${field}`;
    let arr = this.root.fields[fk] as unknown[];
    if (!Array.isArray(arr)) {
      arr = [];
      this.root.fields[fk] = arr;
    }
    if (arr.length < limit) {
      arr.push(value);
    } else {
      const tk = `${fk}_truncated`;
      this.root.fields[tk] = ((this.root.fields[tk] as number) ?? 0) + 1;
    }
  }

  sub(prefix: string): Facet {
    return new FacetImpl(this.root, `${this.prefix}.${prefix}`);
  }
}

/** Suffixes count per key ({@link FacetId}), so unrelated keys never shift each other's labels. */
function resolvePrefix(root: RootState, id: FacetId): string {
  let labels = root.labels.get(id.key);
  if (!labels) {
    labels = new Map();
    root.labels.set(id.key, labels);
  }
  let prefix = labels.get(id);
  if (prefix === undefined) {
    prefix = labels.size === 0 ? id.key : `${id.key}.${labels.size}`;
    labels.set(id, prefix);
  }
  return prefix;
}

/**
 * Per-operation ALS scope. `parentLogger` and `context` are the seed captured by
 * `withLogging`; `obs` and `root` are derived once by the outermost {@link observe}
 * and reused by every inner boundary, so one call produces one wide event.
 */
interface Scope {
  parentLogger: Logger;
  context: Record<string, unknown>;
  obs?: Observability;
  root?: RootState;
}

type AlsLike = {
  getStore(): Scope | undefined;
  run<R>(store: Scope, fn: () => R): R;
};

let als: AlsLike | undefined;
let alsLoad: Promise<AlsLike | undefined> | undefined;

// Imported lazily so bundles that never call `withLogging` don't pull in
// `node:async_hooks`, and so environments lacking it (e.g. unpolyfilled browsers)
// degrade to the no-op path rather than failing to load. Memoizing the in-flight
// promise (not just its result) is load-bearing: scopes racing the first import must
// share one instance, or `withLogging` would open a scope on a storage that `observe`
// isn't reading, and that call would silently emit nothing.
function loadAls(): Promise<AlsLike | undefined> {
  alsLoad ??= import("node:async_hooks").then(
    ({ AsyncLocalStorage }) => {
      als = new AsyncLocalStorage<Scope>();
      return als;
    },
    () => undefined,
  );
  return alsLoad;
}

export interface WithLoggingOpts {
  logger: Logger;
  /** Additional context fields, stamped onto every event emitted in this scope. */
  [key: string]: unknown;
}

/**
 * Opens an ALS scope seeding `logger` and `opts` for the duration of `fn`. Each
 * outermost viem-dlc transport call made inside `fn` (synchronously or via awaits)
 * derives its own child logger and emits one wide event; the transport layers it
 * nests through contribute fields to that same event. Outside the scope, the
 * library emits nothing.
 *
 * Parallel `client.request` calls inside one `withLogging` scope are fully
 * isolated from one another.
 *
 * In environments without `AsyncLocalStorage`, this is a no-op pass-through.
 */
export async function withLogging<T>(fn: () => Promise<T> | T, opts: WithLoggingOpts): Promise<T> {
  const storage = await loadAls();
  if (!storage) return fn();

  const { logger, ...rest } = opts;

  return storage.run({ parentLogger: logger, context: rest }, fn);
}

/** Soft ceiling on accumulated facet bytes emitted on one wide event. */
const MAX_FIELDS_BYTES = 32 * 1024;

function fieldSize(key: string, value: unknown): number {
  try {
    return key.length + estimateUtf8Bytes(value);
  } catch {
    // Unestimable (e.g. circular) values are treated as oversized so they're dropped first.
    return Number.POSITIVE_INFINITY;
  }
}

/**
 * Resolves stat accumulators into scalar fields and enforces `MAX_FIELDS_BYTES`,
 * dropping the largest fields first and recording their names in `truncated_fields`.
 */
function finalizeFields(root: RootState): Record<string, unknown> {
  const { fields } = root;

  for (const [fk, acc] of root.stats) {
    fields[`${fk}.count`] = acc.count;
    fields[`${fk}.min`] = acc.min;
    fields[`${fk}.max`] = acc.max;
    fields[`${fk}.avg`] = acc.sum / acc.count;
  }

  let total = 0;
  for (const [k, v] of Object.entries(fields)) total += fieldSize(k, v);
  if (total <= MAX_FIELDS_BYTES) return fields;

  // Over budget (rare): re-measure so the largest fields can be dropped first.
  const sizes = Object.entries(fields)
    .map(([k, v]) => [k, fieldSize(k, v)] as const)
    .sort((a, b) => b[1] - a[1]);
  const dropped: string[] = [];
  for (const [k, size] of sizes) {
    if (total <= MAX_FIELDS_BYTES) break;
    delete fields[k];
    dropped.push(k);
    total -= size;
  }
  fields.truncated_fields = dropped;

  return fields;
}

/**
 * Inherit-or-originate primitive used by every viem-dlc transport's `request` fn.
 *
 * The outermost boundary for a call derives the per-call child logger and facet
 * accumulator, then emits one `"concluded"` wide event when the call settles:
 * `info`-level on success, `error`-level with `withError` on rejection. Inner
 * boundaries contribute to that same event; no new ALS store is created past the
 * outermost one.
 *
 * Every crossing (outermost included) increments `id`'s `crossings` field, so the
 * event records which transports this call traversed and how many times each.
 * Running on boundary entry also pins `id`'s label before any handler writes.
 */
export function observe<F extends (req: never) => Promise<unknown>>(fn: F, id: FacetId): F {
  const countCrossing = (root: RootState) => {
    const fk = `${resolvePrefix(root, id)}.crossings`;
    root.fields[fk] = ((root.fields[fk] as number) ?? 0) + 1;
  };
  const wrapped = (req: Parameters<F>[0]) => {
    const scope = als?.getStore();
    if (!als || !scope) return fn(req);
    if (scope.root) {
      countCrossing(scope.root);
      return fn(req);
    }

    const call_id = crypto.randomUUID();
    const logger = scope.parentLogger.child().withContext({
      // Seeded context first, so the canonical fields below can't be overwritten.
      ...scope.context,
      library: "viem-dlc",
      // Trimmed so a large calldata or filter payload can't dominate the event.
      req: deepTransform(req, {
        transformLeaf: <T>(v: T) => (typeof v === "string" && v.length > 100 ? v.slice(0, 97).concat("...") : v) as T,
      }),
      call_id,
    });
    const root: RootState = { labels: new Map(), fields: {}, stats: new Map() };
    countCrossing(root);
    const obs: Observability = {
      logger,
      call_id,
      facet: (facetId) => new FacetImpl(root, resolvePrefix(root, facetId)),
    };
    return als.run({ ...scope, obs, root }, () => {
      const t0 = performance.now();
      const conclude = (status: "ok" | "error", error?: unknown) => {
        const fields = finalizeFields(root);
        fields.status = status;
        fields.duration_ms = performance.now() - t0;
        // `withError` on the error path so hosts wired like Morpho's `@repo/observability`
        // (LogLayer plugin forwarding `.withError()` entries to an ErrorReporter) capture it.
        const enriched = logger.withContext(fields);
        if (status === "ok") enriched.info("concluded");
        else enriched.withError(error).error("concluded");
      };
      return fn(req).then(
        (result) => {
          conclude("ok");
          return result;
        },
        (error) => {
          conclude("error", error);
          markObserved(error);
          throw error;
        },
      );
    });
  };
  return wrapped as F;
}

/**
 * Reads the active per-request observability scope from ambient ALS. Returns
 * `undefined` when called outside a `withLogging` scope, in environments
 * without `AsyncLocalStorage`, or before `observe` has derived a per-call
 * child logger.
 *
 * ALS context flows through `await`s, so this may be called at any point in a
 * transport's lifetime.
 */
export function getObservability(): Observability | undefined {
  return als?.getStore()?.obs;
}
