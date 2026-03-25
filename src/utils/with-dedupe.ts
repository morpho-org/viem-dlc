// biome-ignore lint/suspicious/noExplicitAny: cache holds heterogeneous promise types
const promiseCache = new Map<string, Promise<any>>();

/**
 * Wraps an async function with deduplication by id.
 * If a promise with the same id is already in-flight, returns that
 * promise instead of calling `fn` again. The cache entry is automatically
 * removed when the promise settles.
 *
 * @param fn The async function to execute (skipped if a matching id is in-flight)
 * @param options.enabled Whether deduplication is active (default: true)
 * @param options.id Deduplication key. If omitted, `fn` always runs.
 */
export function withDedupe<T>(
  fn: () => Promise<T>,
  { enabled = true, id }: { enabled?: boolean; id?: string },
): Promise<T> {
  if (!enabled || !id) return fn();
  if (promiseCache.has(id)) return promiseCache.get(id)!;

  const promise = fn().finally(() => promiseCache.delete(id));
  promiseCache.set(id, promise);
  return promise;
}
