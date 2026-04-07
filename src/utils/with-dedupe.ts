/**
 * Creates a dedupe context that deduplicates concurrent async calls by key.
 * If a call with the same key is already in flight, callers share the existing
 * promise. Once it settles, the key is cleared so subsequent calls execute fresh.
 */
export function createDedupe() {
  const inflight = new Map<string, Promise<unknown>>();

  return {
    withDedupe<T>(fn: () => Promise<T>, opts: { key: string }): Promise<T> {
      const existing = inflight.get(opts.key);
      if (existing !== undefined) return existing as Promise<T>;

      const promise = fn().finally(() => {
        inflight.delete(opts.key);
      });

      inflight.set(opts.key, promise);
      return promise;
    },
  };
}
