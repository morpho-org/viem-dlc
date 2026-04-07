interface AugmentedArray<T> extends ReadonlyArray<T> {
  /**
   * Bounded-concurrency replacement for `Promise.all(arr.map(fn))`.
   *
   * Only `maxConcurrent` invocations of `fn` are live at any time, so peak
   * memory is O(maxConcurrent) instead of O(arr.length). Results are returned
   * in the same order as the source array.
   */
  mapAsync<U>(fn: (value: T, index: number) => Promise<U>, opts: { maxConcurrent: number }): Promise<U[]>;
}

export function augment<T>(items: readonly T[]): AugmentedArray<T> {
  return Object.assign(items, {
    async mapAsync<U>(fn: (value: T, index: number) => Promise<U>, opts: { maxConcurrent: number }): Promise<U[]> {
      if (opts.maxConcurrent < 1) {
        throw new Error(`mapAsync: maxConcurrent must be at least 1, got ${opts.maxConcurrent}`);
      }

      const results = new Array<U>(items.length);
      let next = 0;

      async function worker(): Promise<void> {
        while (next < items.length) {
          const i = next++;
          results[i] = await fn(items[i]!, i);
        }
      }

      await Promise.all(Array.from({ length: Math.min(opts.maxConcurrent, items.length) }, () => worker()));

      return results;
    },
  });
}
