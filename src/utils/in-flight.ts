/**
 * Tracks in-flight promises and exposes a barrier for all promises accepted so far.
 *
 * - `track` returns the original promise unchanged.
 * - `flush` waits for tracked promises to settle, but never rejects.
 */
export function createInFlightBarrier() {
  const inFlight = new Set<Promise<void>>();

  return {
    track<T>(promise: Promise<T>): Promise<T> {
      const settled = Promise.resolve(promise)
        .then(
          () => {},
          () => {},
        )
        .finally(() => {
          inFlight.delete(settled);
        });

      inFlight.add(settled);

      return promise;
    },

    async flush() {
      await Promise.all([...inFlight]);
    },
  };
}
