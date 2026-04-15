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
      let settled!: Promise<void>;
      const cleanup = () => {
        inFlight.delete(settled);
      };
      settled = promise.then(cleanup, cleanup);

      inFlight.add(settled);

      return promise;
    },

    async flush() {
      await Promise.all([...inFlight]);
    },
  };
}
