/**
 * Creates a new keyed mutex for per-key serialization.
 * Operations on different keys run concurrently; operations on the same key are queued.
 *
 * Useful for preventing read-modify-write race conditions when multiple
 * concurrent operations may target the same resource.
 *
 * @example
 * const { withKeyedMutex } = createKeyedMutex()
 *
 * // Concurrent operations on different keys run in parallel
 * await Promise.all([
 *   withKeyedMutex('groupA', () => writeToGroupA()),
 *   withKeyedMutex('groupB', () => writeToGroupB()),
 * ])
 *
 * // Concurrent operations on the same key are serialized
 * await Promise.all([
 *   withKeyedMutex('groupA', () => writeToGroupA1()), // runs first
 *   withKeyedMutex('groupA', () => writeToGroupA2()), // waits for first
 * ])
 */
export function createKeyedMutex() {
  const mutex = new Map<string, Promise<void>>();

  return {
    /**
     * Wraps an async function with per-key serialization using the keyed mutex.
     *
     * @param key The key to lock on
     * @param fn The async function to execute while holding the lock
     */
    async withKeyedMutex<T>(key: string, fn: () => Promise<T>): Promise<T> {
      const currentLock = mutex.get(key) ?? Promise.resolve();

      let releaseLock!: () => void;
      const newLock = new Promise<void>((resolve) => {
        releaseLock = resolve;
      });
      mutex.set(key, newLock);

      try {
        await currentLock;
        return await fn();
      } finally {
        releaseLock();
        // Cleanup if no other waiter has queued behind us
        if (mutex.get(key) === newLock) {
          mutex.delete(key);
        }
      }
    },
  };
}
