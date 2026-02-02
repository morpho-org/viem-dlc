/**
 * A mutex map for serializing async operations by key.
 * Each key gets its own Promise-based queue.
 */
export type KeyedMutex = Map<string, Promise<void>>;

/**
 * Creates a new keyed mutex for per-key serialization.
 *
 * @example
 * const mutex = createKeyedMutex()
 *
 * // Concurrent operations on different keys run in parallel
 * await Promise.all([
 *   withKeyedMutex('groupA', () => writeToGroupA(), { mutex }),
 *   withKeyedMutex('groupB', () => writeToGroupB(), { mutex }),
 * ])
 *
 * // Concurrent operations on the same key are serialized
 * await Promise.all([
 *   withKeyedMutex('groupA', () => writeToGroupA1(), { mutex }), // runs first
 *   withKeyedMutex('groupA', () => writeToGroupA2(), { mutex }), // waits for first
 * ])
 */
export function createKeyedMutex(): KeyedMutex {
  return new Map();
}

/**
 * Wraps an async function with per-key serialization using a keyed mutex.
 * Operations on different keys run concurrently; operations on the same key are queued.
 *
 * Useful for preventing read-modify-write race conditions when multiple
 * concurrent operations may target the same resource.
 *
 * @param key The key to lock on
 * @param fn The async function to execute while holding the lock
 * @param options.mutex The keyed mutex to use
 */
export async function withKeyedMutex<T>(
  key: string,
  fn: () => Promise<T>,
  { mutex }: { mutex: KeyedMutex },
): Promise<T> {
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
}
