/**
 * Browser stand-in for `node:async_hooks`, aliased in at build time.
 *
 * The library carries its observability scope across awaits through `AsyncLocalStorage` and
 * silently emits nothing where that import fails, which in a browser is always. This tracks a
 * single active scope instead of a real async context, so it is only correct while one scope is
 * open at a time — the UI serializes runs to keep that true.
 */
export class AsyncLocalStorage<T> {
  #current: T | undefined;

  getStore(): T | undefined {
    return this.#current;
  }

  run<R>(store: T, fn: () => R): R {
    const previous = this.#current;
    this.#current = store;

    let result: R;
    try {
      result = fn();
    } catch (error) {
      this.#current = previous;
      throw error;
    }

    if (result instanceof Promise) {
      return result.finally(() => {
        this.#current = previous;
      }) as R;
    }
    this.#current = previous;
    return result;
  }
}
