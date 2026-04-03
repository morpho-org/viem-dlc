/**
 * Creates a debounced wrapper around an async function.
 *
 * - Waits `debounceMs` before invoking `fn`. Each new call resets the timer.
 * - After `maxDelayMs` from the first call in a window, `fn` fires regardless.
 * - At most one `fn` invocation runs at a time. Calls that arrive while `fn`
 *   is running are tracked against the pending window; when `fn` completes,
 *   if `maxDelayMs` has already been exceeded the queued work fires immediately,
 *   otherwise it is scheduled with the remaining time.
 * - `immediate(args)` invokes immediately when idle. If `fn` is already
 *   running, it signals abort on the in-flight AbortController and fires
 *   the queued args as soon as the current invocation settles (no debounce).
 * - `cancel()` clears any pending debounce without invoking `fn`.
 * - If `maxStalenessMs` is set and the pending args have been waiting longer
 *   than that when `fn` completes, they are dropped instead of executed.
 *   Useful in serverless environments where the process may be frozen/thawed.
 */
export function debounce<TArgs extends unknown[]>(
  fn: (signal: AbortSignal, ...args: TArgs) => Promise<void>,
  opts: {
    debounceMs: number;
    maxDelayMs: number;
    maxStalenessMs: number;
    onError?: (error: unknown, args: TArgs) => void;
  },
): DebouncedFn<TArgs> {
  let pending:
    | { args: TArgs; argsSetAt: number; windowStartedAt: number; immediate: boolean }
    | undefined;
  let debounceTimer: ReturnType<typeof setTimeout> | undefined;
  let maxDelayTimer: ReturnType<typeof setTimeout> | undefined;
  let running: Promise<void> | undefined;
  let runningAbort: AbortController | undefined;

  /** Allow the process to exit even if this timer is still pending. */
  const unref = (t: ReturnType<typeof setTimeout>) => {
    if (typeof t === "object" && t !== null && "unref" in t && typeof t.unref === "function") {
      t.unref();
    }
  };

  const isAbortError = (err: unknown) =>
    (err instanceof DOMException || err instanceof Error) &&
    err.name === "AbortError";

  const isStale = () =>
    pending !== undefined &&
    Date.now() - pending.argsSetAt > opts.maxStalenessMs;

  const clear = () => {
    clearTimeout(debounceTimer);
    clearTimeout(maxDelayTimer);
    debounceTimer = undefined;
    maxDelayTimer = undefined;
    pending = undefined;
  };

  /** Set timers based on the current pending state (does not mutate it). */
  const arm = () => {
    if (!pending) return;
    const now = Date.now();
    const remainingMaxDelay = Math.max(0, opts.maxDelayMs - (now - pending.windowStartedAt));
    const remainingDebounce = Math.max(0, pending.argsSetAt + opts.debounceMs - now);

    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(flush, Math.min(remainingDebounce, remainingMaxDelay));
    unref(debounceTimer);

    if (maxDelayTimer === undefined) {
      maxDelayTimer = setTimeout(flush, remainingMaxDelay);
      unref(maxDelayTimer);
    }
  };

  /** Timer callback: invoke if ready, otherwise discard stale work. */
  const flush = () => {
    if (!pending) return;
    if (isStale()) { clear(); return; }
    invoke(pending.args);
  };

  const invoke = (args: TArgs) => {
    clear();
    const ac = new AbortController();
    runningAbort = ac;

    const p = fn(ac.signal, ...args)
      .catch((err) => {
        if (!isAbortError(err)) opts.onError?.(err, args);
      })
      .finally(() => {
        running = undefined;
        runningAbort = undefined;

        if (!pending) return;
        if (isStale()) { clear(); return; }

        if (
          pending.immediate ||
          Date.now() - pending.windowStartedAt >= opts.maxDelayMs
        ) {
          invoke(pending.args);
        } else {
          arm();
        }
      });
    running = p;
  };

  const schedule = (args: TArgs) => {
    const now = Date.now();
    pending = {
      args,
      argsSetAt: now,
      windowStartedAt: pending?.windowStartedAt ?? now,
      immediate: pending?.immediate ?? false,
    };
    if (!running) arm();
  };

  const debounced = (...args: TArgs) => { schedule(args); };

  debounced.immediate = (...args: TArgs) => {
    clear();
    if (running) {
      pending = { args, argsSetAt: Date.now(), windowStartedAt: Date.now(), immediate: true };
      runningAbort?.abort();
    } else {
      invoke(args);
    }
  };

  debounced.cancel = () => {
    clear();
    const dying = running;
    runningAbort?.abort();
    return dying;
  };

  return debounced;
}

export type DebouncedFn<TArgs extends unknown[]> = {
  (...args: TArgs): void;
  immediate(...args: TArgs): void;
  /** Clear pending work, abort any in-flight invocation, and return the dying promise (if any). */
  cancel(): Promise<void> | undefined;
};
