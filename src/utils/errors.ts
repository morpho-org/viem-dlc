import { BaseError } from "viem";

export function tryCatch<T>(fn: () => T) {
  try {
    return { result: fn() };
  } catch (error) {
    return { error };
  }
}

/**
 * Detects timeout-shaped errors anywhere in the BaseError cause chain: viem's TimeoutError,
 * HTTP 408 / 504 / 524, and "timed out" / "timeout" messages.
 */
export function isTimeoutLikeError(error: unknown): boolean {
  if (error instanceof BaseError) return error.walk(isTimeoutShape) !== null;
  return isTimeoutShape(error);
}

function isTimeoutShape(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  if (error.name === "TimeoutError") return true;
  const status = (error as { status?: number }).status;
  if (status === 408 || status === 504 || status === 524) return true;
  return /timed?[ -]?out/i.test(error.message);
}

/**
 * Brand marking an error that is an *answer* rather than a failed attempt: it carries a usable
 * payload, so retrying or falling over discards that payload. Dispatchers must propagate it.
 */
export const TERMINAL_ERROR = "__viemDlcTerminal" as const;

/** True when `e`, or anything in its `cause` chain, carries {@link TERMINAL_ERROR}. */
export function isTerminalError(e: unknown): boolean {
  for (const cur of causeChain(e)) {
    if ((cur as Record<string, unknown>)[TERMINAL_ERROR] === true) return true;
  }
  return false;
}

/**
 * Yields `e` and every error in its `cause` chain, tolerating cycles. Classification must look
 * past the outermost error, which is usually a viem wrapper.
 */
export function* causeChain(e: unknown): Generator<object> {
  const seen = new Set<unknown>();
  for (
    let cur: unknown = e;
    cur && typeof cur === "object" && !seen.has(cur);
    cur = (cur as { cause?: unknown }).cause
  ) {
    seen.add(cur);
    yield cur as object;
  }
}
