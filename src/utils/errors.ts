import { BaseError } from "viem";

export function tryCatch<T>(fn: () => T) {
  try {
    return { result: fn() };
  } catch (error) {
    return { error };
  }
}

export function serializeError(e: unknown) {
  if (!(e instanceof Error)) return { value: String(e) };
  return {
    name: e.name,
    message: e.message,
    code: (e as { code?: unknown }).code,
    data: (e as { data?: unknown }).data,
  };
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
