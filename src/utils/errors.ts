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
