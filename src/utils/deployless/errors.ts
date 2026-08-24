import { BaseError, type Hex } from "viem";

import { causeChain, TERMINAL_ERROR } from "../errors.js";

/** Brand marking a {@link DeploylessPartialResultError} across package instances. */
export const DEPLOYLESS_PARTIAL_RESULT = "__viemDlcPartialResult" as const;

/**
 * Thrown by a paged deployless call when some input elements could not be served: the lens
 * declined them, or a single-element retry ran the frame out of gas. Everything else was
 * fetched, and is carried on {@link DeploylessPartialResultError.data}.
 *
 * `call` stays strict and lets this escape; `call2` catches it and returns its payload. It
 * extends viem's `BaseError` so it survives `buildRequest`'s `UnknownRpcError` wrapping, and
 * carries {@link DEPLOYLESS_PARTIAL_RESULT} so it can be recovered from underneath viem's
 * `CallExecutionError` with `error.walk(isDeploylessPartialResultError)` — bare `walk()` returns
 * the *deepest* cause, which is not necessarily this one.
 */
export class DeploylessPartialResultError extends BaseError {
  override name = "DeploylessPartialResultError";

  readonly [DEPLOYLESS_PARTIAL_RESULT] = true;

  /** Terminal for `failover`: the missing elements are unservable on every provider. */
  readonly [TERMINAL_ERROR] = true;

  /** ABI-encoded `U[]` holding every element that was served, in input order, gaps omitted. */
  readonly data: Hex;

  /** Ascending indices into the input array that could not be served. Never empty. */
  readonly missing: readonly number[];

  constructor({ data, missing, total }: { data: Hex; missing: readonly number[]; total: number }) {
    super(`${missing.length} of ${total} elements could not be served`, {
      metaMessages: [`Missing indices: ${missing.join(", ")}`],
    });
    this.data = data;
    this.missing = missing;
  }
}

export function isDeploylessPartialResultError(e: unknown): e is DeploylessPartialResultError {
  return typeof e === "object" && e !== null && (e as Record<string, unknown>)[DEPLOYLESS_PARTIAL_RESULT] === true;
}

/**
 * Recovers a {@link DeploylessPartialResultError} from `e` or anywhere in its `cause` chain,
 * returning `null` if there is none.
 */
export function findDeploylessPartialResult(e: unknown): DeploylessPartialResultError | null {
  for (const cur of causeChain(e)) {
    if (isDeploylessPartialResultError(cur)) return cur;
  }
  return null;
}
