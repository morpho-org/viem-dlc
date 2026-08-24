import { BaseError, type Hex } from "viem";

import { causeChain, TERMINAL_ERROR } from "../errors.js";

/** Brand marking a {@link DeploylessPartialResultError} across package instances. */
export const DEPLOYLESS_PARTIAL_RESULT = "__viemDlcPartialResult" as const;

/**
 * Thrown by a paged deployless call when some input elements could not be served. Everything
 * else was fetched, and is carried on {@link DeploylessPartialResultError.data}.
 *
 * Recover it with {@link findDeploylessPartialResult}, not `error.walk()`, which returns the
 * *deepest* cause rather than necessarily this one.
 */
export class DeploylessPartialResultError extends BaseError {
  override name = "DeploylessPartialResultError";

  readonly [DEPLOYLESS_PARTIAL_RESULT] = true;

  /** Terminal for `failover` — see {@link TERMINAL_ERROR}. */
  readonly [TERMINAL_ERROR] = true;

  /**
   * Marks the error non-retryable to viem, whose `shouldRetry` retries anything lacking a
   * numeric `code`. Unmapped by `buildRequest`'s code switch, so the error passes through
   * intact. See `viem/utils/buildRequest.ts`.
   */
  readonly code = -32099;

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

/** Recovers a {@link DeploylessPartialResultError} from `e` or its `cause` chain, else `null`. */
export function findDeploylessPartialResult(e: unknown): DeploylessPartialResultError | null {
  for (const cur of causeChain(e)) {
    if (isDeploylessPartialResultError(cur)) return cur;
  }
  return null;
}
