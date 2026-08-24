import { BaseError, type Hex } from "viem";

/** Brand marking a {@link DeploylessPartialResultError} across package instances. */
export const DEPLOYLESS_PARTIAL_RESULT = "__viemDlcPartialResult" as const;

/**
 * Thrown by a paged deployless call when some input elements could not be served: the lens
 * declined them, or a single-element retry ran the frame out of gas. Every other element was
 * fetched successfully and is carried on {@link DeploylessPartialResultError.outputs}.
 *
 * Extends viem's `BaseError` so it survives `createTransport`'s `UnknownRpcError` wrapping, and
 * carries {@link DEPLOYLESS_PARTIAL_RESULT} so `error.walk(isDeploylessPartialResultError)` can
 * find it under viem's `CallExecutionError` without relying on `instanceof` across package
 * instances.
 */
export class DeploylessPartialResultError extends BaseError {
  override name = "DeploylessPartialResultError";

  readonly [DEPLOYLESS_PARTIAL_RESULT] = true;

  /** Per-element outputs aligned to the request's input array; `undefined` at every missing index. */
  readonly outputs: readonly (Hex | undefined)[];

  /** Ascending indices into the request's input array that could not be served. */
  readonly missing: readonly number[];

  constructor({ outputs, missing }: { outputs: readonly (Hex | undefined)[]; missing: readonly number[] }) {
    super(`${missing.length} of ${outputs.length} elements could not be served`, {
      metaMessages: [`Missing indices: ${missing.join(", ")}`],
    });
    this.outputs = outputs;
    this.missing = missing;
  }
}

export function isDeploylessPartialResultError(e: unknown): e is DeploylessPartialResultError {
  return typeof e === "object" && e !== null && (e as Record<string, unknown>)[DEPLOYLESS_PARTIAL_RESULT] === true;
}
