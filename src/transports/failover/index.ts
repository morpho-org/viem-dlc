import type { EIP1193RequestFn, RpcSchema, Transport } from "viem";

import { createFacetId, getObservability, observe } from "../../observability.js";
import type { EIP1193Parameters } from "../../types.js";
import { serializeError } from "../../utils/errors.js";

export const failoverTransportKey = "viem-dlc-failover" as const;

export interface FailoverConfig {
  /**
   * Predicate deciding whether an error should propagate immediately rather than
   * trigger a fallover to the next branch. Defaults to mirroring viem's stock
   * `fallback`: contract reverts and user-rejection errors are not retried.
   */
  shouldThrow?: (error: unknown) => boolean;
}

/**
 * Creates a request-level fallback dispatcher. Each branch's factory is invoked
 * exactly once at composition time, so stateful inner transports (coalescing
 * mutexes, rate-limiter token buckets, etc.) persist across requests instead of
 * being rebuilt per call.
 *
 * Each request tries `transports[0].request` first; on an error that is not
 * classified as "should throw," tries `transports[1].request`, and so on. If
 * every branch fails, the last error is rethrown. Halving for range/size errors should be
 * handled inside each branch — `failover` only sees errors that escape.
 *
 * @example
 * const transport = failover([
 *   cache(http(rpcA), [{ ... }, { maxBlockRange: M_A }, ...]),
 *   cache(http(rpcB), [{ ... }, { maxBlockRange: M_B }, ...]),
 * ]);
 */
export function failover<S extends RpcSchema>(
  transports: readonly Transport<string, unknown, EIP1193RequestFn<S>>[],
  { shouldThrow = defaultShouldThrow }: FailoverConfig = {},
): Transport<typeof failoverTransportKey, unknown, EIP1193RequestFn<S>> {
  if (transports.length === 0) {
    throw new Error("[failover] requires at least one transport");
  }

  const facetId = createFacetId(failoverTransportKey);

  return (params) => {
    const requestFns = transports.map((t) => t(params).request);

    const request = async (args: EIP1193Parameters<S>) => {
      const facet = getObservability()?.facet(facetId);
      const stats: {
        branchErrors: unknown[];
        branchDurationsMs: number[];
        succeededIndex: number;
        terminatedByShouldThrow: boolean;
      } = {
        branchErrors: [],
        branchDurationsMs: [],
        succeededIndex: -1,
        terminatedByShouldThrow: false,
      };

      try {
        for (let i = 0; i < requestFns.length; i++) {
          const t0 = performance.now();
          try {
            const result = await requestFns[i]!(args);
            stats.branchDurationsMs.push(performance.now() - t0);
            stats.succeededIndex = i;
            return result;
          } catch (err) {
            stats.branchDurationsMs.push(performance.now() - t0);
            stats.branchErrors.push(err);
            if (shouldThrow(err)) {
              stats.terminatedByShouldThrow = true;
              throw err;
            }
          }
        }
        throw stats.branchErrors.at(-1);
      } finally {
        facet?.set({
          branches_attempted: stats.branchErrors.length + (stats.succeededIndex >= 0 ? 1 : 0),
          succeeded_index: stats.succeededIndex,
          branch_durations_ms: stats.branchDurationsMs,
        });
        if (stats.branchErrors.length > 0) {
          facet?.set({
            branch_errors: stats.branchErrors.map(serializeError),
            terminated_by_should_throw: stats.terminatedByShouldThrow,
          });
        }
      }
    };

    const observed = observe(request, facetId) as EIP1193RequestFn;

    // Bypass `createTransport` so we don't add a redundant `buildRequest` layer.
    // Failover doesn't classify errors, retry, or dedupe — wrapping here would only
    // re-wrap errors already classified by each branch's own `buildRequest`.
    return {
      config: {
        key: failoverTransportKey,
        name: "[viem-dlc] failover",
        type: failoverTransportKey,
        request: observed,
        retryCount: 0,
      },
      request: observed,
    };
  };
}

/**
 * Mirrors viem's stock fallback `shouldThrow`: don't fall over on contract reverts
 * or user-rejection errors. See `node_modules/viem/_esm/clients/transports/fallback.js`.
 */
export function defaultShouldThrow(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;

  const code = (error as { code?: unknown }).code;
  const message = (error as { message?: unknown }).message;
  if (typeof code !== "number") return false;

  // TransactionRejectedRpcError
  if (code === -32003) return true;
  // UserRejectedRequestError
  if (code === 4001) return true;
  // CAIP UserRejectedRequestError
  if (code === 5000) return true;
  // ExecutionRevertedError (code 3) — also catch by message in case the code was
  // remapped upstream.
  if (code === 3) return true;
  if (typeof message === "string" && /execution reverted|gas required exceeds allowance/.test(message)) {
    return true;
  }

  return false;
}
