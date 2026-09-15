import type { PageGas } from "../../src/utils/deployless/codec.inner.js";

/** The telemetry a page reports over attempts that cost `costs`, in a frame with `budget` to spend after `fixed`. */
export function gasOf(costs: readonly number[], budget = 10_000_000, fixed = 100_000): PageGas {
  const gas: PageGas = { budget: BigInt(budget), fixed: BigInt(fixed), sum: 0n, sumSquares: 0n, max: 0n };
  for (const c of costs.map(BigInt)) {
    gas.sum += c;
    gas.sumSquares += c * c;
    if (c > gas.max) gas.max = c;
  }
  return gas;
}

/** Telemetry of `served` attempts at one flat `cost` each. */
export const flatGas = (served: number, cost = 1_000, budget = 10_000_000): PageGas =>
  gasOf(Array<number>(served).fill(cost), budget);
