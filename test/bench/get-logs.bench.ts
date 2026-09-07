import { bench, describe } from "vitest";

import { createBenchClient, runStrategy, STRATEGIES } from "./get-logs.strategies.ts";

/**
 * Wall-time comparison of the three ways to narrow a shared `eth_getLogs` cache to one account.
 * Needs `RPC_URL`; the first run fills `fixtures/` from the network and is slow.
 */
describe.skipIf(!process.env.RPC_URL)("getLogs2 strategies (wall time)", () => {
  const { client } = createBenchClient(process.env.RPC_URL!);

  for (const strategy of STRATEGIES) {
    bench(
      strategy,
      async () => {
        await runStrategy(client, strategy);
      },
      { warmupIterations: 1, iterations: 5 },
    );
  }
});
