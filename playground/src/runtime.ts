import { withLogging } from "@morpho-org/viem-dlc";
import { rateLimiter } from "@morpho-org/viem-dlc/transports";
import { http, type Transport } from "viem";
import { base } from "viem/chains";

import { compileLens } from "./compile.js";
import type { Example, Settings, TabContext } from "./examples/types.js";
import { LENS_NAME } from "./lens.js";
import { createCapturingLogger, type WideEvent } from "./logger.js";
import { evaluateTab } from "./tab.js";

/**
 * The page ships pointing at a public endpoint, which answers `over rate limit` long before any
 * transport-level limit binds — so the floor here is set for that, not for a paid endpoint.
 */
const POLITE = [{ maxRequestsPerSecond: 4, maxBurstRequests: 1, maxConcurrentRequests: 2 }] as const;

export type RunOutcome = {
  events: WideEvent[];
  summary: Record<string, string | number>;
  elapsedMs: number;
  requests: number;
  compiledInBrowser: boolean;
};

export type RunInput = {
  example: Example;
  settings: Settings;
  script: string;
  /** Present only when the example declares Solidity. */
  solidity?: string;
};

/** `http`, counting the requests that actually leave, under whatever wraps it. */
function countingHttp(url: string, counter: { n: number }): Transport {
  return ((params) => {
    const instance = http(url)(params);
    const request = ((body: never) => {
      counter.n += 1;
      return instance.request(body);
    }) as typeof instance.request;
    return { ...instance, request };
  }) as Transport;
}

export async function run(input: RunInput, log: (message: string) => void): Promise<RunOutcome> {
  const tab = evaluateTab(input.script, input.example.modules);

  const edited = input.solidity !== undefined && input.solidity.trim() !== input.example.solidity?.trim();
  if (edited) log("compiling Solidity (fetching solc on first use)…");
  const lens = input.example.solidity
    ? edited
      ? await compileLens(LENS_NAME, input.solidity!)
      : (await import("./lens.js")).positionsLens
    : undefined;

  const counter = { n: 0 };
  const context: TabContext = {
    transport: rateLimiter(countingHttp(input.settings.rpcUrl!, counter) as never, POLITE as never) as never,
    chain: base,
    settings: input.settings,
    log,
    lens,
  };

  Object.assign(context, await input.example.prepare?.(context));

  const events: WideEvent[] = [];
  const logger = createCapturingLogger((event) => events.push(event));

  const started = performance.now();
  const result = await withLogging(() => tab(context), { logger, service: "playground" });

  return {
    events,
    summary: result.summary,
    elapsedMs: performance.now() - started,
    requests: counter.n,
    compiledInBrowser: edited,
  };
}
