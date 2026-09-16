import { withLogging } from "@morpho-org/viem-dlc";
import { rateLimiter } from "@morpho-org/viem-dlc/transports";
import { http, type Transport } from "viem";
import { base } from "viem/chains";

import { compileLens } from "./compile.js";
import { PREBUILT } from "./lens.js";
import { createCapturingLogger, type WideEvent } from "./logger.js";
import { evaluateTab } from "./tab.js";
import type { Settings, Step, TabContext, Tutorial } from "./tutorials/types.js";

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
  tutorial: Tutorial;
  step: Step;
  settings: Settings;
  script: string;
  /** Present only when the step declares Solidity. */
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

/**
 * `prepare` runs once per page rather than once per step, so every step of a tutorial reads the same
 * corpus and a comparison between two steps compares the transports rather than the inputs. The key
 * carries the endpoint, because that is what would make the result stale.
 */
const prepared = new Map<string, Promise<Record<string, unknown>>>();

export async function run(
  input: RunInput,
  log: (message: string) => void,
  onEvent?: (event: WideEvent) => void,
): Promise<RunOutcome> {
  const tab = evaluateTab(input.script, input.tutorial.modules);

  const declared = input.step.solidity;
  const edited =
    declared !== undefined && input.solidity !== undefined && input.solidity.trim() !== declared.source.trim();
  if (edited) log("compiling Solidity (fetching solc on first use)");
  const lens = declared
    ? edited
      ? await compileLens(declared.name, input.solidity!)
      : PREBUILT[declared.name]
    : undefined;
  if (declared && !lens) throw new Error(`No prebuilt lens named "${declared.name}"`);

  const counter = { n: 0 };
  const context: TabContext = {
    transport: rateLimiter(countingHttp(input.settings.rpcUrl!, counter) as never, POLITE as never) as never,
    chain: base,
    settings: input.settings,
    log,
    lens,
  };

  if (input.tutorial.prepare) {
    const key = `${input.tutorial.id} ${input.settings.rpcUrl}`;
    let pending = prepared.get(key);
    if (!pending) {
      pending = input.tutorial.prepare(context);
      prepared.set(key, pending);
      // A failed corpus fetch must not poison every later run of the page.
      pending.catch(() => prepared.delete(key));
    }
    Object.assign(context, await pending);
  }

  const events: WideEvent[] = [];
  const logger = createCapturingLogger((event) => {
    events.push(event);
    onEvent?.(event);
  });

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
