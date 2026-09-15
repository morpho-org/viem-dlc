import { withLogging } from "@morpho-org/viem-dlc";
import { deployless, logsDivider, rateLimiter } from "@morpho-org/viem-dlc/transports";
import {
  type Address,
  type Chain,
  type Client,
  createPublicClient,
  type Hex,
  http,
  parseAbiItem,
  type Transport,
} from "viem";
import { getBlockNumber, getLogs } from "viem/actions";
import { base } from "viem/chains";

import { compileLens } from "./compile.js";
import { LENS_NAME, MORPHO, positionsLens } from "./lens.js";
import { createCapturingLogger, type WideEvent } from "./logger.js";
import { evaluateTab } from "./tab.js";

export type Settings = { rpcUrl: string; gasLimit: number; elements: number };

/** Public endpoints answer `over rate limit` well before the transport's own limits bind. */
const POLITE = [{ maxRequestsPerSecond: 8, maxBurstRequests: 2, maxConcurrentRequests: 3 }] as const;

const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

type Pair = { id: Hex; user: Address };

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

function createClient(settings: Settings, counter: { n: number }, withDivider: boolean): Client<Transport, Chain> {
  const limited = rateLimiter(countingHttp(settings.rpcUrl, counter) as never, POLITE as never);

  return createPublicClient({
    chain: base,
    transport: deployless(
      withDivider
        ? (logsDivider(limited as never, [
            { maxBlockRange: 2_000 },
            { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
            { maxBytes: 8_192 },
            {},
          ]) as never)
        : (limited as never),
      { gasLimit: settings.gasLimit },
    ),
  }) as unknown as Client<Transport, Chain>;
}

let discovered: Pair[] | undefined;

/** Distinct `(market, borrower)` pairs from recent `Borrow` events; discovered once per session. */
async function discover(settings: Settings, log: (message: string) => void): Promise<Pair[]> {
  if (discovered?.length) return discovered;

  log("discovering (market, borrower) pairs…");
  const client = createClient(settings, { n: 0 }, true);
  const toBlock = await getBlockNumber(client);
  const logs = await getLogs(client, {
    address: MORPHO,
    event: borrowEvent,
    strict: true,
    fromBlock: toBlock - 20_000n,
    toBlock,
  });

  const seen = new Map<string, Pair>();
  for (const { args } of logs) seen.set(`${args.id}:${args.onBehalf}`, { id: args.id, user: args.onBehalf });

  discovered = [...seen.values()];
  return discovered;
}

export type RunResult = {
  events: WideEvent[];
  results: number;
  skipped: number;
  elapsedMs: number;
  requests: number;
  distinct: number;
  compiled: boolean;
};

export type RunInput = Settings & {
  /** The tab's JavaScript, as edited. */
  script: string;
  /** The lens Solidity, as edited; recompiled in the browser only when it differs from the build's. */
  solidity: string;
  pristineSolidity: string;
};

export async function run(input: RunInput, log: (message: string) => void): Promise<RunResult> {
  const tab = evaluateTab(input.script);

  const edited = input.solidity.trim() !== input.pristineSolidity.trim();
  if (edited) log("compiling Solidity (fetching solc on first use)…");
  const lens = edited ? await compileLens(LENS_NAME, input.solidity) : positionsLens;

  const pairs = await discover(input, log);
  if (!pairs.length) throw new Error("No Borrow events in the last 20 000 blocks — try a different endpoint.");

  // Repeated to reach the requested count: every element is a real position, so the packing and gas
  // figures are real; only input distinctness is synthetic.
  const inputs = Array.from({ length: input.elements }, (_, i) => pairs[i % pairs.length]!);

  const events: WideEvent[] = [];
  const logger = createCapturingLogger((event) => events.push(event));
  const counter = { n: 0 };
  const client = createClient(input, counter, false);

  const started = performance.now();
  const outcome = await withLogging(() => tab({ client, lens, inputs, log }), { logger, service: "playground" });

  return {
    events,
    results: outcome.results.length,
    skipped: outcome.skipped.length,
    elapsedMs: performance.now() - started,
    requests: counter.n,
    distinct: pairs.length,
    compiled: edited,
  };
}
