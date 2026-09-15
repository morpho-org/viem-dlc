import { withLogging } from "@morpho-org/viem-dlc";
import { readLens } from "@morpho-org/viem-dlc/actions";
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

import { MORPHO, positionsLens } from "./lens.js";
import { createCapturingLogger, type WideEvent } from "./logger.js";

export type Config = {
  rpcUrl: string;
  gasLimit: number;
  envelope: "initcode" | "override";
  compress: boolean;
  continuations: "fill" | "eager";
  elements: number;
  statedGas: boolean;
};

/** Measured on Base; `examples/04-deployless-batching.ts` states the same figures. */
const MEASURED_GAS = { fixed: 242_000, item: { avg: 7_300, stddev: 150 } };

/** Public endpoints answer `over rate limit` well before the transport's own limits bind. */
const POLITE = [{ maxRequestsPerSecond: 8, maxBurstRequests: 2, maxConcurrentRequests: 3 }] as const;

const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

type Pair = { id: Hex; user: Address };

function batchOptions(config: Config) {
  return {
    ...(config.envelope === "override" ? { envelope: "override" as const } : {}),
    ...(config.compress ? { compress: true } : {}),
    ...(config.continuations === "eager" ? { continuations: "eager" as const } : {}),
    ...(config.statedGas ? { gas: MEASURED_GAS } : {}),
  };
}

/** The program the current controls describe — what runs, not a paraphrase of it. */
export function generateSource(config: Config): string {
  const underscored = (n: number) => n.toLocaleString("en-US").replace(/,/g, "_");
  const entries = Object.entries(batchOptions(config)).map(([key, value]) =>
    key === "gas"
      ? `    gas: { fixed: ${underscored(MEASURED_GAS.fixed)}, item: { avg: ${underscored(MEASURED_GAS.item.avg)}, stddev: ${MEASURED_GAS.item.stddev} } },`
      : `    ${key}: ${JSON.stringify(value)},`,
  );

  return `import { readLens } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { createPublicClient, http } from "viem";
import { base } from "viem/chains";

const client = createPublicClient({
  chain: base,
  transport: deployless(http(rpcUrl), { gasLimit: ${underscored(config.gasLimit)} }),
});

const { results, skipped } = await readLens(client, {
  ...positionsLens.with(),
  functionName: "positionOf",
  args: inputs, // ${underscored(config.elements)} (market, borrower) pairs
${entries.length ? `  batch: {\n${entries.join("\n")}\n  },` : "  // batch: {} — every default"}
});`;
}

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

function createClient(config: Config, counter: { n: number }, withDivider: boolean): Client<Transport, Chain> {
  const limited = rateLimiter(countingHttp(config.rpcUrl, counter) as never, POLITE as never);

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
      { gasLimit: config.gasLimit },
    ),
  }) as unknown as Client<Transport, Chain>;
}

let discovered: Pair[] | undefined;

/** Distinct `(market, borrower)` pairs from recent `Borrow` events; discovered once per session. */
export async function discover(config: Config, onProgress: (message: string) => void): Promise<Pair[]> {
  if (discovered?.length) return discovered;

  onProgress("discovering (market, borrower) pairs…");
  const client = createClient(config, { n: 0 }, true);

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
};

export async function run(config: Config, onProgress: (message: string) => void): Promise<RunResult> {
  const pairs = await discover(config, onProgress);
  if (!pairs.length) throw new Error("No Borrow events in the last 20 000 blocks — try a different endpoint.");

  // Repeated to reach the requested count: every element is a real position, so the packing and gas
  // figures are real; only the distinctness of the inputs is synthetic.
  const inputs = Array.from({ length: config.elements }, (_, i) => pairs[i % pairs.length]!);

  const events: WideEvent[] = [];
  const logger = createCapturingLogger((event) => events.push(event));
  const counter = { n: 0 };
  const client = createClient(config, counter, false);

  onProgress(`reading ${config.elements.toLocaleString("en-US")} positions…`);
  const started = performance.now();

  const outcome = await withLogging(
    () =>
      readLens(client, {
        ...positionsLens.with(),
        functionName: "positionOf",
        args: inputs,
        batch: batchOptions(config),
      }),
    { logger, service: "playground" },
  );

  return {
    events,
    results: outcome.results.length,
    skipped: outcome.skipped.length,
    elapsedMs: performance.now() - started,
    requests: counter.n,
    distinct: pairs.length,
  };
}
