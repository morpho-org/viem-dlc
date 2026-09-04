import { memoryUsage, resourceUsage } from "process";

import { createBenchClient, runStrategy, STRATEGIES, type Strategy } from "./get-logs.strategies.ts";

export const MARKER = "__GETLOGS_MEMORY__";

export type MemoryMeasurement = {
  strategy: Strategy;
  matchingLogs: number;
  peakRssBytes: number;
  durationMs: number;
};

// `maxRSS` is monotonic and kernel-tracked, so it cannot miss a peak between samples the way a
// timer over `heapUsed` can. The warm pass below runs the same strategy, so the lifetime max
// still reflects the strategy under test. Same metric as test/internal/ndjson-map.memory.worker.ts.
const maxRssBytes = () => resourceUsage().maxRSS * 1024;

const strategy = process.env.GETLOGS_BENCH_STRATEGY as Strategy;
if (!STRATEGIES.includes(strategy)) throw new Error(`Invalid GETLOGS_BENCH_STRATEGY: ${strategy}`);
const rpcUrl = process.env.RPC_URL;
if (!rpcUrl) throw new Error("Set RPC_URL");

const { client, store } = createBenchClient(rpcUrl);

// Warm so that the measured pass is cache-side work only.
await runStrategy(client, strategy);
globalThis.gc?.();

const start = performance.now();
const logs = await runStrategy(client, strategy);
const durationMs = performance.now() - start;
const peakRssBytes = Math.max(memoryUsage().rss, maxRssBytes());

await store.flush();

const measurement: MemoryMeasurement = { strategy, matchingLogs: logs.length, peakRssBytes, durationMs };
console.log(`${MARKER} ${JSON.stringify(measurement)}`);
