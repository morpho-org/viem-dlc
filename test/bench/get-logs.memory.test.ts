import { spawnSync } from "child_process";
import { fileURLToPath } from "url";

import { describe, expect, it } from "vitest";

import type { MemoryMeasurement } from "./get-logs.memory.worker.ts";
import { STRATEGIES, type Strategy } from "./get-logs.strategies.ts";

const MARKER = "__GETLOGS_MEMORY__";
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));

function measure(strategy: Strategy): MemoryMeasurement {
  const child = spawnSync(
    process.execPath,
    ["--expose-gc", "--experimental-transform-types", "./test/bench/get-logs.memory.worker.ts"],
    { cwd: repoRoot, encoding: "utf8", env: { ...process.env, GETLOGS_BENCH_STRATEGY: strategy } },
  );
  const line = `${child.stdout}\n${child.stderr}`
    .split("\n")
    .map((l) => l.trim())
    .find((l) => l.startsWith(MARKER));
  if (child.status !== 0 || !line) {
    throw new Error(`worker failed for ${strategy}:\n${child.stdout}\n${child.stderr}`);
  }
  return JSON.parse(line.slice(MARKER.length));
}

/**
 * Peak-heap comparison of the `getLogs2` narrowing strategies. Opt in with `BENCH=1` and
 * `RPC_URL` — the first run fills `fixtures/` from the network.
 */
describe.skipIf(!process.env.BENCH || !process.env.RPC_URL)("getLogs2 strategies (peak heap)", () => {
  it("reduce and search+reduce bound peak RSS below fetch-all, with identical results", () => {
    const [fetchAll, reduce, search] = STRATEGIES.map(measure) as [
      MemoryMeasurement,
      MemoryMeasurement,
      MemoryMeasurement,
    ];

    for (const m of [fetchAll, reduce, search]) {
      console.log(
        `${m.strategy.padEnd(22)} peak ${(m.peakRssBytes / 1024 / 1024).toFixed(1).padStart(7)} MB ` +
          `${m.durationMs.toFixed(0).padStart(6)} ms  ${m.matchingLogs} logs`,
      );
    }

    expect(reduce.matchingLogs).toBe(fetchAll.matchingLogs);
    expect(search.matchingLogs).toBe(fetchAll.matchingLogs);
    expect(reduce.peakRssBytes).toBeLessThanOrEqual(fetchAll.peakRssBytes);
    expect(search.peakRssBytes).toBeLessThanOrEqual(reduce.peakRssBytes);
  }, 600_000);
});
