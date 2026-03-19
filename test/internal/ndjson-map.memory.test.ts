import { spawnSync } from "child_process";

import { describe, expect, it } from "vitest";

type Operation = "reduce" | "upsert";
type PayloadMode = "repeat" | "pseudo-random";

type MemoryMeasurement = {
  operation: Operation;
  payloadMode: PayloadMode;
  compressedBytes: number;
  outputCompressedBytes: number;
  uncompressedBytes: number;
  peakDeltaBytes: number;
  peakMultiple: number;
  lineCount: number;
  valueChars: number;
  updateCount: number;
};

type MeasurementConfig = {
  operation: Operation;
  payloadMode: PayloadMode;
  lineCount: number;
  valueChars: number;
  updateCount: number;
};

const MARKER = "__NDJSON_MEMORY__";

function totalCompressedFootprintMultiple(peakDeltaBytes: number, compressedBytes: number) {
  return 1 + peakDeltaBytes / compressedBytes;
}

function measureNdjsonPeakMemory(config: MeasurementConfig): MemoryMeasurement {
  const child = spawnSync(
    process.execPath,
    ["--expose-gc", "--experimental-transform-types", "./test/data/ndjson-map.memory.worker.ts"],
    {
      cwd: "/Users/haydenshively/Developer/viem-dlc",
      encoding: "utf8",
      env: {
        ...process.env,
        NDJSON_MEMORY_OPERATION: config.operation,
        NDJSON_MEMORY_PAYLOAD_MODE: config.payloadMode,
        NDJSON_MEMORY_LINE_COUNT: String(config.lineCount),
        NDJSON_MEMORY_VALUE_CHARS: String(config.valueChars),
        NDJSON_MEMORY_UPDATE_COUNT: String(config.updateCount),
      },
    },
  );

  const output = `${child.stdout}\n${child.stderr}`;
  const markerLine = output
    .split("\n")
    .map((line) => line.trim())
    .find((line) => line.startsWith(MARKER));

  if (child.status !== 0 || markerLine === undefined) {
    throw new Error(
      `memory worker failed for ${config.operation}\nstatus=${child.status}\nstdout:\n${child.stdout}\nstderr:\n${child.stderr}`,
    );
  }

  return JSON.parse(markerLine.slice(MARKER.length)) as MemoryMeasurement;
}

describe("NdjsonMap memory", () => {
  it("reduce peak RSS stays near compressed input size", { timeout: 30_000 }, () => {
    const result = measureNdjsonPeakMemory({
      operation: "reduce",
      payloadMode: "pseudo-random",
      lineCount: 100_000,
      valueChars: 256,
      updateCount: 2_500,
    });

    expect(result.compressedBytes).toBeGreaterThan(10_000_000);
    expect(totalCompressedFootprintMultiple(result.peakDeltaBytes, result.compressedBytes)).toBeLessThan(2.5);
  });

  it("upsert peak RSS stays within a small multiple of compressed output size", { timeout: 30_000 }, () => {
    const result = measureNdjsonPeakMemory({
      operation: "upsert",
      payloadMode: "pseudo-random",
      lineCount: 100_000,
      valueChars: 256,
      updateCount: 12_500,
    });

    expect(result.outputCompressedBytes).toBeGreaterThan(10_000_000);
    expect(totalCompressedFootprintMultiple(result.peakDeltaBytes, result.outputCompressedBytes)).toBeLessThan(3);
  });
});
