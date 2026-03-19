import { memoryUsage, resourceUsage } from "process";

import { BrotliLineBlob, type Codec, type Entry, NdjsonMap } from "../../dist/data/index.js";
import { parse, stringify } from "../../dist/utils/json.js";

const MARKER = "__NDJSON_MEMORY__";

type Operation = "reduce" | "upsert";
type PayloadMode = "repeat" | "pseudo-random";

const codec: Codec<string> = {
  fromJson: (value) => parse<string>(value, "throw"),
  toJson: stringify,
};

function envInt(name: string): number {
  const value = Number.parseInt(process.env[name] ?? "", 10);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Missing or invalid integer env ${name}`);
  }
  return value;
}

function envOperation(): Operation {
  const value = process.env.NDJSON_MEMORY_OPERATION;
  if (value === "reduce" || value === "upsert") return value;
  throw new Error(`Missing or invalid NDJSON_MEMORY_OPERATION: ${value}`);
}

function envPayloadMode(): PayloadMode {
  const value = process.env.NDJSON_MEMORY_PAYLOAD_MODE;
  if (value === "repeat" || value === "pseudo-random") return value;
  throw new Error(`Missing or invalid NDJSON_MEMORY_PAYLOAD_MODE: ${value}`);
}

function makeAsciiPayload(seed: number, length: number): string {
  let state = (seed + 1) >>> 0;
  let out = "";

  while (out.length < length) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    out += state.toString(16).padStart(8, "0");
  }

  return out.slice(0, length);
}

function makePayload(mode: PayloadMode, seed: number, length: number): string {
  if (mode === "repeat") return String.fromCharCode(97 + (seed % 26)).repeat(length);
  return makeAsciiPayload(seed, length);
}

function serializeLine(key: string, value: string): string {
  return `{"key":${JSON.stringify(key)},"value":${stringify(value)}}`;
}

async function compressFixtureLines(lineCount: number, valueChars: number, payloadMode: PayloadMode) {
  let uncompressedBytes = 0;
  const blob = new BrotliLineBlob();

  await blob.rewriteLines(
    () => {},
    (emit) => {
      for (let i = 0; i < lineCount; i += 1) {
        const line = serializeLine(`k${i}`, makePayload(payloadMode, i, valueChars));
        uncompressedBytes += Buffer.byteLength(`${line}\n`);
        emit(line);
      }
    },
  );

  return { compressed: Buffer.from(blob.toBase64(), "base64"), uncompressedBytes };
}

async function buildFixture(lineCount: number, valueChars: number, updateCount: number, payloadMode: PayloadMode) {
  const upserts: Entry<string, string>[] = [];

  for (let i = 0; i < updateCount; i += 1) {
    upserts.push({ key: `k${i}`, value: makePayload(payloadMode, lineCount + i, valueChars) });
  }

  for (let i = 0; i < updateCount; i += 1) {
    upserts.push({ key: `new-${i}`, value: makePayload(payloadMode, lineCount + updateCount + i, valueChars) });
  }

  const { compressed, uncompressedBytes } = await compressFixtureLines(lineCount, valueChars, payloadMode);

  return {
    compressed,
    compressedBytes: compressed.length,
    uncompressedBytes,
    upserts,
  };
}

function forceGc() {
  global.gc?.();
  global.gc?.();
}

function maxRssBytes() {
  return resourceUsage().maxRSS * 1024;
}

function base64DecodedBytes(base64: string): number {
  const padding = base64.endsWith("==") ? 2 : base64.endsWith("=") ? 1 : 0;
  return (base64.length / 4) * 3 - padding;
}

async function main() {
  const operation = envOperation();
  const payloadMode = envPayloadMode();
  const lineCount = envInt("NDJSON_MEMORY_LINE_COUNT");
  const valueChars = envInt("NDJSON_MEMORY_VALUE_CHARS");
  const updateCount = envInt("NDJSON_MEMORY_UPDATE_COUNT");

  const { compressed, compressedBytes, uncompressedBytes, upserts } = await buildFixture(
    lineCount,
    valueChars,
    updateCount,
    payloadMode,
  );
  const map = new NdjsonMap<string, string>(codec, compressed);

  forceGc();

  const baselinePeakBytes = Math.max(memoryUsage().rss, maxRssBytes());
  const baselineHeapUsedBytes = memoryUsage().heapUsed;
  let observedPeakBytes = baselinePeakBytes;

  const samplePeak = () => {
    observedPeakBytes = Math.max(observedPeakBytes, memoryUsage().rss, maxRssBytes());
  };
  const interval = setInterval(samplePeak, 1);
  interval.unref();
  samplePeak();

  try {
    if (operation === "reduce") {
      let seen = 0;
      const total = await map.reduce((acc, record) => {
        seen += 1;
        if ((seen & 0xff) === 0) samplePeak();
        return acc + record.value.length;
      }, 0);
      if (total <= 0) throw new Error("reduce produced an invalid total");
    } else {
      await map.upsert(upserts);
    }
  } finally {
    samplePeak();
    clearInterval(interval);
  }

  forceGc();

  const finalMemory = memoryUsage();
  const peakBytes = Math.max(observedPeakBytes, maxRssBytes());
  const peakDeltaBytes = Math.max(0, peakBytes - baselinePeakBytes);
  const outputCompressedBytes =
    operation === "upsert" ? base64DecodedBytes(map.toBase64()) : compressedBytes;

  console.log(
    `${MARKER}${JSON.stringify({
      operation,
      payloadMode,
      lineCount,
      valueChars,
      updateCount,
      compressedBytes,
      outputCompressedBytes,
      uncompressedBytes,
      baselinePeakBytes,
      baselineHeapUsedBytes,
      peakBytes,
      peakDeltaBytes,
      peakMultiple: peakDeltaBytes / compressedBytes,
      finalRssBytes: finalMemory.rss,
      finalHeapUsedBytes: finalMemory.heapUsed,
      finalExternalBytes: finalMemory.external,
      finalArrayBuffersBytes: finalMemory.arrayBuffers,
    })}`,
  );
}

await main();
