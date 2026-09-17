/**
 * Proves the claim `src/shim/zlib-gzip.ts` rests on: substituting the codec does not cost the
 * blob pipeline its constant-memory property.
 *
 * Run both sides and compare — the playground config aliases `zlib` to the shim, the node config
 * does not, so the same code runs against gzip-over-CompressionStream and real Node zstd:
 *
 *   NODE_OPTIONS=--expose-gc LABEL="node zstd" pnpm exec vite-node -c vite.node.config.ts playground/scripts/codec-memory.ts
 *   NODE_OPTIONS=--expose-gc LABEL="gzip shim" pnpm exec vite-node -c playground/vite.config.ts playground/scripts/codec-memory.ts
 *
 * Measured 2026-09-15: 0.2 MB and 0.3 MB peak growth respectively, against 52.2 MB of data.
 */
import { CompressedLinesBlob, createSlot } from "../../src/internal/compressed-lines-blob.js";

const LINES = 200_000;
const PAYLOAD = "x".repeat(240);
const line = (i: number) => JSON.stringify({ i, payload: PAYLOAD });
const decompressed = LINES * (line(0).length + 1);

const slot = createSlot();
await new CompressedLinesBlob(slot).rewrite(
  () => {},
  (emit: (line: string) => void) => {
    for (let i = 0; i < LINES; i++) emit(line(i));
  },
);

const gc = globalThis.gc;
if (!gc) throw new Error("run with --expose-gc");

gc();
const before = process.memoryUsage().heapUsed;
let peak = before;
let seen = 0;

await new CompressedLinesBlob(createSlot(slot.get())).rewrite((l: string, emit: (line: string) => void) => {
  seen += 1;
  if (seen % 8192 === 0) {
    gc();
    const used = process.memoryUsage().heapUsed;
    if (used > peak) peak = used;
  }
  emit(l);
});

console.log(
  `${process.env.LABEL}: peak growth ${((peak - before) / 1e6).toFixed(1)} MB  |  data ${(decompressed / 1e6).toFixed(1)} MB  |  compressed ${(slot.get().reduce((n: number, c: Buffer) => n + c.length, 0) / 1e6).toFixed(2)} MB`,
);
