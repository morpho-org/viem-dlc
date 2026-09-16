/**
 * Installs the Node globals `src/` and the stream polyfill assume: Store values are `Buffer[]`
 * (`src/types.ts:161`), and `readable-stream` under `stream-browserify` schedules through
 * `process.nextTick`.
 *
 * Exported as a function rather than a side-effecting module on purpose — the package declares
 * `"sideEffects": false`, so Rollup drops a bare `import "./globals.js"` and the globals silently
 * never appear.
 */
import { Buffer } from "buffer";
import * as processModule from "process";

// The `process` shim is CJS; which half survives interop depends on the bundler, so take whichever
// one actually carries `nextTick`.
const candidate = processModule as unknown as { default?: { nextTick?: unknown }; nextTick?: unknown };
const processShim = typeof candidate.nextTick === "function" ? candidate : candidate.default;

export function installNodeGlobals(): void {
  const globals = globalThis as unknown as Record<string, unknown>;

  globals.Buffer ??= Buffer;
  globals.process ??= processShim;

  if (typeof (globals.process as { nextTick?: unknown } | undefined)?.nextTick !== "function") {
    throw new Error("process.nextTick shim missing — the Node stream polyfill cannot run");
  }
}
