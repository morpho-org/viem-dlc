import { InlineContract } from "soltag";

/** Matches the soltag plugin's `runs`, so browser output is byte-identical to the build's. */
const OPTIMIZER_RUNS = 200;

let worker: Worker | undefined;
let nextId = 0;
const pending = new Map<number, { resolve: (artifacts: never) => void; reject: (error: Error) => void }>();

/**
 * Compiles `source` and wraps the named contract in soltag's own {@link InlineContract}, so the
 * result is the same shape `sol()` produces at build time — `.with()` included.
 *
 * The 8.9 MB compiler is fetched on the first call and never before it, so a visitor who does not
 * edit Solidity never downloads it.
 */
export async function compileLens<name extends string>(name: name, source: string): Promise<InlineContract<name>> {
  if (!worker) {
    worker = new Worker(new URL("./solc.worker.ts", import.meta.url));
    // Without this a failed `importScripts` would leave every caller awaiting forever.
    worker.onerror = (event) => {
      const reason = new Error(`Compiler worker failed to start: ${event.message || "unknown error"}`);
      for (const [id, entry] of pending) {
        pending.delete(id);
        entry.reject(reason);
      }
    };

    worker.onmessage = ({ data }: MessageEvent<{ id: number; artifacts?: never; error?: string }>) => {
      const entry = pending.get(data.id);
      if (!entry) return;
      pending.delete(data.id);
      if (data.error) entry.reject(new Error(data.error));
      else entry.resolve(data.artifacts as never);
    };
  }

  const id = nextId++;
  const artifacts = await new Promise<never>((resolve, reject) => {
    pending.set(id, { resolve, reject });
    worker!.postMessage({ id, soljsonUrl: `${import.meta.env.BASE_URL}soljson.js`, source, runs: OPTIMIZER_RUNS });
  });

  return new InlineContract(name, artifacts);
}
