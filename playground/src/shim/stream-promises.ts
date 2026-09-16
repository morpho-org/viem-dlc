/**
 * Browser stand-in for `node:stream/promises`, aliased in at build time.
 *
 * `node-stdlib-browser` maps `stream` but exposes no `stream/promises` subpath, and
 * `src/internal/compressed-lines-blob.ts` drives its whole read and rewrite path through
 * `pipeline(..., { signal })`.
 */
import { pipeline as callbackPipeline } from "stream";

type Streamish = {
  on?: (event: string, listener: (error: Error) => void) => void;
  destroy?: (error?: Error) => void;
  destroyed?: boolean;
};

export function pipeline(...args: unknown[]): Promise<void> {
  const last = args[args.length - 1];
  const hasOptions = typeof last === "object" && last !== null && !("pipe" in last) && !("then" in last);
  const streams = (hasOptions ? args.slice(0, -1) : args) as Streamish[];
  const signal = hasOptions ? (last as { signal?: AbortSignal }).signal : undefined;
  const tail = streams[streams.length - 1];

  // Node destroys the downstream with the error that actually occurred; readable-stream reports
  // `ERR_STREAM_PREMATURE_CLOSE` to the consumer instead and keeps the original for the callback.
  // `compressed-lines-blob.ts` reads the code off what reaches its `for await`, so carry it across.
  let original: Error | undefined;
  for (const stream of streams) {
    stream.on?.("error", (error) => {
      original ??= error;
      if (stream !== tail && !tail?.destroyed) tail?.destroy?.(error);
    });
  }

  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);

    const composed = (callbackPipeline as unknown as (...a: unknown[]) => Streamish)(
      ...streams,
      (error: Error | null) => {
        signal?.removeEventListener("abort", onAbort);
        if (error) reject(original ?? error);
        else resolve();
      },
    );

    function onAbort() {
      composed.destroy?.(signal?.reason instanceof Error ? signal.reason : new Error("aborted"));
    }

    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
