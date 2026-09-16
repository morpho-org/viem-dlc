/**
 * `stream-browserify`, plus the one thing it refuses to provide.
 *
 * Its `Readable.from` throws "not available in the browser", and
 * `src/internal/compressed-lines-blob.ts` opens both its read and rewrite pipelines with it. This
 * reinstates it with Node's semantics: object mode, pulling one value per `_read` so backpressure
 * still governs how fast the source is consumed.
 *
 * Members are re-exported explicitly rather than with `export *` — `stream-browserify` is CJS, and
 * under vite-node's interop a star re-export yields `undefined` bindings.
 */
import browserify from "stream-browserify";

type AnyIterable<T> = Iterable<T> | AsyncIterable<T>;

const streams = browserify as unknown as {
  Readable: { from?: unknown; new (opts?: unknown): unknown };
  Writable: unknown;
  Duplex: unknown;
  Transform: unknown;
  PassThrough: unknown;
  pipeline: unknown;
  finished: unknown;
  Stream: unknown;
};

streams.Readable.from = <T>(iterable: AnyIterable<T>) => {
  const source = iterable as Iterable<T> & AsyncIterable<T>;
  const iterator = source[Symbol.asyncIterator]?.() ?? source[Symbol.iterator]();

  return new streams.Readable({
    objectMode: true,
    async read(this: { push(value: unknown): void; destroy(error: unknown): void }) {
      try {
        const { done, value } = await iterator.next();
        this.push(done ? null : value);
      } catch (error) {
        this.destroy(error);
      }
    },
  });
};

export const { Readable, Writable, Duplex, Transform, PassThrough, pipeline, finished, Stream } =
  streams as unknown as typeof import("node:stream");

export default browserify;
