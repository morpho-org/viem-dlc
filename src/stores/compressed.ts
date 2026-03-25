/// <reference types="node" />
import { promisify } from "util";
import { type BrotliOptions, brotliCompress, brotliDecompress, constants as zlib } from "zlib";

import type { Store } from "../types.js";
import { createInFlightBarrier } from "../utils/in-flight.js";

const compress = promisify(brotliCompress);
const decompress = promisify(brotliDecompress);
const options: BrotliOptions = {
  params: {
    [zlib.BROTLI_PARAM_QUALITY]: 4,
  },
};

/**
 * A store that transparently compresses/decompresses values with brotli.
 *
 * @deprecated Compression is now handled outside the `Store` stack, so this would be
 * compressing already-compressed data.
 */
export class CompressedStore implements Store {
  private readonly inFlight = createInFlightBarrier();

  constructor(private readonly store: Store) {}

  async get(key: string) {
    const compressed = await this.store.get(key);
    if (compressed === null) return null;

    try {
      return [await decompress(Buffer.concat(compressed))];
    } catch (e) {
      console.warn(`[CompressedStore] Failed to decompress value for key "${key}":`, e);
      return null;
    }
  }

  set(key: string, value: Buffer[]) {
    return this.inFlight.track(
      (async () => {
        try {
          const compressed = await compress(Buffer.concat(value), options);
          return this.store.set(key, [compressed]);
        } catch (e) {
          console.warn(`[CompressedStore] Failed to compress value for key "${key}":`, e);
        }
      })(),
    );
  }

  delete(key: string) {
    return this.inFlight.track(Promise.resolve(this.store.delete(key)));
  }

  async flush() {
    await this.inFlight.flush();
    await this.store.flush();
  }
}
