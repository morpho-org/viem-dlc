/// <reference types="node" />
import { promisify } from "util";
import { type BrotliOptions, brotliCompress, brotliDecompress, constants as zlib } from "zlib";

import type { Store } from "../types.js";

const compress = promisify(brotliCompress);
const decompress = promisify(brotliDecompress);
const options: BrotliOptions = {
  params: {
    [zlib.BROTLI_PARAM_QUALITY]: 4,
  },
};

/** A store that transparently compresses/decompresses values with brotli. */
export class CompressedStore implements Store {
  constructor(private readonly store: Store) {}

  async get(key: string) {
    let value = await this.store.get(key);
    if (value === null) return null;

    try {
      let compressed: Buffer | null = Buffer.from(value, "base64");
      value = null; // release memory
      const decompressed = await decompress(compressed);
      compressed = null; // release memory
      return decompressed.toString("utf8");
    } catch (e) {
      console.warn(`[CompressedStore] Failed to decompress value for key "${key}":`, e);
      return null;
    }
  }

  async set(key: string, value: string) {
    const compressed = await compress(value, options);
    return this.store.set(key, compressed.toString("base64"));
  }

  async delete(key: string) {
    return this.store.delete(key);
  }
}
