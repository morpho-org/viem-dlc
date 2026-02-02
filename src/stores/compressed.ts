/// <reference types="node" />
import { promisify } from "util";
import { gunzip, gzip } from "zlib";

import type { Store } from "../types.js";

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);

/** A store that transparently compresses/decompresses values with gzip. */
export class CompressedStore implements Store {
  constructor(private readonly store: Store) {}

  async get(key: string) {
    const value = await this.store.get(key);
    if (value === null) return null;

    try {
      const compressed = Buffer.from(value, "base64");
      return (await gunzipAsync(compressed)).toString("utf8");
    } catch (e) {
      console.warn(`[CompressedStore] Failed to decompress value for key "${key}":`, e);
      return null;
    }
  }

  async set(key: string, value: string) {
    const compressed = await gzipAsync(value);
    return this.store.set(key, compressed.toString("base64"));
  }

  async delete(key: string) {
    return this.store.delete(key);
  }
}
