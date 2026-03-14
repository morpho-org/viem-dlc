/// <reference types="node" />
import { promisify } from "util";
import { gunzip, gzip } from "zlib";

import type { Store } from "../types.js";
import { createInFlightBarrier } from "../utils/in-flight.js";

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);

/** A store that transparently compresses/decompresses values with gzip. */
export class CompressedStore implements Store {
  private readonly inFlight = createInFlightBarrier();

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

  set(key: string, value: string) {
    return this.inFlight.track(
      (async () => {
        const compressed = await gzipAsync(value);
        return this.store.set(key, compressed.toString("base64"));
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
