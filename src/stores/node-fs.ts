/// <reference types="node" />
import { createHash } from "crypto";
import { mkdir, readFile, rename, unlink, writeFile } from "fs/promises";
import { join } from "path";

import type { Store } from "../types.js";
import { createInFlightBarrier } from "../utils/in-flight.js";

export type NodeFsStoreOptions = {
  /** Root directory for store files. Created recursively on first write if absent. */
  directory: string;
  /**
   * Optional sub-path appended to `directory` before writing, used to
   * namespace one physical directory across multiple consumers.
   * Leading/trailing slashes are stripped.
   */
  prefix?: string;
};

/**
 * A store backed by the local filesystem using Node.js `fs/promises`.
 * Compatible with both Node.js and Bun runtimes.
 *
 * - Keys are SHA256-hashed to produce safe, fixed-length filenames.
 * - Writes are atomic: data lands in a `.tmp` file first, then renamed into place.
 * - Best-effort: methods catch errors and warn rather than throw, per the `Store` contract.
 */
export class NodeFsStore implements Store {
  private readonly dir: string;
  private readonly inFlight = createInFlightBarrier();
  private dirEnsured = false;

  constructor(options: NodeFsStoreOptions) {
    const prefix = (options.prefix ?? "").replace(/^\/+|\/+$/g, "");
    this.dir = prefix ? join(options.directory, prefix) : options.directory;
  }

  private encodeKey(key: string): string {
    return createHash("sha256").update(key).digest("hex");
  }

  private resolvePath(key: string): string {
    return join(this.dir, this.encodeKey(key));
  }

  private async ensureDir(): Promise<void> {
    if (this.dirEnsured) return;
    await mkdir(this.dir, { recursive: true });
    this.dirEnsured = true;
  }

  async get(key: string): Promise<Buffer[] | null> {
    try {
      return [await readFile(this.resolvePath(key))];
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
      console.warn(`[NodeFsStore] Failed to get key "${key}":`, err);
      return null;
    }
  }

  set(key: string, value: Buffer[]): Promise<void> {
    return this.inFlight.track(this._set(key, value));
  }

  private async _set(key: string, value: Buffer[]): Promise<void> {
    try {
      await this.ensureDir();
      const path = this.resolvePath(key);
      await writeFile(`${path}.tmp`, Buffer.concat(value));
      await rename(`${path}.tmp`, path);
    } catch (err) {
      console.warn(`[NodeFsStore] Failed to set key "${key}":`, err);
    }
  }

  async delete(key: string): Promise<void> {
    try {
      await this.inFlight.track(unlink(this.resolvePath(key)));
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
      console.warn(`[NodeFsStore] Failed to delete key "${key}":`, err);
    }
  }

  async flush(): Promise<void> {
    try {
      await this.inFlight.flush();
    } catch (err) {
      console.warn("[NodeFsStore] Failed to flush:", err);
    }
  }
}
