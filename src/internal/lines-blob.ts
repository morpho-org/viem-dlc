/// <reference types="node" />

export type Slot = {
  get(): Buffer[];
  set(value: Buffer[]): void;
};

export type EmitLine = (line: string) => void;

/**
 * Common surface implemented by both {@link LinesBlobCompressed} and
 * {@link LinesBlobRaw}. `NdjsonMap` is parameterized over this interface so
 * the underlying line storage can be swapped without changing merge/sort logic.
 *
 * `lines()` streams logical lines (without trailing newline characters).
 *
 * `rewrite()` is transactional: the slot is swapped on success, left unchanged
 * on abort or error. `onLine` is called for each existing line and may call
 * `emit` zero or more times. `onFlush` is called after all existing lines have
 * been consumed and may emit trailing lines. `emit` must be called synchronously
 * before `onLine`/`onFlush` returns.
 */
export interface LinesBlob {
  lines(): AsyncGenerator<string, void, void>;
  rewrite(
    onLine: (line: string, emit: EmitLine) => void,
    onFlush?: (emit: EmitLine) => void,
    signal?: AbortSignal,
  ): Promise<void>;
}

export function createSlot(compressed?: Buffer | Buffer[]): Slot {
  let chunks: Buffer[] = [];

  if (compressed) {
    if (Array.isArray(compressed)) {
      chunks = compressed;
    } else if (compressed.length > 0) {
      chunks = [compressed];
    }
  }

  return {
    get: () => chunks,
    set: (v) => {
      chunks = v;
    },
  };
}
