import { Buffer } from "buffer";
import { brotliCompressSync } from "zlib";

import { describe, expect, it } from "vitest";

import { BrotliLineBlob } from "../../src/internal/brotli-line-blob.js";

async function collectLines(blob: BrotliLineBlob) {
  const lines: string[] = [];
  for await (const line of blob.lines()) {
    lines.push(line);
  }
  return lines;
}

describe("BrotliLineBlob", () => {
  it("reads base64 input and trims a trailing carriage return on the final unterminated line", async () => {
    const compressed = brotliCompressSync(Buffer.from("alpha\r")).toString("base64");
    const blob = new BrotliLineBlob(compressed);

    expect(await collectLines(blob)).toEqual(["alpha"]);
  });

  it("supports rewriting an empty blob from flush output only", async () => {
    const blob = new BrotliLineBlob();

    expect(await collectLines(blob)).toEqual([]);

    await blob.rewriteLines(() => {}, (emit) => {
      emit("tail");
    });

    expect(await collectLines(blob)).toEqual(["tail"]);
  });

  it("treats an empty compressed buffer input as an empty blob", async () => {
    const blob = new BrotliLineBlob(Buffer.alloc(0));

    expect(blob.toBase64()).toBe("");
    expect(await collectLines(blob)).toEqual([]);
    expect(await blob.reduceLines((acc, line) => acc + line.length, 7)).toBe(7);
  });

  it("clears the stored blob when a rewrite emits no replacement lines", async () => {
    const blob = new BrotliLineBlob(brotliCompressSync(Buffer.from("keep\nremove\n")));

    await blob.rewriteLines(() => {});

    expect(await collectLines(blob)).toEqual([]);
    expect(blob.toBase64()).toBe("");
  });

  it("keeps the existing blob unchanged when rewrite is aborted before commit", async () => {
    const blob = new BrotliLineBlob(brotliCompressSync(Buffer.from("keep\nreplace\n")));
    const controller = new AbortController();
    controller.abort();

    await expect(
      blob.rewriteLines((line, emit) => {
        emit(line === "replace" ? "new" : line);
      }, undefined, controller.signal),
    ).rejects.toMatchObject({ name: "AbortError" });

    expect(await collectLines(blob)).toEqual(["keep", "replace"]);
  });
});
