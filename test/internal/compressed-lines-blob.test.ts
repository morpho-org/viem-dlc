import { Buffer } from "buffer";
import { zstdCompressSync } from "zlib";

import { describe, expect, it } from "vitest";

import { CompressedLinesBlob, createSlot } from "../../src/internal/compressed-lines-blob.js";

async function collectLines(blob: CompressedLinesBlob) {
  const lines: string[] = [];
  for await (const line of blob.lines()) {
    lines.push(line);
  }
  return lines;
}

describe("CompressedLinesBlob", () => {
  it("reads input and trims a trailing carriage return on the final unterminated line", async () => {
    const compressed = zstdCompressSync(Buffer.from("alpha\r"));
    const blob = new CompressedLinesBlob(createSlot(compressed));

    expect(await collectLines(blob)).toEqual(["alpha"]);
  });

  it("treats an empty compressed buffer input as an empty blob", async () => {
    const slot = createSlot(Buffer.alloc(0));
    const blob = new CompressedLinesBlob(slot);

    expect(slot.get()).toEqual([]);
    expect(await collectLines(blob)).toEqual([]);
  });

  describe("rewrite", () => {
    it("rewrites an empty blob and round-trips correctly", async () => {
      const blob = new CompressedLinesBlob(createSlot());

      await blob.rewrite(
        () => {},
        (emit) => {
          emit("alpha");
          emit("beta");
          emit("gamma");
        },
      );
      expect(await collectLines(blob)).toEqual(["alpha", "beta", "gamma"]);
    });

    it("streams existing lines and allows inline rewrites", async () => {
      const blob = new CompressedLinesBlob(createSlot(zstdCompressSync(Buffer.from("alpha\nbeta\ngamma\n"))));
      const seen: string[] = [];

      await blob.rewrite(
        (line, emit) => {
          seen.push(line);
          if (line !== "beta") emit(line);
        },
        (emit) => {
          emit("tail");
        },
      );

      expect(seen).toEqual(["alpha", "beta", "gamma"]);
      expect(await collectLines(blob)).toEqual(["alpha", "gamma", "tail"]);
    });

    it("clears the slot when no lines are emitted", async () => {
      const slot = createSlot(zstdCompressSync(Buffer.from("existing\n")));
      const blob = new CompressedLinesBlob(slot);

      await blob.rewrite(() => {});
      expect(await collectLines(blob)).toEqual([]);
      expect(slot.get()).toEqual([]);
    });

    it("keeps the blob unchanged when aborted", async () => {
      const blob = new CompressedLinesBlob(createSlot(zstdCompressSync(Buffer.from("keep\n"))));
      const controller = new AbortController();
      controller.abort();

      await expect(
        blob.rewrite(
          (_line, emit) => {
            emit("replaced");
          },
          undefined,
          controller.signal,
        ),
      ).rejects.toMatchObject({
        name: "AbortError",
      });

      expect(await collectLines(blob)).toEqual(["keep"]);
    });

    it("keeps the blob unchanged when rewrite throws after emitting partial output", async () => {
      const blob = new CompressedLinesBlob(createSlot(zstdCompressSync(Buffer.from("keep\n"))));

      await expect(
        blob.rewrite((_line, emit) => {
          emit("replaced");
          throw new Error("boom");
        }),
      ).rejects.toThrow("boom");

      expect(await collectLines(blob)).toEqual(["keep"]);
    });
  });
});
