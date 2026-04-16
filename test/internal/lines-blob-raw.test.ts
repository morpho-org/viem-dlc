import { Buffer } from "buffer";

import { describe, expect, it } from "vitest";

import { createSlot } from "../../src/internal/lines-blob.js";
import { LinesBlobRaw } from "../../src/internal/lines-blob-raw.js";

async function collectLines(blob: LinesBlobRaw) {
  const lines: string[] = [];
  for await (const line of blob.lines()) {
    lines.push(line);
  }
  return lines;
}

describe("LinesBlobRaw", () => {
  it("yields nothing for an empty slot", async () => {
    const blob = new LinesBlobRaw(createSlot());
    expect(await collectLines(blob)).toEqual([]);
  });

  it("reads input and trims a trailing carriage return on the final unterminated line", async () => {
    const blob = new LinesBlobRaw(createSlot(Buffer.from("alpha\r")));
    expect(await collectLines(blob)).toEqual(["alpha"]);
  });

  it("treats an empty buffer input as an empty blob", async () => {
    const slot = createSlot(Buffer.alloc(0));
    const blob = new LinesBlobRaw(slot);

    expect(slot.get()).toEqual([]);
    expect(await collectLines(blob)).toEqual([]);
  });

  it("yields multiple lines split on \\n and \\r\\n", async () => {
    const blob = new LinesBlobRaw(createSlot(Buffer.from("alpha\nbeta\r\ngamma\n")));
    expect(await collectLines(blob)).toEqual(["alpha", "beta", "gamma"]);
  });

  describe("rewrite", () => {
    it("rewrites an empty blob and round-trips correctly", async () => {
      const blob = new LinesBlobRaw(createSlot());

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
      const blob = new LinesBlobRaw(createSlot(Buffer.from("alpha\nbeta\ngamma\n")));
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
      const slot = createSlot(Buffer.from("existing\n"));
      const blob = new LinesBlobRaw(slot);

      await blob.rewrite(() => {});
      expect(await collectLines(blob)).toEqual([]);
      expect(slot.get()).toEqual([]);
    });

    it("keeps the blob unchanged when aborted", async () => {
      const blob = new LinesBlobRaw(createSlot(Buffer.from("keep\n")));
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
      const blob = new LinesBlobRaw(createSlot(Buffer.from("keep\n")));

      await expect(
        blob.rewrite((_line, emit) => {
          emit("replaced");
          throw new Error("boom");
        }),
      ).rejects.toThrow("boom");

      expect(await collectLines(blob)).toEqual(["keep"]);
    });

    it("writes raw NDJSON bytes (no zstd magic) to the slot", async () => {
      const slot = createSlot();
      const blob = new LinesBlobRaw(slot);

      await blob.rewrite(
        () => {},
        (emit) => {
          emit('{"key":"a","value":1}');
          emit('{"key":"b","value":2}');
        },
      );

      const concatenated = Buffer.concat(slot.get()).toString("utf8");
      expect(concatenated).toBe('{"key":"a","value":1}\n{"key":"b","value":2}\n');
    });
  });
});
