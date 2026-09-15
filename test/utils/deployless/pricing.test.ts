import type { Hex } from "viem";
import { describe, expect, it } from "vitest";

import { FACTORY_BYTECODE_REVERT } from "../../../src/utils/deployless/codec.envelope.js";
import { copyGas, floorGas, intrinsicGas, sentSize, wireSize } from "../../../src/utils/deployless/pricing.js";

/** Ethereum's memory expansion, written out here so the schedule the prologue is priced on is pinned. */
function memcost(bytes: number): number {
  const words = Math.ceil(bytes / 32);
  return 3 * words + Math.floor((words * words) / 512);
}

/** `bytes` bytes of `fill`, repeated. */
const filled = (bytes: number, fill = "ab") => `0x${fill.repeat(bytes)}` as Hex;

/** Six bytes, four of them zero. */
const MIXED = "0x00ff00ff0000" as const;

describe("wireSize", () => {
  it("counts a byte string's length and its zero bytes", () => {
    expect(wireSize(MIXED)).toEqual({ bytes: 6, zeros: 4 });
    expect(wireSize("0x")).toEqual({ bytes: 0, zeros: 0 });
  });
});

describe("sentSize", () => {
  it("puts the envelope's own bytes on the wire only as initcode", () => {
    const args = wireSize(MIXED);
    const envelope = wireSize(FACTORY_BYTECODE_REVERT);

    expect(sentSize(args, "override")).toEqual(args);
    expect(sentSize(args, "initcode")).toEqual({
      bytes: args.bytes + envelope.bytes,
      zeros: args.zeros + envelope.zeros,
    });
  });
});

describe("intrinsicGas", () => {
  it("charges the transaction base, the calldata's bytes and the sampling opcode", () => {
    expect(intrinsicGas(wireSize(MIXED), "override")).toBe(21_000 + 4 * 4 + 16 * 2 + 2);
  });

  it.each([0, 1, 31, 32, 33, 2_626, 100_000])(
    "omits exactly the creation base and the initcode words of %i bytes by override",
    (bytes) => {
      const size = wireSize(filled(bytes));

      expect(intrinsicGas(size, "initcode") - intrinsicGas(size, "override")).toBe(32_000 + 2 * Math.ceil(bytes / 32));
    },
  );
});

describe("floorGas", () => {
  it("prices EIP-7623's floor at ten per zero byte and forty per non-zero", () => {
    expect(floorGas(wireSize(MIXED))).toBe(21_000 + 10 * 4 + 40 * 2);
  });

  it("outweighs intrinsic gas on a non-zero-heavy chunk, which is why it is the binding line", () => {
    const size = wireSize(filled(100_000));

    expect(floorGas(size)).toBeGreaterThan(intrinsicGas(size, "initcode"));
  });
});

describe("copyGas", () => {
  it.each([
    ["clear", false, 0],
    ["compressed", true, 16_704],
  ])("expands memory to one word past a %s frame's slab", (_name, compressed, history) => {
    const argsBytes = 10_000;
    const argsEnd = 0x80 + argsBytes;
    const slab = Math.ceil(argsEnd / 32) * 32 + 0x1c0 + history;

    expect(copyGas(argsBytes, compressed)).toBe(3 * Math.ceil(argsBytes / 32) + memcost(slab + 0x20));
  });

  it("costs the decompressor's history buffer only on the compressed path", () => {
    expect(copyGas(10_000, true) - copyGas(10_000, false)).toBeGreaterThan(2_000);
  });

  it("grows quadratically, so a multi-megabyte tuple costs millions", () => {
    const mib = copyGas(1 << 20, false);

    expect(mib).toBeGreaterThan(2_000_000);
    // Doubling the tuple more than triples the term: the memory square dominates the linear copy.
    expect(copyGas(2 << 20, false) / mib).toBeGreaterThan(3.5);
  });

  it("is negligible beside the copy itself at the initcode cap", () => {
    expect(copyGas(49_152, false)).toBeLessThan(20_000);
  });
});
