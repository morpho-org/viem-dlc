import type { Hex } from "viem";
import { type AbiFunction, encodeAbiParameters, parseAbiItem, parseAbiParameters, toFunctionSelector } from "viem";
import { describe, expect, it } from "vitest";

import { envelopeConfig } from "../../../src/utils/deployless/codec.envelope.js";
import {
  hexToPage,
  itemFragmentOf,
  type Page,
  pageToHex,
  paginatedAbi,
  resolveArrayFunction,
} from "../../../src/utils/deployless/codec.inner.js";

const DYNAMIC = { mode: "dynamic" } as const;
const STATIC = { mode: "static", size: 32 } as const;

/** The wire form of a gas death at `index`: the 256-bit complement, `~index`. */
const tag = (index: number | bigint) => ((1n << 256n) - 1n) ^ BigInt(index);

/** Encodes `(U[] results, uint256[] skipped)` the way a real lens would, via viem. */
function encodePage(types: string, results: readonly unknown[], skipped: readonly (number | bigint)[]): Hex {
  return encodeAbiParameters(parseAbiParameters(`${types}, uint256[]`), [results, skipped.map(BigInt)] as never);
}

describe("resolveArrayFunction", () => {
  const paginated = parseAbiItem(
    "function page(address[] input) view returns (uint256[] results, uint256[] skipped)",
  ) as AbiFunction;

  it("resolves a paginated fragment and reports both element layouts", () => {
    const resolved = resolveArrayFunction(paginated);
    expect(resolved.inputLayout).toEqual(STATIC);
    expect(resolved.outputLayout).toEqual(STATIC);
    expect(resolved.inputBytes).toBe(32);
    expect(resolved.outputBytes).toBe(32);
  });

  it("rejects a fragment whose second output is not uint256[]", () => {
    const wrong = parseAbiItem("function page(address[] a) view returns (uint256[] r, address[] s)") as AbiFunction;
    expect(() => resolveArrayFunction(wrong)).toThrow(/paginated output 1 must be uint256\[\]/);
  });

  it("rejects a fragment with a single output", () => {
    const single = parseAbiItem("function page(address[] a) view returns (uint256[])") as AbiFunction;
    expect(() => resolveArrayFunction(single)).toThrow(/must return \(U\[\] results, uint256\[\] skipped\)/);
  });

  it("rejects a fragment whose input is not a dynamic array", () => {
    const scalar = parseAbiItem("function page(address a) view returns (uint256[] r, uint256[] s)") as AbiFunction;
    expect(() => resolveArrayFunction(scalar)).toThrow(/expected exactly one dynamic-array input/);
  });

  describe("dynamic element bounds", () => {
    const dynamicInput = parseAbiItem(
      "function page(string[] input) view returns (uint256[] results, uint256[] skipped)",
    ) as AbiFunction;
    const dynamicResult = parseAbiItem(
      "function page(address[] input) view returns (string[] results, uint256[] skipped)",
    ) as AbiFunction;

    it("requires maxItemBytes when the input element is dynamic", () => {
      expect(() => resolveArrayFunction(dynamicInput)).toThrow(
        /the input element is dynamic, so policy\.maxItemBytes \(a positive multiple of 32 bytes\) is required/,
      );
    });

    it("requires maxResultBytes when the result element is dynamic", () => {
      expect(() => resolveArrayFunction(dynamicResult)).toThrow(
        /the result element is dynamic, so policy\.maxResultBytes \(a positive multiple of 32 bytes\) is required/,
      );
    });

    it.each([[0], [31], [48], [-32]])("rejects a bound of %i bytes", (maxItemBytes) => {
      expect(() => resolveArrayFunction(dynamicInput, { maxItemBytes })).toThrow(/policy\.maxItemBytes/);
    });

    it("sizes a dynamic element at its bound plus an offset word", () => {
      const resolved = resolveArrayFunction(dynamicInput, { maxItemBytes: 96 });
      expect(resolved.maxItemBytes).toBe(96);
      expect(resolved.inputBytes).toBe(128);
      expect(resolved.maxResultBytes).toBeUndefined();
      expect(resolved.outputBytes).toBe(32);
    });
  });
});

describe("paginatedAbi", () => {
  const item = (signature: string) => parseAbiItem(signature) as AbiFunction;

  it.each([
    [
      "a static struct in and out",
      "function healthOf((address user, uint256 shares) position) view returns ((uint256 ltv, bool healthy) health)",
      "tuple[]",
      "tuple[]",
    ],
    ["bytes in, string out", "function describe(bytes blob) view returns (string text)", "bytes[]", "string[]"],
    ["an array output", "function pathOf(address market) view returns (uint256[] hops)", "address[]", "uint256[][]"],
    [
      "a fixed-array input",
      "function rootOf(bytes32[3] leaves) view returns (uint256 root)",
      "bytes32[3][]",
      "uint256[]",
    ],
  ])("derives the array-shaped fragment for %s", (_name, signature, inputType, outputType) => {
    const f = item(signature);
    const paginated = paginatedAbi(f);

    expect(paginated.inputs).toEqual([{ ...f.inputs[0], type: inputType }]);
    expect(paginated.outputs).toEqual([
      { ...f.outputs[0], name: "results", type: outputType },
      { name: "skipped", type: "uint256[]" },
    ]);
    // Everything outside the two parameter lists is the lens's own fragment, untouched.
    expect({ ...paginated, inputs: undefined, outputs: undefined }).toEqual({
      ...f,
      inputs: undefined,
      outputs: undefined,
    });
  });

  it.each([
    [
      "a static struct in and out",
      "function healthOf((address user, uint256 shares) position) view returns ((uint256 ltv, bool healthy) health)",
    ],
    ["bytes in, string out", "function describe(bytes blob) view returns (string text)"],
    ["an array output", "function pathOf(address market) view returns (uint256[] hops)"],
    ["a fixed-array input", "function rootOf(bytes32[3] leaves) view returns (uint256 root)"],
  ])("round-trips through itemFragmentOf for %s", (_name, signature) => {
    const f = item(signature);
    const back = itemFragmentOf(paginatedAbi(f) as AbiFunction);

    // `paginatedAbi` renames the sole output to `results` so viem decodes the page tuple by name;
    // that rename is the only thing the round trip does not restore.
    expect(back).toEqual({ ...f, outputs: [{ ...f.outputs[0], name: "results" }] });
    expect(toFunctionSelector(back)).toBe(toFunctionSelector(f));
  });

  it("rejects a function taking two parameters", () => {
    expect(() => paginatedAbi(item("function pairOf(address a, address b) view returns (uint256 r)"))).toThrow(
      /pairOf must take exactly one parameter/,
    );
  });

  it("rejects a function returning nothing", () => {
    expect(() => paginatedAbi(item("function poke(address a) view"))).toThrow(/poke must return exactly one value/);
  });

  it("rejects a function returning two values", () => {
    expect(() => paginatedAbi(item("function bothOf(address a) view returns (uint256 r, uint256 s)"))).toThrow(
      /bothOf must return exactly one value/,
    );
  });

  it("resolves to the per-item selector the envelope has to call", () => {
    const f = item("function healthOf(address user) view returns (uint256 health)");
    expect(resolveArrayFunction(paginatedAbi(f) as AbiFunction).itemSelector).toBe(toFunctionSelector(f));
  });
});

describe("envelopeConfig", () => {
  it("packs a static lens as selector, no dynamic bits, and both strides", () => {
    const f = parseAbiItem("function healthOf(address user) view returns (uint256 health)") as AbiFunction;
    const resolved = resolveArrayFunction(paginatedAbi(f) as AbiFunction);

    expect(envelopeConfig(resolved)).toBe((BigInt(toFunctionSelector(f)) << 224n) | (32n << 64n) | 32n);
  });

  it("packs a dynamic lens as selector, both dynamic bits, and both declared bounds", () => {
    const f = parseAbiItem("function describe(bytes blob) view returns (string text)") as AbiFunction;
    const resolved = resolveArrayFunction(paginatedAbi(f) as AbiFunction, {
      maxItemBytes: 96,
      maxResultBytes: 64,
    });

    expect(envelopeConfig(resolved)).toBe(
      (BigInt(toFunctionSelector(f)) << 224n) | (1n << 223n) | (1n << 222n) | (96n << 64n) | 64n,
    );
  });

  it("sets only the input bit when just the input element is dynamic", () => {
    const f = parseAbiItem("function lengthOf(bytes blob) view returns (uint256 size)") as AbiFunction;
    const resolved = resolveArrayFunction(paginatedAbi(f) as AbiFunction, { maxItemBytes: 128 });

    expect(envelopeConfig(resolved)).toBe((BigInt(toFunctionSelector(f)) << 224n) | (1n << 223n) | (128n << 64n) | 32n);
  });
});

describe("hexToPage", () => {
  it.each([
    ["static U", "uint256[]", STATIC, [1n, 2n, 3n], [1]],
    ["static U, no skips", "uint256[]", STATIC, [1n, 2n], []],
    ["static U, no results", "uint256[]", STATIC, [], [0]],
    ["dynamic U", "string[]", DYNAMIC, ["a", "a much longer string spanning two whole words, definitely"], [2]],
    ["dynamic U, no results", "string[]", DYNAMIC, [], [0, 1]],
    ["dynamic U, empty elements", "bytes[]", DYNAMIC, ["0x", "0x"], []],
    [
      "nested dynamic tuple",
      "(string,uint256[])[]",
      DYNAMIC,
      [
        ["x", [1n, 2n]],
        ["yy", []],
      ],
      [3],
    ],
  ])("round-trips %s against viem's encoder", (_name, types, layout, results, skipped) => {
    const encoded = encodePage(types, results as readonly unknown[], skipped as number[]);
    const page = hexToPage(layout, encoded);

    expect(page.skipped).toEqual(skipped);
    expect(page.died).toBeUndefined();
    expect(page.results).toHaveLength((results as unknown[]).length);
    expect(pageToHex(layout, page)).toBe(encoded);
  });

  it("bounds the last dynamic result at the skipped array rather than end-of-buffer", () => {
    // The whole point of a dedicated paginated codec: with one array the tail runs to end-of-buffer,
    // which would make the final element swallow `skipped` entirely.
    const encoded = encodePage("string[]", ["first", "second"], [5]);
    const { results } = hexToPage(DYNAMIC, encoded);
    const solo = encodeAbiParameters(parseAbiParameters("string[]"), [["first", "second"]]);

    expect(results[1]).toBe(hexToPage(DYNAMIC, encodePage("string[]", ["first", "second"], []))!.results[1]);
    expect(solo.endsWith(results[1]!.slice(2))).toBe(true);
  });

  it("rejects a head shorter than two parameter offsets", () => {
    expect(() => hexToPage(STATIC, `0x${"00".repeat(63)}` as Hex)).toThrow(/shorter than a two-parameter head/);
  });

  it.each([
    ["results after skipped", 0x40, 0x20],
    ["equal offsets", 0x40, 0x40],
    ["skipped past end of buffer", 0x40, 0xffff],
    ["results pointing into the two-word head", 0x00, 0xa0],
    ["a word-misaligned results offset", 0x44, 0xa0],
    ["a word-misaligned skipped offset", 0x40, 0x84],
  ])("rejects %s", (_name, resultsAt, skippedAt) => {
    const word = (n: number) => n.toString(16).padStart(64, "0");
    const encoded = `0x${word(resultsAt)}${word(skippedAt)}${word(0)}${word(0)}` as Hex;
    expect(() => hexToPage(STATIC, encoded)).toThrow(
      /paginated encoding parameter offsets out of order or out of range/,
    );
  });

  it("rejects a skipped array shorter than its declared length", () => {
    const word = (n: number) => n.toString(16).padStart(64, "0");
    const encoded = `0x${word(0x40)}${word(0x60)}${word(0)}${word(4)}` as Hex;
    expect(() => hexToPage(STATIC, encoded)).toThrow(/skipped array shorter than declared length/);
  });

  it.each([
    ["an element offset pointing back into its own offset table", 1, [0]],
    ["a word-misaligned element offset", 1, [0x24]],
    ["duplicate element offsets", 2, [0x40, 0x40]],
    ["descending element offsets", 2, [0x60, 0x40]],
  ])("rejects %s", (_name, length, offsets) => {
    const word = (n: number) => n.toString(16).padStart(64, "0");
    // A `string[]` at 0x40 with the given offset table, then an empty `skipped` array. Without
    // the tail/alignment floor, offset 0 slices the table itself and decodes as a plausible "".
    const table = (offsets as number[]).map(word).join("");
    const skippedAt = 0x60 + (offsets as number[]).length * 32;
    const encoded = `0x${word(0x40)}${word(skippedAt)}${word(length as number)}${table}${word(0)}` as Hex;
    expect(() => hexToPage(DYNAMIC, encoded)).toThrow(/dynamic-layout offsets out of order or out of range/);
  });

  describe("gas-death tag", () => {
    it.each([[0], [1], [1_000_000]])("round-trips a death at index %i", (died) => {
      const encoded = encodePage("uint256[]", [7n], [tag(died)]);
      const page = hexToPage(STATIC, encoded);

      expect(page.died).toBe(died);
      expect(page.skipped).toEqual([]);
      expect(page.results).toHaveLength(1);
      expect(pageToHex(STATIC, page)).toBe(encoded);
    });

    it("keeps plain skips ahead of the tag", () => {
      const encoded = encodePage("uint256[]", [7n], [0, 2, tag(3)]);
      const page = hexToPage(STATIC, encoded);

      expect(page).toMatchObject({ skipped: [0, 2], died: 3 });
      expect(pageToHex(STATIC, page)).toBe(encoded);
    });

    it("rejects a tagged index past the safe integer range", () => {
      const encoded = encodePage("uint256[]", [], [tag(BigInt(Number.MAX_SAFE_INTEGER) + 1n)]);
      expect(() => hexToPage(STATIC, encoded)).toThrow(/tagged skipped index exceeds safe integer range/);
    });

    it("reads a top-bit word outside the last position as a plain, range-checked index", () => {
      // Only the final entry is a tag; anywhere else the word is an ordinary skipped index and
      // fails the range check rather than silently decoding as a death.
      const encoded = encodePage("uint256[]", [], [tag(0), 1]);
      expect(() => hexToPage(STATIC, encoded)).toThrow(/exceeds safe integer range/);
    });
  });
});

describe("pageToHex", () => {
  it("matches viem for an entirely empty page", () => {
    const page: Page = { results: [], skipped: [] };
    expect(pageToHex(DYNAMIC, page)).toBe(encodePage("string[]", [], []));
    expect(pageToHex(STATIC, page)).toBe(encodePage("uint256[]", [], []));
  });

  it("emits a death with no results as a lone tagged word", () => {
    expect(pageToHex(STATIC, { results: [], skipped: [], died: 0 })).toBe(encodePage("uint256[]", [], [tag(0)]));
  });
});
