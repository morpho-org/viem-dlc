import type { Hex } from "viem";
import { type AbiFunction, encodeAbiParameters, parseAbiItem, parseAbiParameters } from "viem";
import { describe, expect, it } from "vitest";

import { hexToPage, type Page, pageToHex, resolveArrayFunction } from "../../../src/utils/deployless/codec.inner.js";

const DYNAMIC = { mode: "dynamic" } as const;
const STATIC = { mode: "static", size: 32 } as const;

/** Encodes `(U[] results, uint256[] skipped)` the way a real lens would, via viem. */
function encodePage(types: string, results: readonly unknown[], skipped: readonly number[]): Hex {
  return encodeAbiParameters(parseAbiParameters(`${types}, uint256[]`), [results, skipped.map(BigInt)] as never);
}

describe("resolveArrayFunction", () => {
  const paged = parseAbiItem(
    "function page(address[] input) view returns (uint256[] results, uint256[] skipped)",
  ) as AbiFunction;

  it("resolves a paged fragment and reports the element layout of `results`", () => {
    const resolved = resolveArrayFunction(paged, true);
    expect(resolved.paged).toBe(true);
    expect(resolved.outputLayout).toEqual(STATIC);
    expect(resolved.inputLayout).toEqual(STATIC);
  });

  it("defaults to the unpaged shape", () => {
    const unpaged = parseAbiItem("function f(address[] a) view returns (uint256[])") as AbiFunction;
    expect(resolveArrayFunction(unpaged).paged).toBe(false);
  });

  it("rejects a paged fragment whose second output is not uint256[]", () => {
    const wrong = parseAbiItem("function page(address[] a) view returns (uint256[] r, address[] s)") as AbiFunction;
    expect(() => resolveArrayFunction(wrong, true)).toThrow(/paged output 1 must be uint256\[\]/);
  });

  it("rejects a paged fragment with a single output", () => {
    const single = parseAbiItem("function page(address[] a) view returns (uint256[])") as AbiFunction;
    expect(() => resolveArrayFunction(single, true)).toThrow(/must return \(U\[\] results, uint256\[\] skipped\)/);
  });

  it("rejects an unpaged fragment with two outputs", () => {
    expect(() => resolveArrayFunction(paged)).toThrow(/expected exactly one dynamic-array output/);
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
    expect(page.results).toHaveLength((results as unknown[]).length);
    expect(pageToHex(layout, page)).toBe(encoded);
  });

  it("bounds the last dynamic result at the skipped array rather than end-of-buffer", () => {
    // The whole point of a dedicated paged codec: with one array the tail runs to end-of-buffer,
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
  ])("rejects %s", (_name, resultsAt, skippedAt) => {
    const word = (n: number) => n.toString(16).padStart(64, "0");
    const encoded = `0x${word(resultsAt)}${word(skippedAt)}${word(0)}${word(0)}` as Hex;
    expect(() => hexToPage(STATIC, encoded)).toThrow(/parameter offsets out of order or out of range/);
  });

  it("rejects a skipped array shorter than its declared length", () => {
    const word = (n: number) => n.toString(16).padStart(64, "0");
    const encoded = `0x${word(0x40)}${word(0x60)}${word(0)}${word(4)}` as Hex;
    expect(() => hexToPage(STATIC, encoded)).toThrow(/skipped array shorter than declared length/);
  });

  it("rejects out-of-order dynamic element offsets inside results", () => {
    const word = (n: number) => n.toString(16).padStart(64, "0");
    // results at 0x40 holding 2 elements whose offset table points backwards; skipped at 0xa0.
    const encoded = `0x${word(0x40)}${word(0xa0)}${word(2)}${word(0x60)}${word(0x40)}${word(0)}` as Hex;
    expect(() => hexToPage(DYNAMIC, encoded)).toThrow(/offsets out of order or out of range/);
  });
});

describe("pageToHex", () => {
  it("matches viem for an entirely empty page", () => {
    const page: Page = { results: [], skipped: [] };
    expect(pageToHex(DYNAMIC, page)).toBe(encodePage("string[]", [], []));
    expect(pageToHex(STATIC, page)).toBe(encodePage("uint256[]", [], []));
  });
});
