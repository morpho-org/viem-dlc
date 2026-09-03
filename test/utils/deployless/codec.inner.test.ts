import type { Hex } from "viem";
import {
  type AbiFunction,
  encodeAbiParameters,
  pad,
  parseAbiItem,
  parseAbiParameters,
  toFunctionSelector,
  toHex,
} from "viem";
import { describe, expect, it } from "vitest";

import { envelopeConfig } from "../../../src/utils/deployless/codec.envelope.js";
import {
  arrayifiedAbi,
  arrayToWire,
  hexToArray,
  hexToPage,
  itemFragmentOf,
  type Page,
  type PageGas,
  pageToHex,
  pageToWire,
  resolveArrayFunction,
  wireToArray,
} from "../../../src/utils/deployless/codec.inner.js";
import { flatGas } from "../../helpers/page.js";

const DYNAMIC = { mode: "dynamic" } as const;
const STATIC = { mode: "static", size: 32 } as const;
const addr = (n: number) => pad(toHex(n), { size: 20 });

/** The wire form of a gas death at `index`: the 256-bit complement, `~index`. */
const tag = (index: number | bigint) => ((1n << 256n) - 1n) ^ BigInt(index);

const word = (n: number | bigint) => BigInt(n).toString(16).padStart(64, "0");
const wordHex = (n: number | bigint) => `0x${word(n)}` as Hex;

/** A success record: `(1 << 255) | L` then the `L` raw bytes. */
const success = (element: Hex) => word((1n << 255n) | BigInt((element.length - 2) / 2)) + element.slice(2);
/** The four telemetry words as they sit on the wire. */
const header = ({ budget, sum, sumSquares, max }: PageGas) => word(budget) + word(sum) + word(sumSquares) + word(max);
/** An outcome stream: `nA`, flat telemetry for every record but a death, then the records verbatim. */
const stream = (...records: string[]) => {
  const served = records.filter((r) => !r.startsWith("ffff")).length;
  return `0x${word(records.length)}${header(flatGas(served))}${records.join("")}` as Hex;
};

/** Encodes `(U[] results, uint256[] skipped)` the way a real lens would, via viem. */
function encodePage(types: string, results: readonly unknown[], skipped: readonly (number | bigint)[]): Hex {
  return encodeAbiParameters(parseAbiParameters(`${types}, uint256[]`), [results, skipped.map(BigInt)] as never);
}

/** The raw element bytes of `values` as `hexToArray` slices them: words for a static `T`, padded tails otherwise. */
function elementsOf(type: string, values: readonly unknown[]): readonly Hex[] {
  return hexToArray(layoutOf(type), encodeAbiParameters(parseAbiParameters(type), [values] as never));
}

function layoutOf(type: string) {
  return resolveArrayFunction(
    parseAbiItem(`function f(${type} a) view returns (${type} results, uint256[] skipped)`) as AbiFunction,
  ).outputLayout;
}

describe("resolveArrayFunction", () => {
  const paginated = parseAbiItem(
    "function page(address[] input) view returns (uint256[] results, uint256[] skipped)",
  ) as AbiFunction;

  it("resolves a paginated fragment and reports both element layouts", () => {
    const resolved = resolveArrayFunction(paginated);
    expect(resolved.inputLayout).toEqual(STATIC);
    expect(resolved.outputLayout).toEqual(STATIC);
  });

  it("accepts dynamic element types on either side without a declared bound", () => {
    const f = parseAbiItem(
      "function page(string[] input) view returns (bytes[] results, uint256[] skipped)",
    ) as AbiFunction;
    const resolved = resolveArrayFunction(f);
    expect(resolved.inputLayout).toEqual(DYNAMIC);
    expect(resolved.outputLayout).toEqual(DYNAMIC);
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
});

describe("arrayifiedAbi", () => {
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
    const paginated = arrayifiedAbi(f);

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
    const back = itemFragmentOf(arrayifiedAbi(f) as AbiFunction);

    // `arrayifiedAbi` renames the sole output to `results` so viem decodes the page tuple by name;
    // that rename is the only thing the round trip does not restore.
    expect(back).toEqual({ ...f, outputs: [{ ...f.outputs[0], name: "results" }] });
    expect(toFunctionSelector(back)).toBe(toFunctionSelector(f));
  });

  it("rejects a function taking two parameters", () => {
    expect(() => arrayifiedAbi(item("function pairOf(address a, address b) view returns (uint256 r)"))).toThrow(
      /pairOf must take exactly one parameter/,
    );
  });

  it("rejects a function returning nothing", () => {
    expect(() => arrayifiedAbi(item("function poke(address a) view"))).toThrow(/poke must return exactly one value/);
  });

  it("rejects a function returning two values", () => {
    expect(() => arrayifiedAbi(item("function bothOf(address a) view returns (uint256 r, uint256 s)"))).toThrow(
      /bothOf must return exactly one value/,
    );
  });

  it("rejects a function that is not view or pure", () => {
    expect(() => arrayifiedAbi(item("function poke(address a) returns (uint256 r)"))).toThrow(
      /poke must be view or pure/,
    );
  });

  it("lays out an external function value as one static word", () => {
    const f: AbiFunction = {
      type: "function",
      name: "hookOf",
      stateMutability: "view",
      inputs: [{ type: "address" }],
      outputs: [{ type: "function" }],
    };
    expect(resolveArrayFunction(arrayifiedAbi(f) as AbiFunction).outputLayout).toEqual({ mode: "static", size: 32 });
  });

  it("resolves to the per-item selector the envelope has to call", () => {
    const f = item("function healthOf(address user) view returns (uint256 health)");
    expect(resolveArrayFunction(arrayifiedAbi(f) as AbiFunction).itemSelector).toBe(toFunctionSelector(f));
  });
});

describe("envelopeConfig", () => {
  it("packs a static lens as selector, no flag bits, and both strides", () => {
    const f = parseAbiItem("function healthOf(address user) view returns (uint256 health)") as AbiFunction;
    const resolved = resolveArrayFunction(arrayifiedAbi(f) as AbiFunction);

    expect(envelopeConfig(resolved, false)).toBe((BigInt(toFunctionSelector(f)) << 224n) | (32n << 64n) | 32n);
  });

  it("packs a dynamic lens as selector, both dynamic bits, and zero strides", () => {
    const f = parseAbiItem("function describe(bytes blob) view returns (string text)") as AbiFunction;
    const resolved = resolveArrayFunction(arrayifiedAbi(f) as AbiFunction);

    expect(envelopeConfig(resolved, false)).toBe((BigInt(toFunctionSelector(f)) << 224n) | (1n << 223n) | (1n << 222n));
  });

  it("sets only the input bit when just the input element is dynamic", () => {
    const f = parseAbiItem("function lengthOf(bytes blob) view returns (uint256 size)") as AbiFunction;
    const resolved = resolveArrayFunction(arrayifiedAbi(f) as AbiFunction);

    expect(envelopeConfig(resolved, false)).toBe((BigInt(toFunctionSelector(f)) << 224n) | (1n << 223n) | 32n);
  });

  it("sets bit 221 when the body is compressed", () => {
    const f = parseAbiItem("function healthOf(address user) view returns (uint256 health)") as AbiFunction;
    const resolved = resolveArrayFunction(arrayifiedAbi(f) as AbiFunction);

    expect(envelopeConfig(resolved, true) ^ envelopeConfig(resolved, false)).toBe(1n << 221n);
  });
});

describe("arrayToWire", () => {
  it("emits n, bodyLen and a body byte-identical to the ABI array body for a static T", () => {
    const values = [addr(1), addr(2), addr(3)];
    const abiBody = encodeAbiParameters(parseAbiParameters("address[]"), [values]).slice(2 + 64 + 64);
    const wire = arrayToWire(STATIC, elementsOf("address[]", values));

    expect(wire).toBe(`0x${word(3)}${word(96)}${abiBody}`);
    expect(wireToArray(STATIC, wire)).toEqual(elementsOf("address[]", values));
  });

  it("emits length-prefixed tails for a dynamic T and round-trips them", () => {
    const values = ["", "a", "a much longer string spanning two whole words, definitely"];
    const elements = elementsOf("string[]", values);
    const wire = arrayToWire(DYNAMIC, elements);

    expect(wire.slice(2, 2 + 128)).toBe(`${word(3)}${word(32 + 64 + 96 + 3 * 32)}`);
    expect(wireToArray(DYNAMIC, wire)).toEqual(elements);
  });

  it("emits an empty body for no elements", () => {
    expect(arrayToWire(STATIC, [])).toBe(`0x${word(0)}${word(0)}`);
    expect(wireToArray(DYNAMIC, arrayToWire(DYNAMIC, []))).toEqual([]);
  });

  it.each([
    ["a body length that does not match", `0x${word(1)}${word(64)}${word(7)}`, /body length does not match/],
    ["a static element past the body", `0x${word(2)}${word(32)}${word(7)}`, /runs past the body/],
    ["a zero dynamic length", `0x${word(1)}${word(32)}${word(0)}`, /declares 0 bytes/],
    ["a misaligned dynamic length", `0x${word(1)}${word(64)}${word(33)}${word(0)}`, /declares 33 bytes/],
    ["a dynamic element past the body", `0x${word(1)}${word(64)}${word(64)}${word(0)}`, /runs past the body/],
    ["a dynamic body with trailing bytes", `0x${word(1)}${word(96)}${word(32)}${word(0)}${word(0)}`, /trailing bytes/],
  ])("rejects %s", (_name, wire, expected) => {
    const layout = wire.length > 2 + 128 && _name.includes("static") ? STATIC : DYNAMIC;
    expect(() => wireToArray(layout, wire as Hex)).toThrow(expected);
  });
});

describe("hexToPage", () => {
  it.each([
    ["static U", "uint256[]", [1n, 2n, 3n], [1], undefined],
    ["static U, no skips", "uint256[]", [1n, 2n], [], undefined],
    ["static U, no results", "uint256[]", [], [0], undefined],
    ["static U, a death", "uint256[]", [7n], [0, 2], 3],
    ["dynamic U", "string[]", ["a", "a much longer string spanning two whole words, definitely"], [2], undefined],
    ["dynamic U, no results", "string[]", [], [0, 1], undefined],
    ["dynamic U, empty elements", "bytes[]", ["0x", "0x"], [], undefined],
    ["dynamic U, a death", "bytes[]", ["0x0102"], [], 1],
    [
      "nested dynamic tuple",
      "(string,uint256[])[]",
      [
        ["x", [1n, 2n]],
        ["yy", []],
      ],
      [1],
      undefined,
    ],
  ])("round-trips %s through the stream and encodes as viem does", (_name, types, results, skipped, died) => {
    const layout = layoutOf(types);
    const gas = flatGas(results.length + skipped.length);
    const page: Page = { results: elementsOf(types, results), skipped, gas, ...(died === undefined ? {} : { died }) };
    const decoded = hexToPage(layout, pageToWire(page));

    expect(decoded).toEqual(page);
    // The caller-facing tuple never carries the death: the client resolves it before aggregating.
    expect(pageToHex(layout, decoded)).toBe(encodePage(types, results, skipped));
  });

  it("reads records in attempt order and binds each to its ordinal", () => {
    const encoded = stream(success(wordHex(7)), word(1), success(wordHex(9)), word(tag(3)));
    expect(hexToPage(STATIC, encoded)).toEqual({
      results: [wordHex(7), wordHex(9)],
      skipped: [1],
      died: 3,
      gas: flatGas(3),
    });
  });

  it.each([
    ["a payload shorter than its header", `0x${word(1)}${word(0).repeat(3)}`, /shorter than its header/],
    ["a page that adjudicated nothing", stream(), /adjudicated no elements/],
    ["more records than the payload can hold", `0x${word(2)}${word(0).repeat(5)}`, /claims 2 records in 192 bytes/],
    ["a decline bound to another ordinal", stream(word(1), word(1)), /record 0 declines element 1/],
    ["a repeated decline", stream(word(0), word(0)), /record 1 declines element 0/],
    ["a death that is not last", stream(word(tag(0)), word(1)), /record 0 of 2 reports a gas death at 0/],
    ["a death bound to another ordinal", stream(word(0), word(tag(2))), /record 1 of 2 reports a gas death at 2/],
    ["two deaths", stream(word(tag(0)), word(tag(1))), /record 0 of 2 reports a gas death at 0/],
    ["the unused record namespace", stream(word(1n << 254n)), /neither a success, a decline nor a death/],
    ["a static result of the wrong size", stream(success("0x0102")), /2-byte result/],
    ["trailing bytes", `${stream(word(0))}00`, /trailing bytes/],
  ])("rejects %s", (_name, encoded, expected) => {
    expect(() => hexToPage(STATIC, encoded as Hex)).toThrow(expected);
  });

  it.each([
    ["a zero-length dynamic result", stream(success("0x")), /0-byte result/],
    ["a misaligned dynamic result", stream(success(`0x${"00".repeat(33)}`)), /33-byte result/],
    [
      "a success running past the payload",
      `0x${word(1)}${header(flatGas(1))}${word((1n << 255n) | 64n)}${word(0)}`,
      /runs past the payload/,
    ],
  ])("rejects %s", (_name, encoded, expected) => {
    expect(() => hexToPage(DYNAMIC, encoded as Hex)).toThrow(expected);
  });

  it("accepts a lone death at index 0", () => {
    expect(hexToPage(STATIC, stream(word(tag(0))))).toEqual({ results: [], skipped: [], died: 0, gas: flatGas(0) });
  });

  const telemetry = (gas: Partial<PageGas>, ...records: string[]) =>
    `0x${word(records.length)}${header({ ...flatGas(records.length), ...gas })}${records.join("")}` as Hex;

  it.each([
    ["a served attempt costing nothing", telemetry({ sum: 0n, sumSquares: 0n, max: 0n }, word(0))],
    ["a maximum above the sum", telemetry({ max: 1_001n }, word(0))],
    ["a sum of squares below the mean's", telemetry({ sumSquares: 1_999_999n }, word(0), word(1))],
    ["a sum of squares above the sum times the maximum", telemetry({ sumSquares: 2_000_001n }, word(0), word(1))],
    ["cost charged to a lone death", telemetry(flatGas(1), word(tag(0)))],
  ])("rejects %s", (_name, encoded) => {
    expect(() => hexToPage(STATIC, encoded)).toThrow(/telemetry is inconsistent/);
  });

  it("accepts a sum above the budget: the last attempt admitted may spend into the reserve", () => {
    const gas = { ...flatGas(1), budget: 999n };
    expect(hexToPage(STATIC, telemetry(gas, word(0))).gas).toEqual(gas);
  });
});

describe("pageToWire", () => {
  it("emits nA, the telemetry, then one record per attempt", () => {
    const wire = pageToWire({ results: [wordHex(7)], skipped: [1], died: 2, gas: flatGas(2) });
    expect(wire).toBe(stream(success(wordHex(7)), word(1), word(tag(2))));
  });

  it.each([
    ["a skip past the attempts", { results: [wordHex(7)], skipped: [5] }, /did not attempt/],
    ["a repeated skip", { results: [], skipped: [0, 0] }, /did not attempt/],
    ["a skip at the death", { results: [], skipped: [1], died: 1 }, /did not attempt/],
    ["a death that is not last", { results: [wordHex(7)], skipped: [], died: 0 }, /not its last record/],
  ])("rejects a page with %s", (_name, page, expected) => {
    expect(() => pageToWire(page as Page)).toThrow(expected);
  });
});

describe("pageToHex", () => {
  it("matches viem for an entirely empty page", () => {
    const page: Page = { results: [], skipped: [] };
    expect(pageToHex(DYNAMIC, page)).toBe(encodePage("string[]", [], []));
    expect(pageToHex(STATIC, page)).toBe(encodePage("uint256[]", [], []));
  });
});
