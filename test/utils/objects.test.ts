import { describe, expect, it } from "vitest";

import { deepTransform, deepTransformOptions as dt } from "../../src/utils/objects.js";

describe("deepTransform + sortKeys", () => {
  it("sorts top-level keys", () => {
    expect(deepTransform({ c: 1, a: 2, b: 3 }, dt.sortKeys)).toEqual({ a: 2, b: 3, c: 1 });
  });

  it("sorts nested object keys", () => {
    expect(deepTransform({ z: { b: 1, a: 2 }, a: 1 }, dt.sortKeys)).toEqual({ a: 1, z: { a: 2, b: 1 } });
  });

  it("recurses into arrays without sorting elements", () => {
    expect(
      deepTransform(
        [
          { b: 1, a: 2 },
          { d: 3, c: 4 },
        ],
        dt.sortKeys,
      ),
    ).toEqual([
      { a: 2, b: 1 },
      { c: 4, d: 3 },
    ]);
  });

  it("returns primitives unchanged", () => {
    expect(deepTransform(42, dt.sortKeys)).toBe(42);
    expect(deepTransform("hello", dt.sortKeys)).toBe("hello");
    expect(deepTransform(null, dt.sortKeys)).toBe(null);
    expect(deepTransform(undefined, dt.sortKeys)).toBe(undefined);
  });

  it("returns a deep copy", () => {
    const inner = { b: 1, a: 2 };
    const obj = { z: inner };
    const result = deepTransform(obj, dt.sortKeys);
    expect(result).toEqual({ z: { a: 2, b: 1 } });
    expect(result).not.toBe(obj);
    expect(result.z).not.toBe(inner);
  });
});

describe("deepTransform + deleteUndefined", () => {
  it("removes top-level undefined keys", () => {
    expect(deepTransform({ a: 1, b: undefined, c: "hello" }, dt.deleteUndefined)).toEqual({ a: 1, c: "hello" });
  });

  it("removes nested undefined keys", () => {
    expect(deepTransform({ a: { b: undefined, c: 2 } }, dt.deleteUndefined)).toEqual({ a: { c: 2 } });
  });

  it("removes empty parent objects after cleanup", () => {
    expect(deepTransform({ a: { b: undefined }, c: 1 }, dt.deleteUndefined)).toEqual({ c: 1 });
  });

  it("handles deeply nested empty objects", () => {
    expect(deepTransform({ a: { b: { c: undefined } } }, dt.deleteUndefined)).toEqual({});
  });

  it("returns non-object values unchanged", () => {
    expect(deepTransform(42, dt.deleteUndefined)).toBe(42);
    expect(deepTransform("hello", dt.deleteUndefined)).toBe("hello");
    expect(deepTransform(null, dt.deleteUndefined)).toBe(null);
    expect(deepTransform(undefined, dt.deleteUndefined)).toBe(undefined);
  });

  it("does not remove null, false, 0, or empty string", () => {
    expect(deepTransform({ a: null, b: false, c: 0, d: "" }, dt.deleteUndefined)).toEqual({
      a: null,
      b: false,
      c: 0,
      d: "",
    });
  });

  it("preserves undefined inside arrays", () => {
    expect(deepTransform({ a: [undefined, 1, undefined] }, dt.deleteUndefined)).toEqual({
      a: [undefined, 1, undefined],
    });
  });

  it("handles an already-clean object", () => {
    expect(deepTransform({ a: 1, b: { c: 2 } }, dt.deleteUndefined)).toEqual({ a: 1, b: { c: 2 } });
  });

  it("returns a copy, not the original", () => {
    const obj = { a: 1, b: undefined };
    const result = deepTransform(obj, dt.deleteUndefined);
    expect(result).not.toBe(obj);
  });
});

describe("deepTransform + lowercaseHex", () => {
  it("lowercases a hex string", () => {
    expect(deepTransform("0xABCDEF", dt.lowercaseHex)).toBe("0xabcdef");
  });

  it("leaves non-hex strings unchanged", () => {
    expect(deepTransform("Hello World", dt.lowercaseHex)).toBe("Hello World");
  });

  it("lowercases hex object keys", () => {
    expect(deepTransform({ "0xABC": 1, foo: 2 }, dt.lowercaseHex)).toEqual({ "0xabc": 1, foo: 2 });
  });

  it("lowercases hex object string values", () => {
    expect(deepTransform({ a: "0xDEAD" }, dt.lowercaseHex)).toEqual({ a: "0xdead" });
  });

  it("leaves non-hex keys and values unchanged", () => {
    expect(deepTransform({ Foo: "BAR" }, dt.lowercaseHex)).toEqual({ Foo: "BAR" });
  });

  it("lowercases hex keys and values recursively", () => {
    expect(deepTransform({ "0xOuter": { "0xInner": "0xVALUE" } }, dt.lowercaseHex)).toEqual({
      "0xouter": { "0xinner": "0xvalue" },
    });
  });

  it("lowercases hex array elements", () => {
    expect(deepTransform(["0xAA", "hello"], dt.lowercaseHex)).toEqual(["0xaa", "hello"]);
  });

  it("handles nested arrays in objects", () => {
    expect(deepTransform({ tags: ["0xFoo", "BAR"] }, dt.lowercaseHex)).toEqual({ tags: ["0xfoo", "BAR"] });
  });

  it("leaves numbers, booleans, and null unchanged", () => {
    expect(deepTransform({ a: 42, b: true, c: null }, dt.lowercaseHex)).toEqual({ a: 42, b: true, c: null });
  });

  it("returns primitives unchanged", () => {
    expect(deepTransform(42, dt.lowercaseHex)).toBe(42);
    expect(deepTransform(null, dt.lowercaseHex)).toBe(null);
    expect(deepTransform(true, dt.lowercaseHex)).toBe(true);
  });

  it("returns a copy, not the original", () => {
    const obj = { "0xKey": "0xValue" };
    const result = deepTransform(obj, dt.lowercaseHex);
    expect(result).not.toBe(obj);
  });
});
