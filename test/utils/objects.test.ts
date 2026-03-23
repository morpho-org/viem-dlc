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

describe("deepTransform + lowercase", () => {
  it("lowercases a plain string", () => {
    expect(deepTransform("Hello World", dt.lowercase)).toBe("hello world");
  });

  it("lowercases object keys", () => {
    expect(deepTransform({ Foo: 1, BAR: 2 }, dt.lowercase)).toEqual({ foo: 1, bar: 2 });
  });

  it("lowercases object string values", () => {
    expect(deepTransform({ a: "HELLO" }, dt.lowercase)).toEqual({ a: "hello" });
  });

  it("lowercases both keys and values recursively", () => {
    expect(deepTransform({ Outer: { Inner: "VALUE" } }, dt.lowercase)).toEqual({ outer: { inner: "value" } });
  });

  it("lowercases array elements", () => {
    expect(deepTransform(["Hello", "WORLD"], dt.lowercase)).toEqual(["hello", "world"]);
  });

  it("handles nested arrays in objects", () => {
    expect(deepTransform({ Tags: ["Foo", "BAR"] }, dt.lowercase)).toEqual({ tags: ["foo", "bar"] });
  });

  it("leaves numbers, booleans, and null unchanged", () => {
    expect(deepTransform({ a: 42, b: true, c: null }, dt.lowercase)).toEqual({ a: 42, b: true, c: null });
  });

  it("returns primitives unchanged", () => {
    expect(deepTransform(42, dt.lowercase)).toBe(42);
    expect(deepTransform(null, dt.lowercase)).toBe(null);
    expect(deepTransform(true, dt.lowercase)).toBe(true);
  });

  it("returns a copy, not the original", () => {
    const obj = { Key: "Value" };
    const result = deepTransform(obj, dt.lowercase);
    expect(result).not.toBe(obj);
  });
});
