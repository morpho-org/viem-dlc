import { measureUtf8Bytes } from "./strings.js";

type BigIntTombstone = { __bigint__: string };

function bigIntReplacer<T = unknown>(_: string, value: T) {
  return (typeof value === "bigint" ? { __bigint__: value.toString() } : value) as T extends bigint
    ? BigIntTombstone
    : T;
}

function bigIntReviver<T = unknown>(_: string, value: T) {
  const wasReplaced = (value: unknown): value is BigIntTombstone => {
    return value !== null && typeof value === "object" && !Array.isArray(value) && "__bigint__" in value;
  };

  return wasReplaced(value) ? BigInt(value.__bigint__) : (value as T extends BigIntTombstone ? bigint : T);
}

/** `JSON.stringify` but with bigint support */
export function stringify(value: unknown): string {
  return JSON.stringify(value, bigIntReplacer);
}

/** `JSON.parse` but with bigint support -- returns undefined on parsing error */
// Tells TypeScript that return type is `T` in `throw` mode
export function parse<T = unknown>(value: string, errorHandling: "throw"): T;
// Tells TypeScript that return type is `T | undefined` otherwise
export function parse<T = unknown>(value: string, errorHandling?: undefined): T | undefined;
// Implementation (must cover both cases)
export function parse<T = unknown>(value: string, errorHandling?: "throw") {
  if (errorHandling === "throw") {
    return JSON.parse(value, bigIntReviver) as T;
  }

  try {
    return JSON.parse(value, bigIntReviver) as T;
  } catch {
    return undefined;
  }
}

/** Estimate the UTF-8 encoded byte length of `stringify(value)` without fully materializing it. */
export function estimateUtf8Bytes(value: unknown): number {
  const seen = new Set<unknown>();

  function visit(v: unknown): number {
    if (v == null) return 4;

    switch (typeof v) {
      case "object":
        break;
      case "string":
        return measureUtf8Bytes(v) + 2;
      case "number":
        return Number.isFinite(v) ? String(v).length : 4; // non-finite -> null
      case "boolean":
        return v ? 4 : 5;
      case "bigint":
        // {"__bigint__":"123"}
        return 2 + (measureUtf8Bytes("__bigint__") + 2) + 1 + (measureUtf8Bytes(v.toString()) + 2);
      case "undefined":
      case "function":
      case "symbol":
      default:
        return 0;
    }

    if (seen.has(v)) {
      throw new TypeError("[estimateUtf8Bytes] Cannot estimate size for circular structure");
    }
    seen.add(v);

    try {
      if (Array.isArray(v)) {
        let total = 2;
        for (let i = 0; i < v.length; i++) {
          if (i > 0) total += 1;

          const item = v[i];
          total += item === undefined || typeof item === "function" || typeof item === "symbol" ? 4 : visit(item);
        }
        return total;
      }

      if (typeof (v as { toJSON?: unknown }).toJSON === "function") {
        return visit((v as { toJSON(): unknown }).toJSON());
      }

      let total = 2;
      let first = true;

      for (const [key, val] of Object.entries(v as Record<string, unknown>)) {
        if (val === undefined || typeof val === "function" || typeof val === "symbol") {
          continue;
        }

        if (!first) total += 1;
        first = false;

        total += measureUtf8Bytes(key) + 2;
        total += 1;
        total += visit(val);
      }

      return total;
    } finally {
      seen.delete(v);
    }
  }

  return visit(value);
}
