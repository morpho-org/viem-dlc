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
