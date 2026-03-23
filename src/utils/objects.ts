export function isPlainObject(value: unknown): value is Record<PropertyKey, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

interface BuildOptions {
  sortKeys?: boolean;
  transformKey?: (key: string) => string;
  transformLeaf?: <T>(value: T) => T;
  /** Called after recursion. Return false to omit the entry. Only applies to object entries, not array elements. */
  shouldInclude?: (key: string, transformed: unknown) => boolean;
}

/** Recursively copies `value`, applying the given transforms at each level. */
export function deepTransform<T = unknown>(value: T, options: BuildOptions): T {
  if (value === null || typeof value !== "object") {
    return options.transformLeaf ? options.transformLeaf(value) : value;
  }

  if (Array.isArray(value)) {
    return value.map((v) => deepTransform(v, options)) as typeof value;
  }

  const obj = value as Record<string, unknown>;
  let keys = Object.keys(obj);
  if (options.sortKeys) keys = keys.sort();

  const result: Record<string, unknown> = {};
  for (const key of keys) {
    const transformed = deepTransform(obj[key], options);
    if (options.shouldInclude && !options.shouldInclude(key, transformed)) continue;
    const newKey = options.transformKey ? options.transformKey(key) : key;
    result[newKey] = transformed;
  }

  return result as T;
}

export const deepTransformOptions = {
  /** Sorts object keys lexicographically at every level. */
  sortKeys: { sortKeys: true },
  /** Omits `undefined` entries and prunes objects that become empty after cleanup. */
  deleteUndefined: {
    shouldInclude: (_: string, v: unknown) =>
      v !== undefined && !(isPlainObject(v) && Object.keys(v as Record<string, unknown>).length === 0),
  },
  /** Lowercases all string keys and string leaf values. */
  lowercase: {
    transformKey: (k: string) => k.toLowerCase(),
    transformLeaf: <T>(v: T) => (typeof v === "string" ? v.toLowerCase() : v) as T,
  },
} as const satisfies Record<string, BuildOptions>;
