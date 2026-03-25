/**
 * A fast and simple 64-bit (or 53-bit) string hash function with decent collision resistance.
 * Largely inspired by MurmurHash2/3, but with a focus on speed/simplicity.
 *
 * cyrb53 (c) 2018 bryc (github.com/bryc). License: Public domain. Attribution appreciated.
 *
 * See:
 * - https://gist.github.com/jlevy/c246006675becc446360a798e2b2d781
 * - https://stackoverflow.com/questions/7616461/generate-a-hash-from-string-in-javascript/52171480#52171480
 * - https://github.com/bryc/code/blob/master/jshash/experimental/cyrb53.js
 */
export function cyrb64(str: string, seed = 0) {
  let h1 = 0xdeadbeef ^ seed,
    h2 = 0x41c6ce57 ^ seed;
  for (let i = 0, ch: number; i < str.length; i++) {
    ch = str.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507);
  h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507);
  h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  // For a single 53-bit numeric return value we could return
  // 4294967296 * (2097151 & h2) + (h1 >>> 0);
  // but we instead return the full 64-bit value:
  return [h2 >>> 0, h1 >>> 0] as const;
}

const paddingLookupTable: Record<number, Record<number, number>> = {};

export function computePadding(bits: number, radix: number): number {
  if (paddingLookupTable[bits]?.[radix] !== undefined) {
    return paddingLookupTable[bits][radix];
  }

  if (!Number.isInteger(bits) || bits < 1 || bits > 52) {
    throw new RangeError("bits must be an integer between 1 and 52");
  }
  if (!Number.isInteger(radix) || radix < 2 || radix > 36) {
    throw new RangeError("radix must be an integer between 2 and 36");
  }

  const limit = 2 ** bits;
  let pow = 1;
  let k = 0;
  while (pow < limit) {
    pow *= radix;
    k++;
  }

  if (!paddingLookupTable[bits]) {
    paddingLookupTable[bits] = {};
  }
  paddingLookupTable[bits][radix] = k;

  return k;
}

/** An *insecure* 64-bit hash that's short, fast, and has no dependencies. Output is always 14 characters. */
export function cyrb64Hash(str: string, radix = 36, seed = 777777) {
  const [h2, h1] = cyrb64(str, seed);
  const padding = computePadding(32, radix);
  return h2.toString(radix).padStart(padding, "0") + h1.toString(radix).padStart(padding, "0");
}
