/**
 * Measure the UTF-8 encoded byte length of a string.
 *
 * Uses code-point iteration -- no allocations, no TextEncoder.
 * Lone surrogates count as 3 bytes (matching TextEncoder / U+FFFD behavior).
 */
export function measureUtf8Bytes(value: string): number {
  const len = value.length;
  let byteLen = 0;

  for (let i = 0; i < len; ) {
    const cp = value.codePointAt(i)!;
    byteLen += cp <= 0x7f ? 1 : cp <= 0x7ff ? 2 : cp <= 0xffff ? 3 : 4;
    i += cp > 0xffff ? 2 : 1;
  }

  return byteLen;
}

/**
 * Split a JS string into chunks whose UTF-8 encoded size is <= maxBytes.
 *
 * - Does not split surrogate pairs (iterates by Unicode code point).
 * - Lone surrogates count as 3 bytes (same byte count as U+FFFD in UTF-8).
 * - Always returns at least one chunk; empty input => [""].
 */
export function shardString(value: string, maxBytes: number): string[] {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
    throw new Error(`[shardString] maxBytes must be a positive safe integer (got ${maxBytes})`);
  }
  if (value.length === 0) return [""];

  const chunks: string[] = [];
  const len = value.length;

  let chunkStart = 0;
  let bytesInChunk = 0;

  for (let i = 0; i < len; ) {
    const cp = value.codePointAt(i)!; // always defined because i < len
    const step = cp > 0xffff ? 2 : 1;

    // UTF-8 byte length for the code point value.
    // Note: lone surrogates are in 0xD800..0xDFFF (<= 0xFFFF) => 3 bytes,
    // which matches the byte-count of U+FFFD (what TextEncoder effectively emits).
    const byteLen = cp <= 0x7f ? 1 : cp <= 0x7ff ? 2 : cp <= 0xffff ? 3 : 4;

    if (byteLen > maxBytes) {
      throw new Error(`[shardString] a single character requires ${byteLen} UTF-8 bytes, exceeds maxBytes=${maxBytes}`);
    }

    if (bytesInChunk + byteLen > maxBytes) {
      // i is always at a code-point boundary, so this won't split surrogate pairs.
      chunks.push(value.slice(chunkStart, i));
      chunkStart = i;
      bytesInChunk = 0;
    }

    bytesInChunk += byteLen;
    i += step;
  }

  chunks.push(value.slice(chunkStart));
  return chunks;
}
