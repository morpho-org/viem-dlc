/**
 * FastLZ (LZ77) compress / decompress — JS port of Solady's LibZip.sol (MIT).
 *   https://github.com/Vectorized/solady/blob/main/js/solady.js
 *
 * Both functions accept and return `Hex` (`0x…`-prefixed strings). The compressed
 * byte format is identical to Solady's on-chain implementation, so data compressed
 * here decompresses correctly in the EVM and vice versa.
 */
import type { Hex } from "viem";

function hexToBytes(hex: Hex): number[] {
  const s = hex.length >= 2 && (hex[1] === "x" || hex[1] === "X") ? hex.slice(2) : hex;
  const bytes: number[] = [];
  for (let i = 0; i < s.length; i += 2) bytes.push(parseInt(s.slice(i, i + 2), 16));
  return bytes;
}

function bytesToHex(bytes: number[]): Hex {
  return `0x${bytes.map((b) => (b & 0xff).toString(16).padStart(2, "0")).join("")}` as Hex;
}

/** Compress `data` with FastLZ. Matches Solady's `LibZip.flzCompress`. */
export function flzCompress(data: Hex): Hex {
  const ib = hexToBytes(data);
  const b = ib.length - 4;
  const ht: number[] = [];
  const ob: number[] = [];
  let a = 0,
    i = 2,
    o = 0;

  const u24 = (p: number) => (ib[p] ?? 0) | ((ib[p + 1] ?? 0) << 8) | ((ib[p + 2] ?? 0) << 16);
  const fnHash = (x: number) => (Math.imul(2654435769, x) >> 19) & 8191;
  const emitLiterals = (count: number, src: number) => {
    while (count >= 32) {
      ob[o++] = 31;
      for (let k = 32; k--; count--) ob[o++] = ib[src++] ?? 0;
    }
    if (count) {
      ob[o++] = count - 1;
      while (count--) ob[o++] = ib[src++] ?? 0;
    }
  };

  while (i < b - 9) {
    let r = 0,
      d = 0,
      s = 0,
      h = 0;

    // Mirrors Solady's do { body } while (i < b - 9 && i++ && s != c):
    // - limit check fires BEFORE the increment (condition step 1)
    // - i++ always runs when not at limit (condition step 2)
    // - match check fires AFTER the increment (condition step 3)
    // On match: exits with i = match_pos + 1; then --i restores match_pos.
    // On limit: exits with i >= b - 9, no increment.
    while (true) {
      s = u24(i);
      h = fnHash(s);
      r = ht[h] ?? 0;
      d = i - r;
      ht[h] = i;
      const matched = s === (d < 8192 ? u24(r) : 0x1000000);
      if (i >= b - 9) break;
      i++;
      if (matched) break;
    }
    if (i >= b - 9) break;
    i--;

    if (i > a) emitLiterals(i - a, a);

    // Count match bytes past the implicit 3-byte hash match.
    let l = 0;
    const r3 = r + 3,
      i3 = i + 3;
    let e = b - i3;
    for (; l < e; l++) e *= ib[r3 + l] === ib[i3 + l] ? 1 : 0;

    // Skip 3-byte-only matches: emitting a type=0 token (l=0 → top 3 bits = 0)
    // is indistinguishable from a literal-run header, so the decompressor would
    // corrupt the output. Let the bytes fall through to the next literal flush.
    if (l === 0) {
      a = i;
      i++;
      continue;
    }

    // Emit match token(s).
    i += l;
    const dd = d - 1;
    while (l > 262) {
      ob[o++] = 224 + (dd >> 8);
      ob[o++] = 253;
      ob[o++] = dd & 255;
      l -= 262;
    }
    if (l < 7) {
      ob[o++] = (l << 5) + (dd >> 8);
      ob[o++] = dd & 255;
    } else {
      ob[o++] = 224 + (dd >> 8);
      ob[o++] = l - 7;
      ob[o++] = dd & 255;
    }

    ht[fnHash(u24(i))] = i++;
    ht[fnHash(u24(i))] = i++;
    a = i;
  }

  emitLiterals(b + 4 - a, a);
  return bytesToHex(ob);
}

/** Decompress FastLZ-compressed `data`. Matches Solady's `LibZip.flzDecompress`. */
export function flzDecompress(data: Hex): Hex {
  const ib = hexToBytes(data);
  const ob: number[] = [];
  let i = 0,
    o = 0;

  while (i < ib.length) {
    const ctrl = ib[i] ?? 0;
    const origT = ctrl >> 5;
    if (!origT) {
      // Literal run: ctrl+1 bytes
      let l = 1 + ctrl;
      i++;
      while (l--) ob[o++] = ib[i++] ?? 0;
    } else {
      // Back-reference
      const isShort = origT < 7;
      const tNew = isShort ? 1 : 0;
      const f = 256 * (ctrl & 31) + (ib[i + 2 - tNew] ?? 0);
      const l = isShort ? 2 + origT : 9 + (ib[i + 1] ?? 0);
      i += 3 - tNew;
      let r = o - f - 1;
      for (let k = l; k--; ) ob[o++] = ob[r++] ?? 0;
    }
  }

  return bytesToHex(ob);
}
