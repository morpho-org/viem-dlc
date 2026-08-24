import type { AbiFunction, AbiParameter, Hex } from "viem";
import { toFunctionSelector } from "viem";

/**
 * Structural ABI codec for a single dynamic array `T[]`. We only decode the outer array
 * structure — element bytes are sliced raw and passed through to the cache and back to
 * the caller without ever instantiating their JS values. This keeps the hot path byte-
 * level regardless of `T` (uints, addresses, structs, strings, nested arrays, etc.).
 */

/**
 * Layout of each element inside the dynamic array's inner tuple encoding.
 *
 * - `static`: element occupies `size` bytes at a fixed stride, no offset table. This is
 *   the layout when `T` is one of uintN/intN/bool/address/bytesN, a fixed-size array of
 *   static types, or a tuple of static types.
 * - `dynamic`: element is referenced by a 32-byte offset (relative to the start of the
 *   inner tuple) in a `k*32` byte head table, with element bytes trailing.
 */
export type ElementLayout = { mode: "static"; size: number } | { mode: "dynamic" };

export type ResolvedArrayFunction = {
  /** 4-byte function selector computed from the original fragment. */
  selector: Hex;
  /** Element layout for the input array. */
  inputLayout: ElementLayout;
  /** Element layout for the output array. */
  outputLayout: ElementLayout;
  /** Whether the fragment is a paged lens — see {@link resolveArrayFunction}. */
  paged: boolean;
};

/**
 * Validates that `fragment` is a single-input function whose input is a dynamic array, and
 * resolves the element layout of that input and of the result array.
 *
 * With `paged`, the fragment must return `(U[] results, uint256[] skipped)` instead of a bare
 * `U[]`; `outputLayout` describes `U` either way. The paged contract — index order, "attempt at
 * least one item", deterministic skips — is documented on `policy`'s `paged` option.
 */
export function resolveArrayFunction(fragment: AbiFunction, paged = false): ResolvedArrayFunction {
  if (fragment.type !== "function") {
    throw new Error("eth_call policy abi must be a function fragment");
  }
  const input = fragment.inputs[0];
  const output = fragment.outputs[0];
  if (fragment.inputs.length !== 1 || !input?.type.endsWith("[]")) {
    throw new Error(`function ${fragment.name}: expected exactly one dynamic-array input`);
  }
  if (paged) {
    const skipped = fragment.outputs[1];
    if (fragment.outputs.length !== 2 || !output?.type.endsWith("[]")) {
      throw new Error(`function ${fragment.name}: paged lenses must return (U[] results, uint256[] skipped)`);
    }
    if (skipped?.type !== "uint256[]") {
      throw new Error(`function ${fragment.name}: paged output 1 must be uint256[], got ${skipped?.type}`);
    }
  } else if (fragment.outputs.length !== 1 || !output?.type.endsWith("[]")) {
    throw new Error(`function ${fragment.name}: expected exactly one dynamic-array output`);
  }
  return {
    selector: toFunctionSelector(fragment),
    inputLayout: layoutOf(input),
    outputLayout: layoutOf(output),
    paged,
  };
}

/*//////////////////////////////////////////////////////////////
                          HEX <-> ARRAY
//////////////////////////////////////////////////////////////*/

/**
 * Slices a single-parameter tuple encoding `(T[])` into its per-element raw byte
 * slices. `encoded` matches what `encodeAbiParameters([arrayParam], [array])` returns
 * — that is, it starts with a 32-byte offset pointer (always `0x20` for a single
 * dynamic parameter) followed by the inner `length | elements` encoding. The handler
 * calls this with both function calldata bodies (after the 4-byte selector) and raw
 * `eth_call` response hex, both of which are in this layout.
 */
export function hexToArray(layout: ElementLayout, encoded: Hex): readonly Hex[] {
  if (encoded.length < 2 + 64) {
    throw new Error("array encoding shorter than a parameter-tuple offset");
  }
  return sliceArray(layout, encoded, readUint256(encoded, 0), hexByteLength(encoded));
}

/** A paged lens's return tuple — see {@link hexToPage}. */
export type Page = {
  /** Raw element bytes for the attempted-and-served items, in input order. */
  results: readonly Hex[];
  /** Indices (into *this call's* input) the lens attempted and declined. */
  skipped: readonly number[];
};

/**
 * Slices a paged lens's `(U[] results, uint256[] skipped)` return tuple, keeping `results`
 * as raw element bytes the way {@link hexToArray} does and instantiating only `skipped`.
 *
 * Bounding `results` needs both head words: with one array the body runs to end-of-buffer, but
 * here `skipped`'s offset is where `results` stops. Reusing {@link hexToArray} would let the
 * final `U` swallow the whole `skipped` array whenever `U` is dynamic.
 */
export function hexToPage(layout: ElementLayout, encoded: Hex): Page {
  if (encoded.length < 2 + 128) {
    throw new Error("paged encoding shorter than a two-parameter head");
  }
  const totalBytes = hexByteLength(encoded);
  const resultsAt = readUint256(encoded, 0);
  const skippedAt = readUint256(encoded, 32);
  if (resultsAt >= skippedAt || skippedAt > totalBytes) {
    throw new Error("paged encoding parameter offsets out of order or out of range");
  }

  const skippedLength = readUint256(encoded, skippedAt);
  const skippedStart = 2 + (skippedAt + 32) * 2;
  if (encoded.length < skippedStart + skippedLength * 64) {
    throw new Error("paged skipped array shorter than declared length");
  }
  const skipped = new Array<number>(skippedLength);
  for (let i = 0; i < skippedLength; i++) {
    skipped[i] = readUint256(encoded, skippedAt + 32 + i * 32);
  }

  return { results: sliceArray(layout, encoded, resultsAt, skippedAt), skipped };
}

/**
 * Builds a single-parameter tuple encoding `(T[])` from pre-sliced raw element bytes.
 * Emits `[offset=0x20][length][inner tuple]`. Element bytes for a dynamic layout must
 * be the tail bytes that originally sat at each offset.
 */
export function arrayToHex(layout: ElementLayout, elements: readonly Hex[]): Hex {
  return `0x${writeUint256(32)}${encodeArrayBody(layout, elements)}` as Hex;
}

/** Inverse of {@link hexToPage}; used to build paged responses in tests and fixtures. */
export function pageToHex(layout: ElementLayout, { results, skipped }: Page): Hex {
  const resultsBody = encodeArrayBody(layout, results);
  const skippedBody = encodeArrayBody(
    { mode: "static", size: 32 },
    skipped.map((i) => `0x${writeUint256(i)}` as Hex),
  );
  const skippedAt = 64 + resultsBody.length / 2;
  return `0x${writeUint256(64)}${writeUint256(skippedAt)}${resultsBody}${skippedBody}` as Hex;
}

/*//////////////////////////////////////////////////////////////
                         ARRAY BODY CODEC
//////////////////////////////////////////////////////////////*/

/**
 * Slices the array whose length word sits at `arrayAt`, treating `regionEnd` as the end of its
 * body. Callers must pass the true end: for a dynamic layout the last element's extent is only
 * knowable from it.
 */
function sliceArray(layout: ElementLayout, encoded: Hex, arrayAt: number, regionEnd: number): readonly Hex[] {
  if (arrayAt + 32 > regionEnd) {
    throw new Error("array encoding shorter than declared length position");
  }
  const length = readUint256(encoded, arrayAt);
  const innerStartBytes = arrayAt + 32;
  const innerStartHex = 2 + innerStartBytes * 2;
  const innerBytes = regionEnd - innerStartBytes;

  if (layout.mode === "static") {
    const hexPerElement = layout.size * 2;
    if (innerBytes < length * layout.size) {
      throw new Error("static-layout array body shorter than declared length");
    }
    const out: Hex[] = new Array(length);
    for (let i = 0; i < length; i++) {
      const start = innerStartHex + i * hexPerElement;
      out[i] = `0x${encoded.slice(start, start + hexPerElement)}` as Hex;
    }
    return out;
  }

  // Dynamic layout: k offsets (32 bytes each) inside the inner tuple, each measured from
  // the start of the inner tuple. Use consecutive offsets (or end-of-region for the last)
  // to derive element byte ranges.
  if (innerBytes < length * 32) {
    throw new Error("dynamic-layout array body shorter than offset table");
  }
  const offsets = new Array<number>(length);
  for (let i = 0; i < length; i++) {
    const off = Number.parseInt(encoded.slice(innerStartHex + i * 64, innerStartHex + (i + 1) * 64), 16);
    if (!Number.isSafeInteger(off)) {
      throw new Error("dynamic-layout offset exceeds safe integer range");
    }
    offsets[i] = off;
  }
  const out: Hex[] = new Array(length);
  for (let i = 0; i < length; i++) {
    const startByte = offsets[i]!;
    const endByte = i + 1 < length ? offsets[i + 1]! : innerBytes;
    if (startByte > endByte || endByte > innerBytes) {
      throw new Error("dynamic-layout offsets out of order or out of range");
    }
    out[i] = `0x${encoded.slice(innerStartHex + startByte * 2, innerStartHex + endByte * 2)}` as Hex;
  }
  return out;
}

/** Emits `[length][inner tuple]` — the body {@link sliceArray} reads, without a leading offset. */
function encodeArrayBody(layout: ElementLayout, elements: readonly Hex[]): string {
  const lengthWord = writeUint256(elements.length);
  const body = elements.map((e) => e.slice(2)).join("");

  if (layout.mode === "static") {
    return `${lengthWord}${body}`;
  }

  // Dynamic inner type: write an offset table that points at each element's position
  // within the inner tuple (counted from the start of the offset table itself).
  let cursor = elements.length * 32;
  const head = elements
    .map((el) => {
      const word = writeUint256(cursor);
      cursor += hexByteLength(el);
      return word;
    })
    .join("");
  return `${lengthWord}${head}${body}`;
}

function hexByteLength(hex: Hex): number {
  return (hex.length - 2) / 2;
}

/*//////////////////////////////////////////////////////////////
                        CALLDATA <-> ARRAY
//////////////////////////////////////////////////////////////*/

/**
 * Verifies the selector prefix of `targetData` against `resolved` and slices the input
 * array into per-element raw bytes.
 */
export function calldataToArray(resolved: ResolvedArrayFunction, calldata: Hex): readonly Hex[] {
  if (calldata.length < 10) {
    throw new Error("eth_call target calldata shorter than a function selector");
  }
  const givenSelector = calldata.slice(0, 10).toLowerCase();
  if (givenSelector !== resolved.selector.toLowerCase()) {
    throw new Error(`eth_call selector ${givenSelector} does not match policy abi selector ${resolved.selector}`);
  }
  return hexToArray(resolved.inputLayout, `0x${calldata.slice(10)}` as Hex);
}

/**
 * Builds a full target calldata payload from a selector and an array of pre-sliced
 * raw element bytes.
 */
export function arrayToCalldata(resolved: ResolvedArrayFunction, inputElements: readonly Hex[]): Hex {
  const arrayEncoding = arrayToHex(resolved.inputLayout, inputElements);
  return `${resolved.selector}${arrayEncoding.slice(2)}` as Hex;
}

/*//////////////////////////////////////////////////////////////
                            PRIVATE
//////////////////////////////////////////////////////////////*/

/** Reads a 32-byte big-endian unsigned integer from `hex` at byte offset `byteOffset`. */
function readUint256(hex: string, byteOffset: number): number {
  const start = 2 + byteOffset * 2;
  // Upper 31 bytes must be zero for length/offset fields — Number is sufficient for realistic sizes.
  const value = hex.slice(start, start + 64);
  const n = Number.parseInt(value, 16);
  if (!Number.isSafeInteger(n)) {
    throw new Error(`array length or offset exceeds safe integer range`);
  }
  return n;
}

function writeUint256(n: number): string {
  return n.toString(16).padStart(64, "0");
}

/**
 * Computes the static ABI size of a type, or returns `null` if the type is dynamic.
 * Recurses through tuple components and fixed-size arrays.
 */
function staticSizeOf(param: AbiParameter): number | null {
  const type = param.type;

  if (type === "bytes" || type === "string") return null;
  if (/^u?int\d*$/.test(type) || type === "bool" || type === "address") return 32;
  const bytesMatch = /^bytes(\d+)$/.exec(type);
  if (bytesMatch) {
    const n = Number(bytesMatch[1]);
    return n >= 1 && n <= 32 ? 32 : null;
  }

  // Fixed-size array `T[K]` — element type is the prefix, K is the size
  const fixedArrayMatch = /^(.+)\[(\d+)\]$/.exec(type);
  if (fixedArrayMatch) {
    const innerSize = staticSizeOf({ ...param, type: fixedArrayMatch[1]! } as AbiParameter);
    return innerSize === null ? null : innerSize * Number(fixedArrayMatch[2]!);
  }

  // Dynamic array `T[]`
  if (type.endsWith("[]")) return null;

  // Tuple — static iff all components are static
  if (type === "tuple") {
    const components = (param as AbiParameter & { components?: readonly AbiParameter[] }).components;
    if (!components) return null;
    let total = 0;
    for (const c of components) {
      const s = staticSizeOf(c);
      if (s === null) return null;
      total += s;
    }
    return total;
  }

  return null;
}

/** Derives the element layout for a dynamic-array parameter `T[]`. */
function layoutOf(arrayParam: AbiParameter): ElementLayout {
  const element: AbiParameter = { ...arrayParam, type: arrayParam.type.slice(0, -2) };
  const size = staticSizeOf(element);
  return size === null ? { mode: "dynamic" } : { mode: "static", size };
}
