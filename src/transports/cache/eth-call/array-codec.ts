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
  const element = { ...arrayParam, type: arrayParam.type.slice(0, -2) } as AbiParameter;
  const size = staticSizeOf(element);
  return size === null ? { mode: "dynamic" } : { mode: "static", size };
}

export type ResolvedArrayFunction = {
  /** 4-byte function selector computed from the original fragment. */
  selector: Hex;
  /** Element layout for the input array. */
  inputLayout: ElementLayout;
  /** Element layout for the output array. */
  outputLayout: ElementLayout;
};

/**
 * Validates that `fragment` is a single-input, single-output function whose input and
 * output are both dynamic arrays, and resolves the element layout for each.
 */
export function resolveArrayFunction(fragment: AbiFunction): ResolvedArrayFunction {
  if (fragment.type !== "function") {
    throw new Error("[cache] eth_call policy abi must be a function fragment");
  }
  const input = fragment.inputs[0];
  const output = fragment.outputs[0];
  if (fragment.inputs.length !== 1 || !input?.type.endsWith("[]")) {
    throw new Error(`[cache] function ${fragment.name}: expected exactly one dynamic-array input`);
  }
  if (fragment.outputs.length !== 1 || !output?.type.endsWith("[]")) {
    throw new Error(`[cache] function ${fragment.name}: expected exactly one dynamic-array output`);
  }
  return {
    selector: toFunctionSelector(fragment),
    inputLayout: layoutOf(input),
    outputLayout: layoutOf(output),
  };
}

/*//////////////////////////////////////////////////////////////
                    HEX-LEVEL SLICE / BUILD
//////////////////////////////////////////////////////////////*/

/** Reads a 32-byte big-endian unsigned integer from `hex` at byte offset `byteOffset`. */
function readUint256(hex: string, byteOffset: number): number {
  const start = 2 + byteOffset * 2;
  // Upper 31 bytes must be zero for length/offset fields — Number is sufficient for realistic sizes.
  const value = hex.slice(start, start + 64);
  const n = Number.parseInt(value, 16);
  if (!Number.isSafeInteger(n)) {
    throw new Error(`[cache] array length or offset exceeds safe integer range`);
  }
  return n;
}

function writeUint256(n: number): string {
  return n.toString(16).padStart(64, "0");
}

/**
 * Slices a single-parameter tuple encoding `(T[])` into its per-element raw byte
 * slices. `encoded` matches what `encodeAbiParameters([arrayParam], [array])` returns
 * — that is, it starts with a 32-byte offset pointer (always `0x20` for a single
 * dynamic parameter) followed by the inner `length | elements` encoding. The handler
 * calls this with both function calldata bodies (after the 4-byte selector) and raw
 * `eth_call` response hex, both of which are in this layout.
 */
export function sliceArrayElements(layout: ElementLayout, encoded: Hex): readonly Hex[] {
  if (encoded.length < 2 + 64) {
    throw new Error("[cache] array encoding shorter than a parameter-tuple offset");
  }
  const arrayOffsetBytes = readUint256(encoded, 0);
  if (encoded.length < 2 + (arrayOffsetBytes + 32) * 2) {
    throw new Error("[cache] array encoding shorter than declared length position");
  }
  const length = readUint256(encoded, arrayOffsetBytes);
  const innerStartBytes = arrayOffsetBytes + 32;
  const innerStartHex = 2 + innerStartBytes * 2;
  const innerBytes = (encoded.length - innerStartHex) / 2;

  if (layout.mode === "static") {
    const hexPerElement = layout.size * 2;
    if (innerBytes < length * layout.size) {
      throw new Error("[cache] static-layout array body shorter than declared length");
    }
    const out: Hex[] = new Array(length);
    for (let i = 0; i < length; i++) {
      const start = innerStartHex + i * hexPerElement;
      out[i] = `0x${encoded.slice(start, start + hexPerElement)}` as Hex;
    }
    return out;
  }

  // Dynamic layout: k offsets (32 bytes each) inside the inner tuple, each measured from
  // the start of the inner tuple. Use consecutive offsets (or end-of-buffer for the last)
  // to derive element byte ranges.
  if (innerBytes < length * 32) {
    throw new Error("[cache] dynamic-layout array body shorter than offset table");
  }
  const offsets = new Array<number>(length);
  for (let i = 0; i < length; i++) {
    const off = Number.parseInt(encoded.slice(innerStartHex + i * 64, innerStartHex + (i + 1) * 64), 16);
    if (!Number.isSafeInteger(off)) {
      throw new Error("[cache] dynamic-layout offset exceeds safe integer range");
    }
    offsets[i] = off;
  }
  const out: Hex[] = new Array(length);
  for (let i = 0; i < length; i++) {
    const startByte = offsets[i]!;
    const endByte = i + 1 < length ? offsets[i + 1]! : innerBytes;
    if (startByte > endByte || endByte > innerBytes) {
      throw new Error("[cache] dynamic-layout offsets out of order or out of range");
    }
    out[i] = `0x${encoded.slice(innerStartHex + startByte * 2, innerStartHex + endByte * 2)}` as Hex;
  }
  return out;
}

/**
 * Builds a single-parameter tuple encoding `(T[])` from pre-sliced raw element bytes.
 * Emits `[offset=0x20][length][inner tuple]`. Element bytes for a dynamic layout must
 * be the tail bytes that originally sat at each offset.
 */
export function buildArrayEncoding(layout: ElementLayout, elements: readonly Hex[]): Hex {
  const offsetWord = writeUint256(32);
  const lengthWord = writeUint256(elements.length);
  const body = elements.map((e) => e.slice(2)).join("");

  if (layout.mode === "static") {
    return `0x${offsetWord}${lengthWord}${body}` as Hex;
  }

  // Dynamic inner type: write an offset table that points at each element's position
  // within the inner tuple (counted from the start of the offset table itself).
  let cursor = elements.length * 32;
  const head = elements
    .map((el) => {
      const word = writeUint256(cursor);
      cursor += (el.length - 2) / 2;
      return word;
    })
    .join("");
  return `0x${offsetWord}${lengthWord}${head}${body}` as Hex;
}

/**
 * Builds a full target calldata payload from a selector and an array of pre-sliced
 * raw element bytes.
 */
export function buildTargetCalldata(resolved: ResolvedArrayFunction, inputElements: readonly Hex[]): Hex {
  const arrayEncoding = buildArrayEncoding(resolved.inputLayout, inputElements);
  return `${resolved.selector}${arrayEncoding.slice(2)}` as Hex;
}

/**
 * Verifies the selector prefix of `targetData` against `resolved` and slices the input
 * array into per-element raw bytes.
 */
export function sliceInputArray(resolved: ResolvedArrayFunction, targetData: Hex): readonly Hex[] {
  if (targetData.length < 10) {
    throw new Error("[cache] eth_call target calldata shorter than a function selector");
  }
  const givenSelector = targetData.slice(0, 10).toLowerCase();
  if (givenSelector !== resolved.selector.toLowerCase()) {
    throw new Error(
      `[cache] eth_call selector ${givenSelector} does not match policy abi selector ${resolved.selector}`,
    );
  }
  return sliceArrayElements(resolved.inputLayout, `0x${targetData.slice(10)}` as Hex);
}
