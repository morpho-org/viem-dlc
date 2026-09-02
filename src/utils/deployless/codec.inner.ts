import type { AbiFunction, AbiParameter, Hex } from "viem";
import { toFunctionSelector } from "viem";

/**
 * The array-shaped fragment `f(T[]) returns (U[] results, uint256[] skipped)` derived from a
 * per-item lens function `f(T) returns (U)`. Types are transformed alongside values, so viem
 * decodes `results` with `U`'s field names intact.
 */
export type ArrayifiedAbi<F extends AbiFunction> = Omit<F, "inputs" | "outputs"> & {
  readonly inputs: readonly [Omit<F["inputs"][0], "type"> & { readonly type: `${F["inputs"][0]["type"]}[]` }];
  readonly outputs: readonly [
    Omit<F["outputs"][0], "type" | "name"> & {
      readonly name: "results";
      readonly type: `${F["outputs"][0]["type"]}[]`;
    },
    { readonly name: "skipped"; readonly type: "uint256[]" },
  ];
};

/**
 * Derives the array-shaped fragment the transports read a paginated lens through from the lens's
 * real per-item function. The function must take exactly one parameter and return exactly one
 * value; take it from the contract's ABI (`getAbiItem`) so the name and types are the compiler's.
 * {@link itemFragmentOf} inverts it up to the output's name, which becomes `results`.
 */
export function arrayifiedAbi<const F extends AbiFunction>(item: F): ArrayifiedAbi<F> {
  if (item.type !== "function") throw new Error("arrayifiedAbi: expected a function fragment");
  const input = item.inputs[0];
  const output = item.outputs[0];
  if (item.inputs.length !== 1 || !input) {
    throw new Error(`arrayifiedAbi: ${item.name} must take exactly one parameter`);
  }
  if (item.outputs.length !== 1 || !output) {
    throw new Error(`arrayifiedAbi: ${item.name} must return exactly one value`);
  }
  return {
    ...item,
    inputs: [{ ...input, type: `${input.type}[]` }],
    outputs: [
      { ...output, name: "results", type: `${output.type}[]` },
      { name: "skipped", type: "uint256[]" },
    ],
  } as ArrayifiedAbi<F>;
}

/**
 * Recovers the per-item fragment `f(T) returns (U)` from an array-shaped
 * `f(T[]) returns (U[] results, uint256[] skipped)` by removing the terminal `[]` from the sole
 * input and the first output; parameter names (as they stand on `fragment`) and tuple components
 * are preserved. Its selector is what the envelope calls.
 */
export function itemFragmentOf(fragment: AbiFunction): AbiFunction {
  const input = fragment.inputs[0]!;
  const output = fragment.outputs[0]!;
  return {
    ...fragment,
    inputs: [{ ...input, type: input.type.slice(0, -2) }],
    outputs: [{ ...output, type: output.type.slice(0, -2) }],
  };
}

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
 * Declared upper bounds for dynamic element types, in padded ABI tail bytes (length word plus
 * padded data, heads excluded). Facts about the lens's types, checked against every element.
 */
export type ElementBounds = {
  maxItemBytes?: number;
  maxResultBytes?: number;
};

export type ResolvedArrayFunction = {
  /** 4-byte selector of the array-shaped fragment — what the caller's calldata is encoded with. */
  selector: Hex;
  /** 4-byte selector of the per-item function the envelope calls, see {@link itemFragmentOf}. */
  itemSelector: Hex;
  /** Element layout for the input array. */
  inputLayout: ElementLayout;
  /** Element layout for the result array `U[]`. */
  outputLayout: ElementLayout;
  /** Bytes one input element occupies in calldata: its stride, or its declared bound plus offset word. */
  inputBytes: number;
  /** Bytes one result element occupies in a page: its stride, or its declared bound plus offset word. */
  outputBytes: number;
  /** Present iff `inputLayout` is dynamic. */
  maxItemBytes?: number;
  /** Present iff `outputLayout` is dynamic. */
  maxResultBytes?: number;
};

/**
 * Validates that `fragment` is a paginated lens's array-shaped fragment — one dynamic-array
 * input, returning `(U[] results, uint256[] skipped)` — and resolves the element layouts and the
 * per-item selector. A dynamic `T` or `U` must come with its bound in `bounds`; the client packs
 * and verifies against it.
 */
export function resolveArrayFunction(fragment: AbiFunction, bounds: ElementBounds = {}): ResolvedArrayFunction {
  if (fragment.type !== "function") {
    throw new Error("eth_call policy abi must be a function fragment");
  }
  const input = fragment.inputs[0];
  const output = fragment.outputs[0];
  const skipped = fragment.outputs[1];
  if (fragment.inputs.length !== 1 || !input?.type.endsWith("[]")) {
    throw new Error(`function ${fragment.name}: expected exactly one dynamic-array input`);
  }
  if (fragment.outputs.length !== 2 || !output?.type.endsWith("[]")) {
    throw new Error(`function ${fragment.name}: paginated lenses must return (U[] results, uint256[] skipped)`);
  }
  if (skipped?.type !== "uint256[]") {
    throw new Error(`function ${fragment.name}: paginated output 1 must be uint256[], got ${skipped?.type}`);
  }
  const inputLayout = layoutOf(input);
  const outputLayout = layoutOf(output);
  const requireBound = (name: keyof ElementBounds, what: string) => {
    const bound = bounds[name];
    if (!Number.isSafeInteger(bound) || bound! < 32 || bound! % 32 !== 0) {
      throw new Error(
        `function ${fragment.name}: ${what} is dynamic, so policy.${name} (a positive multiple of 32 bytes) is required`,
      );
    }
    return bound!;
  };
  const maxItemBytes = inputLayout.mode === "dynamic" ? requireBound("maxItemBytes", "the input element") : undefined;
  const maxResultBytes =
    outputLayout.mode === "dynamic" ? requireBound("maxResultBytes", "the result element") : undefined;
  return {
    selector: toFunctionSelector(fragment),
    itemSelector: toFunctionSelector(itemFragmentOf(fragment)),
    inputLayout,
    outputLayout,
    inputBytes: inputLayout.mode === "static" ? inputLayout.size : 32 + maxItemBytes!,
    outputBytes: outputLayout.mode === "static" ? outputLayout.size : 32 + maxResultBytes!,
    ...(maxItemBytes !== undefined && { maxItemBytes }),
    ...(maxResultBytes !== undefined && { maxResultBytes }),
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

/** A paginated lens's return tuple — see {@link hexToPage}. */
export type Page = {
  /** Raw element bytes for the attempted-and-served items, in input order. */
  results: readonly Hex[];
  /** Indices (into *this call's* input) declined: the per-item call reverted, or the element exceeded its bound. */
  skipped: readonly number[];
  /**
   * Index (into *this call's* input) gas could not resolve — the page's last adjudicated element,
   * carried on the wire as `~index` at the end of `skipped` and never surfaced past the client.
   */
  died?: number;
};

const UINT256_MAX = (1n << 256n) - 1n;

/**
 * Slices a paginated lens's `(U[] results, uint256[] skipped)` return tuple, keeping `results`
 * as raw element bytes the way {@link hexToArray} does and instantiating only `skipped`.
 *
 * Bounding `results` needs both head words: with one array the body runs to end-of-buffer, but
 * here `skipped`'s offset is where `results` stops. Reusing {@link hexToArray} would let the
 * final `U` swallow the whole `skipped` array whenever `U` is dynamic.
 *
 * A top-bit-set word is legal only as the last `skipped` entry and decodes to {@link Page.died}
 * as the 256-bit complement; anywhere else it is a malformed page.
 */
export function hexToPage(layout: ElementLayout, encoded: Hex): Page {
  if (encoded.length < 2 + 128) {
    throw new Error("paginated encoding shorter than a two-parameter head");
  }
  const totalBytes = hexByteLength(encoded);
  const resultsAt = readUint256(encoded, 0);
  const skippedAt = readUint256(encoded, 32);
  // `skippedAt` doubles as the end bound for `results`, so a head-relative offset would truncate it.
  if (
    resultsAt < 64 ||
    resultsAt % 32 !== 0 ||
    skippedAt % 32 !== 0 ||
    resultsAt >= skippedAt ||
    skippedAt > totalBytes
  ) {
    throw new Error("paginated encoding parameter offsets out of order or out of range");
  }

  const skippedLength = readUint256(encoded, skippedAt);
  const skippedStart = 2 + (skippedAt + 32) * 2;
  if (encoded.length < skippedStart + skippedLength * 64) {
    throw new Error("paginated skipped array shorter than declared length");
  }
  const skipped = new Array<number>(skippedLength);
  let died: number | undefined;
  for (let i = 0; i < skippedLength; i++) {
    const at = skippedAt + 32 + i * 32;
    if (i === skippedLength - 1 && isTopBitSet(encoded, at)) {
      died = toSafeNumber(BigInt(`0x${encoded.slice(2 + at * 2, 2 + at * 2 + 64)}`) ^ UINT256_MAX);
      skipped.length = i;
      break;
    }
    skipped[i] = readUint256(encoded, at);
  }

  const page: Page = { results: sliceArray(layout, encoded, resultsAt, skippedAt), skipped };
  return died === undefined ? page : { ...page, died };
}

function isTopBitSet(hex: string, byteOffset: number): boolean {
  return Number.parseInt(hex[2 + byteOffset * 2]!, 16) >= 8;
}

function toSafeNumber(n: bigint): number {
  if (n > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("tagged skipped index exceeds safe integer range");
  return Number(n);
}

/**
 * Builds a single-parameter tuple encoding `(T[])` from pre-sliced raw element bytes.
 * Emits `[offset=0x20][length][inner tuple]`. Element bytes for a dynamic layout must
 * be the tail bytes that originally sat at each offset.
 */
export function arrayToHex(layout: ElementLayout, elements: readonly Hex[]): Hex {
  return `0x${writeUint256(32)}${encodeArrayBody(layout, elements)}` as Hex;
}

/** Inverse of {@link hexToPage}; used to build paginated responses in tests and fixtures. */
export function pageToHex(layout: ElementLayout, { results, skipped, died }: Page): Hex {
  const resultsBody = encodeArrayBody(layout, results);
  const skippedWords = skipped.map((i) => `0x${writeUint256(i)}` as Hex);
  if (died !== undefined) skippedWords.push(`0x${(BigInt(died) ^ UINT256_MAX).toString(16).padStart(64, "0")}` as Hex);
  const skippedBody = encodeArrayBody({ mode: "static", size: 32 }, skippedWords);
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
  const tableBytes = length * 32;
  for (let i = 0; i < length; i++) {
    const off = Number.parseInt(encoded.slice(innerStartHex + i * 64, innerStartHex + (i + 1) * 64), 16);
    if (!Number.isSafeInteger(off)) {
      throw new Error("dynamic-layout offset exceeds safe integer range");
    }
    // Strictly increasing, since every dynamic value occupies at least a length word. Offsets
    // below `tableBytes` would slice the offset table itself, which decodes as a plausible value.
    if (off < tableBytes || off % 32 !== 0 || (i > 0 && off <= offsets[i - 1]!)) {
      throw new Error("dynamic-layout offsets out of order or out of range");
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
