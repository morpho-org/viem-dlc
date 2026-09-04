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
  if (item.stateMutability !== "view" && item.stateMutability !== "pure") {
    throw new Error(`arrayifiedAbi: ${item.name} must be view or pure`);
  }
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

export type ResolvedArrayFunction = {
  /** 4-byte selector of the array-shaped fragment — what the caller's calldata is encoded with. */
  selector: Hex;
  /** 4-byte selector of the per-item function the envelope calls, see {@link itemFragmentOf}. */
  itemSelector: Hex;
  /** Element layout for the input array. */
  inputLayout: ElementLayout;
  /** Element layout for the result array `U[]`. */
  outputLayout: ElementLayout;
};

/**
 * Validates that `fragment` is a paginated lens's array-shaped fragment — one dynamic-array
 * input, returning `(U[] results, uint256[] skipped)` — and resolves the element layouts and the
 * per-item selector.
 */
export function resolveArrayFunction(fragment: AbiFunction): ResolvedArrayFunction {
  if (fragment.type !== "function") {
    throw new Error("eth_call policy abi must be a function fragment");
  }
  if (fragment.stateMutability !== "view" && fragment.stateMutability !== "pure") {
    throw new Error(`function ${fragment.name}: a lens is called with STATICCALL, so it must be view or pure`);
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
  return {
    selector: toFunctionSelector(fragment),
    itemSelector: toFunctionSelector(itemFragmentOf(fragment)),
    inputLayout: layoutOf(input),
    outputLayout: layoutOf(output),
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

/** The envelope's gas telemetry for one page, ahead of its records — see {@link hexToPage}. */
export type PageGas = {
  /** What the loop could spend on attempts: the frame's gas at the loop's start, less the reserve every admission keeps. */
  budget: bigint;
  /** Over the per-attempt gas of every record but a death: the sum, the sum of squares, the maximum. */
  sum: bigint;
  sumSquares: bigint;
  max: bigint;
};

/** A page: what one envelope call adjudicated, in the order it was attempted — see {@link hexToPage}. */
export type Page = {
  /** Raw element bytes for the attempted-and-served items, in input order. */
  results: readonly Hex[];
  /** Indices (into *this call's* input) the per-item call reverted on. */
  skipped: readonly number[];
  /**
   * Index (into *this call's* input) gas could not resolve — the page's last adjudicated element,
   * carried on the wire as `~index` in the stream's last record and never surfaced past the client.
   */
  died?: number;
  gas: PageGas;
};

/** `nA` and the four {@link PageGas} words. */
const PAGE_HEADER_BYTES = 160;
const UINT256_MAX = (1n << 256n) - 1n;
const SUCCESS_BIT = 1n << 255n;

/**
 * Decodes the envelope's outcome stream: `nA`, the four gas words, then one record per adjudicated
 * element in attempt order — success `(1 << 255) | L ‖ L bytes of raw U`, decline `i`, death `~i`
 * (last only). Every record is bound to its ordinal, the payload must be consumed exactly, and the
 * gas words must be consistent with each other, so anything this accepts is a well-formed page; it
 * is the protocol boundary for responses.
 */
export function hexToPage(layout: ElementLayout, encoded: Hex): Page {
  const totalBytes = hexByteLength(encoded);
  if (totalBytes < PAGE_HEADER_BYTES) throw new Error("page shorter than its header");
  const attempted = readUint256(encoded, 0);
  if (attempted < 1) throw new Error("page adjudicated no elements");
  if (attempted > (totalBytes - PAGE_HEADER_BYTES) / 32) {
    throw new Error(`page claims ${attempted} records in ${totalBytes} bytes`);
  }
  const gas: PageGas = {
    budget: readWord(encoded, 32),
    sum: readWord(encoded, 64),
    sumSquares: readWord(encoded, 96),
    max: readWord(encoded, 128),
  };
  const results: Hex[] = [];
  const skipped: number[] = [];
  let died: number | undefined;
  let at = PAGE_HEADER_BYTES;
  for (let j = 0; j < attempted; j++) {
    if (at + 32 > totalBytes) throw new Error(`page record ${j} is missing`);
    const word = readWord(encoded, at);
    at += 32;
    switch (word >> 254n) {
      case 0n:
        if (word !== BigInt(j)) throw new Error(`page record ${j} declines element ${word}`);
        skipped.push(j);
        break;
      case 3n:
        if ((word ^ UINT256_MAX) !== BigInt(j) || j !== attempted - 1) {
          throw new Error(`page record ${j} of ${attempted} reports a gas death at ${word ^ UINT256_MAX}`);
        }
        died = j;
        break;
      case 2n: {
        const length = word ^ SUCCESS_BIT;
        const fits = layout.mode === "static" ? length === BigInt(layout.size) : length >= 32n && length % 32n === 0n;
        if (!fits) throw new Error(`page record ${j} carries a ${length}-byte result, which does not fit the layout`);
        const end = at + Number(length);
        if (end > totalBytes) throw new Error(`page record ${j} runs past the payload`);
        results.push(`0x${encoded.slice(2 + at * 2, 2 + end * 2)}` as Hex);
        at = end;
        break;
      }
      default:
        throw new Error(`page record ${j} is neither a success, a decline nor a death`);
    }
  }
  if (at !== totalBytes) throw new Error("page has trailing bytes");
  checkPageGas(gas, BigInt(attempted - (died === undefined ? 0 : 1)));
  return died === undefined ? { results, skipped, gas } : { results, skipped, died, gas };
}

/**
 * The relations any sum, sum of squares and maximum of `served` non-negative samples satisfy:
 * `sum² ≤ served·sumSquares` (Cauchy–Schwarz) and `max ≤ sum`, `sumSquares ≤ sum·max`. The sum may
 * exceed the budget: the last attempt admitted can spend into the reserve.
 */
function checkPageGas({ sum, sumSquares, max }: PageGas, served: bigint): void {
  const consistent =
    served === 0n
      ? sum === 0n && sumSquares === 0n && max === 0n
      : sum > 0n && max <= sum && sum * sum <= served * sumSquares && sumSquares <= sum * max;
  if (!consistent) throw new Error("page gas telemetry is inconsistent");
}

/** Inverse of {@link hexToPage}; builds envelope responses in tests and mocks. */
export function pageToWire({ results, skipped, died, gas }: Page): Hex {
  const attempted = results.length + skipped.length + (died === undefined ? 0 : 1);
  const declined = new Set(skipped);
  if (declined.size !== skipped.length || skipped.some((i) => i >= attempted || i === died)) {
    throw new Error("page skips an index it did not attempt");
  }
  if (died !== undefined && died !== attempted - 1) throw new Error("page death is not its last record");
  let out =
    writeUint256(attempted) +
    writeWord(gas.budget) +
    writeWord(gas.sum) +
    writeWord(gas.sumSquares) +
    writeWord(gas.max);
  for (let j = 0, served = 0; j < attempted; j++) {
    if (j === died) out += writeWord(BigInt(j) ^ UINT256_MAX);
    else if (declined.has(j)) out += writeUint256(j);
    else {
      const result = results[served++]!;
      out += writeWord(SUCCESS_BIT | BigInt(hexByteLength(result))) + result.slice(2);
    }
  }
  return `0x${out}` as Hex;
}

/**
 * Builds a single-parameter tuple encoding `(T[])` from pre-sliced raw element bytes.
 * Emits `[offset=0x20][length][inner tuple]`. Element bytes for a dynamic layout must
 * be the tail bytes that originally sat at each offset.
 */
export function arrayToHex(layout: ElementLayout, elements: readonly Hex[]): Hex {
  return `0x${writeUint256(32)}${encodeArrayBody(layout, elements)}` as Hex;
}

/** Encodes the caller-facing `(U[] results, uint256[] skipped)` ABI tuple; a death never reaches it. */
export function pageToHex(layout: ElementLayout, { results, skipped }: Pick<Page, "results" | "skipped">): Hex {
  const resultsBody = encodeArrayBody(layout, results);
  const skippedWords = skipped.map((i) => `0x${writeUint256(i)}` as Hex);
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
                     CALLDATA / WIRE <-> ARRAY
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
 * The envelope's input wire, `n ‖ bodyLen ‖ body`: the body is `n` strides for a static layout
 * (byte-identical to the ABI array body) or `n` records `L ‖ E` for a dynamic one, `E` the padded
 * ABI tail {@link hexToArray} yields. Compression, when used, applies to the body alone.
 */
export function arrayToWire(layout: ElementLayout, elements: readonly Hex[]): Hex {
  const body = elements
    .map((e) => (layout.mode === "static" ? e.slice(2) : writeUint256(hexByteLength(e)) + e.slice(2)))
    .join("");
  return `0x${writeUint256(elements.length)}${writeUint256(body.length / 2)}${body}` as Hex;
}

/** Inverse of {@link arrayToWire}, with the envelope's own checks; for tests and mocks. */
export function wireToArray(layout: ElementLayout, wire: Hex): readonly Hex[] {
  const totalBytes = hexByteLength(wire);
  if (totalBytes < 64) throw new Error("wire shorter than its header");
  const n = readUint256(wire, 0);
  if (64 + readUint256(wire, 32) !== totalBytes) throw new Error("wire body length does not match the payload");
  const out: Hex[] = new Array(n);
  let at = 64;
  for (let i = 0; i < n; i++) {
    let length: number;
    if (layout.mode === "static") length = layout.size;
    else {
      if (at + 32 > totalBytes) throw new Error(`wire element ${i} has no length word`);
      length = readUint256(wire, at);
      at += 32;
      if (length < 32 || length % 32 !== 0) throw new Error(`wire element ${i} declares ${length} bytes`);
    }
    if (at + length > totalBytes) throw new Error(`wire element ${i} runs past the body`);
    out[i] = `0x${wire.slice(2 + at * 2, 2 + (at + length) * 2)}` as Hex;
    at += length;
  }
  if (at !== totalBytes) throw new Error("wire has trailing bytes");
  return out;
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

function readWord(hex: string, byteOffset: number): bigint {
  return BigInt(`0x${hex.slice(2 + byteOffset * 2, 2 + byteOffset * 2 + 64)}`);
}

function writeWord(n: bigint): string {
  return n.toString(16).padStart(64, "0");
}

/**
 * Computes the static ABI size of a type, or returns `null` if the type is dynamic.
 * Recurses through tuple components and fixed-size arrays.
 */
function staticSizeOf(param: AbiParameter): number | null {
  const type = param.type;

  if (type === "bytes" || type === "string") return null;
  if (/^u?int\d*$/.test(type) || type === "bool" || type === "address" || type === "function") return 32;
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
