/**
 * Located errors. Everything the transform refuses, and everything solc refuses, reaches the author as
 * a file, a line and a column of TypeScript.
 */
import type { Span } from "oxc-parser";

import type { LensDiagnostic, SourceModule, SourcePosition } from "./types.js";

/**
 * What ends a line in ECMAScript: a line feed, a carriage return, the two together as one, U+2028 and
 * U+2029. Every line the build counts, it counts with this.
 */
export const LINE_TERMINATOR = /\r\n|[\n\r\u2028\u2029]/g;

/** The 1-based line and column of an offset into a module's text. */
export const positionOf = (module: SourceModule, offset: number): SourcePosition => {
  const { lineStarts } = module;
  let low = 0;
  let high = lineStarts.length - 1;
  while (low < high) {
    const mid = (low + high + 1) >> 1;
    /* v8 ignore next -- @preserve: the index is in range, and the fallback after ?? is there for the type checker */
    if ((lineStarts[mid] ?? 0) <= offset) low = mid;
    else high = mid - 1;
  }
  /* v8 ignore next -- @preserve: the index is in range, and the fallback after ?? is there for the type checker */
  return { file: module.file, line: low + 1, column: offset - (lineStarts[low] ?? 0) + 1 };
};

/** `lens.ts:41:17 - error: Type uint256 is not implicitly convertible to expected type uint128.` */
export const formatDiagnostic = (diagnostic: LensDiagnostic): string =>
  `${diagnostic.file}:${diagnostic.line}:${diagnostic.column} - ${diagnostic.severity}: ${diagnostic.message}`;

/** Thrown by every stage. An adapter reports `diagnostics` through its host's own error channel. */
export class LensError extends Error {
  override readonly name = "LensError";
  readonly diagnostics: readonly LensDiagnostic[];

  constructor(diagnostics: readonly LensDiagnostic[]) {
    super(diagnostics.map(formatDiagnostic).join("\n"));
    this.diagnostics = diagnostics;
  }
}

/** Builds a transform diagnostic at a node. */
export const diagnosticAt = (module: SourceModule, node: Span, message: string, lens?: string): LensDiagnostic => ({
  ...positionOf(module, node.start),
  severity: "error",
  source: "transform",
  message,
  ...(lens === undefined ? {} : { lens }),
});

/** Why a Number literal that is not a safe integer is refused, in a body and in an assembly block. */
export const UNSAFE_NUMBER =
  "this Number literal is not a safe integer, so JavaScript does not hold the value it spells and a compiler may print another. Write an integer literal as a bigint, with the n suffix";

/** Stops the build with one error at a node. */
export function fail(module: SourceModule, node: Span, message: string, lens?: string): never {
  throw new LensError([diagnosticAt(module, node, message, lens)]);
}

/**
 * Stops the build at a construct the design defines but this version does not print yet. Distinct from
 * {@link fail}, which is for what a lens may never contain: search for `notImplemented(` to find the
 * work that is left.
 */
/* v8 ignore next -- @preserve: nothing a lens can hold is left unimplemented today, so nothing calls this */
export function notImplemented(module: SourceModule, node: Span, what: string): never {
  return fail(module, node, `${what} is not implemented yet in @morpho-org/viem-dlc/solidity.`);
}
