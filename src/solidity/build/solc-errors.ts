/**
 * solc's errors, pointed at the TypeScript.
 *
 * solc is the type checker of a lens, and it reports a range of the printed Solidity. The innermost
 * printed node covering that range gives the TypeScript file, line and column. An error solc gives
 * without a location is reported against the lens.
 */
import { positionOf } from "./errors.js";
import { lookupPosition } from "./position-map.js";
import type { LensDiagnostic, PrintedLens, ResolvedLens, SolcDiagnostic } from "./types.js";

/** solc counts UTF-8 bytes and the position map counts UTF-16 code units. */
const toStringOffset = (solidity: string, bytes: Buffer | undefined, byteOffset: number): number =>
  bytes ? bytes.subarray(0, byteOffset).toString("utf8").length : Math.min(byteOffset, solidity.length);

export function mapSolcDiagnostics(
  diagnostics: readonly SolcDiagnostic[],
  printed: PrintedLens,
  lens: ResolvedLens,
): LensDiagnostic[] {
  const { solidity, map } = printed;
  const bytes = Buffer.byteLength(solidity, "utf8") === solidity.length ? undefined : Buffer.from(solidity, "utf8");
  const atLens = positionOf(lens.module, lens.site.node.start);
  return diagnostics.map((diagnostic) => {
    const entry =
      diagnostic.range &&
      lookupPosition(
        map,
        toStringOffset(solidity, bytes, diagnostic.range.start),
        toStringOffset(solidity, bytes, diagnostic.range.end),
      );
    const position = entry ? { file: entry.file, line: entry.line, column: entry.column } : atLens;
    return {
      ...position,
      severity: diagnostic.severity === "error" ? "error" : "warning",
      source: "solc",
      message: diagnostic.message,
      lens: lens.name,
      ...(diagnostic.errorCode === undefined ? {} : { code: diagnostic.errorCode }),
    };
  });
}
