/**
 * Compile: printed Solidity to an ABI and bytecode, with solc, behind a content-addressed disk cache.
 *
 * solc is an optional peer dependency and is loaded only on a cache miss, so a warm cache needs no
 * compiler. Nothing here runs in production: the transform has replaced every lens with its bytecode
 * before production code is built.
 */
import { createHash } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";

import { requireStaticCall } from "./config.js";
import type { CompiledArtifact, CompileInput, CompileResult, SolcDiagnostic, SolidityConfig } from "./types.js";

/** Bump when the cache entry's shape or the compiler input's shape changes. */
const CACHE_FORMAT = "viem-dlc-solidity-1";

/** Thrown when a lens needs compiling and solc is not installed, or is not the version the repository pins. */
export class SolcUnavailableError extends Error {
  override readonly name = "SolcUnavailableError";
}

interface Solc {
  version(): string;
  compile(input: string): string;
}

let loaded: Solc | undefined;

/** Runs solc on a standard-JSON input and returns its parsed output. Uncached. */
export function runSolc(input: unknown, version: string): Record<string, unknown> {
  if (!loaded) {
    try {
      loaded = createRequire(import.meta.url)("solc") as Solc;
    } catch (cause) {
      /* v8 ignore next -- @preserve: solc is installed wherever the tests run */
      throw new SolcUnavailableError(
        `solc is not installed (${(cause as { code?: string }).code ?? (cause as Error).message}). It is an optional peer dependency of @morpho-org/viem-dlc, needed wherever lenses are built or tested: add solc@${version} as a devDependency.`,
      );
    }
  }
  if (!loaded.version().startsWith(`${version}+`))
    throw new SolcUnavailableError(`the repository pins solc ${version} and solc ${loaded.version()} is installed`);
  return JSON.parse(loaded.compile(JSON.stringify(input))) as Record<string, unknown>;
}

interface RawSolcError {
  severity: SolcDiagnostic["severity"];
  type: string;
  message: string;
  formattedMessage?: string;
  errorCode?: string;
  sourceLocation?: { start: number; end: number };
}

/** solc's `errors` output as {@link SolcDiagnostic}s. */
export const diagnosticsOf = (output: Record<string, unknown>): SolcDiagnostic[] =>
  ((output.errors ?? []) as RawSolcError[]).map((error) => ({
    severity: error.severity,
    type: error.type,
    message: error.message,
    formattedMessage: error.formattedMessage ?? error.message,
    ...(error.errorCode === undefined ? {} : { errorCode: error.errorCode }),
    ...(error.sourceLocation && error.sourceLocation.start >= 0
      ? { range: { start: error.sourceLocation.start, end: error.sourceLocation.end } }
      : {}),
  }));

interface CacheEntry {
  artifact: CompiledArtifact;
  warnings: SolcDiagnostic[];
}

export function compile(input: CompileInput, config: SolidityConfig, options: { cache?: boolean } = {}): CompileResult {
  const { contractName, solidity } = input;
  const { version, settings } = config.solc;
  // Before the cache is read: an entry compiled for such a target is refused too.
  requireStaticCall(settings, config.path);
  const useCache = options.cache !== false;
  const key = createHash("sha256")
    .update(JSON.stringify([CACHE_FORMAT, version, settings, contractName, solidity]))
    .digest("hex");
  const cacheFile = path.join(config.cacheDir, `${key}.json`);
  if (useCache) {
    try {
      const hit = JSON.parse(fs.readFileSync(cacheFile, "utf8")) as CacheEntry;
      return { ok: true, artifact: hit.artifact, warnings: hit.warnings, cached: true };
    } catch {
      // A miss, or an entry that cannot be read: compile.
    }
  }

  const source = `${contractName}.sol`;
  const output = runSolc(
    {
      language: "Solidity",
      sources: { [source]: { content: solidity } },
      settings: { ...settings, outputSelection: { "*": { "*": ["abi", "evm.bytecode.object"] } } },
    },
    version,
  );
  const diagnostics = diagnosticsOf(output);
  const errors = diagnostics.filter((diagnostic) => diagnostic.severity === "error");
  const warnings = diagnostics.filter((diagnostic) => diagnostic.severity !== "error");
  if (errors.length) return { ok: false, errors, warnings };

  const contracts = output.contracts as Record<
    string,
    Record<string, { abi: CompiledArtifact["abi"]; evm: { bytecode: { object: string } } }>
  >;
  const compiled = contracts[source]?.[contractName];
  if (!compiled) throw new Error(`solc compiled ${source} without a contract named ${contractName}`);
  const artifact: CompiledArtifact = { abi: compiled.abi, bytecode: `0x${compiled.evm.bytecode.object}` };

  if (useCache) {
    fs.mkdirSync(config.cacheDir, { recursive: true });
    // Written beside the entry and renamed, so a concurrent build never reads half an entry.
    const temporary = `${cacheFile}.${process.pid}.tmp`;
    fs.writeFileSync(temporary, JSON.stringify({ artifact, warnings } satisfies CacheEntry));
    fs.renameSync(temporary, cacheFile);
  }
  return { ok: true, artifact, warnings, cached: false };
}
