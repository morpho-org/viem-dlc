/**
 * The orchestrator, and the whole API an adapter needs.
 *
 * ```ts
 * const result = await transform(code, id, { resolve, addDependency });
 * if (result) return result.code;
 * ```
 *
 * For one module it finds the lenses, and takes each through resolve, print and compile, then rewrites
 * the module and reports every file it read. It returns null for a module with no lens, after one
 * substring test and no parse when the module does not mention the package. Every failure is a
 * {@link LensError} whose diagnostics carry a TypeScript file, line and column.
 *
 * Nothing here evaluates anything or imports an application module.
 */
import fs from "node:fs";

import { compile, SolcUnavailableError } from "./compile.js";
import { ConfigError, findConfig } from "./config.js";
import { diagnosticAt, LensError } from "./errors.js";
import { find, mentionsPackage } from "./find.js";
import { parseModule } from "./module.js";
import { printSolidity } from "./print-solidity.js";
import { resolve } from "./resolve.js";
import { rewrite } from "./rewrite.js";
import { mapSolcDiagnostics } from "./solc-errors.js";
import type {
  BuiltLens,
  BuiltModule,
  ModuleLoader,
  SourceModule,
  SyncTransformHost,
  TransformHost,
  TransformOptions,
  TransformResult,
} from "./types.js";

/** Raised inside a stage when the host's asynchronous resolver has not been asked about an import yet. */
class PendingResolution extends Error {
  readonly specifier: string;
  readonly importer: string;

  constructor(specifier: string, importer: string) {
    super(`unresolved import "${specifier}" from ${importer}`);
    this.specifier = specifier;
    this.importer = importer;
  }
}

type Resolver = (specifier: string, importer: string) => string | undefined;

const buildOnce = (
  code: string,
  id: string,
  resolveImport: Resolver,
  host: Pick<TransformHost, "readFile" | "warn">,
  options: TransformOptions,
  parsed: Map<string, SourceModule>,
): BuiltModule | null => {
  if (!mentionsPackage(code)) return null;
  const readFile = host.readFile ?? ((file: string) => fs.readFileSync(file, "utf8"));
  const loader: ModuleLoader = {
    resolve: resolveImport,
    load(file) {
      let module = parsed.get(file);
      if (!module) {
        module = parseModule(file, readFile(file));
        parsed.set(file, module);
      }
      return module;
    },
  };
  let module = parsed.get(id);
  if (!module || module.code !== code) {
    module = parseModule(id, code);
    parsed.set(id, module);
  }
  const sites = find(module);
  const [firstSite] = sites;
  if (!firstSite) return null;

  const dependencies = new Set<string>();
  const lenses: BuiltLens[] = [];
  for (const site of sites) {
    const resolved = resolve(site, module, loader);
    for (const file of resolved.files) if (file !== id) dependencies.add(file);
    const printed = printSolidity(resolved);
    const refuse = (message: string): never => {
      throw new LensError([diagnosticAt(module, site.node, message, site.name)]);
    };
    let config = options.config;
    try {
      config ??= findConfig(id);
    } catch (cause) {
      if (cause instanceof ConfigError) refuse(cause.message);
      throw cause;
    }
    dependencies.add(config.path);
    let result: ReturnType<typeof compile>;
    try {
      result = compile({ contractName: resolved.name, solidity: printed.solidity }, config, { cache: options.cache });
    } catch (cause) {
      if (cause instanceof SolcUnavailableError) refuse(`the lens ${site.name} needs compiling: ${cause.message}`);
      throw cause;
    }
    for (const warning of mapSolcDiagnostics(result.warnings, printed, resolved)) host.warn?.(warning);
    if (!result.ok) throw new LensError(mapSolcDiagnostics(result.errors, printed, resolved));
    lenses.push({ resolved, printed, artifact: result.artifact });
  }
  return { module, lenses, dependencies: [...dependencies] };
};

/**
 * A module's lenses, compiled, without the rewrite: what the check command and the tests read.
 * Null when the module has no lens. Dependencies are not reported to the host; see {@link transform}.
 */
export async function build(
  code: string,
  id: string,
  host: TransformHost,
  options: TransformOptions = {},
): Promise<BuiltModule | null> {
  // The stages are synchronous and the host's resolver may not be. An import the host has not been asked
  // about raises PendingResolution; it is awaited and the build runs again, reusing every parse.
  const resolved = new Map<string, string | undefined>();
  const parsed = new Map<string, SourceModule>();
  const lookup: Resolver = (specifier, importer) => {
    const key = `${importer}\0${specifier}`;
    if (!resolved.has(key)) throw new PendingResolution(specifier, importer);
    return resolved.get(key);
  };
  for (;;) {
    try {
      return buildOnce(code, id, lookup, host, options, parsed);
    } catch (cause) {
      if (!(cause instanceof PendingResolution)) throw cause;
      resolved.set(`${cause.importer}\0${cause.specifier}`, await host.resolve(cause.specifier, cause.importer));
    }
  }
}

/** {@link build} for a host whose resolver is synchronous. */
export function buildSync(
  code: string,
  id: string,
  host: SyncTransformHost,
  options: TransformOptions = {},
): BuiltModule | null {
  return buildOnce(code, id, (specifier, importer) => host.resolve(specifier, importer), host, options, new Map());
}

const finish = (built: BuiltModule | null, host: TransformHost): TransformResult | null => {
  if (!built) return null;
  for (const dependency of built.dependencies) host.addDependency(dependency);
  return { ...built, code: rewrite(built.module, built.lenses) };
};

/**
 * Transforms one module. `id` is the module's absolute path, without a query. Returns null when the
 * module has no lens, and the host then keeps the module as it is.
 */
export async function transform(
  code: string,
  id: string,
  host: TransformHost,
  options: TransformOptions = {},
): Promise<TransformResult | null> {
  return finish(await build(code, id, host, options), host);
}

/** {@link transform} for a host whose resolver is synchronous, such as a Node `load` hook. */
export function transformSync(
  code: string,
  id: string,
  host: SyncTransformHost,
  options: TransformOptions = {},
): TransformResult | null {
  return finish(buildSync(code, id, host, options), host);
}
