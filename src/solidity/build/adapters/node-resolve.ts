/**
 * The resolver of the two adapters that run in plain Node: the import hook and the check command.
 *
 * An import names the file this Node process would load for it: Node's own resolution is asked first,
 * through every resolve hook registered before this module was loaded, under the conditions the process
 * runs with. So a lens reads the module the application imports, whatever `--conditions` and package
 * `exports` select, and an explicit `./unit.js` is that file even beside a `unit.ts`.
 *
 * Only an import Node cannot resolve falls back to TypeScript's conventions: an extensionless path, a
 * `.js` specifier that names a `.ts` file, and the `paths` aliases of the nearest `tsconfig.json`.
 * Such an import is one the rewrite removes, or the script could not run. The configuration files the
 * aliases come from are inputs of the result, so each is reported as a dependency.
 *
 * `oxc-resolver` is an optional peer dependency, needed only by these two adapters.
 */
import fs from "node:fs";
import { createRequire, registerHooks } from "node:module";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { ResolverFactory } from "oxc-resolver";

/**
 * Node resolves an import against a parent only while importing. `import.meta.resolve` runs the same
 * resolution, hooks and conditions included, but always against this module, so the hook below swaps
 * the one specifier it is called with for the import that is being asked about.
 */
const REQUEST = "viem-dlc-solidity:resolve";
let pending: { specifier: string; parentURL: string } | undefined;
/** The conditions Node resolves an `import` with in this process, as its resolver last reported them. */
let conditions: readonly string[] = ["node", "import"];

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier !== REQUEST || !pending) return nextResolve(specifier, context);
    const request = pending;
    pending = undefined;
    conditions = context.conditions;
    return nextResolve(request.specifier, { ...context, parentURL: request.parentURL });
  },
});

const isFile = (file: string): boolean => fs.statSync(file, { throwIfNoEntry: false })?.isFile() === true;

/** The URL Node resolves the import to, or undefined when Node cannot load a module for it. */
const nodeResolve = (specifier: string, importer: string): string | undefined => {
  pending = { specifier, parentURL: pathToFileURL(importer).href };
  let url: string;
  try {
    url = import.meta.resolve(REQUEST);
  } catch {
    return undefined;
  } finally {
    pending = undefined;
  }
  // A relative specifier resolves to a URL whether or not a file is there.
  return url.startsWith("file:") && !isFile(fileURLToPath(url)) ? undefined : url;
};

const nearestTsconfig = (from: string): string | undefined => {
  for (let dir = from; ; dir = path.dirname(dir)) {
    const candidate = path.join(dir, "tsconfig.json");
    if (fs.existsSync(candidate)) return candidate;
    if (dir === path.dirname(dir)) return undefined;
  }
};

/** A tsconfig's text as JSON: without comments and trailing commas. Undefined when it is neither. */
const parseTsconfig = (text: string): { extends?: unknown; references?: unknown } | undefined => {
  const strings = String.raw`"(?:\\.|[^"\\])*"`;
  try {
    return JSON.parse(
      text
        .replace(new RegExp(`(${strings})|//[^\\n]*|/\\*[^]*?\\*/`, "g"), (_, string?: string) => string ?? "")
        .replace(new RegExp(`(${strings})|,(?=\\s*[}\\]])`, "g"), (_, string?: string) => string ?? ""),
    );
  } catch {
    return undefined;
  }
};

/**
 * Every file a tsconfig's aliases are read from, with its text: the file, what it extends, and the
 * projects it references. A file that cannot be read or parsed ends its branch; the resolver reports it.
 */
const tsconfigInputs = (file: string, inputs = new Map<string, string>()): Map<string, string> => {
  if (inputs.has(file) || !isFile(file)) return inputs;
  const text = fs.readFileSync(file, "utf8");
  inputs.set(file, text);
  const config = parseTsconfig(text);
  const dir = path.dirname(file);
  for (const base of [config?.extends].flat()) {
    if (typeof base !== "string") continue;
    const candidates = [base, `${base}.json`, `${base}/tsconfig.json`];
    const found = /^\.{0,2}\//.test(base)
      ? candidates.map((candidate) => path.resolve(dir, candidate)).find(isFile)
      : candidates.flatMap((candidate) => {
          try {
            return [createRequire(file).resolve(candidate)];
          } catch {
            return [];
          }
        })[0];
    if (found) tsconfigInputs(found, inputs);
  }
  for (const reference of Array.isArray(config?.references) ? config.references : []) {
    const target = (reference as { path?: unknown } | null)?.path;
    if (typeof target !== "string") continue;
    const resolved = path.resolve(dir, target);
    tsconfigInputs(isFile(resolved) ? resolved : path.join(resolved, "tsconfig.json"), inputs);
  }
  return inputs;
};

/** One resolver per tsconfig, with what it was made from: it reads its configuration once. */
const resolvers = new Map<string | undefined, { key: string; resolver: ResolverFactory }>();

/**
 * The absolute path `specifier` names when imported from the file `importer`. `addDependency` receives
 * every configuration file the answer was read from, whether or not the import resolved.
 */
export function resolveSync(
  specifier: string,
  importer: string,
  addDependency: (file: string) => void = () => {},
): string | undefined {
  const url = nodeResolve(specifier, importer);
  if (url !== undefined) return url.startsWith("file:") ? fileURLToPath(url) : undefined;

  const dir = path.dirname(importer);
  const tsconfig = nearestTsconfig(dir);
  const inputs = tsconfig ? [...tsconfigInputs(tsconfig)] : [];
  for (const [file] of inputs) addDependency(file);
  // Keyed on the text of the configuration, so an edit to an alias makes a new resolver.
  const key = JSON.stringify([conditions, inputs]);
  let entry = resolvers.get(tsconfig);
  if (entry?.key !== key) {
    entry = {
      key,
      resolver: new ResolverFactory({
        conditionNames: [...conditions, "default"],
        extensions: [".ts", ".tsx", ".mts", ".js", ".mjs", ".jsx"],
        extensionAlias: { ".js": [".ts", ".tsx", ".js"], ".mjs": [".mts", ".mjs"] },
        ...(tsconfig ? { tsconfig: { configFile: tsconfig, references: "auto" as const } } : {}),
      }),
    };
    resolvers.set(tsconfig, entry);
  }
  return entry.resolver.sync(dir, specifier).path;
}
