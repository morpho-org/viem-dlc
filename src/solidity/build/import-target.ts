/**
 * Which file an import names for a lens, checked against the answer the host's resolver gave.
 *
 * A lens is compiled from the module an import names, and the application imports that module too. The
 * two agree only where the import names one file: whatever conditions the host resolves with, and
 * whether the module is imported or required. Where it may not, the build stops; it never picks.
 *
 * - A relative or absolute path names a file, and the host's answer is taken.
 * - A `#` import and a package name are read from the manifest that decides them: the entry of
 *   `imports` or `exports` must give one target under every condition, apart from `types`, and the
 *   host's answer must be that file.
 * - Any other name is an alias of the host's, and must stay inside the package of the module that
 *   imports it: another package is entered only through its `exports`.
 *
 * Nothing here resolves an import for the host. It reads the one entry that decides an import, and
 * refuses what that entry leaves open.
 */
import fs from "node:fs";
import path from "node:path";

/** Thrown for an import a lens cannot read a module through. `transform` reports it at the import. */
export class ImportRefused extends Error {
  override readonly name = "ImportRefused";
}

function refuse(message: string): never {
  throw new ImportRefused(message);
}

const RULE = "A lens reads a module only through an import that names one file under every condition.";

const isFile = (file: string): boolean => fs.statSync(file, { throwIfNoEntry: false })?.isFile() === true;

/** The path without symbolic links, or the path itself when nothing is there. */
const real = (file: string): string => {
  try {
    return fs.realpathSync(file);
  } catch {
    return file;
  }
};

/** Each directory from the one holding `file` up to the root. */
function* directoriesAbove(file: string): Generator<string> {
  for (let dir = path.dirname(file); ; dir = path.dirname(dir)) {
    yield dir;
    if (dir === path.dirname(dir)) return;
  }
}

/** The manifest of the package `file` belongs to: the nearest `package.json` above it. */
const manifestAbove = (file: string): string | undefined => {
  for (const dir of directoriesAbove(file)) {
    const candidate = path.join(dir, "package.json");
    if (isFile(candidate)) return candidate;
  }
  return undefined;
};

const readManifest = (manifest: string): Record<string, unknown> => {
  try {
    const parsed: unknown = JSON.parse(fs.readFileSync(manifest, "utf8"));
    if (typeof parsed === "object" && parsed !== null) return parsed as Record<string, unknown>;
  } catch {
    // Refused below, like a manifest that is not an object.
  }
  return refuse(`${manifest} cannot be read as a package manifest.`);
};

/**
 * The manifest of the package `name`, as a module at `importer` reaches it: the importer's own package
 * under its own name, or the nearest one installed above the importer.
 */
const manifestOfPackage = (name: string, importer: string): string | undefined => {
  const own = manifestAbove(importer);
  if (own !== undefined && readManifest(own).name === name) return own;
  for (const dir of directoriesAbove(importer)) {
    const candidate = path.join(dir, "node_modules", name, "package.json");
    if (isFile(candidate)) return real(candidate);
  }
  return undefined;
};

/** A specifier as a package name and the subpath asked of it, or undefined when it cannot name a package. */
const packageRequest = (specifier: string): { name: string; subpath: string } | undefined => {
  const [, name, rest = ""] = /^((?:@[^/]+\/)?[^/@][^/]*)(\/.*)?$/.exec(specifier) ?? [];
  return name === undefined ? undefined : { name, subpath: `.${rest}` };
};

/** Every target an entry can give, whatever the conditions. What `types` gives is for the type checker. */
const targetsOf = (entry: unknown, into = new Set<string>()): Set<string> => {
  if (typeof entry === "string") into.add(entry);
  else if (Array.isArray(entry)) for (const item of entry) targetsOf(item, into);
  else if (typeof entry === "object" && entry !== null)
    for (const [condition, value] of Object.entries(entry)) if (condition !== "types") targetsOf(value, into);
  return into;
};

/** A field as a map from key to entry. `exports` without subpaths is, as a whole, the entry of `"."`. */
const entriesOf = (field: unknown): Record<string, unknown> | undefined => {
  if (field === undefined || field === null) return undefined;
  return typeof field === "object" && !Array.isArray(field) && Object.keys(field).some((key) => /^[.#]/.test(key))
    ? (field as Record<string, unknown>)
    : { ".": field };
};

const quoted = (names: readonly string[]): string => names.map((name) => `"${name}"`).join(" and ");

interface Target {
  /** The file the entry names. */
  readonly file: string;
  /** Which entry, for an error: its key, its field and its manifest. */
  readonly entry: string;
}

/**
 * The one file the entry for `request` names in the `imports` or the `exports` of a manifest. `read`
 * receives each manifest the answer is read from.
 */
const targetOf = (
  manifest: string,
  field: "imports" | "exports",
  request: string,
  read: (manifest: string) => void,
): Target => {
  read(manifest);
  const entries = entriesOf(readManifest(manifest)[field]);
  if (!entries)
    return refuse(`${manifest} has no "${field}", so each tool chooses the file "${request}" names. ${RULE}`);
  // The entry under the key itself, or else under the one pattern that matches.
  const keys = Object.hasOwn(entries, request)
    ? [request]
    : Object.keys(entries).filter((key) => {
        const [before = "", after, ...more] = key.split("*");
        return (
          after !== undefined &&
          more.length === 0 &&
          request.length >= key.length &&
          request.startsWith(before) &&
          request.endsWith(after)
        );
      });
  const [key] = keys;
  if (key === undefined || keys.length > 1)
    return refuse(
      `${manifest} has ${key === undefined ? "no entry" : `the entries ${quoted(keys)}`} for "${request}" in "${field}". ${RULE}`,
    );
  const entry = `the entry "${key}" of "${field}" in ${manifest}`;
  const targets = [...targetsOf(entries[key])];
  const [only] = targets;
  if (only === undefined || targets.length > 1)
    return refuse(`${entry} names ${only === undefined ? "no file" : `${quoted(targets)}, by condition`}. ${RULE}`);
  const [before = "", after = ""] = key.split("*");
  const target = only.replaceAll("*", request.slice(before.length, request.length - after.length));
  if (target.startsWith("./")) return { file: path.join(path.dirname(manifest), target), entry };
  // An entry of "imports" may name another package, which then answers through its own "exports".
  const outside = field === "imports" ? packageRequest(target) : undefined;
  const installed = outside && manifestOfPackage(outside.name, manifest);
  if (!outside || !installed)
    return refuse(
      `${entry} names "${target}", which is neither a file of the package nor a package installed for it. ${RULE}`,
    );
  return targetOf(installed, "exports", outside.subpath, read);
};

/**
 * Checks the file a host resolved an import to. Throws {@link ImportRefused} when the import may name
 * another file under other conditions, or names another file than the host found. `read` receives each
 * package manifest that decided the answer: an edit to one can change it.
 */
export function checkImport(specifier: string, importer: string, file: string, read: (manifest: string) => void): void {
  if (/^\.{0,2}(?:\/|$)/.test(specifier) || path.isAbsolute(specifier)) return;
  const from = real(importer);
  const own = manifestAbove(from);
  let target: Target;
  if (specifier.startsWith("#")) {
    if (own === undefined)
      refuse(`"${specifier}" is read from the "imports" of a package.json, and there is none above ${importer}.`);
    target = targetOf(own, "imports", specifier, read);
  } else {
    const request = packageRequest(specifier);
    const installed = request && manifestOfPackage(request.name, from);
    if (!request || !installed) {
      // No package has this name, so the host answered from its own aliases.
      if (manifestAbove(real(file)) !== own)
        refuse(
          `"${specifier}" names no installed package, and resolves to ${file}, outside the package of the module that imports it. A lens reads a module of another package only through that package's "exports": import it by the package's name, or by a relative path.`,
        );
      return;
    }
    target = targetOf(installed, "exports", request.subpath, read);
  }
  if (real(target.file) !== real(file))
    refuse(`"${specifier}" resolves to ${file}, and ${target.entry} names ${target.file}. ${RULE}`);
}
