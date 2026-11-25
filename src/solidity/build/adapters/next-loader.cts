/**
 * `@morpho-org/viem-dlc/solidity/next/loader`: the loader behind `withSolidity`, run by Turbopack.
 * CommonJS, because that is how a loader is loaded; the core is ESM and is imported on first use.
 *
 * An import is resolved by Turbopack's own resolver, with the aliases and the conditions of the layer
 * the module is built for. One thing that resolver is not told for a loader is whether the module is
 * imported or required, so where a package's `exports` answer the two differently it gives the first
 * of the two answers. The application's `import` gets the "import" answer. An answer that the
 * package lists under a "require" condition is therefore refused, not read.
 */
import fs = require("node:fs");
import path = require("node:path");

interface LoaderContext {
  resourcePath: string;
  async(): (error: Error | null, code?: string) => void;
  resolve(context: string, request: string, callback: (error: Error | null, file?: string) => void): void;
  addDependency(file: string): void;
  emitWarning?(warning: Error): void;
  emitError?(error: Error): void;
}

/** The targets a package's `exports` or `imports` list under a "require" condition. */
const requireTargets = (node: unknown, required: boolean, into: string[]): string[] => {
  if (typeof node === "string") {
    if (required) into.push(node);
  } else if (Array.isArray(node)) {
    for (const item of node) requireTargets(item, required, into);
  } else if (typeof node === "object" && node !== null) {
    for (const [key, value] of Object.entries(node)) requireTargets(value, required || key === "require", into);
  }
  return into;
};

/** The manifest above `file` that gives it to "require", if one does. Every manifest read is reported. */
const requiredBy = (file: string, read: (manifest: string) => void): string | undefined => {
  for (let dir = path.dirname(file); dir !== path.dirname(dir); dir = path.dirname(dir)) {
    const manifest = path.join(dir, "package.json");
    if (!fs.existsSync(manifest)) continue;
    read(manifest);
    const { exports, imports } = JSON.parse(fs.readFileSync(manifest, "utf8")) as Record<string, unknown>;
    const relative = `./${path.relative(dir, file)}`;
    const selects = (target: string) =>
      target.endsWith("/")
        ? relative.startsWith(target)
        : new RegExp(
            `^${target
              .split("*")
              .map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
              .join(".*")}$`,
          ).test(relative);
    if (requireTargets([exports, imports], false, []).some(selects)) return manifest;
  }
  return undefined;
};

function solidityLoader(this: LoaderContext, source: string): void {
  const callback = this.async();
  const resolveImport = (specifier: string, importer: string) =>
    new Promise<string | undefined>((resolve) =>
      this.resolve(path.dirname(importer), specifier, (error, file) => {
        if (error || file === undefined) return resolve(undefined);
        // A relative import names a file, not a package's `exports`.
        const manifest = /^\.{0,2}\//.test(specifier)
          ? undefined
          : requiredBy(file, (read) => this.addDependency(read));
        if (manifest === undefined) return resolve(file);
        this.emitError?.(
          new Error(
            `${importer}: cannot tell which file an import of "${specifier}" loads. ${manifest} gives ${path.relative(path.dirname(manifest), file)} to "require", and Turbopack does not tell a loader whether a module is imported or required. A lens cannot read from a module resolved this way.`,
          ),
        );
        resolve(undefined);
      }),
    );
  import("../transform.js")
    .then(({ transform }) =>
      transform(source, this.resourcePath, {
        resolve: resolveImport,
        addDependency: (file) => this.addDependency(file),
        warn: (diagnostic) =>
          this.emitWarning?.(
            new Error(`${diagnostic.file}:${diagnostic.line}:${diagnostic.column} - warning: ${diagnostic.message}`),
          ),
      }),
    )
    .then((result) => callback(null, result ? result.code : source))
    .catch((error: Error) => callback(error));
}

export = solidityLoader;
