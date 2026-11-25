/**
 * `@morpho-org/viem-dlc/solidity/next/loader`: the loader behind `withSolidity`, run by Turbopack.
 * CommonJS, because that is how a loader is loaded; the core is ESM and is imported on first use.
 *
 * An import is resolved by Turbopack's own resolver, with the aliases of the application. That resolver
 * is not told, for a loader, whether the module is imported or required, and it answers for the layer
 * the module is built for. Neither decides which file a lens reads: the transform takes an answer only
 * for an import that names one file under every condition, and stops the build at any other.
 */
import path = require("node:path");

interface LoaderContext {
  resourcePath: string;
  async(): (error: Error | null, code?: string) => void;
  resolve(context: string, request: string, callback: (error: Error | null, file?: string) => void): void;
  addDependency(file: string): void;
  emitWarning?(warning: Error): void;
}

function solidityLoader(this: LoaderContext, source: string): void {
  const callback = this.async();
  import("../transform.js")
    .then(({ transform }) =>
      transform(source, this.resourcePath, {
        resolve: (specifier, importer) =>
          new Promise<string | undefined>((resolve) =>
            this.resolve(path.dirname(importer), specifier, (error, file) => resolve(error ? undefined : file)),
          ),
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
