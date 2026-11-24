/**
 * `@morpho-org/viem-dlc/solidity/vite`: the plugin for Vitest, Vite, Rolldown and tsdown.
 *
 * ```ts
 * import { solidity } from "@morpho-org/viem-dlc/solidity/vite";
 * export default defineConfig({ plugins: [solidity()] });
 * ```
 *
 * A lens is compiled from every file its names resolve through, and the rewrite removes the imports
 * that tied the lens module to them. So the plugin, not the bundler's module graph, has to make an edit
 * to one of those files reach the lens: each is declared with `addWatchFile`, and the two Vite-side
 * caches that key a transform on the module's own text are handled below.
 */
import { SOLIDITY_SPECIFIER } from "../builtins.js";
import { formatDiagnostic, LensError } from "../errors.js";
import { transform } from "../transform.js";
import type { TransformOptions } from "../types.js";

/** The part of a Rollup-style plugin context the plugin uses. */
interface PluginContext {
  resolve(source: string, importer?: string): Promise<{ id: string } | null>;
  addWatchFile(file: string): void;
  warn(message: string): void;
  error(error: { message: string; id?: string; loc?: { file?: string; line: number; column: number } }): never;
}

interface ModuleGraph {
  getModulesByFile(file: string): Iterable<unknown> | undefined;
  invalidateModule(module: unknown): void;
}

/** The part of Vite's dev server the plugin uses. */
interface DevServer {
  watcher: { on(event: "change" | "unlink", listener: (file: string) => void): unknown };
  environments?: Record<string, { moduleGraph: ModuleGraph }>;
  moduleGraph?: ModuleGraph;
}

type CacheKeyGenerator = (context: { sourceCode: string }) => string | false | undefined;
/** The part of Vitest's plugin context the plugin uses. */
interface VitestContext {
  defineCacheKeyGenerator?: (generator: CacheKeyGenerator) => void;
  experimental_defineCacheKeyGenerator?: (generator: CacheKeyGenerator) => void;
}

export interface SolidityPlugin {
  name: string;
  enforce: "pre";
  transform(this: PluginContext, code: string, id: string): Promise<{ code: string; map: null } | null>;
  configureServer(server: DevServer): void;
  configureVitest(context: VitestContext): void;
}

const SOURCE_FILE = /\.[cm]?[jt]sx?$/;

export function solidity(options: TransformOptions = {}): SolidityPlugin {
  /** A file a lens was compiled from, to the modules holding such a lens. */
  const dependents = new Map<string, Set<string>>();
  return {
    name: "viem-dlc-solidity",
    // Before the host strips types, so the transform sees the module as written and its positions hold.
    enforce: "pre",
    async transform(code, id) {
      const file = id.split("?")[0] as string;
      if (!SOURCE_FILE.test(file) || file.includes("/node_modules/")) return null;
      try {
        const result = await transform(
          code,
          file,
          {
            resolve: async (specifier, importer) => (await this.resolve(specifier, importer))?.id.split("?")[0],
            addDependency: (dependency) => {
              this.addWatchFile(dependency);
              const lensModules = dependents.get(dependency) ?? new Set<string>();
              dependents.set(dependency, lensModules);
              lensModules.add(file);
            },
            warn: (diagnostic) => this.warn(formatDiagnostic(diagnostic)),
          },
          options,
        );
        return result ? { code: result.code, map: null } : null;
      } catch (cause) {
        if (!(cause instanceof LensError)) throw cause;
        const [first] = cause.diagnostics;
        return this.error({
          message: cause.message,
          id: first?.file ?? file,
          ...(first ? { loc: { file: first.file, line: first.line, column: first.column - 1 } } : {}),
        });
      }
    },
    // Vite keeps a transform until the module itself changes, so declaring a watched file reruns tests on
    // the old bytecode. A change to a dependency therefore drops the transform of every lens built from it.
    configureServer(server) {
      const graphs = () => [
        ...Object.values(server.environments ?? {}).map((environment) => environment.moduleGraph),
        ...(server.moduleGraph ? [server.moduleGraph] : []),
      ];
      const invalidate = (file: string) => {
        for (const lensModule of dependents.get(file) ?? [])
          for (const graph of graphs())
            for (const module of graph.getModulesByFile(lensModule) ?? []) graph.invalidateModule(module);
      };
      server.watcher.on("change", invalidate);
      server.watcher.on("unlink", invalidate);
    },
    // Vitest's file-system module cache keys a transform on the module's own text, which is not all a lens
    // is compiled from. Modules that mention the package are kept out of it; compiled bytecode still comes
    // from the compile cache, which is keyed on the Solidity.
    configureVitest(context) {
      const define = context.defineCacheKeyGenerator ?? context.experimental_defineCacheKeyGenerator;
      define?.(({ sourceCode }) => (sourceCode.includes(SOLIDITY_SPECIFIER) ? false : undefined));
    },
  };
}

export default solidity;
