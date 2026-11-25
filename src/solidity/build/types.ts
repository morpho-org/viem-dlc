/**
 * The contracts between the stages of the build half.
 *
 * The transform is six stages run in order:
 *
 *   find  ->  resolve  ->  print  ->  compile  ->  rewrite        (report: every file read)
 *
 * Each stage is one module under `src/solidity/build/` and talks to the others only through the types
 * in this file, so a stage can be built and tested without touching its neighbours:
 *
 * | Stage | Module | Takes | Gives |
 * | --- | --- | --- | --- |
 * | parse | `module.ts` | a file's text | {@link SourceModule} |
 * | find | `find.ts` | {@link SourceModule} | {@link LensSite}[] |
 * | resolve | `resolve.ts` | {@link LensSite}, {@link ModuleLoader} | {@link ResolvedLens} |
 * | print | `print-solidity.ts`, `print-yul.ts`, `position-map.ts` | {@link ResolvedLens} | {@link PrintedLens} |
 * | compile | `compile.ts`, `config.ts` | {@link CompileInput}, {@link SolidityConfig} | {@link CompileResult} |
 * | diagnose | `solc-errors.ts` | {@link SolcDiagnostic}[], {@link PrintedLens} | {@link LensDiagnostic}[] |
 * | rewrite | `rewrite.ts` | {@link SourceModule}, {@link BuiltLens}[] | the module's new text |
 * | orchestrate | `transform.ts` | `(code, id, host)` | {@link TransformResult} |
 *
 * Two rules hold for every stage. It never evaluates anything: no arithmetic, no hashing, no operator
 * typing. And it never imports or runs an application module: modules are parsed, and only parsed.
 */
import type {
  ArrowFunctionExpression,
  CallExpression,
  Class,
  Expression,
  Function as FunctionNode,
  ImportDeclaration,
  ImportDeclarationSpecifier,
  ObjectExpression,
  Program,
  Span,
  Statement,
  VariableDeclarator,
} from "oxc-parser";
import type { Abi, Hex } from "viem";

// =====================================================================================================
// Sources and positions
// =====================================================================================================

/** A 1-based line and column in a source file, as editors and tsc print them. */
export interface SourcePosition {
  readonly file: string;
  readonly line: number;
  readonly column: number;
}

/** Where something came from: a node of a parsed module. Every located error is built from one. */
export interface Origin {
  readonly module: SourceModule;
  readonly node: Span;
}

/** A module-level binding of a parsed module, as the parser sees it. Nothing is evaluated. */
export type ModuleBinding =
  | {
      readonly kind: "import";
      readonly name: string;
      /** The import's specifier, as written. */
      readonly source: string;
      /** The exported name on the other side: a name, `"default"`, or `"*"` for a namespace import. */
      readonly imported: string;
      readonly node: ImportDeclarationSpecifier;
      readonly statement: ImportDeclaration;
    }
  | {
      readonly kind: "variable";
      readonly name: string;
      readonly declarationKind: "const" | "let" | "var" | "using" | "await using";
      readonly init: Expression | null;
      readonly node: VariableDeclarator;
      readonly statement: Statement;
      /** False when the statement declares several names, so it cannot be removed for one of them. */
      readonly single: boolean;
      exported: boolean;
    }
  | {
      readonly kind: "class";
      readonly name: string;
      readonly node: Class;
      readonly statement: Statement;
      exported: boolean;
    }
  | {
      readonly kind: "function";
      readonly name: string;
      readonly node: FunctionNode;
      readonly statement: Statement;
      exported: boolean;
    };

/** One export of a module: a local binding, a re-export from another module, or a default expression. */
export type ModuleExport =
  | { readonly kind: "local"; readonly local: string }
  | { readonly kind: "reexport"; readonly source: string; readonly imported: string }
  | { readonly kind: "default"; readonly node: Expression };

/**
 * One parsed file. Produced by `module.ts`, which is the only module that calls the TypeScript parser.
 * Type annotations, generics and `as` are part of the tree and are skipped by every later stage: the
 * transform reads no TypeScript type, so a module means the same read from `.ts` source and from a
 * package's built `.js`.
 */
export interface SourceModule {
  /** Absolute path. */
  readonly file: string;
  readonly code: string;
  readonly program: Program;
  /** Offset of the first character of each line, for {@link SourcePosition} lookups. */
  readonly lineStarts: readonly number[];
  readonly bindings: ReadonlyMap<string, ModuleBinding>;
  readonly exports: ReadonlyMap<string, ModuleExport>;
  /** The specifiers of this module's `export * from "…"` declarations, in order. */
  readonly starExports: readonly string[];
}

/**
 * How a stage reaches other files. `resolve` and `load` are synchronous. Under a host whose resolver is
 * asynchronous, `resolve` throws an internal signal that `transform.ts` catches to await the host and
 * run the stage again, so a stage must let exceptions it did not raise pass through, and must not keep
 * state between calls.
 */
export interface ModuleLoader {
  /** The absolute path `specifier` names when imported from the file `importer`, or undefined when it names nothing. */
  resolve(specifier: string, importer: string): string | undefined;
  /** Parses a file, once. Every file loaded is reported to the host as a dependency. */
  load(file: string): SourceModule;
}

// =====================================================================================================
// Diagnostics
// =====================================================================================================

/** One problem, always at a TypeScript position. An error solc gives without a location is reported at its lens. */
export interface LensDiagnostic extends SourcePosition {
  readonly severity: "error" | "warning";
  readonly message: string;
  /** Which side found it: the transform's own rules, or solc type-checking the printed Solidity. */
  readonly source: "transform" | "solc";
  /** The lens being built, when one was. */
  readonly lens?: string;
  /** solc's error code, such as `"7407"`. */
  readonly code?: string;
}

// =====================================================================================================
// Find
// =====================================================================================================

/**
 * One `contract(name, functions, options?).implementation({ … })` whose `contract` is imported from the
 * package directly. Found at any nesting depth. `find` checks the shape (lens parts are
 * literals written in place) and nothing else.
 */
export interface LensSite {
  /** The whole expression, which `rewrite` replaces. */
  readonly node: CallExpression;
  /** The inner `contract(…)` call. */
  readonly declaration: CallExpression;
  /** The contract's name, from the string literal given first. */
  readonly name: string;
  /** The entry-point signatures: the object literal given second. */
  readonly functions: ObjectExpression;
  /** The options object given third, if any. Its `immutables` lists the lens's immutables. */
  readonly options: ObjectExpression | undefined;
  /** The object literal given to `.implementation`. */
  readonly implementation: ObjectExpression;
}

// =====================================================================================================
// Resolve: what the resolver hands the printers
// =====================================================================================================

/** Every elementary type name a lens can write. */
export type ElementaryTypeName =
  | `uint${number}`
  | `int${number}`
  | "address"
  | "bool"
  | "bytes32"
  | "bytes4"
  | "bytes"
  | "string";

/** A type as a declaration names it. `[T]` is an array of T. */
export type TypeRef =
  | { readonly kind: "elementary"; readonly name: ElementaryTypeName }
  | { readonly kind: "struct"; readonly declaration: StructDeclaration }
  | { readonly kind: "interface"; readonly declaration: InterfaceDeclaration }
  | { readonly kind: "array"; readonly element: TypeRef };

/** `view(A, B).returns(C)`, resolved. */
export interface ResolvedSignature {
  readonly mutability: "view" | "pure";
  readonly inputs: readonly TypeRef[];
  readonly outputs: readonly TypeRef[];
  readonly origin: Origin;
}

/** A function literal written in place: an arrow function, a function expression, or an object method. */
export type FunctionBody = ArrowFunctionExpression | FunctionNode;

/** A function with a body: an entry point, a library function or a helper. */
export interface ResolvedFunction {
  /** The Solidity name. */
  readonly name: string;
  readonly signature: ResolvedSignature;
  readonly body: FunctionBody;
  /** The module the body is written in, for positions. */
  readonly module: SourceModule;
  /** The declarations the body names directly, in order of first use. */
  readonly dependencies: readonly ResolvedDeclaration[];
}

interface DeclarationBase {
  /**
   * The Solidity identifier, unique among the lens's declarations: the name the declaration has in the
   * module that declares it, with `_2`, `_3`… appended when two different declarations share a name.
   * A constant has no Solidity name of its own (it is printed in place) and keeps its module name.
   */
  readonly name: string;
  /** The module that declares it. */
  readonly module: SourceModule;
  /** The node that declares it: the class, or the initializer of the `const`. */
  readonly node: Span;
  /** The declarations this one names directly, in order of first use. */
  readonly dependencies: readonly ResolvedDeclaration[];
}

/** `class Quote extends Struct({ price: uint256, decimals: uint8 }) {}` */
export interface StructDeclaration extends DeclarationBase {
  readonly kind: "struct";
  /** In the order written. */
  readonly fields: readonly { readonly name: string; readonly type: TypeRef; readonly origin: Origin }[];
}

/** `const IPriceFeed = iface({ latest: view().returns(uint256) })` */
export interface InterfaceDeclaration extends DeclarationBase {
  readonly kind: "interface";
  /** In the order written. */
  readonly functions: readonly { readonly name: string; readonly signature: ResolvedSignature }[];
}

/** `const mulDivDown = internal(pure(uint256, uint256, uint256).returns(uint256), (x, y, d) => …)` */
export interface HelperDeclaration extends DeclarationBase, ResolvedFunction {
  readonly kind: "helper";
}

/** `const FEED = immutable(IPriceFeed)` */
export interface ImmutableDeclaration extends DeclarationBase {
  readonly kind: "immutable";
  readonly type: TypeRef;
}

/**
 * `const WAD = 10n ** 18n`. The resolver has checked the rule for a constant a lens reads: it is
 * built only from bigint literals, booleans, strings that do not start with `0x`, other such
 * constants, and operators other than `/`. A number in it is a bigint: a Number literal is refused.
 * The printer prints `expression` in place, in parentheses, at each use.
 */
export interface ConstantDeclaration extends DeclarationBase {
  readonly kind: "constant";
  readonly expression: Expression;
}

/** `const Lib = library('Name', { … }).implementation({ … })`. `name` is the string literal. */
export interface LibraryDeclaration extends DeclarationBase {
  readonly kind: "library";
  /** In the order the signatures are written. */
  readonly functions: readonly ResolvedFunction[];
}

export type ResolvedDeclaration =
  | StructDeclaration
  | InterfaceDeclaration
  | HelperDeclaration
  | ImmutableDeclaration
  | ConstantDeclaration
  | LibraryDeclaration;

/** Everything the package exports that a body can name. `global` holds `Number` and `BigInt`. */
export interface BuiltinReference {
  readonly kind: "builtin";
  readonly module: "solidity" | "yul" | "global";
  /** The exported name: `"uint256"`, `"keccak256"`, `"mload"`, `"Number"`… */
  readonly name: string;
}

/** What a name in a body, a signature or a constant resolved to. */
export type Reference = { readonly kind: "declaration"; readonly declaration: ResolvedDeclaration } | BuiltinReference;

/**
 * From a node to what it names. The keys are the `Identifier` nodes that are free in their function
 * (not a parameter and not a local), and the `MemberExpression` nodes that read a name off a namespace
 * import (`math.mulDivDown`). A node that is absent is a local, a parameter, a property name or `this`:
 * the resolver has done the scope analysis, and the printers do not repeat it.
 */
export type ReferenceMap = ReadonlyMap<Span, Reference>;

/**
 * One lens with every name resolved, ready to print. Nothing here is Solidity text yet, and nothing has
 * been evaluated: bodies are the original trees, and types are {@link TypeRef}s to declarations.
 */
export interface ResolvedLens {
  readonly site: LensSite;
  /** The module the lens is written in. */
  readonly module: SourceModule;
  /** The contract's name. */
  readonly name: string;
  /** The entry points, in the order `contract` declares them. */
  readonly entries: readonly ResolvedFunction[];
  /** The lens's immutables in constructor order. `key` is the name `with({ … })` takes. */
  readonly immutables: readonly { readonly key: string; readonly declaration: ImmutableDeclaration }[];
  /**
   * Every declaration the lens reaches, each once: declarations from other modules first, in the order
   * the resolver first reached their module, then the lens module's own, each module's in source order.
   * This is the order the Solidity printer prints each kind in.
   */
  readonly declarations: readonly ResolvedDeclaration[];
  readonly references: ReferenceMap;
  /** Every file read to resolve the lens, the lens's own included. */
  readonly files: readonly string[];
}

// =====================================================================================================
// Print
// =====================================================================================================

/**
 * One printed node: a range of the Solidity text, in UTF-16 offsets of {@link PrintedLens.solidity},
 * and the TypeScript it came from.
 */
export interface PositionMapEntry extends SourcePosition {
  readonly start: number;
  readonly end: number;
}

/** Every node the printer printed. Entries nest and never partially overlap. */
export interface PositionMap {
  readonly entries: readonly PositionMapEntry[];
}

/** What the printers produce for one lens. */
export interface PrintedLens {
  /** The whole Solidity source unit: pragma, structs, interfaces, libraries and the contract. */
  readonly solidity: string;
  readonly map: PositionMap;
}

/**
 * The text sink both printers write to. `node` records the range everything written inside it covers,
 * which is how solc's errors find their way back to TypeScript.
 */
export interface SourceWriter {
  write(text: string): void;
  /** Writes a line break and the current indentation. */
  line(): void;
  /** Runs `body` one indentation level deeper. */
  indented(body: () => void): void;
  /** Runs `body` and records what it wrote as printed from `origin`. */
  node<T>(origin: Origin, body: () => T): T;
}

/**
 * What the Solidity printer hands the Yul printer for one `assembly(…)` call. The Solidity printer has
 * already written `assembly ("memory-safe") `; the Yul printer writes the block, braces included.
 */
export interface YulBlockInput {
  /** The callback given to `assembly`. */
  readonly block: FunctionBody;
  readonly module: SourceModule;
  readonly references: ReferenceMap;
  /**
   * The Solidity variable that receives the block's final `return word`, when the block is a value.
   * Undefined when the block is a statement.
   */
  readonly result: string | undefined;
  /** Solidity locals in scope at the block, which the block may read and assign. */
  readonly locals: ReadonlySet<string>;
  readonly writer: SourceWriter;
}

// =====================================================================================================
// Compile
// =====================================================================================================

/**
 * The one configuration file of a repository, `solidity.config.json`, found by walking up from the lens
 * file. A lens compiles the same way in a build and in a test because both read this file.
 */
export interface SolidityConfig {
  /** Absolute path of the configuration file. Reported to the host as a dependency of every lens. */
  readonly path: string;
  /** The directory holding the file. */
  readonly root: string;
  /** Where compile results are cached by content: `node_modules/.cache/viem-dlc-solidity` under `root`. */
  readonly cacheDir: string;
  readonly solc: {
    /** The exact solc version, such as `"0.8.36"`. The installed `solc` package must match it. */
    readonly version: string;
    /** solc's standard-JSON `settings`, without `outputSelection`. */
    readonly settings: Readonly<Record<string, unknown>>;
  };
}

export interface CompileInput {
  /** The contract to take the ABI and bytecode of. */
  readonly contractName: string;
  readonly solidity: string;
}

/** What a compiled lens is made of. Cached on disk under a hash of the Solidity, the settings and the version. */
export interface CompiledArtifact {
  /** solc's ABI, constructor included. */
  readonly abi: Abi;
  /** Creation bytecode, without constructor arguments. */
  readonly bytecode: Hex;
}

/** One entry of solc's `errors` output. */
export interface SolcDiagnostic {
  readonly severity: "error" | "warning" | "info";
  /** solc's category, such as `"TypeError"`. */
  readonly type: string;
  readonly message: string;
  readonly formattedMessage: string;
  readonly errorCode?: string;
  /** The range in the printed Solidity, in UTF-8 byte offsets as solc reports it. Absent when solc gives none. */
  readonly range?: { readonly start: number; readonly end: number };
}

/**
 * The outcome of compiling one lens. A missing or mismatched solc is not a result: `compile` throws
 * `SolcUnavailableError`, and only when the cache has no entry, so a warm cache needs no compiler.
 */
export type CompileResult =
  | {
      readonly ok: true;
      readonly artifact: CompiledArtifact;
      readonly warnings: readonly SolcDiagnostic[];
      /** True when the artifact came from the disk cache and solc did not run. */
      readonly cached: boolean;
    }
  | { readonly ok: false; readonly errors: readonly SolcDiagnostic[]; readonly warnings: readonly SolcDiagnostic[] };

// =====================================================================================================
// Transform: what an adapter calls
// =====================================================================================================

/** One lens, taken through every stage but the rewrite. */
export interface BuiltLens {
  readonly resolved: ResolvedLens;
  readonly printed: PrintedLens;
  readonly artifact: CompiledArtifact;
}

/**
 * What the core asks of the tool it runs in. An adapter is this object plus a call to `transform`.
 */
export interface TransformHost {
  /**
   * The absolute path `specifier` names when imported from the file `importer`, by the host's own
   * resolver, so package `exports` and path aliases behave as they do in the application. Undefined when
   * it cannot be resolved.
   */
  resolve(specifier: string, importer: string): string | undefined | Promise<string | undefined>;
  /**
   * Called once for every file the result was built from, other than the module itself: helper and
   * constant modules, and the configuration file. The host must rerun the transform when any changes.
   */
  addDependency(file: string): void;
  /** Reads a file the resolver named. Defaults to the file system. */
  readFile?(file: string): string;
  /** Receives solc's warnings. Defaults to dropping them. */
  warn?(diagnostic: LensDiagnostic): void;
}

/** A host whose resolver answers at once, for `transformSync`. */
export interface SyncTransformHost extends TransformHost {
  resolve(specifier: string, importer: string): string | undefined;
}

export interface TransformOptions {
  /** Use this configuration instead of looking for `solidity.config.json` above the file. */
  readonly config?: SolidityConfig;
  /** Set to false to skip the disk cache, reading and writing. */
  readonly cache?: boolean;
}

/** A module's lenses, compiled. What `build` returns, and what the check command prints from. */
export interface BuiltModule {
  readonly module: SourceModule;
  readonly lenses: readonly BuiltLens[];
  /** Every file the lenses were built from, other than the module itself. */
  readonly dependencies: readonly string[];
}

export interface TransformResult extends BuiltModule {
  /**
   * The module's new text: each lens expression replaced by its compiled lens, and the helpers and
   * imports that only bodies used removed. Every edit keeps the line count, so no source map is needed
   * for line numbers.
   */
  readonly code: string;
}
