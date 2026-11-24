/**
 * `@morpho-org/viem-dlc/solidity`: what a lens module imports.
 *
 * Everything here is a stub. A lens body is compiled by a build integration and never runs as
 * JavaScript, so the stubs carry types for the editor and do nothing at run time: a declaration
 * (`Struct`, `iface`, `view`, `internal`, `immutable`) returns an inert value, and anything that only
 * means something inside a body throws when called. The one part that runs is {@link compiledLens},
 * which the build integration puts in place of each `contract(…).implementation(…)`.
 *
 * This entry holds no parser, no compiler and no implementation of any built-in.
 */
import { encodeAbiParameters, getContractAddress, type Hex, type Abi as ViemAbi, zeroAddress } from "viem";

import type * as Types from "./types.js";
import type {
  Abi,
  Assembly,
  Block,
  CallOptions,
  DeploylessCall,
  Handle,
  Iface,
  Immutable,
  Immutables,
  ImmutableType,
  ImmutableTypes,
  Impl,
  InArgs,
  Lens,
  Library,
  Mutability,
  Out,
  OutArgs,
  Returned,
  Returns,
  Sig,
  SigBuilder,
  SolType,
  StructClass,
  StructFields,
  Tokens,
} from "./types.js";

export type * from "./types.js";

/** The build integrations, named by every error a lens raises when none of them ran. */
const INTEGRATIONS =
  "Add the build integration once per tool: the plugin from @morpho-org/viem-dlc/solidity/vite " +
  "(Vitest, Vite, Rolldown, tsdown), withSolidity from @morpho-org/viem-dlc/solidity/next (Next with " +
  "Turbopack), or node --import @morpho-org/viem-dlc/solidity/register (Node scripts).";

/** A stub for something that only means something inside a lens body. */
const bodyOnly = <T>(what: string): T =>
  ((): never => {
    throw new Error(
      `@morpho-org/viem-dlc/solidity: ${what} is only meaningful inside a lens body. Bodies are compiled to Solidity at build time and never run as JavaScript.`,
    );
  }) as T;
const token = <T>(name: string): T => bodyOnly<T>(`${name}(…)`);

// ---- types --------------------------------------------------------------------------------------------
export const uint8 = token<Tokens["uint8"]>("uint8");
export const uint16 = token<Tokens["uint16"]>("uint16");
export const uint24 = token<Tokens["uint24"]>("uint24");
export const uint32 = token<Tokens["uint32"]>("uint32");
export const uint40 = token<Tokens["uint40"]>("uint40");
export const uint48 = token<Tokens["uint48"]>("uint48");
export const uint56 = token<Tokens["uint56"]>("uint56");
export const uint64 = token<Tokens["uint64"]>("uint64");
export const uint72 = token<Tokens["uint72"]>("uint72");
export const uint80 = token<Tokens["uint80"]>("uint80");
export const uint88 = token<Tokens["uint88"]>("uint88");
export const uint96 = token<Tokens["uint96"]>("uint96");
export const uint104 = token<Tokens["uint104"]>("uint104");
export const uint112 = token<Tokens["uint112"]>("uint112");
export const uint120 = token<Tokens["uint120"]>("uint120");
export const uint128 = token<Tokens["uint128"]>("uint128");
export const uint136 = token<Tokens["uint136"]>("uint136");
export const uint144 = token<Tokens["uint144"]>("uint144");
export const uint152 = token<Tokens["uint152"]>("uint152");
export const uint160 = token<Tokens["uint160"]>("uint160");
export const uint168 = token<Tokens["uint168"]>("uint168");
export const uint176 = token<Tokens["uint176"]>("uint176");
export const uint184 = token<Tokens["uint184"]>("uint184");
export const uint192 = token<Tokens["uint192"]>("uint192");
export const uint200 = token<Tokens["uint200"]>("uint200");
export const uint208 = token<Tokens["uint208"]>("uint208");
export const uint216 = token<Tokens["uint216"]>("uint216");
export const uint224 = token<Tokens["uint224"]>("uint224");
export const uint232 = token<Tokens["uint232"]>("uint232");
export const uint240 = token<Tokens["uint240"]>("uint240");
export const uint248 = token<Tokens["uint248"]>("uint248");
export const uint256 = token<Tokens["uint256"]>("uint256");
export const int8 = token<Tokens["int8"]>("int8");
export const int16 = token<Tokens["int16"]>("int16");
export const int24 = token<Tokens["int24"]>("int24");
export const int32 = token<Tokens["int32"]>("int32");
export const int40 = token<Tokens["int40"]>("int40");
export const int48 = token<Tokens["int48"]>("int48");
export const int56 = token<Tokens["int56"]>("int56");
export const int64 = token<Tokens["int64"]>("int64");
export const int72 = token<Tokens["int72"]>("int72");
export const int80 = token<Tokens["int80"]>("int80");
export const int88 = token<Tokens["int88"]>("int88");
export const int96 = token<Tokens["int96"]>("int96");
export const int104 = token<Tokens["int104"]>("int104");
export const int112 = token<Tokens["int112"]>("int112");
export const int120 = token<Tokens["int120"]>("int120");
export const int128 = token<Tokens["int128"]>("int128");
export const int136 = token<Tokens["int136"]>("int136");
export const int144 = token<Tokens["int144"]>("int144");
export const int152 = token<Tokens["int152"]>("int152");
export const int160 = token<Tokens["int160"]>("int160");
export const int168 = token<Tokens["int168"]>("int168");
export const int176 = token<Tokens["int176"]>("int176");
export const int184 = token<Tokens["int184"]>("int184");
export const int192 = token<Tokens["int192"]>("int192");
export const int200 = token<Tokens["int200"]>("int200");
export const int208 = token<Tokens["int208"]>("int208");
export const int216 = token<Tokens["int216"]>("int216");
export const int224 = token<Tokens["int224"]>("int224");
export const int232 = token<Tokens["int232"]>("int232");
export const int240 = token<Tokens["int240"]>("int240");
export const int248 = token<Tokens["int248"]>("int248");
export const int256 = token<Tokens["int256"]>("int256");
export const address = token<Tokens["address"]>("address");
export const bytes32 = token<Tokens["bytes32"]>("bytes32");
export const bytes4 = token<Tokens["bytes4"]>("bytes4");
export const bytes = token<Tokens["bytes"]>("bytes");
export const bool = token<Tokens["bool"]>("bool");
export const string = token<Tokens["string"]>("string");

// Each type name is also the type its values have inside a body, so `uint128` reads the same in a
// declaration and in an annotation. A value declared here hides the same name of `export type *`, so the
// types are named again beside their values.
export type uint8 = Types.uint8;
export type uint16 = Types.uint16;
export type uint24 = Types.uint24;
export type uint32 = Types.uint32;
export type uint40 = Types.uint40;
export type uint48 = Types.uint48;
export type uint56 = Types.uint56;
export type uint64 = Types.uint64;
export type uint72 = Types.uint72;
export type uint80 = Types.uint80;
export type uint88 = Types.uint88;
export type uint96 = Types.uint96;
export type uint104 = Types.uint104;
export type uint112 = Types.uint112;
export type uint120 = Types.uint120;
export type uint128 = Types.uint128;
export type uint136 = Types.uint136;
export type uint144 = Types.uint144;
export type uint152 = Types.uint152;
export type uint160 = Types.uint160;
export type uint168 = Types.uint168;
export type uint176 = Types.uint176;
export type uint184 = Types.uint184;
export type uint192 = Types.uint192;
export type uint200 = Types.uint200;
export type uint208 = Types.uint208;
export type uint216 = Types.uint216;
export type uint224 = Types.uint224;
export type uint232 = Types.uint232;
export type uint240 = Types.uint240;
export type uint248 = Types.uint248;
export type uint256 = Types.uint256;
export type int8 = Types.int8;
export type int16 = Types.int16;
export type int24 = Types.int24;
export type int32 = Types.int32;
export type int40 = Types.int40;
export type int48 = Types.int48;
export type int56 = Types.int56;
export type int64 = Types.int64;
export type int72 = Types.int72;
export type int80 = Types.int80;
export type int88 = Types.int88;
export type int96 = Types.int96;
export type int104 = Types.int104;
export type int112 = Types.int112;
export type int120 = Types.int120;
export type int128 = Types.int128;
export type int136 = Types.int136;
export type int144 = Types.int144;
export type int152 = Types.int152;
export type int160 = Types.int160;
export type int168 = Types.int168;
export type int176 = Types.int176;
export type int184 = Types.int184;
export type int192 = Types.int192;
export type int200 = Types.int200;
export type int208 = Types.int208;
export type int216 = Types.int216;
export type int224 = Types.int224;
export type int232 = Types.int232;
export type int240 = Types.int240;
export type int248 = Types.int248;
export type int256 = Types.int256;
export type address = Types.address;
export type bytes32 = Types.bytes32;
export type bytes4 = Types.bytes4;
export type bytes = Types.bytes;
export type bool = Types.bool;

// ---- declarations ----------------------------------------------------------------------------------------
/**
 * `class Position extends Struct({ … }) {}`: the class is the declaration, the type of a value inside
 * a body, and the Solidity name. `new Position()` is the zero value. A struct has at least one field.
 */
export function Struct<F extends StructFields>(_fields: keyof F extends never ? never : F): StructClass<F> {
  return class {
    constructor() {
      bodyOnly<() => never>("new on a struct")();
    }
  } as unknown as StructClass<F>;
}

const signature =
  <M extends Mutability>(mutability: M) =>
  <I extends readonly SolType[]>(...inputs: I): SigBuilder<I, M> => ({
    inputs,
    outputs: [],
    mutability,
    returns: (...outputs) => ({ inputs, outputs, mutability }),
  });
/** A function that reads state: `view(bytes32, address).returns(uint128)`. */
export const view = signature("view");
/** A function that reads nothing: `pure(uint256, uint256).returns(uint256)`. */
export const pure = signature("pure");

/** An interface. Only `view` and `pure` exist, so a state-changing call cannot be declared. */
export const iface = <F extends Record<string, Sig>>(_functions: F): Iface<F> =>
  bodyOnly<Iface<F>>("an interface handle");

/**
 * A helper: `internal(pure(uint256, uint256).returns(uint256), (a, b) => …)`. The signature is written
 * out because Solidity infers neither a return type nor a mutability.
 */
export const internal = <S extends Sig>(
  _signature: S,
  _body: (...args: OutArgs<S["inputs"]>) => Returns<S["outputs"]>,
): ((...args: InArgs<S["inputs"]>) => Returned<S["outputs"]>) => bodyOnly("an internal(…) helper");

/**
 * `const FEED = immutable(IPriceFeed)`: a module-level name that helpers and entry points both use. It is
 * listed once on the contract, which fixes its constructor position and its name in `with({ … })`.
 */
export const immutable = <T extends ImmutableType>(_type: T): Immutable<T> => Object.freeze({}) as Immutable<T>;

/** A library in the same Solidity file: `library('Name', { … }).implementation({ … })`. */
export const library = <const N extends string, F extends Record<string, Sig>>(
  _name: N,
  _functions: F,
): { implementation(impl: Impl<F>): Library<F> } => ({
  implementation: () => Object.freeze({ kind: "library" }) as Library<F>,
});

/**
 * A lens: `contract('Name', { f: view(Query).returns(Position) }).implementation({ f(e) { … } })`.
 *
 * A build integration replaces the whole expression with the compiled lens. Reached at run time, it
 * means no integration ran, and the lens throws on first use.
 */
export const contract = <
  const N extends string,
  F extends Record<string, Sig>,
  Im extends Immutables = Record<never, never>,
>(
  name: N,
  _functions: F,
  _options?: { immutables?: Im },
): {
  /** Entry points see the contract as `this`: `this.f(x)` is the external self-call Solidity spells the same way. */
  implementation(impl: Impl<F> & ThisType<Handle<F>>): Lens<N, F, ImmutableTypes<Im>>;
} => ({
  implementation: () => {
    const uncompiled = (): never => {
      throw new Error(
        `@morpho-org/viem-dlc/solidity: the lens ${name} was not compiled. contract(…).implementation(…) is replaced with bytecode at build time, and this module was loaded without a build integration. ${INTEGRATIONS}`,
      );
    };
    return {
      name,
      get abi() {
        return uncompiled();
      },
      get bytecode() {
        return uncompiled();
      },
      with: uncompiled,
    };
  },
});

// ---- the compiled lens ---------------------------------------------------------------------------------------
/** The deterministic deployment proxy, present on every chain a lens is read on. */
export const CREATE2_FACTORY = "0x4e59b44847b379578588920cA78FbF26c0B4956C";
/** The salt every lens is deployed under, so a lens has one counterfactual address per chain. */
export const CREATE2_SALT: Hex = `${zeroAddress}51A1E51A1E51A1E51A1E51A1`;

/** What a build integration writes in place of `contract(…).implementation(…)`. */
export interface CompiledLensArtifact {
  readonly name: string;
  /** solc's ABI, constructor included. */
  readonly abi: ViemAbi;
  /** Creation bytecode, without constructor arguments. */
  readonly bytecode: Hex;
  /** The keys of `contract`'s `immutables`, in constructor order. */
  readonly immutables: readonly string[];
}

/**
 * Wraps a compiled lens. Emitted by the build integrations; a lens module never calls it itself.
 *
 * @internal
 */
export function compiledLens(artifact: CompiledLensArtifact): Lens<string, Record<string, Sig>, Record<string, never>> {
  const { name, abi, bytecode, immutables } = artifact;
  const constructorInputs = abi.find((item) => item.type === "constructor")?.inputs ?? [];
  const withImmutables = (values: Readonly<Record<string, unknown>> = {}): DeploylessCall<ViemAbi> => {
    const args = immutables.map((key) => {
      if (!(key in values)) throw new Error(`@morpho-org/viem-dlc/solidity: ${name}.with(…) is missing "${key}".`);
      return values[key];
    });
    const initcode: Hex = args.length
      ? `${bytecode}${encodeAbiParameters(constructorInputs, args).slice(2)}`
      : bytecode;
    return {
      abi,
      address: getContractAddress({ bytecode: initcode, from: CREATE2_FACTORY, opcode: "CREATE2", salt: CREATE2_SALT }),
      factory: CREATE2_FACTORY,
      factoryData: `${CREATE2_SALT}${initcode.slice(2)}`,
    };
  };
  return Object.freeze({ name, abi, bytecode, with: withImmutables }) as unknown as Lens<
    string,
    Record<string, Sig>,
    Record<string, never>
  >;
}

// ---- built-ins --------------------------------------------------------------------------------------------------
/** `keccak256(…)`, computed by solc inside a body. */
export const keccak256 = bodyOnly<(data: Types.bytes | Hex) => Types.bytes32>("keccak256(…)");
export const abi: Abi = Object.freeze({
  encode: bodyOnly<Abi["encode"]>("abi.encode(…)"),
  decode: bodyOnly<Abi["decode"]>("abi.decode(…)"),
});
export const block: Block = Object.freeze({
  get timestamp() {
    return bodyOnly<() => Types.uint256>("block.timestamp")();
  },
  get number() {
    return bodyOnly<() => Types.uint256>("block.number")();
  },
});
export const gasleft = bodyOnly<() => Types.uint256>("gasleft()");
export const require = bodyOnly<(condition: boolean, reason?: string) => void>("require(…)");
/** Declared as a function, so tsc applies control-flow analysis: no `throw` is needed in front. */
export function revert(_reason?: string): never {
  return bodyOnly<() => never>("revert(…)")();
}
/** `new T[](n)` */
export const array = bodyOnly<<T extends SolType>(of: T, length: bigint | number) => Out<readonly T[]>>("array(…)");
/** `target.staticcall{gas: g}(data)`: every external call a lens makes is a `STATICCALL`. */
export const staticcall =
  bodyOnly<
    (
      target: Types.address,
      data: Types.bytes | Hex,
      options?: CallOptions,
    ) => [success: boolean, returndata: Types.bytes]
  >("staticcall(…)");
/** `a.code.length` */
export const codeSize = bodyOnly<(account: Types.address) => Types.uint256>("codeSize(…)");
/** `assembly ("memory-safe") { … }`, with opcodes imported from `@morpho-org/viem-dlc/solidity/yul`. */
export const assembly = bodyOnly<Assembly>("assembly(…)");
