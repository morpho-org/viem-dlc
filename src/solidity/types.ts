/**
 * The editor's view of a lens: every type a lens module sees, and nothing that runs.
 *
 * Inside a lens the semantics are Solidity's. These types exist so tsc can check declarations,
 * arguments and the ABI that viem consumes; solc is the type checker for everything tsc cannot
 * follow.
 */
import type { Address, Hex } from "viem";

declare const TYPE: unique symbol;
declare const WIDTH: unique symbol;
declare const NOMINAL: unique symbol;
declare const IFACE: unique symbol;
declare const FIELDS: unique symbol;
declare const ELEMENT: unique symbol;
declare const IMMUTABLE: unique symbol;

// ---- the width lattice -----------------------------------------------------------------------------
// Each integer type is a `bigint` tagged with the set of types that convert to it implicitly. The tag
// is an optional readonly property, so S is assignable to T exactly when S's set is inside T's: every
// widening passes, a direct narrowing is a tsc error (as it is a solc error), and a plain `bigint`
// (a literal, any arithmetic result) is accepted everywhere and left for solc to check.
type U8 = "u8";
type U16 = U8 | "u16";
type U24 = U16 | "u24";
type U32 = U24 | "u32";
type U40 = U32 | "u40";
type U48 = U40 | "u48";
type U56 = U48 | "u56";
type U64 = U56 | "u64";
type U72 = U64 | "u72";
type U80 = U72 | "u80";
type U88 = U80 | "u88";
type U96 = U88 | "u96";
type U104 = U96 | "u104";
type U112 = U104 | "u112";
type U120 = U112 | "u120";
type U128 = U120 | "u128";
type U136 = U128 | "u136";
type U144 = U136 | "u144";
type U152 = U144 | "u152";
type U160 = U152 | "u160";
type U168 = U160 | "u168";
type U176 = U168 | "u176";
type U184 = U176 | "u184";
type U192 = U184 | "u192";
type U200 = U192 | "u200";
type U208 = U200 | "u208";
type U216 = U208 | "u216";
type U224 = U216 | "u224";
type U232 = U224 | "u232";
type U240 = U232 | "u240";
type U248 = U240 | "u248";
type U256 = U248 | "u256";
type I8 = "i8";
type I16 = I8 | "i16" | U8;
type I24 = I16 | "i24" | U16;
type I32 = I24 | "i32" | U24;
type I40 = I32 | "i40" | U32;
type I48 = I40 | "i48" | U40;
type I56 = I48 | "i56" | U48;
type I64 = I56 | "i64" | U56;
type I72 = I64 | "i72" | U64;
type I80 = I72 | "i80" | U72;
type I88 = I80 | "i88" | U80;
type I96 = I88 | "i96" | U88;
type I104 = I96 | "i104" | U96;
type I112 = I104 | "i112" | U104;
type I120 = I112 | "i120" | U112;
type I128 = I120 | "i128" | U120;
type I136 = I128 | "i136" | U128;
type I144 = I136 | "i144" | U136;
type I152 = I144 | "i152" | U144;
type I160 = I152 | "i160" | U152;
type I168 = I160 | "i168" | U160;
type I176 = I168 | "i176" | U168;
type I184 = I176 | "i184" | U176;
type I192 = I184 | "i192" | U184;
type I200 = I192 | "i200" | U192;
type I208 = I200 | "i208" | U200;
type I216 = I208 | "i216" | U208;
type I224 = I216 | "i224" | U216;
type I232 = I224 | "i232" | U224;
type I240 = I232 | "i240" | U232;
type I248 = I240 | "i248" | U240;
type I256 = I248 | "i256" | U248;

type Width<X> = { readonly [WIDTH]?: X };
export type uint8 = bigint & Width<U8>;
export type uint16 = bigint & Width<U16>;
export type uint24 = bigint & Width<U24>;
export type uint32 = bigint & Width<U32>;
export type uint40 = bigint & Width<U40>;
export type uint48 = bigint & Width<U48>;
export type uint56 = bigint & Width<U56>;
export type uint64 = bigint & Width<U64>;
export type uint72 = bigint & Width<U72>;
export type uint80 = bigint & Width<U80>;
export type uint88 = bigint & Width<U88>;
export type uint96 = bigint & Width<U96>;
export type uint104 = bigint & Width<U104>;
export type uint112 = bigint & Width<U112>;
export type uint120 = bigint & Width<U120>;
export type uint128 = bigint & Width<U128>;
export type uint136 = bigint & Width<U136>;
export type uint144 = bigint & Width<U144>;
export type uint152 = bigint & Width<U152>;
export type uint160 = bigint & Width<U160>;
export type uint168 = bigint & Width<U168>;
export type uint176 = bigint & Width<U176>;
export type uint184 = bigint & Width<U184>;
export type uint192 = bigint & Width<U192>;
export type uint200 = bigint & Width<U200>;
export type uint208 = bigint & Width<U208>;
export type uint216 = bigint & Width<U216>;
export type uint224 = bigint & Width<U224>;
export type uint232 = bigint & Width<U232>;
export type uint240 = bigint & Width<U240>;
export type uint248 = bigint & Width<U248>;
export type uint256 = bigint & Width<U256>;
export type int8 = bigint & Width<I8>;
export type int16 = bigint & Width<I16>;
export type int24 = bigint & Width<I24>;
export type int32 = bigint & Width<I32>;
export type int40 = bigint & Width<I40>;
export type int48 = bigint & Width<I48>;
export type int56 = bigint & Width<I56>;
export type int64 = bigint & Width<I64>;
export type int72 = bigint & Width<I72>;
export type int80 = bigint & Width<I80>;
export type int88 = bigint & Width<I88>;
export type int96 = bigint & Width<I96>;
export type int104 = bigint & Width<I104>;
export type int112 = bigint & Width<I112>;
export type int120 = bigint & Width<I120>;
export type int128 = bigint & Width<I128>;
export type int136 = bigint & Width<I136>;
export type int144 = bigint & Width<I144>;
export type int152 = bigint & Width<I152>;
export type int160 = bigint & Width<I160>;
export type int168 = bigint & Width<I168>;
export type int176 = bigint & Width<I176>;
export type int184 = bigint & Width<I184>;
export type int192 = bigint & Width<I192>;
export type int200 = bigint & Width<I200>;
export type int208 = bigint & Width<I208>;
export type int216 = bigint & Width<I216>;
export type int224 = bigint & Width<I224>;
export type int232 = bigint & Width<I232>;
export type int240 = bigint & Width<I240>;
export type int248 = bigint & Width<I248>;
export type int256 = bigint & Width<I256>;

// ---- opaque value types ----------------------------------------------------------------------------
// Bodies never run, so these do not have to be JavaScript strings. Distinct opaque types let tsc tell
// an address from a bytes32 and reject `address + 1n`.
export interface address {
  readonly [NOMINAL]: "address";
}
export interface bytes32 {
  readonly [NOMINAL]: "bytes32";
}
export interface bytes4 {
  readonly [NOMINAL]: "bytes4";
}
export interface bytes {
  readonly [NOMINAL]: "bytes";
}
export type bool = boolean;

interface IntegerBodies {
  uint8: uint8;
  uint16: uint16;
  uint24: uint24;
  uint32: uint32;
  uint40: uint40;
  uint48: uint48;
  uint56: uint56;
  uint64: uint64;
  uint72: uint72;
  uint80: uint80;
  uint88: uint88;
  uint96: uint96;
  uint104: uint104;
  uint112: uint112;
  uint120: uint120;
  uint128: uint128;
  uint136: uint136;
  uint144: uint144;
  uint152: uint152;
  uint160: uint160;
  uint168: uint168;
  uint176: uint176;
  uint184: uint184;
  uint192: uint192;
  uint200: uint200;
  uint208: uint208;
  uint216: uint216;
  uint224: uint224;
  uint232: uint232;
  uint240: uint240;
  uint248: uint248;
  uint256: uint256;
  int8: int8;
  int16: int16;
  int24: int24;
  int32: int32;
  int40: int40;
  int48: int48;
  int56: int56;
  int64: int64;
  int72: int72;
  int80: int80;
  int88: int88;
  int96: int96;
  int104: int104;
  int112: int112;
  int120: int120;
  int128: int128;
  int136: int136;
  int144: int144;
  int152: int152;
  int160: int160;
  int168: int168;
  int176: int176;
  int184: int184;
  int192: int192;
  int200: int200;
  int208: int208;
  int216: int216;
  int224: int224;
  int232: int232;
  int240: int240;
  int248: int248;
  int256: int256;
}
/** What a value of each elementary type is inside a body. */
interface Bodies extends IntegerBodies {
  address: address;
  bool: boolean;
  bytes32: bytes32;
  bytes4: bytes4;
  bytes: bytes;
  string: string;
}
/** What may be written where each elementary type is expected: the value, or a literal of it. */
type Inputs = { [K in keyof IntegerBodies]: IntegerBodies[K] | number } & {
  address: address | Address;
  bool: boolean;
  bytes32: bytes32 | Hex;
  bytes4: bytes4 | Hex;
  bytes: bytes | Hex;
  string: string;
};
export type Elementary = keyof Bodies;
export type IntegerName = keyof IntegerBodies;

// ---- type names are values, and calling one converts -----------------------------------------------
export interface Token<S extends Elementary = Elementary> {
  readonly [TYPE]: S;
}
export interface IntToken<S extends IntegerName> extends Token<S> {
  /** Solidity's conversion, spelled the same: `uint64(block.timestamp)`. An address converts to `uint160` only. */
  (value: bigint | number | bytes32 | (S extends "uint160" ? address : never)): Bodies[S];
}
export interface AddressToken extends Token<"address"> {
  // biome-ignore lint/suspicious/noExplicitAny: any interface handle converts to an address
  (value: bigint | number | address | Address | Handle<any>): address;
}
export interface Bytes32Token extends Token<"bytes32"> {
  (value: bigint | number | Hex): bytes32;
}

// ---- declarations ------------------------------------------------------------------------------------
/** The fields of a struct. Field order is the order the keys are written in. */
export interface StructFields {
  readonly [field: string]: SolType;
}
/** `class Position extends Struct({ … }) {}`: a named type, and `new Position()` is its zero value. */
// biome-ignore lint/suspicious/noExplicitAny: the default has to admit every field set
export interface StructClass<F extends StructFields = any> {
  new (init?: StructInput<F>): StructBody<F> & { readonly [FIELDS]: F };
  readonly [FIELDS]: F;
}
export interface IfaceTag<F extends Record<string, Sig> = Record<string, Sig>> {
  readonly [IFACE]: F;
}
/** A Solidity type as a lens names it: an elementary type, a struct class, an interface, or `[T]` for `T[]`. */
// biome-ignore lint/suspicious/noExplicitAny: an interface of any function set is a type
export type SolType = Token | IfaceTag<any> | StructClass | readonly [SolType];

/** A lens only reads, so no other mutability can be declared. */
export type Mutability = "view" | "pure";
export interface Sig<
  // biome-ignore lint/suspicious/noExplicitAny: the default has to admit every signature
  I extends readonly SolType[] = readonly any[],
  // biome-ignore lint/suspicious/noExplicitAny: the default has to admit every signature
  O extends readonly SolType[] = readonly any[],
  M extends Mutability = Mutability,
> {
  readonly inputs: I;
  readonly outputs: O;
  readonly mutability: M;
}
export interface SigBuilder<I extends readonly SolType[], M extends Mutability> extends Sig<I, [], M> {
  returns<O extends readonly SolType[]>(...outputs: O): Sig<I, O, M>;
}

// ---- body types ----------------------------------------------------------------------------------------
/**
 * Only what a Solidity array can do: index and length. It carries its element type, so a JavaScript array
 * literal is not one: an array is made with `array(T, n)`.
 */
export interface SolArray<B> {
  [index: number]: B;
  readonly length: number;
  readonly [ELEMENT]: B;
}
export type StructBody<F> = { -readonly [K in keyof F]: Out<F[K]> };
export type StructInput<F> = { readonly [K in keyof F]: In<F[K]> };

/** The type a value of declared type T has inside a body. */
export type Out<T> =
  T extends Token<infer S>
    ? Bodies[S]
    : T extends IfaceTag<infer F>
      ? Handle<F>
      : T extends StructClass
        ? InstanceType<T>
        : T extends readonly (infer U)[]
          ? SolArray<Out<U>>
          : never;
/** What may be passed where T is expected. */
export type In<T> =
  T extends Token<infer S>
    ? Inputs[S]
    : T extends IfaceTag<infer F>
      ? Handle<F>
      : T extends StructClass
        ? InstanceType<T>
        : T extends readonly (infer U)[]
          ? SolArray<Out<U>>
          : never;
export type OutArgs<T extends readonly unknown[]> = { -readonly [I in keyof T]: Out<T[I]> };
export type InArgs<T extends readonly unknown[]> = { -readonly [I in keyof T]: In<T[I]> };
export type Returned<T extends readonly unknown[]> = T extends readonly []
  ? // biome-ignore lint/suspicious/noConfusingVoidType: the return type of a function that returns nothing
    void
  : T extends readonly [infer O]
    ? Out<O>
    : OutArgs<T>;
export type Returns<T extends readonly unknown[]> = T extends readonly []
  ? // biome-ignore lint/suspicious/noConfusingVoidType: the return type of a function that returns nothing
    void
  : T extends readonly [infer O]
    ? In<O>
    : InArgs<T>;

/** What a call may be given besides its arguments: `IPriceFeed(feed, { gas: 100_000 })`. */
export interface CallOptions {
  gas: bigint | number;
}
export type Handle<F extends Record<string, Sig>> = {
  readonly [K in keyof F]: (...args: InArgs<F[K]["inputs"]>) => Returned<F[K]["outputs"]>;
};
/**
 * An interface value: call it on an address to get a handle; its members give selectors and calldata.
 * Solidity converts a handle only to its own interface, so a handle of another interface goes through
 * its address: `I(address(handle))`.
 */
export type Iface<F extends Record<string, Sig>> = IfaceTag<F> &
  ((at: address | Address | Handle<F>, options?: CallOptions) => Handle<F>) & {
    readonly [K in keyof F]: { readonly selector: bytes4; encode(...args: InArgs<F[K]["inputs"]>): bytes };
  };

// ---- the ABI viem consumes, computed from the declaration alone --------------------------------------
type AbiName<T> =
  T extends Token<infer S>
    ? S
    : // biome-ignore lint/suspicious/noExplicitAny: matches an interface of any function set
      T extends IfaceTag<any>
      ? "address"
      : T extends readonly (infer U)[]
        ? `${AbiName<U>}[]`
        : "tuple";
// biome-ignore lint/suspicious/noExplicitAny: matches an interface of any function set
type Leaf<T> = T extends Token | IfaceTag<any> | StructClass ? T : T extends readonly (infer U)[] ? Leaf<U> : T;
type FieldsOf<T> = T extends StructClass<infer F> ? F : T;
export type Param<N, T> =
  // biome-ignore lint/suspicious/noExplicitAny: matches an interface of any function set
  Leaf<T> extends Token | IfaceTag<any>
    ? { readonly name: N; readonly type: AbiName<T> }
    : { readonly name: N; readonly type: AbiName<T>; readonly components: Components<FieldsOf<Leaf<T>>> };
/**
 * An array of the fields, not a tuple: object keys carry no order at the type level. viem does not
 * need the order to type a struct whose components are all named.
 */
export type Components<F> = readonly { [K in keyof F]: Param<K, F[K]> }[keyof F][];
/** A parameter's own name comes from the implementation, which the declaration cannot see. */
type Params<T extends readonly unknown[]> = { readonly [I in keyof T]: Param<string, T[I]> };
export type AbiFunction<Name extends string, S extends Sig> = {
  readonly type: "function";
  readonly name: Name;
  readonly stateMutability: S["mutability"];
  readonly inputs: Params<S["inputs"]>;
  readonly outputs: Params<S["outputs"]>;
};
export type AbiOf<Fns extends Record<string, Sig>> = readonly {
  [K in keyof Fns & string]: AbiFunction<K, Fns[K]>;
}[keyof Fns & string][];

// ---- implementations ------------------------------------------------------------------------------------
export type Impl<F extends Record<string, Sig>> = {
  [K in keyof F]: (...args: OutArgs<F[K]["inputs"]>) => Returns<F[K]["outputs"]>;
};
/** What an immutable may hold: a value type. Solidity has no immutable `bytes` or `string`. */
// biome-ignore lint/suspicious/noExplicitAny: an immutable may hold an interface of any function set
export type ImmutableType = Token<Exclude<Elementary, "bytes" | "string">> | IfaceTag<any>;
/** What `immutable(T)` returns: a value of T inside bodies, carrying its declaration for `contract`. */
export type Immutable<T extends ImmutableType> = Out<T> & { readonly [IMMUTABLE]: T };
export type Immutables = Record<string, { readonly [IMMUTABLE]: ImmutableType }>;
export type ImmutableTypes<Im extends Immutables> = { readonly [K in keyof Im]: Im[K][typeof IMMUTABLE] };
export type Library<F extends Record<string, Sig>> = Handle<F> & { readonly kind: "library" };

/** What `Lens.with({ … })` takes for an immutable of type T. This one runs, so it is viem's value. */
export type ImmutableValue<T> =
  T extends Token<infer S>
    ? S extends IntegerName
      ? bigint
      : S extends "bool"
        ? boolean
        : S extends "address"
          ? Address
          : Hex
    : Address;
/** The shape viem's deployless calls take, and viem-dlc's lens reader passes on. */
export interface DeploylessCall<A> {
  abi: A;
  /** The lens's counterfactual address: what `factory` deploys when called with `factoryData`. */
  address: Address;
  factory: Address;
  factoryData: Hex;
}
/** A compiled lens. Without a build integration, every member but `name` throws and names the integrations. */
export interface Lens<N extends string, F extends Record<string, Sig>, Im> {
  readonly name: N;
  readonly abi: AbiOf<F>;
  /** Creation bytecode, without constructor arguments. */
  readonly bytecode: Hex;
  /** Immutables by name; they become the constructor arguments in the order `contract` lists them. */
  with(
    ...args: Record<never, never> extends Im ? [] : [immutables: { readonly [K in keyof Im]: ImmutableValue<Im[K]> }]
  ): DeploylessCall<AbiOf<F>>;
}

// ---- typed Yul ---------------------------------------------------------------------------------------------
/** Inside assembly every value is one 256-bit word. */
export type Word = bigint | number | boolean | address | bytes32 | bytes4 | bytes | string;
type Op0 = () => uint256;
type Op1 = (a: Word) => uint256;
type Op2 = (a: Word, b: Word) => uint256;
type Op3 = (a: Word, b: Word, c: Word) => uint256;
type Copy = (to: Word, from: Word, size: Word) => void;
/** The opcodes `@morpho-org/viem-dlc/solidity/yul` exports. None of them writes state. */
export interface Yul {
  add: Op2;
  sub: Op2;
  mul: Op2;
  div: Op2;
  sdiv: Op2;
  mod: Op2;
  smod: Op2;
  exp: Op2;
  signextend: Op2;
  addmod: Op3;
  mulmod: Op3;
  lt: Op2;
  gt: Op2;
  slt: Op2;
  sgt: Op2;
  eq: Op2;
  iszero: Op1;
  and: Op2;
  or: Op2;
  xor: Op2;
  not: Op1;
  byte: Op2;
  shl: Op2;
  shr: Op2;
  sar: Op2;
  keccak256: Op2;
  mload: Op1;
  mstore: (ptr: Word, value: Word) => void;
  mstore8: (ptr: Word, value: Word) => void;
  mcopy: Copy;
  calldataload: Op1;
  calldatasize: Op0;
  calldatacopy: Copy;
  returndatasize: Op0;
  returndatacopy: Copy;
  staticcall: (gas: Word, target: Word, inPtr: Word, inSize: Word, outPtr: Word, outSize: Word) => uint256;
  extcodesize: Op1;
  balance: Op1;
  selfbalance: Op0;
  address: Op0;
  caller: Op0;
  timestamp: Op0;
  number: Op0;
  chainid: Op0;
  gas: Op0;
  pop: (x: Word) => void;
}
type YulType = Bytes32Token | Token<"bytes"> | Token<"string">;
/**
 * A block that ends in `return word` is a value: a uint256, or the type named first (the word is then
 * the bytes32 itself, or the memory pointer of the string or bytes). The type is an argument and not a
 * type parameter because a type parameter is erased before the transform reads the source. A block
 * with no `return` is a statement. Every block is `"memory-safe"`, and says so.
 */
export interface Assembly {
  <T extends YulType>(type: T, flag: "memory-safe", block: () => bigint): Out<T>;
  (flag: "memory-safe", block: () => bigint): uint256;
  (flag: "memory-safe", block: () => void): void;
}

// ---- built-ins -----------------------------------------------------------------------------------------------
export type Tokens = { [K in IntegerName]: IntToken<K> } & {
  address: AddressToken;
  bytes32: Bytes32Token;
  bool: Token<"bool">;
  bytes4: Token<"bytes4">;
  bytes: Token<"bytes">;
  string: Token<"string">;
};
export interface Abi {
  encode(...values: unknown[]): bytes;
  /** `abi.decode(ret, [uint128, uint128])` is `abi.decode(ret, (uint128, uint128))`. */
  decode<const T extends readonly SolType[]>(data: bytes, types: T): Returned<T>;
}
export interface Block {
  readonly timestamp: uint256;
  readonly number: uint256;
}
