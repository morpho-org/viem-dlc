// biome-ignore-all lint/suspicious/noExplicitAny: `any` is required for tuple pattern matching inference

export type Tuple = readonly unknown[];

export type Tuple2d = readonly Tuple[];

export type IsTuple2d<T extends Tuple> = Exclude<T[number], Tuple> extends never ? true : false;

/** Concatenation of `A` and `B`. */
export type Concat<A extends Tuple, B extends Tuple> = readonly [...A, ...B];

/** Like `T`, but with each element optional. */
export type Optionalize<T extends Tuple> = T extends readonly [infer H, ...infer R]
  ? readonly [H?, ...Optionalize<R>]
  : readonly [];

/**
 * Union of all prefixes of a tuple (including empty).
 *
 * @example
 * type inits = Inits<[X, Y, Z]>; // [] | [X] | [X, Y] | [X, Y, Z]
 */
export type Inits<T extends Tuple> = T extends readonly [infer H, ...infer R]
  ? readonly [] | readonly [H, ...Inits<R>]
  : readonly [];

/*//////////////////////////////////////////////////////////////
                        ELEMENTWISE UNIONS
//////////////////////////////////////////////////////////////*/

/** Type of `T[0]` **iff its length > 1**. */
type Head<T extends Tuple> = T extends readonly [infer H, ...any[]] ? H : never;

/** Union of types of `T.slice(1)` **iff its length > 1**. */
type Tail<T extends Tuple> = T extends readonly [any, ...infer R] ? R : never;

/** Whether `T.length > 1` */
type HasMultipleElements<T extends Tuple> = Exclude<T, readonly [any, ...any[]]> extends never ? true : false;

/**
 * The element-wise union of all tuples in union `T`.
 *
 * @example
 * type u = ElementwiseUnionUnion<[A] | [B, C]>; // [A | B, (C | undefined)?]
 */
export type ElementwiseUnionUnion<T extends Tuple, OptionalTail extends boolean = false> = Head<T> extends never
  ? readonly []
  : HasMultipleElements<T> extends true
    ? OptionalTail extends true
      ? readonly [Head<T>?, ...ElementwiseUnionUnion<Tail<T>, true>]
      : readonly [Head<T>, ...ElementwiseUnionUnion<Tail<T>, false>]
    : readonly [Head<T>?, ...ElementwiseUnionUnion<Tail<T>, true>];

/**
 * The element-wise union of all tuples in 2D tuple `T`.
 *
 * @example
 * type u = ElementwiseTupleUnion<[[A] | [B, C]]>; // [A | B, (C | undefined)?]
 */
export type ElementwiseTupleUnion<T extends Tuple2d> = ElementwiseUnionUnion<T[number]>;
