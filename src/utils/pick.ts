import type { Prettify } from "viem";

export function pick<T extends object, K extends readonly (keyof T)[]>(x: T, keys: K): Prettify<Pick<T, K[number]>> {
  return Object.fromEntries(keys.map((key) => [key, x[key]])) as Prettify<Pick<T, K[number]>>;
}
