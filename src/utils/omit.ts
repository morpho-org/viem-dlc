import type { Prettify } from "viem";

export function omit<T extends object, K extends readonly (keyof T)[]>(x: T, keys: K): Prettify<Omit<T, K[number]>> {
  const y = { ...x };
  for (const key of keys) {
    delete y[key];
  }
  return y;
}
