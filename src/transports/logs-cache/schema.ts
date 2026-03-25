import type { PublicRpcSchema } from "viem";

import type { EIP1193Parameters } from "../../types.js";

export type LogsCacheRpcSchema = PublicRpcSchema;

export const cachedMethods = [
  "eth_call",
  "eth_getLogs",
] as const satisfies EIP1193Parameters<LogsCacheRpcSchema>["method"][];

export type CachedMethod = (typeof cachedMethods)[number];
