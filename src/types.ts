import type { BlockNumber, BlockTag, EIP1193RequestFn, Hex, PublicRpcSchema } from "viem";

export type BlockNumberish = BlockNumber | BlockTag | Hex;

export type BlockRange = { fromBlock: bigint; toBlock: bigint };

export type EIP1193RequestOptions = Parameters<EIP1193RequestFn>["1"];

export type EIP1193PublicRequestFn = EIP1193RequestFn<PublicRpcSchema>;

export type EthGetLogsSchema = Extract<PublicRpcSchema[number], { Method: "eth_getLogs" }>;

export type EthGetLogsParams = EthGetLogsSchema["Parameters"][0];

export type EthGetLogsParamsWithoutBlockHash = Omit<EthGetLogsParams, "blockHash">;

export interface Store {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
  delete(key: string): Promise<void>;
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface Cache<T extends {}> {
  read(keys: string[]): Promise<(T | undefined)[]>;
  write(items: { key: string; value: T }[]): Promise<void>;
}
