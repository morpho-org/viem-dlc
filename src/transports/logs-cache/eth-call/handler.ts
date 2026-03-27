import type { Address, EIP1193RequestFn, Hex } from "viem";

import { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import type { RpcSignature, Store } from "../../../types.js";
import { parse, stringify } from "../../../utils/json.js";
import type { LogsDividerRpcSchema } from "../../logs-divider/schema.js";
import { keychain } from "../keychain.js";
import type { LogsCacheRpcSchema } from "../schema.js";

import {
  type Call3,
  decodeAggregate3,
  decodeAggregate3Result,
  encodeAggregate3,
  encodeAggregate3Result,
  isMulticall3,
} from "./multicall3.js";
import type { CachedEthCallEntry } from "./types.js";

export async function handleEthCall(
  requestFn: EIP1193RequestFn<LogsDividerRpcSchema>,
  chainId: number,
  params: RpcSignature<LogsCacheRpcSchema, "eth_call">["Parameters"],
  blobKey: string,
  { store }: { store: Store },
): Promise<Hex> {
  // Step 1: Extract params & detect multicall
  const txObj = params[0];
  const block = params[1];
  const stateOverride = params[2];
  const blockOverride = params[3];
  const ttl = params[4]!.ttl;

  const { to, data, ...rest } = txObj;

  if (to === undefined || data === undefined) {
    throw new Error("[logsCache] eth_call with blobKey requires `to` and `data`");
  }

  const multicall = isMulticall3(to as Address, data);
  const subCalls: Call3[] = multicall
    ? decodeAggregate3(data)
    : [{ target: to as Address, callData: data, allowFailure: false }];

  // Fast path: empty multicall
  if (subCalls.length === 0) {
    return encodeAggregate3Result([]);
  }

  // Step 2: Compute entry keys (handle duplicate sub-calls)
  const keyToInfo = new Map<string, { indices: number[]; subCall: Call3 }>();

  for (let i = 0; i < subCalls.length; i++) {
    const sub = subCalls[i]!;
    const ek = keychain.entryKey(chainId, "eth_call", {
      to: sub.target,
      data: sub.callData,
      block,
      rest,
      stateOverride,
      blockOverride,
    });
    const existing = keyToInfo.get(ek.data);
    if (existing) {
      existing.indices.push(i);
    } else {
      keyToInfo.set(ek.data, { indices: [i], subCall: sub });
    }
  }

  // Step 3: Open blob, scan for hits
  let buffers = (await store.get(blobKey)) ?? [];
  const ndjson = new LazyNdjsonMap<CachedEthCallEntry>(
    { toJson: stringify, fromJson: parse },
    { autoFlushThresholdBytes: 1 << 20 }, // 1MB
    {
      get: () => buffers,
      set: (value) => {
        buffers = value;
        void store.set(blobKey, value);
      },
    },
  );

  const hits = new Array<CachedEthCallEntry>(subCalls.length);
  const misses: { entryKey: string; indices: number[]; subCall: Call3 }[] = [];

  const now = Date.now();

  for await (const record of ndjson.records()) {
    const match = keyToInfo.get(record.key);
    if (!match) continue;
    keyToInfo.delete(record.key);

    if (now - record.value.fetchedAt < ttl) {
      for (const idx of match.indices) hits[idx] = record.value;
    } else {
      misses.push({ entryKey: record.key, ...match });
    }

    if (keyToInfo.size === 0) break;
  }

  // Keys not found in blob at all
  for (const [entryKey, info] of keyToInfo) {
    misses.push({ entryKey, ...info });
  }

  // Step 4: Fetch misses
  if (misses.length > 0) {
    const fetchedAt = Date.now();

    if (multicall) {
      // Re-aggregate misses into one multicall3 call
      const missedCalls = misses.map((m) => m.subCall);
      const calldata = encodeAggregate3(missedCalls);

      const rpcResult = await requestFn({
        method: "eth_call",
        params: [{ ...rest, to, data: calldata }, block, stateOverride, blockOverride] as [
          (typeof params)[0],
          (typeof params)[1],
          (typeof params)[2],
          (typeof params)[3],
        ],
      });
      const decoded = decodeAggregate3Result(rpcResult);

      const entries = misses.map((miss, i) => {
        const result: CachedEthCallEntry = {
          success: decoded[i]!.success,
          returnData: decoded[i]!.returnData,
          fetchedAt,
        };
        for (const idx of miss.indices) hits[idx] = result;
        return { key: miss.entryKey, value: result };
      });
      ndjson.upsert(entries);
    } else {
      // Direct eth_call (single sub-call)
      const rpcResult = await requestFn({
        method: "eth_call",
        params: [txObj, block, stateOverride, blockOverride] as [
          (typeof params)[0],
          (typeof params)[1],
          (typeof params)[2],
          (typeof params)[3],
        ],
      });
      const result: CachedEthCallEntry = { success: true, returnData: rpcResult, fetchedAt };
      ndjson.upsert([{ key: misses[0]!.entryKey, value: result }]);
      hits[0] = result;
    }

    await ndjson.flush();
  }

  // Step 5: Assemble response
  if (multicall) {
    return encodeAggregate3Result(hits.map((h) => ({ success: h.success, returnData: h.returnData })));
  }
  return hits[0]!.returnData;
}
