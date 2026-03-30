import type { Address, Hex } from "viem";

import { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import type { EIP1193Parameters } from "../../../types.js";
import { cyrb64Hash } from "../../../utils/hash.js";
import { parse, stringify } from "../../../utils/json.js";
import { keychain } from "../keychain.js";
import type { CacheSchema } from "../schema.js";
import type { HandlerContext } from "../types.js";

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
  { store, coalesce, requestFn, chainId }: HandlerContext,
  req: EIP1193Parameters<CacheSchema, "eth_call">,
): Promise<Hex> {
  const blobKey = keychain.blobKey(chainId, req);
  if (!blobKey) {
    return requestFn({
      method: req.method,
      params: [req.params[0], req.params[1], req.params[2], req.params[3]],
    });
  }

  // Step 1: Extract params & detect multicall
  const txObj = req.params[0];
  const block = req.params[1];
  const stateOverride = req.params[2];
  const blockOverride = req.params[3];
  const ttl = req.params[4]!.ttl;

  // Normalization strips txObj to { to, data } when blobKey is present
  const { to, data } = txObj;

  if (to === undefined || data === undefined) {
    throw new Error("[cache] eth_call with blobKey requires `to` and `data`");
  }

  const multicall = isMulticall3(to as Address, data);
  const subCalls: Call3[] = multicall
    ? decodeAggregate3(data)
    : [{ target: to as Address, callData: data, allowFailure: false }];

  // Fast path: empty multicall
  if (subCalls.length === 0) {
    return encodeAggregate3Result([]);
  }

  const reqHash = cyrb64Hash(JSON.stringify(req.params));

  return coalesce(blobKey, req, async (_leaderReq, collectFollowers) => {
    /*//////////////////////////////////////////////////////////////
                               LEADER OPS
    //////////////////////////////////////////////////////////////*/

    const keyToInfo = new Map<string, { indices: number[]; subCall: Call3 }>();

    for (let i = 0; i < subCalls.length; i++) {
      const sub = subCalls[i]!;
      const ek = keychain.entryKey(chainId, "eth_call", {
        to: sub.target,
        data: sub.callData,
        block,
        stateOverride,
        blockOverride,
      });
      const existing = keyToInfo.get(ek.data);
      if (existing) {
        existing.indices.push(i);
        // allowFailure:false is stricter -- if any duplicate requires revert-on-failure, all must
        if (!sub.allowFailure) existing.subCall = { ...existing.subCall, allowFailure: false };
      } else {
        keyToInfo.set(ek.data, { indices: [i], subCall: sub });
      }
    }

    // Step 3: Open blob, scan for hits
    let buffers = (await store.get(blobKey)) ?? [];
    const ndjson = new LazyNdjsonMap<CachedEthCallEntry>(
      { toJson: stringify, fromJson: parse },
      { autoFlushThresholdBytes: 1 << 26 }, // 64MB (flushing too often strains CPU, flushing too late strains memory)
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

      if (now - record.value.fetchedAt < ttl && (record.value.success || match.subCall.allowFailure)) {
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

        const rpcResult = await requestFn(
          {
            method: "eth_call",
            params: [{ to, data: calldata }, block, stateOverride, blockOverride] as [
              (typeof req.params)[0],
              (typeof req.params)[1],
              (typeof req.params)[2],
              (typeof req.params)[3],
            ],
          },
          { dedupe: true },
        );
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
        const rpcResult = await requestFn(
          {
            method: "eth_call",
            params: [txObj, block, stateOverride, blockOverride],
          },
          { dedupe: true },
        );
        const result: CachedEthCallEntry = { success: true, returnData: rpcResult, fetchedAt };
        ndjson.upsert([{ key: misses[0]!.entryKey, value: result }]);
        hits[0] = result;
      }

      await ndjson.flush();
    }

    /*//////////////////////////////////////////////////////////////
                                FAN OUT
    //////////////////////////////////////////////////////////////*/

    const result: Hex = multicall
      ? encodeAggregate3Result(hits.map((h) => ({ success: h.success, returnData: h.returnData })))
      : hits[0]!.returnData;

    const collected = collectFollowers();
    const matching = collected.filter((f) => cyrb64Hash(JSON.stringify(f.args.params)) === reqHash);

    return {
      leader: { action: "resolve", result },
      followers: matching.map((f) => ({ slot: f.slot, action: "resolve" as const, result })),
    };
  });
}
