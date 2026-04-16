import type { Hex } from "viem";

import { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import type { EIP1193Parameters } from "../../../types.js";
import { cyrb64Hash } from "../../../utils/hash.js";
import {
  arrayToHex,
  calldataToArray,
  type EthCallRpcContext,
  factorisedFactoryCall,
  resolveArrayFunction,
  unwrapDeploylessFactoryCall,
} from "../../../utils/index.js";
import { parse, stringify } from "../../../utils/json.js";
import { extractEthCallPolicy } from "../../state-overrides.js";
import { keychain } from "../keychain.js";
import type { CacheSchema } from "../schema.js";
import type { HandlerContext } from "../types.js";

import type { CachedEthCallEntry } from "./types.js";

export async function handleEthCall(
  { store, coalesce, requestFn, chainId }: HandlerContext,
  req: EIP1193Parameters<CacheSchema, "eth_call">,
): Promise<Hex> {
  const [txn, block, , blockOverride] = req.params;
  const extracted = extractEthCallPolicy(req.params[2]);

  /*//////////////////////////////////////////////////////////////
                          INPUT VALIDATION
  //////////////////////////////////////////////////////////////*/

  if (!extracted) {
    return requestFn({ method: req.method, params: req.params });
  }
  if (txn.data === undefined) {
    throw new Error("[cache] eth_call with policy requires `data`");
  }
  {
    const txnKeys = Object.keys(txn).filter((k) => txn[k as keyof typeof txn] !== undefined);
    if (txnKeys.length > 1) {
      const extras = txnKeys.filter((k) => k !== "data");
      throw new Error(
        `[cache] eth_call with policy: tx object may only set \`data\` (found extras: ${extras.join(", ")})`,
      );
    }
  }

  const { stateOverride, policy } = extracted;
  const solidity = resolveArrayFunction(policy.abi);
  const { target, targetData } = unwrapDeploylessFactoryCall(txn.data);
  const inputElements = calldataToArray(solidity, targetData);

  if (inputElements.length === 0) {
    return arrayToHex(solidity.outputLayout, []);
  }

  const blobKey = keychain.blobKey(chainId, req);
  const { ttl, delta } = policy.cache ?? {};
  const rpcContext: EthCallRpcContext = { block, stateOverride, blockOverride };

  // No TTL → caching disabled. Still honor `batchSize` by splitting the call, but skip
  // all cache reads, writes, coalescing, and dedup.
  if (!blobKey || ttl === undefined) {
    const outputs = await factorisedFactoryCall(requestFn, {
      elements: inputElements,
      solidity,
      batchSize: policy.batchSize,
      target,
      rpcContext,
    });
    return arrayToHex(solidity.outputLayout, outputs);
  }

  return coalesce(blobKey, req, async (_leaderReq, collectFollowers) => {
    /*//////////////////////////////////////////////////////////////
                               LEADER OPS
    //////////////////////////////////////////////////////////////*/

    // Dedup identical input elements so repeated keys map to a single blob entry.
    const keyToInfo = new Map<string, { indices: number[]; element: Hex }>();
    inputElements.forEach((element, i) => {
      const ek = keychain.entryKey(chainId, "eth_call", {
        targetTo: target.address,
        factory: target.factory,
        factoryData: target.factoryData,
        selector: solidity.selector,
        inputElement: element,
        block,
        stateOverride,
        blockOverride,
      }).data;
      const existing = keyToInfo.get(ek);
      if (existing) {
        existing.indices.push(i);
      } else {
        keyToInfo.set(ek, { indices: [i], element });
      }
    });

    // Open blob lazily — read once, buffer writes, flush when done.
    let buffers = (await store.get(blobKey)) ?? [];
    const ndjson = new LazyNdjsonMap<CachedEthCallEntry>(
      { toJson: stringify, fromJson: parse },
      {
        get: () => buffers,
        set: (value) => {
          buffers = value;
          void store.set(blobKey, value);
        },
      },
      { debounceMs: 500, maxDelayMs: 2_500 },
    );

    const hits = new Array<Hex>(inputElements.length);
    const misses: { entryKey: string; indices: number[]; element: Hex }[] = [];
    const now = Date.now();

    await ndjson.scan((record) => {
      const match = keyToInfo.get(record.key);
      if (!match) return;
      keyToInfo.delete(record.key);

      const age = now - record.value.fetchedAt;
      const xfetch = delta ? delta * Math.log(1 - Math.random()) : 0;
      if (age - xfetch < ttl) {
        for (const idx of match.indices) hits[idx] = record.value.output;
      } else {
        misses.push({ entryKey: record.key, ...match });
      }

      if (keyToInfo.size === 0) return false;
    });

    for (const [entryKey, info] of keyToInfo) {
      misses.push({ entryKey, ...info });
    }

    // Fetch misses
    if (misses.length > 0) {
      const fetchedAt = Date.now();

      const outputs = await factorisedFactoryCall(requestFn, {
        elements: misses.map((m) => m.element),
        solidity,
        batchSize: policy.batchSize,
        target,
        rpcContext,
      });

      const allEntries = misses.map((miss, i) => {
        const output = outputs[i]!;
        for (const idx of miss.indices) hits[idx] = output;
        return { key: miss.entryKey, value: { output, fetchedAt } };
      });

      ndjson.upsert(allEntries);
      await ndjson.flush();
    }

    /*//////////////////////////////////////////////////////////////
                                FAN OUT
    //////////////////////////////////////////////////////////////*/

    const result = arrayToHex(solidity.outputLayout, hits);

    const leaderHash = cyrb64Hash(JSON.stringify(req.params));
    const collected = collectFollowers();
    const matching = collected.filter((f) => cyrb64Hash(JSON.stringify(f.args.params)) === leaderHash);

    return {
      leader: { action: "resolve", result },
      followers: matching.map((f) => ({ slot: f.slot, action: "resolve" as const, result })),
    };
  });
}
