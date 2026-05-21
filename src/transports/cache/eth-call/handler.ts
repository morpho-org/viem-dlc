import type { Hex } from "viem";

import { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import type { EIP1193Parameters } from "../../../types.js";
import { cyrb64Hash } from "../../../utils/hash.js";
import {
  arrayToHex,
  calldataToArray,
  factorisedFactoryCall,
  resolveArrayFunction,
  unwrapDeploylessFactoryCall,
} from "../../../utils/index.js";
import { parse, stringify } from "../../../utils/json.js";
import { extractEthCallPolicy } from "../../state-overrides.js";
import { keychain } from "../keychain.js";
import { type CacheSchema, cacheTransportKey } from "../schema.js";
import type { HandlerContext } from "../types.js";

import type { CachedEthCallEntry } from "./types.js";

export async function handleEthCall(
  ctx: HandlerContext,
  req: EIP1193Parameters<CacheSchema, "eth_call">,
): Promise<Hex> {
  const { store, coalesce, requestFn, chainId } = ctx;

  const extracted = extractEthCallPolicy(req.params[2]);
  if (!extracted) {
    return requestFn(req);
  }

  const [txn, ...restOfEthCallParams] = req.params;
  if (txn.data === undefined) {
    throw new Error("[cache] eth_call with policy requires `data`");
  }
  {
    const txnKeys = Object.keys(txn).filter((k) => txn[k as keyof typeof txn] !== undefined);
    // `txn.data` must be the only field on `txn`
    if (txnKeys.length > 1) {
      const extras = txnKeys.filter((k) => k !== "data");
      throw new Error(
        `[cache] eth_call with policy: tx object may only set \`data\` (found extras: ${extras.join(", ")})`,
      );
    }
  }
  // `stateOverride` must be overwritten with the cleaned/extracted version.
  // trailing undefined args must be removed for RPC compatibility.
  if (restOfEthCallParams.length >= 2) {
    restOfEthCallParams[1] = extracted.stateOverride ?? (restOfEthCallParams[2] ? {} : undefined);
    const lastDefinedParamIdx = restOfEthCallParams.reduce((acc, x, i) => (x === undefined ? acc : i), -1);
    restOfEthCallParams.splice(lastDefinedParamIdx + 1);
  }

  const { target, targetData } = unwrapDeploylessFactoryCall(txn.data);
  const solidity = resolveArrayFunction(extracted.policy.abi);
  const inputElements = calldataToArray(solidity, targetData);

  const tag = `${cacheTransportKey}.${ctx.observability?.counter}.eth_call`;
  ctx.observability?.logger?.withContext({ [`${tag}.input_elements`]: inputElements.length });

  if (inputElements.length === 0) {
    return arrayToHex(solidity.outputLayout, []);
  }

  const blobKey = keychain.blobKey(chainId, req);
  const { ttl, delta } = extracted.policy.cache ?? {};

  // No TTL → caching disabled. Still honor `batch` by splitting the call, but skip
  // all cache reads, writes, coalescing, and dedup.
  if (!blobKey || ttl === undefined) {
    ctx.observability?.logger?.withContext({ [`${tag}.elements_fetched`]: inputElements.length });
    const outputs = await factorisedFactoryCall(requestFn, {
      target,
      elements: inputElements,
      solidity,
      batch: extracted.policy.batch,
      restOfEthCallParams,
    });
    return arrayToHex(solidity.outputLayout, outputs);
  }

  ctx.observability?.logger?.withContext({ [`${tag}.cache`]: { blobKey, ttl, delta } });
  return coalesce(blobKey, req, async (_leaderReq, collectFollowers) => {
    /*//////////////////////////////////////////////////////////////
                               LEADER OPS
    //////////////////////////////////////////////////////////////*/

    // Dedup identical input elements so repeated keys map to a single blob entry.
    const keyToInfo = new Map<string, { indices: number[]; element: Hex }>();
    inputElements.forEach((element, i) => {
      const ek = keychain.entryKey(chainId, "eth_call", {
        target,
        selector: solidity.selector,
        element,
        restOfEthCallParams,
      }).data;
      const existing = keyToInfo.get(ek);
      if (existing) {
        existing.indices.push(i);
      } else {
        keyToInfo.set(ek, { indices: [i], element });
      }
    });
    ctx.observability?.logger?.withContext({ [`${tag}.input_elements_unique`]: keyToInfo.size });

    // Open blob lazily — read once, buffer writes, flush when done.
    const t0 = performance.now();
    let buffers = (await store.get(blobKey)) ?? [];
    const t1 = performance.now();

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

    const t2 = performance.now();
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
    const t3 = performance.now();

    for (const [entryKey, info] of keyToInfo) {
      misses.push({ entryKey, ...info });
    }

    ctx.observability?.logger?.withContext({ [`${tag}.elements_fetched`]: misses.length });

    // Fetch misses
    if (misses.length > 0) {
      const fetchedAt = Date.now();

      const outputs = await factorisedFactoryCall(requestFn, {
        target,
        elements: misses.map((m) => m.element),
        solidity,
        batch: extracted.policy.batch,
        restOfEthCallParams,
      });

      const allEntries = misses.map((miss, i) => {
        const output = outputs[i]!;
        for (const idx of miss.indices) hits[idx] = output;
        return { key: miss.entryKey, value: { output, fetchedAt } };
      });

      const t4 = performance.now();
      ndjson.upsert(allEntries);
      await ndjson.flush();
      const t5 = performance.now();

      ctx.observability?.logger?.withContext({
        [`${tag}.duration_ms`]: { fetch_cache: t1 - t0, read_cache: t3 - t2, write_cache: t5 - t4 },
      });
    }

    /*//////////////////////////////////////////////////////////////
                                FAN OUT
    //////////////////////////////////////////////////////////////*/

    const result = arrayToHex(solidity.outputLayout, hits);

    const leaderHash = cyrb64Hash(JSON.stringify(req.params));
    const collected = collectFollowers();
    const matching = collected.filter((f) => cyrb64Hash(JSON.stringify(f.args.params)) === leaderHash);
    ctx.observability?.logger?.withContext({ [`${tag}.followers`]: matching.length });

    return {
      leader: { action: "resolve", result },
      followers: matching.map((f) => ({ slot: f.slot, action: "resolve" as const, result })),
    };
  });
}
