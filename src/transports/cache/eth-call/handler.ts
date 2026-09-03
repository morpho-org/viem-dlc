import type { Hex } from "viem";

import { LazyNdjsonMap } from "../../../internal/lazy-ndjson-map.js";
import { getObservability } from "../../../observability.js";
import type { EIP1193Parameters } from "../../../types.js";
import { cyrb64Hash } from "../../../utils/hash.js";
import {
  calldataToArray,
  factorisedFactoryCall,
  pageToHex,
  type ResolvedArrayFunction,
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
  { store, coalesce, requestFn, chainId, facetId }: HandlerContext,
  req: EIP1193Parameters<CacheSchema, "eth_call">,
): Promise<Hex> {
  const extracted = extractEthCallPolicy(req.params[2]);
  if (!extracted) {
    return requestFn(req);
  }

  const facet = getObservability()?.facet(facetId).sub("eth_call");

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

  facet?.set({ input_elements: inputElements.length });

  if (inputElements.length === 0) {
    return encodeResponse(solidity, [], []);
  }

  const blobKey = keychain.blobKey(chainId, req);
  const { ttl, delta } = extracted.policy.cache ?? {};

  // No TTL → caching disabled. Still honor `batch` by splitting the call, but skip
  // all cache reads, writes, coalescing, and dedup.
  if (!blobKey || ttl === undefined) {
    const { outputs, missing } = await factorisedFactoryCall(requestFn, {
      target,
      elements: inputElements,
      solidity,
      batch: extracted.policy.batch,
      restOfEthCallParams,
      facet,
    });
    return encodeResponse(solidity, outputs, missing);
  }

  facet?.set({ blob_key: blobKey, ttl_ms: ttl, delta_ms: delta });
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
    facet?.set({ input_elements_unique: keyToInfo.size });

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
    const unservable: number[] = [];
    const unresolved: number[] = [];
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

    // `factorisedFactoryCall` overwrites both when it runs; these cover the zero-misses
    // (full cache hit) case, where it doesn't run at all.
    facet?.set({ elements_requested: misses.length, elements_fetched: 0 });

    // Fetch misses
    if (misses.length > 0) {
      const fetchedAt = Date.now();

      try {
        const fetchedResult = await factorisedFactoryCall(requestFn, {
          target,
          elements: misses.map((m) => m.element),
          solidity,
          batch: extracted.policy.batch,
          restOfEthCallParams,
          facet,
          // Buffer per chunk, so a later chunk failing doesn't discard the siblings that landed.
          onResolved: (entries) => {
            ndjson.upsert(
              entries.map(({ index, output }) => {
                const miss = misses[index]!;
                for (const idx of miss.indices) hits[idx] = output;
                return { key: miss.entryKey, value: { output, fetchedAt } };
              }),
            );
          },
        });
        // `missing` indexes deduped misses; callers expect indices into their own input array.
        let oversize = 0;
        for (const i of fetchedResult.missing) unservable.push(...misses[i]!.indices);
        for (const i of fetchedResult.unresolved) unresolved.push(...misses[i]!.indices);
        for (const i of fetchedResult.oversize) oversize += misses[i]!.indices.length;
        // Deduping means one unservable entry can stand for several caller inputs; restamp so
        // the fields match the `skipped` array the response actually carries.
        facet?.set({
          elements_missing: unservable.length,
          elements_unresolved: unresolved.length,
          elements_declined_oversize: oversize,
        });
      } finally {
        const t4 = performance.now();
        await ndjson.flush();
        facet?.set({ fetch_cache_ms: t1 - t0, read_cache_ms: t3 - t2, flush_cache_ms: performance.now() - t4 });
      }
    }

    /*//////////////////////////////////////////////////////////////
                                FAN OUT
    //////////////////////////////////////////////////////////////*/

    const result = encodeResponse(
      solidity,
      hits,
      unservable.sort((a, b) => a - b),
    );

    const leaderHash = cyrb64Hash(JSON.stringify(req.params));
    const collected = collectFollowers();
    const matching = collected.filter((f) => cyrb64Hash(JSON.stringify(f.args.params)) === leaderHash);
    facet?.set({ n_followers: matching.length });

    return {
      leader: { action: "resolve", result },
      followers: matching.map((f) => ({ slot: f.slot, action: "resolve" as const, result })),
    };
  });
}

/**
 * Encodes the aggregated `(U[] results, uint256[] skipped)` page, with `outputs` sparse at every
 * skipped index and `skipped` already expressed against the caller's input array.
 */
function encodeResponse(
  solidity: ResolvedArrayFunction,
  outputs: readonly (Hex | undefined)[],
  skipped: readonly number[],
): Hex {
  return pageToHex(solidity.outputLayout, { results: outputs.filter((o) => o !== undefined), skipped });
}
