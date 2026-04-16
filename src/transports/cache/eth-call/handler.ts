import type { Hex } from "viem";

import { LinesBlobCompressed } from "../../../internal/lines-blob-compressed.js";
import { NdjsonMapLazy } from "../../../internal/ndjson-map-lazy.js";
import type { EIP1193Parameters } from "../../../types.js";
import { cyrb64Hash } from "../../../utils/hash.js";
import { parse, stringify } from "../../../utils/json.js";
import { keychain } from "../keychain.js";
import type { CacheSchema } from "../schema.js";
import type { HandlerContext } from "../types.js";

import { buildArrayEncoding, buildTargetCalldata, sliceArrayElements, sliceInputArray } from "./array-codec.js";
import { packByCalldataBytes } from "./batch-packer.js";
import { unwrapDeploylessFactoryCall, wrapDeploylessFactoryCall } from "./deployless.js";
import { extractEthCallCachePolicy } from "./state-override.js";
import type { CachedEthCallEntry } from "./types.js";

export async function handleEthCall(
  { store, coalesce, requestFn, chainId }: HandlerContext,
  req: EIP1193Parameters<CacheSchema, "eth_call">,
): Promise<Hex> {
  const extracted = extractEthCallCachePolicy(req.params[2]);
  if (!extracted) {
    return requestFn({ method: req.method, params: req.params });
  }

  const blobKey = keychain.blobKey(chainId, req)!;
  const { ttl, delta, batchSize } = extracted.policy;
  const { resolved, cleanStateOverride } = extracted;

  const txObj = req.params[0];
  const block = req.params[1];
  const blockOverride = req.params[3];

  if (txObj.data === undefined) {
    throw new Error("[cache] eth_call with policy requires `data`");
  }
  const definedKeys = Object.keys(txObj).filter((k) => (txObj as Record<string, unknown>)[k] !== undefined);
  if (definedKeys.length > 1) {
    const extras = definedKeys.filter((k) => k !== "data");
    throw new Error(
      `[cache] eth_call with policy: tx object may only set \`data\` (found extras: ${extras.join(", ")}) — ` +
        `cache keys index only on deployless factory parts, so envelope fields must not affect execution`,
    );
  }

  const { targetTo, targetData, factory, factoryData } = unwrapDeploylessFactoryCall(txObj.data);
  const inputElements = sliceInputArray(resolved, targetData);
  const n = inputElements.length;

  if (n === 0) {
    // Fast path: empty input → empty output. No RPC, no cache access.
    return buildArrayEncoding(resolved.outputLayout, []);
  }

  const reqHash = cyrb64Hash(JSON.stringify(req.params));

  return coalesce(blobKey, req, async (_leaderReq, collectFollowers) => {
    /*//////////////////////////////////////////////////////////////
                               LEADER OPS
    //////////////////////////////////////////////////////////////*/

    // Dedup identical input elements so repeated keys map to a single blob entry.
    const keyToInfo = new Map<string, { indices: number[]; element: Hex }>();
    for (let i = 0; i < n; i++) {
      const element = inputElements[i]!;
      const ek = keychain.entryKey(chainId, "eth_call", {
        targetTo,
        factory,
        factoryData,
        selector: resolved.selector,
        inputElement: element,
        block,
        stateOverride: cleanStateOverride,
        blockOverride,
      }).data;
      const existing = keyToInfo.get(ek);
      if (existing) {
        existing.indices.push(i);
      } else {
        keyToInfo.set(ek, { indices: [i], element });
      }
    }

    // Open blob lazily — read once, buffer writes, flush when done.
    let buffers = (await store.get(blobKey)) ?? [];
    const ndjson = new NdjsonMapLazy<CachedEthCallEntry>(
      { toJson: stringify, fromJson: parse },
      new LinesBlobCompressed({
        get: () => buffers,
        set: (value) => {
          buffers = value;
          void store.set(blobKey, value);
        },
      }),
      { debounceMs: 500, maxDelayMs: 2_500 },
    );

    const hits = new Array<Hex>(n);
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

      const wrapMiss = (elements: readonly Hex[]): Hex =>
        wrapDeploylessFactoryCall({
          targetTo,
          targetData: buildTargetCalldata(resolved, elements),
          factory,
          factoryData,
        });

      // Per-miss byte contribution: static layouts contribute a constant `layout.size`;
      // dynamic layouts contribute one offset word plus the already-padded element bytes.
      // Both are multiples of 32, so the outer `bytes` wrapper padding for `targetData`
      // stays invariant and `overhead = referenceBytes - sum(perMissBytes)` is an exact
      // per-batch constant.
      const perMissBytes = misses.map((m) =>
        resolved.inputLayout.mode === "static" ? resolved.inputLayout.size : 32 + (m.element.length - 2) / 2,
      );
      const referenceWrapped = wrapMiss(misses.map((m) => m.element));
      const referenceBytes = (referenceWrapped.length - 2) / 2;
      const overheadBytes = referenceBytes - perMissBytes.reduce((a, b) => a + b, 0);

      const ranges = packByCalldataBytes(perMissBytes, overheadBytes, batchSize);

      const allEntries: { key: string; value: CachedEthCallEntry }[] = [];

      const fetchChunk = async ([start, end]: readonly [number, number]) => {
        const chunkWrapped =
          ranges.length === 1 ? referenceWrapped : wrapMiss(misses.slice(start, end).map((m) => m.element));

        const rpcResult = await requestFn({
          method: "eth_call",
          params:
            blockOverride !== undefined
              ? [{ data: chunkWrapped }, block, cleanStateOverride, blockOverride]
              : cleanStateOverride !== undefined
                ? [{ data: chunkWrapped }, block, cleanStateOverride]
                : [{ data: chunkWrapped }, block],
        });

        const chunkOutputs = sliceArrayElements(resolved.outputLayout, rpcResult);
        if (chunkOutputs.length !== end - start) {
          throw new Error(`[cache] eth_call returned ${chunkOutputs.length} output elements, expected ${end - start}`);
        }

        for (let j = 0; j < chunkOutputs.length; j++) {
          const miss = misses[start + j]!;
          const output = chunkOutputs[j]!;
          for (const idx of miss.indices) hits[idx] = output;
          allEntries.push({ key: miss.entryKey, value: { output, fetchedAt } });
        }
      };

      await Promise.all(ranges.map(fetchChunk));

      ndjson.upsert(allEntries);
      await ndjson.flush();
    }

    /*//////////////////////////////////////////////////////////////
                                FAN OUT
    //////////////////////////////////////////////////////////////*/

    const result = buildArrayEncoding(resolved.outputLayout, hits);

    const collected = collectFollowers();
    const matching = collected.filter((f) => cyrb64Hash(JSON.stringify(f.args.params)) === reqHash);

    return {
      leader: { action: "resolve", result },
      followers: matching.map((f) => ({ slot: f.slot, action: "resolve" as const, result })),
    };
  });
}
