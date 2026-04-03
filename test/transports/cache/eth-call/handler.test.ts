import { toHex } from "viem";
import { describe, expect, it, vi } from "vitest";

import { LazyNdjsonMap } from "../../../../src/internal/index.js";
import { MemoryStore } from "../../../../src/stores/memory.js";
import { handleEthCall } from "../../../../src/transports/cache/eth-call/handler.js";
import {
  ETH_CALL_CACHE_POLICY_ADDRESS,
  extractEthCallCachePolicy,
} from "../../../../src/transports/cache/eth-call/state-override.js";
import type { CachedEthCallEntry } from "../../../../src/transports/cache/eth-call/types.js";
import { keychain } from "../../../../src/transports/cache/keychain.js";
import type { CacheSchema } from "../../../../src/transports/cache/schema.js";
import type { HandlerContext } from "../../../../src/transports/cache/types.js";
import type { EIP1193Parameters } from "../../../../src/types.js";
import { createCoalescingMutex } from "../../../../src/utils/coalescing-mutex.js";
import { parse, stringify } from "../../../../src/utils/json.js";

type EthCallRequest = EIP1193Parameters<CacheSchema, "eth_call">;

const codec = { toJson: stringify, fromJson: parse } as const;
const chainId = 1;
const ttl = 60_000;
const to = "0x1234567890123456789012345678901234567890" as const;
const data = "0xabcdef" as const;

function createReq(withBlobKey = true): EthCallRequest {
  return {
    method: "eth_call",
    params: withBlobKey
      ? [
          { to, data },
          "latest",
          { [ETH_CALL_CACHE_POLICY_ADDRESS]: { code: toHex(JSON.stringify({ blobKey: "test-blob", ttl })) } },
        ]
      : [{ to, data }, "latest"],
  };
}

async function populateStore(store: MemoryStore, req: ReturnType<typeof createReq>, value: CachedEthCallEntry) {
  const blobKey = keychain.blobKey(chainId, req)!;
  let buffers = store.get(blobKey) ?? [];
  const ndjson = new LazyNdjsonMap<CachedEthCallEntry>(
    codec,
    { debounceMs: 86_400_000, maxDelayMs: 86_400_000, maxStalenessMs: 86_400_000 },
    {
      get: () => buffers,
      set: (next) => {
        buffers = next;
        store.set(blobKey, next);
      },
    },
  );

  const cleanStateOverride = extractEthCallCachePolicy(req.params[2])?.cleanStateOverride;
  const entryKey = keychain.entryKey(chainId, "eth_call", {
    to,
    data,
    block: req.params[1],
    stateOverride: cleanStateOverride,
    blockOverride: req.params[3],
  });
  ndjson.upsert([{ key: entryKey.data, value }]);
  await ndjson.flush();
}

describe("handleEthCall", () => {
  it("passes through when blobKey is omitted", async () => {
    const { coalesce } = createCoalescingMutex();
    const requestFn = vi.fn().mockResolvedValue("0x1234");

    const result = await handleEthCall(
      { store: new MemoryStore(), coalesce, requestFn, chainId, binSize: 10_000, invalidationStrategy: () => 0 },
      createReq(false),
    );

    expect(result).toBe("0x1234");
    expect(requestFn).toHaveBeenCalledWith({ method: "eth_call", params: [{ to, data }, "latest"] }, { dedupe: true });
  });

  it("fetches and caches a direct call on miss", async () => {
    const store = new MemoryStore();
    const { coalesce } = createCoalescingMutex();
    const requestFn = vi.fn().mockResolvedValue("0x1234");

    const result = await handleEthCall(
      { store, coalesce, requestFn, chainId, binSize: 10_000, invalidationStrategy: () => 0 },
      createReq(),
    );

    expect(result).toBe("0x1234");
    expect(requestFn).toHaveBeenCalledTimes(1);
    expect(store.get(keychain.blobKey(chainId, createReq())!)).not.toBeNull();
  });

  it("returns a cached direct call without fetching", async () => {
    const store = new MemoryStore();
    const { coalesce } = createCoalescingMutex();
    const requestFn = vi.fn() as unknown as HandlerContext["requestFn"];
    const req = createReq();

    await populateStore(store, req, {
      success: true,
      returnData: "0x1234",
      fetchedAt: Date.now(),
    });

    const result = await handleEthCall(
      { store, coalesce, requestFn, chainId, binSize: 10_000, invalidationStrategy: () => 0 },
      req,
    );

    expect(result).toBe("0x1234");
    expect(requestFn).not.toHaveBeenCalled();
  });
});
