import { describe, expect, it } from "vitest";

import { keychain } from "../../../src/transports/cache/keychain.js";
import type { CacheSchema } from "../../../src/transports/cache/schema.js";
import type { EIP1193Parameters } from "../../../src/types.js";

import { chainId, entryKey } from "./eth-get-logs/_helpers.js";

type EthGetLogsRequest = EIP1193Parameters<CacheSchema, "eth_getLogs">;

describe("keychain", () => {
  it("zero-pads entry keys to 20 digits", () => {
    const key = entryKey(0n, 9999n);
    expect(key.metadata).toBe("0:00000000000000000000:00000000000000009999");
    expect(key.data).toBe("1:00000000000000000000:00000000000000009999");
  });

  it("produces lexicographically correct order across digit-length boundaries", () => {
    const key9k = entryKey(9000n, 9999n);
    const key10k = entryKey(10000n, 19999n);
    expect(key9k.data < key10k.data).toBe(true);
  });

  it("generates deterministic blob keys", () => {
    const req = {
      method: "eth_getLogs" as const,
      params: [
        { address: "0x1234567890123456789012345678901234567890", topics: ["0xabc"] },
      ] as unknown as EthGetLogsRequest["params"],
    };
    expect(keychain.blobKey(chainId, req)).toBe(keychain.blobKey(chainId, req));
  });
});
