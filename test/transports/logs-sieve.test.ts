import { custom, type Hex, type RpcLog, toHex } from "viem";
import { describe, expect, it, vi } from "vitest";

import { logsSieve } from "../../src/transports/logs-sieve/index.js";
import { estimateUtf8Bytes } from "../../src/utils/json.js";

function createMockLog(blockNumber: bigint, logIndex = 0): RpcLog {
  return {
    address: "0x1234567890123456789012345678901234567890",
    topics: ["0xabc"],
    data: "0x",
    blockNumber: toHex(blockNumber),
    transactionHash: `0x${"a".repeat(64)}`,
    transactionIndex: "0x0",
    blockHash: `0x${"b".repeat(64)}`,
    logIndex: toHex(logIndex),
    removed: false,
  };
}

describe("logsSieve", () => {
  it("filters out logs above maxBytes and keeps logs exactly at the limit", async () => {
    const smallLog = createMockLog(0n, 0);
    const largeLog = { ...createMockLog(0n, 1), data: `0x${"a".repeat(1_000)}` as Hex };
    const maxBytes = estimateUtf8Bytes(smallLog);
    const requestFn = vi.fn().mockResolvedValue([smallLog, largeLog]);
    const transport = logsSieve(custom({ request: requestFn }), [{ maxBytes }])({} as never);

    const logs = await transport.request({ method: "eth_getLogs", params: [{ fromBlock: "0x0", toBlock: "0x50" }] });

    expect(logs).toEqual([smallLog]);
    expect(estimateUtf8Bytes(largeLog)).toBeGreaterThan(maxBytes);
  });

  it("passes non-eth_getLogs requests through unchanged", async () => {
    const requestFn = vi.fn().mockResolvedValue("0x64");
    const transport = logsSieve(custom({ request: requestFn }), [{ maxBytes: 128 }])({} as never);

    const blockNumber = await transport.request({ method: "eth_blockNumber" });

    expect(blockNumber).toBe("0x64");
    expect(requestFn).toHaveBeenCalledWith({ method: "eth_blockNumber" });
  });
});
