import { describe, expect, it } from "vitest";

import { createSink } from "../../../../src/transports/cache/eth-get-logs/sink.js";
import type { CachedLogs, CachedMetadata } from "../../../../src/transports/cache/eth-get-logs/types.js";

import { binSize, chainId, collectRecords, createMockLog, createNdjson, entryKey } from "./_helpers.js";

describe("createSink", () => {
  it("writes metadata + logs as a batch for complete bins", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [createMockLog(5000n)],
      fromBlock: 0n,
      toBlock: 9999n,
      fetchedAtBlock: 50000n,
      fetchedAt: 1000,
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);

    const ek = entryKey(0n, 9999n);
    expect(records).toHaveLength(2);
    expect(records[0]!.key).toBe(ek.metadata);
    expect((records[0]!.value as CachedMetadata).__type).toBe("metadata");
    expect((records[0]!.value as CachedMetadata).fetchedAt).toBe(1000);
    expect(records[1]!.key).toBe(ek.data);
    expect(records[1]!.value).toHaveLength(1);
    expect((records[1]!.value as CachedLogs)[0]!.blockNumber).toBe("0x1388");
  });

  it("does not write incomplete bins", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [createMockLog(5000n)],
      fromBlock: 0n,
      toBlock: 5000n,
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    await ndjson.flush();
    expect(await collectRecords(ndjson)).toHaveLength(0);
  });

  it("accumulates multiple responses until bin is complete", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [createMockLog(2500n)],
      fromBlock: 0n,
      toBlock: 4999n,
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    await ndjson.flush();
    expect(await collectRecords(ndjson)).toHaveLength(0);

    sink({
      logs: [createMockLog(7500n)],
      fromBlock: 5000n,
      toBlock: 9999n,
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);
    expect(records).toHaveLength(2);
    expect(records[1]!.value).toHaveLength(2);
  });

  it("sorts logs within bin by blockNumber then logIndex", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [createMockLog(8000n, 1), createMockLog(8000n, 0), createMockLog(2000n, 0)],
      fromBlock: 0n,
      toBlock: 9999n,
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);
    const logs = records[1]!.value as CachedLogs;

    expect(logs.map((l) => [BigInt(l.blockNumber!), BigInt(l.logIndex!)])).toEqual([
      [2000n, 0n],
      [8000n, 0n],
      [8000n, 1n],
    ]);
  });

  it("distributes multi-bin responses across bins", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [createMockLog(5000n), createMockLog(15000n)],
      fromBlock: 0n,
      toBlock: 19999n,
      fetchedAtBlock: 50000n,
      fetchedAt: Date.now(),
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);

    expect(records).toHaveLength(4);
    expect(records[0]!.key).toContain("0:");
    expect(records[1]!.key).toContain("0:");
    expect(records[2]!.key).toContain("1:");
    expect(records[3]!.key).toContain("1:");

    const bin0Logs = records[2]!.value as CachedLogs;
    expect(bin0Logs).toHaveLength(1);
    expect(bin0Logs[0]!.blockNumber).toBe("0x1388");

    const bin1Logs = records[3]!.value as CachedLogs;
    expect(bin1Logs).toHaveLength(1);
    expect(bin1Logs[0]!.blockNumber).toBe("0x3a98");
  });

  it("tracks fetchedAt and fetchedAtBlock as max across responses", async () => {
    const { ndjson } = createNdjson();
    const sink = createSink({ chainId, binSize, ndjson });

    sink({
      logs: [],
      fromBlock: 0n,
      toBlock: 4999n,
      fetchedAtBlock: 40000n,
      fetchedAt: 1000,
    });

    sink({
      logs: [],
      fromBlock: 5000n,
      toBlock: 9999n,
      fetchedAtBlock: 50000n,
      fetchedAt: 2000,
    });

    await ndjson.flush();
    const records = await collectRecords(ndjson);
    const metadata = records[0]!.value as CachedMetadata;

    expect(metadata.fetchedAt).toBe(2000);
    expect(metadata.fetchedAtBlock).toBe(50000n);
  });
});
