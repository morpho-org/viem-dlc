import { getLogs2 } from "@morpho-org/viem-dlc/actions";
import { HierarchicalStore, LruStore, TtlStore } from "@morpho-org/viem-dlc/stores";
import { cache, createSimpleInvalidation } from "@morpho-org/viem-dlc/transports/cache";
import { createPublicClient, parseAbiItem } from "viem";

/** @type {`0x${string}`} */
const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb";
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

/**
 * A `Store` is four methods over `Buffer[]`. That is the entire interface, which is why one can
 * wrap another.
 *
 * `Counting` below is a complete, working store in a dozen lines — it just delegates and tallies.
 * Putting one around each tier makes the fall-through visible: reads try tier 1 first, and a miss
 * that tier 2 answers is written back up, so the same read a second time never reaches tier 2.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/shared.js").RangeContext & { settings: { hotBytes: string } }>}
 */
const run = async ({ transport, chain, settings, range, log }) => {
  /** Wraps any store and counts what passes through it. */
  class Counting {
    /** @param {import("@morpho-org/viem-dlc").Store} inner @param {string} label */
    constructor(inner, label) {
      this.inner = inner;
      this.label = label;
      this.hits = 0;
      this.misses = 0;
      this.writes = 0;
    }
    /** @param {string} key */
    async get(key) {
      const value = await this.inner.get(key);
      if (value) this.hits += 1;
      else this.misses += 1;
      return value;
    }
    /** @param {string} key @param {Buffer[]} value */
    async set(key, value) {
      this.writes += 1;
      return this.inner.set(key, value);
    }
    /** @param {string} key */
    delete(key) {
      return this.inner.delete(key);
    }
    flush() {
      return this.inner.flush();
    }
  }

  // Tier 1 is small and short-lived; tier 2 is the durable one. Outside a browser tier 2 would be
  // `new CompressedStore(new NodeFsStore({ directory: ".cache" }))`, or a remote tier.
  const hot = new Counting(
    new TtlStore(new LruStore({ maxBytes: Number(settings.hotBytes) }), { ttlMs: 60_000 }),
    "hot",
  );
  const cold = new Counting(new LruStore({ maxBytes: 100_000_000 }), "cold");
  // `populateOnMiss` is what writes a tier-2 answer back into tier 1.
  const store = new HierarchicalStore([hot, cold], { populateOnMiss: true });

  // The interface is public, so you can use a store directly — nothing about it is transport-shaped.
  // This one is separate so the probe doesn't land in the counters below.
  const scratch = new LruStore({ maxBytes: 1_000 });
  await scratch.set("hello", [Buffer.from("world")]);
  log(`scratch.get("hello") -> ${(await scratch.get("hello"))?.map(String).join("")}`);
  await scratch.delete("hello");

  const client = createPublicClient({
    chain,
    transport: cache(transport, [
      { binSize: Number(settings.binSize), store, invalidationStrategy: createSimpleInvalidation() },
      { maxBlockRange: 2_000 },
      { retryCount: 3, retryDelay: 1_000, blockTimestamp: false },
      { maxBytes: 8_192 },
      { maxRequestsPerSecond: 8, maxBurstRequests: 2, maxConcurrentRequests: 3 },
    ]),
  });

  const query = { address: MORPHO, event: borrowEvent, strict: true, ...range };
  const logs = await getLogs2(client, query);
  log(`cold pass: ${logs.length} logs`);
  await getLogs2(client, query);
  log("warm pass done");

  // Debounced and remote tiers finish their writes here.
  await store.flush();

  return {
    summary: {
      logs: logs.length,
      "hot hits": hot.hits,
      "hot misses": hot.misses,
      "cold hits": cold.hits,
      "cold misses": cold.misses,
      "writes hot/cold": `${hot.writes}/${cold.writes}`,
    },
  };
};

export default run;
