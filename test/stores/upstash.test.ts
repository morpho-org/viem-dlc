import { createHash, randomBytes } from "crypto";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  decodeContinuationShard,
  decodeHeadShard,
  encodeShards,
  PUBLISH_SCRIPT,
  PUBLISH_SHA,
  planShards,
  WRITE_DIRECT_SCRIPT,
  WRITE_DIRECT_SHA,
} from "../../src/stores/upstash.internal.js";
import { UpstashStore, type UpstashStoreOptions } from "../../src/stores/upstash.js";
import { type FakeRequest, FakeUpstash } from "../helpers/fake-upstash.js";
import { createStubLogger } from "../helpers/logger.js";

const S = 128;
const MAX_REQUEST = 2048;
const MAX_RESPONSE = 4096;

const bytes = (s: string): Buffer[] => [Buffer.from(s)];
const concat = (v: Buffer[] | null) => (v === null ? null : Buffer.concat(v));
const utf8 = (v: unknown) => Buffer.byteLength(JSON.stringify(v));

let fake: FakeUpstash;

function createStore(opts: Partial<UpstashStoreOptions> = {}) {
  return new UpstashStore({
    maxRequestBytes: MAX_REQUEST,
    maxResponseBytes: MAX_RESPONSE,
    shardBytes: S,
    ...opts,
    redis: { url: fake.url, token: "token", retry: { retries: 3, backoff: () => 0 }, ...opts.redis },
  });
}

/** Every request body is exactly under the bound; every read request asks for at most `maxResponse / S` elements. */
function assertBudgets(maxRequest = MAX_REQUEST, maxResponse = MAX_RESPONSE) {
  for (const req of fake.requests) {
    expect(req.requestBytes).toBeLessThanOrEqual(maxRequest);
    const commands = req.body as (string | number)[][];
    if (commands[0]![0] === "lrange") {
      const elements = commands.reduce((n, c) => n + (Number(c[3]) - Number(c[2]) + 1), 0);
      expect(elements).toBeLessThanOrEqual(Math.floor(maxResponse / S));
    }
  }
}

/** The recovery invariant: every key is a caller key or a `tmp:*` key carrying a TTL (lazily expired ones are gone). */
function assertKeyspace(callerKeys: Set<string>) {
  for (const [key, entry] of fake.keys) {
    if (callerKeys.has(key)) continue;
    expect(key.startsWith("tmp:")).toBe(true);
    expect(entry.expireAt).not.toBeNull();
  }
}

const isPublish = (req: FakeRequest) =>
  req.body.some((c) => Array.isArray(c) && c[0] === "evalsha" && c[1] === PUBLISH_SHA);
const isStaging = (req: FakeRequest) => req.path === "multi-exec";
const isHeadRead = (req: FakeRequest) => {
  const cmd = req.body[0] as (string | number)[];
  return cmd[0] === "lrange" && cmd[2] === 0;
};

beforeEach(() => {
  fake = new FakeUpstash();
  vi.stubGlobal("fetch", fake.fetch);
});
afterEach(() => {
  vi.unstubAllGlobals();
});

describe("scripts", () => {
  it("SHA constants match the sources", () => {
    expect(createHash("sha1").update(WRITE_DIRECT_SCRIPT).digest("hex")).toBe(WRITE_DIRECT_SHA);
    expect(createHash("sha1").update(PUBLISH_SCRIPT).digest("hex")).toBe(PUBLISH_SHA);
    expect(WRITE_DIRECT_SCRIPT.startsWith("#!lua flags=allow-key-locking\n")).toBe(true);
    expect(PUBLISH_SCRIPT.startsWith("#!lua flags=allow-key-locking\n")).toBe(true);
  });

  describe("PUBLISH return contract", () => {
    const head = "0123456789abcdef|0|3|aaaa";
    const shards = [head, "0123456789abcdef|1|bbbb", "0123456789abcdef|2|cccc"];
    const foreign = ["fedcba9876543210|0|1|zzzz"];

    const publish = (deadline: string, k = 3) =>
      JSON.parse(
        fake.replay({
          path: "pipeline",
          body: [["evalsha", PUBLISH_SHA, 2, "tmp:k:u", "k", head, k, deadline]],
          requestBytes: 0,
          responseBytes: 0,
        }).payload,
      )[0].result as number;

    const stagingStates: Record<string, string[] | null> = {
      absent: null,
      complete: shards,
      short: shards.slice(0, 2),
      duplicate: [...shards, shards[2]!],
      continuationAtHead: shards.slice(1),
    };
    const liveStates: Record<string, string[] | null> = { absent: null, ours: shards, foreign };
    const deadlines: Record<string, () => string> = {
      future: () => String(fake.nowMs + 60_000),
      past: () => String(fake.nowMs),
      persistent: () => "",
    };

    for (const [sName, staging] of Object.entries(stagingStates)) {
      for (const [lName, live] of Object.entries(liveStates)) {
        for (const [dName, deadline] of Object.entries(deadlines)) {
          it(`staging=${sName} live=${lName} deadline=${dName}`, () => {
            if (staging) fake.keys.set("tmp:k:u", { list: [...staging], expireAt: fake.nowMs + 60_000 });
            if (live) fake.keys.set("k", { list: [...live], expireAt: null });
            const before = fake.keys.get("k")?.list;
            const stagingBefore = fake.keys.get("tmp:k:u")?.list;

            const code = publish(deadline());

            const expected =
              dName === "past" ? 0 : sName === "complete" ? 1 : lName === "ours" ? 2 : lName === "foreign" ? -2 : -1;
            expect(code).toBe(expected);

            if (code === 1) {
              expect(fake.keys.get("k")!.list).toEqual(shards);
              expect(fake.keys.has("tmp:k:u")).toBe(false);
              expect(fake.keys.get("k")!.expireAt).toBe(dName === "future" ? fake.nowMs + 60_000 : null);
            } else {
              expect(fake.keys.get("k")?.list).toEqual(before);
            }
            if (code === 0) expect(fake.keys.has("tmp:k:u")).toBe(false);
            if (code !== 0 && code !== 1) expect(fake.keys.get("tmp:k:u")?.list).toEqual(stagingBefore);
          });
        }
      }
    }

    it("returns -3 on invalid arguments", () => {
      expect(publish("12.5")).toBe(-3);
      expect(publish(String(fake.nowMs + 1000), 0)).toBe(-3);
    });
  });
});

describe("header codec", () => {
  it("grows D monotonically until it stabilizes", () => {
    const plan = planShards(1_000_000, S)!;
    expect(String(plan.k).length).toBe(plan.D);
    expect(plan.headCapacity).toBe(S - 19 - 2 * plan.D);
    expect(plan.continuationCapacity).toBe(S - 18 - plan.D);
    expect(plan.headCapacity + (plan.k - 1) * plan.continuationCapacity).toBeGreaterThanOrEqual(1_000_000);
    expect(plan.headCapacity + (plan.k - 2) * plan.continuationCapacity).toBeLessThan(1_000_000);
  });

  it("returns null when S cannot hold a header", () => {
    expect(planShards(10, 20)).toBeNull();
  });

  it("round-trips and keeps every complete element <= S (fuzz around boundaries)", () => {
    const boundaries = [0, 1, S - 21, S - 20, S - 19, 2 * S, 10 * S, 100 * S];
    for (const b of boundaries) {
      for (let delta = -3; delta <= 3; delta++) {
        const n = b + delta;
        if (n < 0) continue;
        const value = randomBytes(n);
        const shards = encodeShards([value], S)!;
        for (const shard of shards) expect(Buffer.byteLength(shard)).toBeLessThanOrEqual(S);

        const head = decodeHeadShard(shards[0]!)!;
        expect(head.k).toBe(shards.length);
        let payload = head.payload;
        for (let i = 1; i < shards.length; i++) {
          const p = decodeContinuationShard(shards[i]!, head, i);
          expect(p).not.toBeNull();
          payload += p;
        }
        expect(Buffer.from(payload, "base64").equals(value)).toBe(true);
      }
    }
  });

  it("rejects misindexed or foreign continuations", () => {
    const shards = encodeShards([randomBytes(3 * S)], S)!;
    const head = decodeHeadShard(shards[0]!)!;
    expect(decodeContinuationShard(shards[2]!, head, 1)).toBeNull();
    expect(decodeContinuationShard(shards[1]!, { ...head, wid: "0".repeat(16) }, 1)).toBeNull();
    expect(decodeHeadShard(shards[1]!)).toBeNull();
    expect(decodeHeadShard("legacy|payload")).toBeNull();
  });

  it("drops values whose stored elements exceed the record cap", () => {
    expect(encodeShards([Buffer.alloc(76 * 1024 * 1024)], 64 * 1024)).toBeNull();
  });
});

describe("UpstashStore", () => {
  describe("constructor", () => {
    it("validates options", () => {
      expect(() => createStore({ maxRequestBytes: 0 })).toThrow("maxRequestBytes");
      expect(() => createStore({ shardBytes: 20 })).toThrow("shardBytes");
      expect(() => createStore({ maxResponseBytes: S - 1 })).toThrow("maxResponseBytes");
      expect(() => createStore({ ttl: 0 })).toThrow("ttl");
      expect(() => createStore({ ttl: 1.5 })).toThrow("ttl");
    });
  });

  describe("direct writes", () => {
    it("writes many small keys in one request and reads them back in one request", async () => {
      const store = createStore();
      const entries = Array.from({ length: 10 }, (_, i) => [`k${i}`, bytes(`v${i}`)] as const);
      await store.mset(entries);
      expect(fake.requests).toHaveLength(1);
      expect(fake.requests[0]!.path).toBe("pipeline");
      expect(fake.requests[0]!.body).toHaveLength(10);

      const got = await store.mget(entries.map(([k]) => k));
      expect(fake.requests).toHaveLength(2);
      expect(got.map(concat)).toEqual(entries.map(([, v]) => Buffer.concat(v)));
      assertBudgets();
    });

    it("splits direct writes across requests only when the exact body would exceed maxRequestBytes", async () => {
      const store = createStore();
      const entries = Array.from({ length: 20 }, (_, i) => [`k${i}`, [randomBytes(S / 2)] as Buffer[]] as const);
      await store.mset(entries);

      const pipelines = fake.requests;
      expect(pipelines.length).toBeGreaterThan(1);
      for (let i = 0; i < pipelines.length; i++) {
        expect(pipelines[i]!.requestBytes).toBeLessThanOrEqual(MAX_REQUEST);
        // Next-fit is tight: the first command of the following request would not have fit here.
        const next = pipelines[i + 1]?.body[0];
        if (next) expect(pipelines[i]!.requestBytes + 1 + utf8(next)).toBeGreaterThan(MAX_REQUEST);
      }
      expect(pipelines.flatMap((p) => p.body).length).toBe(20);
      expect((await store.mget(entries.map(([k]) => k))).map(concat)).toEqual(entries.map(([, v]) => v[0]));
    });

    it("handles empty values, misses, duplicates, and positional alignment", async () => {
      const store = createStore();
      await store.mset([
        ["empty", [Buffer.alloc(0)]],
        ["a", bytes("A")],
        ["a", bytes("A2")],
      ]);
      const got = await store.mget(["missing", "a", "empty", "a", "missing"]);
      expect(got.map(concat)).toEqual([null, Buffer.from("A2"), Buffer.alloc(0), Buffer.from("A2"), null]);
    });

    it("issues no I/O for empty input", async () => {
      const store = createStore();
      await store.mset([]);
      await store.mdelete([]);
      expect(await store.mget([])).toEqual([]);
      expect(fake.requests).toHaveLength(0);
    });

    it("applies an absolute deadline that replays cannot move", async () => {
      const store = createStore({ ttl: 30 });
      fake.onRequest = () => "network-after";
      const before = Date.now();
      await store.mset([["k", bytes("v")]]);
      const after = Date.now();

      expect(fake.requests.length).toBeGreaterThan(1);
      expect(new Set(fake.requests.map((r) => JSON.stringify(r.body))).size).toBe(1);
      const deadline = Number((fake.requests[0]!.body as string[][])[0]![4]);
      expect(deadline).toBeGreaterThanOrEqual(before + 30_000);
      expect(deadline).toBeLessThanOrEqual(after + 30_000);
      expect(fake.ttlOf("k")).toBe(deadline);
      expect(fake.keys.get("k")!.list).toHaveLength(1);
    });

    it("persists when no ttl is configured", async () => {
      const store = createStore();
      await store.mset([["k", bytes("v")]]);
      expect(fake.ttlOf("k")).toBeNull();
    });

    it("leaves the old value untouched when the deadline is already past", async () => {
      await createStore().mset([["k", bytes("old")]]);
      const store = createStore({ ttl: 1 });
      fake.nowMs = Date.now() + 10_000;
      await store.mset([["k", bytes("new")]]);
      expect(concat((await store.mget(["k"]))[0]!)).toEqual(Buffer.from("old"));
    });
  });

  describe("NOSCRIPT recovery", () => {
    it("reloads once and reissues only the failed slots", async () => {
      const store = createStore();
      // Emulate a partially-cached script: slots 0..4 succeed, 5..9 NOSCRIPT.
      const original = fake.fetch;
      vi.stubGlobal("fetch", async (url: string, init: RequestInit) => {
        const body = JSON.parse(String(init.body)) as unknown[][];
        if (fake.requests.length === 0 && body.length === 10) {
          fake.flushScripts();
          const res = await original(url, init);
          const results = (await res.json()) as { result?: unknown; error?: string }[];
          for (let i = 0; i < 5; i++) results[i] = { result: 1 };
          return new Response(JSON.stringify(results), { status: 200 });
        }
        return original(url, init);
      });

      const entries = Array.from({ length: 10 }, (_, i) => [`k${i}`, bytes(`v${i}`)] as const);
      await store.mset(entries);

      const [first, load, reissue] = fake.requests;
      expect(first!.body).toHaveLength(10);
      expect(load!.body).toEqual(["script", "load", WRITE_DIRECT_SCRIPT]);
      expect(reissue!.body).toHaveLength(5);
      expect((reissue!.body as string[][]).map((c) => c[3])).toEqual(["k5", "k6", "k7", "k8", "k9"]);

      for (const req of fake.requests) {
        if (req.body[0] === "script") continue;
        expect(JSON.stringify(req.body)).not.toContain("redis.call");
      }
      const got = await store.mget(["k5", "k9"]);
      expect(got.map(concat)).toEqual([Buffer.from("v5"), Buffer.from("v9")]);
    });

    it("recovers a staged publish after script eviction", async () => {
      const store = createStore();
      const value = randomBytes(20 * S);
      fake.flushScripts();
      await store.mset([["big", [value]]]);
      expect(fake.requests.filter((r) => r.body[0] === "script").map((r) => r.body[2])).toEqual([PUBLISH_SCRIPT]);
      const publishes = fake.requests.filter(isPublish);
      expect(publishes.map((r) => r.results![0]!.error?.startsWith("NOSCRIPT") ?? r.results![0]!.result)).toEqual([
        true,
        1,
      ]);
      expect(new Set(fake.requests.filter(isStaging).map((r) => (r.body as string[][])[0]![1])).size).toBe(1);
      expect(concat((await store.mget(["big"]))[0]!)).toEqual(value);
    });
  });

  describe("staged writes", () => {
    it("stages, publishes, and reads back a multi-shard value under both budgets", async () => {
      const store = createStore({ ttl: 120 });
      const value = randomBytes(40 * S);
      await store.mset([["big", [value]]]);

      const staging = fake.requests.filter(isStaging);
      expect(staging.length).toBeGreaterThan(1);
      for (const req of staging) {
        expect((req.body as string[][])[0]![0]).toBe("rpush");
        expect((req.body as unknown[][])[1]).toEqual(["expire", (req.body as string[][])[0]![1], 60]);
      }
      expect(fake.requests.filter(isPublish)).toHaveLength(1);
      expect([...fake.keys.keys()]).toEqual(["big"]);
      expect(fake.ttlOf("big")).toBeGreaterThan(Date.now() + 119_000);

      fake.requests.length = 0;
      const got = await store.mget(["big"]);
      expect(concat(got[0]!)).toEqual(value);
      expect(fake.requests.length).toBeGreaterThan(1);
      assertBudgets();
    });

    it("packs stage-2 pages of several keys and stays under maxResponseBytes", async () => {
      const store = createStore();
      const values = Array.from({ length: 5 }, () => randomBytes(7 * S));
      await store.mset(values.map((v, i) => [`k${i}`, [v]] as const));
      fake.requests.length = 0;

      const got = await store.mget(values.map((_, i) => `k${i}`));
      expect(got.map(concat)).toEqual(values);
      assertBudgets();
    });

    it("never publishes a replay-duplicated staging list; readers see old-or-complete", async () => {
      const store = createStore({ ttl: 60 });
      const old = randomBytes(2 * S);
      await store.mset([["big", [old]]]);
      const seen = new Set<string>();
      fake.onRequest = (req) => {
        const body = JSON.stringify(req.body);
        if (seen.has(body)) return undefined;
        seen.add(body);
        return "network-after";
      };
      const value = randomBytes(10 * S);
      await store.mset([["big", [value]]]);

      // Every staging batch landed twice, so LLEN != k and PUBLISH refuses (-2: foreign live value): dropped, not spliced.
      const publishes = fake.requests.filter(isPublish);
      expect(publishes.length).toBeGreaterThan(0);
      expect(publishes.map((r) => r.results![0]!.result)).toEqual(publishes.map(() => -2));
      expect(concat((await store.mget(["big"]))[0]!)).toEqual(old);
      assertKeyspace(new Set(["big"]));
    });

    it("restarts exactly once, under a fresh staging key and wid, when a staging batch fails before publish", async () => {
      const store = createStore();
      let failures = 0;
      fake.onRequest = (req) => (isStaging(req) && failures++ < 4 ? "network-before" : undefined);
      const value = randomBytes(10 * S);
      await store.mset([["big", [value]]]);

      const stagingKeys = new Set(fake.requests.filter(isStaging).map((r) => (r.body as string[][])[0]![1]));
      const wids = new Set(fake.requests.filter(isStaging).map((r) => (r.body as string[][])[0]![2]!.slice(0, 16)));
      expect(stagingKeys.size).toBe(2);
      expect(wids.size).toBe(2);
      expect(fake.requests.filter(isPublish)).toHaveLength(1);
      expect(concat((await store.mget(["big"]))[0]!)).toEqual(value);
    });

    it("gives up after a second staging failure without publishing", async () => {
      const store = createStore();
      await store.mset([["big", [randomBytes(2 * S)]]]);
      const before = [...fake.keys.get("big")!.list];
      fake.onRequest = (req) => (isStaging(req) ? "network-before" : undefined);
      await store.mset([["big", [randomBytes(10 * S)]]]);
      expect(new Set(fake.requests.filter(isStaging).map((r) => (r.body as string[][])[0]![1])).size).toBe(2);
      expect(fake.requests.filter(isPublish)).toHaveLength(0);
      expect(fake.keys.get("big")!.list).toEqual(before);
    });

    it("retries a publish whose response was lost and accepts the already-live code without restaging", async () => {
      const store = createStore({ ttl: 60 });
      fake.onRequest = (req) =>
        isPublish(req) && fake.requests.filter(isPublish).length === 1 ? "network-after" : undefined;
      const value = randomBytes(10 * S);
      await store.mset([["big", [value]]]);

      const publishes = fake.requests.filter(isPublish);
      expect(publishes.map((r) => r.results![0]!.result)).toEqual([1, 2]);
      expect(new Set(publishes.map((r) => JSON.stringify(r.body))).size).toBe(1);
      const stagingKeys = new Set(fake.requests.filter(isStaging).map((r) => (r.body as string[][])[0]![1]));
      expect(stagingKeys.size).toBe(1);
      expect(concat((await store.mget(["big"]))[0]!)).toEqual(value);
      expect([...fake.keys.keys()]).toEqual(["big"]);
    });

    it("does not publish when staging expires after the last batch but before publish", async () => {
      const store = createStore();
      const old = randomBytes(2 * S);
      await store.mset([["big", [old]]]);
      let frozen = 0;
      fake.onRequest = (req) => {
        if (isPublish(req)) {
          frozen++;
          fake.nowMs += 61_000;
        }
        return undefined;
      };
      await store.mset([["big", [randomBytes(10 * S)]]]);

      expect(frozen).toBe(1);
      expect(fake.requests.filter(isPublish).map((r) => r.results![0]!.result)).toEqual([-2]);
      expect(concat((await store.mget(["big"]))[0]!)).toEqual(old);
      for (const k of [...fake.keys.keys()]) if (k.startsWith("tmp:")) expect(fake.ttlOf(k)).toBeUndefined();
      assertKeyspace(new Set(["big"]));
    });

    it("keeps the absolute deadline fixed across a staging restart and a replayed publish", async () => {
      const store = createStore({ ttl: 30 });
      let stagingFailed = false;
      let publishReplayed = false;
      fake.onRequest = (req) => {
        if (isStaging(req) && !stagingFailed) {
          stagingFailed = true;
          return "network-before";
        }
        if (isPublish(req) && !publishReplayed) {
          publishReplayed = true;
          return "network-after";
        }
        return undefined;
      };
      const before = Date.now();
      await store.mset([["big", [randomBytes(10 * S)]]]);
      const after = Date.now();

      const deadlines = new Set(fake.requests.filter(isPublish).map((r) => (r.body as string[][])[0]![7]));
      expect(fake.requests.filter(isPublish).length).toBeGreaterThanOrEqual(2);
      expect(deadlines.size).toBe(1);
      const deadline = Number([...deadlines][0]);
      expect(deadline).toBeGreaterThanOrEqual(before + 30_000);
      expect(deadline).toBeLessThanOrEqual(after + 30_000);
      expect(fake.ttlOf("big")).toBe(deadline);
    });
  });

  describe("read integrity", () => {
    it("rejects a recombined list that publication would accept (index contiguity)", async () => {
      const store = createStore();
      const shards = encodeShards([randomBytes(5 * S)], S)!;
      expect(shards.length).toBeGreaterThanOrEqual(6);
      const recombined = [shards[0]!, shards[1]!, shards[0]!, shards[1]!, ...shards.slice(4)];
      expect(recombined).toHaveLength(shards.length);
      fake.keys.set("k", { list: recombined, expireAt: null });

      expect((await store.mget(["k"]))[0]).toBeNull();
      expect(fake.requests.filter(isHeadRead)).toHaveLength(2);
      expect(fake.keys.get("k")!.list).toEqual(recombined);
    });

    it("retries a torn read and returns the replacement", async () => {
      const store = createStore();
      const first = randomBytes(5 * S);
      const second = randomBytes(5 * S);
      await store.mset([["k", [first]]]);
      const secondList = encodeShards([second], S)!;

      let swapped = false;
      fake.onRequest = (req) => {
        const cmd = (req.body as (string | number)[][])[0]!;
        if (!swapped && cmd[0] === "lrange" && cmd[2] !== 0) {
          swapped = true;
          fake.keys.set("k", { list: secondList, expireAt: null });
        }
        return undefined;
      };
      expect(concat((await store.mget(["k"]))[0]!)).toEqual(second);
    });

    it("returns null after bounded retries when the list keeps changing", async () => {
      const store = createStore();
      await store.mset([["k", [randomBytes(5 * S)]]]);
      fake.onRequest = (req) => {
        const cmd = (req.body as (string | number)[][])[0]!;
        if (cmd[0] === "lrange" && cmd[2] !== 0) {
          fake.keys.set("k", { list: encodeShards([randomBytes(5 * S)], S)!, expireAt: null });
        }
        return undefined;
      };
      expect((await store.mget(["k"]))[0]).toBeNull();
      expect(fake.requests.filter(isHeadRead)).toHaveLength(2);
    });

    it("treats a shrunken list (short stage-2 page) as torn and retries", async () => {
      const store = createStore();
      const small = randomBytes(1);
      await store.mset([["k", [randomBytes(5 * S)]]]);
      fake.onRequest = (req) => {
        const cmd = (req.body as (string | number)[][])[0]!;
        if (cmd[0] === "lrange" && cmd[2] !== 0)
          fake.keys.set("k", { list: encodeShards([small], S)!, expireAt: null });
        return undefined;
      };
      expect(concat((await store.mget(["k"]))[0]!)).toEqual(small);
    });

    it("treats unrecognized (legacy) heads as misses without retrying", async () => {
      const store = createStore();
      fake.keys.set("k", { list: ["0123456789abcdef|legacy"], expireAt: null });
      expect((await store.mget(["k"]))[0]).toBeNull();
      expect(fake.requests).toHaveLength(1);
    });

    it("chunks stage 1 by elements per response", async () => {
      const store = createStore({ maxResponseBytes: 3 * S });
      const keys = Array.from({ length: 10 }, (_, i) => `k${i}`);
      await store.mset(keys.map((k) => [k, bytes(k)] as const));
      fake.requests.length = 0;
      const got = await store.mget(keys);
      expect(got.map((v) => concat(v)!.toString())).toEqual(keys);
      expect(fake.requests).toHaveLength(4);
      assertBudgets(MAX_REQUEST, 3 * S);
    });

    it("recovers from a transport failure on read", async () => {
      const store = createStore();
      await store.mset([["k", bytes("v")]]);
      let failures = 0;
      fake.onRequest = () => (failures++ < 4 ? "network-before" : undefined);
      fake.requests.length = 0;
      expect(concat((await store.mget(["k"]))[0]!)).toEqual(Buffer.from("v"));
      expect(fake.requests).toHaveLength(5); // 4 client retries exhaust attempt 1; attempt 2 succeeds
    });
  });

  describe("delete, flush, and singular adapters", () => {
    it("unlinks keys in one request", async () => {
      const store = createStore();
      await store.mset([
        ["a", bytes("a")],
        ["b", bytes("b")],
      ]);
      await store.mdelete(["a", "b", "a", "missing"]);
      expect(fake.requests.at(-1)!.body).toHaveLength(3);
      expect(await store.mget(["a", "b"])).toEqual([null, null]);
    });

    it("exposes get/set/delete over the plural methods", async () => {
      const store = createStore();
      await store.set("k", bytes("v"));
      expect(concat(await store.get("k"))).toEqual(Buffer.from("v"));
      await store.delete("k");
      expect(await store.get("k")).toBeNull();
    });

    it("flush waits for in-flight writes", async () => {
      const store = createStore();
      const value = randomBytes(10 * S);
      void store.mset([["k", [value]]]);
      await store.flush();
      expect(fake.requests.filter(isPublish)).toHaveLength(1);
      expect([...fake.keys.keys()]).toEqual(["k"]);
      expect(concat((await store.mget(["k"]))[0]!)).toEqual(value);
    });

    it("never throws on transport failure", async () => {
      const { logger, events } = createStubLogger();
      const store = createStore({ logger });
      fake.onRequest = () => "network-before";
      await expect(store.mset([["k", bytes("v")]])).resolves.toBeUndefined();
      await expect(store.mdelete(["k"])).resolves.toBeUndefined();
      await expect(store.mget(["k"])).resolves.toEqual([null]);
      const names = events.map((e) => e.name);
      expect(names).toContain("write failed");
      expect(names).toContain("mdelete failed");
      expect(names).toContain("exhausted retries");
      expect(events.filter((e) => e.name === "write failed").every((e) => e.error instanceof Error)).toBe(true);
    });
  });

  describe("keyspace audit", () => {
    it("leaves only caller keys or TTL'd tmp keys under randomized faults", async () => {
      const store = createStore({ ttl: 300 });
      const callerKeys = new Set<string>();
      let seed = 42;
      const rand = () => {
        seed = (seed * 1103515245 + 12345) & 0x7fffffff;
        return seed / 0x7fffffff;
      };
      fake.onRequest = () => {
        const r = rand();
        if (r < 0.1) return "network-before";
        if (r < 0.2) return "network-after";
        if (r < 0.25) fake.nowMs += 61_000;
        return undefined;
      };

      const written = new Map<string, Buffer[]>();
      for (let round = 0; round < 15; round++) {
        const entries = Array.from({ length: 4 }, (_, i) => {
          const key = `key${(round * 4 + i) % 7}`;
          callerKeys.add(key);
          const value: Buffer[] = [randomBytes(Math.floor(rand() * 12 * S))];
          written.set(key, [...(written.get(key) ?? []), value[0]!]);
          return [key, value] as const;
        });
        await store.mset(entries);

        // Delayed network replays of earlier staging batches, at least one landing after its staging TTL.
        const staging = fake.requests.filter(isStaging);
        for (let i = 0; i < 3 && staging.length > 0; i++) {
          if (i === 0) fake.nowMs += 61_000;
          fake.replay(staging[Math.floor(rand() * staging.length)]!);
        }

        assertKeyspace(callerKeys);
      }

      fake.onRequest = undefined;
      const keys = [...callerKeys];
      const got = await store.mget(keys);
      for (let i = 0; i < got.length; i++) {
        const v = concat(got[i]!);
        if (v === null) continue;
        // Whatever is readable is one complete writer's value, never a splice.
        expect(written.get(keys[i]!)!.some((w) => w.equals(v))).toBe(true);
      }
      assertBudgets();
    });
  });
});

describe("request measurement", () => {
  it("routes direct vs staged on the exact serialized body size", async () => {
    const key = 'ключ/🔑 "quoted"';
    const value = randomBytes(S);
    await createStore().mset([[key, [value]]]);
    const exact = fake.requests[0]!.requestBytes;
    expect(fake.requests[0]!.path).toBe("pipeline");
    expect(exact).toBe(utf8(fake.requests[0]!.body));

    fake.requests.length = 0;
    await createStore({ maxRequestBytes: exact }).mset([[key, [value]]]);
    expect(fake.requests.map((r) => r.path)).toEqual(["pipeline"]);

    fake.requests.length = 0;
    await createStore({ maxRequestBytes: exact - 1 }).mset([[key, [value]]]);
    expect(fake.requests.map((r) => r.path)).toEqual(["multi-exec", "multi-exec", "pipeline"]);
    expect(concat((await createStore().mget([key]))[0]!)).toEqual(value);
  });
});
