import { randomBytes, randomUUID } from "crypto";

import { Redis } from "@upstash/redis";
import { afterAll, describe, expect, it } from "vitest";

import { PUBLISH_SCRIPT, PUBLISH_SHA } from "../../src/stores/upstash.internal.js";
import { UpstashStore } from "../../src/stores/upstash.js";

const url = process.env.UPSTASH_REDIS_REST_URL;
const token = process.env.UPSTASH_REDIS_REST_TOKEN;

/** Runs only with `UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN` set; writes under a unique prefix and cleans up. */
describe.skipIf(!url || !token)("UpstashStore (live)", () => {
  const S = 1024;
  const prefix = `viem-dlc-test:${randomUUID()}:`;
  const raw = new Redis({ url, token, automaticDeserialization: false, responseEncoding: false });
  const written: string[] = [];
  const key = (name: string) => {
    written.push(prefix + name);
    return prefix + name;
  };

  afterAll(async () => {
    if (written.length > 0) await raw.unlink(...written);
  });

  const createStore = (ttl?: number) =>
    new UpstashStore({
      maxRequestBytes: 16 * 1024,
      maxResponseBytes: 8 * 1024,
      shardBytes: S,
      ttl,
      redis: { url, token },
    });

  it("round-trips direct and staged values, carrying the TTL through publish", async () => {
    const store = createStore(120);
    const small = randomBytes(100);
    const big = randomBytes(40 * S);
    await store.mset([
      [key("small"), [small]],
      [key("big"), [big]],
    ]);

    const got = await store.mget([`${prefix}small`, `${prefix}big`, `${prefix}missing`]);
    expect(Buffer.concat(got[0]!)).toEqual(small);
    expect(Buffer.concat(got[1]!)).toEqual(big);
    expect(got[2]).toBeNull();

    expect(await raw.pttl(`${prefix}big`)).toBeGreaterThan(100_000);
    expect(await raw.pttl(`${prefix}small`)).toBeGreaterThan(100_000);
    expect((await raw.keys(`tmp:${prefix}*`)).length).toBe(0);
  });

  it("persists when no ttl is configured", async () => {
    await createStore().mset([[key("persist"), [randomBytes(5 * S)]]]);
    expect(await raw.pttl(`${prefix}persist`)).toBe(-1);
  });

  it("PUBLISH returns the documented codes on real Lua", async () => {
    const tmp = key("tmp:pub");
    const live = key("pub");
    const head = "0123456789abcdef|0|2|aaaa";
    expect(await raw.scriptLoad(PUBLISH_SCRIPT)).toBe(PUBLISH_SHA);
    await raw.rpush(tmp, head, "0123456789abcdef|1|bbbb");
    const call = (k: number, deadline = "") =>
      raw.evalsha<(string | number)[], number>(PUBLISH_SHA, [tmp, live], [head, k, deadline]);

    expect(await call(3)).toBe(-1);
    expect(await call(2)).toBe(1);
    expect(await call(2)).toBe(2);
    expect(await call(1)).toBe(-2);
    expect(await call(0)).toBe(-3);
    expect(await call(2, String(Date.now() - 1000))).toBe(0);
  });
});
