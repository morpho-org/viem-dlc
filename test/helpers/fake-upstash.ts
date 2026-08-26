import { createHash } from "crypto";

import { PUBLISH_SHA, WRITE_DIRECT_SHA } from "../../src/stores/upstash.internal.js";

export type FakeRequest = {
  path: string;
  body: unknown[];
  requestBytes: number;
  responseBytes: number;
  /** Per-command results of the (first) execution; absent when the request was not executed. */
  results?: Result[];
};
/** `before` fails without executing; `after` executes then fails, so the client's auto-retry replays it. */
export type Fault = "network-before" | "network-after";
type Entry = { list: string[]; expireAt: number | null };
export type Result = { result?: unknown; error?: string };

const NOSCRIPT = "NOSCRIPT No matching script. Please use EVAL.";

/**
 * In-memory Upstash REST endpoint covering the command subset `UpstashStore` uses. Wire it in with
 * `vi.stubGlobal("fetch", fake.fetch)`. Time is virtual (`nowMs`); expiry is lazy, like Redis.
 * `onRequest` runs before execution and may mutate state or inject a {@link Fault}.
 */
export class FakeUpstash {
  readonly url = "https://fake.upstash.io";
  nowMs = 1_700_000_000_000;
  readonly keys = new Map<string, Entry>();
  readonly loadedScripts = new Set<string>([WRITE_DIRECT_SHA, PUBLISH_SHA]);
  readonly requests: FakeRequest[] = [];
  onRequest?: (req: FakeRequest) => Fault | undefined;

  private guard: Set<string> | null = null;

  readonly fetch = async (url: string | URL, init?: RequestInit): Promise<Response> => {
    const path = new URL(String(url)).pathname.replace(/^\//, "");
    const raw = String(init?.body);
    const body = JSON.parse(raw) as unknown[];
    const req: FakeRequest = { path, body, requestBytes: Buffer.byteLength(raw), responseBytes: 0 };
    this.requests.push(req);

    const fault = this.onRequest?.(req);
    if (fault === "network-before") throw new TypeError("fetch failed");

    const { status, payload } = this.execute(path, body);
    req.responseBytes = Buffer.byteLength(payload);
    req.results = [JSON.parse(payload)].flat() as Result[];
    if (fault === "network-after") throw new TypeError("fetch failed");

    return new Response(payload, { status, headers: { "Content-Type": "application/json" } });
  };

  /** Re-executes a captured request body, as a delayed network replay would. */
  replay(req: FakeRequest) {
    return this.execute(req.path, req.body);
  }

  flushScripts() {
    this.loadedScripts.clear();
  }

  ttlOf(key: string): number | null | undefined {
    const e = this.touch(key);
    return e === undefined ? undefined : e.expireAt;
  }

  private execute(path: string, body: unknown[]): { status: number; payload: string } {
    if (path === "pipeline" || path === "multi-exec") {
      const results = (body as unknown[][]).map((cmd) => this.run(cmd));
      return { status: 200, payload: JSON.stringify(results) };
    }
    const result = this.run(body);
    return { status: result.error === undefined ? 200 : 400, payload: JSON.stringify(result) };
  }

  private touch(key: string): Entry | undefined {
    if (this.guard && !this.guard.has(key)) {
      throw new Error("ERR Dynamic keys are not allowed in Lua scripts when 'allow-key-locking' flag is set");
    }
    const e = this.keys.get(key);
    if (e && e.expireAt !== null && e.expireAt <= this.nowMs) {
      this.keys.delete(key);
      return undefined;
    }
    return e;
  }

  private run(cmd: unknown[]): Result {
    try {
      return { result: this.dispatch(cmd.map(String)) };
    } catch (err) {
      return { error: (err as Error).message };
    }
  }

  private dispatch([name, ...args]: string[]): unknown {
    switch (name!.toLowerCase()) {
      case "rpush": {
        const [key, ...els] = args;
        if (els.length === 0) throw new Error("ERR wrong number of arguments for 'rpush' command");
        const e = this.touch(key!) ?? { list: [], expireAt: null };
        e.list.push(...els);
        this.keys.set(key!, e);
        return e.list.length;
      }
      case "lrange": {
        const [key, start, stop] = args;
        const list = this.touch(key!)?.list ?? [];
        const norm = (i: number) => (i < 0 ? Math.max(0, list.length + i) : i);
        return list.slice(norm(Number(start)), norm(Number(stop)) + 1);
      }
      case "llen":
        return this.touch(args[0]!)?.list.length ?? 0;
      case "lindex": {
        const list = this.touch(args[0]!)?.list;
        const i = Number(args[1]);
        return list?.[i < 0 ? list.length + i : i] ?? null;
      }
      case "unlink":
      case "del": {
        let n = 0;
        for (const key of args) if (this.touch(key) && this.keys.delete(key)) n++;
        return n;
      }
      case "expire": {
        const e = this.touch(args[0]!);
        if (!e) return 0;
        e.expireAt = this.nowMs + Number(args[1]) * 1000;
        return 1;
      }
      case "pexpireat": {
        const e = this.touch(args[0]!);
        if (!e) return 0;
        e.expireAt = Number(args[1]);
        return 1;
      }
      case "persist": {
        const e = this.touch(args[0]!);
        if (!e || e.expireAt === null) return 0;
        e.expireAt = null;
        return 1;
      }
      case "rename": {
        const [src, dst] = args;
        const e = this.touch(src!);
        if (!e) throw new Error("ERR no such key");
        this.touch(dst!);
        this.keys.delete(src!);
        this.keys.set(dst!, e);
        return "OK";
      }
      case "time":
        return [String(Math.floor(this.nowMs / 1000)), String((this.nowMs % 1000) * 1000)];
      case "script": {
        if (args[0]!.toLowerCase() !== "load") throw new Error("ERR unsupported SCRIPT subcommand");
        const sha = createHash("sha1").update(args[1]!).digest("hex");
        this.loadedScripts.add(sha);
        return sha;
      }
      case "evalsha": {
        const [sha, nkeys, ...rest] = args;
        if (!this.loadedScripts.has(sha!)) throw new Error(NOSCRIPT);
        const keys = rest.slice(0, Number(nkeys));
        const argv = rest.slice(Number(nkeys));
        this.guard = new Set(keys);
        try {
          if (sha === WRITE_DIRECT_SHA) return this.writeDirect(keys, argv);
          if (sha === PUBLISH_SHA) return this.publish(keys, argv);
          throw new Error(`fake has no implementation for script ${sha}`);
        } finally {
          this.guard = null;
        }
      }
      default:
        throw new Error(`ERR unknown command '${name}'`);
    }
  }

  private parseDeadline(arg: string): number | null | "bad" | "past" {
    if (arg === "") return null;
    const n = Number(arg);
    if (!Number.isInteger(n)) return "bad";
    return n <= this.nowMs ? "past" : n;
  }

  private writeDirect(keys: string[], argv: string[]): number {
    const deadline = this.parseDeadline(argv[0]!);
    if (deadline === "bad") return -3;
    if (deadline === "past") return 0;

    this.dispatch(["unlink", keys[0]!]);
    this.dispatch(["rpush", keys[0]!, ...argv.slice(1)]);
    if (deadline !== null) this.dispatch(["pexpireat", keys[0]!, String(deadline)]);
    return 1;
  }

  private publish([tmp, live]: string[], [expectedHead, kArg, deadlineArg]: string[]): number {
    const k = Number(kArg);
    if (!Number.isInteger(k) || k < 1) return -3;

    const deadline = this.parseDeadline(deadlineArg!);
    if (deadline === "bad") return -3;
    if (deadline === "past") {
      this.dispatch(["unlink", tmp!]);
      return 0;
    }

    if (this.dispatch(["lindex", tmp!, "0"]) === expectedHead && this.dispatch(["llen", tmp!]) === k) {
      if (deadline !== null) this.dispatch(["pexpireat", tmp!, String(deadline)]);
      else this.dispatch(["persist", tmp!]);
      this.dispatch(["rename", tmp!, live!]);
      return 1;
    }

    const liveHead = this.dispatch(["lindex", live!, "0"]);
    if (liveHead === expectedHead && this.dispatch(["llen", live!]) === k) return 2;
    if (liveHead !== null) return -2;
    return -1;
  }
}
