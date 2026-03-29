export type PendingFollower<TArgs> = {
  slot: number;
  args: TArgs;
};

/**
 * Per-follower outcome returned by the leader handler.
 * - `{ action: "resolve", result }` — resolve the follower's promise
 * - `{ action: "reject", error }` — reject the follower's promise
 * - Omitting a slot defers that follower to the next leader cycle.
 */
export type FollowerOutcome<TResult> =
  | { slot: number; action: "resolve"; result: TResult }
  | { slot: number; action: "reject"; error: unknown };

/**
 * Creates a coalescing mutex for per-resource leader/follower batching.
 *
 * Calls on the same `resourceKey` are serialized. The first queued call
 * becomes the leader for its cycle; later calls wait as followers.
 *
 * The leader may call `collectFollowers()` (at most once) to snapshot the
 * current follower queue as `PendingFollower<TArgs>[]`. The handler then
 * returns:
 * - `leader`: the leader's own result
 * - `followers`: sparse per-slot outcomes for collected followers.
 *   Omitted slots are deferred to the next leader cycle.
 *
 * If the leader throws, all collected followers are deferred.
 *
 * @example
 * return coalesce(resourceKey, args, async (leaderArgs, collectFollowers) => {
 *   const data = await doExpensiveWork(leaderArgs);
 *   const followers = collectFollowers();
 *   const serveable = followers.filter(f => canServe(leaderArgs, f.args));
 *
 *   return {
 *     leader: extractResult(data, leaderArgs),
 *     followers: serveable.map(f => ({
 *       slot: f.slot,
 *       action: "resolve",
 *       result: extractResult(data, f.args),
 *     })),
 *   };
 * });
 * 
 * @dev FIFO for deferred followers relies on microtask ordering, which is not a hard guarantee.
 * A fresh `coalesce()` call already queued as a microtask can slip ahead of a deffered follower's
 * continuation. This can only happen at the instant a leader finishes, and persistent starvation
 * would require it on every successive cycle -- not a realistic failure mode. If strict FIFO is
 * ever needed, replace the Promise-based deferral loop with an explicit queue.
 */
export function createCoalescing<TArgs, TResult>() {
  type Follower = {
    args: TArgs;
    outcome?: { value: TResult } | { error: unknown };
  };

  type Handler = (
    args: TArgs,
    collectFollowers: () => PendingFollower<TArgs>[],
  ) => Promise<{
    leader: TResult;
    followers?: readonly FollowerOutcome<TResult>[];
  }>;

  const gates = new Map<
    string,
    {
      followers: Follower[];
      done: Promise<void>;
    }
  >();

  return {
    async coalesce(resourceKey: string, args: TArgs, handler: Handler): Promise<TResult> {
      // Persistent identity across deferral iterations. When the leader
      // settles this follower, it writes to `outcome`. The follower checks
      // it after each leader cycle.
      const self: Follower = { args };

      // If a leader is active, enqueue and wait. On deferral, loop back.
      while (true) {
        const active = gates.get(resourceKey);
        if (!active) break;

        active.followers.push(self);
        await active.done;

        if (self.outcome) {
          if ("error" in self.outcome) throw self.outcome.error;
          return self.outcome.value;
        }
        // Not settled → deferred. Loop back and try next leader.
      }

      // Become leader
      let resolveDone!: () => void;
      const done = new Promise<void>((r) => {
        resolveDone = r;
      });
      const entry = { followers: [] as Follower[], done };
      gates.set(resourceKey, entry);

      try {
        let _collected: Follower[] | null = null;

        const outcome = await handler(args, () => {
          if (_collected !== null) throw error_collectFollowersCalledTwice;
          _collected = entry.followers;
          entry.followers = [];
          return _collected.map((f, slot) => ({ slot, args: f.args }));
        });

        // Settle collected followers from the handler's return value.
        // Type assertion: TS can't narrow variables mutated inside closures.
        const collected = _collected as Follower[] | null;
        if (collected) {
          const entries = outcome.followers ?? [];
          // Validate all slots before applying any outcomes.
          const seen = new Set<number>();
          for (const e of entries) {
            if (!Number.isInteger(e.slot) || e.slot < 0 || e.slot >= collected.length) {
              throw new Error(`[coalescing] invalid follower slot ${e.slot}`);
            }
            if (seen.has(e.slot)) {
              throw new Error(`[coalescing] duplicate follower slot ${e.slot}`);
            }
            seen.add(e.slot);
          }
          for (const e of entries) {
            collected[e.slot]!.outcome =
              e.action === "resolve" ? { value: e.result } : { error: e.error };
          }
          // Unsettled followers loop back automatically when `done` resolves.
        }

        return outcome.leader;
      } finally {
        gates.delete(resourceKey);
        resolveDone();
      }
    },
  };
}

const error_collectFollowersCalledTwice = new Error("[coalescing] collectFollowers() called more than once");
