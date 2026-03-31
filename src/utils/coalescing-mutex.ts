export type PendingFollower<TArgs> = {
  slot: number;
  args: TArgs;
};

/**
 * Outcome for a participant (leader or follower).
 * - `"resolve"` — resolve the participant's promise with `result`
 * - `"reject"` — reject the participant's promise with `error`
 * - For followers, omitting a slot defers that follower to the next leader cycle.
 */
export type Outcome<TResult> = { action: "resolve"; result: TResult } | { action: "reject"; error: unknown };

export type FollowerOutcome<TResult> = Outcome<TResult> & { slot: number };

/** Creates a Promise alongside its externalized `resolve` and `reject` handles. */
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Mutate `target` in-place to `[...items, ...target]` (stack-safe). */
function prepend<T>(target: T[], items: T[]) {
  const tail = target.splice(0);
  for (const item of items) target.push(item);
  for (const item of tail) target.push(item);
}

type QueuedCall = {
  // biome-ignore lint/suspicious/noExplicitAny: queue holds heterogeneous call types
  args: any;
  handler: (
    // biome-ignore lint/suspicious/noExplicitAny: queue holds heterogeneous call types
    args: any,
    // biome-ignore lint/suspicious/noExplicitAny: queue holds heterogeneous call types
    collectFollowers: () => PendingFollower<any>[],
  ) => Promise<{
    // biome-ignore lint/suspicious/noExplicitAny: queue holds heterogeneous call types
    leader: Outcome<any>;
    // biome-ignore lint/suspicious/noExplicitAny: queue holds heterogeneous call types
    followers?: readonly FollowerOutcome<any>[];
  }>;
  // biome-ignore lint/suspicious/noExplicitAny: queue holds heterogeneous call types
  resolve: (result: any) => void;
  reject: (error: unknown) => void;
};

/**
 * Creates a coalescing mutex for per-resource leader/follower batching.
 *
 * Calls on the same `resourceKey` are serialized. The first queued call
 * becomes the leader for its cycle; later calls wait as followers.
 *
 * The leader may call `collectFollowers()` (at most once) to snapshot the
 * current follower queue as `PendingFollower<TArgs>[]`. The handler then
 * returns:
 * - `leader`: the leader's own result, which can be a graceful rejection
 * - `followers`: sparse per-slot outcomes for collected followers.
 *   Omitted slots are deferred to the next leader cycle.
 *
 * If the leader throws before return, all collected followers are deferred.
 *
 * @example
 * return coalesce(resourceKey, args, async (leaderArgs, collectFollowers) => {
 *   const data = await doExpensiveWork(leaderArgs);
 *   const followers = collectFollowers();
 *   const serveable = followers.filter(f => canServe(leaderArgs, f.args));
 *
 *   return {
 *     leader: { action: "resolve", result: extractResult(data, leaderArgs) },
 *     followers: serveable.map(f => ({
 *       slot: f.slot,
 *       action: "resolve",
 *       result: extractResult(data, f.args),
 *     })),
 *   };
 * });
 */
export function createCoalescingMutex() {
  const gates = new Map<string, QueuedCall[]>();

  const run = async (resourceKey: string, queue: QueuedCall[]) => {
    while (queue.length > 0) {
      const leader = queue.shift()!;
      let collected: QueuedCall[] | null = null;

      try {
        const outcome = await leader.handler(leader.args, () => {
          if (collected !== null) throw new Error("[coalesce] collectFollowers() called more than once");
          collected = queue.splice(0);
          return collected.map((c, slot) => ({ slot, args: c.args }));
        });

        // TS can't narrow variables mutated inside closures.
        const batch = collected as QueuedCall[] | null;
        if (batch) {
          const entries = outcome.followers ?? [];
          // Validate all slots before applying any outcomes.
          const seen = new Set<number>();
          for (const e of entries) {
            if (!Number.isInteger(e.slot) || e.slot < 0 || e.slot >= batch.length) {
              throw new Error(`[coalesce] invalid follower slot ${e.slot}`);
            }
            if (seen.has(e.slot)) {
              throw new Error(`[coalesce] duplicate follower slot ${e.slot}`);
            }
            seen.add(e.slot);
          }
          for (const e of entries) {
            if (e.action === "resolve") batch[e.slot]!.resolve(e.result);
            else batch[e.slot]!.reject(e.error);
          }
          prepend(
            queue,
            batch.filter((_, i) => !seen.has(i)),
          );
        }

        if (outcome.leader.action === "resolve") leader.resolve(outcome.leader.result);
        else leader.reject(outcome.leader.error);
      } catch (error) {
        const batch = collected as QueuedCall[] | null;
        if (batch) prepend(queue, batch);
        leader.reject(error);
      }
    }

    gates.delete(resourceKey);
  };

  return {
    coalesce<TArgs, TResult>(
      resourceKey: string,
      args: TArgs,
      handler: (
        args: TArgs,
        collectFollowers: () => PendingFollower<TArgs>[],
      ) => Promise<{
        leader: Outcome<TResult>;
        followers?: readonly FollowerOutcome<TResult>[];
      }>,
    ): Promise<TResult> {
      const { promise, resolve, reject } = deferred<TResult>();
      const queue = gates.get(resourceKey);

      if (queue) {
        queue.push({ args, handler, resolve, reject });
      } else {
        const newQueue: QueuedCall[] = [{ args, handler, resolve, reject }];
        gates.set(resourceKey, newQueue);
        void run(resourceKey, newQueue);
      }

      return promise;
    },
  };
}

/** Collect all followers and resolve them with the leader's result. */
export function broadcast<TArgs, TResult>(
  result: TResult,
  collectFollowers: () => PendingFollower<TArgs>[],
): { leader: Outcome<TResult>; followers: FollowerOutcome<TResult>[] } {
  const followers = collectFollowers();
  return {
    leader: { action: "resolve", result },
    followers: followers.map((f) => ({ slot: f.slot, action: "resolve", result })),
  };
}
