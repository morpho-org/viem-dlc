/*//////////////////////////////////////////////////////////////
                          TOKEN BUCKET
//////////////////////////////////////////////////////////////*/

/** Token bucket for rate limiting */
export interface TokenBucket {
  /** Current number of available tokens */
  tokens: number;
  /** Timestamp of last refill calculation */
  lastRefill: number;
  /** Maximum tokens the bucket can hold (burst capacity) */
  maxTokens: number;
  /** Tokens added per second */
  refillRate: number;
}

/**
 * Creates a new token bucket for rate limiting.
 * Starts with full tokens to allow initial burst.
 *
 * @param maxTokens Maximum tokens (burst capacity)
 * @param refillRate Tokens added per second. Use `Infinity` for no rate limiting.
 *   Use `0` to allow only the initial burst (no refill).
 */
export function createTokenBucket(maxTokens: number, refillRate: number): TokenBucket {
  if (maxTokens <= 0) {
    throw new Error(`[createTokenBucket] maxTokens must be positive, got ${maxTokens}`);
  }
  if (refillRate < 0) {
    throw new Error(`[createTokenBucket] refillRate must be non-negative, got ${refillRate}`);
  }

  return {
    tokens: maxTokens,
    lastRefill: Date.now(),
    maxTokens,
    refillRate,
  };
}

/**
 * Attempts to consume one token from the bucket.
 * Returns true if successful, false if bucket is empty.
 */
export function tryConsume(bucket: TokenBucket): boolean {
  refillBucket(bucket);

  if (bucket.tokens >= 1) {
    bucket.tokens -= 1;
    return true;
  }
  return false;
}

/**
 * Returns the time in milliseconds until a token becomes available.
 * Returns `Infinity` if `refillRate` is 0 (tokens will never regenerate).
 */
export function timeUntilToken(bucket: TokenBucket): number {
  refillBucket(bucket);

  if (bucket.tokens >= 1) return 0;
  if (bucket.refillRate <= 0) return Infinity;

  const needed = 1 - bucket.tokens;
  return Math.ceil((needed / bucket.refillRate) * 1000);
}

/**
 * Refills the bucket based on elapsed time.
 * Called internally before consuming tokens.
 */
function refillBucket(bucket: TokenBucket): void {
  const now = Date.now();
  const elapsedSeconds = (now - bucket.lastRefill) / 1000;
  bucket.tokens = Math.min(bucket.maxTokens, bucket.tokens + elapsedSeconds * bucket.refillRate);
  bucket.lastRefill = now;
}

/*//////////////////////////////////////////////////////////////
                            RATE LIMIT
//////////////////////////////////////////////////////////////*/

/** Job waiting to be admitted by the limiter */
interface Job {
  /** Resolver used to unblock the waiting caller */
  resolve: () => void;
  /** Lower numbers execute first */
  priority: number;
  /** Monotonic sequence number for FIFO within same priority */
  seq: number;
}

interface RateLimitContext {
  /** Token bucket for rate limiting */
  bucket: TokenBucket;
  /** Queue of resolvers waiting for tokens */
  queue: Job[];
  /** Maximum number of in-flight operations allowed */
  maxConcurrent: number;
  /** Current number of in-flight operations */
  inFlight: number;
  /** Sequence number used to preserve FIFO order within a priority */
  nextSeq: number;
  /** Whether a drain timer is already scheduled */
  drainScheduled: boolean;
}

/**
 * Removes and returns the next job to run:
 * - lower numbers are considered higher priority (P0 before P1)
 * - FIFO for jobs with the same priority
 */
function dequeueNext(queue: Job[]): Job | undefined {
  if (queue.length === 0) return undefined;

  let bestIndex = 0;

  for (let i = 1; i < queue.length; i++) {
    const candidate = queue[i]!;
    const best = queue[bestIndex]!;

    if (candidate.priority < best.priority || (candidate.priority === best.priority && candidate.seq < best.seq)) {
      bestIndex = i;
    }
  }

  const [job] = queue.splice(bestIndex, 1);
  return job;
}

/**
 * Schedules a drain if not already scheduled and queue is non-empty.
 *
 * Important:
 * - If blocked only by concurrency, do not schedule a timer.
 *   Completion of an in-flight task will call drainQueue().
 * - If blocked by tokens, schedule wake-up for next token.
 */
function scheduleDrain(ctx: RateLimitContext): void {
  if (ctx.drainScheduled || ctx.queue.length === 0 || ctx.inFlight >= ctx.maxConcurrent) return;

  const waitTime = timeUntilToken(ctx.bucket);
  if (!Number.isFinite(waitTime)) return;

  ctx.drainScheduled = true;
  setTimeout(() => {
    ctx.drainScheduled = false;
    drainQueue(ctx);
  }, waitTime);
}

/**
 * Starts as many queued requests as allowed by both:
 * - token availability
 * - concurrency availability
 *
 * Queue selection is by lowest priority value first, then FIFO within same priority.
 */
function drainQueue(ctx: RateLimitContext): void {
  refillBucket(ctx.bucket);

  while (ctx.queue.length > 0 && ctx.inFlight < ctx.maxConcurrent && ctx.bucket.tokens >= 1) {
    ctx.bucket.tokens -= 1;
    ctx.inFlight += 1;

    const job = dequeueNext(ctx.queue)!;
    job.resolve();
  }

  scheduleDrain(ctx);
}

/**
 * Releases one concurrency slot after an operation finishes.
 */
function release(ctx: RateLimitContext): void {
  if (ctx.inFlight > 0) {
    ctx.inFlight -= 1;
  }

  drainQueue(ctx);
}

export function createRateLimit(maxTokens: number, refillRate: number, maxConcurrent: number) {
  if (maxConcurrent < 1) {
    throw new Error(`[createRateLimit] maxConcurrent must be at least 1, got ${maxConcurrent}`);
  }
  const ctx: RateLimitContext = {
    bucket: createTokenBucket(maxTokens, refillRate),
    queue: [],
    maxConcurrent,
    inFlight: 0,
    nextSeq: 0,
    drainScheduled: false,
  };

  return {
    /**
     * Wraps an async function with:
     * - token-bucket rate limiting
     * - concurrency limiting
     * - priority scheduling
     *
     * Lower numeric priority runs first (P0 before P1).
     * Jobs with the same priority are processed FIFO.
     *
     * @example
     * const { withRateLimit } = createRateLimit(5, 10, 2) // 5 burst, 10/sec refill, 2 concurrent
     *
     * const result = await withRateLimit(() => fetch('/api'), {})
     *
     * const results = await Promise.all([
     *   withRateLimit(() => fetch('/low'), { priority: 10 }),
     *   withRateLimit(() => fetch('/high'), { priority: 0 }),
     *   withRateLimit(() => fetch('/medium'), { priority: 5 }),
     * ])
     */
    async withRateLimit<T>(fn: () => Promise<T>, { priority = Infinity }: { priority?: number }): Promise<T> {
      await new Promise<void>((resolve) => {
        ctx.queue.push({
          resolve,
          priority,
          seq: ctx.nextSeq++,
        });
        drainQueue(ctx);
      });

      try {
        return await fn();
      } finally {
        release(ctx);
      }
    },
  };
}
