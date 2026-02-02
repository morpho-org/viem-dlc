/** Token bucket state for rate limiting */
export interface TokenBucket {
  /** Current number of available tokens */
  tokens: number;
  /** Timestamp of last refill calculation */
  lastRefill: number;
  /** Maximum tokens the bucket can hold (burst capacity) */
  maxTokens: number;
  /** Tokens added per second */
  refillRate: number;
  /** Queue of resolvers waiting for tokens (FIFO) */
  queue: Array<() => void>;
  /** Whether a drain timer is already scheduled */
  drainScheduled: boolean;
}

/**
 * Creates a new token bucket for rate limiting.
 * Starts with full tokens to allow initial burst.
 *
 * @param maxTokens Maximum tokens (burst capacity)
 * @param refillRate Tokens added per second. Use `Infinity` for no rate limiting.
 * Use `0` to allow only the initial burst (no refill).
 */
export function createTokenBucket(maxTokens: number, refillRate: number): TokenBucket {
  return {
    tokens: maxTokens,
    lastRefill: Date.now(),
    maxTokens,
    refillRate,
    queue: [],
    drainScheduled: false,
  };
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
 * Schedules a drain if not already scheduled and queue is non-empty.
 */
function scheduleDrain(bucket: TokenBucket): void {
  if (bucket.drainScheduled || bucket.queue.length === 0) return;

  bucket.drainScheduled = true;
  const waitTime = timeUntilToken(bucket);

  setTimeout(() => {
    bucket.drainScheduled = false;
    drainQueue(bucket);
  }, waitTime);
}

/**
 * Resolves as many queued requests as tokens allow (FIFO order).
 * Schedules next drain if queue still has items.
 */
function drainQueue(bucket: TokenBucket): void {
  while (bucket.queue.length > 0 && tryConsume(bucket)) {
    const resolve = bucket.queue.shift()!;
    resolve();
  }
  scheduleDrain(bucket);
}

/**
 * Wraps an async function with rate limiting using a token bucket.
 * Requests are processed in FIFO order.
 *
 * @example
 * const bucket = createTokenBucket(5, 10) // 5 burst, 10/sec refill
 *
 * // Single request
 * const result = await withRateLimit(() => fetch('/api'), { bucket })
 *
 * // Multiple concurrent requests (rate limited, FIFO order)
 * const results = await Promise.all(
 *   urls.map(url => withRateLimit(() => fetch(url), { bucket }))
 * )
 */
export async function withRateLimit<data>(fn: () => Promise<data>, { bucket }: { bucket: TokenBucket }): Promise<data> {
  await new Promise<void>((resolve) => {
    bucket.queue.push(resolve);
    drainQueue(bucket);
  });

  return fn();
}
