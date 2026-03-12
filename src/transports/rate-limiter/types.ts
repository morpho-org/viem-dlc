export interface RateLimiterConfig {
  /** Rate at which requests are sent after burst is depleted. @default 20 */
  maxRequestsPerSecond?: number;
  /** Max requests that can be kicked off simultaneously. @default 1 */
  maxBurstRequests?: number;
  /** Max requests that can be in-flight simultaneously. @default Infinity */
  maxConcurrentRequests?: number;
}
