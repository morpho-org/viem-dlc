export interface LogsEnricherConfig {
  /** Overrides the retry count for enrichment requests; helps avoid one enrichment step causing cascading failure. */
  retryCount: number;
  /** Override the retry delay for enrichment requests; helps avoid one enrichment step causing cascading failure. */
  retryDelay: number;
  /** Whether to fetch block headers to populate `blockTimestamp` on `eth_getLogs` results that lack it. */
  blockTimestamp: boolean;
}
