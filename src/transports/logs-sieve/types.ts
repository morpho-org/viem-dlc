export interface LogsSieveConfig {
  /**
   * The maximum valid log size in UTF-8 encoded bytes. Logs larger than this are
   * considered spam and silently ignored.
   */
  maxBytes: number;
}
