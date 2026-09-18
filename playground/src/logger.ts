import type { LogBuilder, Logger } from "@morpho-org/viem-dlc";

export type WideEvent = {
  level: "info" | "warn" | "error";
  message: string;
  fields: Record<string, unknown>;
};

/**
 * A {@link Logger} that keeps the wide events instead of printing them. Each call derives a child
 * and accumulates its facets through `withContext`, so one event lands per outermost transport
 * call, exactly as a real LogLayer would receive it.
 */
export function createCapturingLogger(onEvent: (event: WideEvent) => void): Logger {
  const build = (context: Record<string, unknown>): Logger => {
    const emit = (level: WideEvent["level"], extra: Record<string, unknown>) => (message?: string) =>
      onEvent({ level, message: message ?? "", fields: { ...context, ...extra } });

    const builder = (extra: Record<string, unknown>): LogBuilder => ({
      withMetadata: (metadata) => builder({ ...extra, ...metadata }),
      withError: (error) => builder({ ...extra, error: `${error}` }),
      info: emit("info", extra),
      warn: emit("warn", extra),
      error: emit("error", extra),
    });

    return {
      child: () => build({ ...context }),
      withContext: (added) => build({ ...context, ...added }),
      metadataOnly: (metadata) => onEvent({ level: "info", message: "", fields: { ...context, ...metadata } }),
      ...builder({}),
    };
  };

  return build({});
}
