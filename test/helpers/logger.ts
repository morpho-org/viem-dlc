export interface StubEvent {
  name: string;
  context: Record<string, unknown>;
  metadata: Record<string, unknown>;
  error?: unknown;
}

/**
 * Hand-rolled LogLayer stub. Implements just enough of the API for the library
 * to call into without importing the real package. Each method returns `this`
 * so chaining works; emissions are captured into `events`.
 */
export function createStubLogger() {
  const events: StubEvent[] = [];

  // biome-ignore lint/suspicious/noExplicitAny: stub LogLayer surface
  function makeLayer(parentContext: Record<string, unknown>): any {
    let context = { ...parentContext };
    let pendingMetadata: Record<string, unknown> = {};
    let pendingError: unknown;

    const emit = (name: string) => {
      events.push({ name, context: { ...context }, metadata: pendingMetadata, error: pendingError });
      pendingMetadata = {};
      pendingError = undefined;
      return layer;
    };

    const layer = {
      child() {
        return makeLayer(context);
      },
      withContext(extra: Record<string, unknown>) {
        context = { ...context, ...extra };
        return layer;
      },
      withMetadata(extra: Record<string, unknown>) {
        pendingMetadata = { ...pendingMetadata, ...extra };
        return layer;
      },
      withError(err: unknown) {
        pendingError = err;
        return layer;
      },
      info: emit,
      warn: emit,
      error: emit,
      metadataOnly(extra: Record<string, unknown>) {
        pendingMetadata = { ...pendingMetadata, ...extra };
        emit("");
      },
    };
    return layer;
  }

  return { logger: makeLayer({}), events };
}

/**
 * Reads the `<transportKey>.<field>` entry in a wide event's context. The first
 * instance of a key touched in a call writes under the bare key, which is all
 * these tests exercise. `field` may be dotted (e.g. "eth_call.input_elements").
 */
export function findDotted(context: Record<string, unknown>, transportKey: string, field: string) {
  return context[`${transportKey}.${field}`];
}
