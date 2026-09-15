/**
 * The envelope's codecs, for building fixtures and mocks: everything needed to read a chunk off a
 * request the transports sent and to answer it the way a node running the envelope would.
 *
 * The packer, the cost model and the FastLZ codec stay internal — their shapes follow the
 * implementation rather than the wire.
 */
export * from "./codec.envelope.js";
export * from "./codec.inner.js";
