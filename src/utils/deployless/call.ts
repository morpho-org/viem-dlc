import { BaseError, type EIP1193RequestFn, type Hex, type PublicRpcSchema, toHex } from "viem";

import type { ChainDefinition, EthCallGas } from "../../chains/index.js";
import type { Facet } from "../../observability.js";
import { causeChain, isTimeoutLikeError } from "../errors.js";

import {
  type DeploylessTarget,
  decodeEnvelopeRevert,
  deliveryParams,
  ENVELOPE_ADDRESS,
  type EnvelopeDelivery,
  type EnvelopeRevert,
  encodeEnvelopeArgs,
  envelopeConfig,
  overridesEnvelopeAddress,
  type RestOfEthCallParams,
  type RpcEthCallParams,
} from "./codec.envelope.js";
import { arrayToWire, type Page, type ResolvedArrayFunction, streamToPage } from "./codec.inner.js";
import { costModel, hexByteLength, type LensGas, sentSize, type WireSize, wireSize, zeroBytes } from "./pricing.js";

/** Ascending indices into `elements`, sent as one request. */
type Chunk = readonly number[];

type FactorisedFactoryCallParams = {
  target: DeploylessTarget;
  elements: readonly Hex[];
  lens: ResolvedArrayFunction;
  batch?: BatchOptions;
  provider: Provider;
  restOfEthCallParams: RestOfEthCallParams;
  /**
   * Invoked with each freshly fetched element as its chunk lands, before siblings finish, and
   * awaited — so a caller's results survive a later chunk failing.
   */
  onResolved?: (entries: readonly ResolvedElement[]) => void | Promise<void>;
  facet?: Facet;
};

/** `policy().batch`: how a paginated lens's elements are chunked, priced and delivered. */
export type BatchOptions = {
  /**
   * Maximum bytes of a chunk's `eth_call` `data`; elements are greedy-packed under it and fetched in
   * parallel. The chain's initcode cap (`MAX_INITCODE_SIZE` on Ethereum) is the usual value for
   * initcode delivery; by override the provider's request size limit is the bound.
   */
  batchSize?: number;
  /**
   * FastLZ-compress calldata on the wire so more elements fit per chunk, at the cost of encoding
   * time and decompression gas.
   */
  compress?: boolean;
  /**
   * The lens's cost, as the wide event reports it: `fixed` from `fixed_gas`, `item.avg` and
   * `item.stddev` from `item_gas_avg` and `item_gas_stddev`. With the transport's `gasLimit`, sizes
   * the opening wave; every later chunk is sized from what the pages report, the stated cost standing
   * in only until an attempt has been costed. Over-estimating costs extra parallel requests,
   * under-estimating costs one continuation.
   */
  gas?: LensGas;
  /**
   * When the elements a page did not reach are re-sent. `fill` (default) pools them across pages and
   * sends full pages at once and the remainder once no earlier chunk that could still add to it is
   * in flight: fewer requests. `eager` sends every tail as its page lands: more requests, no waiting.
   */
  continuations?: ContinuationMode;
  /**
   * How a chunk reaches the node. `initcode` (default) creates the envelope with the elements
   * trailing it, bounded by the chain's initcode cap. `override` calls the envelope at a fixed
   * address placed by `eth_call`'s state-override parameter, so the frame's gas is the only bound; a
   * provider that does not honour overrides is detected on the opening wave and the range re-fetched
   * as initcode, with unambiguous non-support remembered per transport instance. Pays only when
   * bytes bind: `(gas_limit_observed − fixed_gas) / item_gas_avg` well above
   * `elements_requested / nominal_batches` on the wide event.
   */
  envelope?: EnvelopeDelivery;
};

/**
 * A transport instance's view of the node it talks to, resolved once when the transport is created
 * by {@link providerOf}.
 */
export type Provider = {
  /**
   * The stated `eth_call` cap, when usable: sizes the opening wave's bytes and, with `batch.gas`,
   * its items; every later chunk is sized from what the pages report.
   */
  cap?: number;
  /**
   * Sent as every chunk's `gas`: the cap, on a chain whose nodes give an unspecified `gas` a fixed
   * default ({@link EthCallGas}); nothing elsewhere, where the node grants its cap unasked.
   */
  gas?: Hex;
  memo: DeliveryMemo;
};

export function providerOf(chain: ChainDefinition, gasLimit: number | undefined): Provider {
  const cap = gasLimit !== undefined && Number.isSafeInteger(gasLimit) && gasLimit > 0 ? gasLimit : undefined;
  return {
    cap,
    gas: cap !== undefined && chain.ethCall.gasWhenUnspecified === "fixedDefault" ? toHex(cap) : undefined,
    memo: { unsupported: false },
  };
}

/**
 * One transport instance's memory that its provider does not honour `eth_call` state overrides:
 * set only on unambiguous evidence (a call that returned instead of reverting, or an invalid-params
 * refusal), never cleared, and never consulted by a request without `batch.envelope: "override"`.
 * Correctness never depends on it; only the count of wasted requests does.
 */
export type DeliveryMemo = { unsupported: boolean };

/**
 * When the tails pages leave behind are sent. `fill` sends a tail once enough of them are pending
 * to fill a page, and the remainder once no earlier chunk that could add to it is in flight;
 * `eager` sends every tail as soon as its page lands. Anything else reads as `fill`.
 */
export type ContinuationMode = "fill" | "eager";

/** An input element's index paired with the raw output bytes fetched for it. */
export type ResolvedElement = { index: number; output: Hex };

export type FactorisedFactoryCallResult = {
  /** Per-element outputs aligned to `elements`, sparse exactly at {@link FactorisedFactoryCallResult.missing}. */
  outputs: readonly (Hex | undefined)[];
  /** Ascending indices no chunk could serve: declined by the lens, declined for size, or unresolved by gas. */
  missing: readonly number[];
  /** The subset of `missing` that gas could not resolve even as a singleton; another provider might. */
  unresolved: readonly number[];
  /** The subset of `missing` declined client-side for size, with no request made. */
  oversize: readonly number[];
};

/**
 * What became of one element: served, or declined by the lens's per-item revert, by gas (a death
 * the element suffered alone), or before any request (its bytes alone exceed a protocol bound).
 */
type ElementOutcome = { kind: "resolved"; output: Hex } | { kind: "declined"; by: DeclineReason };
type DeclineReason = "lens" | "gas" | "preflight";

/** One request's worth of work; `generation` counts the continuations behind it, 0 for the opening wave. */
type ChunkJob = {
  indices: Chunk;
  generation: number;
  delivery: EnvelopeDelivery;
  /** Halvings a timeout may still buy; a size refusal does not spend them. */
  timeoutSplits: number;
  /** Halvings behind this chunk, for `splits_max_depth`. */
  depth: number;
};

/** A timeout may be the request's size or a slow node, so it buys this many halvings before it propagates. */
const TIMEOUT_SPLITS = 1;

type FallbackReason = "unsupported" | "unproven" | "exhausted";

/**
 * Packs `elements` into deployless `eth_call` chunks under the wire budget (`batch.batchSize`, the
 * sent `data` bytes) and the gas each chunk is predicted to need; fetches them in parallel; returns
 * per-element outputs aligned to `elements`. The prediction runs on the stated `gasLimit` and
 * `batch.gas` until the first page lands and on the pages' own telemetry after, so the stated
 * figures size the opening wave and, until an attempt has been costed, the item cost, and nothing
 * else. The tails pages leave behind are pooled and re-packed together, sent as
 * {@link ContinuationMode} says; an element gas could not resolve is retried once alone.
 *
 * With `batch.envelope: "override"` a chunk is a call to {@link ENVELOPE_ADDRESS} with the envelope's
 * code in the state override and the argument tuple as calldata, so the initcode cap does not bound
 * it; a chunk that fails without proving the envelope ran is re-fetched as initcode.
 */
export async function factorisedFactoryCall(
  requestFn: EIP1193RequestFn<PublicRpcSchema>,
  {
    target,
    elements,
    lens,
    batch,
    provider: { cap, gas: sentGas, memo },
    restOfEthCallParams,
    onResolved,
    facet,
  }: FactorisedFactoryCallParams,
): Promise<FactorisedFactoryCallResult> {
  const compress = batch?.compress ?? false;
  const envelope: EnvelopeDelivery = batch?.envelope === "override" ? "override" : "initcode";
  if (envelope === "override" && overridesEnvelopeAddress(restOfEthCallParams[1])) {
    throw new Error(`[deployless] a caller's state override at ${ENVELOPE_ADDRESS} conflicts with the envelope's own`);
  }
  /** The delivery a new chunk takes: override while requested and not yet found unsupported. */
  const currentDelivery = () => (envelope === "override" && !memo.unsupported ? "override" : "initcode");

  const everything = elements.map((_, i) => i);
  const outcomes = new Array<ElementOutcome | undefined>(elements.length);
  const decline = (index: number, by: DeclineReason) => {
    outcomes[index] = { kind: "declined", by };
  };
  const declined = (by?: DeclineReason) =>
    everything.filter((i) => {
      const outcome = outcomes[i];
      return outcome?.kind === "declined" && (by === undefined || outcome.by === by);
    });

  const config = envelopeConfig(lens, compress);
  const encode = (indices: Chunk) =>
    encodeEnvelopeArgs(
      {
        target,
        targetData: arrayToWire(
          lens.inputLayout,
          indices.map((i) => elements[i]!),
        ),
      },
      config,
    );
  let wholeArgs: Hex | undefined;
  /** The argument tuple of `indices`; the whole input's is encoded once, every chunk being an ascending subset. */
  const args = (indices: Chunk) => {
    if (indices.length !== elements.length) return encode(indices);
    wholeArgs ??= encode(indices);
    return wholeArgs;
  };

  // Static layouts contribute `layout.size` per element; dynamic ones a length word plus their
  // padded bytes. Both are multiples of 32, so the wrapper's own padding is a per-batch constant.
  const layout = lens.inputLayout;
  const bytesOf: number[] = [];
  const zerosOf: number[] = [];
  let totalBytes = 0;
  let totalZeros = 0;
  for (const element of elements) {
    const bytes = layout.mode === "static" ? layout.size : 32 + hexByteLength(element);
    const zeros = zeroBytes(element) + (layout.mode === "static" ? 0 : 32 - nonzeroBytesOf(hexByteLength(element)));
    bytesOf.push(bytes);
    zerosOf.push(zeros);
    totalBytes += bytes;
    totalZeros += zeros;
  }
  let overhead: WireSize | undefined;
  /** Sizes the argument tuple of the sub-lists `[start, end)` of `indices`. */
  const measurer = (indices: Chunk): ((start: number, end: number) => WireSize) => {
    if (compress) return (start, end) => wireSize(args(indices.slice(start, end)));
    if (overhead === undefined) {
      const whole = wireSize(args(everything));
      overhead = {
        bytes: whole.bytes - totalBytes,
        zeros: whole.zeros - totalZeros - countingWordZeros(elements.length, totalBytes),
      };
    }
    const constant = overhead;
    const prefixBytes = [0];
    const prefixZeros = [0];
    for (let pos = 0; pos < indices.length; pos++) {
      prefixBytes.push(prefixBytes[pos]! + bytesOf[indices[pos]!]!);
      prefixZeros.push(prefixZeros[pos]! + zerosOf[indices[pos]!]!);
    }
    return (start, end) => {
      const body = prefixBytes[end]! - prefixBytes[start]!;
      return {
        bytes: constant.bytes + body,
        zeros: constant.zeros + prefixZeros[end]! - prefixZeros[start]! + countingWordZeros(end - start, body),
      };
    };
  };

  const wireCap = batch?.batchSize && batch.batchSize > 0 ? batch.batchSize : Infinity;
  const cost = costModel(cap, batch?.gas, compress);

  /**
   * Chunks `indices` for `delivery` under the wire cap and the gas prediction; an element that
   * fits neither alone is declined before any request.
   */
  const pack = (indices: Chunk, delivery: EnvelopeDelivery) => {
    if (indices.length === 0) return [];
    if (wireCap === Infinity && !cost.known) return [indices];
    const measure = measurer(indices);
    const fits = (start: number, end: number) => {
      const tuple = measure(start, end);
      return sentSize(tuple, delivery).bytes <= wireCap && cost.fits(tuple, end - start, delivery);
    };
    const packed = packBatches(indices, fits, compress);
    for (const index of packed.oversize) decline(index, "preflight");
    return packed.chunks;
  };

  const opening = currentDelivery();
  const chunks = pack(everything, opening);

  facet?.set({
    elements_requested: elements.length,
    nominal_batches: chunks.length,
    ...(cap === undefined ? {} : { gas_limit: cap }),
    ...(envelope === "override" ? { delivery_memo: memo.unsupported } : {}),
  });
  // Sizes of the *initial* packing, to compare realized utilization against the wire budget.
  // Halved children and continuations are not resampled. Guarded rather than
  // `facet?.stat(...)` so unobserved calls skip re-measuring.
  if (facet)
    for (const chunk of chunks) facet.stat("batch_bytes", sentSize(measurer(chunk)(0, chunk.length), opening).bytes);
  const splits = { size: 0, timeout: 0, maxDepth: 0 };
  const sent = { override: 0, initcode: 0 };
  const fallbacks: Record<FallbackReason, number> = { unsupported: 0, unproven: 0, exhausted: 0 };
  // A lens stopping early is a continuation, a mid-page gas death an escalation; neither is a
  // split, which means only "the provider refused the request's size or timed out".
  const pages = { continued: 0, unresolvedAttempts: 0, allSkipped: 0 };
  const continuations: ContinuationMode = batch?.continuations === "eager" ? "eager" : "fill";
  const job = (indices: Chunk, generation: number, delivery: EnvelopeDelivery): ChunkJob => ({
    indices,
    generation,
    delivery,
    timeoutSplits: TIMEOUT_SPLITS,
    depth: 0,
  });

  const commit = async (entries: readonly ResolvedElement[]) => {
    for (const { index, output } of entries) outcomes[index] = { kind: "resolved", output };
    if (entries.length > 0) await onResolved?.(entries);
  };

  const halve = ({ indices, ...rest }: ChunkJob, timeoutSplits: number) => {
    const mid = Math.floor(indices.length / 2);
    for (const half of [indices.slice(0, mid), indices.slice(mid)]) {
      wave.dispatch({ ...rest, indices: half, timeoutSplits, depth: rest.depth + 1 });
    }
  };

  /**
   * An override chunk that failed without proving the envelope ran is re-packed as initcode and
   * each piece dispatched at its generation, so the pending list waits for them as for any chunk.
   */
  const fallback = ({ indices, generation }: ChunkJob, reason: FallbackReason) => {
    fallbacks[reason] += 1;
    for (const piece of pack(indices, "initcode")) wave.dispatch(job(piece, generation, "initcode"));
  };

  const runChunk = async (chunk: ChunkJob) => {
    const { indices, generation, delivery } = chunk;
    if (chunk.depth > splits.maxDepth) splits.maxDepth = chunk.depth;
    const count = indices.length;
    const tuple = args(indices);
    const tupleSize = wireSize(tuple);
    sent[delivery] += 1;

    const outcome = await send(requestFn, deliveryParams(delivery, tuple, restOfEthCallParams, sentGas));
    if (outcome.kind !== "page") {
      const action = classifyOutcome(outcome, chunk);
      switch (action.kind) {
        case "throw":
          throw new Error(
            `[deployless] ${action.message}`,
            action.cause === undefined ? undefined : { cause: action.cause },
          );
        case "propagate":
          throw action.error;
        case "halve":
          splits[action.reason] += 1;
          return halve(chunk, action.timeoutSplits);
        case "fallback":
          if (action.reason === "unsupported") memo.unsupported = true;
          return fallback(chunk, action.reason);
      }
    }

    const page = streamToPage(lens.outputLayout, outcome.data);
    const attempted = adjudicated(page, count);
    cost.observe(page.gas, attempted - (page.died === undefined ? 0 : 1), tupleSize, delivery);
    facet?.stat("page_adjudicated", attempted);
    if (page.died === undefined && page.results.length === 0 && page.skipped.length > 0) pages.allSkipped += 1;

    const skipped = new Set(page.skipped);
    const entries: ResolvedElement[] = [];
    for (let i = 0, served = 0; i < attempted; i++) {
      if (i === page.died) continue;
      if (skipped.has(i)) decline(indices[i]!, "lens");
      else entries.push({ index: indices[i]!, output: page.results[served++]! });
    }
    await commit(entries);

    if (page.died !== undefined) {
      pages.unresolvedAttempts += 1;
      const pos = indices[page.died]!;
      if (count > 1) {
        wave.dispatch(job([pos], generation, currentDelivery()));
      } else {
        decline(pos, "gas");
      }
    }

    if (attempted < count) {
      pages.continued += 1;
      wave.defer(indices.slice(attempted), generation + 1);
    }
  };

  const wave = createWave<ChunkJob>({
    pack: (indices, generation) => {
      const delivery = currentDelivery();
      return pack(indices, delivery).map((chunk) => job(chunk, generation, delivery));
    },
    send: runChunk,
    eager: continuations === "eager",
  });

  try {
    for (const chunk of chunks) wave.dispatch(job(chunk, 0, opening));
    await wave.run();
  } finally {
    const unresolved = declined("gas").length;
    facet?.set({
      ...cost.fields(),
      elements_fetched: outcomes.filter((o) => o?.kind === "resolved").length,
      splits_count: splits.size + splits.timeout,
      splits_size: splits.size,
      splits_timeout: splits.timeout,
      splits_max_depth: splits.maxDepth,
      attempts_unresolved: pages.unresolvedAttempts,
      pages_escalated: pages.unresolvedAttempts - unresolved,
      pages_all_skipped: pages.allSkipped,
      pages_continued: pages.continued,
      continuations,
      continuation_depth_max: wave.generations,
      flushes: wave.flushes.full + wave.flushes.drain + wave.flushes.eager,
      flushes_full: wave.flushes.full,
      flushes_drain: wave.flushes.drain,
      flushes_eager: wave.flushes.eager,
      elements_declined_oversize: declined("preflight").length,
      elements_missing: declined().length,
      elements_unresolved: unresolved,
      chunks_override: sent.override,
      chunks_initcode: sent.initcode,
      override_fallbacks: fallbacks.unsupported + fallbacks.unproven + fallbacks.exhausted,
      override_fallbacks_unsupported: fallbacks.unsupported,
      override_fallbacks_unproven: fallbacks.unproven,
      override_fallbacks_exhausted: fallbacks.exhausted,
    });
  }

  return {
    outputs: outcomes.map((o) => (o?.kind === "resolved" ? o.output : undefined)),
    missing: declined(),
    unresolved: declined("gas"),
    oversize: declined("preflight"),
  };
}

/**
 * Sends jobs and pools the tails they leave behind. Knows nothing of deliveries, gas or pages:
 * `pack` turns pending elements into jobs, `send` performs one. Every job but the last a `pack`
 * returns is full — the next element did not fit it — so only the last waits, for jobs of an
 * earlier generation, the ones whose tails could still join it; `eager` sends it at once instead.
 * Nothing is dispatched after a failure: what is in flight settles, its results committed, and then
 * `run` rejects with the first failure.
 */
function createWave<Job extends { indices: Chunk; generation: number }>({
  pack,
  send,
  eager,
}: {
  pack: (pending: readonly number[], generation: number) => Job[];
  send: (job: Job) => Promise<void>;
  eager: boolean;
}) {
  let pending: number[] = [];
  let pendingGeneration = 0;
  let inFlight = 0;
  // Jobs in flight that could still leave a tail, by generation; a singleton adjudicates its whole input.
  const openBy: number[] = [];
  const openBefore = (generation: number) => openBy.slice(0, generation).reduce((n, c) => n + c, 0);
  const flushes = { full: 0, drain: 0, eager: 0 };
  let failure: { reason: unknown } | undefined;
  let settle!: { resolve: () => void; reject: (reason: unknown) => void };
  const done = new Promise<void>((resolve, reject) => {
    settle = { resolve, reject };
  });

  const dispatch = (job: Job) => {
    if (failure !== undefined) return;
    inFlight += 1;
    const open = job.indices.length > 1 ? 1 : 0;
    openBy[job.generation] = (openBy[job.generation] ?? 0) + open;
    send(job)
      .then(undefined, (reason) => {
        failure ??= { reason };
      })
      .then(() => {
        inFlight -= 1;
        openBy[job.generation]! -= open;
        pump();
      });
  };

  const flush = () => {
    const generation = pendingGeneration;
    const jobs = pack(
      pending.sort((a, b) => a - b),
      generation,
    );
    const remainder = jobs.pop();
    for (const job of jobs) dispatch(job);
    flushes.full += jobs.length;
    pending = [];
    pendingGeneration = 0;
    if (remainder === undefined) return;
    const release = openBefore(generation) === 0 ? "drain" : eager ? "eager" : undefined;
    if (release === undefined) {
      pending = [...remainder.indices];
      pendingGeneration = generation;
      return;
    }
    flushes[release] += 1;
    dispatch(remainder);
  };

  const pump = () => {
    if (failure === undefined && pending.length > 0) {
      try {
        flush();
      } catch (reason) {
        failure = { reason };
      }
    }
    if (failure !== undefined) {
      pending = [];
      if (inFlight === 0) settle.reject(failure.reason);
      return;
    }
    if (inFlight === 0) settle.resolve();
  };

  return {
    dispatch,
    /** Pools `indices` for a later flush at `generation`, the one behind the job that left them. */
    defer(indices: readonly number[], generation: number) {
      pending.push(...indices);
      if (generation > pendingGeneration) pendingGeneration = generation;
    },
    /** Settles once nothing is pending or in flight. */
    run(): Promise<void> {
      pump();
      return done;
    },
    flushes,
    /** The deepest generation dispatched: how many continuations lay behind the last tail. */
    get generations() {
      return Math.max(0, openBy.length - 1);
    },
  };
}

/**
 * Zero bytes in the four words of a clear chunk's wrapper that count its `k` elements and `body`
 * bytes: the wire's `n` and `bodyLen`, the ABI length of the wire (`64 + body`) and the offset of
 * `factoryData` behind it (`256 + body`). Everything else in the wrapper is the same for every chunk.
 */
function countingWordZeros(k: number, body: number): number {
  return 128 - nonzeroBytesOf(k) - nonzeroBytesOf(body) - nonzeroBytesOf(64 + body) - nonzeroBytesOf(256 + body);
}

/** Non-zero bytes in the 32-byte big-endian encoding of `n`. */
function nonzeroBytesOf(n: number): number {
  let count = 0;
  for (let rest = n; rest > 0; rest = Math.floor(rest / 256)) if (rest % 256 !== 0) count++;
  return count;
}

/**
 * The number of elements the page adjudicated, in `1..count`: the decoder has already bound each
 * record to its ordinal and refused an empty page, so only a page longer than its chunk is left to catch.
 */
function adjudicated({ results, skipped, died }: Page, count: number): number {
  const attempted = results.length + skipped.length + (died === undefined ? 0 : 1);
  if (attempted > count) {
    throw new Error(`paginated lens attempted ${attempted} of ${count} elements, expected 1..${count}`);
  }
  return attempted;
}

/**
 * What one request came back with. A page is a revert carrying the sentinel; a call that returns
 * instead reached an account with no code; anything else failed upstream or with a revert of its own.
 */
type ChunkOutcome =
  | Extract<EnvelopeRevert, { kind: "page" }>
  | { kind: "returned" }
  | { kind: "failed"; error: unknown };

async function send(requestFn: EIP1193RequestFn<PublicRpcSchema>, params: RpcEthCallParams): Promise<ChunkOutcome> {
  try {
    await requestFn({ method: "eth_call", params }, { retryCount: 0 });
  } catch (error) {
    const revert = decodeEnvelopeRevert(error);
    return revert?.kind === "page" ? revert : { kind: "failed", error };
  }
  return { kind: "returned" };
}

type Action =
  | { kind: "throw"; message: string; cause?: unknown }
  | { kind: "propagate"; error: unknown }
  | { kind: "halve"; reason: "size" | "timeout"; timeoutSplits: number }
  | { kind: "fallback"; reason: FallbackReason };

/** The envelope's reverts that prove it ran and that no smaller chunk can cure. */
const FATAL_MESSAGES: Record<Exclude<EnvelopeRevert["kind"], "page">, string> = {
  malformedResult: "lens returned a per-item result that does not fit its declared layout",
  malformedInput: "envelope rejected the input wire (codec bug)",
  counterfactualDeployFailed: "counterfactual deploy failed: target occupied, constructor reverted, or no code",
  outOfGas: "counterfactual deploy (factory or constructor) ran out of gas under this node's cap",
};

/**
 * What a chunk that did not page gets: proof the envelope ran is fatal; a size or timeout refusal
 * halves while there is room; an initcode chunk's other failures propagate. An override chunk
 * failing without proof that the envelope ran gets one initcode attempt, and only a refusal of the
 * request's shape — or a call that returned, reaching an account with no code — says the provider
 * does not honour overrides.
 */
function classifyOutcome(
  outcome: Exclude<ChunkOutcome, { kind: "page" }>,
  { delivery, indices, timeoutSplits }: ChunkJob,
): Action {
  if (outcome.kind === "returned") {
    return delivery === "initcode"
      ? { kind: "throw", message: "revert-mode wrapper returned without reverting" }
      : { kind: "fallback", reason: "unsupported" };
  }
  const { error } = outcome;
  const revert = decodeEnvelopeRevert(error);
  if (revert !== null && revert.kind !== "page") {
    return { kind: "throw", message: FATAL_MESSAGES[revert.kind], cause: error };
  }
  const cause = classifyChunkError(error);
  const divisible = indices.length > 1;
  if (cause === "size" && divisible) return { kind: "halve", reason: "size", timeoutSplits };
  if (cause === "timeout" && divisible && timeoutSplits > 0) {
    return { kind: "halve", reason: "timeout", timeoutSplits: timeoutSplits - 1 };
  }
  if (delivery === "initcode") return { kind: "propagate", error };
  if (cause !== null) return { kind: "fallback", reason: "exhausted" };
  if (isInvalidParamsError(error)) return { kind: "fallback", reason: "unsupported" };
  return { kind: "fallback", reason: "unproven" };
}

/** A JSON-RPC refusal of the request's shape: code `-32602`, or a message naming the state override. */
function isInvalidParamsError(error: unknown): boolean {
  for (const cur of causeChain(error)) {
    if ((cur as { code?: unknown }).code === -32602) return true;
    const msg = (cur as { message?: unknown }).message;
    if (typeof msg === "string" && /state[ _-]?overrides?|third param/i.test(msg)) return true;
  }
  return false;
}

/**
 * Greedy packer over positions in `indices`: each chunk takes the longest prefix of the remainder
 * that `fits`, found by binary search. An element that does not fit alone is reported in `oversize`
 * and left out rather than sent. A clear tuple's size is monotone in its elements; a compressed
 * one's is not (FastLZ), so `shrink` walks the found end back until it fits.
 */
function packBatches(
  indices: Chunk,
  fits: (start: number, end: number) => boolean,
  shrink: boolean,
): { chunks: Chunk[]; oversize: number[] } {
  const chunks: Chunk[] = [];
  const oversize: number[] = [];
  const count = indices.length;

  let start = 0;
  while (start < count) {
    if (fits(start, count)) {
      chunks.push(indices.slice(start));
      break;
    }
    if (!fits(start, start + 1)) {
      oversize.push(indices[start]!);
      start += 1;
      continue;
    }

    let end = start + 1;
    let hi = count;
    while (end < hi) {
      const mid = Math.floor((end + hi + 1) / 2);
      if (fits(start, mid)) {
        end = mid;
      } else {
        hi = mid - 1;
      }
    }
    if (shrink) {
      while (end > start + 1 && !fits(start, end)) {
        end--;
      }
    }

    chunks.push(indices.slice(start, end));
    start = end;
  }
  return { chunks, oversize };
}

/**
 * Classifies an upstream error for the deployless batcher: `null` for unrelated errors, which
 * propagate without retry. Timeout is checked first so a TimeoutError with an incidentally
 * size-shaped message still routes through the cautious-bisect path: a timeout may be the request's
 * size or a slow node, so callers bisect a bounded number of times rather than `2^depth`. A size
 * refusal scales with the request (HTTP 413, calldata, EIP-3860 initcode, intrinsic and EIP-7623
 * floor gas), so bisecting always helps.
 */
function classifyChunkError(error: unknown): "size" | "timeout" | null {
  if (isTimeoutLikeError(error)) return "timeout";

  const e = error instanceof BaseError ? error.walk() : error;
  const status = (e as { status?: number }).status;
  const msg = (e as { message?: string }).message ?? "";

  if (status === 413) return "size";
  if (
    /too large/i.test(msg) ||
    /request.{0,10}size/i.test(msg) ||
    /code.{0,10}size/i.test(msg) ||
    /intrinsic gas/i.test(msg) ||
    /floor data gas/i.test(msg)
  ) {
    return "size";
  }

  return null;
}
