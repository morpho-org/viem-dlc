import { BaseError, type EIP1193RequestFn, type Hex, type PublicRpcSchema, toHex } from "viem";

import type { ChainDefinition, EthCallGas } from "../../chains/index.js";
import type { Facet } from "../../observability.js";
import type { EIP1193Parameters } from "../../types.js";
import { causeChain, isTimeoutLikeError } from "../errors.js";
import type { Tail } from "../tuples.js";

import {
  type DeploylessTarget,
  deliveryParams,
  ENVELOPE_ADDRESS,
  type EnvelopeDelivery,
  encodeEnvelopeArgs,
  envelopeConfig,
  extractRevertData,
  isCounterfactualDeployFailedRevert,
  isMalformedInputRevert,
  isMalformedResultRevert,
  isOutOfGasRevert,
  overridesEnvelopeAddress,
} from "./codec.envelope.js";
import { arrayToWire, hexToPage, type Page, type ResolvedArrayFunction } from "./codec.inner.js";
import { costModel, hexByteLength, type LensGas, sentSize, type WireSize, wireSize, zeroBytes } from "./pricing.js";

export type RestOfEthCallParams = Tail<EIP1193Parameters<PublicRpcSchema, "eth_call">["params"]>;

type FactorisedFactoryCallParams = {
  target: DeploylessTarget;
  elements: readonly Hex[];
  solidity: ResolvedArrayFunction;
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
 * by {@link provider}.
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
  delivery: DeliveryMemo;
};

export function provider(chain: ChainDefinition, gasLimit: number | undefined): Provider {
  const cap = gasLimit !== undefined && Number.isSafeInteger(gasLimit) && gasLimit > 0 ? gasLimit : undefined;
  return {
    cap,
    gas: cap !== undefined && chain.ethCall.gasWhenUnspecified === "fixedDefault" ? toHex(cap) : undefined,
    delivery: { unsupported: false },
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

export type { LensGas } from "./pricing.js";

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
    solidity,
    batch,
    provider: { cap, gas: sentGas, delivery: memo },
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
  const missing: number[] = [];
  const unresolved: number[] = [];
  const oversize: number[] = [];
  const config = envelopeConfig(solidity, compress);
  const args = (indices: Chunk): Hex =>
    encodeEnvelopeArgs(
      {
        target,
        targetData: arrayToWire(
          solidity.inputLayout,
          indices.map((i) => elements[i]!),
        ),
      },
      { compress, config },
    );

  let referenceArgs: Hex | undefined;
  const getReferenceArgs = () => {
    if (!referenceArgs) referenceArgs = args(everything);
    return referenceArgs;
  };

  // Static layouts contribute `layout.size` per element; dynamic ones a length word plus their
  // padded bytes. Both are multiples of 32, so the wrapper's own padding is a per-batch constant.
  const everything: number[] = [];
  const bytesOf: number[] = [];
  const zerosOf: number[] = [];
  const layout = solidity.inputLayout;
  let totalBytes = 0;
  let totalZeros = 0;
  for (let pos = 0; pos < elements.length; pos++) {
    const element = elements[pos]!;
    const bytes = layout.mode === "static" ? layout.size : 32 + hexByteLength(element);
    const zeros = zeroBytes(element) + (layout.mode === "static" ? 0 : 32 - nonzeroBytesOf(hexByteLength(element)));
    everything.push(pos);
    bytesOf.push(bytes);
    zerosOf.push(zeros);
    totalBytes += bytes;
    totalZeros += zeros;
  }
  let overhead: WireSize | undefined;
  /** Sizes the argument tuple of the sub-lists `[start, end)` of `indices`. */
  const measurer = (indices: Chunk): ((start: number, end: number) => WireSize) => {
    if (compress) {
      return (start, end) =>
        wireSize(
          start === 0 && end === indices.length && end === elements.length
            ? getReferenceArgs()
            : args(indices.slice(start, end)),
        );
    }
    if (overhead === undefined) {
      const whole = wireSize(getReferenceArgs());
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
   * fits neither alone is declined as oversize.
   */
  const pack = (indices: Chunk, delivery: EnvelopeDelivery): Chunk[] => {
    if (indices.length === 0) return [];
    if (wireCap === Infinity && !cost.known) return [indices];
    const measure = measurer(indices);
    const fits = (start: number, end: number) => {
      const tuple = measure(start, end);
      return sentSize(tuple, delivery).bytes <= wireCap && cost.fits(tuple, end - start, delivery);
    };
    const packed = packBatches(indices, fits);
    for (const index of packed.oversize) {
      oversize.push(index);
      missing.push(index);
    }
    return packed.chunks;
  };

  const opening = envelope;
  const chunks = pack(everything, opening);
  const outputs = new Array<Hex>(elements.length);

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
  let fetched = 0;
  const splits = { count: 0, size: 0, timeout: 0, maxDepth: 0 };
  const sent = { override: 0, initcode: 0 };
  const fallbacks = { unsupported: 0, unproven: 0, exhausted: 0 };
  // A lens stopping early is a continuation, a mid-page gas death an escalation; neither is a
  // split, which means only "the provider refused the request's size or timed out".
  const pages = { continued: 0, escalated: 0, unresolvedAttempts: 0, allSkipped: 0 };
  const continuations: ContinuationMode = batch?.continuations === "eager" ? "eager" : "fill";
  const flushes = { full: 0, drain: 0, eager: 0 };
  let generationMax = 0;

  const commit = async (entries: readonly ResolvedElement[]) => {
    for (const { index, output } of entries) outputs[index] = output;
    fetched += entries.length;
    if (entries.length > 0) await onResolved?.(entries);
  };

  /**
   * An override chunk that failed without proving the envelope ran is re-packed as initcode and
   * each piece dispatched at its generation, so the pending list waits for them as for any chunk.
   */
  const fallback = (indices: Chunk, generation: number, reason: keyof typeof fallbacks) => {
    fallbacks[reason] += 1;
    for (const piece of pack(indices, "initcode")) dispatch(piece, generation, "initcode");
  };

  /** `generation` counts the continuations behind a chunk: 0 for the opening wave, one more per tail. */
  const fetchRecursive = async (
    indices: Chunk,
    generation: number,
    delivery: EnvelopeDelivery,
    precomputed?: Hex,
    timeoutSplitsRemaining = 1,
    depth = 0,
  ): Promise<void> => {
    if (depth > splits.maxDepth) splits.maxDepth = depth;
    const count = indices.length;
    const tuple = precomputed ?? args(indices);
    const tupleSize = wireSize(tuple);
    sent[delivery] += 1;

    let outcome: Awaited<ReturnType<typeof fetchChunk>>;
    try {
      outcome = await fetchChunk(requestFn, deliveryParams(delivery, tuple, restOfEthCallParams, sentGas));
    } catch (e) {
      if (isMalformedResultRevert(e)) {
        throw new Error("[deployless] lens returned a per-item result that does not fit its declared layout", {
          cause: e,
        });
      }
      if (isMalformedInputRevert(e)) {
        throw new Error("[deployless] envelope rejected the input wire (codec bug)", { cause: e });
      }
      if (isCounterfactualDeployFailedRevert(e)) {
        throw new Error(
          "[deployless] counterfactual deploy failed: target occupied, constructor reverted, or no code",
          {
            cause: e,
          },
        );
      }
      if (isOutOfGasRevert(e)) {
        throw new Error(
          "[deployless] counterfactual deploy (factory or constructor) ran out of gas under this node's cap",
          {
            cause: e,
          },
        );
      }
      const halve = (nextBudget = timeoutSplitsRemaining) => {
        const mid = Math.floor(count / 2);
        dispatch(indices.slice(0, mid), generation, delivery, undefined, nextBudget, depth + 1);
        dispatch(indices.slice(mid), generation, delivery, undefined, nextBudget, depth + 1);
      };
      const cause = classifyChunkError(e);
      if (cause === "size" && count > 1) {
        splits.count += 1;
        splits.size += 1;
        return halve();
      }
      if (cause === "timeout" && count > 1 && timeoutSplitsRemaining > 0) {
        splits.count += 1;
        splits.timeout += 1;
        return halve(timeoutSplitsRemaining - 1);
      }
      if (delivery === "initcode") throw e;
      // Nothing above proved the envelope ran, so the range gets one initcode attempt; only a
      // refusal of the request's shape says the provider does not honour overrides.
      if (cause !== null) return fallback(indices, generation, "exhausted");
      if (isInvalidParamsError(e)) return fallback(indices, generation, "unsupported");
      return fallback(indices, generation, "unproven");
    }
    if (outcome.kind === "returned") {
      // The call reached an account with no code: the provider dropped the override.
      if (delivery === "initcode") throw new Error("revert-mode wrapper returned without reverting");
      return fallback(indices, generation, "unsupported");
    }

    const page = hexToPage(solidity.outputLayout, outcome.returndata);
    const attempted = validatePage(page, count);
    cost.observe(page.gas, attempted - (page.died === undefined ? 0 : 1), tupleSize, delivery);
    facet?.stat("page_adjudicated", attempted);
    if (page.died === undefined && page.results.length === 0 && page.skipped.length > 0) pages.allSkipped += 1;

    const declined = new Set(page.skipped);
    const entries: ResolvedElement[] = [];
    for (let i = 0, served = 0; i < attempted; i++) {
      if (i === page.died) continue;
      if (declined.has(i)) missing.push(indices[i]!);
      else entries.push({ index: indices[i]!, output: page.results[served++]! });
    }
    await commit(entries);

    if (page.died !== undefined) {
      pages.unresolvedAttempts += 1;
      const pos = indices[page.died]!;
      if (count > 1) {
        pages.escalated += 1;
        dispatch([pos], generation, envelope);
      } else {
        missing.push(pos);
        unresolved.push(pos);
      }
    }

    if (attempted < count) {
      pages.continued += 1;
      for (let i = attempted; i < count; i++) pending.push(indices[i]!);
      if (generation + 1 > pendingGeneration) pendingGeneration = generation + 1;
    }
  };

  // Tails wait in `pending` and every chunk settling runs the pump, which re-packs them from the
  // pool as it stands and sends what the mode allows. Halves and singleton escalations are chunks
  // like any other. Nothing is dispatched after a failure; what is in flight settles first, its
  // results committed, and then the failure surfaces.
  let pending: number[] = [];
  let pendingGeneration = 0;
  let inFlight = 0;
  // Chunks in flight that could still leave a tail, by generation; a singleton adjudicates its whole input.
  const openBy: number[] = [];
  const openBefore = (generation: number) => openBy.slice(0, generation).reduce((n, c) => n + c, 0);
  let failure: { reason: unknown } | undefined;
  let settle!: { resolve: () => void; reject: (reason: unknown) => void };
  const done = new Promise<void>((resolve, reject) => {
    settle = { resolve, reject };
  });

  const dispatch = (
    indices: Chunk,
    generation: number,
    delivery: EnvelopeDelivery,
    precomputed?: Hex,
    timeoutSplits?: number,
    depth?: number,
  ) => {
    if (failure !== undefined) return;
    inFlight += 1;
    const open = indices.length > 1 ? 1 : 0;
    openBy[generation] = (openBy[generation] ?? 0) + open;
    if (generation > generationMax) generationMax = generation;
    fetchRecursive(indices, generation, delivery, precomputed, timeoutSplits, depth)
      .then(undefined, (reason) => {
        failure ??= { reason };
      })
      .then(() => {
        inFlight -= 1;
        openBy[generation]! -= open;
        pump();
      });
  };

  /**
   * Every chunk but the last the packer builds from `pending` is full: the next element did not fit
   * it. The last waits only for chunks of an earlier generation, the ones whose tails could still join it.
   */
  const flush = () => {
    const generation = pendingGeneration;
    const chunks = pack(
      pending.sort((a, b) => a - b),
      envelope,
    );
    const remainder = chunks.pop();
    for (const chunk of chunks) dispatch(chunk, generation, envelope);
    flushes.full += chunks.length;
    pending = [];
    pendingGeneration = 0;
    if (remainder === undefined) return;
    const release = openBefore(generation) === 0 ? "drain" : continuations === "eager" ? "eager" : undefined;
    if (release === undefined) {
      pending = [...remainder];
      pendingGeneration = generation;
      return;
    }
    flushes[release] += 1;
    dispatch(remainder, generation, envelope);
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

  try {
    const isWholeInput = chunks.length === 1 && chunks[0]!.length === elements.length;
    for (const chunk of chunks) dispatch(chunk, 0, opening, isWholeInput ? getReferenceArgs() : undefined);
    pump();
    await done;
  } finally {
    facet?.set({
      ...cost.fields(),
      elements_fetched: fetched,
      splits_count: splits.count,
      splits_size: splits.size,
      splits_timeout: splits.timeout,
      splits_max_depth: splits.maxDepth,
      attempts_unresolved: pages.unresolvedAttempts,
      pages_escalated: pages.escalated,
      pages_all_skipped: pages.allSkipped,
      pages_continued: pages.continued,
      continuations,
      continuation_depth_max: generationMax,
      flushes: flushes.full + flushes.drain + flushes.eager,
      flushes_full: flushes.full,
      flushes_drain: flushes.drain,
      flushes_eager: flushes.eager,
      elements_declined_oversize: oversize.length,
      elements_missing: missing.length,
      elements_unresolved: unresolved.length,
      chunks_override: sent.override,
      chunks_initcode: sent.initcode,
      override_fallbacks: fallbacks.unsupported + fallbacks.unproven + fallbacks.exhausted,
      override_fallbacks_unsupported: fallbacks.unsupported,
      override_fallbacks_unproven: fallbacks.unproven,
      override_fallbacks_exhausted: fallbacks.exhausted,
    });
  }

  return {
    outputs,
    missing: missing.sort((a, b) => a - b),
    unresolved: unresolved.sort((a, b) => a - b),
    oversize: oversize.sort((a, b) => a - b),
  };
}

/** Ascending indices into `elements`, sent as one request. */
type Chunk = readonly number[];

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
 * Returns the number of elements the page adjudicated, in `1..count`. The floor is what keeps every
 * chunk making progress; the decoder has already bound each record to its ordinal.
 */
function validatePage({ results, skipped, died }: Page, count: number): number {
  const attempted = results.length + skipped.length + (died === undefined ? 0 : 1);
  if (attempted < 1 || attempted > count) {
    throw new Error(`paginated lens attempted ${attempted} of ${count} elements, expected 1..${count}`);
  }
  return attempted;
}

/**
 * Sends one chunk, built by {@link deliveryParams}. A page is a revert carrying the sentinel; a call
 * that returns instead reached an account with no code, which only the caller can interpret.
 */
async function fetchChunk(
  requestFn: EIP1193RequestFn<PublicRpcSchema>,
  params: unknown[],
): Promise<{ kind: "page"; returndata: Hex } | { kind: "returned" }> {
  try {
    await requestFn({ method: "eth_call", params: params as never }, { retryCount: 0 });
  } catch (e) {
    const decoded = extractRevertData(e);
    if (!decoded.ok) throw e;
    return { kind: "page", returndata: decoded.returnData };
  }
  return { kind: "returned" };
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
 * that `fits`, found by binary search with a defensive linear shrink for measures that are not
 * perfectly monotone. An element that does not fit alone is reported in `oversize` and left out
 * rather than sent.
 */
function packBatches(
  indices: Chunk,
  fits: (start: number, end: number) => boolean,
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
    while (end > start + 1 && !fits(start, end)) {
      end--;
    }

    chunks.push(indices.slice(start, end));
    start = end;
  }
  return { chunks, oversize };
}

/**
 * Classifies an upstream error for the deployless batcher. Returns `null` for unrelated
 * errors (which should propagate without retry). Timeout is checked first so a TimeoutError
 * with an incidentally size-shaped message still routes through the cautious-bisect path.
 *
 * `"timeout"` covers errors that *may* be batch-induced but can also indicate a slow/flaky
 * upstream. Callers should bisect cautiously (e.g. limited splits per chunk) so a downed node
 * doesn't get hammered with `2^depth` retries:
 *   - viem TimeoutError, HTTP 408 / 504 / 524, generic "timed out" / "timeout" messages
 *
 * `"size"` covers errors that scale deterministically with batch size; bisecting always helps:
 *   - Calldata size:   HTTP 413; messages containing "too large" or "request size"
 *   - Initcode size (EIP-3860): "max initcode size exceeded" — matched by /code.*size/
 *   - Pre-execution gas (intrinsic, EIP-7623 floor): geth's "intrinsic gas" / "floor data gas"
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
