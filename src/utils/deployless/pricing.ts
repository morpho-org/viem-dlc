import type { Hex } from "viem";

import { type EnvelopeDelivery, FACTORY_BYTECODE_REVERT } from "./codec.envelope.js";
import type { PageGas } from "./codec.inner.js";

/** A byte string as the fee schedule sees it: its length and how many of its bytes are zero. */
export type WireSize = { bytes: number; zeros: number };

export function wireSize(hex: Hex): WireSize {
  return { bytes: hexByteLength(hex), zeros: zeroBytes(hex) };
}

export function hexByteLength(hex: Hex): number {
  return (hex.length - 2) / 2;
}

export function zeroBytes(hex: Hex): number {
  let zeros = 0;
  for (let i = 2; i < hex.length; i += 2) if (hex.charCodeAt(i) === 48 && hex.charCodeAt(i + 1) === 48) zeros++;
  return zeros;
}

const ENVELOPE_SIZE = wireSize(FACTORY_BYTECODE_REVERT);

/** The `data` a chunk with argument tuple `args` puts on the wire: the envelope's bytes ride only as initcode. */
export function sentSize(args: WireSize, delivery: EnvelopeDelivery): WireSize {
  return delivery === "initcode"
    ? { bytes: args.bytes + ENVELOPE_SIZE.bytes, zeros: args.zeros + ENVELOPE_SIZE.zeros }
    : args;
}

/**
 * What a node deducts from its cap before the envelope's first `gas()` returns: the transaction
 * base, calldata by byte (EIP-2028), as initcode the creation base and the initcode words
 * (EIP-3860), and that opcode's own 2. Ethereum's schedule; a chain that prices differently shifts
 * `gas_limit_observed` by the difference.
 */
export function intrinsicGas({ bytes, zeros }: WireSize, delivery: EnvelopeDelivery): number {
  const calldata = 21_000 + 4 * zeros + 16 * (bytes - zeros) + 2;
  return delivery === "initcode" ? calldata + 32_000 + 2 * Math.ceil(bytes / 32) : calldata;
}

/**
 * The least gas a message carrying these bytes may start with (EIP-7623, Prague): a node refuses
 * the call before anything runs, so a chunk above it is oversize, not short of gas.
 */
export function floorGas({ bytes, zeros }: WireSize): number {
  return 21_000 + 10 * zeros + 40 * (bytes - zeros);
}

const ENVELOPE_BASE = 0x80;
const ENVELOPE_FRAME = 0x1c0;
const ENVELOPE_HISTORY = 2 * 8192 + 320;

/**
 * What the envelope's prologue spends copying an `argsBytes`-long tuple into memory and expanding
 * memory to the slab's first word, where the budget is sampled — the part of a page's `fixed` that
 * is the chunk's bytes rather than the lens's. Layout constants are Envelope.yul's frame, pinned in
 * test/forge; memory is priced on Ethereum's schedule, a ceiling for chains that price it lower.
 */
export function copyGas(argsBytes: number, compressed: boolean): number {
  const argsEnd = ENVELOPE_BASE + argsBytes;
  const frame = argsEnd + ((32 - (argsEnd % 32)) % 32);
  const slab = frame + ENVELOPE_FRAME + (compressed ? ENVELOPE_HISTORY : 0);
  return 3 * Math.ceil(argsBytes / 32) + memcost(slab + 0x20);
}

function memcost(bytes: number): number {
  const w = Math.ceil(bytes / 32);
  return 3 * w + Math.floor((w * w) / 512);
}

/**
 * A lens's cost as the caller states it, in the units the wide event reports: `fixed` is what a
 * frame spends before its first attempt less the copy of the chunk's own bytes (`fixed_gas`),
 * `item` the per-attempt mean and deviation (`item_gas_avg`, `item_gas_stddev`). Both are
 * properties of the lens, not of any provider or chunk.
 */
export type LensGas = { fixed: number; item: { avg: number; stddev?: number } };

/**
 * The gas a request's chunks are predicted against: the stated cap and lens cost until a page has
 * landed, the pooled observations after. Prediction and observation price a chunk's bytes through
 * the same functions, so the two cannot drift.
 */
export type CostModel = {
  /** False until anything bounds a chunk's gas; the packer then packs by bytes alone. */
  readonly known: boolean;
  /**
   * Whether a chunk of `k` elements with argument tuple `tuple` is predicted to fit: its bytes must
   * clear EIP-7623's floor and leave room after intrinsic gas and the prologue's copy, and its
   * attempts with headroom must fit what remains after the lens's prologue. A lone element always
   * fits the attempt line: that estimate may shorten a chunk but never withhold an element, so the
   * envelope decides what is served. The byte lines are protocol bounds the node enforces before
   * anything runs, so a lone element above them is oversize.
   */
  fits(tuple: WireSize, k: number, delivery: EnvelopeDelivery): boolean;
  /** Pools one page's telemetry, priced against the chunk that fetched it; `costed` is its attempts less a death. */
  observe(page: PageGas, costed: number, tuple: WireSize, delivery: EnvelopeDelivery): void;
  /** The wide event's gas fields, empty until a page has landed. */
  fields(): Record<string, number>;
};

export function costModel(cap: number | undefined, lens: LensGas | undefined, compressed: boolean): CostModel {
  const stated = statedGas(cap, lens);
  let pooled: GasStats | undefined;
  const params = (): GasParams | undefined => {
    if (pooled === undefined) return stated;
    const item = pooled.costed === 0n ? stated?.item : { fixed: Number(pooled.fixedLessCopy), ...moments(pooled) };
    return { cap: Number(pooled.cap), item };
  };
  return {
    get known() {
      return params() !== undefined;
    },
    fits(tuple, k, delivery) {
      const p = params();
      if (p === undefined) return true;
      const sent = sentSize(tuple, delivery);
      if (floorGas(sent) > p.cap) return false;
      const bytes = intrinsicGas(sent, delivery) + copyGas(tuple.bytes, compressed);
      if (bytes > p.cap) return false;
      return k === 1 || p.item === undefined || bytes + p.item.fixed + chunkCost(k, p.item.avg, p.item.stddev) <= p.cap;
    },
    observe(page, costed, tuple, delivery) {
      const intrinsic = intrinsicGas(sentSize(tuple, delivery), delivery);
      pooled = pool(pooled, page, costed, intrinsic, copyGas(tuple.bytes, compressed));
    },
    fields() {
      return pooled === undefined ? {} : gasFields(pooled);
    },
  };
}

/**
 * What {@link CostModel.fits} needs: a provider's cap, and, when known, a lens's prologue and
 * per-attempt cost. The cap alone bounds a chunk's bytes; the item figures bound its elements.
 */
type GasParams = { cap: number; item?: { fixed: number; avg: number; stddev: number } };

/** The provider's cap and `batch.gas` as the prediction uses them: nothing without a cap, the cap alone without a usable cost. */
function statedGas(cap: number | undefined, gas: LensGas | undefined): GasParams | undefined {
  if (cap === undefined) return undefined;
  const capOnly = { cap };
  if (typeof gas !== "object" || gas === null || typeof gas.item !== "object" || gas.item === null) return capOnly;
  const { fixed } = gas;
  const { avg, stddev = 0 } = gas.item;
  const usable =
    Number.isFinite(fixed) && fixed >= 0 && Number.isFinite(avg) && avg > 0 && Number.isFinite(stddev) && stddev >= 0;
  return usable ? { cap, item: { fixed, avg, stddev } } : capOnly;
}

/**
 * Deviations of headroom a predicted chunk keeps below the budget. A target, not a bound: attempt
 * costs are correlated, so Cantelli's `1 / (1 + z²)` does not hold (see docs/000016-tib-paginated-lenses.md).
 * An overshoot costs one continuation, packed from more data.
 */
const PACKING_SIGMAS = 2;

/** The cost of a chunk of `k` attempts at mean `mean` and deviation `sigma` each, with headroom for the spread. */
function chunkCost(k: number, mean: number, sigma: number): number {
  return k * mean + PACKING_SIGMAS * sigma * Math.sqrt(k);
}

/**
 * The gas telemetry of every page a request has seen, pooled: `budget` is the smallest frame's,
 * `fixedLessCopy` the largest prologue less the copy of the chunk's own bytes ({@link copyGas}),
 * `cap` the smallest gas limit a page's frame implies, and the attempt moments over `costed` attempts.
 */
type GasStats = {
  budget: bigint;
  fixedLessCopy: bigint;
  cap: bigint;
  costed: bigint;
  sum: bigint;
  sumSquares: bigint;
  max: bigint;
};

/** `intrinsic` is what the node deducted for the chunk's bytes before the frame began, `copy` what the prologue spent on them. */
function pool(stats: GasStats | undefined, page: PageGas, costed: number, intrinsic: number, copy: number): GasStats {
  const cap = BigInt(intrinsic) + page.fixed + page.budget;
  const fixedLessCopy = page.fixed > BigInt(copy) ? page.fixed - BigInt(copy) : 0n;
  if (stats === undefined) {
    return {
      budget: page.budget,
      fixedLessCopy,
      cap,
      costed: BigInt(costed),
      sum: page.sum,
      sumSquares: page.sumSquares,
      max: page.max,
    };
  }
  return {
    budget: page.budget < stats.budget ? page.budget : stats.budget,
    fixedLessCopy: fixedLessCopy > stats.fixedLessCopy ? fixedLessCopy : stats.fixedLessCopy,
    cap: cap < stats.cap ? cap : stats.cap,
    costed: stats.costed + BigInt(costed),
    sum: stats.sum + page.sum,
    sumSquares: stats.sumSquares + page.sumSquares,
    max: page.max > stats.max ? page.max : stats.max,
  };
}

/** Mean and population deviation of one attempt's cost; the variance's numerator stays exact in bigint. */
function moments({ costed, sum, sumSquares }: GasStats) {
  const n = Number(costed);
  return {
    avg: Number(sum) / n,
    stddev: Math.sqrt(Number(costed * sumSquares - sum * sum)) / n,
  };
}

function gasFields(gas: GasStats): Record<string, number> {
  const frame = {
    frame_gas: Number(gas.budget),
    fixed_gas: Number(gas.fixedLessCopy),
    gas_limit_observed: Number(gas.cap),
  };
  if (gas.costed === 0n) return frame;
  const { avg, stddev } = moments(gas);
  return { ...frame, item_gas_avg: avg, item_gas_stddev: stddev, item_gas_max: Number(gas.max) };
}
