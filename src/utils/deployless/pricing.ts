import type { Hex } from "viem";

import { type EnvelopeDelivery, FACTORY_BYTECODE_REVERT } from "./codec.envelope.js";

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
