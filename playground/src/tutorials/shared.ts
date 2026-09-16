import { type Address, createPublicClient, type Hex } from "viem";
import { getBlockNumber } from "viem/actions";

import type { TabContext } from "./types.js";

export const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;

export const BORROW_EVENT =
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)" as const;

export type Pair = { id: Hex; user: Address };

export type Range = { fromBlock: bigint; toBlock: bigint };

/** Context added by {@link alignedRange}, plus the controls the cache examples declare. */
export type RangeContext = { range: Range; settings: { blocks: string; binSize: string } };

/**
 * A range ending below the chain tip and aligned to `binSize`.
 *
 * Both matter for the cache examples: an unaligned end leaves a partial bin, and a range touching
 * the tip is invalidated on every run, so the warm pass would refetch and the comparison would be
 * meaningless.
 */
export async function alignedRange(context: TabContext) {
  const client = createPublicClient({ chain: context.chain, transport: context.transport });
  const binSize = BigInt(context.settings.binSize ?? "10000");
  const head = await getBlockNumber(client);
  const toBlock = (head / binSize) * binSize - binSize;

  return { range: { fromBlock: toBlock - BigInt(context.settings.blocks ?? "50000"), toBlock } };
}
