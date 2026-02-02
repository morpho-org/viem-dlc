import type { Account, Address, Chain, Client, Transport } from "viem";
import { getBlockNumber, getCode } from "viem/actions";

export type GetDeploymentBlockParameters = {
  /** The contract address to find the deployment block for */
  address: Address;
  /** Optional lower bound for binary search. Defaults to 0. */
  fromBlock?: bigint;
  /** Optional upper bound for binary search. Defaults to latest block. */
  toBlock?: bigint;
};

export type GetDeploymentBlockReturnType = bigint | null;

/**
 * Finds the block at which a contract was deployed using binary search.
 *
 * Returns `null` if no code is found at the address (contract doesn't exist
 * or was self-destructed).
 *
 * @example
 * import { createPublicClient, http } from 'viem'
 * import { mainnet } from 'viem/chains'
 * import { getDeploymentBlock } from '@morpho-org/viem-dlc'
 *
 * const client = createPublicClient({
 *   chain: mainnet,
 *   transport: http()
 * })
 *
 * const deploymentBlock = await getDeploymentBlock(client, {
 *   address: '0x1234...'
 * })
 */
export async function getDeploymentBlockNumber<
  TTransport extends Transport = Transport,
  TChain extends Chain | undefined = Chain | undefined,
  TAccount extends Account | undefined = Account | undefined,
>(
  client: Client<TTransport, TChain, TAccount>,
  { address, fromBlock, toBlock }: GetDeploymentBlockParameters,
): Promise<GetDeploymentBlockReturnType> {
  const latestBlock = toBlock ?? (await getBlockNumber(client));

  // First check if contract exists at all
  const currentCode = await getCode(client, {
    address,
    blockNumber: latestBlock,
  });
  if (!currentCode) {
    return null;
  }

  // Binary search to find exact deployment block
  let low = fromBlock ?? 0n;
  let high = latestBlock;
  let x: bigint | null = null;

  while (low <= high) {
    const mid = (low + high) / 2n;
    const code = await getCode(client, { address, blockNumber: mid });

    if (code) {
      // Contract exists at mid, search lower half
      x = mid;
      high = mid - 1n;
    } else {
      // Contract doesn't exist at mid, search upper half
      low = mid + 1n;
    }
  }

  return x;
}
