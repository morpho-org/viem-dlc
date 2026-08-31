/**
 * `getDeploymentBlockNumber`: binary-search `eth_getCode` to find where a contract first appeared.
 * Useful as the `fromBlock` for a full-history `getLogs2`, instead of hardcoding it.
 */
import { getDeploymentBlockNumber } from "@morpho-org/viem-dlc/actions";
import { createPublicClient, http } from "viem";
import { base } from "viem/chains";

const rpcUrl = process.env.RPC_URL;
if (!rpcUrl) throw new Error("Set RPC_URL (see examples/README.md)");

const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb" as const;
const MORPHO_DEPLOYMENT_BLOCK = 13_977_148n;

const client = createPublicClient({ chain: base, transport: http(rpcUrl) });

const block = await getDeploymentBlockNumber(client, { address: MORPHO });

console.log(`Morpho Blue was deployed at block ${block} (expected ${MORPHO_DEPLOYMENT_BLOCK})`);
