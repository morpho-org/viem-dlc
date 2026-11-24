/**
 * A local EVM for the solidity tests: one Anvil per test file, started on a free port.
 *
 * Anvil over an in-process EVM because it adds no dependency, it is the node the rest of this
 * repository's end-to-end tests already use, and a lens is read through it exactly as in production:
 * viem's deployless `eth_call`.
 */
import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { createServer } from "node:net";

import { type Address, createPublicClient, createWalletClient, type Hex, http, type PublicClient } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { foundry } from "viem/chains";

/** Anvil's first default account. */
const ANVIL_ACCOUNT_0 = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

export const hasAnvil = (() => {
  try {
    execFileSync("anvil", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

/** In CI a missing Anvil is a failure, not a skip: these tests are what proves a lens executes. */
export const requireAnvil = (): boolean => {
  if (!hasAnvil && process.env.CI) throw new Error("anvil is not on PATH, and the solidity tests need it in CI");
  return hasAnvil;
};

const freePort = () =>
  new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as { port: number };
      server.close(() => resolve(port));
    });
  });

/** The outcome of one `eth_call`: the exact return data, or the exact revert data. */
export interface CallOutcome {
  status: "success" | "revert";
  data: Hex;
}

export interface Evm {
  client: PublicClient;
  /** Deploys creation bytecode (constructor arguments appended) and returns the contract's address. */
  deploy(initcode: Hex): Promise<Address>;
  /** One raw `eth_call`, with nothing between the test and the node. */
  call(to: Address, data: Hex): Promise<CallOutcome>;
  stop(): void;
}

export async function startAnvil(): Promise<Evm> {
  const port = await freePort();
  const url = `http://127.0.0.1:${port}`;
  const child: ChildProcess = spawn("anvil", ["--port", String(port), "--silent"], { stdio: "ignore" });
  // Anvil mines a transaction as it arrives, so a receipt is there on the first poll.
  const client = createPublicClient({ chain: foundry, transport: http(url), pollingInterval: 10 });
  const wallet = createWalletClient({
    chain: foundry,
    transport: http(url),
    account: privateKeyToAccount(ANVIL_ACCOUNT_0),
  });

  for (let attempt = 0; ; attempt++) {
    try {
      await client.getBlockNumber();
      break;
    } catch (cause) {
      if (attempt === 100) {
        child.kill();
        throw new Error(`anvil did not start on port ${port}`, { cause });
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }

  return {
    client,
    async deploy(initcode) {
      const hash = await wallet.sendTransaction({ data: initcode });
      const receipt = await client.waitForTransactionReceipt({ hash });
      if (receipt.status !== "success" || !receipt.contractAddress) throw new Error("the deployment reverted");
      return receipt.contractAddress;
    },
    async call(to, data) {
      const response = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "eth_call", params: [{ to, data }, "latest"] }),
      });
      const body = (await response.json()) as { result?: Hex; error?: { message: string; data?: Hex } };
      if (body.result !== undefined) return { status: "success", data: body.result };
      if (body.error?.data !== undefined || /revert/i.test(body.error?.message ?? ""))
        return { status: "revert", data: body.error?.data ?? "0x" };
      throw new Error(`eth_call failed: ${JSON.stringify(body.error)}`);
    },
    stop: () => void child.kill(),
  };
}
