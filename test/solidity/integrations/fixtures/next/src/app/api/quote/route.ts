// Executes the lens: one deployless eth_call against the node the test started.
import { createPublicClient, http } from "viem";

import { QuoteLens } from "@/lens";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const rpcUrl = new URL(request.url).searchParams.get("rpc");
  if (!rpcUrl) return Response.json({ bytecode: QuoteLens.bytecode });
  const client = createPublicClient({ transport: http(rpcUrl) });
  const quote = await client.readContract({
    abi: QuoteLens.abi,
    code: QuoteLens.bytecode,
    functionName: "quote",
    args: [3n * 10n ** 18n, 2000n * 10n ** 18n, 3n * 10n ** 15n],
  });
  return Response.json({
    bytecode: QuoteLens.bytecode,
    quote: { gross: String(quote.gross), fee: String(quote.fee), net: String(quote.net) },
  });
}
