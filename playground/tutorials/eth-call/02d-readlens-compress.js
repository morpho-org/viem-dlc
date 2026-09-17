import { readLens } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { createPublicClient } from "viem";

/**
 * The other way to fit more elements under a byte cap: send fewer bytes.
 *
 * `compress: true` FastLZ-compresses the element array on the wire, and the envelope decompresses
 * element by element as it attempts them — so a highly compressible chunk pages like any other and
 * costs nothing before its first element. Addresses padded to 32 bytes compress well, which is most
 * element types.
 *
 * You pay encoding time on the client and decompression gas on the node. Compare `batch_bytes.max`
 * and `nominal_batches` in the table against the other tabs; if bytes weren't binding, this buys
 * nothing and costs a little.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ transport, chain, settings, lens, vaults, log }) => {
  const client = createPublicClient({ chain, transport: deployless(transport) });

  const count = Number(settings.vaults);
  const grief = BigInt(settings.grief);
  const inputs = vaults.slice(0, count).map((vault, i) => ({ vault, grief: i === 0 ? grief : 0n }));

  log(`${inputs.length} vaults, calldata compressed`);

  const { results, skipped } = await readLens(client, {
    ...lens.with(),
    functionName: "snapshotOf",
    args: inputs,
    batch: { compress: true },
  });

  log(`${results.length} served, ${skipped.length} skipped`);

  return { summary: { served: results.length, skipped: skipped.length } };
};

export default run;
