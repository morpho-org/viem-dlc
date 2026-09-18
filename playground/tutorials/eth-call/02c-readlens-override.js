import { readLens } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { createPublicClient } from "viem";

/**
 * Delivery by state override instead of initcode.
 *
 * By default a chunk *is* the envelope's initcode, so EIP-3860's 49,152-byte cap bounds it at
 * roughly 690 elements however much gas the frame has. `envelope: "override"` places the envelope
 * at a fixed address through `eth_call`'s state-override parameter, so the byte cap stops applying
 * and only the frame's gas and the provider's request size limit bound a chunk.
 *
 * Reach for it when the wide event says bytes bind well before gas. A provider that ignores
 * overrides is detected on the opening wave and the request finishes as initcode, which
 * `override_fallbacks_unsupported` reports. That costs one wasted wave and no results.
 *
 * Watch `chunks_override` and `chunks_initcode` below to see which delivery actually carried it.
 *
 * @type {import("../../src/tutorials/types.js").Tab<import("../../src/tutorials/eth-call.js").EthCallContext>}
 */
const run = async ({ transport, chain, settings, lens, vaults, log }) => {
  const client = createPublicClient({ chain, transport: deployless(transport) });

  const count = Number(settings.vaults);
  const grief = BigInt(settings.grief);
  const inputs = vaults.slice(0, count).map((vault, i) => ({ vault, grief: i === 0 ? grief : 0n }));

  log(`${inputs.length} vaults, delivered by state override`);

  const { results, skipped } = await readLens(client, {
    ...lens.with(),
    functionName: "snapshotOf",
    args: inputs,
    batch: { envelope: "override" },
  });

  log(`${results.length} served, ${skipped.length} skipped`);

  return { summary: { served: results.length, skipped: skipped.length } };
};

export default run;
