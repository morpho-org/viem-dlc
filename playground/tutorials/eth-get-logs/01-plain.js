import { createPublicClient, parseAbiItem } from "viem";
import { getBlockNumber, getLogs } from "viem/actions";

/** @type {`0x${string}`} */
const MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb";
const borrowEvent = parseAbiItem(
  "event Borrow(bytes32 indexed id, address caller, address indexed onBehalf, address indexed receiver, uint256 assets, uint256 shares)",
);

/**
 * Plain viem, no transport under it. Ask for a widening range and watch where the endpoint stops
 * answering. Which span fails depends on the endpoint, and nothing tells you in advance.
 *
 * @type {import("../../src/tutorials/types.js").Tab<{ settings: { blocks: string } }>}
 */
const run = async ({ transport, chain, log }) => {
  const client = createPublicClient({ chain, transport });
  const head = await getBlockNumber(client);

  const spans = [2_000n, 10_000n, 50_000n, 200_000n, 1_000_000n];
  let widest = 0n;
  let widestMs = 0;
  let firstError = "";

  for (const span of spans) {
    const started = performance.now();
    try {
      const logs = await getLogs(client, {
        address: MORPHO,
        event: borrowEvent,
        strict: true,
        fromBlock: head - span,
        toBlock: head,
      });
      const ms = performance.now() - started;
      log(`${span} blocks: ${logs.length} logs in ${ms.toFixed(0)} ms`);
      widest = span;
      widestMs = ms;
    } catch (error) {
      const message = `${error}`
        .split("\n")
        .find((line) => /limited|range|exceed|413|too many|response size/i.test(line))
        ?.replace(/^\s*Details:\s*/, "")
        .trim();
      log(`${span} blocks: rejected: ${message ?? "see the error"}`);
      firstError = message ?? "rejected";
      break;
    }
  }

  // The endpoint's own words go to the feed rather than a badge, since they are a sentence.
  log(firstError ? `the endpoint's limit: ${firstError}` : "no limit hit within the widest span tried");

  return {
    summary: {
      "widest span answered": Number(widest),
      "at that span": `${widestMs.toFixed(0)} ms`,
      "spans rejected": firstError ? spans.length - spans.indexOf(widest) - 1 : 0,
    },
  };
};

export default run;
