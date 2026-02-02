import type { Address, LogTopic } from "viem";

import type { EthGetLogsParams } from "../../types.js";
import { cyrb64Hash } from "../../utils/hash.js";

/** Separator between filter part and block range part of cache key */
export const CACHE_KEY_SEPARATOR = "+";

function normalizeAddress(address: EthGetLogsParams["address"]): string {
  if (!address) return "*";
  if (Array.isArray(address)) {
    return address
      .map((a) => a.toLowerCase())
      .sort()
      .join(",");
  }
  return address.toLowerCase();
}

function normalizeTopics(topics: EthGetLogsParams["topics"]): string {
  if (!topics) return "*";
  return JSON.stringify(
    topics.map((t) => {
      if (t === null) return null;
      if (Array.isArray(t)) return t.map((x) => x.toLowerCase()).sort();
      return t.toLowerCase();
    }),
  );
}

export function computeCacheKey(params: {
  chainId: number;
  address?: Address | Address[];
  topics?: LogTopic[];
  fromBlock: bigint;
  toBlock: bigint;
}): string {
  const addressPart = normalizeAddress(params.address);
  const topicsPart = normalizeTopics(params.topics);

  const filterPart = cyrb64Hash(`${addressPart}:${topicsPart}`, 36, 777777);
  const rangePart = `${params.fromBlock}:${params.toBlock}`;

  return `${params.chainId}:ethGetLogs:${filterPart}${CACHE_KEY_SEPARATOR}${rangePart}`;
}
