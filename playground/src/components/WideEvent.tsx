import { Badge, Flex, Table, Text } from "@radix-ui/themes";

import type { RunOutcome } from "../runtime.js";

/** Facets that answer "did this change anything" — shown first, and never hidden. */
const HIGHLIGHTS = [
  "elements_requested",
  "elements_fetched",
  "elements_missing",
  "elements_unresolved",
  "nominal_batches",
  "pages_continued",
  "pages_escalated",
  "chunks_initcode",
  "chunks_override",
  "override_fallbacks_unsupported",
  "batch_bytes.max",
  "gaps_fetched",
  "blob_bytes_written",
  "fetch_ms",
  "nominal_ranges",
  "logs_fetched",
  "splits_count",
  "gas_limit_observed",
  "fixed_gas",
  "item_gas_avg",
  "item_gas_stddev",
];

export const format = (value: unknown) =>
  typeof value === "number" ? Math.round(value).toLocaleString("en-US") : String(value);

function facetRows(fields: Record<string, unknown>) {
  const keyOf = (name: string) => Object.keys(fields).find((key) => key === name || key.endsWith(`.${name}`));
  const shown = new Set<string>();

  const leading = HIGHLIGHTS.flatMap((name) => {
    const key = keyOf(name);
    if (key === undefined) return [];
    shown.add(key);
    return [{ name, value: fields[key], highlight: true }];
  });

  const rest = Object.keys(fields)
    .filter((key) => !shown.has(key))
    .sort()
    .map((key) => ({ name: key, value: fields[key], highlight: false }));

  return [...leading, ...rest];
}

/**
 * The transport's own account of the run. Picks the event carrying transport facets rather than the
 * first fat one, so a step that also makes plain calls still shows the interesting event.
 */
export function WideEvent({ outcome }: { outcome: RunOutcome }) {
  const event =
    outcome.events.find((candidate) => Object.keys(candidate.fields).some((key) => key.startsWith("viem-dlc-"))) ??
    outcome.events.find((candidate) => Object.keys(candidate.fields).length > 3);

  return (
    <>
      <Flex gap="2" wrap="wrap">
        {Object.entries(outcome.summary).map(([key, value]) => (
          <Badge key={key} color="gray" variant="soft" size="2">
            {format(value)} {key}
          </Badge>
        ))}
        <Badge color="jade" variant="soft" size="2">
          {outcome.requests} requests
        </Badge>
        <Badge color="gray" variant="soft" size="2">
          {outcome.elapsedMs.toFixed(0)} ms
        </Badge>
        {outcome.compiledInBrowser ? (
          <Badge color="amber" variant="soft" size="2">
            browser-compiled lens
          </Badge>
        ) : null}
      </Flex>
      {event ? (
        <Table.Root size="1" variant="surface">
          <Table.Body>
            {facetRows(event.fields).map((row) => (
              <Table.Row key={row.name}>
                <Table.Cell>
                  <Text color={row.highlight ? "jade" : undefined}>{row.name}</Text>
                </Table.Cell>
                <Table.Cell align="right">{format(row.value)}</Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      ) : (
        <Text color="gray">No wide event captured.</Text>
      )}
    </>
  );
}
