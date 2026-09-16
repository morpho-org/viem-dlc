import { Badge, Button, Flex, Text } from "@radix-ui/themes";
import * as React from "react";

import type { WideEvent } from "../logger.js";

import { format } from "./WideEvent.js";

type Stamp = { id: number; at: number };
export type StreamInput =
  | { kind: "log"; step: string; text: string }
  | { kind: "event"; step: string; event: WideEvent };
export type StreamEntry = StreamInput & Stamp;

/** Keeps the pane bounded on a long session; the per-step table holds the full record anyway. */
const MAX_ENTRIES = 400;

let nextId = 0;

export function useEventStream() {
  const [entries, setEntries] = React.useState<StreamEntry[]>([]);
  const started = React.useRef(performance.now());

  const push = React.useCallback((entry: StreamInput) => {
    setEntries((prev) =>
      [...prev, { ...entry, id: nextId++, at: performance.now() - started.current }].slice(-MAX_ENTRIES),
    );
  }, []);

  const clear = React.useCallback(() => setEntries([]), []);

  return { entries, push, clear };
}

/** The facets worth a one-line summary, in the order they answer "what just happened". */
const SUMMARY_KEYS = ["elements_fetched", "logs_fetched", "nominal_batches", "pages_continued", "gaps_fetched"];

function summarize(event: WideEvent) {
  const namespace = Object.keys(event.fields)
    .find((key) => key.startsWith("viem-dlc-"))
    ?.split(".")[0];

  const parts = SUMMARY_KEYS.flatMap((name) => {
    const key = Object.keys(event.fields).find((candidate) => candidate.endsWith(`.${name}`) || candidate === name);
    return key === undefined ? [] : [`${name}=${format(event.fields[key])}`];
  });

  const duration = event.fields.duration_ms;
  if (typeof duration === "number") parts.push(`${duration.toFixed(0)}ms`);

  return { source: namespace ?? "event", detail: parts.join("  ") || event.message || "concluded" };
}

function Row({ entry }: { entry: StreamEntry }) {
  const [open, setOpen] = React.useState(false);

  if (entry.kind === "log") {
    return (
      <div className="stream-row">
        <span className="stream-at">{(entry.at / 1000).toFixed(1)}s</span>
        <Text size="1" color="gray">
          {entry.step}
        </Text>
        <Text size="1">{entry.text}</Text>
      </div>
    );
  }

  const { source, detail } = summarize(entry.event);

  return (
    <>
      <button type="button" className="stream-row stream-row-button" onClick={() => setOpen((prev) => !prev)}>
        <span className="stream-at">{(entry.at / 1000).toFixed(1)}s</span>
        <Badge size="1" color="jade" variant="soft">
          {source}
        </Badge>
        <Text size="1" color="gray">
          {detail}
        </Text>
      </button>
      {open ? (
        <div className="stream-detail">
          {Object.entries(entry.event.fields)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([key, value]) => (
              <div key={key}>
                <Text size="1" color="gray">
                  {key}
                </Text>
                <Text size="1">{format(value)}</Text>
              </div>
            ))}
        </div>
      ) : null}
    </>
  );
}

/**
 * A session-long feed of what the transports emitted, docked below the page. The per-step table is
 * the record of one run; this is the view of the run happening.
 */
export function EventStream({ entries, onClear }: { entries: StreamEntry[]; onClear: () => void }) {
  const [open, setOpen] = React.useState(true);
  const tail = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    if (open) tail.current?.scrollIntoView({ block: "end" });
  }, [open]);

  return (
    <aside className={open ? "stream stream-open" : "stream"}>
      <Flex align="center" gap="3" px="3" py="2" className="stream-bar">
        <Button size="1" variant="ghost" color="gray" onClick={() => setOpen((prev) => !prev)}>
          {open ? "▾" : "▴"} events
        </Button>
        <Badge size="1" color="gray" variant="soft">
          {entries.length}
        </Badge>
        <div className="stream-spacer" />
        <Button size="1" variant="ghost" color="gray" onClick={onClear}>
          clear
        </Button>
      </Flex>
      {open ? (
        <div className="stream-body">
          {entries.length === 0 ? (
            <Text size="1" color="gray">
              Run a step and its events appear here as they land.
            </Text>
          ) : (
            entries.map((entry) => <Row key={entry.id} entry={entry} />)
          )}
          <div ref={tail} />
        </div>
      ) : null}
    </aside>
  );
}
