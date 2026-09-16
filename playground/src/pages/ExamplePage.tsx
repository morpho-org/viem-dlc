import Badge from "@components/Badge";
import BlockLoader from "@components/BlockLoader";
import Button from "@components/Button";
import Card from "@components/Card";
import Divider from "@components/Divider";
import Input from "@components/Input";
import Table from "@components/Table";
import TableColumn from "@components/TableColumn";
import TableRow from "@components/TableRow";
import * as React from "react";

import { CodeEditor, type EditorHandle } from "../editor.js";
import type { Example, Settings } from "../examples/types.js";
import { type RunOutcome, run } from "../runtime.js";

/** Facets that answer "did this change anything" — shown first, and never hidden. */
const HIGHLIGHTS = [
  "elements_requested",
  "elements_fetched",
  "nominal_batches",
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
];

/**
 * Endpoints often carry credentials in the query string, and viem puts the full request URL in its
 * error text — which would otherwise be rendered here and end up in any screenshot.
 */
const redact = (message: string) => message.replace(/(https?:\/\/[^\s?]+)\?\S*/g, "$1?<redacted>");

const format = (value: unknown) => (typeof value === "number" ? value.toLocaleString("en-US") : String(value));

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

export function ExamplePage({
  example,
  rpcUrl,
  onRpcUrl,
}: {
  example: Example;
  rpcUrl: string;
  onRpcUrl: (v: string) => void;
}) {
  const [values, setValues] = React.useState<Settings>(() =>
    Object.fromEntries(example.controls.map((control) => [control.id, control.value])),
  );
  const [active, setActive] = React.useState(example.scripts[0]!.id);
  const [status, setStatus] = React.useState("");
  const [failed, setFailed] = React.useState(false);
  const [busy, setBusy] = React.useState(false);
  const [outcome, setOutcome] = React.useState<RunOutcome | undefined>();
  const [edited, setEdited] = React.useState(false);

  const solidityRef = React.useRef<EditorHandle>(null);
  const scriptRef = React.useRef<EditorHandle>(null);
  const scripts = React.useRef(new Map(example.scripts.map((s) => [s.id, s.source])));

  // Switching examples remounts the editors and resets local state.
  React.useEffect(() => {
    setValues(Object.fromEntries(example.controls.map((c) => [c.id, c.value])));
    setActive(example.scripts[0]!.id);
    scripts.current = new Map(example.scripts.map((s) => [s.id, s.source]));
    setOutcome(undefined);
    setStatus("");
    setEdited(false);
  }, [example]);

  const selectTab = (id: string) => {
    if (id === active) return;
    scripts.current.set(active, scriptRef.current?.value() ?? "");
    setActive(id);
    scriptRef.current?.set(scripts.current.get(id)!);
  };

  const onRun = async () => {
    setBusy(true);
    setFailed(false);
    setOutcome(undefined);

    try {
      const result = await run(
        {
          example,
          settings: { ...values, rpcUrl },
          script: scriptRef.current?.value() ?? "",
          solidity: example.solidity ? (solidityRef.current?.value() ?? example.solidity) : undefined,
        },
        setStatus,
      );
      setOutcome(result);
      setStatus("done");
    } catch (error) {
      setFailed(true);
      setStatus(
        redact(
          // Only the browser's own network failure means CORS; "gap fetch failed" is not that.
          error instanceof Error && /Failed to fetch|NetworkError|Load failed/i.test(error.message)
            ? `${error.message} — this endpoint may not allow browser requests (no CORS headers).`
            : `${error}`,
        ),
      );
    } finally {
      setBusy(false);
    }
  };

  const event = outcome?.events.find((e) => Object.keys(e.fields).length > 3);

  return (
    <>
      <Card title={example.title.toUpperCase()}>{example.blurb}</Card>

      <Card title="SETTINGS">
        <Input label="RPC URL (BASE)" name="rpc" value={rpcUrl} onChange={(e) => onRpcUrl(e.target.value)} />
        {example.controls.map((control) => (
          <Input
            key={control.id}
            label={control.label}
            name={control.id}
            value={values[control.id] ?? ""}
            onChange={(e) => setValues((prev) => ({ ...prev, [control.id]: e.target.value }))}
          />
        ))}
        <Divider type="DOUBLE" />
        <div className="actions">
          <Button onClick={onRun} isDisabled={busy}>
            {busy ? "RUNNING" : "RUN"}
          </Button>
          {busy ? <BlockLoader mode={9} /> : null}
          <span className={failed ? "status failed" : "status"}>{status}</span>
        </div>
      </Card>

      {example.scripts.length > 1 ? (
        <div className="tabs">
          {example.scripts.map((script) => (
            <Button
              key={script.id}
              theme={script.id === active ? "PRIMARY" : "SECONDARY"}
              onClick={() => selectTab(script.id)}
            >
              {script.title}
            </Button>
          ))}
        </div>
      ) : null}

      <div className={example.solidity ? "split" : ""}>
        {example.solidity ? (
          <Card title={edited ? "LENS · EDITED, COMPILES IN-BROWSER" : "LENS · PREBUILT"}>
            <CodeEditor
              key={`${example.id}-sol`}
              ref={solidityRef}
              doc={example.solidity}
              language="solidity"
              onChange={(doc) => setEdited(doc.trim() !== example.solidity!.trim())}
            />
            <Divider />
            <Button
              theme="SECONDARY"
              onClick={() => {
                solidityRef.current?.set(example.solidity!);
                setEdited(false);
              }}
            >
              RESET
            </Button>
          </Card>
        ) : null}

        <Card title="SCRIPT">
          <CodeEditor key={`${example.id}-js`} ref={scriptRef} doc={example.scripts[0]!.source} language="javascript" />
          <Divider />
          <Button
            theme="SECONDARY"
            onClick={() => {
              const pristine = example.scripts.find((s) => s.id === active)!.source;
              scripts.current.set(active, pristine);
              scriptRef.current?.set(pristine);
            }}
          >
            RESET
          </Button>
        </Card>
      </div>

      <Card title="WIDE EVENT">
        {outcome ? (
          <>
            <div className="summary">
              {Object.entries(outcome.summary).map(([key, value]) => (
                <Badge key={key}>
                  {format(value)} {key.toUpperCase()}
                </Badge>
              ))}
              <Badge>{outcome.requests} REQUESTS</Badge>
              <Badge>{outcome.elapsedMs.toFixed(0)} MS</Badge>
              {outcome.compiledInBrowser ? <Badge>BROWSER-COMPILED LENS</Badge> : null}
            </div>
            <Divider type="DOUBLE" />
            {event ? (
              <Table>
                {facetRows(event.fields).map((row) => (
                  <TableRow key={row.name}>
                    <TableColumn className={row.highlight ? "facet highlight" : "facet"}>{row.name}</TableColumn>
                    <TableColumn className="value">{format(row.value)}</TableColumn>
                  </TableRow>
                ))}
              </Table>
            ) : (
              "No wide event captured."
            )}
          </>
        ) : (
          "Run it to see the facets."
        )}
      </Card>
    </>
  );
}
