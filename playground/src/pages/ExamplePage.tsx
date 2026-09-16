import { Badge, Button, Card, Flex, Heading, Separator, Spinner, Table, Tabs, Text, TextField } from "@radix-ui/themes";
import * as React from "react";

import { CodeEditor, type EditorHandle } from "../editor.js";
import type { Control, Example, Settings } from "../examples/types.js";
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

function Panel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <Card size="2">
      <Flex direction="column" gap="3">
        <Heading as="h2" size="1" color="gray" className="panel-title">
          {title}
        </Heading>
        {children}
      </Flex>
    </Card>
  );
}

function Field({
  label,
  value,
  type,
  onChange,
}: {
  label: string;
  value: string;
  type?: Control["type"];
  onChange: (value: string) => void;
}) {
  const id = React.useId();

  return (
    <div>
      <Text as="label" htmlFor={id} size="1" color="gray" mb="1" className="field-label">
        {label}
      </Text>
      <TextField.Root
        id={id}
        value={value}
        type={type === "number" ? "number" : "text"}
        spellCheck={false}
        onChange={(event) => onChange(event.target.value)}
      />
    </div>
  );
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
    <Flex direction="column" gap="4">
      <header>
        <Heading as="h1" size="4" mb="1">
          {example.title}
        </Heading>
        <Text as="p" color="gray">
          {example.blurb}
        </Text>
      </header>

      <Panel title="settings">
        <Field label="RPC URL (Base)" value={rpcUrl} onChange={onRpcUrl} />
        {example.controls.map((control) => (
          <Field
            key={control.id}
            label={control.label}
            value={values[control.id] ?? ""}
            type={control.type}
            onChange={(next) => setValues((prev) => ({ ...prev, [control.id]: next }))}
          />
        ))}
        <Separator size="4" />
        <Flex align="center" gap="3" wrap="wrap">
          <Button onClick={onRun} disabled={busy} variant="solid" highContrast>
            <Spinner loading={busy} />
            {busy ? "Running" : "Run"}
          </Button>
          <Text size="1" color={failed ? "red" : "gray"}>
            {status}
          </Text>
        </Flex>
      </Panel>

      {example.scripts.length > 1 ? (
        <Tabs.Root value={active} onValueChange={selectTab}>
          <Tabs.List>
            {example.scripts.map((script) => (
              <Tabs.Trigger key={script.id} value={script.id}>
                {script.title}
              </Tabs.Trigger>
            ))}
          </Tabs.List>
        </Tabs.Root>
      ) : null}

      <div className={example.solidity ? "split" : ""}>
        {example.solidity ? (
          <Panel title={edited ? "lens · edited, compiles in-browser" : "lens · prebuilt"}>
            <CodeEditor
              key={`${example.id}-sol`}
              ref={solidityRef}
              doc={example.solidity}
              language="solidity"
              onChange={(doc) => setEdited(doc.trim() !== example.solidity!.trim())}
            />
            <Flex>
              <Button
                variant="soft"
                color="gray"
                onClick={() => {
                  solidityRef.current?.set(example.solidity!);
                  setEdited(false);
                }}
              >
                Reset
              </Button>
            </Flex>
          </Panel>
        ) : null}

        <Panel title="script">
          <CodeEditor key={`${example.id}-js`} ref={scriptRef} doc={example.scripts[0]!.source} language="javascript" />
          <Flex>
            <Button
              variant="soft"
              color="gray"
              onClick={() => {
                const pristine = example.scripts.find((s) => s.id === active)!.source;
                scripts.current.set(active, pristine);
                scriptRef.current?.set(pristine);
              }}
            >
              Reset
            </Button>
          </Flex>
        </Panel>
      </div>

      <Panel title="wide event">
        {outcome ? (
          <>
            <Flex gap="2" wrap="wrap">
              {Object.entries(outcome.summary).map(([key, value]) => (
                <Badge key={key} color="gray" variant="soft" size="2">
                  {format(value)} {key}
                </Badge>
              ))}
              <Badge color="gray" variant="soft" size="2">
                {outcome.requests} requests
              </Badge>
              <Badge color="gray" variant="soft" size="2">
                {outcome.elapsedMs.toFixed(0)} ms
              </Badge>
              {outcome.compiledInBrowser ? (
                <Badge color="jade" variant="soft" size="2">
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
        ) : (
          <Text color="gray">Run it to see the facets.</Text>
        )}
      </Panel>
    </Flex>
  );
}
