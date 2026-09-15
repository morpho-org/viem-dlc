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

import initcodeScript from "../tabs/initcode.js?raw";
import overrideScript from "../tabs/override.js?raw";
import pristineSolidity from "../tabs/positions.sol?raw";

import { CodeEditor, type EditorHandle } from "./editor.js";
import { type RunResult, run } from "./run.js";

/** Facets that answer "did this change anything" — shown first, and never hidden. */
const HIGHLIGHTS = [
  "elements_requested",
  "elements_fetched",
  "elements_missing",
  "nominal_batches",
  "chunks_initcode",
  "chunks_override",
  "override_fallbacks_unsupported",
  "batch_bytes.max",
  "pages_continued",
  "continuations",
  "gas_limit_observed",
  "fixed_gas",
  "item_gas_avg",
];

const TABS = [
  { id: "initcode", title: "INITCODE", script: initcodeScript },
  { id: "override", title: "OVERRIDE", script: overrideScript },
];

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

const format = (value: unknown) => (typeof value === "number" ? value.toLocaleString("en-US") : String(value));

export function App() {
  const [rpcUrl, setRpcUrl] = React.useState("https://mainnet.base.org");
  const [elements, setElements] = React.useState("2000");
  const [gasLimit, setGasLimit] = React.useState("600000000");
  const [active, setActive] = React.useState(TABS[0]!.id);
  const [status, setStatus] = React.useState("");
  const [failed, setFailed] = React.useState(false);
  const [busy, setBusy] = React.useState(false);
  const [result, setResult] = React.useState<RunResult | undefined>();
  const [edited, setEdited] = React.useState(false);

  const solidityRef = React.useRef<EditorHandle>(null);
  const scriptRef = React.useRef<EditorHandle>(null);
  const scripts = React.useRef(new Map(TABS.map((tab) => [tab.id, tab.script])));

  const selectTab = (id: string) => {
    if (id === active) return;
    scripts.current.set(active, scriptRef.current?.value() ?? "");
    setActive(id);
    scriptRef.current?.set(scripts.current.get(id)!);
  };

  const onRun = async () => {
    setBusy(true);
    setFailed(false);
    setResult(undefined);

    try {
      const outcome = await run(
        {
          rpcUrl,
          elements: Number(elements),
          gasLimit: Number(gasLimit),
          script: scriptRef.current?.value() ?? "",
          solidity: solidityRef.current?.value() ?? pristineSolidity,
          pristineSolidity,
        },
        setStatus,
      );
      setResult(outcome);
      setStatus(`done — ${outcome.distinct.toLocaleString("en-US")} distinct pairs discovered`);
    } catch (error) {
      setFailed(true);
      setStatus(
        error instanceof Error && /fetch|CORS|Failed to fetch/i.test(error.message)
          ? `${error.message} — this endpoint may not allow browser requests (no CORS headers).`
          : `${error}`,
      );
    } finally {
      setBusy(false);
    }
  };

  const event = result?.events.find((e) => Object.keys(e.fields).some((k) => k.includes("elements_requested")));

  return (
    <div className="page">
      <Card title="VIEM-DLC · DEPLOYLESS LENS PLAYGROUND">
        Reads Morpho positions on Base through a lens that is never deployed, using the <strong>deployless</strong>{" "}
        transport straight from <strong>src/</strong>. Both panes are editable and both run: the Solidity is recompiled
        in your browser when you change it, and the JavaScript is the program that executes. Switch tabs to compare
        deliveries — <strong>override</strong> lifts the 49 152-byte initcode cap, so the same elements collapse into
        fewer, larger chunks.
      </Card>

      <Card title="SETTINGS">
        <Input label="RPC URL (BASE)" name="rpc" value={rpcUrl} onChange={(e) => setRpcUrl(e.target.value)} />
        <Input label="ELEMENTS" name="elements" value={elements} onChange={(e) => setElements(e.target.value)} />
        <Input label="TRANSPORT GASLIMIT" name="gas" value={gasLimit} onChange={(e) => setGasLimit(e.target.value)} />
        <Divider type="DOUBLE" />
        <div className="actions">
          <Button onClick={onRun} isDisabled={busy}>
            {busy ? "RUNNING" : "RUN"}
          </Button>
          {busy ? <BlockLoader mode={9} /> : null}
          <span className={failed ? "status failed" : "status"}>{status}</span>
        </div>
      </Card>

      <div className="tabs">
        {TABS.map((tab) => (
          <Button key={tab.id} theme={tab.id === active ? "PRIMARY" : "SECONDARY"} onClick={() => selectTab(tab.id)}>
            {tab.title}
          </Button>
        ))}
      </div>

      <div className="split">
        <Card title={edited ? "LENS · EDITED, COMPILES IN-BROWSER" : "LENS · PREBUILT"}>
          <CodeEditor
            ref={solidityRef}
            doc={pristineSolidity}
            language="solidity"
            onChange={(doc) => setEdited(doc.trim() !== pristineSolidity.trim())}
          />
          <Divider />
          <Button
            theme="SECONDARY"
            onClick={() => {
              solidityRef.current?.set(pristineSolidity);
              setEdited(false);
            }}
          >
            RESET
          </Button>
        </Card>

        <Card title={`SCRIPT · ${active.toUpperCase()}`}>
          <CodeEditor ref={scriptRef} doc={TABS[0]!.script} language="javascript" />
          <Divider />
          <Button
            theme="SECONDARY"
            onClick={() => {
              const pristine = TABS.find((tab) => tab.id === active)!.script;
              scripts.current.set(active, pristine);
              scriptRef.current?.set(pristine);
            }}
          >
            RESET
          </Button>
        </Card>
      </div>

      <Card title="WIDE EVENT">
        {result ? (
          <>
            <div className="summary">
              <Badge>{result.results.toLocaleString("en-US")} RESULTS</Badge>
              <Badge>{result.skipped.toLocaleString("en-US")} SKIPPED</Badge>
              <Badge>{result.requests} ETH_CALL</Badge>
              <Badge>{result.elapsedMs.toFixed(0)} MS</Badge>
              <Badge>{result.compiled ? "BROWSER-COMPILED LENS" : "PREBUILT LENS"}</Badge>
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
    </div>
  );
}
