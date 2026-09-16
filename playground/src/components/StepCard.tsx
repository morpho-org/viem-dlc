import { Button, Card, Flex, Heading, Separator, Spinner, Tabs, Text } from "@radix-ui/themes";
import * as React from "react";

import { CodeEditor, type EditorHandle } from "../editor.js";
import type { WideEvent } from "../logger.js";
import { Field } from "../pages/Field.js";
import { type RunOutcome, run } from "../runtime.js";
import type { Settings, Step, Tutorial } from "../tutorials/types.js";

import { WideEvent as WideEventTable } from "./WideEvent.js";

/**
 * Endpoints often carry credentials in the query string, and viem puts the full request URL in its
 * error text — which would otherwise be rendered here and end up in any screenshot.
 */
const redact = (message: string) => message.replace(/(https?:\/\/[^\s?]+)\?\S*/g, "$1?<redacted>");

/**
 * One runnable unit, owning its own editors and result so two steps can hold two outcomes at once.
 * `pageSettings` arrive from the tutorial's shared controls; the step's own controls are merged over
 * them, so a step can narrow a page-wide value.
 */
export function StepCard({
  tutorial,
  step,
  pageSettings,
  onLog,
  onEvent,
}: {
  tutorial: Tutorial;
  step: Step;
  pageSettings: Settings;
  onLog: (step: string, message: string) => void;
  onEvent: (step: string, event: WideEvent) => void;
}) {
  const [values, setValues] = React.useState<Settings>(() =>
    Object.fromEntries((step.controls ?? []).map((control) => [control.id, control.value])),
  );
  const [active, setActive] = React.useState(step.scripts[0]!.id);
  const [status, setStatus] = React.useState("");
  const [failed, setFailed] = React.useState(false);
  const [busy, setBusy] = React.useState(false);
  const [outcome, setOutcome] = React.useState<RunOutcome | undefined>();
  const [edited, setEdited] = React.useState(false);

  const solidityRef = React.useRef<EditorHandle>(null);
  const scriptRef = React.useRef<EditorHandle>(null);
  const scripts = React.useRef(new Map(step.scripts.map((script) => [script.id, script.source])));

  const selectTab = (id: string) => {
    if (id === active) return;
    scripts.current.set(active, scriptRef.current?.value() ?? "");
    setActive(id);
    scriptRef.current?.set(scripts.current.get(id)!);
    setOutcome(undefined);
    setStatus("");
  };

  const onRun = async () => {
    setBusy(true);
    setFailed(false);
    setOutcome(undefined);

    const log = (message: string) => {
      setStatus(message);
      onLog(step.id, message);
    };

    try {
      const result = await run(
        {
          tutorial,
          step,
          settings: { ...pageSettings, ...values },
          script: scriptRef.current?.value() ?? "",
          solidity: step.solidity ? (solidityRef.current?.value() ?? step.solidity.source) : undefined,
        },
        log,
        (event) => onEvent(step.id, event),
      );
      setOutcome(result);
      setStatus("done");
    } catch (error) {
      setFailed(true);
      const message = redact(
        // Only the browser's own network failure means CORS; "gap fetch failed" is not that.
        error instanceof Error && /Failed to fetch|NetworkError|Load failed/i.test(error.message)
          ? `${error.message} — this endpoint may not allow browser requests (no CORS headers).`
          : `${error}`,
      );
      setStatus(message);
      onLog(step.id, message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card size="2">
      <Flex direction="column" gap="3">
        {step.scripts.length > 1 ? (
          <Tabs.Root value={active} onValueChange={selectTab}>
            <Tabs.List>
              {step.scripts.map((script) => (
                <Tabs.Trigger key={script.id} value={script.id}>
                  {script.title}
                </Tabs.Trigger>
              ))}
            </Tabs.List>
          </Tabs.Root>
        ) : null}

        {(step.controls ?? []).map((control) => (
          <Field
            key={control.id}
            label={control.label}
            value={values[control.id] ?? ""}
            type={control.type}
            onChange={(next) => setValues((prev) => ({ ...prev, [control.id]: next }))}
          />
        ))}

        <div className={step.solidity ? "split" : ""}>
          {step.solidity ? (
            <Flex direction="column" gap="2">
              <Heading as="h3" size="1" color="gray" className="panel-title">
                {edited ? `${step.solidity.name} · edited, compiles in-browser` : step.solidity.name}
              </Heading>
              <CodeEditor
                key={`${step.id}-sol`}
                ref={solidityRef}
                doc={step.solidity.source}
                language="solidity"
                onChange={(doc) => setEdited(doc.trim() !== step.solidity!.source.trim())}
              />
              <Flex>
                <Button
                  variant="soft"
                  color="gray"
                  onClick={() => {
                    solidityRef.current?.set(step.solidity!.source);
                    setEdited(false);
                  }}
                >
                  Reset
                </Button>
              </Flex>
            </Flex>
          ) : null}

          <Flex direction="column" gap="2">
            <Heading as="h3" size="1" color="gray" className="panel-title">
              script
            </Heading>
            <CodeEditor key={`${step.id}-js`} ref={scriptRef} doc={step.scripts[0]!.source} language="javascript" />
            <Flex>
              <Button
                variant="soft"
                color="gray"
                onClick={() => {
                  const pristine = step.scripts.find((script) => script.id === active)!.source;
                  scripts.current.set(active, pristine);
                  scriptRef.current?.set(pristine);
                }}
              >
                Reset
              </Button>
            </Flex>
          </Flex>
        </div>

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

        {outcome ? <WideEventTable outcome={outcome} /> : null}
      </Flex>
    </Card>
  );
}
