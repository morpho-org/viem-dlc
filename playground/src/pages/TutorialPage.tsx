import { Card, Flex, Heading, Separator, Text } from "@radix-ui/themes";
import * as React from "react";

import { EventStream, useEventStream } from "../components/EventStream.js";
import { StepCard } from "../components/StepCard.js";
import { Toc } from "../components/Toc.js";
import type { WideEvent } from "../logger.js";
import { renderMarkdown, slugify } from "../markdown.js";
import { outline } from "../tutorials/outline.js";
import type { Settings, Tutorial } from "../tutorials/types.js";

import { Field } from "./Field.js";

function Prose({ markdown }: { markdown: string }) {
  const html = React.useMemo(() => renderMarkdown(markdown), [markdown]);
  // Tutorial prose is this repo's own content, inlined at build time — there is no untrusted input.
  // biome-ignore lint/security/noDangerouslySetInnerHtml: trusted, build-time-inlined markdown
  return <article className="prose" dangerouslySetInnerHTML={{ __html: html }} />;
}

export function TutorialPage({
  tutorial,
  rpcUrl,
  onRpcUrl,
}: {
  tutorial: Tutorial;
  rpcUrl: string;
  onRpcUrl: (value: string) => void;
}) {
  const [values, setValues] = React.useState<Settings>(() =>
    Object.fromEntries((tutorial.controls ?? []).map((control) => [control.id, control.value])),
  );
  const { entries, push, clear } = useEventStream();
  const headings = React.useMemo(() => outline(tutorial), [tutorial]);

  // Switching tutorials resets the shared controls and the feed, so one page's run never reads as
  // part of the next one.
  React.useEffect(() => {
    setValues(Object.fromEntries((tutorial.controls ?? []).map((control) => [control.id, control.value])));
    clear();
  }, [tutorial, clear]);

  const onLog = React.useCallback((step: string, text: string) => push({ kind: "log", step, text }), [push]);
  const onEvent = React.useCallback((step: string, event: WideEvent) => push({ kind: "event", step, event }), [push]);

  const settings = { ...values, rpcUrl };

  return (
    <>
      <div className="with-toc">
        <Flex direction="column" gap="4" className="tutorial">
          <header>
            <Heading as="h1" size="5" mb="1">
              {tutorial.title}
            </Heading>
            <Text as="p" color="gray" className="blurb">
              {tutorial.blurb}
            </Text>
          </header>

          <Card size="2">
            <Flex direction="column" gap="3">
              <Heading as="h2" size="1" color="gray" className="panel-title">
                settings
              </Heading>
              <div className="fields">
                <Field label="RPC URL (Base)" value={rpcUrl} wide onChange={onRpcUrl} />
                {(tutorial.controls ?? []).map((control) => (
                  <Field
                    key={control.id}
                    label={control.label}
                    value={values[control.id] ?? ""}
                    type={control.type}
                    onChange={(next) => setValues((prev) => ({ ...prev, [control.id]: next }))}
                  />
                ))}
              </div>
            </Flex>
          </Card>

          {tutorial.sections.map((section, index) => (
            <Flex key={section.heading || section.step?.id} direction="column" gap="3" asChild>
              <section>
                {index > 0 && section.heading ? <Separator size="4" /> : null}
                {section.heading ? (
                  <Heading as="h2" size="4" id={slugify(section.heading)}>
                    {section.heading}
                  </Heading>
                ) : null}
                {section.prose ? <Prose markdown={section.prose} /> : null}
                {section.step ? (
                  <StepCard
                    key={`${tutorial.id}-${section.step.id}`}
                    tutorial={tutorial}
                    step={section.step}
                    pageSettings={settings}
                    onLog={onLog}
                    onEvent={onEvent}
                  />
                ) : null}
              </section>
            </Flex>
          ))}
        </Flex>

        <Toc key={tutorial.id} entries={headings} />
      </div>

      <EventStream entries={entries} onClear={clear} />
    </>
  );
}
