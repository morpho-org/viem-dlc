import { javascript } from "@codemirror/lang-javascript";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { EditorState } from "@codemirror/state";
import { EditorView } from "@codemirror/view";
import { tags } from "@lezer/highlight";
import { solidity } from "@replit/codemirror-lang-solidity";
import { basicSetup } from "codemirror";
import * as React from "react";

/**
 * Colours name CSS variables rather than literals, so the palette resolves to SRCL's `--ansi-*`
 * primitives and follows its theme with the rest of the page.
 */
const highlight = HighlightStyle.define([
  { tag: [tags.keyword, tags.moduleKeyword, tags.controlKeyword], color: "var(--syn-keyword)" },
  { tag: [tags.typeName, tags.className, tags.standard(tags.typeName)], color: "var(--syn-type)" },
  { tag: [tags.function(tags.variableName), tags.function(tags.propertyName)], color: "var(--syn-function)" },
  { tag: [tags.string, tags.special(tags.string)], color: "var(--syn-string)" },
  { tag: [tags.number, tags.bool, tags.literal], color: "var(--syn-number)" },
  { tag: [tags.comment, tags.blockComment, tags.lineComment], color: "var(--syn-comment)", fontStyle: "italic" },
  { tag: [tags.propertyName, tags.attributeName], color: "var(--syn-property)" },
  { tag: [tags.operator, tags.punctuation, tags.separator], color: "var(--syn-punctuation)" },
  { tag: [tags.definition(tags.variableName), tags.definition(tags.propertyName)], color: "var(--syn-definition)" },
]);

const theme = EditorView.theme({
  "&": { fontSize: "12px", backgroundColor: "transparent", color: "var(--theme-text)" },
  "&.cm-focused": { outline: "none" },
  ".cm-scroller": { fontFamily: "var(--font-family-mono)", lineHeight: "1.6", maxHeight: "440px" },
  ".cm-content": { caretColor: "var(--theme-text)", minHeight: "300px" },
  ".cm-gutters": { backgroundColor: "transparent", color: "var(--theme-border-subdued)", border: "none" },
  ".cm-activeLine, .cm-activeLineGutter": { backgroundColor: "transparent" },
  ".cm-cursor, .cm-dropCursor": { borderLeftColor: "var(--theme-text)" },
  "&.cm-focused .cm-selectionBackground, .cm-selectionBackground, .cm-content ::selection": {
    backgroundColor: "var(--theme-focused-foreground-subdued)",
  },
});

export type EditorHandle = { value: () => string; set: (doc: string) => void };

type Props = { doc: string; language: "solidity" | "javascript"; onChange?: (doc: string) => void };

/**
 * Mounts once and is thereafter uncontrolled — re-rendering never replaces the document, so a
 * parent state change cannot discard what someone is typing. Use the handle's `set` to overwrite.
 */
export const CodeEditor = React.forwardRef<EditorHandle, Props>(({ doc, language, onChange }, ref) => {
  const host = React.useRef<HTMLDivElement>(null);
  const view = React.useRef<EditorView>(null);
  const notify = React.useRef(onChange);
  notify.current = onChange;

  // biome-ignore lint/correctness/useExhaustiveDependencies: `doc` seeds the document once; depending on it would rebuild the editor on every keystroke.
  React.useEffect(() => {
    const instance = new EditorView({
      parent: host.current!,
      state: EditorState.create({
        doc,
        extensions: [
          basicSetup,
          language === "solidity" ? solidity : javascript(),
          syntaxHighlighting(highlight),
          theme,
          EditorView.updateListener.of((update) => {
            if (update.docChanged) notify.current?.(update.state.doc.toString());
          }),
        ],
      }),
    });

    view.current = instance;
    return () => instance.destroy();
  }, [language]);

  React.useImperativeHandle(ref, () => ({
    value: () => view.current?.state.doc.toString() ?? doc,
    set: (next) => view.current?.dispatch({ changes: { from: 0, to: view.current.state.doc.length, insert: next } }),
  }));

  return <div ref={host} />;
});

CodeEditor.displayName = "CodeEditor";
