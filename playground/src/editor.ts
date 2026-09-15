import { javascript } from "@codemirror/lang-javascript";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { EditorState } from "@codemirror/state";
import { EditorView } from "@codemirror/view";
import { tags } from "@lezer/highlight";
import { solidity } from "@replit/codemirror-lang-solidity";
import { basicSetup } from "codemirror";

/**
 * Colours name CSS variables rather than literals so the page's light/dark tokens stay the single
 * place either palette is defined.
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
  "&": {
    fontSize: "12px",
    backgroundColor: "var(--bg)",
    color: "var(--ink)",
    border: "1px solid var(--line)",
    borderRadius: "6px",
  },
  "&.cm-focused": { outline: "none", borderColor: "var(--accent)" },
  ".cm-scroller": {
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
    lineHeight: "1.6",
    maxHeight: "460px",
  },
  ".cm-content": { caretColor: "var(--ink)" },
  ".cm-gutters": { backgroundColor: "transparent", color: "var(--muted)", border: "none" },
  ".cm-activeLine, .cm-activeLineGutter": { backgroundColor: "transparent" },
  ".cm-cursor, .cm-dropCursor": { borderLeftColor: "var(--ink)" },
  "&.cm-focused .cm-selectionBackground, .cm-selectionBackground, .cm-content ::selection": {
    backgroundColor: "var(--selection)",
  },
});

export type Editor = { value: () => string; set: (doc: string) => void };

export function createEditor(parent: HTMLElement, doc: string, language: "solidity" | "javascript"): Editor {
  const view = new EditorView({
    parent,
    state: EditorState.create({
      doc,
      extensions: [basicSetup, language === "solidity" ? solidity : javascript(), syntaxHighlighting(highlight), theme],
    }),
  });

  return {
    value: () => view.state.doc.toString(),
    set: (next) => view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: next } }),
  };
}
