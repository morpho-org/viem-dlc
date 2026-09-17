import { slugify } from "../markdown.js";

import type { Tutorial } from "./types.js";

export type OutlineEntry = { id: string; text: string; depth: 1 | 2 };

const FENCE = /^\s*(```|~~~)/;
const HEADING = /^(#{2,3})\s+(.+?)\s*#*\s*$/;

/** `##`/`###` headings in a markdown document, ignoring anything inside a fenced code block. */
function headings(markdown: string): { text: string; level: 2 | 3 }[] {
  const found: { text: string; level: 2 | 3 }[] = [];
  let fenced = false;
  for (const line of markdown.split("\n")) {
    if (FENCE.test(line)) fenced = !fenced;
    else if (!fenced) {
      const match = HEADING.exec(line);
      if (match) found.push({ text: match[2]!, level: match[1]!.length as 2 | 3 });
    }
  }
  return found;
}

const entry = (markdown: string, depth: 1 | 2): OutlineEntry => ({
  id: slugify(markdown),
  text: markdown.replace(/`/g, ""),
  depth,
});

/** Section headings, each followed by the headings its prose declares. */
export function outline(tutorial: Tutorial): OutlineEntry[] {
  return tutorial.sections.flatMap((section) => [
    ...(section.heading ? [entry(section.heading, 1)] : []),
    ...headings(section.prose).map((found) => entry(found.text, 2)),
  ]);
}

/** The same, for a standalone document: its `##`s are the top level and its `###`s nest under them. */
export function outlineOf(markdown: string): OutlineEntry[] {
  return headings(markdown).map((found) => entry(found.text, found.level === 2 ? 1 : 2));
}
