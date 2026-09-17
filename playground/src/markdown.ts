import { marked } from "marked";

/**
 * Heading slug, matching GitHub's: punctuation and symbols drop, `-` and `_` survive, spaces become
 * hyphens. Shared by the renderer below and by {@link outline}, which reads the same markdown
 * without rendering it — the two have to agree for a table of contents entry to find its heading.
 *
 * Matching GitHub matters because the About page renders `README.md`, whose intra-document links
 * are written against the anchors GitHub generates.
 */
export const slugify = (text: string) =>
  text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\p{M}\- _]/gu, "")
    .trim()
    .replace(/ /g, "-");

marked.use({
  renderer: {
    heading(token) {
      const content = this.parser.parseInline(token.tokens);
      return `<h${token.depth} id="${slugify(token.text)}">${content}</h${token.depth}>\n`;
    },
  },
});

export const renderMarkdown = (markdown: string) => marked.parse(markdown, { async: false });
