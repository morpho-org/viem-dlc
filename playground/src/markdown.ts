import { marked } from "marked";

/**
 * Heading slug. Shared by the renderer below and by {@link outline}, which reads the same markdown
 * without rendering it — the two have to agree for a table of contents entry to find its heading.
 */
export const slugify = (text: string) =>
  text
    .toLowerCase()
    .replace(/[`*_]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

marked.use({
  renderer: {
    heading(token) {
      const content = this.parser.parseInline(token.tokens);
      return `<h${token.depth} id="${slugify(token.text)}">${content}</h${token.depth}>\n`;
    },
  },
});

export const renderMarkdown = (markdown: string) => marked.parse(markdown, { async: false });
