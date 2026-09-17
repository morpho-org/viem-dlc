import readme from "../../../README.md?raw";
import { Toc } from "../components/Toc.js";
import { renderMarkdown } from "../markdown.js";
import { outlineOf } from "../tutorials/outline.js";

const REPO = "https://github.com/morpho-org/viem-dlc/blob/main";

/** Relative links resolve against the repo on GitHub, not against this site. */
const rewritten = readme.replace(/\]\(\.\/([^)]+)\)/g, `](${REPO}/$1)`);

const html = renderMarkdown(rewritten);
const headings = outlineOf(rewritten);

export function About() {
  return (
    <div className="with-toc with-toc-prose">
      {/* The content is this repo's own README, compiled at build time — there is no untrusted input. */}
      {/* biome-ignore lint/security/noDangerouslySetInnerHtml: trusted, build-time-inlined markdown */}
      <article className="prose" dangerouslySetInnerHTML={{ __html: html }} />
      <Toc entries={headings} />
    </div>
  );
}
