import { marked } from "marked";

import readme from "../../../README.md?raw";

const REPO = "https://github.com/morpho-org/viem-dlc/blob/main";

/** Relative links resolve against the repo on GitHub, not against this site. */
const rewritten = readme.replace(/\]\(\.\/([^)]+)\)/g, `](${REPO}/$1)`);

const html = marked.parse(rewritten, { async: false });

export function About() {
  // The content is this repo's own README, compiled at build time — there is no untrusted input.
  // biome-ignore lint/security/noDangerouslySetInnerHtml: trusted, build-time-inlined markdown
  return <article className="prose" dangerouslySetInnerHTML={{ __html: html }} />;
}
