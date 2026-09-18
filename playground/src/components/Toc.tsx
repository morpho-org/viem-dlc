import * as React from "react";

import type { OutlineEntry } from "../tutorials/outline.js";

/** How far below the viewport top a heading counts as the one you're reading. */
const ACTIVE_OFFSET = 140;

/**
 * Page outline, rendered beside the article. The entries are buttons rather than anchors because
 * the site routes on `location.hash` — an `href="#heading"` would navigate, not scroll.
 */
export function Toc({ entries }: { entries: OutlineEntry[] }) {
  const [active, setActive] = React.useState(entries[0]?.id ?? "");

  React.useEffect(() => {
    let frame = 0;
    const measure = () => {
      frame = 0;
      let current = entries[0]?.id ?? "";
      for (const entry of entries) {
        const top = document.getElementById(entry.id)?.getBoundingClientRect().top;
        if (top !== undefined && top <= ACTIVE_OFFSET) current = entry.id;
      }
      setActive(current);
    };
    const onScroll = () => {
      if (!frame) frame = requestAnimationFrame(measure);
    };

    measure();
    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onScroll);
    return () => {
      if (frame) cancelAnimationFrame(frame);
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
    };
  }, [entries]);

  if (entries.length < 2) return null;

  return (
    <nav className="toc" aria-label="On this page">
      <div className="panel-title toc-title">on this page</div>
      {entries.map((entry) => (
        <button
          key={entry.id}
          type="button"
          className={`toc-item toc-depth-${entry.depth}${entry.id === active ? " toc-item-active" : ""}`}
          aria-current={entry.id === active ? "true" : undefined}
          onClick={() => document.getElementById(entry.id)?.scrollIntoView({ behavior: "smooth" })}
        >
          {entry.text}
        </button>
      ))}
    </nav>
  );
}
