/**
 * The position map: for every node the printers print, the range of Solidity text and the TypeScript
 * it came from. solc reports an error with a range of the Solidity, and the innermost recorded node
 * that covers the range gives the TypeScript position.
 */
import { positionOf } from "./errors.js";
import type { Origin, PositionMap, PositionMapEntry, PrintedLens, SourceWriter } from "./types.js";

const INDENT = "    ";

/** The {@link SourceWriter} both printers write to. */
export class Writer implements SourceWriter {
  #text = "";
  #depth = 0;
  readonly #entries: PositionMapEntry[] = [];

  write(text: string): void {
    this.#text += text;
  }

  line(): void {
    this.#text += `\n${INDENT.repeat(this.#depth)}`;
  }

  indented(body: () => void): void {
    this.#depth++;
    try {
      body();
    } finally {
      this.#depth--;
    }
  }

  node<T>(origin: Origin, body: () => T): T {
    const start = this.#text.length;
    const result = body();
    this.#entries.push({ start, end: this.#text.length, ...positionOf(origin.module, origin.node.start) });
    return result;
  }

  finish(): PrintedLens {
    return { solidity: this.#text, map: { entries: this.#entries } };
  }
}

/**
 * The innermost printed node covering `[start, end)` of the Solidity text, in UTF-16 offsets. When no
 * node covers the whole range, the innermost one covering its start. Undefined when nothing does.
 */
export function lookupPosition(map: PositionMap, start: number, end: number): PositionMapEntry | undefined {
  const innermost = (covers: (entry: PositionMapEntry) => boolean): PositionMapEntry | undefined => {
    let best: PositionMapEntry | undefined;
    // An inner node is recorded before the node around it, so the first of two equal ranges is the inner one.
    for (const entry of map.entries)
      if (covers(entry) && (!best || entry.end - entry.start < best.end - best.start)) best = entry;
    return best;
  };
  return (
    innermost((entry) => entry.start <= start && end <= entry.end) ??
    innermost((entry) => entry.start <= start && start < entry.end)
  );
}
