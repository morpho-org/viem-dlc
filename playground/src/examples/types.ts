import type { InlineContract } from "soltag";
import type { Chain, Transport } from "viem";

export type Settings = Record<string, string>;

/**
 * What a tab's exported function receives.
 *
 * `transport` is a plain `http` already rate-limited and counting requests — scripts compose the
 * transport under test over it, so the composition stays visible and editable rather than hidden in
 * the harness, which is the thing these examples exist to show.
 */
export type TabContext = {
  transport: Transport;
  chain: Chain;
  settings: Settings;
  log: (message: string) => void;
  /** Present only where the example declares Solidity. */
  lens?: InlineContract<"MorphoPositionsLens">;
  /** Whatever the example's `prepare` returned. */
  [extra: string]: unknown;
};

/** Badge values shown above the wide event. Keep them short — they render as chips. */
export type TabResult = { summary: Record<string, string | number> };

/**
 * A tab's exported function. `Extras` names whatever that example's `prepare` adds and which
 * `settings` keys its controls declare, so each script is checked against its own context rather
 * than the loosest one.
 */
export type Tab<Extras = Record<never, never>> = (context: TabContext & Extras) => Promise<TabResult>;

export type Control = {
  id: string;
  label: string;
  value: string;
  /** `number` renders a numeric input; anything else renders text. */
  type?: "number" | "text";
};

export type Example = {
  /** Route segment, e.g. `deployless` for `#/deployless`. */
  id: string;
  title: string;
  blurb: string;
  /** Beyond the shared RPC URL input. */
  controls: Control[];
  /** Editable lens source; omit for examples with no contract. */
  solidity?: string;
  scripts: { id: string; title: string; source: string }[];
  /** Extra module exports the scripts may import, merged over the shared base in `tab.ts`. */
  modules?: Record<string, Record<string, unknown>>;
  /** Runs before the script; anything returned is merged into the context. */
  prepare?: (context: TabContext) => Promise<Record<string, unknown>>;
};
