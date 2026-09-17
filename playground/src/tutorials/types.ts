import type { InlineContract } from "soltag";
import type { Chain, Transport } from "viem";

export type Settings = Record<string, string>;

/**
 * What a step's exported function receives.
 *
 * `transport` is a plain `http` already rate-limited and counting requests — scripts compose the
 * transport under test over it, so the composition stays visible and editable rather than hidden in
 * the harness, which is the thing these tutorials exist to show.
 */
export type TabContext = {
  transport: Transport;
  chain: Chain;
  settings: Settings;
  log: (message: string) => void;
  /** Present only where the step declares Solidity, keyed by the contract's name. */
  lens?: InlineContract;
  /** Whatever the tutorial's `prepare` returned. */
  [extra: string]: unknown;
};

/** Badge values shown above the wide event. Keep them short — they render as chips. */
export type TabResult = { summary: Record<string, string | number> };

/**
 * A step's exported function. `Extras` names whatever that tutorial's `prepare` adds and which
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

export type Script = { id: string; title: string; source: string };

/**
 * One runnable unit: its own editors, its own Run button, its own result. A section without a step
 * is prose alone.
 */
export type Step = {
  id: string;
  /** Beyond the tutorial's page-scoped controls. */
  controls?: Control[];
  scripts: Script[];
  /** Editable lens source. `name` selects which contract `compileLens` wraps after an edit. */
  solidity?: { name: string; source: string };
};

/** Markdown, rendered above the step it introduces. */
export type Section = { heading: string; prose: string; step?: Step };

export type Tutorial = {
  /** Route segment, for example `eth-call` for `#/eth-call`. */
  id: string;
  title: string;
  blurb: string;
  /** Shown once at the top, beside the RPC URL. */
  controls?: Control[];
  sections: Section[];
  /** Extra module exports the scripts may import, merged over the shared base in `tab.ts`. */
  modules?: Record<string, Record<string, unknown>>;
  /**
   * Runs once per page rather than once per step, so every step sees the same corpus. Memoized
   * against the tutorial, the endpoint, and {@link Tutorial.prepareKeys}; anything returned is
   * merged into each step's context.
   */
  prepare?: (context: TabContext) => Promise<Record<string, unknown>>;
  /**
   * Which control ids `prepare` reads. Changing one of these re-runs it; every other control is
   * left out of the key on purpose, so a control that only narrows what a step displays keeps the
   * memoized corpus rather than refetching it.
   */
  prepareKeys?: readonly string[];
};
