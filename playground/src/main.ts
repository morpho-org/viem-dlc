import initcodeScript from "../tabs/initcode.js?raw";
import overrideScript from "../tabs/override.js?raw";
import pristineSolidity from "../tabs/positions.sol?raw";

import { run, type Settings } from "./run.js";

/** Facets that answer "did this change anything" — shown first, and never hidden. */
const HIGHLIGHTS = [
  "elements_requested",
  "elements_fetched",
  "elements_missing",
  "nominal_batches",
  "chunks_initcode",
  "chunks_override",
  "override_fallbacks_unsupported",
  "batch_bytes.max",
  "pages_continued",
  "continuations",
  "gas_limit_observed",
  "fixed_gas",
  "item_gas_avg",
];

const TABS = [
  { id: "initcode", title: "initcode", script: initcodeScript },
  { id: "override", title: "override", script: overrideScript },
];

const settings: Settings = { rpcUrl: "https://mainnet.base.org", gasLimit: 600_000_000, elements: 2_000 };

const scripts = new Map(TABS.map((tab) => [tab.id, tab.script]));
let solidity = pristineSolidity;
let active = TABS[0]!.id;

const app = document.querySelector<HTMLElement>("#app")!;

app.innerHTML = `
  <h1>viem-dlc — deployless lens playground</h1>
  <p class="lede">
    Reads Morpho positions on Base through a lens that is never deployed, using
    <code>deployless</code> straight from <code>src/</code>. Both panes are editable and both run:
    the Solidity is recompiled in your browser when you change it, and the JavaScript is the program
    that executes. Switch tabs to compare deliveries — <code>override</code> lifts the 49 152-byte
    initcode cap, so the same elements collapse into fewer, larger chunks.
  </p>

  <div class="panel">
    <div class="controls">
      <div><label for="rpc">RPC URL (Base)</label><input id="rpc" type="text" value="${settings.rpcUrl}" /></div>
      <div><label for="elements">Elements</label><input id="elements" type="number" min="1" max="20000" step="100" value="${settings.elements}" /></div>
      <div><label for="gas">Transport gasLimit</label><input id="gas" type="number" min="1000000" step="50000000" value="${settings.gasLimit}" /></div>
    </div>
    <button id="go">Run</button>
    <div class="status" id="status"></div>
  </div>

  <div class="tabs" id="tabs">
    ${TABS.map((tab) => `<button class="tab" data-tab="${tab.id}">${tab.title}</button>`).join("")}
  </div>

  <div class="split">
    <div class="panel">
      <h2>Lens <span class="hint" id="sol-state"></span> <button class="reset" id="reset-sol">reset</button></h2>
      <textarea id="solidity" spellcheck="false"></textarea>
    </div>
    <div class="panel">
      <h2>Script <button class="reset" id="reset-js">reset</button></h2>
      <textarea id="script" spellcheck="false"></textarea>
    </div>
  </div>

  <div class="panel"><h2>Wide event</h2><div id="out"><p class="lede" style="margin:0">Run it to see the facets.</p></div></div>
`;

const el = <T extends HTMLElement>(id: string) => document.querySelector<T>(`#${id}`)!;
const status = el("status");
const out = el("out");
const button = el<HTMLButtonElement>("go");
const solidityBox = el<HTMLTextAreaElement>("solidity");
const scriptBox = el<HTMLTextAreaElement>("script");
const solState = el("sol-state");

function paint() {
  solidityBox.value = solidity;
  scriptBox.value = scripts.get(active)!;
  solState.textContent = solidity.trim() === pristineSolidity.trim() ? "prebuilt" : "edited — compiles in-browser";
  for (const tab of document.querySelectorAll<HTMLElement>(".tab")) {
    tab.classList.toggle("active", tab.dataset.tab === active);
  }
}

paint();

el("tabs").addEventListener("click", (event) => {
  const id = (event.target as HTMLElement).dataset.tab;
  if (!id) return;
  scripts.set(active, scriptBox.value);
  active = id;
  paint();
});

solidityBox.addEventListener("input", () => {
  solidity = solidityBox.value;
  solState.textContent = solidity.trim() === pristineSolidity.trim() ? "prebuilt" : "edited — compiles in-browser";
});
scriptBox.addEventListener("input", () => scripts.set(active, scriptBox.value));

el("reset-sol").addEventListener("click", () => {
  solidity = pristineSolidity;
  paint();
});
el("reset-js").addEventListener("click", () => {
  scripts.set(active, TABS.find((tab) => tab.id === active)!.script);
  paint();
});

function render(fields: Record<string, unknown>) {
  const keyOf = (name: string) => Object.keys(fields).find((key) => key === name || key.endsWith(`.${name}`));
  const shown = new Set<string>();
  const row = (label: string, value: unknown, highlight: boolean) =>
    `<tr class="${highlight ? "highlight" : ""}"><td>${label}</td><td>${
      typeof value === "number" ? value.toLocaleString("en-US") : String(value)
    }</td></tr>`;

  const rows = HIGHLIGHTS.flatMap((name) => {
    const key = keyOf(name);
    if (key === undefined) return [];
    shown.add(key);
    return [row(name, fields[key], true)];
  });
  const rest = Object.keys(fields)
    .filter((key) => !shown.has(key))
    .sort()
    .map((key) => row(key, fields[key], false));

  return `<table>${rows.join("")}${rest.join("")}</table>`;
}

button.addEventListener("click", async () => {
  settings.rpcUrl = el<HTMLInputElement>("rpc").value.trim();
  settings.elements = Number(el<HTMLInputElement>("elements").value);
  settings.gasLimit = Number(el<HTMLInputElement>("gas").value);
  scripts.set(active, scriptBox.value);

  button.disabled = true;
  status.className = "status";
  out.innerHTML = "";

  const log = (message: string) => {
    status.textContent = message;
  };

  try {
    const result = await run({ ...settings, script: scriptBox.value, solidity, pristineSolidity }, log);
    const event = result.events.find((e) => Object.keys(e.fields).some((k) => k.includes("elements_requested")));

    out.innerHTML = `<div class="summary">
        <div><b>${result.results.toLocaleString("en-US")}</b><span>results</span></div>
        <div><b>${result.skipped.toLocaleString("en-US")}</b><span>skipped</span></div>
        <div><b>${result.requests}</b><span>eth_call requests</span></div>
        <div><b>${result.elapsedMs.toFixed(0)} ms</b><span>elapsed</span></div>
        <div><b>${result.compiled ? "browser" : "prebuilt"}</b><span>lens</span></div>
      </div>${event ? render(event.fields) : '<p class="lede">No wide event captured.</p>'}`;
    status.textContent = `done — ${result.distinct.toLocaleString("en-US")} distinct pairs discovered`;
  } catch (error) {
    status.className = "status error";
    status.textContent =
      error instanceof Error && /fetch|CORS|Failed to fetch/i.test(error.message)
        ? `${error.message} — this endpoint may not allow browser requests (no CORS headers).`
        : `${error}`;
  } finally {
    button.disabled = false;
  }
});
