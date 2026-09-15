import { type Config, generateSource, run } from "./run.js";

/** Facets that answer "did the toggle do anything" — shown first, and never hidden. */
const HIGHLIGHTS = [
  "elements_requested",
  "elements_fetched",
  "elements_missing",
  "nominal_batches",
  "batch_bytes.max",
  "chunks_initcode",
  "chunks_override",
  "override_fallbacks_unsupported",
  "pages_continued",
  "continuations",
  "gas_limit_observed",
  "fixed_gas",
  "item_gas_avg",
];

const config: Config = {
  rpcUrl: "https://mainnet.base.org",
  gasLimit: 600_000_000,
  envelope: "initcode",
  compress: false,
  continuations: "fill",
  elements: 2_000,
  statedGas: true,
};

const app = document.querySelector<HTMLElement>("#app")!;

app.innerHTML = `
  <h1>viem-dlc — deployless lens playground</h1>
  <p class="lede">
    Reads Morpho positions on Base through a lens contract that is never deployed, using the
    <code>deployless</code> transport straight from <code>src/</code>. Change a knob, run it, and watch the
    wide event: <code>envelope: "override"</code> lifts the 49 152-byte initcode cap, so the same elements
    collapse into fewer, larger chunks.
  </p>

  <div class="panel">
    <div class="controls">
      <div><label for="rpc">RPC URL (Base)</label><input id="rpc" type="text" value="${config.rpcUrl}" /></div>
      <div><label for="elements">Elements</label><input id="elements" type="number" min="1" max="20000" step="100" value="${config.elements}" /></div>
      <div><label for="gas">Transport gasLimit</label><input id="gas" type="number" min="1000000" step="50000000" value="${config.gasLimit}" /></div>
      <div><label for="envelope">batch.envelope</label><select id="envelope"><option value="initcode">initcode (default)</option><option value="override">override</option></select></div>
      <div><label for="continuations">batch.continuations</label><select id="continuations"><option value="fill">fill (default)</option><option value="eager">eager</option></select></div>
    </div>
    <div class="toggles">
      <label><input id="compress" type="checkbox" /> <code>batch.compress</code></label>
      <label><input id="statedGas" type="checkbox" checked /> state <code>batch.gas</code> (measured figures)</label>
    </div>
    <button id="go">Run</button>
    <div class="status" id="status"></div>
  </div>

  <div class="split">
    <div class="panel"><h2>The program this runs</h2><pre id="source"></pre></div>
    <div class="panel"><h2>Wide event</h2><div id="out"><p class="lede" style="margin:0">Run it to see the facets.</p></div></div>
  </div>
`;

const el = <T extends HTMLElement>(id: string) => document.querySelector<T>(`#${id}`)!;
const source = el("source");
const status = el("status");
const out = el("out");
const button = el<HTMLButtonElement>("go");

function readControls() {
  config.rpcUrl = el<HTMLInputElement>("rpc").value.trim();
  config.elements = Number(el<HTMLInputElement>("elements").value);
  config.gasLimit = Number(el<HTMLInputElement>("gas").value);
  config.envelope = el<HTMLSelectElement>("envelope").value as Config["envelope"];
  config.continuations = el<HTMLSelectElement>("continuations").value as Config["continuations"];
  config.compress = el<HTMLInputElement>("compress").checked;
  config.statedGas = el<HTMLInputElement>("statedGas").checked;
  source.textContent = generateSource(config);
}

for (const id of ["rpc", "elements", "gas", "envelope", "continuations", "compress", "statedGas"]) {
  el(id).addEventListener("input", readControls);
}
readControls();

function render(fields: Record<string, unknown>) {
  const keyOf = (name: string) => Object.keys(fields).find((k) => k === name || k.endsWith(`.${name}`));
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
  readControls();
  button.disabled = true;
  status.className = "status";
  out.innerHTML = "";

  try {
    const result = await run(config, (message) => {
      status.textContent = message;
    });
    const event = result.events.find((e) => Object.keys(e.fields).some((k) => k.includes("elements_requested")));

    out.innerHTML = `<div class="summary">
        <div><b>${result.results.toLocaleString("en-US")}</b><span>results</span></div>
        <div><b>${result.skipped.toLocaleString("en-US")}</b><span>skipped</span></div>
        <div><b>${result.requests}</b><span>eth_call requests</span></div>
        <div><b>${result.elapsedMs.toFixed(0)} ms</b><span>elapsed</span></div>
      </div>${event ? render(event.fields) : '<p class="lede">No wide event captured.</p>'}`;
    status.textContent = "done";
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
