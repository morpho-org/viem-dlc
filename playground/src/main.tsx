import * as React from "react";
import { createRoot } from "react-dom/client";

import { installNodeGlobals } from "./shim/globals.js";

import "../vendor/srcl/global.css";
import "./style.css";
import { App } from "./App.js";

/** SRCL scopes its palette to `body.theme-light` / `body.theme-dark`; neither is set by default. */
installNodeGlobals();

function followSystemTheme() {
  const dark = window.matchMedia("(prefers-color-scheme: dark)");
  const apply = () => {
    document.body.classList.toggle("theme-dark", dark.matches);
    document.body.classList.toggle("theme-light", !dark.matches);
  };
  apply();
  dark.addEventListener("change", apply);
}

followSystemTheme();

createRoot(document.querySelector("#app")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
