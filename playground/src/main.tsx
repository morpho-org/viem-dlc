import * as React from "react";
import { createRoot } from "react-dom/client";

import { installNodeGlobals } from "./shim/globals.js";

// Modular token imports: the bundled `styles.css` carries all 26 colour scales, of which the page
// uses seven. `sand` backs `grayColor`; the rest are the syntax palette in `style.css`.
import "@radix-ui/themes/tokens/base.css";
import "@radix-ui/themes/tokens/colors/gray.css";
import "@radix-ui/themes/tokens/colors/sand.css";
import "@radix-ui/themes/tokens/colors/amber.css";
import "@radix-ui/themes/tokens/colors/cyan.css";
import "@radix-ui/themes/tokens/colors/indigo.css";
import "@radix-ui/themes/tokens/colors/jade.css";
import "@radix-ui/themes/tokens/colors/plum.css";
import "@radix-ui/themes/tokens/colors/red.css";
import "@radix-ui/themes/components.css";
import "@radix-ui/themes/utilities.css";
import "./style.css";
import { App } from "./App.js";

installNodeGlobals();

createRoot(document.querySelector("#app")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
