// The Vite application's entry.
import { QuoteLens } from "./lens";

const target = document.querySelector("#bytecode");
if (target) target.textContent = QuoteLens.bytecode;
