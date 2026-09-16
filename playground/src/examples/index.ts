import { cacheExample } from "./cache.js";
import { deploylessExample } from "./deployless.js";
import { logsDividerExample } from "./logs-divider.js";
import { searchReduceExample } from "./search-reduce.js";
import type { Example } from "./types.js";

/** Sidebar order, after the About page. */
export const EXAMPLES: Example[] = [logsDividerExample, cacheExample, searchReduceExample, deploylessExample];

export const exampleById = (id: string) => EXAMPLES.find((example) => example.id === id);
