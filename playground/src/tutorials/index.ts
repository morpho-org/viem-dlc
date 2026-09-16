import { cacheTutorial } from "./cache.js";
import { ethCallTutorial } from "./eth-call.js";
import { logsDividerTutorial } from "./logs-divider.js";
import { searchReduceTutorial } from "./search-reduce.js";
import type { Tutorial } from "./types.js";

/** Sidebar order, after the About page. */
export const TUTORIALS: Tutorial[] = [ethCallTutorial, logsDividerTutorial, cacheTutorial, searchReduceTutorial];

export const tutorialById = (id: string) => TUTORIALS.find((tutorial) => tutorial.id === id);
