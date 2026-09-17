import { ethCallTutorial } from "./eth-call.js";
import { ethGetLogsTutorial } from "./eth-get-logs.js";
import type { Tutorial } from "./types.js";

/** Sidebar order, after the About page. */
export const TUTORIALS: Tutorial[] = [ethGetLogsTutorial, ethCallTutorial];

export const tutorialById = (id: string) => TUTORIALS.find((tutorial) => tutorial.id === id);
