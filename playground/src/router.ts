import * as React from "react";

/**
 * Hash routing, because GitHub Pages serves a static tree with no SPA fallback — a real path would
 * 404 on reload or on a shared link.
 */
export const routeOf = (hash: string) => hash.replace(/^#\/?/, "") || "about";

export function useRoute(): string {
  const [route, setRoute] = React.useState(() => routeOf(window.location.hash));

  React.useEffect(() => {
    const onChange = () => setRoute(routeOf(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);

  return route;
}

export const navigate = (route: string) => {
  window.location.hash = `#/${route}`;
  // A hash change never scrolls, so a new page would otherwise open at the old page's offset.
  window.scrollTo({ top: 0 });
};
