/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* A move to another page, server or server tab starts at the top (#6159). The page scrolls as a whole, so a tab that opened
   while the last one was scrolled part-way down opened part-way down too. The 60 s poll re-renders the same route and must
   leave the reader where they are, so only a route that differs from the last one resets the scroll. */

/** A string that is the same for two renders of one page, server and tab. */
export function routeScrollKey(r) {
  return [r && r.name, r && r.param, r && r.tab].map((v) => (v == null ? "" : String(v))).join("|");
}

/** Returns the function route() calls on every render: it calls `scrollToTop` when the route changed and the render is not the poll's. */
export function makeScrollReset(scrollToTop) {
  let last = null;
  return (route, isPoll) => {
    const key = routeScrollKey(route);
    const changed = last !== null && key !== last;
    last = key;
    if (changed && !isPoll) scrollToTop();
    return changed && !isPoll;
  };
}
