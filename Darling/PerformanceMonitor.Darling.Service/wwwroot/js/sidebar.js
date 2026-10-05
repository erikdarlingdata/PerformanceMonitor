/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The sidebar server list (#5243): a live search box, an optional group-by-tag view, and the list itself. The
 * browser computes nothing here: /api/fleet already carries each card's tags and the tag forest, so this only
 * filters and groups what the shell read (fleet-groups.js holds the rules the Fleet page shares).
 *
 * The search term is module state, not persisted: the 60 s repaint and a route change rebuild the list, never
 * the search box, and the box re-reads the term. Whether the list is grouped and which groups are collapsed
 * persist per browser through viewer-local.js (ids and booleans only, never a name).
 */

import { el, mount, bandClass } from "./util.js";
import { navigateServer } from "./panels.js";
import {
  favoritesFirst, isFavorite, isSidebarGrouped, setSidebarGrouped, isSidebarGroupCollapsed, toggleSidebarGroup,
} from "./viewer-local.js";
import { favoriteStar, alertBadge } from "./viewer-local-ui.js";
import { sidebarRows } from "./fleet-groups.js";

const INDENT_REM = 0.75; // per group depth
const BASE_PAD_REM = 1.25; // .server-item's own left padding

let searchTerm = "";
let lastList = null; // { container, fleet, activeParam } from the last paint, so typing repaints without a refetch

const byName = (a, b) => a.display_name.localeCompare(b.display_name);

/** Builds the search box and the group-by-tag toggle into `host`. Called once at start-up. */
export function initSidebarSearch(host) {
  if (!host) return;
  const input = el("input", {
    class: "search-input sidebar-search-input",
    type: "search",
    placeholder: "Search servers",
    "aria-label": "Search servers by name or tag",
  });
  input.value = searchTerm;
  input.addEventListener("input", () => {
    searchTerm = input.value;
    repaint();
  });
  const grouped = el("input", { type: "checkbox", "aria-label": "Group servers by tag" });
  grouped.checked = isSidebarGrouped();
  grouped.addEventListener("change", () => {
    setSidebarGrouped(grouped.checked); // repaints through the local-state listener
  });
  mount(host, [input, el("label", { class: "group-control sidebar-group-control" }, [grouped, el("span", { text: "Group by tag" })])]);
}

function repaint() {
  if (lastList) paintServerList(lastList.container, lastList.fleet, lastList.activeParam);
}

/** The current search text, for the status the page shows and for tests. */
export function sidebarSearchTerm() {
  return searchTerm;
}

function serverItem(c, depth, activeParam, grouped) {
  const target = c.server_name || c.display_name;
  const active = activeParam != null && (activeParam === c.server_name || activeParam === c.display_name);
  return el(
    "div",
    {
      class: "server-item" + (active ? " active" : ""),
      dataset: { server: target, display: c.display_name },
      style: grouped ? "padding-left:" + (BASE_PAD_REM + depth * INDENT_REM) + "rem" : null,
      onActivate: () => navigateServer(target),
    },
    [
      el("span", { class: "dot " + bandClass(c.band) }),
      el("span", { class: "name", text: c.display_name }),
      alertBadge(c.server_id),
      favoriteStar(c.server_id),
    ]
  );
}

function groupHeader(g) {
  const header = el(
    "div",
    {
      class: "sidebar-group-header",
      style: "padding-left:" + (0.75 + g.depth * INDENT_REM) + "rem",
      "aria-expanded": g.collapsed ? "false" : "true",
      dataset: { group: String(g.id) },
      onActivate: () => toggleSidebarGroup(g.id),
    },
    [
      el("span", { class: "sidebar-group-chevron", text: g.collapsed ? "\u25B8" : "\u25BE" }),
      el("span", { class: "sidebar-group-name", text: g.name }),
      el("span", { class: "sidebar-group-count", text: g.count ? "(" + g.count + ")" : "" }),
    ]
  );
  return header;
}

/**
 * Paints the server list into `container` from the /api/fleet payload. `activeParam` is the server named by the
 * current route (null when the route is not a server page).
 */
export function paintServerList(container, fleet, activeParam) {
  lastList = { container, fleet, activeParam };
  const grouped = isSidebarGrouped();
  const model = sidebarRows(fleet.cards || [], fleet.tags || [], {
    term: searchTerm,
    grouped,
    sortFn: favoritesFirst(byName),
    isFavorite: (c) => isFavorite(c.server_id),
    isCollapsed: isSidebarGroupCollapsed,
  });
  if (model.empty) {
    mount(container, searchTerm.trim() ? el("div", { class: "muted sidebar-no-match", text: "No servers match" }) : []);
    return;
  }
  mount(
    container,
    model.rows.map((r) => (r.kind === "group" ? groupHeader(r) : serverItem(r.card, r.depth, activeParam, grouped)))
  );
}
