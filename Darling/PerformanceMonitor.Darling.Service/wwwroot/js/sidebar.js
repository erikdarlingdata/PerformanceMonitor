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
 * the search box, and the box re-reads the term. Typing repaints through the callback app.js hands to
 * initSidebarSearch (its paintSidebar), so the active row is read from the CURRENT route, never a cached one. Whether the list is grouped and which groups are collapsed
 * persist per browser through viewer-local.js (ids and booleans only, never a name).
 */

import { el, mount, bandClass } from "./util.js";
import { navigateServer } from "./panels.js";
import { healthBandClass, healthLabel } from "./store-picker.js";
import {
  favoritesFirst, isFavorite, isSidebarGrouped, setSidebarGrouped, isSidebarGroupCollapsed, toggleSidebarGroup,
} from "./viewer-local.js";
import { favoriteStar, alertBadge } from "./viewer-local-ui.js";
import { sidebarRows } from "./fleet-groups.js";
import { byDisplayName, rememberFleet } from "./server-order.js";

const INDENT_REM = 0.75; // per group depth
const BASE_PAD_REM = 1.25; // .server-item's own left padding

let searchTerm = "";

/* The order every server pick list on the web shares (server-order.js): display name, favourites first. */
const byName = byDisplayName;

/**
 * Builds the search box and the group-by-tag toggle into `host`. Called once at start-up. `repaint` is the app's
 * sidebar paint (it reads the last fleet and the current route fresh and calls paintServerList).
 */
export function initSidebarSearch(host, repaint) {
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
    if (repaint) repaint();
  });
  const grouped = el("input", { type: "checkbox", "aria-label": "Group servers by tag" });
  grouped.checked = isSidebarGrouped();
  grouped.addEventListener("change", () => {
    setSidebarGrouped(grouped.checked); // repaints through the local-state listener
  });
  mount(host, [input, el("label", { class: "group-control sidebar-group-control" }, [grouped, el("span", { text: "Group by tag" })])]);
}

/** The current search text, for the status the page shows and for tests. */
export function sidebarSearchTerm() {
  return searchTerm;
}

/* `store` is the store the row belongs to (front door only; undefined keeps the link to the one store there is). `own` says the row's
   store is the one in use: the favourite star and the alert badge read ids and counts that belong to the current store, so a row of
   another store carries neither. */
function serverItem(c, depth, activeParam, grouped, store, own = true) {
  const target = c.server_name || c.display_name;
  const active = activeParam != null && (activeParam === c.server_name || activeParam === c.display_name);
  return el(
    "div",
    {
      class: "server-item" + (active ? " active" : ""),
      dataset: { server: target, display: c.display_name },
      style: grouped ? "padding-left:" + (BASE_PAD_REM + depth * INDENT_REM) + "rem" : null,
      onActivate: () => navigateServer(target, store),
    },
    [
      el("span", { class: "dot " + bandClass(c.band) }),
      el("span", { class: "name", text: c.display_name }),
      own ? alertBadge(c.server_id) : null,
      own ? favoriteStar(c.server_id) : null,
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
      // a collapsed header says how many servers it hides: its whole subtree, counting a multi-tag server once
      el("span", { class: "sidebar-group-count", text: (g.collapsed ? g.subtreeCount : g.count) ? "(" + (g.collapsed ? g.subtreeCount : g.count) + ")" : "" }),
    ]
  );
  return header;
}

/**
 * Paints the server list into `container` from the /api/fleet payload. `activeParam` is the server named by the
 * current route (null when the route is not a server page).
 */
export function paintServerList(container, fleet, activeParam) {
  rememberFleet(fleet.cards);
  const refocus = focusedGroupId(container);
  const rows = serverNodes(fleet, activeParam, undefined, true, false);
  if (rows.empty) {
    mount(container, searchTerm.trim() ? el("div", { class: "muted sidebar-no-match", text: "No servers match" }) : []);
    return;
  }
  mount(container, rows.nodes);
  restoreGroupFocus(container, refocus);
}

/**
 * The front door's list: one block per store, each a header (the store's name and health) and that store's servers, built
 * from the store's own /api/fleet. `groups` is [{ store: { name, health }, res }] where `res` is apiGetFleet's answer: a store
 * whose read failed shows its own line and the others still draw. `current` is the store in use; only its rows carry the
 * favourite star and alert badge. The search box filters every store's rows. The lists are flat: a tag's id means a different
 * tag in each store, so the group-by-tag collapse state cannot be shared between them.
 */
export function paintStoreServerLists(container, groups, activeParam, current) {
  const refocus = focusedGroupId(container);
  const nodes = [];
  let anyRow = false;
  for (const g of groups) {
    const name = g.store.name;
    const header = el("div", { class: "sidebar-store-header", dataset: { store: name } }, [
      el("span", { class: "dot " + healthBandClass(g.store.health) }),
      el("span", { class: "sidebar-store-name", text: name }),
      g.store.health === "ok" ? null : el("span", { class: "sidebar-store-health", text: healthLabel(g.store.health) }),
    ]);
    if (g.res.kind !== "data") {
      nodes.push(header, el("div", { class: "muted sidebar-store-error", style: "padding:0.25rem 1.25rem", text: g.res.message || "Fleet unavailable" }));
      continue;
    }
    if (name === current) rememberFleet(g.res.data.cards);
    const rows = serverNodes(g.res.data, name === current ? activeParam : null, name, name === current, true);
    nodes.push(header);
    if (rows.empty) continue;
    anyRow = true;
    nodes.push(...rows.nodes);
  }
  if (!anyRow && searchTerm.trim()) nodes.push(el("div", { class: "muted sidebar-no-match", text: "No servers match" }));
  mount(container, nodes);
  restoreGroupFocus(container, refocus);
}

/* The rows (group headers and server rows) for one /api/fleet payload, under the search term and the grouping choice. */
function serverNodes(fleet, activeParam, store, own, flat) {
  const grouped = flat ? false : isSidebarGrouped();
  const model = sidebarRows(fleet.cards || [], fleet.tags || [], {
    term: searchTerm,
    grouped,
    sortFn: favoritesFirst(byName),
    isFavorite: (c) => isFavorite(c.server_id),
    isCollapsed: isSidebarGroupCollapsed,
  });
  if (model.empty) return { empty: true, nodes: [] };
  return {
    empty: false,
    nodes: model.rows.map((r) => (r.kind === "group" ? groupHeader(r) : serverItem(r.card, r.depth, activeParam, grouped, store, own))),
  };
}

/* A repaint replaces every row, which destroys the element a keyboard user just activated. When that was a group
   header, put focus back on the same group's new header so Enter/Space can toggle it again without tabbing down
   from the top. (The favourite star keeps its dev behaviour: the same server can sit under several groups, so
   there is no single row to hand focus back to.) */
function focusedGroupId(container) {
  const f = document.activeElement;
  if (!f || !f.dataset || f.dataset.group == null) return null;
  return Array.from(container.children).includes(f) ? f.dataset.group : null;
}

function restoreGroupFocus(container, id) {
  if (id == null) return;
  const header = Array.from(container.children).find((n) => n.dataset && n.dataset.group === id);
  if (header) header.focus();
}
