/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* One order for every server list on the web: the sidebar's. The sidebar sorts by display name, favourites first;
   a pick list that sorted another way (the registry's order, or list_servers' order) put the same servers in a
   different order from the sidebar beside it. The sidebar, the pickers and the scope lists all sort through here.

   The favourites are keyed on the numeric server id the fleet cards carry. list_servers rows carry no id, so the sidebar
   hands every fleet read to rememberFleet() and a row without an id borrows the one the last fleet read gave its
   server_name. Before the first fleet read nothing is known, and the order is the plain display-name order. */

import { favoritesFirst } from "./viewer-local.js";

/** server_name -> server_id from the last fleet read. */
let idByName = new Map();

function labelOf(r) {
  return String((r && (r.display_name || r.label || r.server_name || r.value)) || "");
}

/** The sidebar's comparator: display name (falling back to the server name), as the user reads it. */
export function byDisplayName(a, b) {
  return labelOf(a).localeCompare(labelOf(b));
}

/** Remember which server_id each server_name has, from the cards of a /api/fleet read. */
export function rememberFleet(cards) {
  const next = new Map();
  for (const c of Array.isArray(cards) ? cards : []) {
    if (!c || !Number.isInteger(c.server_id)) continue;
    if (c.server_name) next.set(c.server_name, c.server_id);
    if (c.display_name && !next.has(c.display_name)) next.set(c.display_name, c.server_id);
  }
  idByName = next;
}

function idOf(r) {
  if (!r) return undefined;
  if (Number.isInteger(r.server_id)) return r.server_id;
  return idByName.get(r.server_name || r.value) ?? idByName.get(r.display_name);
}

/**
 * A copy of `rows` in the sidebar's order. A row is a list_servers row (`server_name`, `display_name`), a fleet card
 * (`server_id` too) or a picker option (`value`, `label`): whichever of those names it has is used.
 */
export function orderServers(rows) {
  const keyed = (rows || []).map((r) => ({ r, server_id: idOf(r), display_name: labelOf(r) }));
  keyed.sort(favoritesFirst(byDisplayName));
  return keyed.map((k) => k.r);
}
