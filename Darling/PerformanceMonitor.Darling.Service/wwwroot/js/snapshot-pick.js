/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #6302: which stored snapshot the Active Queries tab shows, per server, in memory like the wait filter (util.js queryWaitFilter): not
 * persisted and not in the URL. The values are "latest" (the default), "all", a collection_time string, or {atMs} when a jump-to-moment
 * link asked for the snapshot nearest an instant (the snapshot list is read after the click, so the nearest one is resolved when the
 * list arrives). It has no imports so charts.js can set it without pulling in the page code. The picker is pages/active-snapshots.js.
 */

export const SNAPSHOT_LATEST = "latest";
export const SNAPSHOT_ALL = "all";

const picks = new Map();

export function snapshotPick(server) {
  return picks.get(server) || SNAPSHOT_LATEST;
}

export function setSnapshotPick(server, pick) {
  if (pick == null || pick === "" || pick === SNAPSHOT_LATEST) picks.delete(server);
  else picks.set(server, pick);
}

/** A jump to a moment (a chart's "Active queries at this time", a wait link on a row) selects the snapshot nearest `tMs`. */
export function focusSnapshotAt(server, tMs) {
  if (Number.isFinite(tMs)) picks.set(server, { atMs: tMs });
}
