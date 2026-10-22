/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #6302: the snapshot picker on the Active Queries tab. The desktop viewer lists the stored snapshots and opens on the latest one;
 * the web drew one flat table over the window. A snapshot is one capture instant (one collection_time). The row read caps ROWS,
 * not captures, so a 50-row page of a busy hour can hold two snapshots and hide the rest: the list comes from get_active_queries
 * with snapshots_only, which groups in SQL under the same filters the table uses, and a pick reads one snapshot with
 * collection_time. "All snapshots in window" is the table as it was before the picker.
 *
 * Every label is drawn as text (el's text path). The picker is a native select, so it is as usable at 375 px as the other pickers.
 */

import { el, mount, readToolWithinKeptHistory, loadingStrip, localTime, parseUtc } from "../util.js";
import { getPanelSignal, setPanelSignal } from "../panels.js";
import { SNAPSHOT_LATEST, SNAPSHOT_ALL, snapshotPick, setSnapshotPick } from "../snapshot-pick.js";
import { windowParams } from "../time-slicer.js";

export { SNAPSHOT_LATEST, SNAPSHOT_ALL, snapshotPick, setSnapshotPick, focusSnapshotAt } from "../snapshot-pick.js";

/* The newest snapshot times one list holds. A one-hour window at the collector's pace is far below this. */
export const SNAPSHOT_LIST_CAP = 500;

/** The sentence for a snapshot list the read cut to its newest rows, or "" when it was whole (the picker and the time slicer's Active strip say it alike). */
export function snapshotTruncatedNote(truncated, shown, total) {
  return truncated ? "newest " + shown + " of " + total + " snapshot times" : "";
}
/* A snapshot's own rows: the cap the wait drill-down uses, for the same reason (a busy capture holds many requests). */
export const SNAPSHOT_ROW_CAP = 500;

/** The collection_time of the snapshot nearest `atMs` in `snapshots` (rows of {collection_time}), or null when none parses. */
export function nearestSnapshot(snapshots, atMs) {
  let best = null;
  let bestGap = Infinity;
  for (const s of snapshots || []) {
    const at = parseUtc(s && s.collection_time);
    if (!at) continue;
    const gap = Math.abs(at.getTime() - atMs);
    if (gap < bestGap) {
      best = s.collection_time;
      bestGap = gap;
    }
  }
  return best;
}

/**
 * What the list and the pick say to draw: `{ all: true }` for the whole window, or `{ time }` for one snapshot. A pick whose
 * snapshot is no longer listed (the window moved on) falls back to the latest, which is what the viewer opens on.
 */
export function resolveSnapshotPick(pick, snapshots) {
  if (pick === SNAPSHOT_ALL) return { all: true };
  const list = snapshots || [];
  if (list.length === 0) return { all: true };
  if (pick && typeof pick === "object" && Number.isFinite(pick.atMs)) {
    return { time: nearestSnapshot(list, pick.atMs) || list[0].collection_time };
  }
  if (typeof pick === "string" && pick !== SNAPSHOT_LATEST && list.some((s) => s.collection_time === pick)) return { time: pick };
  return { time: list[0].collection_time };
}

function rowsText(n) {
  return n === 1 ? "1 row" : n + " rows";
}

/** The picker's options: the latest, the whole window, then every listed snapshot newest first. */
export function snapshotOptions(snapshots) {
  const list = snapshots || [];
  return [
    { value: SNAPSHOT_LATEST, label: "Latest snapshot" + (list.length ? " (" + localTime(list[0].collection_time) + ")" : "") },
    { value: SNAPSHOT_ALL, label: "All snapshots in window" },
    ...list.map((s) => ({ value: s.collection_time, label: localTime(s.collection_time) + " (" + rowsText(s.row_count) + ")" })),
  ];
}

function snapshotPicker(options, selected, truncatedNote, onPick) {
  const sel = el(
    "select",
    { class: "range-select-inline", "aria-label": "Snapshot" },
    options.map((o) => el("option", { value: o.value, text: o.label }))
  );
  sel.value = options.some((o) => o.value === selected) ? selected : SNAPSHOT_LATEST;
  sel.addEventListener("change", () => onPick(sel.value));
  const kids = [el("label", { class: "range-control" }, [el("span", { text: "Snapshot" }), sel])];
  if (truncatedNote) kids.push(el("span", { class: "range-control", text: truncatedNote }));
  return el("div", { class: "picker-row" }, kids);
}

/**
 * The Active Queries card with its snapshot picker. `drawTable(snapshotTime, control)` builds the grid: the whole window when
 * `snapshotTime` is null, otherwise that one snapshot, with `control` (the picker) placed under its title. The list is read once
 * per slot (the tab rebuilds every 60 s, so "Latest" follows the newest capture); a pick draws the grid again over the same list.
 * A list read that does not answer with data (empty, refused, failed) draws the whole-window grid with no picker, and that grid
 * says its own empty or error state.
 */
export function activeQueriesSnapshotSlot(server, ctx, wait, extraParams, drawTable) {
  const slot = el("div", {}, []);
  slot.style.display = "contents";
  let drawing = null;
  let list = null;

  const build = (snapshots, truncatedNote, signal) => {
    const pick = snapshotPick(server);
    const chosen = resolveSnapshotPick(pick, snapshots);
    /* A jump-to-moment pick is kept as the snapshot it resolved to, so a redraw does not drift to a newer nearest one. */
    if (pick && typeof pick === "object" && chosen.time) setSnapshotPick(server, chosen.time);
    const now = snapshotPick(server);
    const selected = chosen.all ? SNAPSHOT_ALL : typeof now === "string" && now === chosen.time ? chosen.time : SNAPSHOT_LATEST;
    const picker = snapshotPicker(snapshotOptions(snapshots), selected, truncatedNote, (value) => {
      setSnapshotPick(server, value);
      draw();
    });
    return drawWith(signal, chosen.all ? null : chosen.time, picker);
  };

  /* The grid fetches on its own, under the panel signal that is current when it is built; this draw's signal is set around that
     one synchronous call, as the ranking card does. */
  const drawWith = (signal, snapshotTime, control) => {
    const prev = getPanelSignal();
    setPanelSignal(signal);
    const card = drawTable(snapshotTime, control);
    setPanelSignal(prev);
    return card;
  };

  const draw = async () => {
    if (drawing) drawing.abort();
    const mine = new AbortController();
    drawing = mine;
    const outer = getPanelSignal();
    if (outer) outer.addEventListener("abort", () => mine.abort(), { once: true });
    if (list === null) {
      mount(slot, [loadingStrip("Loading active-query snapshots…")]);
      let res;
      try {
        /* #6301: a narrowed window (ctx.narrowed) is read with its own end (as_of) as well as its hours. */
        const params = windowParams({ server, hours: ctx.hours, limit: SNAPSHOT_LIST_CAP, snapshots_only: true, ...extraParams }, ctx, true);
        if (wait) params.wait_type = wait;
        /* The grid's own rule (util.js readWithinKeptHistory): a window longer than the history the server keeps is asked again
           at the hours it keeps, so the list and the grid read the same window (#6302). */
        res = await readToolWithinKeptHistory("get_active_queries", params, mine.signal);
      } catch (e) {
        res = { kind: "error" };
      }
      if (mine.signal.aborted || drawing !== mine) return;
      const data = res && res.kind === "data" && res.data && Array.isArray(res.data.snapshots) ? res.data : null;
      list = data ? { snapshots: data.snapshots, truncated: data.truncated === true, total: data.total_snapshot_times } : { snapshots: [] };
    }
    if (list.snapshots.length === 0) {
      mount(slot, [drawWith(mine.signal, null, null)]);
      return;
    }
    const note = snapshotTruncatedNote(list.truncated, list.snapshots.length, list.total);
    mount(slot, [build(list.snapshots, note, mine.signal)]);
  };

  draw();
  return slot;
}
