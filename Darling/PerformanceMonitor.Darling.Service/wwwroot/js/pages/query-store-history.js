/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #5234: the Query Store grid's History button. A click opens a panel under the row with that query's history, plan by
 * plan: a per-plan chart of average duration, a per-plan table, and a Plan button on each table row. It is served by
 * one store-only read (get_query_store_query_history); nothing here reaches the monitored server.
 *
 * An open panel is kept at module scope under its grid row's key, so the page's 60 s rebuild draws it again without a
 * second read. The grid has a row per plan, execution type and replica role of a query, so the key names all of them
 * and History opens under the row clicked, not under every row of the query. The entry also remembers the window it
 * was read for (hours and the custom range), and a rebuild under a different window reads it again. Every string is
 * drawn through el() as text.
 */
import { el, readTool, fmtMs, localTime, windowFromHours, activeRangeStamp, windowNoteText } from "../util.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS } from "../charts.js";
import { planSourceCell } from "./plan-viewer.js";

/* key -> { phase: "loading" | "done", stamp, window, result: { kind: "data" | "none" | "error", ... } }. `window` is the chart's
   axis ({ windowStart, windowEnd } or null), taken when the read was made so a redraw later keeps the axis the data was read for. */
const openHistories = new Map();
/* key -> Set of redraw functions, one per cell currently showing the key. Each draw carries `draw.host`, the cell it draws
   into, so redraw() can tell a cell the page threw away (a grid rebuild, a tab switch) from one still on it. */
const views = new Map();
/* The grid build a cell belongs to. queryStoreHistoryColumn() runs once per grid build, so each call starts a new generation,
   and a cell stamps its draw with the generation it registers under. `sweptGeneration` is the last one swept (see
   dropOlderDetached). */
let generation = 0;
let sweptGeneration = 0;

/* One key per grid row: get_query_store_top groups by database, query, plan, execution type and replica role. A part a row
   does not carry (a standalone server has no replica role) joins as nothing. */
const keyOf = (server, row) => [server, row.database_name, row.query_id, row.plan_id, row.execution_type_desc, row.replica_role].join("|");
/* The window a read was made for: the preset hours and the page's custom range, so a changed window reads again. */
const stampOf = (hours) => hours + "@" + activeRangeStamp();

/* A draw whose cell has left the page is dropped here instead of being drawn into detached DOM: every rebuild renders a
   fresh cell per row, and the old cell's draw would otherwise stay in the set for as long as the page lives (#5234).
   Pruning happens at redraw, never at registration, because a cell is not on the page yet while its grid is being
   built. The key leaves the map once its last cell is gone. */
const redraw = (key) => {
  const set = views.get(key);
  if (!set) return;
  for (const draw of [...set]) {
    if (draw.host && draw.host.isConnected === false) set.delete(draw);
    else draw();
  }
  if (set.size === 0) views.delete(key);
};

/* Runs when the first cell of a generation registers: every draw of an OLDER generation whose cell has left the page goes,
   under whatever key. redraw() prunes only the key it is called for, and a row nobody clicks (or one that has left the top
   list) never reaches it, so without this each rebuild would leave a cell per row in the set for the life of the page (#5234).
   The generation is what makes this safe: the cells of the build in progress are not on the page yet (the grid is attached
   after its rows are drawn) and are never dropped here, and a cell of an older build that is still on the page stays too. */
const dropOlderDetached = () => {
  for (const [key, set] of views) {
    for (const draw of [...set]) {
      if (draw.generation < generation && draw.host && draw.host.isConnected === false) set.delete(draw);
    }
    if (set.size === 0) views.delete(key);
  }
};

/** Forgets every open panel. Tests only; the page never needs it. */
export function resetQueryStoreHistory() {
  openHistories.clear();
  views.clear();
}

/** The keys of the open panels, for tests. */
export function openQueryStoreHistoryKeys() {
  return [...openHistories.keys()];
}

/** How many cells are registered to redraw for each key, as { key: count }, for tests. */
export function queryStoreHistoryViewCounts() {
  return Object.fromEntries([...views].map(([key, set]) => [key, set.size]));
}

/* The note an empty answer carries under `hints` when the window reaches past what the store holds (#5300), in the page's own
   clock, or null: an empty answer over a cut window is not a report that the query never ran. */
const emptyWindowNote = (hints) => (hints && typeof hints === "object" && hints.window_truncated === true ? windowNoteText(hints) : null);

/** Turns a read result into { kind: "data" | "none" | "error", ... }, or null when the read was abandoned. */
export function classifyHistoryRead(res) {
  if (!res || res.kind === "aborted" || res.kind === "auth") return null;
  const text = typeof res.message === "string" ? res.message : "";
  if (res.kind === "data" && res.data && Array.isArray(res.data.plans)) return { kind: "data", data: res.data };
  if (res.kind === "error") return { kind: "error", message: text || "The history could not be read." };
  return { kind: "none", message: text || "No history was found for this query in the window.", note: emptyWindowNote(res.hints) };
}

async function load(key, server, row, hours, stamp) {
  let res;
  try {
    res = await readTool("get_query_store_query_history", {
      server,
      database_name: row.database_name,
      query_id: row.query_id,
      hours,
    });
  } catch (e) {
    res = { kind: "error", message: e && e.message ? e.message : String(e) };
  }
  const current = openHistories.get(key);
  if (!current || current.stamp !== stamp) return; // closed, or read again for another window, while the read was out
  const outcome = classifyHistoryRead(res);
  if (outcome === null) openHistories.delete(key);
  else openHistories.set(key, { phase: "done", stamp, window: current.window, result: outcome });
  redraw(key);
}

function toggle(server, row, hours) {
  const key = keyOf(server, row);
  if (openHistories.has(key)) {
    openHistories.delete(key);
    redraw(key);
    return;
  }
  const stamp = stampOf(hours);
  openHistories.set(key, { phase: "loading", stamp, window: windowFromHours(hours) });
  redraw(key);
  return load(key, server, row, hours, stamp);
}

const cell = (text, cls) => el("td", { class: cls || null, text: text == null ? "—" : String(text) });
const num = (v, f) => (v == null ? "—" : f(v));
const whole = (v) => Number(v).toLocaleString("en-US", { maximumFractionDigits: 0 });

/* `scope` is the panel's own key. It keys each Plan button's panel, so a plan opened here is not also open in the grid's Plan
   column or in the History table under another row of the same query. */
function planTable(server, data, scope) {
  const head = ["Plan ID", "Execs", "Avg Duration", "Total Duration", "Avg CPU", "Total CPU", "First seen", "Last seen", "Forced", "Plan"];
  const rows = data.plans.map((p) =>
    el("tr", {}, [
      cell(p.plan_id, "num"),
      cell(num(p.execution_count, whole), "num"),
      cell(num(p.avg_duration_ms, fmtMs), "num"),
      cell(num(p.total_duration_ms, fmtMs), "num"),
      cell(num(p.avg_cpu_ms, fmtMs), "num"),
      cell(num(p.total_cpu_ms, fmtMs), "num"),
      cell(p.first_execution_time == null ? null : localTime(p.first_execution_time)),
      cell(p.last_execution_time == null ? null : localTime(p.last_execution_time)),
      cell(p.is_forced_plan ? "Yes" : "No"),
      el("td", {}, [
        planSourceCell(
          server,
          { kind: "query_store", database_name: data.database_name, query_id: data.query_id, plan_id: p.plan_id, scope },
          "Plan",
          "Show the stored plan for plan " + p.plan_id
        ),
      ]),
    ])
  );
  return el("table", { class: "data-table qs-history-table" }, [
    el("thead", {}, [el("tr", {}, head.map((h) => el("th", { text: h })))]),
    el("tbody", {}, rows),
  ]);
}

/* `axis` is the chart's window ({ windowStart, windowEnd }, or null for the data's own extent) as it was when the read was made.
   Under a preset, windowFromHours() reads the clock, so asking again at each redraw would slide the axis away from the data. */
/* The chart's id is built from the panel's own key, so two panels of one query (two grid rows) never share a chart's zoom or
   its hidden series. The chart keeps both under this id and the scope. */
function chartFor(key, data, hours, axis) {
  const points = (data.points || []).map((p) => ({ collection_time: p.collection_time, ["p" + p.plan_id]: p.avg_duration_ms }));
  const series = data.plans.map((p, i) => ({ key: "p" + p.plan_id, label: "Plan " + p.plan_id, color: SERIES_COLORS[i % SERIES_COLORS.length] }));
  return zoomableLineChart(
    { points, xKey: "collection_time", series, formatValue: (v) => fmtMs(v), unit: "ms", ...(axis || {}) },
    "qs-history|" + key,
    chartZoomScope(hours)
  );
}

function panelFor(key, server, hours) {
  const state = openHistories.get(key);
  if (state.phase === "loading") return el("div", { class: "plan-panel qs-history" }, [el("div", { class: "strip loading", text: "Loading the query's history..." })]);
  const r = state.result;
  if (r.kind !== "data") {
    const note = r.note ? el("div", { class: "strip notice", text: r.note }) : null;
    return el("div", { class: "plan-panel qs-history" }, [note, el("div", { class: r.kind === "error" ? "strip error" : "strip empty", text: r.message })]);
  }
  const d = r.data;
  const notice = d.window_truncated
    ? el("div", {
        class: "strip notice",
        text: "Stored history starts at " + localTime(d.effective_start) + ", later than the window asked for, so older runs are not shown.",
      })
    : null;
  const plansCut = d.plans_truncated ? el("div", { class: "strip notice", text: "Only the " + d.plans.length + " plans with the most total duration are listed." }) : null;
  const cut = d.points_truncated ? el("div", { class: "strip notice", text: "The chart shows the newest " + d.points.length.toLocaleString("en-US") + " snapshots of the window; older ones are not drawn." }) : null;
  return el("div", { class: "plan-panel qs-history" }, [notice, plansCut, cut, chartFor(key, d, hours, state.window), planTable(server, d, key)]);
}

/**
 * The History column for the Query Store grid: a button per row, and the panel under it while that query's history is
 * open. A row without a database or query id gets a dash. The key is synthetic (no row field carries it), so the
 * column is never sorted, filtered, exported or copied. The page calls this once per grid build, so each call starts a new
 * generation of cells for the redraw set.
 */
export function queryStoreHistoryColumn(server, hours) {
  generation += 1;
  return {
    key: "query_store_history",
    label: "History",
    render: (row) => {
      if (!row || row.database_name == null || row.query_id == null) return document.createTextNode("—");
      const key = keyOf(server, row);
      const host = el("div", { class: "plan-cell" });
      /* A rebuild under another window (a new preset or range) reads the open panel again. */
      const open = openHistories.get(key);
      if (open && open.stamp !== stampOf(hours)) {
        const stamp = stampOf(hours);
        openHistories.set(key, { phase: "loading", stamp, window: windowFromHours(hours) });
        load(key, server, row, hours, stamp);
      }
      const draw = () => {
        while (host.firstChild) host.removeChild(host.firstChild);
        const isOpen = openHistories.has(key);
        const button = el("button", {
          type: "button",
          class: "grid-tool",
          text: isOpen ? "Hide history" : "History",
          "aria-expanded": isOpen ? "true" : "false",
          title: "Show this query's history, plan by plan",
        });
        button.addEventListener("click", () => toggle(server, row, hours));
        host.appendChild(button);
        if (isOpen) host.appendChild(panelFor(key, server, hours));
      };
      draw.host = host;
      draw.generation = generation;
      if (sweptGeneration !== generation) {
        sweptGeneration = generation;
        dropOlderDetached();
      }
      if (!views.has(key)) views.set(key, new Set());
      views.get(key).add(draw);
      draw();
      return host;
    },
    sortable: false,
    filter: false,
    csv: false,
    copy: false,
  };
}
