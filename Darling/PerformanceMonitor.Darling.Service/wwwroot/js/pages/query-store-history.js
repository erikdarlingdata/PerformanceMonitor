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
 * An open panel is kept at module scope under server|database|query_id, so the page's 60 s rebuild draws it again
 * without a second read. Every string is drawn through el() as text.
 */
import { el, readTool, fmtMs, localTime, windowFromHours } from "../util.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS } from "../charts.js";
import { planSourceCell } from "./plan-viewer.js";

/* key -> { phase: "loading" | "done", result: { kind: "data" | "none" | "error", ... } }. */
const openHistories = new Map();
/* key -> Set of redraw functions, one per cell currently showing the key. */
const views = new Map();

const keyOf = (server, row) => [server, row.database_name, row.query_id].join("|");

const redraw = (key) => {
  for (const draw of views.get(key) || []) draw();
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

/** Turns a read result into { kind: "data" | "none" | "error", ... }, or null when the read was abandoned. */
export function classifyHistoryRead(res) {
  if (!res || res.kind === "aborted" || res.kind === "auth") return null;
  const text = typeof res.message === "string" ? res.message : "";
  if (res.kind === "data" && res.data && Array.isArray(res.data.plans)) return { kind: "data", data: res.data };
  if (res.kind === "error") return { kind: "error", message: text || "The history could not be read." };
  return { kind: "none", message: text || "No history was found for this query in the window." };
}

async function load(key, server, row, hours) {
  let res;
  try {
    res = await readTool("get_query_store_query_history", {
      server,
      database_name: row.database_name,
      query_id: row.query_id,
      hours_back: hours,
    });
  } catch (e) {
    res = { kind: "error", message: e && e.message ? e.message : String(e) };
  }
  if (!openHistories.has(key)) return; // closed while the read was out
  const outcome = classifyHistoryRead(res);
  if (outcome === null) openHistories.delete(key);
  else openHistories.set(key, { phase: "done", result: outcome });
  redraw(key);
}

function toggle(server, row, hours) {
  const key = keyOf(server, row);
  if (openHistories.has(key)) {
    openHistories.delete(key);
    redraw(key);
    return;
  }
  openHistories.set(key, { phase: "loading" });
  redraw(key);
  return load(key, server, row, hours);
}

const cell = (text, cls) => el("td", { class: cls || null, text: text == null ? "—" : String(text) });
const num = (v, f) => (v == null ? "—" : f(v));
const whole = (v) => Number(v).toLocaleString("en-US", { maximumFractionDigits: 0 });

function planTable(server, data, hours) {
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
          { kind: "query_store", database_name: data.database_name, query_id: data.query_id, plan_id: p.plan_id },
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

function chartFor(data, hours) {
  const points = (data.points || []).map((p) => ({ collection_time: p.collection_time, ["p" + p.plan_id]: p.avg_duration_ms }));
  const series = data.plans.map((p, i) => ({ key: "p" + p.plan_id, label: "Plan " + p.plan_id, color: SERIES_COLORS[i % SERIES_COLORS.length] }));
  return zoomableLineChart(
    { points, xKey: "collection_time", series, formatValue: (v) => fmtMs(v), unit: "ms", ...(windowFromHours(hours) || {}) },
    "qs-history|" + data.database_name + "|" + data.query_id,
    chartZoomScope(hours)
  );
}

function panelFor(key, server, hours) {
  const state = openHistories.get(key);
  if (state.phase === "loading") return el("div", { class: "plan-panel qs-history" }, [el("div", { class: "strip loading", text: "Loading the query's history..." })]);
  const r = state.result;
  if (r.kind !== "data") {
    return el("div", { class: "plan-panel qs-history" }, [el("div", { class: r.kind === "error" ? "strip error" : "strip empty", text: r.message })]);
  }
  const d = r.data;
  const notice = d.window_truncated
    ? el("div", {
        class: "strip notice",
        text: "Stored history starts at " + localTime(d.effective_start) + ", later than the window asked for, so older runs are not shown.",
      })
    : null;
  const cut = d.points_truncated ? el("div", { class: "strip notice", text: "The chart shows the first 2,000 snapshots of the window." }) : null;
  return el("div", { class: "plan-panel qs-history" }, [notice, cut, chartFor(d, hours), planTable(server, d, hours)]);
}

/**
 * The History column for the Query Store grid: a button per row, and the panel under it while that query's history is
 * open. A row without a database or query id gets a dash. The key is synthetic (no row field carries it), so the
 * column is never sorted, filtered, exported or copied.
 */
export function queryStoreHistoryColumn(server, hours) {
  return {
    key: "query_store_history",
    label: "History",
    render: (row) => {
      if (!row || row.database_name == null || row.query_id == null) return document.createTextNode("—");
      const key = keyOf(server, row);
      const host = el("div", { class: "plan-cell" });
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
