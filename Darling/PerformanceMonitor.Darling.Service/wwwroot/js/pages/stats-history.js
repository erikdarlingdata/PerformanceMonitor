/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #6303: the History button on the Top Queries and Top Procedures grids, the browser's twin of the desktop viewer's double click on
 * those rows (QueryStatsHistoryWindow, ProcedureHistoryWindow). A click opens a panel under the row with that one query's or
 * procedure's history over the page's window; a second click closes it.
 *
 * Queries read get_query_trend (query_hash + database) and draw average CPU and average elapsed. A procedure reads
 * get_procedure_duration_trend limited to that procedure (database_name + procedure_name, the grid's own full_name) and draws one
 * metric at a time from a selector, as the viewer's window does: average duration per execution, average CPU per execution (the
 * read's avg_cpu_ms, worker time over executions), duration in ms per second, and executions per second. The viewer's spill series is
 * not drawn: the hourly rollup keeps no spills column, so the read cannot carry it on both tiers; logical reads are left out on purpose. A point with no rate (a restart, or a bucket with nothing rated) is a gap in the line, never a 0.
 *
 * An open panel is kept at module scope under its row's key, so the page's 60 s rebuild draws it again without a second read. The
 * entry remembers the window it was read for, and a rebuild under a different window reads it again. Every string is drawn as text.
 */
import { el, readTool, windowFromHours, activeRangeStamp } from "../util.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS } from "../charts.js";
import { dockPanelUnderRow, undockPanel } from "../plan-row.js";

/* key -> { phase: "loading" | "done", stamp, window, metric, result: { kind: "data" | "none" | "error", ... } } */
const open = new Map();

/** The procedure chart's metrics: the selector's options, the first being the default (minus the viewer's spill series, which this read does not carry). */
export const PROCEDURE_METRICS = [
  { value: "avg_elapsed_ms", label: "Avg Duration (ms)", unit: "ms" },
  { value: "avg_cpu_ms", label: "Avg CPU (ms)", unit: "ms" },
  { value: "elapsed_ms_per_second", label: "Duration (ms/s)", unit: "ms/s" },
  { value: "executions_per_second", label: "Executions (per second)", unit: "/s" },
];

/**
 * One chart point per trend point, with the average duration per execution derived from the two rates the read returns (elapsed
 * ms per second over executions per second). A point missing either rate, or with no executions, has no average: null, which the
 * chart leaves as a gap. avg_cpu_ms is the read's own figure, null where the bucket executed nothing. Pure; tests run it.
 */
export function procedurePoints(trend) {
  return (Array.isArray(trend) ? trend : []).map((p) => {
    const ms = typeof p.elapsed_ms_per_second === "number" ? p.elapsed_ms_per_second : null;
    const eps = typeof p.executions_per_second === "number" ? p.executions_per_second : null;
    return {
      time: p.time,
      elapsed_ms_per_second: ms,
      executions_per_second: eps,
      avg_elapsed_ms: ms != null && eps != null && eps > 0 ? ms / eps : null,
      avg_cpu_ms: typeof p.avg_cpu_ms === "number" ? p.avg_cpu_ms : null,
    };
  });
}

/** The row's identity, as the page's grid shows it: what is sent to the read, or null when the row cannot be trended. */
export function historyTarget(kind, row) {
  if (!row || !row.database_name) return null;
  if (kind === "procedures") return row.full_name ? { database_name: row.database_name, procedure_name: row.full_name } : null;
  return row.query_hash ? { database_name: row.database_name, query_hash: row.query_hash } : null;
}

const keyOf = (kind, server, target) => [kind, server, target.database_name, target.procedure_name || target.query_hash].join("|");
const stampOf = (hours) => hours + "@" + activeRangeStamp();

async function load(kind, key, server, target, hours, stamp) {
  let res;
  try {
    res =
      kind === "procedures"
        ? await readTool("get_procedure_duration_trend", { server, hours, ...target })
        : await readTool("get_query_trend", { server, hours, ...target });
  } catch (e) {
    res = { kind: "error", message: e && e.message ? e.message : String(e) };
  }
  const current = open.get(key);
  if (!current || current.stamp !== stamp) return; // closed, or read again for another window, while the read was out
  if (!res || res.kind === "aborted" || res.kind === "auth") open.delete(key);
  else if (res.kind === "data" && res.data) open.set(key, { ...current, phase: "done", result: { kind: "data", data: res.data } });
  else if (res.kind === "error") open.set(key, { ...current, phase: "done", result: { kind: "error", message: res.message || "The history could not be read." } });
  else open.set(key, { ...current, phase: "done", result: { kind: "none", message: res.message || "No history was found in the window.", note: null } });
  current.redraw();
}

function strip(cls, text) {
  return el("div", { class: "strip " + cls, text });
}

function chartFor(kind, key, data, hours, axis, metric) {
  if (kind === "procedures") {
    const m = PROCEDURE_METRICS.find((x) => x.value === metric) || PROCEDURE_METRICS[0];
    return zoomableLineChart(
      {
        points: procedurePoints(data.trend),
        xKey: "time",
        series: [{ key: m.value, label: m.label, color: SERIES_COLORS[0] }],
        formatValue: (v) => (m.value === "executions_per_second" ? Number(v).toFixed(3) : Math.round(v).toLocaleString() + " " + m.unit),
        unit: m.unit,
        ...(axis || {}),
      },
      "stats-history|" + key + "|" + m.value,
      chartZoomScope(hours)
    );
  }
  return zoomableLineChart(
    {
      points: data.trend || [],
      xKey: "collection_time",
      series: [
        { key: "avg_cpu_ms", label: "Avg CPU", color: SERIES_COLORS[0] },
        { key: "avg_elapsed_ms", label: "Avg Elapsed", color: SERIES_COLORS[1] },
      ],
      formatValue: (v) => Math.round(v).toLocaleString() + " ms",
      unit: "ms",
      ...(axis || {}),
    },
    "stats-history|" + key,
    chartZoomScope(hours)
  );
}

function panelFor(kind, key, state, hours, onMetric) {
  if (state.phase === "loading") return el("div", { class: "plan-panel stats-history" }, [strip("loading", "Loading the history...")]);
  const r = state.result;
  if (r.kind !== "data") return el("div", { class: "plan-panel stats-history" }, [strip(r.kind === "error" ? "error" : "empty", r.message)]);
  const d = r.data;
  const rows = Array.isArray(d.trend) ? d.trend : [];
  if (!rows.length) return el("div", { class: "plan-panel stats-history" }, [strip("empty", "No history was recorded for this row in the window.")]);
  const notes = [];
  if (d.window_truncated) {
    notes.push(
      "History starts " + d.effective_hours_back + " hours back, not the " + d.hours_back + " the window asked for: earlier samples have aged out of the tier that answered."
    );
  }
  if (d.aggregate_note) notes.push(d.aggregate_note);
  const parts = [notes.length ? strip("notice", notes.join(" ")) : null];
  if (kind === "procedures") {
    const select = el("select", { class: "stats-history-metric", "aria-label": "Metric" }, PROCEDURE_METRICS.map((m) => el("option", { value: m.value, text: m.label })));
    select.value = state.metric;
    select.addEventListener("change", () => onMetric(select.value));
    parts.push(el("div", { class: "picker-row" }, [select]));
  }
  parts.push(chartFor(kind, key, d, hours, state.window, state.metric));
  return el("div", { class: "plan-panel stats-history" }, parts);
}

/**
 * The History column: a button per row, and the panel under it while that row's history is open. A row without the keys the read
 * needs gets a dash. `kind` is "queries" or "procedures". The key is synthetic, so the column is never sorted, filtered, exported
 * or copied.
 */
export function statsHistoryColumn(kind, server, hours) {
  return {
    key: kind + "_history",
    label: "History",
    render: (row) => {
      const target = historyTarget(kind, row);
      if (!target) return document.createTextNode("—");
      const key = keyOf(kind, server, target);
      const host = el("div", { class: "plan-cell" });
      const draw = () => {
        while (host.firstChild) host.removeChild(host.firstChild);
        const state = open.get(key);
        const button = el("button", {
          type: "button",
          class: "grid-tool",
          text: state ? "Hide history" : "History",
          "aria-expanded": state ? "true" : "false",
          title: kind === "procedures" ? "Show this procedure's history" : "Show this query's history",
        });
        button.addEventListener("click", () => {
          if (open.has(key)) {
            open.delete(key);
            draw();
            return;
          }
          const stamp = stampOf(hours);
          open.set(key, { phase: "loading", stamp, window: windowFromHours(hours), metric: PROCEDURE_METRICS[0].value, redraw: draw });
          draw();
          load(kind, key, server, target, hours, stamp);
        });
        host.appendChild(button);
        if (state) {
          state.redraw = draw;
          dockPanelUnderRow(host, panelFor(kind, key, state, hours, (metric) => { state.metric = metric; draw(); }));
        } else undockPanel(host);
      };
      /* A rebuild under another window (a new preset or range) reads the open panel again. */
      const kept = open.get(key);
      if (kept && kept.stamp !== stampOf(hours)) {
        const stamp = stampOf(hours);
        open.set(key, { phase: "loading", stamp, window: windowFromHours(hours), metric: kept.metric, redraw: draw });
        load(kind, key, server, target, hours, stamp);
      } else if (kept) kept.redraw = draw;
      draw();
      return host;
    },
    sortable: false,
    filter: false,
    csv: false,
    copy: false,
  };
}
