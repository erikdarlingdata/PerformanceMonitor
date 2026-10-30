/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The Memory tab's two memory-grant charts (#6299): grant sizing and grant activity over the page's window, the web twin of the
 * desktop viewer's Memory Grants charts. They draw `get_memory_grants` with `chart: true`: one point per bucket per resource pool
 * across the whole window, never the newest snapshot's rows (#3653 took the old chart out because it drew that one snapshot as a
 * window trend; the series here has a collection time of its own on every point).
 *
 * Zero handling is the desktop's. A snapshot with no grants is a point of zeros and draws as a 0 on the line, because the collector
 * did run and there were none. A window with no collected snapshot has no points at all, and the panel says so in words: it never
 * draws a line of zeros for time nobody sampled. The read's own empty or unavailable answer is that sentence.
 */

import { el, readToolWithinKeptHistory, keptWindowStrip, mount, loadingStrip, readErrorStrip, emptyStrip, chartNote, chartNoteText, windowFromHours, fmtInt, dbScopeChip } from "../util.js";
import { zoomableLineChart, chartZoomScope, CATEGORICAL_COLORS } from "../charts.js";
import { emptyHeadline } from "../panels.js";

/* The desktop's sizing metrics. The two ceilings are the workspace-memory limit, not measured series. */
export const GRANT_SIZING_METRICS = [
  { key: "available_memory_mb", label: "Available MB" },
  { key: "granted_memory_mb", label: "Granted MB" },
  { key: "used_memory_mb", label: "Used MB" },
  { key: "target_memory_mb", label: "Target MB (ceiling)" },
  { key: "max_target_memory_mb", label: "Max Target MB (ceiling)" },
];

/* The desktop's activity metrics: two levels (grantees, waiters) and two per-interval counts (timeouts, forced grants). */
export const GRANT_ACTIVITY_METRICS = [
  { key: "grantee_count", label: "Grantees" },
  { key: "waiter_count", label: "Waiters" },
  { key: "timeout_error_count_delta", label: "Timeouts" },
  { key: "forced_grant_count_delta", label: "Forced Grants" },
];

/** The activity chart's note: the width, then which series are averaged and which summed (#6359); null when the read states no width. */
function mixedNote(data) {
  const width = chartNoteText(data);
  if (!width) return null;
  return el("div", { class: "mp-metric-note", text: width === "Each point is one collection." ? width : width + " Grantees and waiters are averaged over the bucket; timeouts and forced grants are summed." });
}

/**
 * Shape the read's entries ({ collection_time, pool_id, ...metrics }) into one chart's points and series: a line per pool
 * per metric, "Pool N: Metric", each pool x metric on its own colour in the order the desktop assigns them (pool order, then
 * metric order). A pool with no entry at a time has no value there, so its line has a gap and is not drawn to 0. Entries that
 * carry no time, or a non-number value, are dropped. An empty `entries` gives no points and no series.
 */
export function grantChartSeries(entries, metrics) {
  const rows = (Array.isArray(entries) ? entries : []).filter((r) => r && r.collection_time != null);
  const pools = [...new Set(rows.map((r) => Number(r.pool_id) || 0))].sort((a, b) => a - b);
  const byTime = new Map();
  const series = [];
  pools.forEach((pool) => {
    metrics.forEach((m) => {
      const key = "p" + pool + "_" + m.key;
      series.push({ key, label: "Pool " + pool + ": " + m.label, color: CATEGORICAL_COLORS[series.length % CATEGORICAL_COLORS.length] });
      for (const r of rows) {
        if ((Number(r.pool_id) || 0) !== pool || typeof r[m.key] !== "number") continue;
        let point = byTime.get(r.collection_time);
        if (!point) {
          point = { collection_time: r.collection_time };
          byTime.set(r.collection_time, point);
        }
        point[key] = r[m.key];
      }
    });
  });
  const points = [...byTime.values()].sort((a, b) => (a.collection_time < b.collection_time ? -1 : a.collection_time > b.collection_time ? 1 : 0));
  return { points, series };
}

function chartPanel(title, subtitle) {
  const body = el("div", { class: "panel-body" }, [loadingStrip()]);
  /* `data-panel` / `data-title` are the hooks the web UI smoke check finds a panel by (#5701). */
  const panel = el("div", { class: "panel card span-2", "data-panel": "get_memory_grants", "data-title": title }, [
    el("h3", {}, [title, el("span", { class: "panel-sub", text: " " + subtitle }), dbScopeChip("get_memory_grants")]),
    body,
  ]);
  return { panel, body };
}

const SIZING_EMPTY = "No memory grant snapshots in this window, so there is no sizing to chart.";
const ACTIVITY_EMPTY = "No memory grant snapshots in this window, so there is no activity to chart.";

/**
 * The two chart panels, [sizing, activity], over ONE read of get_memory_grants with chart: true. The tables beside them keep
 * their own reads (grants[] and window[]). `ctx` is the page's `{ hours, label, signal }`.
 */
export function memoryGrantChartPanels(server, ctx) {
  const sizing = chartPanel("Memory Grant Sizing", ctx.label + ", every snapshot in the window, per resource pool");
  const activity = chartPanel("Memory Grant Activity", ctx.label + ", every snapshot in the window, per resource pool");
  (async () => {
    const res = await readToolWithinKeptHistory("get_memory_grants", { server, hours: ctx.hours, chart: true }, ctx && ctx.signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") {
      mount(sizing.body, readErrorStrip(res.message));
      mount(activity.body, readErrorStrip(res.message));
      return;
    }
    const drawAll = [
      [sizing, GRANT_SIZING_METRICS, SIZING_EMPTY, "grant-sizing", "MB"],
      [activity, GRANT_ACTIVITY_METRICS, ACTIVITY_EMPTY, "grant-activity", "count"],
    ];
    for (const [target, metrics, emptyText, id, unit] of drawAll) {
      const spec = { emptyText };
      if (res.kind === "empty") {
        mount(target.body, [keptWindowStrip(res), emptyStrip(emptyHeadline(spec, res) || emptyText)]);
        continue;
      }
      const { points, series } = grantChartSeries(res.data.entries, metrics);
      if (!points.length) {
        mount(target.body, [keptWindowStrip(res), emptyStrip(emptyText)]);
        continue;
      }
      const counts = unit === "count";
      mount(target.body, [
        keptWindowStrip(res),
        zoomableLineChart({
          atTime: { server },
          points,
          xKey: "collection_time",
          series,
          formatValue: counts ? (v, o) => fmtInt(v, o) : (v) => (Math.round(v * 100) / 100).toLocaleString() + " MB",
          unit,
          integerTicks: counts,
          ...windowFromHours(res.keptHours || ctx.hours),
        }, id + "|" + server, chartZoomScope(ctx.hours)),
        /* #6359: said from the read's width fields, not the read's MCP-facing `aggregate_note`. The sizing chart is all levels
           (each point the bucket's mean); the activity chart mixes two levels with two summed counts, so it says so. */
        counts ? mixedNote(res.data) : chartNote(res.data, "level"),
      ]);
    }
  })();
  return [sizing.panel, activity.panel];
}
