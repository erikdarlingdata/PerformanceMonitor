/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, readToolWithinKeptHistory, keptWindowStrip, windowFloorStrip, mount, loadingStrip, readErrorStrip, emptyStrip, noticeStrip, chartNote, parseUtc, windowFromHours } from "../../util.js";
import { getPanelSignal, VIZ } from "../../panels.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS, CATEGORICAL_COLORS } from "../../charts.js";
import { multiPicker, mergeSeriesRows } from "../../multi-picker.js";
import { panelShell, discontinuityNotes, renderInto, AZURE_SQL_DATABASE, withPanelSignal } from "./shared.js";

/* On an Azure SQL Database (engine_edition 5) the collector fills total_physical_memory_mb from the database's own committed
   target and available_physical_memory_mb as that target minus what is committed, so the first two tiles are the database's
   memory limit and the room left under it, not the host's RAM, and are named that way. */
export const MEMORY_STATS = [
  { key: "total_physical_memory_mb", label: "Physical", format: "mb", hideWhen: AZURE_SQL_DATABASE },
  { key: "total_physical_memory_mb", label: "Memory limit", format: "mb", showWhen: AZURE_SQL_DATABASE },
  { key: "available_physical_memory_mb", label: "Available", format: "mb", hideWhen: AZURE_SQL_DATABASE },
  { key: "available_physical_memory_mb", label: "Available under limit", format: "mb", showWhen: AZURE_SQL_DATABASE },
  { key: "memory_utilization_pct", label: "Utilization", format: "pct" },
  { key: "total_server_memory_mb", label: "Total server", format: "mb" },
  { key: "target_server_memory_mb", label: "Target server", format: "mb" },
  { key: "buffer_pool_mb", label: "Buffer pool", format: "mb" },
  { key: "plan_cache_mb", label: "Plan cache (all cache stores)", format: "mb" },
  { key: "system_memory_state", label: "System state", format: "text", small: true, nullKey: "system_memory_state_note" },
  { key: "sql_memory_model", label: "Memory model", format: "text", small: true },
];

export const PLAN_CACHE_STATS = [
  { key: "summary.bloat_level", label: "Bloat", format: "text", small: true },
  { key: "summary.total_plans", label: "Plans", format: "int" },
  { key: "summary.single_use_plans", label: "Single-use", format: "int" },
  { key: "summary.single_use_percent", label: "Single-use %", format: "num1" },
  { key: "summary.total_size_mb", label: "Cached plans size", format: "mb" },
  { key: "summary.single_use_size_mb", label: "Single-use size", format: "mb" },
  { key: "summary.wasted_percent", label: "Wasted %", format: "num1" },
  { key: "summary.bloat_recommendation", label: "Recommendation", format: "text", small: true },
];

export const MEMORY_SERIES = [
  { key: "total_server_memory_mb", label: "Total Server" },
  { key: "target_server_memory_mb", label: "Target" },
  { key: "buffer_pool_mb", label: "Buffer Pool" },
  { key: "plan_cache_mb", label: "Plan Cache" },
];

export const CLERK_COLUMNS = [
  { key: "clerk_type", label: "Clerk" },
  { key: "memory_mb", label: "Memory", format: "mb" },
];

/* #3653: the `window[]` half of the two memory-grant reads (#3637) - one row per pool aggregating EVERY
   snapshot in the window. Peak waiters and its instant lead because "how many sessions ever queued for a
   grant, and when" is the question the window exists to answer; the sums come last because they are the
   per-interval deltas added up across the window, not a counter read at one instant. `snapshots_in_window`
   is the denominator that says how much of the window the peaks were taken over. */
export const GRANT_WINDOW_COLUMNS = [
  { key: "pool_id", label: "Pool", format: "int" },
  { key: "snapshots_in_window", label: "Snapshots", format: "int" },
  { key: "peak_waiter_count", label: "Peak waiters", format: "int" },
  { key: "peak_waiters_at", label: "Peak waiters at", format: "time" },
  { key: "peak_granted_memory_mb", label: "Peak granted", format: "mb" },
  { key: "peak_granted_at", label: "Peak granted at", format: "time" },
  { key: "min_available_memory_mb", label: "Min available", format: "mb" },
  { key: "timeout_errors_in_window", label: "Timeouts", format: "int" },
  { key: "forced_grants_in_window", label: "Forced grants", format: "int" },
  { key: "first_snapshot_at", label: "First", format: "time", small: true },
  { key: "last_snapshot_at", label: "Last", format: "time", small: true },
];

/* The semaphore lens carries the same window shape keyed one level finer; the pool lens's rows carry a null
   semaphore id by design, so the column is added here rather than shared and rendered as a dash there. */
export const SEMAPHORE_WINDOW_COLUMNS = [{ key: "resource_semaphore_id", label: "Semaphore", format: "int" }, ...GRANT_WINDOW_COLUMNS];

/* The `grants[]` half of get_memory_grants: the NEWEST snapshot, one row per pool with its semaphores summed.
   Waiters lead the deltas: the deltas are the interval ending at this snapshot, the waiter count is the
   instant itself. */
export const GRANT_COLUMNS = [
  { key: "collection_time", label: "Time", format: "time" },
  { key: "pool_id", label: "Pool", format: "int" },
  { key: "granted_memory_mb", label: "Granted", format: "mb" },
  { key: "used_memory_mb", label: "Used", format: "mb" },
  { key: "available_memory_mb", label: "Available", format: "mb" },
  { key: "grantee_count", label: "Grantees", format: "int" },
  { key: "waiter_count", label: "Waiters", format: "int" },
  { key: "timeout_error_count_delta", label: "Timeouts", format: "int" },
  { key: "forced_grant_count_delta", label: "Forced grants", format: "int" },
];

export const SEMAPHORE_COLUMNS = [
  { key: "collection_time", label: "Time", format: "time" },
  { key: "pool_id", label: "Pool", format: "int" },
  { key: "target_memory_mb", label: "Target", format: "mb" },
  { key: "total_memory_mb", label: "Total", format: "mb" },
  { key: "granted_memory_mb", label: "Granted", format: "mb" },
  { key: "used_memory_mb", label: "Used", format: "mb" },
  { key: "available_memory_mb", label: "Available", format: "mb" },
  { key: "grantee_count", label: "Grantees", format: "int" },
  { key: "waiter_count", label: "Waiters", format: "int" },
  { key: "timeout_error_count_delta", label: "Timeouts", format: "int" },
  { key: "forced_grant_count_delta", label: "Forced grants", format: "int" },
];

const PRESSURE_COLUMNS = [
  { key: "sample_time", label: "Time", format: "time" },
  { key: "memory_notification", label: "Notification" },
  { key: "memory_indicators_process", label: "Process", format: "int" },
  { key: "memory_indicators_system", label: "System", format: "int" },
];

/** Both memory-pressure severities as the desktop chart counts them: indicator 2 is medium, 3 and above severe. */
const PRESSURE_LINES = [
  { key: "sql_medium", label: "SQL Server (medium)", color: SERIES_COLORS[0] },
  { key: "sql_severe", label: "SQL Server (severe)", color: SERIES_COLORS[1] },
  { key: "os_medium", label: "Operating System (medium)", color: SERIES_COLORS[2] },
  { key: "os_severe", label: "Operating System (severe)", color: SERIES_COLORS[3] },
];

const HOUR_MS = 3600000;

/** Said wherever an hour reads as quiet: the read does not say where collection began, so a blank hour may predate it. */
const PRESSURE_COVERAGE_CAVEAT = "No bar means no event was recorded, which for a server monitored for less than this range includes the hours before collection began.";

/**
 * The pressure events counted per hour, the way the desktop chart counts them: only samples where SQL Server or the
 * operating system reads 2 or more are drawn, and each hour holds a count per source and severity. Every hour of the
 * window is present, an hour with no event as 0, which the stacked-bar chart draws as no bar, as the desktop does. `events` are the read's rows (sample_time is naive UTC); the window is `{ windowStart, windowEnd }` in
 * epoch ms. Returns `{ points, drawn }`: the hourly rows (`time` is naive-UTC ISO text) and the number of samples counted.
 */
export function pressureEventBuckets(events, win) {
  const counts = new Map();
  let drawn = 0;
  for (const e of Array.isArray(events) ? events : []) {
    const at = parseUtc(e && e.sample_time);
    if (!at) continue;
    const process = Number(e.memory_indicators_process);
    const system = Number(e.memory_indicators_system);
    if (!(process >= 2 || system >= 2)) continue;
    drawn++;
    const hour = Math.floor(at.getTime() / HOUR_MS) * HOUR_MS;
    const row = counts.get(hour) || { sql_medium: 0, sql_severe: 0, os_medium: 0, os_severe: 0 };
    if (process === 2) row.sql_medium++;
    else if (process >= 3) row.sql_severe++;
    if (system === 2) row.os_medium++;
    else if (system >= 3) row.os_severe++;
    counts.set(hour, row);
  }
  const hours = new Set(counts.keys());
  if (win) {
    for (let h = Math.floor(win.windowStart / HOUR_MS) * HOUR_MS; h <= win.windowEnd; h += HOUR_MS) hours.add(h);
  }
  const points = [...hours]
    .sort((a, b) => a - b)
    .map((h) => ({ time: new Date(h).toISOString().slice(0, 19), ...(counts.get(h) || { sql_medium: 0, sql_severe: 0, os_medium: 0, os_severe: 0 }) }));
  return { points, drawn };
}

/** The most clerk types get_server_trend takes in one read (its own cap), and how many the desktop checks to begin with. */
const MAX_CLERKS_CHARTED = 10;
const DEFAULT_CLERKS_CHECKED = 5;

/**
 * Memory Clerks trend with a clerk-type selector, the web twin of the desktop's clerk picker. The options are the clerks
 * of the latest snapshot (get_memory_clerks, heaviest first); the first time a server is shown the heaviest five are
 * checked, as the desktop does. The checked set and search text live in multi-picker.js's module state keyed by server (the first show and the Top clerks button both check the heaviest five),
 * so the 60 s rebuild keeps them.
 */
export function memoryClerksTrendPanel(server, ctx) {
  ctx = withPanelSignal(ctx);
  const { panel, body } = panelShell("Memory Clerks Trend", ctx.label + ", with a trend for the clerks you check", 2, "get_memory_clerks");
  (async () => {
    const res = await readToolWithinKeptHistory("get_memory_clerks", { server }, ctx && ctx.signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, [keptWindowStrip(res), emptyStrip(res.message)]);
    const options = (res.data.clerks || []).map((c) => c.clerk_type);
    if (!options.length) return mount(body, emptyStrip("No memory clerks in the latest snapshot — the clerk collector may not have run yet."));

    const key = "clerks|" + server;
    const chartSlot = el("div", {}, [loadingStrip()]);
    const picker = multiPicker({
      key,
      label: "Clerks",
      options,
      max: MAX_CLERKS_CHARTED,
      noun: "clerk",
      defaultsLabel: "Top clerks",
      defaults: (o) => o.slice(0, DEFAULT_CLERKS_CHECKED),
      onChange: (checked) => drawClerkTrends(chartSlot, server, ctx, checked),
    });
    mount(body, [picker.node, chartSlot]);
    drawClerkTrends(chartSlot, server, ctx, picker.checked());
    picker.restoreFocus();
  })();
  return panel;
}

/* The newest clerk draw's AbortController per server: a new draw aborts the previous one's read. */
const clerkDraws = new Map();

export async function drawClerkTrends(slot, server, ctx, checked) {
  const clerkTypes = checked.slice(0, MAX_CLERKS_CHARTED);
  const previous = clerkDraws.get(server);
  if (previous) previous.abort();
  const mine = new AbortController();
  clerkDraws.set(server, mine);
  if (!clerkTypes.length) {
    mount(slot, emptyStrip("Check at least one clerk to chart its trend."));
    return;
  }
  mount(slot, loadingStrip());
  const signal = ctx && ctx.signal && typeof AbortSignal !== "undefined" && AbortSignal.any ? AbortSignal.any([mine.signal, ctx.signal]) : mine.signal;
  const trend = await readToolWithinKeptHistory(
    "get_server_trend",
    { server, metric: "memory_clerks", hours: ctx.hours, clerk_types: clerkTypes.join(",") },
    signal
  );
  if (clerkDraws.get(server) !== mine || mine.signal.aborted || trend.kind === "aborted") return;

  if (trend.kind === "error") return mount(slot, readErrorStrip(trend.message));
  const missingOf = (data) => (data && Array.isArray(data.missing_clerk_types) ? data.missing_clerk_types : []);
  const missingStrip = (data) => (missingOf(data).length ? noticeStrip("No samples in this window for: " + missingOf(data).join(", ") + ".") : null);
  if (trend.kind === "empty") {
    /* The empty envelope carries its lists under hints; the server's sentence points at them as JSON paths a
       reader cannot open, so they are shown as text. */
    const hints = trend.hints || {};
    const heaviest = Array.isArray(hints.heaviest_clerk_types) ? hints.heaviest_clerk_types : [];
    return mount(slot, [
      keptWindowStrip(trend),
      missingStrip(hints),
      emptyStrip(trend.message || "No clerk samples in this window."),
      heaviest.length ? el("div", { class: "mp-metric-note", text: "Heaviest clerks in this window: " + heaviest.join(", ") + "." }) : null,
    ]);
  }

  const drawn = (trend.data.series || []).map((s, i) => ({
    key: "c" + i,
    label: s.clerk_type,
    rows: s.trend || [],
    color: CATEGORICAL_COLORS[i % CATEGORICAL_COLORS.length],
  }));
  if (!drawn.length) return mount(slot, [keptWindowStrip(trend), missingStrip(trend.data), emptyStrip("No clerk samples in this window.")]);
  /* #3653 A5: the payload's baseline discontinuities as a notice above the chart. */
  const notes = discontinuityNotes(trend.data);
  mount(slot, [
    keptWindowStrip(trend),
    notes.length ? noticeStrip(notes.join(" ")) : null,
    missingStrip(trend.data),
    zoomableLineChart({
      atTime: { server },
      points: mergeSeriesRows(drawn, "time", "memory_mb"),
      xKey: "time",
      series: drawn.map((d) => ({ key: d.key, label: d.label, color: d.color })),
      formatValue: (v) => (Math.round(v * 100) / 100).toLocaleString(),
      unit: "MB",
      ...windowFromHours(trend.keptHours || ctx.hours),
    }, "clerk-trend|" + server, chartZoomScope(ctx.hours)),
    chartNote(trend.data, "level"),
  ]);
}

/**
 * The Memory tab's pressure events: an hourly count chart over the read's rows and the rows themselves, from ONE
 * fetch. The read returns every sample in the window (it has no row cap), so the counts are complete for what the
 * store holds. It does not say where collection began, so the notice says that a 0 hour is an hour with no event
 * recorded, which on a server monitored for less than the range includes the hours before collection started.
 */
export function memoryPressurePanels(server, ctx) {
  const chart = panelShell("Memory Pressure Events per Hour", ctx.label, 2, "get_memory_pressure_events");
  const grid = panelShell("Memory Pressure Events", ctx.label, 1, "get_memory_pressure_events");
  const signal = getPanelSignal();
  (async () => {
    const res = await readToolWithinKeptHistory("get_memory_pressure_events", { server, hours: ctx.hours }, signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") {
      mount(chart.body, readErrorStrip(res.message));
      return mount(grid.body, readErrorStrip(res.message));
    }
    const empty = "No memory pressure events in this window — the healthy state for this read. " + PRESSURE_COVERAGE_CAVEAT;
    if (res.kind === "empty") {
      mount(chart.body, [keptWindowStrip(res), emptyStrip(empty)]);
      return mount(grid.body, [keptWindowStrip(res), windowFloorStrip(res.data, { viz: "table" }), emptyStrip(res.message)]);
    }
    renderInto(chart.body, () => {
      const hours = res.keptHours || ctx.hours;
      const { points, drawn } = pressureEventBuckets(res.data.events, windowFromHours(hours));
      if (!drawn) return [keptWindowStrip(res), emptyStrip("No sample in this window reached medium pressure (an indicator of 2 or more); the table beside this lists every sample the collector stored. " + PRESSURE_COVERAGE_CAVEAT)];
      const lines = PRESSURE_LINES.filter((l) => points.some((p) => p[l.key] > 0));
      return [
        keptWindowStrip(res),
        noticeStrip(
          "Samples where an indicator reads 2 or more, counted per hour as stacked bars; an hour with none draws no bar. " + PRESSURE_COVERAGE_CAVEAT
        ),
        zoomableLineChart({
          atTime: { server },
          points,
          xKey: "time",
          series: lines,
          formatValue: (v) => String(Math.round(v)),
          integerTicks: true,
          mode: "stacked-bar",
          unit: "events",
          ...windowFromHours(hours),
        }, "memory-pressure-events", chartZoomScope(ctx.hours)),
      ];
    });
    renderInto(grid.body, () => [
      keptWindowStrip(res),
      windowFloorStrip(res.data, { viz: "table" }),
      VIZ.table(res.data, { rowsKey: "events", columns: PRESSURE_COLUMNS, emptyText: empty }),
    ]);
  })();
  return [chart.panel, grid.panel];
}

export const CACHE_TYPE_COLUMNS = [
  { key: "cache_type", label: "Cache" },
  { key: "object_type", label: "Object type" },
  { key: "total_plans", label: "Plans", format: "int" },
  { key: "total_size_mb", label: "Size", format: "mb" },
  { key: "single_use_plans", label: "Single-use", format: "int" },
  { key: "single_use_size_mb", label: "Single-use size", format: "mb" },
  { key: "avg_use_count", label: "Avg uses", format: "num1" },
];

