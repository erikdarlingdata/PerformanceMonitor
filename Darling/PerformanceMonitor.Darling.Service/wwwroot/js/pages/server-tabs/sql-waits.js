/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, readToolWithinKeptHistory, keptWindowStrip, windowFloorStrip, mount, loadingStrip, errorStrip, readErrorStrip, emptyStrip, noticeStrip, chartNote, fmtInt, windowFromHours } from "../../util.js";
import { getPanelSignal, VIZ } from "../../panels.js";
import { zoomableLineChart, chartZoomScope, CATEGORICAL_COLORS } from "../../charts.js";
import { multiPicker, mergeSeriesRows } from "../../multi-picker.js";
import { panelShell, discontinuityNotes, withWaitLink, withPanelSignal } from "./shared.js";

/** The most waits the Wait Stats chart draws at once: one read each, and the palette has ten colors. */
const MAX_WAITS_CHARTED = 10;

/** The metrics the wait trend read carries. It has no waits-per-interval count, so the desktop's ms/wait is not offered. */
const WAIT_METRICS = [
  { value: "wait_time_ms_per_second", label: "Wait ms/s" },
  { value: "signal_wait_time_ms_per_second", label: "Signal wait ms/s" },
];

/**
 * Wait Stats table + a multi-series trend over the waits the reader checks.
 *
 * The desktop viewer's Wait Stats tab is a checkbox list of wait types over a multi-series chart; this is the same
 * idea. The picker's options are the rows of the table directly above it, heaviest first, so the reader is choosing
 * from what they can already see rather than from a second list that may disagree with it. That is also why
 * get_wait_types is NOT read here — it returns the full distinct set, which would offer wait types absent from the
 * table and make the two disagree. Each checked wait is one get_wait_trend read; the checked set, the search text and
 * the metric live in multi-picker.js's module state keyed by server, so the 60 s rebuild keeps them.
 */
export function waitsPanel(server, ctx) {
  const { panel, body } = panelShell("Wait Stats", ctx.label + ", with a trend for the waits you check", 2, "get_wait_stats");
  const signal = getPanelSignal();
  (async () => {
    const res = await readToolWithinKeptHistory("get_wait_stats", { server, hours: ctx.hours, limit: 20 }, signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, [keptWindowStrip(res), emptyStrip(res.message)]);

    const waits = res.data.waits || [];
    /* #4966: the Wait Stats grid says where its table's data starts when that is after the window's start. */
    const parts = [keptWindowStrip(res), windowFloorStrip(res.data, { viz: "table" }), VIZ.table(res.data, { rowsKey: "waits", columns: withWaitLink(WAIT_COLUMNS, server, null) })];

    if (waits.length) {
      const chartSlot = el("div", {}, [loadingStrip()]);
      const picker = multiPicker({
        key: "waits|" + server,
        label: "Waits",
        options: waits.map((w) => w.wait_type),
        max: MAX_WAITS_CHARTED,
        metrics: WAIT_METRICS,
        onChange: (checked, metric) => drawWaitTrends(chartSlot, server, ctx, checked, metric),
      });
      parts.push(picker.node, chartSlot);
      drawWaitTrends(chartSlot, server, ctx, picker.checked(), picker.metric());
      mount(body, parts);
      picker.restoreFocus();
      return;
    }
    mount(body, parts);
  })();
  return panel;
}

/* The newest draw's AbortController per server (the 60 s rebuild makes a new chart slot, so the slot is no key). A new
   draw aborts the previous one's reads, and a read that finishes after that is dropped. */
const waitDraws = new Map();

export async function drawWaitTrends(slot, server, ctx, checked, metric) {
  const waitTypes = checked.slice(0, MAX_WAITS_CHARTED);
  const previous = waitDraws.get(server);
  if (previous) previous.abort();
  const mine = new AbortController();
  waitDraws.set(server, mine);
  if (!waitTypes.length) {
    mount(slot, emptyStrip("Check at least one wait to chart its trend."));
    return;
  }
  mount(slot, loadingStrip());
  const results = await Promise.all(
    waitTypes.map((waitType) => readToolWithinKeptHistory("get_wait_trend", { server, wait_type: waitType, hours: ctx.hours }, mine.signal))
  );
  if (waitDraws.get(server) !== mine || mine.signal.aborted) return;

  const drawn = [];
  const notes = [];
  const kept = [];
  let keptHours = 0;
  const seenNotes = new Set();
  let failed = 0;
  results.forEach((trend, i) => {
    const waitType = waitTypes[i];
    if (trend.kind !== "data") {
      if (trend.kind !== "empty") failed++;
      notes.push(waitType + ": " + (trend.message || (trend.kind === "empty" ? "no trend data." : "the read failed.")));
      return;
    }
    if (trend.keptHours) {
      keptHours = Math.max(keptHours, trend.keptHours);
      kept.push(trend);
    }
    /* #3653 A5: wait_stats is the first identity-epoch carrier, so this is the chart whose step a restart or
       failover most directly manufactures; the payload's discontinuities render as a notice above it. They are
       about the server, not the wait, so the same sentence from two waits shows once. */
    for (const n of discontinuityNotes(trend.data)) seenNotes.add(n);
    drawn.push({ key: "w" + i, label: waitType, rows: trend.data.trend || [], color: CATEGORICAL_COLORS[i % CATEGORICAL_COLORS.length] });
  });

  const failures = notes.map((n) => noticeStrip(n));
  if (!drawn.length && !failed) {
    mount(slot, emptyStrip("None of the " + waitTypes.length + " checked waits has trend data in this window."));
    return;
  }
  if (!drawn.length) {
    mount(slot, [failures.length ? failures : null, errorStrip("None of the " + waitTypes.length + " checked waits returned a trend.")]);
    return;
  }
  const metricLabel = (WAIT_METRICS.find((m) => m.value === metric) || WAIT_METRICS[0]).label;
  mount(slot, [
    kept.length ? keptWindowStrip(kept[0]) : null,
    seenNotes.size ? noticeStrip([...seenNotes].join(" ")) : null,
    ...failures,
    zoomableLineChart({
      atTime: { server, item: "wait" },
      points: mergeSeriesRows(drawn, "time", metric),
      xKey: "time",
      series: drawn.map((d) => ({ key: d.key, label: d.label, color: d.color })),
      formatValue: (v, o) => fmtInt(v, o),
      unit: "ms/s",
      /* #2802: axis spans the requested window (ctx.hours ending now), not the data's own extent. A narrowed
         read spans the hours it answered for. */
      ...windowFromHours(keptHours || ctx.hours),
    }, "wait-trend|" + server + "|" + metric, chartZoomScope(ctx.hours)),
    el("div", { class: "mp-metric-note", text: metricLabel + ", one line per checked wait." }),
  ]);
}

/* ─────────────────────────── table columns ─────────────────────────── */

const WAIT_COLUMNS = [
  { key: "wait_type", label: "Wait Type" },
  { key: "total_wait_time_ms", label: "Total Wait", format: "ms" },
  { key: "resource_wait_ms", label: "Resource", format: "ms" },
  { key: "total_signal_wait_ms", label: "Signal", format: "ms" },
  { key: "waiting_tasks", label: "Tasks", format: "int" },
  { key: "signal_wait_pct", label: "Signal %", format: "num1" },
];

export const WAITING_TASK_COLUMNS = [
  { key: "collection_time", label: "Time", format: "time" },
  { key: "session_id", label: "SPID", format: "int" },
  { key: "wait_type", label: "Wait" },
  { key: "wait_duration_ms", label: "Duration", format: "ms" },
  { key: "blocking_session_id", label: "Blocked by", format: "int" },
  { key: "database_name", label: "Database" },
];

export const LATCH_COLUMNS = [
  { key: "latch_class", label: "Latch Class" },
  { key: "severity", label: "Severity", statusSev: true },
  { key: "total_delta_wait_time_ms", label: "Wait", format: "ms" },
  { key: "total_delta_waiting_requests", label: "Requests", format: "int" },
  { key: "avg_wait_ms_per_request", label: "Avg/req", format: "num2" },
  /* #5644 W10: the panel reads "last 24 hours" and the totals are, but this rate is the latest interval's only. */
  { key: "wait_ms_per_second", label: "ms/s, latest interval", format: "num2" },
  { key: "description", label: "What it means", wrap: true },
];

export const SPINLOCK_COLUMNS = [
  { key: "spinlock_name", label: "Spinlock" },
  { key: "total_delta_collisions", label: "Collisions", format: "int" },
  { key: "total_delta_spins", label: "Spins", format: "int" },
  { key: "total_delta_backoffs", label: "Backoffs", format: "int" },
  { key: "spins_per_collision", label: "Spins/coll", format: "num1" },
  { key: "collisions_per_second", label: "Coll/s, latest interval", format: "num2" },
  { key: "description", label: "What it means", wrap: true },
];

/**
 * The latch and spinlock trends get_server_trend serves as one line per name (#5170). The options are the classes of the
 * table above (heaviest first, read with the same window); the heaviest five are checked the first time, as the read's
 * own default does. The checked set and search text live in multi-picker.js's module state keyed by kind and server, so
 * the 60 s rebuild keeps them.
 */
const NAMED_TRENDS = {
  latch: {
    title: "Latch Trend",
    optionsTool: "get_latch_stats",
    rowsKey: "latches",
    nameKey: "latch_class",
    seriesKey: "latch_class",
    unit: "ms/s",
    noun: "latch class",
    plural: "latch classes",
    label: "Latch classes",
    note: "Wait ms/s, one line per checked latch class.",
    noOptions: "No latch classes accumulated waits in this window.",
  },
  spinlock: {
    title: "Spinlock Trend",
    optionsTool: "get_spinlock_stats",
    rowsKey: "spinlocks",
    nameKey: "spinlock_name",
    seriesKey: "spinlock_name",
    unit: "collisions/s",
    noun: "spinlock",
    plural: "spinlocks",
    label: "Spinlocks",
    note: "Collisions/s, one line per checked spinlock.",
    noOptions: "No spinlocks recorded collisions in this window.",
  },
};
const MAX_NAMES_CHARTED = 10;
const DEFAULT_NAMES_CHECKED = 5;

export function namedTrendPanel(server, ctx, kind) {
  ctx = withPanelSignal(ctx);
  const spec = NAMED_TRENDS[kind];
  const { panel, body } = panelShell(spec.title, ctx.label + ", with a trend for the " + spec.plural + " you check", 2, spec.optionsTool);
  (async () => {
    const res = await readToolWithinKeptHistory(spec.optionsTool, { server, hours: ctx.hours, top: MAX_NAMES_CHARTED }, ctx && ctx.signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, [keptWindowStrip(res), emptyStrip(res.message || spec.noOptions)]);
    const options = (res.data[spec.rowsKey] || []).map((r) => r[spec.nameKey]).filter(Boolean);
    if (!options.length) return mount(body, emptyStrip(spec.noOptions));

    const chartSlot = el("div", {}, [loadingStrip()]);
    const picker = multiPicker({
      key: kind + "|" + server,
      label: spec.label,
      options,
      max: MAX_NAMES_CHARTED,
      noun: spec.noun,
      defaultsLabel: "Top " + spec.plural,
      defaults: (o) => o.slice(0, DEFAULT_NAMES_CHECKED),
      onChange: (checked) => drawNamedTrends(chartSlot, server, ctx, kind, checked),
    });
    mount(body, [picker.node, chartSlot]);
    drawNamedTrends(chartSlot, server, ctx, kind, picker.checked());
    picker.restoreFocus();
  })();
  return panel;
}

/* The newest named-trend draw's AbortController per kind and server: a new draw aborts the previous one's read. */
const namedDraws = new Map();

export async function drawNamedTrends(slot, server, ctx, kind, checked) {
  const spec = NAMED_TRENDS[kind];
  const names = checked.slice(0, MAX_NAMES_CHARTED);
  const drawKey = kind + "|" + server;
  const previous = namedDraws.get(drawKey);
  if (previous) previous.abort();
  const mine = new AbortController();
  namedDraws.set(drawKey, mine);
  if (!names.length) {
    mount(slot, emptyStrip("Check at least one " + spec.noun + " to chart its trend."));
    return;
  }
  mount(slot, loadingStrip());
  const signal = ctx && ctx.signal && typeof AbortSignal !== "undefined" && AbortSignal.any ? AbortSignal.any([mine.signal, ctx.signal]) : mine.signal;
  const trend = await readToolWithinKeptHistory("get_server_trend", { server, metric: kind, hours: ctx.hours, names: names.join(",") }, signal);
  if (namedDraws.get(drawKey) !== mine || mine.signal.aborted || trend.kind === "aborted") return;

  if (trend.kind === "error") return mount(slot, readErrorStrip(trend.message));
  /* The read names the series it found no samples for under missing_names at the top level on a partial match and
     under hints on an empty answer. */
  const missingOf = (data) => {
    const top = data && Array.isArray(data.missing_names) ? data.missing_names : [];
    const hinted = data && data.hints && Array.isArray(data.hints.missing_names) ? data.hints.missing_names : [];
    return [...new Set([...top, ...hinted])];
  };
  const missingStrip = (data) => (missingOf(data).length ? noticeStrip("No samples in this window for: " + missingOf(data).join(", ") + ".") : null);
  if (trend.kind === "empty") {
    return mount(slot, [keptWindowStrip(trend), missingStrip(trend.hints || {}), emptyStrip(trend.message || "No " + spec.noun + " samples in this window.")]);
  }

  const drawn = (trend.data.series || []).map((s, i) => ({
    key: "n" + i,
    label: s[spec.seriesKey],
    rows: s.trend || [],
    color: CATEGORICAL_COLORS[i % CATEGORICAL_COLORS.length],
  }));
  if (!drawn.length) return mount(slot, [keptWindowStrip(trend), missingStrip(trend.data), emptyStrip("No " + spec.noun + " samples in this window.")]);
  /* #3653 A5: the payload's baseline discontinuities as a notice above the chart. */
  const notes = discontinuityNotes(trend.data);
  mount(slot, [
    keptWindowStrip(trend),
    notes.length ? noticeStrip(notes.join(" ")) : null,
    missingStrip(trend.data),
    zoomableLineChart({
      atTime: { server },
      points: mergeSeriesRows(drawn, "time", kind === "latch" ? "wait_time_ms_per_second" : "collisions_per_second"),
      xKey: "time",
      series: drawn.map((d) => ({ key: d.key, label: d.label, color: d.color })),
      formatValue: (v) => (Math.round(v * 100) / 100).toLocaleString(),
      unit: spec.unit,
      ...windowFromHours(trend.keptHours || ctx.hours),
    }, kind + "-trend|" + server, chartZoomScope(ctx.hours)),
    el("div", { class: "mp-metric-note", text: spec.note }),
    chartNote(trend.data, "summed"),
  ]);
}

