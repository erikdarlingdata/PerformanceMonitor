/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, readTool, readToolWithinKeptHistory, keptWindowStrip, mount, loadingStrip, errorStrip, readErrorStrip, emptyStrip, noticeStrip, chartNote, fmtRate, fmtInt, fmtNum, fmtPlain, windowFromHours, clampText } from "../../util.js";
import { estimatedPlotPx } from "../../trend-points.js";
import { getPanelSignal, VIZ } from "../../panels.js";
import { zoomableLineChart, chartZoomScope, CATEGORICAL_COLORS } from "../../charts.js";
import { multiPicker, mergeSeriesRows } from "../../multi-picker.js";
import { panelShell, discontinuityStrip, withPanelSignal } from "./shared.js";

/** The most counters the Perfmon chart draws at once: one read each, and the palette has ten colors. */
const MAX_COUNTERS_CHARTED = 10;

/** The desktop's General Throughput pack: the counters checked when the reader has not chosen any. */
const PERFMON_DEFAULT_COUNTERS = [
  "Batch Requests/sec",
  "SQL Compilations/sec",
  "SQL Re-Compilations/sec",
  "Query optimizations/sec",
  "Network IO waits",
];

/** The default counter set, bounded by `max` and drawn only from the counters this server holds (case-insensitive,
    as the desktop matches them); when none of the pack is collected, the first counter in the list. */
function perfmonDefaults(available, max) {
  const picked = [];
  for (const want of PERFMON_DEFAULT_COUNTERS) {
    const hit = available.find((n) => n.toLowerCase() === want.toLowerCase());
    if (hit && picked.length < max && !picked.includes(hit)) picked.push(hit);
  }
  return picked.length || !available.length ? picked : [available[0]];
}

/**
 * Perfmon counters: the latest snapshot's table, and a multi-series trend over the counters the reader checks.
 *
 * The desktop viewer's Perfmon tab is a searchable counter list over a multi-series chart, opening on the General
 * Throughput pack. get_perfmon_trend REQUIRES a counter_name, so the options come from get_perfmon_stats (the latest
 * snapshot's counter list) rather than being hardcoded: a hardcoded name is exactly how you end up charting a counter
 * this server does not collect. The trend read takes a counter name and no instance, so the list is by counter name.
 * Each checked counter is one get_perfmon_trend read; the checked set and the search text live in multi-picker.js's
 * module state keyed by server, so the 60 s rebuild keeps them.
 */
export function perfmonPanel(server, ctx) {
  const { panel, body } = panelShell("Perfmon Counters", "latest snapshot, with a trend for the counters you check", 2, "get_perfmon_stats");
  const signal = getPanelSignal();
  (async () => {
    const res = await readTool("get_perfmon_stats", { server }, signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, emptyStrip(res.message));

    /* #7550: the store sends these in its name order without case, but the picker must not depend on reply order, so the page
       sorts the same way: names folded to lower case (A-Z only, as the store's lower() does), then the raw name. A plain .sort()
       put every capitalized counter above every lower-case one. */
    const fold = (n) => n.replace(/[A-Z]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) + 32));
    const byName = (a, b) => { const x = fold(a), y = fold(b); return x < y ? -1 : x > y ? 1 : a < b ? -1 : a > b ? 1 : 0; };
    const names = [...new Set((res.data.counters || []).map((c) => c.counter_name).filter(Boolean))].sort(byName);
    if (!names.length) return mount(body, emptyStrip("The latest snapshot holds no perfmon counters."));

    /* The snapshot table lives HERE rather than in its own descriptor: this composite has already fetched the
       exact payload it would render, and a second panel would have paid for get_perfmon_stats twice to show the
       list the picker above it is built from. */
    const chartSlot = el("div", {}, [loadingStrip()]);
    const picker = multiPicker({
      key: "perfmon|" + server,
      label: "Counters",
      options: names,
      max: MAX_COUNTERS_CHARTED,
      noun: "counter",
      defaultsLabel: "Default counters",
      defaults: perfmonDefaults,
      onChange: (checked) => drawPerfmonTrends(chartSlot, server, ctx, checked),
    });
    mount(body, [
      VIZ.table({ ...res.data, counters: perfmonRows(res.data.counters) }, {
        rowsKey: "counters",
        columns: perfmonColumns(res.data.counters),
        emptyText: "No perfmon counters in the latest snapshot.",
      }),
      picker.node,
      chartSlot,
    ]);
    picker.restoreFocus();
    drawPerfmonTrends(chartSlot, server, ctx, picker.checked());
  })();
  return panel;
}

/* The newest Perfmon draw's AbortController per server: a new draw aborts the previous one's reads, and a read that
   finishes after that is dropped. */
const perfmonDraws = new Map();

export async function drawPerfmonTrends(slot, server, ctx, checked) {
  const counters = checked.slice(0, MAX_COUNTERS_CHARTED);
  const previous = perfmonDraws.get(server);
  if (previous) previous.abort();
  const mine = new AbortController();
  perfmonDraws.set(server, mine);
  if (!counters.length) {
    mount(slot, emptyStrip("Check at least one counter to chart its trend."));
    return;
  }
  mount(slot, loadingStrip());
  const results = await Promise.all(
    counters.map((counter) => readToolWithinKeptHistory("get_perfmon_trend", { server, counter_name: counter, hours: ctx.hours }, mine.signal, { plotPx: estimatedPlotPx() }))
  );
  if (perfmonDraws.get(server) !== mine || mine.signal.aborted) return;

  const drawn = [];
  const notes = [];
  const kept = [];
  let keptHours = 0;
  const seenSteps = new Map();
  let failed = 0;
  let allRates = true;
  results.forEach((trend, i) => {
    const counter = counters[i];
    if (trend.kind !== "data") {
      if (trend.kind !== "empty") failed++;
      /* The server's no-trend sentence points at hints.collected_counters, a JSON path a reader cannot open, so
         the counters it holds are listed here as text instead. */
      const collected = trend.hints && Array.isArray(trend.hints.collected_counters) ? trend.hints.collected_counters : null;
      const said = collected && trend.message ? trend.message.replace(/\s*[:—-]\s*see hints\.collected_counters.*$/, ".") : trend.message;
      notes.push(counter + ": " + (said || (trend.kind === "empty" ? "no trend data." : "could not be read.")) + (collected && collected.length ? " Collected here: " + collected.join(", ") + "." : ""));
      return;
    }
    if (trend.keptHours) {
      keptHours = Math.max(keptHours, trend.keptHours);
      kept.push(trend);
    }
    /* #3653 A5: the payload's baseline discontinuities render as a notice above the chart. They are about the
       server, so the same step from two counters shows once. */
    const points = trend.data.trend || [];
    /* A rate counter plots its per_second (its stored value only climbs); any other counter plots its value. */
    const rate = points.some((p) => p && "per_second" in p);
    /* #7416: only a rate counter is a delta, so only it has a baseline that a restart starts over. */
    if (rate) for (const d of trend.data.discontinuities || []) seenSteps.set(d.at + "|" + d.reason, d);
    if (!rate) allRates = false;
    drawn.push({
      key: "c" + i,
      label: counter,
      rows: points.map((p) => ({ time: p.time, v: rate ? p.per_second : p.value })),
      color: CATEGORICAL_COLORS[i % CATEGORICAL_COLORS.length],
    });
  });

  const failures = notes.map((n) => noticeStrip(n));
  if (!drawn.length && !failed) {
    mount(slot, emptyStrip("None of the " + counters.length + " checked counters has trend data in this window."));
    return;
  }
  if (!drawn.length) {
    mount(slot, [failures.length ? failures : null, errorStrip("None of the " + counters.length + " checked counters returned a trend.")]);
    return;
  }
  mount(slot, [
    kept.length ? keptWindowStrip(kept[0]) : null,
    discontinuityStrip({ discontinuities: [...seenSteps.values()] }),
    ...failures,
    zoomableLineChart({
      atTime: { server },
      points: mergeSeriesRows(drawn, "time", "v"),
      xKey: "time",
      series: drawn.map((d) => ({ key: d.key, label: d.label, color: d.color })),
      /* Rates print through fmtRate so a small real rate never reads as 0; a chart that mixes in a gauge or an
         average prints plain numbers, since one axis serves every line. */
      ...(allRates
        ? { unit: "/s", formatValue: fmtRate }
        : { formatValue: fmtPlain }),
      /* #2802: axis spans the requested window (ctx.hours ending now), not the data's own extent. A narrowed
         read spans the hours it answered for. */
      ...windowFromHours(keptHours || ctx.hours),
    }, "perfmon-trend|" + server, chartZoomScope(ctx.hours)),
    el("div", { class: "mp-metric-note", text: "Rate counters plot per second; other counters plot their value." }),
  ]);
}

/* A rate counter's stored value is its running total since the counter started, not a rate. The Perfmon grid shows
   such a row two ways: the per-second figure the server worked out for it (per_second, a key only a rate row
   carries) under Per second, and the running total under Total since counter start. The header says what the
   number is: the raw counter value, which counts from the counter's own start (an instance restart, or a
   database's own restart for a per-database counter), not from when monitoring began.
   The total stays whether or not a rate is known, because for a counter that seldom fires it is the only count
   there is: a deadlock counter that has fired 37 times can read 0.0033 a second. Where no delta was knowable
   (per_second is null: a first collection, a counter reset or a restart) the stored delta beside it is a stand-in
   0, not a count, so that cell is left blank. A gauge's value is its reading and stays under Value, and so does any
   other row's. */
function perfmonRows(counters) {
  return (counters || []).map((c) =>
    c && "per_second" in c
      ? { ...c, running_total: c.value, value: null, delta_value: c.per_second == null ? null : c.delta_value }
      : c
  );
}

export const SESSION_STATS = [
  { key: "summary.total_connections", label: "Connections", format: "int" },
  { key: "summary.total_running", label: "Running", format: "int" },
  { key: "summary.total_sleeping", label: "Sleeping", format: "int" },
  { key: "summary.total_dormant", label: "Dormant", format: "int" },
  { key: "summary.distinct_applications", label: "Applications", format: "int" },
  /* #3653: the snapshot's stamp under the census's one spelling. get_session_stats stamped itself as
     `collection_time` before #3637 fixed the vocabulary, and this tile reading the old key was the only
     thing that kept the read (and its three siblings) out of the captured_at roster. */
  { key: "captured_at", label: "Collected", format: "reltime", small: true },
];

/** The session counts the Activity tab charts; the read also carries databases_with_connections and the top application and host. */
const SESSION_TREND_SERIES = [
  { key: "total_sessions", label: "Total" },
  { key: "running_sessions", label: "Running" },
  { key: "sleeping_sessions", label: "Sleeping" },
  { key: "background_sessions", label: "Background" },
  { key: "dormant_sessions", label: "Dormant" },
  { key: "idle_sessions_over_30min", label: "Idle over 30 min" },
  { key: "sessions_waiting_for_memory", label: "Waiting for memory" },
];

export const SESSION_TREND_SCOPE = "Counts all sessions on the server, background (system) sessions and the monitor's own included. The Sessions tile counts user sessions with an application name and leaves the monitor out, so it reads lower.";

/** Session Stats trend: the server-wide session counts per bucket, with the newest bucket's top application and host as text. */
export function sessionStatsTrendPanel(server, ctx) {
  ctx = withPanelSignal(ctx);
  const { panel, body } = panelShell("Session Stats Trend", ctx.label, 2, "get_server_trend");
  (async () => {
    const res = await readToolWithinKeptHistory("get_server_trend", { server, metric: "session_stats", hours: ctx.hours }, ctx && ctx.signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, [keptWindowStrip(res), emptyStrip(res.message || "No session samples in this window.")]);
    const points = res.data.trend || [];
    if (!points.length) return mount(body, [keptWindowStrip(res), emptyStrip("No session samples in this window.")]);
    /* #3653 A5: the payload's baseline discontinuities as a notice above the chart. */
    const newest = points[points.length - 1];
    const top = [];
    if (newest.top_application_name) top.push("application " + newest.top_application_name + " (" + newest.top_application_connections + ")");
    if (newest.top_host_name) top.push("host " + newest.top_host_name + " (" + newest.top_host_connections + ")");
    mount(body, [
      keptWindowStrip(res),
      discontinuityStrip(res.data, { delta: false }), /* #7416: session counts are levels */
      zoomableLineChart({
        atTime: { server },
        points,
        xKey: "time",
        series: SESSION_TREND_SERIES.map((s, i) => ({ key: s.key, label: s.label, color: CATEGORICAL_COLORS[i % CATEGORICAL_COLORS.length] })),
        formatValue: fmtPlain,
        unit: "sessions",
        ...windowFromHours(res.keptHours || ctx.hours),
      }, "server-trend|" + server + "|session_stats", chartZoomScope(ctx.hours)),
      /* #7416: this trend counts every row of sys.dm_exec_sessions, background sessions and the monitor's own included. The Sessions tile
         beside it counts user sessions with an application name and leaves the monitor out, so the two numbers differ by design. */
      el("div", { class: "mp-metric-note", text: SESSION_TREND_SCOPE }),
      top.length ? el("div", { class: "mp-metric-note", text: "Top in the newest bucket: " + top.join("; ") + "." }) : null,
      chartNote(res.data, "level"),
    ]);
  })();
  return panel;
}

export const APPLICATION_COLUMNS = [
  { key: "program_name", label: "App" },
  { key: "connections", label: "Connections", format: "int" },
  { key: "running", label: "Running", format: "int" },
  { key: "sleeping", label: "Sleeping", format: "int" },
  { key: "dormant", label: "Dormant", format: "int" },
  { key: "total_cpu_time_ms", label: "CPU", format: "ms" },
];

export const JOB_COLUMNS = [
  { key: "job_name", label: "Job", name: true },
  { key: "job_enabled", label: "Enabled", format: "bool" },
  { key: "start_time", label: "Started", format: "time" },
  { key: "current_duration_formatted", label: "Running for" },
  { key: "avg_duration_formatted", label: "Average" },
  { key: "p95_duration_formatted", label: "p95" },
  { key: "percent_of_average", label: "% of avg", format: "num1" },
  { key: "is_running_long", label: "Long", format: "bool" },
  { key: "successful_run_count", label: "Successes", format: "int" },
];

/* The Job History grid on the Activity tab: the cross-server page's columns less Server, since the tab is one server's. */
export const JOB_HISTORY_COLUMNS = [
  { key: "run_time", label: "Run Time", format: "time" },
  { key: "job_name", label: "Job", name: true },
  { key: "category", label: "Category" },
  { key: "step", label: "Step" },
  { key: "status", label: "Status" },
  { key: "duration_formatted", label: "Duration", sortValue: (r) => r.duration_seconds },
  { key: "retries", label: "Retries", format: "int" },
  { key: "message", label: "Message", clamp: true, render: jobMessageCell },
];

/* The same squeeze as the Job History page's Message column (W2): a readable width, ONE line (#5644 W11), and the rest on a click (this tab has no detail pane). */
function jobMessageCell(r) {
  const text = r && r.message != null ? String(r.message) : "";
  return el("div", { class: "jh-message" }, [clampText(text)]);
}

/* #7416: a stored counter value is a whole number (a gauge's reading, a delta), and "1.00" or "86.00" claims a precision it does not
   have. A whole number prints whole, a fraction keeps two places. */
export function wholeOrTwoPlaces(v) {
  if (v == null) return "—";
  const n = Number(v);
  return Number.isInteger(n) ? fmtInt(n) : fmtNum(n, 2);
}

const PERFMON_COLUMNS = [
  { key: "counter_name", label: "Counter" },
  { key: "instance_name", label: "Instance", name: true },
  { key: "per_second", label: "Per second", format: "rate" },
  { key: "running_total", label: "Total since counter start", format: "int" },
  { key: "value", label: "Value", align: "right", display: (r) => wholeOrTwoPlaces(r.value) },
  { key: "delta_value", label: "Delta", align: "right", display: (r) => wholeOrTwoPlaces(r.delta_value) },
];

/* #7416: the same counter and instance can come from two objects (Transaction Delay with instance _Total is in both the database replica
   and the database mirroring object), and the two rows then read alike. The Object column is drawn only when a row would repeat another. */
export function perfmonColumns(counters) {
  const seen = new Set();
  let repeats = false;
  for (const c of counters || []) {
    const key = (c.counter_name || "") + "|" + (c.instance_name || "");
    if (seen.has(key)) repeats = true;
    seen.add(key);
  }
  if (!repeats) return PERFMON_COLUMNS;
  return [PERFMON_COLUMNS[0], { key: "object_name", label: "Object" }, ...PERFMON_COLUMNS.slice(1)];
}

export const INDEX_COLUMNS = [
  { key: "database_name", label: "Database", name: true },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_type", label: "Type" },
  { key: "classification", label: "Classification" },
  { key: "reserved_mb", label: "Reserved", format: "mbfit" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "total_reads", label: "Reads", format: "int" },
  { key: "user_updates", label: "Updates", format: "int" },
  { key: "last_user_access", label: "Last access", format: "time" },
];

