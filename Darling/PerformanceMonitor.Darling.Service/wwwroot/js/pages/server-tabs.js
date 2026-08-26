/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The server page's TAB REGISTRIES — the web port of the desktop viewer's per-server TabControl
 * (Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.xaml, ~65 TabItems across 38 partials).
 *
 * There are TWO, and serverTabsFor(card) picks between them from the fleet card's server-derived `is_postgres`
 * (#2530). SERVER_TABS is the SQL Server registry and also the default for a card that makes no engine claim —
 * the name is kept for that second job, because "no claim" has always rendered these tabs and still should.
 * POSTGRES_TABS is the eight-tab PostgreSQL registry; its own header says why that is the answer and not twelve.
 *
 * Every entry is `{ id, label, note?, build(server, ctx) }` and `build` returns an array of nodes, almost all of
 * them PANEL DESCRIPTORS run through the unmodified renderPanel (the #1563 seam): a `read` naming an MCP tool
 * served at GET /api/read/{read}, `params`, and a `viz` from the four-kind registry in panels.js. No fifth viz
 * kind was added — see the PR for why the property-grid shape that tempted one is served honestly by `stat`
 * (the reads that return a flat object) and `table` (the reads that already return rows).
 *
 * `ctx.hours` is the page's time range, the web twin of ViewerServerTab.TimeRange.cs's preset picker. Panels
 * whose read takes no time window ignore it and say so in their subtitle, because a panel labelled "last 6
 * hours" that is really a latest-snapshot read is a lie the reader cannot see.
 *
 * TWO HONESTY RULES run through this file:
 *   1. Every panel carries an `emptyText` saying WHY it could be empty in the reader's own terms. A read that
 *      returns the {status,message} envelope supplies its own (better) sentence and renderPanel shows that
 *      instead; emptyText only covers the data-arrived-but-zero-rows case, which would otherwise read as a
 *      blank rectangle.
 *   2. A tab whose desktop twin does something the browser genuinely cannot do carries a `note` naming it and
 *      pointing at the desktop viewer. A reader told "open the desktop viewer for plan analysis" is better
 *      served than one given a web page that looks like plan analysis and is not.
 *
 * R4 (XSS): every value reaches the DOM through renderPanel/util.el's text path. The custom cell renderers here
 * (query text, XML) build a <pre> through el(), the sentinel renderers build a text node, and none of them
 * touches innerHTML.
 */

import { el, readTool, mount, truncate, loadingStrip, errorStrip, emptyStrip, disclosure, noticeStrip, fmtMs } from "../util.js";
import { renderPanel, VIZ } from "../panels.js";
import { renderLineChart, SERIES_COLORS } from "../charts.js";

/* ─────────────────────────── shared cell renderers ─────────────────────────── */

/** Query-text cell: a truncated one-liner that expands to the full statement (mono) — the B2 disclosure. */
function codeDisclosure(text) {
  if (text == null || text === "") return document.createTextNode("—");
  return disclosure(text, el("pre", { class: "code" }, [text]), { max: 100 });
}

/**
 * A cell over a column whose store writes -1 for "there was none of this".
 *
 * Every numeric formatter in util.js would print that sentinel as a negative number — `fmtMs(-1)` is "-1 ms",
 * `fmtInt(-1)` is "-1" — and a negative duration or a negative age beside real ones reads as a small value
 * rather than as an absence. The dash is the same answer the WPF grids give the same sentinel.
 */
function sentinelCell(value, formatter) {
  return document.createTextNode(value == null || Number(value) < 0 ? "—" : formatter(value));
}

/** XML cell: the same disclosure over a captured payload (blocked-process report, deadlock graph). The desktop
 *  viewer renders these as a graph; the browser has no graph viewer, so it hands over the payload verbatim
 *  rather than pretending. The tab's note says where the graph lives. */
function xmlDisclosure(text) {
  if (text == null || text === "") return document.createTextNode("—");
  return disclosure("XML capture (" + String(text).length.toLocaleString() + " chars)", el("pre", { class: "code" }, [text]), {
    max: 60,
  });
}

/**
 * An array-valued cell — a blocking chain's databases, a lock cycle's PIDs. The generic path stringifies an
 * array, which is nearly right for a populated one and renders an EMPTY one as a blank cell rather than as the
 * "—" every other absent value on the page uses, so the two absences would not look alike.
 */
function listCell(values) {
  return document.createTextNode(Array.isArray(values) && values.length ? values.join(", ") : "—");
}

/**
 * A duration cell that honours the PostgreSQL collectors' −1 "not applicable" sentinel.
 *
 * −1 is deliberate on that side — 0 would read as "started this instant", which is a measurement rather than
 * an absence — and it reaches the wire, because the reader coalesces the NULL rather than propagating it. The
 * shared ms formatter would print "−1 ms", which reads as a measurement again, one row further on.
 */
function sentinelDuration(key) {
  return (row) => {
    const value = row[key];
    return document.createTextNode(value == null || value < 0 ? "—" : fmtMs(value));
  };
}

/* ─────────────────────────── composites ─────────────────────────── */

/* Two panels chain reads or reshape rows, so they are built by hand rather than declared. Both were already on
   the page before the tabs existed; they keep their behaviour and move into the tab that owns them. */

function panelShell(title, subtitle, span = 2) {
  const body = el("div", { class: "panel-body" }, [loadingStrip()]);
  const panel = el("div", { class: "panel card" + (span === 2 ? " span-2" : "") }, [
    el("h3", {}, [title, subtitle ? el("span", { class: "panel-sub", text: " " + subtitle }) : null]),
    body,
  ]);
  return { panel, body };
}

/**
 * Several panels over ONE fetch of one read.
 *
 * A descriptor owns its own fetch, which is the right default and is what makes the seam composable — but a
 * read that feeds two or three panels on the SAME tab then runs two or three times, and `readTool`/`apiGet` have
 * no cache to absorb it. That is a real cost rather than a tidiness point: `get_collection_health` rolls up
 * seven days of collector logs and computes sweep pressure, and the Collection Health tab renders three slices
 * of one payload (`sweep_pressure.*`, `collectors`, `sweep_pressure.heaviest_collectors`), so opening it was
 * running that query three times. Same shape for plan corrections (recommendations + automatic tuning), plan
 * cache (summary + per-type), sessions (summary + per-application) and system_health (chart + entries).
 *
 * Each spec is an ordinary panel descriptor minus `read`/`params` — the same viz registry, the same three-kind
 * response mapping renderPanel does — so nothing about the seam changes except how many times the wire is used.
 */
function fanout(read, params, specs) {
  for (const spec of specs) {
    if ((spec.viz === "table" || spec.viz === "line") && !spec.emptyText) {
      throw new Error("fanout(" + spec.title + "): a data panel must explain its own empty state.");
    }
  }
  const shells = specs.map((s) => panelShell(s.title, s.subtitle, s.span ?? 2));
  (async () => {
    const res = await readTool(read, params);
    specs.forEach((spec, i) => {
      const body = shells[i].body;
      if (res.kind === "error") return mount(body, errorStrip(res.message));
      if (res.kind === "empty") return mount(body, emptyStrip(res.message));
      try {
        mount(body, VIZ[spec.viz](res.data, spec));
      } catch (e) {
        mount(body, errorStrip("Could not render this panel: " + (e && e.message ? e.message : String(e))));
      }
    });
  })();
  return shells.map((s) => s.panel);
}

/**
 * Wait Stats table + a trend for ONE wait type, chosen from a picker seeded with the heaviest.
 *
 * The desktop viewer's Wait Stats tab is a checkbox list of wait types over a multi-series chart. This is the
 * single-select version of the same idea: the picker's options are the rows of the table directly above it,
 * heaviest first, so the reader is choosing from what they can already see rather than from a second list that
 * may disagree with it. That is also why get_wait_types is NOT read here — it returns the full distinct set,
 * which would offer wait types absent from the table and make the two disagree.
 */
export function waitsPanel(server, ctx) {
  const { panel, body } = panelShell("Wait Stats", ctx.label + ", with a trend for the wait you pick");
  (async () => {
    const res = await readTool("get_wait_stats", { server, hours: ctx.hours, limit: 20 });
    if (res.kind === "error") return mount(body, errorStrip(res.message));
    if (res.kind === "empty") return mount(body, emptyStrip(res.message));

    const waits = res.data.waits || [];
    const parts = [VIZ.table(res.data, { rowsKey: "waits", columns: WAIT_COLUMNS })];

    if (waits.length) {
      const chartSlot = el("div", {}, [loadingStrip()]);
      const picker = pickerControl(
        "Trend",
        waits.map((w) => w.wait_type),
        (waitType) => drawWaitTrend(chartSlot, server, ctx, waitType)
      );
      parts.push(el("div", { class: "picker-row" }, [picker]), chartSlot);
      drawWaitTrend(chartSlot, server, ctx, waits[0].wait_type);
    }
    mount(body, parts);
  })();
  return panel;
}

async function drawWaitTrend(slot, server, ctx, waitType) {
  mount(slot, loadingStrip());
  const trend = await readTool("get_wait_trend", { server, wait_type: waitType, hours: ctx.hours });
  if (trend.kind !== "data") {
    mount(slot, trend.kind === "empty" ? emptyStrip(trend.message) : errorStrip(trend.message));
    return;
  }
  mount(
    slot,
    renderLineChart({
      points: trend.data.trend || [],
      xKey: "time",
      series: [
        { key: "wait_time_ms_per_second", label: "Wait ms/s", color: SERIES_COLORS[0] },
        { key: "signal_wait_time_ms_per_second", label: "Signal ms/s", color: SERIES_COLORS[1] },
      ],
      formatValue: (v) => Math.round(v).toLocaleString(),
      unit: "ms/s",
    })
  );
}

/**
 * Perfmon counters: a picker over the counters this server actually collects, charting the chosen one.
 *
 * The desktop viewer's Perfmon tab is a searchable counter list over a multi-series chart. get_perfmon_trend
 * REQUIRES a counter_name, so without a picker the read is unreachable from the browser — which is why the
 * options come from get_perfmon_stats (the latest snapshot's counter list) rather than being hardcoded: a
 * hardcoded name is exactly how you end up charting a counter this server does not collect.
 *
 * When the trend read still comes back empty it can carry hints.collected_counters. Its message tells the
 * reader to "see hints.collected_counters", which is a JSON path no browser reader can open, so the hint list
 * is rendered here instead of being dropped.
 */
export function perfmonPanel(server, ctx) {
  const { panel, body } = panelShell("Perfmon Counters", "latest snapshot, with a trend for the counter you pick");
  (async () => {
    const res = await readTool("get_perfmon_stats", { server });
    if (res.kind === "error") return mount(body, errorStrip(res.message));
    if (res.kind === "empty") return mount(body, emptyStrip(res.message));

    const names = [...new Set((res.data.counters || []).map((c) => c.counter_name).filter(Boolean))].sort();
    if (!names.length) return mount(body, emptyStrip("The latest snapshot holds no perfmon counters."));

    /* The snapshot table lives HERE rather than in its own descriptor: this composite has already fetched the
       exact payload it would render, and a second panel would have paid for get_perfmon_stats twice to show the
       list the picker above it is built from. */
    const chartSlot = el("div", {}, [loadingStrip()]);
    const picker = pickerControl("Counter", names, (name) => drawPerfmonTrend(chartSlot, server, ctx, name));
    mount(body, [
      VIZ.table(res.data, {
        rowsKey: "counters",
        columns: PERFMON_COLUMNS,
        emptyText: "No perfmon counters in the latest snapshot.",
      }),
      el("div", { class: "picker-row" }, [picker]),
      chartSlot,
    ]);
    drawPerfmonTrend(chartSlot, server, ctx, names[0]);
  })();
  return panel;
}

async function drawPerfmonTrend(slot, server, ctx, counterName) {
  mount(slot, loadingStrip());
  const trend = await readTool("get_perfmon_trend", { server, counter_name: counterName, hours: ctx.hours });
  if (trend.kind === "error") return mount(slot, errorStrip(trend.message));
  if (trend.kind === "empty") {
    const hinted = trend.hints && Array.isArray(trend.hints.collected_counters) ? trend.hints.collected_counters : null;
    mount(slot, [
      emptyStrip(trend.message),
      hinted && hinted.length
        ? el("div", { class: "muted", style: "margin-top:0.4rem", text: "Collected here: " + hinted.join(", ") })
        : null,
    ]);
    return;
  }
  mount(
    slot,
    renderLineChart({
      points: trend.data.trend || [],
      xKey: "time",
      series: [
        { key: "value", label: "Value", color: SERIES_COLORS[0] },
        { key: "delta_value", label: "Delta", color: SERIES_COLORS[1] },
      ],
      formatValue: (v) => Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 }),
    })
  );
}

/**
 * Top Queries by CPU, plus that ONE query's per-collection history — the browser's per-query drill-down (#2520).
 *
 * The desktop viewer reaches this by double-clicking a Top Queries row, which opens QueryStatsHistoryWindow:
 * the chosen query's metric chart over the window and a grid of its per-collection snapshots. This is the same
 * drill-down with the selection made explicit, and it is the only way get_query_trend can be reached from a
 * browser at all — the read requires BOTH a query_hash and a database_name, and every other panel on this page
 * fetches with nothing but a server and a window, so there was no query_hash anywhere on the surface to send.
 *
 * THE PICKER IS SEEDED FROM THIS PAYLOAD'S OWN ROWS, in the order the table above it renders them, and an
 * option's value is that row's index in the array the table drew. So the query being trended is by construction
 * one of the queries shown — not merely the same query by name, the same array element. That is waitsPanel's
 * rule (the reason get_wait_types is deliberately NOT read there) applied to queries: a second, broader "every
 * query" read would offer queries absent from the table and make the two disagree, and a reader who cannot find
 * the trended query in the table above has been given two answers, not one.
 *
 * The chart carries avg CPU and avg elapsed only. Both are milliseconds, so one y-domain holds them honestly,
 * and both survive the hourly rollup this read falls back to past the raw tier's four days. The per-collection
 * measures that do NOT survive it — executions, DOP, the plan hash — go in the grid below, where the read's
 * null reads as the blank it is instead of flattening a chart to a zero nobody measured.
 */
export function topQueriesPanel(server, ctx) {
  const { panel, body } = panelShell("Top Queries by CPU", ctx.label + ", with a per-collection trend for the query you pick");
  (async () => {
    const res = await readTool("get_top_queries_by_cpu", { server, hours: ctx.hours, top: 20 });
    if (res.kind === "error") return mount(body, errorStrip(res.message));
    if (res.kind === "empty") return mount(body, emptyStrip(res.message));

    const queries = res.data.queries || [];
    const parts = [
      VIZ.table(res.data, {
        rowsKey: "queries",
        columns: TOP_QUERY_COLUMNS,
        emptyText:
          "No query stats in this window. Delta-based collection needs at least two cycles (~30 minutes) " +
          "before it reports non-zero values.",
      }),
    ];

    /* get_query_trend keys on both values, so a row carrying neither cannot be trended and is not offered.
       That is a real case rather than defensive coding: rows collected before a column existed read as null,
       and offering one would send a request the read answers with a 400 the reader cannot act on. The rank
       kept here is the row's position in the TABLE, so the label points at a row the reader can see. */
    const trendable = [];
    queries.forEach((q, i) => {
      if (q.query_hash && q.database_name) trendable.push({ rank: i + 1, query: q });
    });

    if (trendable.length) {
      const chartSlot = el("div", {}, [loadingStrip()]);
      const options = trendable.map((t, i) => ({
        value: String(i),
        label: "#" + t.rank + " · " + t.query.database_name + " · " + truncate(t.query.query_text || "(no statement text captured)", 60),
      }));
      const picker = pickerControl("Query", options, (i) => drawQueryTrend(chartSlot, server, ctx, trendable[Number(i)].query));
      parts.push(el("div", { class: "picker-row" }, [picker]), chartSlot);
      drawQueryTrend(chartSlot, server, ctx, trendable[0].query);
    } else if (queries.length) {
      parts.push(
        emptyStrip(
          "None of these rows carries both a query_hash and a database name, which is what get_query_trend " +
            "keys on, so there is nothing here to trend."
        )
      );
    }
    mount(body, parts);
  })();
  return panel;
}

async function drawQueryTrend(slot, server, ctx, query) {
  mount(slot, loadingStrip());
  const trend = await readTool("get_query_trend", {
    server,
    query_hash: query.query_hash,
    database_name: query.database_name,
    hours: ctx.hours,
  });
  if (trend.kind !== "data") {
    mount(slot, trend.kind === "empty" ? emptyStrip(trend.message) : errorStrip(trend.message));
    return;
  }

  /* #2353: the read routes to the hourly rollup once the window reaches past the raw tier's four-day
     retention, and reports which tier answered and how far it really reached. Those sentences are rendered
     rather than dropped, because this page's range goes to 30 days: without them the DOP and plan-hash
     columns simply go blank, and a blank column reads as "nothing to see" rather than "not measured here". */
  const notes = [];
  if (trend.data.aggregate_note) notes.push(trend.data.aggregate_note);
  if (trend.data.truncated) {
    notes.push(
      "History for this query starts " +
        trend.data.effective_hours_back +
        " hours back, not the " +
        trend.data.hours_back +
        " the window asked for — earlier collections have aged out of the tier that answered."
    );
  }

  mount(slot, [
    notes.length ? noticeStrip(notes.join(" ")) : null,
    renderLineChart({
      points: trend.data.trend || [],
      xKey: "collection_time",
      series: [
        { key: "avg_cpu_ms", label: "Avg CPU", color: SERIES_COLORS[0] },
        { key: "avg_elapsed_ms", label: "Avg Elapsed", color: SERIES_COLORS[1] },
      ],
      formatValue: (v) => Math.round(v).toLocaleString() + " ms",
      unit: "ms",
    }),
    VIZ.table(trend.data, {
      rowsKey: "trend",
      columns: QUERY_TREND_COLUMNS,
      emptyText: "No collections recorded this query in the window the read could reach.",
    }),
  ]);
}

/**
 * A labelled <select>, calling back with the chosen value. An option is a plain string when its value IS its
 * label (a wait type, a counter name) and a {value, label} pair when it is not — the query picker's value is a
 * row index, because a query has no short name to key on and its text is far too long to be one. Both the value
 * and the label are set through el()'s text/attribute paths, so neither a counter name nor a statement captured
 * from a monitored server can become markup (R4).
 */
function pickerControl(label, options, onPick) {
  const items = options.map((o) => (typeof o === "string" ? { value: o, label: o } : o));
  const sel = el(
    "select",
    { class: "range-select-inline", "aria-label": label },
    items.map((o) => el("option", { value: o.value, text: o.label }))
  );
  sel.value = items[0].value;
  sel.addEventListener("change", () => onPick(sel.value));
  return el("label", { class: "range-control" }, [el("span", { text: label }), sel]);
}

/** File I/O latency: pivot the flat per-(time, database) trend into one read-latency series per database. */
export function fileIoPanel(server, ctx) {
  const { panel, body } = panelShell("File I/O Latency", "avg read latency per database, " + ctx.label);
  (async () => {
    const res = await readTool("get_file_io_trend", { server, hours: ctx.hours });
    if (res.kind === "error") return mount(body, errorStrip(res.message));
    if (res.kind === "empty") return mount(body, emptyStrip(res.message));

    const { points, series } = pivot(res.data.trend || [], {
      xKey: "time",
      seriesKey: "database_name",
      valueKey: "avg_read_latency_ms",
    });
    if (!series.length) return mount(body, emptyStrip("No file I/O samples in this window."));
    mount(body, renderLineChart({ points, xKey: "time", series, formatValue: (v) => Math.round(v) + " ms" }));
  })();
  return panel;
}

/** Reshape flat rows into per-series points, keeping the top `maxSeries` series by peak value. */
function pivot(rows, { xKey, seriesKey, valueKey }, maxSeries = 8) {
  const byTime = new Map();
  const peak = new Map();
  for (const r of rows) {
    const t = r[xKey];
    const name = r[seriesKey];
    const v = r[valueKey];
    if (t == null || name == null) continue;
    if (!byTime.has(t)) byTime.set(t, { [xKey]: t });
    byTime.get(t)[name] = v;
    peak.set(name, Math.max(peak.get(name) ?? -Infinity, v ?? -Infinity));
  }
  const names = [...peak.keys()].sort((a, b) => peak.get(b) - peak.get(a)).slice(0, maxSeries);
  const points = [...byTime.values()].sort((a, b) => String(a[xKey]).localeCompare(String(b[xKey])));
  const series = names.map((n, i) => ({ key: n, label: n, color: SERIES_COLORS[i % SERIES_COLORS.length] }));
  return { points, series };
}

/* ─────────────────────────── descriptor helpers ─────────────────────────── */

/**
 * A table panel over a read, spanning both grid columns (the default for anything wider than ~5 columns).
 *
 * `emptyText` is REQUIRED, and the throw is the reason it is a parameter rather than an option. renderPanel
 * already shows a read's own {status,message} envelope when the read has nothing, and that sentence is better
 * than anything a descriptor could carry. What it does not cover is the read returning data whose row array is
 * empty — where vizTable falls back to a generic "No rows in this window", which on a collector that is off,
 * opt-in, or daily reads as a fault. Every tab is built during the DOM-shim run, so a missing sentence fails
 * there rather than shipping as a blank rectangle nobody notices.
 */
function table(title, read, params, rowsKey, columns, subtitle, emptyText, span = 2) {
  if (!emptyText) throw new Error("table(" + title + "): a table panel must explain its own empty state.");
  return renderPanel({ title, subtitle, read, params, viz: "table", rowsKey, columns, emptyText, span });
}

/**
 * A stat-tile panel over a read's top-level object (dotted keys reach into a nested summary).
 *
 * `emptyText` is OPTIONAL here where table()'s and line()'s are required, and the asymmetry is the honest one:
 * a read that always returns its summary object cannot reach the empty case, and forcing a sentence onto all
 * thirty of those would be noise around the handful that need it. What needs it is a read whose HEALTHY answer
 * is a data body carrying prose and none of the summary keys — get_pg_xmin_horizon's {status:"no_holder",
 * finding} is the type — which is data, not the {status,message} envelope, so it reaches vizStat and renders
 * as a row of em-dashes. vizStat spends this at exactly the all-null case; see the guard there.
 */
function stat(title, read, params, stats, subtitle, span = 1, emptyText) {
  return renderPanel({ title, subtitle, read, params, viz: "stat", stats, span, emptyText });
}

/**
 * A line panel over a read's row array. `opts.emptyText` is REQUIRED for the same reason table()'s is: without
 * a sentence, a read whose empty array means the thing simply did not happen inherits renderLineChart's
 * "Not enough data points to chart yet", which describes a condition it never had. get_blocking_trend and
 * get_deadlock_trend used to be the sharpest example; #2485 gave those two a {status,message} envelope, so they
 * now answer above this layer and their emptyText is the fallback rather than the sentence you see. Every other
 * line read still lands here at zero rows.
 */
function line(title, read, params, rowsKey, xKey, series, opts = {}) {
  if (!opts.emptyText) throw new Error("line(" + title + "): a chart panel must explain its own empty state.");
  return renderPanel({
    title,
    subtitle: opts.subtitle,
    read,
    params,
    viz: "line",
    rowsKey,
    xKey,
    series,
    format: opts.format,
    unit: opts.unit,
    emptyText: opts.emptyText,
    span: opts.span ?? 1,
  });
}

/* The subtitle every latest-snapshot panel carries. These reads take no time window at all — they return the
   most recent collected snapshot — so letting the page's range label sit above them would claim a window the
   data does not have. */
const SNAPSHOT = "latest snapshot";

/* ─────────────────────────── the tabs ─────────────────────────── */

/**
 * The tab registry, in the order the desktop viewer presents them. `build(server, ctx)` returns the tab's nodes;
 * `ctx` is `{ hours, label }` — the page's time range and its human label.
 */
export const SERVER_TABS = [
  {
    id: "overview",
    label: "Overview",
    build: (server, ctx) => [
      stat("Overview", "get_server_summary", { server }, OVERVIEW_STATS, SNAPSHOT, 2),
      stat("Server Properties", "get_server_properties", { server }, PROPERTY_STATS, SNAPSHOT, 2),
      line("CPU Utilization", "get_cpu_utilization", { server, hours: ctx.hours }, "samples", "sample_time", CPU_SERIES, {
        subtitle: ctx.label,
        format: "pct",
        unit: "%",
        emptyText: "No CPU samples in this window.",
      }),
      line("Memory", "get_memory_trend", { server, hours: ctx.hours }, "trend", "time", MEMORY_SERIES, {
        subtitle: ctx.label,
        format: "mb",
        emptyText: "No memory samples in this window.",
      }),
      line("Blocking Events", "get_blocking_trend", { server, hours: ctx.hours }, "trend", "time", COUNT_SERIES, {
        subtitle: ctx.label,
        emptyText: "No blocking events in this window — an empty trend here means none happened, not that nothing was collected.",
      }),
      line("Deadlocks", "get_deadlock_trend", { server, hours: ctx.hours }, "trend", "time", COUNT_SERIES, {
        subtitle: ctx.label,
        emptyText: "No deadlocks in this window — an empty trend here means none happened, not that nothing was collected.",
      }),
      fileIoPanel(server, ctx),
      table(
        "Analysis Findings",
        "get_analysis_findings",
        { server, hours: ctx.hours },
        "findings",
        FINDING_COLUMNS,
        ctx.label,
        "No findings in this window. Findings are written by the analysis pass, which needs at least 24 hours of collected history."
      ),
      stat("Daily Summary", "get_daily_summary", { server }, DAILY_STATS, "today (UTC)", 2),
      /* #2484: the month range behind the desktop viewer's Performance Calendar. A SECOND read rather than a
         wider get_daily_summary, which is also why it can sit beside the tile above: a tab must not fetch one
         read twice, so the today tile and the month grid cannot be the same read. The span is a fixed 30 days
         and says so, deliberately not ctx.hours — the page's range tops out well short of a month, and a
         calendar drawn over six hours is not a calendar. */
      table(
        "Daily Health Calendar",
        "get_daily_summary_range",
        { server, days_back: 30 },
        "days",
        DAILY_RANGE_COLUMNS,
        "last 30 days (UTC)",
        "No collected days in this range. A day with ANY collection appears here even when every signal was quiet, so a missing day is a gap in collection rather than a quiet one."
      ),
    ],
  },

  {
    id: "waits",
    label: "Wait Stats",
    build: (server, ctx) => [
      waitsPanel(server, ctx),
      table(
        "Waiting Tasks",
        "get_waiting_tasks",
        { server, hours: ctx.hours, limit: 30 },
        "tasks",
        WAITING_TASK_COLUMNS,
        ctx.label,
        "No waiting tasks were captured in this window."
      ),
      table(
        "Latch Stats",
        "get_latch_stats",
        { server, hours: ctx.hours, top: 10 },
        "latches",
        LATCH_COLUMNS,
        ctx.label,
        "No latch classes accumulated waits in this window."
      ),
      table(
        "Spinlock Stats",
        "get_spinlock_stats",
        { server, hours: ctx.hours, top: 10 },
        "spinlocks",
        SPINLOCK_COLUMNS,
        ctx.label,
        "No spinlocks recorded collisions in this window."
      ),
    ],
  },

  {
    id: "cpu",
    label: "CPU",
    build: (server, ctx) => [
      line("CPU Utilization", "get_cpu_utilization", { server, hours: ctx.hours }, "samples", "sample_time", CPU_SERIES, {
        subtitle: ctx.label,
        format: "pct",
        unit: "%",
        span: 2,
        emptyText: "No CPU samples in this window.",
      }),
      stat("Scheduler Pressure", "get_cpu_scheduler_pressure", { server }, SCHEDULER_STATS, SNAPSHOT, 2),
      table(
        "Top Queries by CPU",
        "get_top_queries_by_cpu",
        { server, hours: ctx.hours, top: 20 },
        "queries",
        TOP_QUERY_COLUMNS,
        ctx.label,
        "No query stats in this window. Delta-based collection needs at least two cycles (~30 minutes) before it reports non-zero values."
      ),
      table(
        "Top Procedures by CPU",
        "get_top_procedures_by_cpu",
        { server, hours: ctx.hours, top: 20 },
        "procedures",
        TOP_PROC_COLUMNS,
        ctx.label,
        "No procedure stats in this window. Delta-based collection needs at least two cycles (~30 minutes)."
      ),
    ],
  },

  {
    id: "memory",
    label: "Memory",
    build: (server, ctx) => [
      stat("Memory", "get_memory_stats", { server }, MEMORY_STATS, SNAPSHOT, 2),
      line("Memory Trend", "get_memory_trend", { server, hours: ctx.hours }, "trend", "time", MEMORY_SERIES, {
        subtitle: ctx.label,
        format: "mb",
        span: 2,
        emptyText: "No memory samples in this window.",
      }),
      table(
        "Memory Clerks",
        "get_memory_clerks",
        { server },
        "clerks",
        CLERK_COLUMNS,
        SNAPSHOT,
        "No memory clerks in the latest snapshot — the clerk collector may not have run yet.",
        1
      ),
      line(
        "Memory Grants",
        "get_memory_grants",
        { server, hours: ctx.hours },
        "grants",
        "collection_time",
        GRANT_SERIES,
        { subtitle: ctx.label, format: "mb", emptyText: "No memory grant samples in this window." }
      ),
      table(
        "Resource Semaphore",
        "get_resource_semaphore",
        { server, hours: ctx.hours },
        "grants",
        SEMAPHORE_COLUMNS,
        ctx.label,
        "No resource-semaphore samples in this window."
      ),
      table(
        "Memory Pressure Events",
        "get_memory_pressure_events",
        { server, hours: ctx.hours },
        "events",
        PRESSURE_COLUMNS,
        ctx.label,
        "No memory pressure events in this window — the healthy state for this read.",
        1
      ),
      ...fanout("get_plan_cache_bloat", { server, hours: ctx.hours }, [
        { title: "Plan Cache", subtitle: ctx.label, viz: "stat", stats: PLAN_CACHE_STATS },
        {
          title: "Plan Cache by Type",
          subtitle: ctx.label,
          viz: "table",
          rowsKey: "cache_types",
          columns: CACHE_TYPE_COLUMNS,
          emptyText: "No plan-cache breakdown in this window.",
        },
      ]),
    ],
  },

  {
    id: "blocking",
    label: "Blocking",
    note:
      "Blocked-process reports and deadlock graphs are shown here as their captured XML. The block-chain view " +
      "and the interactive deadlock graph are desktop-viewer features.",
    build: (server, ctx) => [
      line("Blocking Events", "get_blocking_trend", { server, hours: ctx.hours }, "trend", "time", COUNT_SERIES, {
        subtitle: ctx.label,
        emptyText: "No blocking events in this window — an empty trend here means none happened, not that nothing was collected.",
      }),
      line("Deadlocks", "get_deadlock_trend", { server, hours: ctx.hours }, "trend", "time", COUNT_SERIES, {
        subtitle: ctx.label,
        emptyText: "No deadlocks in this window — an empty trend here means none happened, not that nothing was collected.",
      }),
      table(
        "Blocking",
        "get_blocking",
        { server, hours: ctx.hours, limit: 30 },
        "events",
        BLOCKING_COLUMNS,
        ctx.label,
        "No blocking events in this window."
      ),
      table(
        "Deadlocks",
        "get_deadlocks",
        { server, hours: ctx.hours, limit: 20 },
        "deadlocks",
        DEADLOCK_COLUMNS,
        ctx.label,
        "No deadlocks in this window."
      ),
      /* #2484: the Current Waits tab the viewer has and the browser did not. ONE read, two panels --
         via fanout, not two line() calls, because the tab must not fetch the same read twice (there is
         a pin for it, and the reason is real: this read returns both series in one payload precisely so
         they cannot be looked at separately). A wait spike with no blocked sessions is a resource wait;
         the same spike with them is contention. */
      ...fanout("get_current_waits_trend", { server, hours: ctx.hours }, [
        {
          title: "Waiting Tasks",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "waiting_tasks",
          xKey: "collection_time",
          series: WAITING_TASK_SERIES,
          emptyText:
            "Nothing was waiting in this window. If the server has never been sampled the read says so " +
            "explicitly rather than reporting this as an all-clear.",
        },
        {
          title: "Blocked Sessions",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "blocked_sessions",
          xKey: "collection_time",
          series: BLOCKED_SESSION_SERIES,
          emptyText: "No blocked sessions in this window.",
        },
      ]),
      /* #2484: the aggregate lock-wait lane, the third chart on the viewer's Blocking Trends tab. Charts ONE
         numeric key and lets the read's own (collection, wait type) grouping stand, the same choice the two
         Current Waits panels below make — the wait type is the grouping the read already applied, not a
         second axis. get_wait_trend can chart one LCK type; this is the whole family. */
      line("Lock Waits", "get_lock_wait_trend", { server, hours: ctx.hours }, "trend", "collection_time", LOCK_WAIT_SERIES, {
        subtitle: ctx.label,
        emptyText:
          "No lock waits in this window. If wait stats have never been collected for this server the read " +
          "says so explicitly rather than reporting an absence of lock contention.",
      }),
      /* #2484: severity, the companion to the two count trends at the top of this tab. One read, two
         charts, via fanout for the same reason as above. */
      ...fanout("get_blocking_stats", { server, hours: ctx.hours }, [
        {
          title: "Blocking Severity",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "blocking_duration",
          xKey: "time",
          series: BLOCKING_SEVERITY_SERIES,
          emptyText:
            "No blocking in this window. If neither capture path has ever produced a row the read says " +
            "so explicitly rather than reporting a clean bill of health.",
        },
        {
          title: "Deadlock Severity",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "deadlock_severity",
          xKey: "time",
          series: DEADLOCK_SEVERITY_SERIES,
          emptyText: "No deadlocks in this window.",
        },
      ]),
      table(
        "Deadlock Graphs",
        "get_deadlock_detail",
        { server, hours: ctx.hours, limit: 5 },
        "deadlocks",
        DEADLOCK_XML_COLUMNS,
        ctx.label,
        "No deadlock graph XML captured in this window."
      ),
      table(
        "Blocked Process Reports",
        "get_blocked_process_xml",
        { server, hours: ctx.hours, limit: 5 },
        "reports",
        BPR_COLUMNS,
        ctx.label,
        "No blocked-process report XML in this window. The report is only written when the blocked process threshold is configured on the target."
      ),
      table(
        "Object Contention",
        "get_object_locking",
        { server },
        "objects",
        OBJECT_LOCK_COLUMNS,
        "daily collection",
        "No lock-wait rows recorded. Index and object stats are collected daily."
      ),
    ],
  },

  {
    id: "io",
    label: "File I/O",
    build: (server, ctx) => [
      fileIoPanel(server, ctx),
      table(
        "File I/O Stats",
        "get_file_io_stats",
        { server },
        "files",
        FILE_IO_COLUMNS,
        SNAPSHOT,
        "No file I/O rows in the latest snapshot."
      ),
      line("tempdb", "get_tempdb_trend", { server, hours: ctx.hours }, "trend", "time", TEMPDB_SERIES, {
        subtitle: ctx.label,
        format: "mb",
        span: 2,
        emptyText: "No tempdb samples in this window.",
      }),
      table(
        "Database Sizes",
        "get_database_sizes",
        { server },
        "databases",
        DB_SIZE_COLUMNS,
        SNAPSHOT,
        "No database sizes in the latest snapshot.",
        1
      ),
      table(
        "Table & Index Sizes",
        "get_table_index_sizes",
        { server },
        "tables",
        TABLE_SIZE_COLUMNS,
        "daily collection",
        "No object size rows recorded. Index and object stats are collected daily."
      ),
      table(
        "Persistent Version Store",
        "get_pvs_stats",
        { server },
        "databases",
        PVS_COLUMNS,
        SNAPSHOT,
        "No PVS rows. The collector reads a SQL Server 2019+ DMV, and a server with Accelerated Database Recovery off has nothing to report."
      ),
    ],
  },

  {
    id: "queries",
    label: "Queries",
    note:
      "Execution-plan analysis, the query heatmap, cached-plan retrieval and actual-plan re-execution are " +
      "desktop-viewer features — they need a plan renderer and a command back to the monitored server, neither " +
      "of which this read-only web seat has.",
    build: (server, ctx) => [
      table(
        "Active Queries",
        "get_active_queries",
        { server, hours: ctx.hours, limit: 50 },
        "queries",
        ACTIVE_COLUMNS,
        ctx.label,
        "No active-query snapshots in this window."
      ),
      /* #2484: the viewer's Performance Trends tab is four charts over three reads. Duration and the
         execution rate come from ONE payload -- via fanout, not two line() calls, because the tab must not
         fetch the same read twice. They get separate charts rather than separate series because ms/sec and
         executions/sec are two units, and one y-domain cannot hold both honestly. */
      ...fanout("get_query_duration_trend", { server, hours: ctx.hours }, [
        {
          title: "Query Duration Trend",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "trend",
          xKey: "time",
          series: DURATION_SERIES,
          format: "ms",
          span: 2,
          emptyText: "No query duration samples in this window.",
        },
        {
          title: "Executions per Second",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "trend",
          xKey: "time",
          series: EXECUTION_RATE_SERIES,
          span: 2,
          emptyText: "No query executions recorded in this window.",
        },
      ]),
      line(
        "Procedure Duration Trend",
        "get_procedure_duration_trend",
        { server, hours: ctx.hours },
        "trend",
        "time",
        DURATION_SERIES,
        {
          subtitle: ctx.label,
          format: "ms",
          span: 2,
          emptyText:
            "No stored-procedure activity in this window. A server that runs no procedures lands here too, " +
            "and the read says which of the two it found.",
        }
      ),
      line(
        "Query Store Duration Trend",
        "get_query_store_duration_trend",
        { server, hours: ctx.hours },
        "trend",
        "time",
        DURATION_SERIES,
        {
          subtitle: ctx.label,
          format: "ms",
          span: 2,
          emptyText:
            "No Query Store activity in this window. If Query Store is off on this server's databases the " +
            "read says so rather than showing an empty chart.",
        }
      ),
      /* #2520: the same table this tab always had, now with the drill-down attached. The CPU tab keeps the
         plain table — the drill-down belongs where the second question gets asked, and two of these would
         mean two fetches of get_top_queries_by_cpu for one page's worth of the same twenty rows. */
      topQueriesPanel(server, ctx),
      table(
        "Top Procedures by CPU",
        "get_top_procedures_by_cpu",
        { server, hours: ctx.hours, top: 20 },
        "procedures",
        TOP_PROC_COLUMNS,
        ctx.label,
        "No procedure stats in this window. Delta-based collection needs at least two cycles (~30 minutes)."
      ),
      table(
        "Query Store",
        "get_query_store_top",
        { server, hours: ctx.hours, top: 20 },
        "queries",
        QUERY_STORE_COLUMNS,
        ctx.label,
        "No Query Store rows in this window."
      ),
      /* #2484: the Query Store Regressions tab -- the only tab in the per-server page that was entirely
         unreachable from a browser rather than merely reduced. Built with table(), not an object literal:
         mount() stringifies anything it cannot consume, so a literal renders as [object Object] and never
         fetches the read at all. */
      table(
        "Query Store Regressions",
        "get_query_store_regressions",
        { server, hours: ctx.hours, limit: 50 },
        "regressions",
        QUERY_STORE_REGRESSION_COLUMNS,
        ctx.label,
        "No query regressed against its baseline in this window. If this server has no history OLDER than " +
          "the window there is nothing to compare against, and the read says so rather than calling it clear."
      ),
      /* #2484: the Query Heatmap tab. The interactive plot stays desktop-only by design -- there is no
         heatmap viz in this page's vocabulary and inventing a fifth one is not what the issue asked for --
         but the READ is portable, and a bucketed table is the same answer: one row per (time bin x
         magnitude bucket) cell. Bins are the viewer's own 5 minutes (the read's default, left unset here on
         purpose) so this table and the desktop draw the same picture for the same server and window.
         Built with table(), not an object literal: mount() stringifies anything it cannot consume, so a
         literal renders as [object Object] and never fetches the read at all. */
      table(
        "Query Heatmap",
        "get_query_heatmap",
        { server, hours: ctx.hours, limit: 500 },
        "cells",
        QUERY_HEATMAP_COLUMNS,
        ctx.label,
        "No query executed in this window. Query stats are collected every cycle whether or not anything " +
          "ran, so the read distinguishes a server nobody collected from one that was simply idle."
      ),
      table(
        "Long Query Completions",
        "get_long_query_completions",
        { server, hours: ctx.hours, limit: 30 },
        "completions",
        LONG_QUERY_COLUMNS,
        ctx.label,
        "No long-running completions in this window. This collector is opt-in and off by default."
      ),
      /* One read, two panels. get_plan_corrections returns both arrays, and automatic_tuning comes from an
         unconditional latest-snapshot query that ignores hours/limit entirely — so the second fetch was paying
         for the recommendations work twice to render a slice that never varied with the window. */
      ...fanout("get_plan_corrections", { server, hours: ctx.hours, limit: 50 }, [
        {
          title: "Plan Corrections",
          subtitle: ctx.label,
          viz: "table",
          rowsKey: "recommendations",
          columns: PLAN_CORRECTION_COLUMNS,
          emptyText: "No tuning recommendations in this window.",
        },
        {
          title: "Automatic Tuning",
          subtitle: SNAPSHOT,
          span: 1,
          viz: "table",
          rowsKey: "automatic_tuning",
          columns: AUTO_TUNING_COLUMNS,
          emptyText: "No per-database FORCE_LAST_GOOD_PLAN state recorded.",
        },
      ]),
    ],
  },

  {
    id: "config",
    label: "Configuration",
    build: (server) => [
      stat("Server Properties", "get_server_properties", { server }, PROPERTY_STATS, SNAPSHOT, 2),
      table(
        "Configuration Audit",
        "audit_config",
        { server },
        "recommendations",
        AUDIT_COLUMNS,
        SNAPSHOT,
        "The audit found nothing to flag."
      ),
      table(
        "Server Configuration",
        "get_server_config",
        { server },
        "settings",
        SERVER_CONFIG_COLUMNS,
        SNAPSHOT,
        "No sp_configure snapshot yet."
      ),
      table(
        "Database Configuration",
        "get_database_config",
        { server },
        "databases",
        DB_CONFIG_COLUMNS,
        SNAPSHOT,
        "No database configuration snapshot yet."
      ),
      table(
        "Query Store Health",
        "get_query_store_health",
        { server },
        "databases",
        QS_HEALTH_COLUMNS,
        "hourly collection",
        "No Query Store health rows yet."
      ),
      table(
        "Trace Flags",
        "get_trace_flags",
        { server },
        "trace_flags",
        TRACE_FLAG_COLUMNS,
        SNAPSHOT,
        "No trace flags are enabled on this server.",
        1
      ),
    ],
  },

  {
    id: "changes",
    label: "Config Changes",
    build: (server, ctx) => [
      table(
        "Server Configuration Changes",
        "get_server_config_changes",
        { server, hours: ctx.hours },
        "changes",
        SERVER_CHANGE_COLUMNS,
        ctx.label,
        "No server configuration changed in this window."
      ),
      table(
        "Database Configuration Changes",
        "get_database_config_changes",
        { server, hours: ctx.hours },
        "changes",
        DB_CHANGE_COLUMNS,
        ctx.label,
        "No database configuration changed in this window."
      ),
      table(
        "Trace Flag Changes",
        "get_trace_flag_changes",
        { server, hours: ctx.hours },
        "changes",
        TRACE_FLAG_CHANGE_COLUMNS,
        ctx.label,
        "No trace flags changed in this window."
      ),
    ],
  },

  {
    id: "activity",
    label: "Activity",
    build: (server, ctx) => [
      perfmonPanel(server, ctx),
      ...fanout("get_session_stats", { server }, [
        { title: "Sessions", subtitle: SNAPSHOT, viz: "stat", stats: SESSION_STATS },
        {
          title: "Sessions by Application",
          subtitle: SNAPSHOT,
          viz: "table",
          rowsKey: "applications",
          columns: APPLICATION_COLUMNS,
          emptyText: "No application rows in the latest session snapshot.",
        },
      ]),
      table(
        "Running Jobs",
        "get_running_jobs",
        { server },
        "jobs",
        JOB_COLUMNS,
        SNAPSHOT,
        "No SQL Agent jobs were running at the last collection — the normal state for most servers."
      ),
      table(
        "Index Usage",
        "get_index_usage",
        { server },
        "indexes",
        INDEX_COLUMNS,
        "daily collection",
        "No index usage rows recorded. Index and object stats are collected daily."
      ),
    ],
  },

  {
    id: "events",
    label: "System Events",
    note:
      "These are the system_health session and default trace, parsed on read. The desktop viewer additionally " +
      "charts the corruption and contention counters hour-by-hour; here they are the raw parsed rows.",
    build: (server, ctx) => [
      ...fanout("get_health_parser_system_health", { server, hours: ctx.hours, limit: 50 }, [
        {
          title: "system_health CPU",
          subtitle: ctx.label,
          viz: "line",
          rowsKey: "entries",
          xKey: "event_time",
          series: HEALTH_CPU_SERIES,
          format: "pct",
          unit: "%",
          emptyText: "No system_health entries in this window.",
        },
        {
          title: "system_health Entries",
          subtitle: ctx.label,
          viz: "table",
          rowsKey: "entries",
          columns: HEALTH_ENTRY_COLUMNS,
          emptyText: "No system_health entries in this window.",
        },
      ]),
      table(
        "Severe Errors",
        "get_health_parser_severe_errors",
        { server, hours: ctx.hours, limit: 50 },
        "errors",
        SEVERE_ERROR_COLUMNS,
        ctx.label,
        "No severe errors in this window — the healthy state for this read."
      ),
      table(
        "Scheduler Issues",
        "get_health_parser_scheduler_issues",
        { server, hours: ctx.hours, limit: 50 },
        "issues",
        SCHEDULER_ISSUE_COLUMNS,
        ctx.label,
        "No scheduler issues in this window — the healthy state for this read."
      ),
      table(
        "I/O Issues",
        "get_health_parser_io_issues",
        { server, hours: ctx.hours, limit: 50 },
        "issues",
        IO_ISSUE_COLUMNS,
        ctx.label,
        "No I/O issues in this window — the healthy state for this read."
      ),
      table(
        "CPU Tasks",
        "get_health_parser_cpu_tasks",
        { server, hours: ctx.hours, limit: 50 },
        "events",
        CPU_TASK_COLUMNS,
        ctx.label,
        "No CPU task events in this window."
      ),
      table(
        "Memory Conditions",
        "get_health_parser_memory_conditions",
        { server, hours: ctx.hours, limit: 50 },
        "events",
        MEMORY_CONDITION_COLUMNS,
        ctx.label,
        "No memory condition events in this window."
      ),
      table(
        "Memory Broker",
        "get_health_parser_memory_broker",
        { server, hours: ctx.hours, limit: 50 },
        "events",
        MEMORY_BROKER_COLUMNS,
        ctx.label,
        "No memory broker events in this window."
      ),
      table(
        "Memory Node OOM",
        "get_health_parser_memory_node_oom",
        { server, hours: ctx.hours, limit: 50 },
        "events",
        MEMORY_OOM_COLUMNS,
        ctx.label,
        "No memory node OOM events in this window — the healthy state for this read."
      ),
      /* #2484: the ninth member of the get_health_parser_* family. Built with table(), not a bare object
         literal -- mount() stringifies anything it cannot consume, so a literal renders as [object Object]
         and never fetches the read at all. */
      table(
        "Significant Waits",
        "get_health_parser_significant_waits",
        { server, hours: ctx.hours, limit: 50 },
        "waits",
        SIGNIFICANT_WAIT_COLUMNS,
        ctx.label,
        "No significant waits (a real session waiting 500 ms+ on a non-idle wait type) in this window."
      ),
      table(
        "Default Trace",
        "get_default_trace_events",
        { server, hours: ctx.hours, limit: 100 },
        "events",
        DEFAULT_TRACE_COLUMNS,
        ctx.label,
        "No significant default trace events in this window."
      ),
    ],
  },

  {
    id: "health",
    label: "Collection Health",
    build: (server, ctx) => [
      /* One read, three panels. get_collection_health rolls up seven days of collector logs AND computes sweep
         pressure; these are three slices of that single payload, so three descriptors meant running the tab's
         heaviest query three times to open it. */
      ...fanout("get_collection_health", { server }, [
        { title: "Sweep Pressure", subtitle: "trailing 7 days", viz: "stat", stats: SWEEP_STATS },
        {
          title: "Collectors",
          subtitle: "trailing 7 days",
          viz: "table",
          rowsKey: "collectors",
          columns: COLLECTOR_COLUMNS,
          emptyText: "No collection log rows for this server yet.",
        },
        {
          title: "Heaviest Collectors",
          subtitle: "trailing 7 days",
          viz: "table",
          rowsKey: "sweep_pressure.heaviest_collectors",
          columns: HEAVIEST_COLUMNS,
          emptyText: "No per-collector timings recorded yet.",
        },
      ]),
      /* #2484: the RAW per-run log under the rollup above. Its own read, not a slice of the fanout --
         the rollup aggregates seven days into one row per collector, and no projection of it can give
         back the individual runs. This is the tab people reach for when the rollup says HEALTHY and
         collection still looks wrong, and until now the WPF viewer was the only way to it. */
      table(
        "Collection Log",
        "get_collection_log",
        { server, hours: ctx.hours, limit: 200 },
        "runs",
        COLLECTION_LOG_COLUMNS,
        "individual runs, newest first, over the selected window",
        "No collector runs in the selected window.",
      ),
    ],
  },
];

/* ─────────────────────────── the PostgreSQL tabs ─────────────────────────── */

/**
 * The PostgreSQL registry (#2530). EIGHT tabs against the SQL Server registry's twelve, and the difference is the
 * design rather than a shortfall: parity was explicitly not the constraint. Bloat, wraparound, the xmin horizon
 * and autovacuum have no SQL Server analogue and are what actually pages a PostgreSQL DBA; tempdb, Query Store,
 * trace flags, plan cache and the system_health ring buffer have no PostgreSQL analogue, and rendering them at a
 * PostgreSQL target is the defect this registry removes rather than a shape to reproduce.
 *
 * Every get_pg_* read the service serves lands on exactly one of these tabs, and a pin asserts it in both
 * directions — derived from the dispatch, so a NEW PostgreSQL read cannot ship reachable only through MCP,
 * which is how the first eight spent three releases. It has already caught one: #2539's
 * get_pg_database_stats landed on dev while this was in review, and the pin refused the merge until the
 * Activity tab showed it.
 *
 * THE ENGINE-NEUTRAL BORROWING, and why it is only three reads. get_collection_health, get_collection_log and
 * get_analysis_findings read the collection log and the findings store — neither is a SQL Server collector's
 * output — so they answer a PostgreSQL target honestly and are the same panels the SQL Server Overview and
 * Collection Health tabs build. Nothing else is shared. get_server_summary and get_daily_summary LOOK
 * engine-neutral and are not: they roll up SQL Server wait types, memory pressure and deadlocks, and at a
 * PostgreSQL target they answer `unavailable` with a sentence about a collector that will never run.
 */
export const POSTGRES_TABS = [
  {
    id: "overview",
    label: "Overview",
    build: (server, ctx) => [
      /* The vitals are the three Tier 0 outage predictors plus the backlog that feeds two of them, as tiles
         rather than the tables the owning tabs carry: the question on a triage screen is "is anything wrong",
         not "which table". Each read is fetched again by the tab that owns it — the same deliberate duplication
         the SQL Server Overview makes with file I/O and findings. The cost is one read per tab actually opened;
         the alternative is an Overview that cannot answer its own question. */
      stat(
        "Freeze Headroom",
        "get_pg_wraparound_risk",
        { server, hours: ctx.hours },
        PG_WRAPAROUND_STATS,
        ctx.label,
        1,
        "No freeze-headroom samples in this window."
      ),
      stat(
        "xmin Horizon",
        "get_pg_xmin_horizon",
        { server, hours: ctx.hours },
        PG_XMIN_STATS,
        ctx.label,
        1,
        "Nothing held the xmin horizon back in this window — the healthy state, and the reason this tile is a sentence rather than three em-dashes."
      ),
      /* The counts are over the rows RETURNED, not over the database: the read ranks worst-first and caps at
         `limit`, and table_count / past_threshold_count / growing_count are all computed after that cap. So the
         subtitle says "worst 20" and the labels say "of those" — a tile reading "20 tables" on a server with
         4,000 tables behind on vacuum would be the most reassuring wrong number on the page. */
      stat(
        "Autovacuum Backlog",
        "get_pg_autovacuum_health",
        { server, hours: ctx.hours, limit: 20 },
        PG_AUTOVACUUM_STATS,
        ctx.label + ", worst 20 tables",
        1,
        "No table has dead tuples, pending analyze work, inserts since its last vacuum, or autovacuum disabled. The collector records only tables with pending work, so this is the healthy answer rather than missing data."
      ),
      stat(
        "Replication Slots",
        "get_pg_replication_slots",
        { server, hours: ctx.hours },
        PG_SLOT_STATS,
        ctx.label,
        1,
        "This server has no replication slots. A slot is the thing that retains WAL indefinitely, so none is one fewer way to fill a disk."
      ),
      table(
        "Analysis Findings",
        "get_analysis_findings",
        { server, hours: ctx.hours },
        "findings",
        FINDING_COLUMNS,
        ctx.label,
        "No findings in this window. On a PostgreSQL target the analysis pass writes the three Tier 0 outage predictors — wraparound, the xmin horizon and replication-slot retention — and nothing else yet, so an empty grid here is narrower than it looks."
      ),
      /* The SQL Server registry gives collection health a tab of its own. Six tabs is not enough to spend one
         on it, and on a PostgreSQL target it is the FIRST question anyone asks — eight collectors, several of
         them gated off by major version or by Aurora-ness — so it lands on the Overview instead. One read,
         three panels, for the reason fanout exists. */
      ...fanout("get_collection_health", { server }, [
        { title: "Sweep Pressure", subtitle: "trailing 7 days", viz: "stat", stats: SWEEP_STATS },
        {
          title: "Collectors",
          subtitle: "trailing 7 days",
          viz: "table",
          rowsKey: "collectors",
          columns: COLLECTOR_COLUMNS,
          emptyText: "No collection log rows for this server yet.",
        },
        {
          title: "Heaviest Collectors",
          subtitle: "trailing 7 days",
          viz: "table",
          rowsKey: "sweep_pressure.heaviest_collectors",
          columns: HEAVIEST_COLUMNS,
          emptyText: "No per-collector timings recorded yet.",
        },
      ]),
      table(
        "Collection Log",
        "get_collection_log",
        { server, hours: ctx.hours, limit: 200 },
        "runs",
        COLLECTION_LOG_COLUMNS,
        "individual runs, newest first, over the selected window",
        "No collector runs in the selected window. A collector gated off for this engine writes no log row at all rather than a zero-row success, so an absence here is the gate working."
      ),
      /* #2629: the extension inventory. On Overview because this is the "what IS this server" tab, and
         because it is the answer to most of the empty panels elsewhere on the page — a state of
         'available' means the files are there and one CREATE EXTENSION fills a grid that currently reads
         as a permanent absence. */
      table(
        "Extensions",
        "get_pg_extensions",
        { server, hours: ctx.hours, limit: 50 },
        "extensions",
        PG_EXTENSION_COLUMNS,
        ctx.label + ", per DATABASE not per cluster; 'available' means CREATE EXTENSION would work",
        "No extension inventory in this window. This collector runs DAILY, so a short window can be empty on a healthy server."
      ),
    ],
  },

  {
    id: "activity",
    label: "Activity",

    /* HALF of this tab is Aurora-only, and only half. get_pg_blocking runs everywhere — including on
       standbys, where a recovery conflict is blocking that happens nowhere else — while get_pg_top_queries
       is fed by pg_statement_stats, whose AppliesTo gate is target.IsAurora, so it reads
       aurora_stat_statements() and has no core-PostgreSQL equivalent in any version. Same treatment as the
       Waits tab, for the same reason: the panel self-explains via not_collected, and the note says so before
       the reader clicks. Saying it here rather than only in the panel matters more on THIS tab than on
       Waits, because the rest of the tab does fill, so a reader could reasonably read one empty grid among
       three as a fault. */
    note:
      "The blocking panels are collected at every PostgreSQL target, standbys included. Top Query Shapes is " +
      "not: it comes from Amazon Aurora's aurora_stat_statements(), which core PostgreSQL has in no version, " +
      "so on a stock PostgreSQL target that one panel is permanently empty and says so in its own words " +
      "while the two above it keep working.",
    build: (server, ctx) => [
      /* One read, three panels, and the FIRST of them is the denominator. get_pg_blocking is a periodic SAMPLE,
         not an event log: PostgreSQL records nothing unless asked, so "no chains" is ambiguous between a quiet
         server and a collector that never ran, and only the capture counts separate them. Showing the chains
         without the counts is the shape of answer this read was built to refuse. */
      ...fanout("get_pg_blocking", { server, hours: ctx.hours, limit: 50 }, [
        {
          title: "Blocking Sampling",
          subtitle: ctx.label + ", the denominator behind the two grids below",
          viz: "stat",
          stats: PG_BLOCKING_STATS,
          span: 2,
        },
        {
          title: "Blocking Chains",
          subtitle: ctx.label + ", root blocker attributed",
          viz: "table",
          rowsKey: "chains",
          columns: PG_BLOCKING_CHAIN_COLUMNS,
          emptyText:
            "No blocking chain was sampled in this window. Read that against the capture count above: with captures and no chains, nobody was blocked at any moment the collector looked.",
        },
        {
          title: "Lock Cycles",
          subtitle: ctx.label + ", mutual waits with no root",
          viz: "table",
          rowsKey: "cycles",
          columns: PG_BLOCKING_CYCLE_COLUMNS,
          emptyText:
            "No lock cycle was sampled in this window — the healthy state. A cycle has no root blocker to attribute, which is why it is a separate grid rather than a chain with a missing root.",
        },
      ]),
      table(
        "Top Query Shapes",
        "get_pg_top_queries",
        { server, hours: ctx.hours, limit: 20 },
        "queries",
        PG_TOP_QUERY_COLUMNS,
        ctx.label + ", by total execution time",
        "No query statistics in this window."
      ),
      /* Directly under the query shapes, joined on queryid: a plan only means something beside the
         statement it belongs to. The plan JSON is REDACTED at collection - query text dropped, literals
         replaced - so nothing customer-specific reaches this grid, and the empty text names the usual
         cause rather than implying the server is quiet. */
      table(
        "Captured Plans",
        "get_pg_plans",
        { server, hours: ctx.hours, limit: 10 },
        "plans",
        PG_PLAN_COLUMNS,
        ctx.label + ", grouped by plan shape - plans are redacted at collection",
        "No captured plans. Usually auto_explain is not loaded, or the monitoring login cannot read the server log; on Aurora and RDS there is no log file to read at all."
      ),
      /* Directly UNDER the query shapes, because that is the question it answers (#2539). A statement whose
         time makes no sense from its row count usually spilled, and pg_stat_database's temp counters are the
         only evidence of that we collect — the statement stats themselves cannot see it. The deadlock and
         rollback counters ride along because they come from the same free read; the raw blks_hit / blks_read
         columns do not, because `cache_hit_pct` and the read's own `cache_finding` say what they are for
         without a third and fourth column of block counts to be misread as a workload measure. */
      ...fanout("get_pg_database_stats", { server, hours: ctx.hours, limit: 20 }, [
        {
          title: "Database Activity",
          subtitle: ctx.label + ", totals over the databases returned",
          viz: "stat",
          stats: PG_DATABASE_STATS,
          span: 2,
          emptyText: "No database recorded transactions, block accesses, temp files or deadlocks in this window.",
        },
        {
          title: "By Database",
          subtitle: ctx.label + ", biggest spiller first",
          viz: "table",
          rowsKey: "databases",
          columns: PG_DATABASE_COLUMNS,
          emptyText:
            "No per-database activity in this window. These are windowed DIFFERENCES, so a single snapshot is not enough — the panel above says which of the two it is.",
        },
      ]),
      /* #2629: sampled lock activity. On Activity rather than a Blocking tab because PostgreSQL has no
         Blocking tab — get_pg_blocking's chains live here too — and because this answers a different
         question from that one: which modes and relations are contended OVER TIME, rather than who is
         blocking whom right now. */
      table(
        "Lock Activity (sampled)",
        "get_pg_lock_stats",
        { server, hours: ctx.hours, limit: 25 },
        "locks",
        PG_LOCK_STATS_COLUMNS,
        ctx.label + ", a sample of pg_locks rather than an event log; ungranted rows are the contended ones",
        "No lock activity sampled in this window. This is a SAMPLE, so a lock taken and released between two captures does not appear - an empty grid is the healthy state, not proof nothing was ever locked."
      ),
      /* #2629: what the queries above actually FILTER on. Placed on Activity rather than Storage because
         it is evidence about the workload, not about the disk — and it belongs beside top queries, since
         a predicate row and a query row are two views of the same execution. */
      table(
        "Predicate Selectivity",
        "get_pg_predicate_stats",
        { server, hours: ctx.hours, limit: 25 },
        "predicates",
        PG_PREDICATE_COLUMNS,
        ctx.label + ", SAMPLED counts - scale by 1/sample rate and treat the product as an estimate",
        "No predicate statistics in this window. pg_qualstats samples executions (1% by default) and needs shared_preload_libraries plus a restart, so an empty panel here usually means it is not loaded."
      ),
      table(
        "Column Statistics",
        "get_pg_column_stats",
        { server, hours: ctx.hours, limit: 25 },
        "columns",
        PG_COLUMN_STATS_COLUMNS,
        ctx.label + ", the numbers the planner turns into row estimates; n_distinct is a ratio when negative",
        "No column statistics in this window. This collector runs DAILY and reads only tables above a size floor, and pg_stats is privilege-filtered - a login without SELECT on a table sees nothing for it."
      ),
      /* #2661 deadlocks, on Activity beside blocking because they are the same subject at its limit: a
         blocking chain that resolved, and one the server had to break by cancelling somebody. Below
         blocking rather than above it because blocking is the far more common finding. */
      table(
        "Deadlocks",
        "get_pg_deadlocks",
        { server, hours: ctx.hours },
        "deadlocks",
        PG_DEADLOCK_COLUMNS,
        ctx.label + ", newest first; Sightings counts re-reads of the same report, not repeats",
        "No deadlock was reported in this window. That is the healthy answer - but it is the same shape as a server whose log cannot be read, which the plan-capture readiness panel reports on because it reads the same file. pg_stat_database's deadlock counter is the independent check."
      ),
    ],
  },

  {
    id: "vacuum",
    label: "Vacuum",
    note:
      "Four reads, one tab, on purpose. A session holds a transaction open; that transaction holds the xmin " +
      "horizon; an old horizon starves vacuum of anything it is allowed to reclaim; starved vacuum falls " +
      "behind on freezing; freezing falling behind is what ends in wraparound. Read alone each panel looks " +
      "survivable — one long session here, a holder there, a backlog, plenty of XIDs left — and together " +
      "they are one escalating story, in the order they are shown. Only the first panel names the SESSION, " +
      "which is where the fix has to be made, and only it says whether that session pins anything at all: an " +
      "open transaction holding no snapshot and no transaction id costs vacuum nothing, however long it has " +
      "been idle.",
    build: (server, ctx) => [
      /* Ordered by CAUSE, not by severity: session, then horizon, then backlog, then headroom.

         Session states leads because it is the link UPSTREAM of the horizon panel that used to open this
         tab. get_pg_xmin_horizon names the CLASS of holder (a session, a slot, standby feedback, a prepared
         transaction); this names WHICH session, and the remedy is always in whatever opened the
         transaction. It is also the only panel that can withhold the causal claim: a long
         idle-in-transaction session that pins nothing is the shape everybody misreads, and meeting it here
         is what stops the backlog two panels down being blamed on it. */
      ...fanout("get_pg_session_states", { server, hours: ctx.hours, limit: 25 }, [
        {
          title: "Sessions Holding a Transaction Open",
          subtitle: ctx.label + ", a sample at the collection interval rather than an event log",
          viz: "stat",
          stats: PG_SESSION_STATE_STATS,
          span: 2,
          emptyText:
            "No session held a transaction open past the collector's floor in this window — the healthy state, and a real all-clear rather than missing data. It is an all-clear about a SAMPLE, though: a transaction that opened and closed between two captures left no trace here.",
        },
        {
          title: "By Session",
          subtitle: ctx.label + ", horizon holders first, then the longest transaction",
          viz: "table",
          rowsKey: "sessions",
          columns: PG_SESSION_STATE_COLUMNS,
          emptyText:
            "No session held a transaction open past the collector's floor. Zero rows is the healthy answer — the collector stores nothing when every transaction is short — and it is a sample, so a transaction that opened and closed between two captures is invisible.",
        },
      ]),
      ...fanout("get_pg_xmin_horizon", { server, hours: ctx.hours }, [
        {
          title: "What Holds the Horizon",
          subtitle: ctx.label,
          viz: "stat",
          stats: PG_XMIN_STATS,
          span: 2,
          emptyText:
            "Nothing held the xmin horizon back in this window — the healthy state. Vacuum can reclaim everything it finds.",
        },
        {
          title: "Horizon Holders",
          subtitle: ctx.label + ", every cause seen, not only the current winner",
          viz: "table",
          rowsKey: "holders",
          columns: PG_XMIN_COLUMNS,
          emptyText:
            "No holder in this window. Four unrelated causes produce the same symptom — a long-running session, a replication slot, standby feedback, a prepared transaction — so this grid names the cause rather than repeating that the horizon is old.",
        },
      ]),
      ...fanout("get_pg_autovacuum_health", { server, hours: ctx.hours, limit: 20 }, [
        {
          title: "Autovacuum Backlog",
          subtitle: ctx.label + ", worst 20 tables",
          viz: "stat",
          stats: PG_AUTOVACUUM_STATS,
          span: 2,
          emptyText:
            "No table has dead tuples, pending analyze work, inserts since its last vacuum, or autovacuum disabled — the healthy answer.",
        },
        {
          title: "Tables Behind",
          subtitle: ctx.label + ", ranked by how far past each table's own threshold",
          viz: "table",
          rowsKey: "tables",
          columns: PG_AUTOVACUUM_COLUMNS,
          emptyText:
            "No table is behind on vacuum or analyze. The collector records only tables with pending work, so an empty grid is the healthy case rather than a collector that has not run.",
        },
      ]),
      ...fanout("get_pg_wraparound_risk", { server, hours: ctx.hours }, [
        {
          title: "Freeze Headroom",
          subtitle: ctx.label,
          viz: "stat",
          stats: PG_WRAPAROUND_STATS,
          span: 1,
          emptyText: "No freeze-headroom samples in this window.",
        },
        {
          /* The percentages above mean nothing without these four, and they are constants the READ ships, so
             that the browser is not the third place in this repo that decides where the failsafe engages. */
          title: "Where the Thresholds Are",
          subtitle: "PostgreSQL's own, not ours",
          viz: "stat",
          stats: PG_WRAPAROUND_THRESHOLD_STATS,
          span: 1,
        },
        {
          title: "Per-Database Headroom",
          subtitle: ctx.label + ", XID and MultiXact freeze headroom",
          viz: "table",
          rowsKey: "databases",
          columns: PG_WRAPAROUND_COLUMNS,
          emptyText:
            "No per-database freeze headroom in this window. Both counters are tracked: a database can be comfortable on XIDs and in trouble on MultiXacts, which is why they are separate columns rather than one worst-of.",
        },
      ]),
    ],
  },

  {
    id: "waits",
    label: "Waits",
    /* THE DECISION, argued in the PR: this tab is SHOWN at every PostgreSQL target, including the stock ones
       where its read can never have content. get_pg_wait_stats reads aurora_stat_system_waits(), which core
       PostgreSQL has in no version, so on `postgres` the panel is permanently empty — and it is permanently
       empty WITH A SENTENCE, because #2532 taught the read to answer that state with `not_collected` naming
       the server, the engine, the collector and the exact Aurora surface, and saying "and never will". A panel
       that explains itself is not the defect #2530 was filed about; twelve unexplained blank SQL Server tabs
       were. Hiding it would buy a tidier stock-PostgreSQL page at the price of a tab set that changes shape
       between two PostgreSQL servers in one fleet, and of making the one Aurora-specific capability we have
       invisible to the operator best placed to want it. */
    note:
      "Wait-event sampling on PostgreSQL is an Amazon Aurora feature: it comes from " +
      "aurora_stat_system_waits(), which core PostgreSQL has in no version. On a stock PostgreSQL target this " +
      "tab is permanently empty and the panel says so in its own words. The tab is here anyway so that the tab " +
      "set does not change shape between two PostgreSQL servers in the same fleet.",
    build: (server, ctx) => [
      table(
        "Wait Events",
        "get_pg_wait_stats",
        { server, hours: ctx.hours, limit: 20 },
        "waits",
        PG_WAIT_COLUMNS,
        ctx.label + ", background-worker and client-idle waits already excluded by the collector",
        "No wait events in this window."
      ),
      /* #2629: the same tab carries the stock-PostgreSQL answer, so the two are read TOGETHER rather than
         one being a consolation for the other's blank. Exactly one of them fills on any given target — the
         Aurora grid above on Aurora, this one wherever pg_wait_sampling is loaded — and each explains its
         own emptiness in its own words, which is what makes a permanently-empty panel informative rather
         than a defect. */
      table(
        "Sampled Waits (pg_wait_sampling)",
        "get_pg_wait_sampling",
        { server, hours: ctx.hours, limit: 20 },
        "waits",
        PG_WAIT_SAMPLING_COLUMNS,
        ctx.label + ", sample counts from a periodic profiler; event type CPU means running, not waiting",
        "No sampled waits in this window."
      ),
      table(
        "OS CPU by Query (pg_stat_kcache)",
        "get_pg_kernel_stats",
        { server, hours: ctx.hours, limit: 20 },
        "queries",
        PG_KERNEL_COLUMNS,
        ctx.label + ", CPU measured by the operating system; device bytes are not logical I/O",
        "No per-query OS resource usage in this window."
      ),
    ],
  },

  {
    id: "io",
    label: "I/O",
    note:
      "PostgreSQL reports I/O by BACKEND TYPE, object and context rather than by file, so there is no " +
      "per-file grid here and the shape is deliberately not the SQL Server one. On Aurora the whole write side " +
      "is NULL — backends there do not write data files, storage does — so the write columns read as blank " +
      "rather than as zero, which would claim a measurement never taken.",
    build: (server, ctx) => [
      ...fanout("get_pg_io_stats", { server, hours: ctx.hours, limit: 20 }, [
        {
          title: "I/O Summary",
          subtitle: ctx.label + ", differenced across the window",
          viz: "stat",
          stats: PG_IO_SUMMARY_STATS,
          span: 2,
          emptyText: "No I/O activity recorded in this window.",
        },
        {
          title: "By Backend, Object and Context",
          subtitle: ctx.label + ", busiest by read time first",
          viz: "table",
          rowsKey: "combinations",
          columns: PG_IO_COLUMNS,
          emptyText:
            "No I/O in this window. pg_stat_io needs PostgreSQL 16 or newer; on an older major the collector does not run at all, and the Collection Health panels on the Overview tab are where that shows.",
        },
      ]),
      /* #2629: what the I/O above is reading INTO, and what is writing it back out. Residency and
         checkpoint pressure are the two halves of a page's life either side of the I/O counters this tab
         already shows, so all three belong together. */
      table(
        "Buffer Pool Residency",
        "get_pg_buffer_usage",
        { server, hours: ctx.hours, limit: 25 },
        "relations",
        PG_BUFFER_USAGE_COLUMNS,
        ctx.label + ", residency is not read volume - a small hot table and a large one scanned once read alike",
        "No buffer pool contents recorded. This needs the pg_buffercache extension in the database the collector connects to."
      ),
      /* stat, not table: this read answers with ONE object describing the whole window, not a row set. */
      stat(
        "Checkpoints and WAL",
        "get_pg_write_stats",
        { server, hours: ctx.hours },
        PG_WRITE_STATS,
        ctx.label + ", requested checkpoints mean write volume filled max_wal_size before the interval elapsed",
        2,
        "No checkpoint or WAL activity differenced yet. These are differenced across snapshots, so a single collection has nothing to difference against and the window fills on the second."
      ),
    ],
  },

  {
    id: "replication",
    label: "Replication",
    build: (server, ctx) => [
      ...fanout("get_pg_replication_slots", { server, hours: ctx.hours }, [
        {
          title: "Slot Summary",
          subtitle: ctx.label,
          viz: "stat",
          stats: PG_SLOT_STATS,
          span: 2,
          emptyText:
            "This server has no replication slots. A slot is the thing that retains WAL indefinitely, so none is one fewer way to fill a disk.",
        },
        {
          title: "Slots",
          subtitle: ctx.label + ", most WAL retained first",
          viz: "table",
          rowsKey: "slots",
          columns: PG_SLOT_COLUMNS,
          emptyText:
            "No replication slots on this server. An inactive slot retains WAL forever and is the usual way a PostgreSQL instance fills its disk with nobody watching, so an empty grid here is good news.",
        },
      ]),
      /* #2629: the connected replicas, beside the slots. Two different questions and the DANGEROUS case is
         the disagreement — a slot that persists after its replica stops connecting retains WAL forever,
         and neither panel alone shows that. Slots first because that is the one that fills a disk. */
      table(
        "Connected Replicas",
        "get_pg_replication_stats",
        { server, hours: ctx.hours, limit: 25 },
        "replicas",
        PG_REPLICATION_STATS_COLUMNS,
        ctx.label + ", worst-in-window beside latest; lag is spiky and one sample catches one instant",
        "No replica was connected in this window. Expected on a server with no replicas - but if one is supposed to be attached, check the slots panel above: a slot with nothing connected to it retains WAL indefinitely."
      ),
    ],
  },

  /* Storage (#2541, #2542). Two reads, one question: where the space went and whether it is earning its
     keep. Bloat is deliberately NOT on the Vacuum tab despite being what vacuum lag costs - that tab is the
     cause chain read in causal order, and dropping the damage into the middle of it breaks the sequence
     that makes those three panels one story.

     Bloat is placed first because its number is an ESTIMATE and is the more dangerous of the two to act on:
     the summary tile carries the suppression count so a reader meets the caveat before any percentage. */
  {
    id: "storage",
    label: "Storage",
    build: (server, ctx) => [
      ...fanout("get_pg_table_bloat", { server, hours: ctx.hours, limit: 25 }, [
        {
          title: "Bloat Estimates",
          subtitle: ctx.label + ", estimates only - the tables are never read",
          viz: "stat",
          stats: PG_BLOAT_STATS,
          span: 2,
          emptyText:
            "No table of at least 1 MB was measured on this server. Below that floor bloat is not an actionable amount of space, so an empty panel here is the healthy answer rather than a missing read.",
        },
        {
          title: "By Table",
          subtitle: ctx.label + ", biggest estimated waste first; rows with no publishable estimate sort last",
          viz: "table",
          rowsKey: "tables",
          columns: PG_BLOAT_COLUMNS,
          emptyText:
            "No table of at least 1 MB was measured on this server. Bloat under a megabyte is not worth a maintenance window, so the collector does not store it.",
        },
      ]),
      ...fanout("get_pg_index_usage", { server, hours: ctx.hours, limit: 25 }, [
        {
          title: "Index Usage",
          subtitle: ctx.label + ", candidates are not conclusions",
          viz: "stat",
          stats: PG_INDEX_USAGE_STATS,
          span: 2,
          emptyText:
            "No index of at least 64 KB was recorded on this server. Below that floor an index costs effectively nothing to keep, so an empty panel here is the healthy answer rather than a missing read.",
        },
        {
          title: "By Index",
          subtitle: ctx.label + ", biggest unscanned first; invalid indexes on top",
          viz: "table",
          rowsKey: "indexes",
          columns: PG_INDEX_USAGE_COLUMNS,
          emptyText:
            "No index of at least 64 KB was recorded on this server, so there is nothing here to judge. This collector runs daily and is gated off on read replicas, where scan counts are the replica's own rather than the writer's.",
        },
      ]),
      /* #2629: MEASURED index bloat, beside the ESTIMATED table bloat above and the usage counts. The
         three answer one question in sequence — how much space, is it earning its keep, and is the index
         itself wasting it — and measured bloat sits last deliberately: it is the only one of the three
         that is not an estimate, so a reader who has just been warned about estimates meets the real
         measurement immediately after. */
      table(
        "Index Bloat (measured)",
        "get_pg_index_bloat",
        { server, hours: ctx.hours, limit: 25 },
        "indexes",
        PG_INDEX_BLOAT_COLUMNS,
        ctx.label + ", measured by walking the index; a value in Not Measured means the collector's per-cycle budget skipped it",
        "No index was measured on this server. This collector runs DAILY and measuring walks the index, so a short window or a fresh install is legitimately empty here."
      ),
    ],
  },

  /* #2658. The eighth tab, and the header's argument for seven admits it: what that argument rejects is
     reproducing SQL-Server-only CONCEPTS at a PostgreSQL target, not adding a question PostgreSQL genuinely
     has. "What is this server set to, and what changed" is one every engine has, and it was the only
     remaining get_pg_* pair with nowhere to land.

     Changes ABOVE current settings, deliberately. Somebody opening this tab during an incident is asking
     what moved, not what the server is; the full configuration is reference material and reads better as
     the thing underneath it. */
  {
    id: "config",
    label: "Configuration",
    build: (server, ctx) => [
      table(
        "Configuration Changes",
        "get_pg_server_config_changes",
        { server, hours: ctx.hours },
        "changes",
        PG_CONFIG_CHANGE_COLUMNS,
        ctx.label + ", newest first; the collector runs hourly, so a change happened in the hour before it was seen",
        "No configuration parameter changed value in this window. This compares consecutive snapshots, so an unchanged server is legitimately empty here - it is a finding, not missing data."
      ),
      table(
        "Settings",
        "get_pg_server_config",
        { server },
        "settings",
        PG_SERVER_CONFIG_COLUMNS,
        "non-default first; pending restart means the file and the running server disagree",
        "No configuration snapshot has been collected yet. This collector runs hourly, so a server registered within the last hour has not reached its first collection."
      ),
    ],
  },
];

/**
 * The tab registry for a fleet card — the ONE place the engine branch lives.
 *
 * Only a POSITIVE PostgreSQL claim moves a server off the SQL Server registry. `is_postgres` is derived
 * server-side from `collect.servers.engine_kind` (#2530) and is false for a NULL kind, for a token this build
 * does not recognise, and for a card that never arrived — every one of which means "no claim", not "SQL
 * Server". Defaulting an unclaimed server to the SQL Server tabs is the pre-#2530 behaviour, unchanged, and it
 * is the right default while the claim is absent: a PostgreSQL target stamps its kind on its first connect, so
 * the unclaimed population is servers that have not connected since the rung landed, and guessing PostgreSQL
 * from an absence would break every one of them. The browser does not re-derive the boolean (R1) — the card
 * carries it, decoded on the server through MonitoredEngineKind.
 */
export function serverTabsFor(card) {
  return card && card.is_postgres === true ? POSTGRES_TABS : SERVER_TABS;
}

/** The tab for an id within a registry, falling back to the first (Overview) — an unknown/absent id is a deep
 *  link, not an error. `overview`, `activity`, `waits` and `io` exist in BOTH registries, so those deep links
 *  survive a server turning out to be the other engine; the rest fall back rather than break. */
export function findServerTab(id, tabs) {
  const registry = tabs || SERVER_TABS;
  return registry.find((t) => t.id === id) || registry[0];
}

/** The tab's note as a rendered strip, or null. Kept here so the shell has no opinion about its wording. */
export function tabNote(tab) {
  return tab.note ? noticeStrip(tab.note) : null;
}

/* ─────────────────────────── stat descriptors ─────────────────────────── */

const OVERVIEW_STATS = [
  { key: "cpu_percent", label: "CPU", format: "pct" },
  { key: "memory_mb", label: "Memory", format: "mb" },
  { key: "blocking_count", label: "Blocking (recent)", format: "int" },
  { key: "deadlock_count", label: "Deadlocks (recent)", format: "int" },
  { key: "last_collection", label: "Last collection", format: "reltime", small: true },
];

const PROPERTY_STATS = [
  { key: "product_version", label: "Version", format: "text", small: true },
  { key: "edition", label: "Edition", format: "text", small: true },
  { key: "product_level", label: "Level", format: "text", small: true },
  { key: "cpu_count", label: "Logical CPUs", format: "int" },
  { key: "socket_count", label: "Sockets", format: "int" },
  { key: "cores_per_socket", label: "Cores/socket", format: "int" },
  { key: "hyperthread_ratio", label: "HT ratio", format: "int" },
  { key: "physical_memory_mb", label: "Physical memory", format: "mb" },
  { key: "is_clustered", label: "Clustered", format: "bool" },
  { key: "is_hadr_enabled", label: "Always On", format: "bool" },
  { key: "service_objective", label: "Service objective", format: "text", small: true },
];

const DAILY_STATS = [
  { key: "summary_date", label: "Date", format: "text", small: true },
  { key: "health_band", label: "Band", format: "text", small: true },
  { key: "overall_health", label: "Health", format: "num1" },
  { key: "top_wait_type", label: "Top wait", format: "text", small: true },
  { key: "total_wait_time_sec", label: "Total wait", format: "int" },
  { key: "unique_queries", label: "Unique queries", format: "int" },
  { key: "blocking_events", label: "Blocking", format: "int" },
  { key: "deadlock_count", label: "Deadlocks", format: "int" },
  { key: "alert_count", label: "Alerts", format: "int" },
  { key: "collection_errors", label: "Collection errors", format: "int" },
];

/* #2484: one row per collected day, the Performance Calendar's month grid as a table. Band first, because
   the point of the read is to scan for the day that stands out and then drill in with get_daily_summary. */
const DAILY_RANGE_COLUMNS = [
  { key: "summary_date", label: "Date" },
  { key: "health_band", label: "Band" },
  { key: "top_wait_type", label: "Top wait" },
  { key: "total_wait_time_sec", label: "Total wait (s)", format: "int" },
  { key: "unique_queries", label: "Unique queries", format: "int" },
  { key: "blocking_events", label: "Blocking", format: "int" },
  { key: "max_block_duration_ms", label: "Peak block", format: "ms" },
  { key: "deadlock_count", label: "Deadlocks", format: "int" },
  { key: "high_cpu_events", label: "High CPU", format: "int" },
  { key: "memory_pressure_events", label: "Mem pressure", format: "int" },
  { key: "alert_count", label: "Alerts", format: "int" },
  { key: "collection_errors", label: "Collection errors", format: "int" },
];

const SCHEDULER_STATS = [
  { key: "pressure_level", label: "Pressure", format: "text", small: true },
  { key: "schedulers", label: "Schedulers", format: "int" },
  { key: "runnable_tasks", label: "Runnable tasks", format: "int" },
  { key: "avg_runnable_per_scheduler", label: "Runnable/sched", format: "num2" },
  { key: "runnable_percent", label: "Runnable %", format: "num1" },
  { key: "workers", label: "Workers", format: "int" },
  { key: "max_workers", label: "Max workers", format: "int" },
  { key: "worker_utilization_percent", label: "Worker use %", format: "num1" },
  { key: "active_requests", label: "Active requests", format: "int" },
  { key: "queued_requests", label: "Queued requests", format: "int" },
  { key: "recommendation", label: "Recommendation", format: "text", small: true },
];

const MEMORY_STATS = [
  { key: "total_physical_memory_mb", label: "Physical", format: "mb" },
  { key: "available_physical_memory_mb", label: "Available", format: "mb" },
  { key: "memory_utilization_pct", label: "Utilization", format: "pct" },
  { key: "total_server_memory_mb", label: "Total server", format: "mb" },
  { key: "target_server_memory_mb", label: "Target server", format: "mb" },
  { key: "buffer_pool_mb", label: "Buffer pool", format: "mb" },
  { key: "plan_cache_mb", label: "Plan cache", format: "mb" },
  { key: "system_memory_state", label: "System state", format: "text", small: true },
  { key: "sql_memory_model", label: "Memory model", format: "text", small: true },
];

const PLAN_CACHE_STATS = [
  { key: "summary.bloat_level", label: "Bloat", format: "text", small: true },
  { key: "summary.total_plans", label: "Plans", format: "int" },
  { key: "summary.single_use_plans", label: "Single-use", format: "int" },
  { key: "summary.single_use_percent", label: "Single-use %", format: "num1" },
  { key: "summary.total_size_mb", label: "Cache size", format: "mb" },
  { key: "summary.single_use_size_mb", label: "Single-use size", format: "mb" },
  { key: "summary.wasted_percent", label: "Wasted %", format: "num1" },
  { key: "summary.bloat_recommendation", label: "Recommendation", format: "text", small: true },
];

const SESSION_STATS = [
  { key: "summary.total_connections", label: "Connections", format: "int" },
  { key: "summary.total_running", label: "Running", format: "int" },
  { key: "summary.total_sleeping", label: "Sleeping", format: "int" },
  { key: "summary.total_dormant", label: "Dormant", format: "int" },
  { key: "summary.distinct_applications", label: "Applications", format: "int" },
  { key: "collection_time", label: "Collected", format: "reltime", small: true },
];

const SWEEP_STATS = [
  { key: "sweep_pressure.verdict", label: "Verdict", format: "text", small: true },
  { key: "sweep_pressure.busy_percent", label: "Sweep busy %", format: "num1" },
  { key: "sweep_pressure.busy_ms_per_minute", label: "Busy ms/min", format: "int" },
  { key: "sweep_pressure.peak_cycle_ms", label: "Peak cycle", format: "ms" },
  { key: "sweep_pressure.peak_cycle_percent", label: "Peak cycle %", format: "num1" },
  { key: "sweep_pressure.peak_cycle_risk", label: "Peak risk", format: "text", small: true },
];

/* ─────────────────────────── line series ─────────────────────────── */

/* Neutral series colors assigned by the chart's ramp (B1) — no severity colors on chart lines. idle_cpu is
   dropped (B3): it would force a 0-100 domain and crush the real SQL/other/total series. */
const CPU_SERIES = [
  { key: "sql_server_cpu", label: "SQL CPU %" },
  { key: "other_process_cpu", label: "Other %" },
  { key: "total_cpu", label: "Total %" },
];

const MEMORY_SERIES = [
  { key: "total_server_memory_mb", label: "Total Server" },
  { key: "target_server_memory_mb", label: "Target" },
  { key: "buffer_pool_mb", label: "Buffer Pool" },
  { key: "plan_cache_mb", label: "Plan Cache" },
];

/* The two trend reads that return {time, count}. */
const COUNT_SERIES = [{ key: "count", label: "Events" }];

/* #2484: the aggregate lock-wait rate. One numeric key, per the same reasoning as the Current Waits series
   below — the LCK wait type is the grouping the read applied, not a second axis. */
const LOCK_WAIT_SERIES = [{ key: "wait_time_ms_per_second", label: "Lock wait (ms/sec)" }];

/* #2484: the two Current Waits series. Each charts ONE numeric key; the wait type and database name are
   the grouping the read already applied, not extra axes. */
const WAITING_TASK_SERIES = [{ key: "total_wait_ms", label: "Total Wait (ms)" }];
const BLOCKED_SESSION_SERIES = [{ key: "blocked_count", label: "Blocked Sessions" }];

/* #2484 severity series. Total and max share one millisecond axis honestly; the event/victim COUNTS are
   deliberately not charted beside them, because a count and a duration on one y-domain is the two-units
   mistake the CPU chart's dropped idle series exists to avoid. Counts live in the count trends above. */
const BLOCKING_SEVERITY_SERIES = [
  { key: "total_duration_ms", label: "Total Wait (ms)" },
  { key: "max_duration_ms", label: "Max Wait (ms)" },
];
const DEADLOCK_SEVERITY_SERIES = [
  { key: "total_wait_ms", label: "Total Wait (ms)" },
  { key: "max_wait_ms", label: "Max Wait (ms)" },
];

/* The three Performance-Trends reads all return {time, value, execution_count, executions_per_second} and
   share this series: `value` is milliseconds per second. The execution rate is NOT charted beside it — a
   count and a millisecond on one y-domain is the mistake the CPU chart's dropped idle_cpu series exists to
   avoid — it gets its own panel off the same payload. */
const DURATION_SERIES = [{ key: "value", label: "Avg duration" }];

/* #2484: executions/sec, the viewer's fourth Performance-Trends chart. Charts executions_per_second and
   not execution_count: the two are the same quantity, and the integer one reports ZERO on any server
   running under one execution a second, which would draw a flat line along the axis for a server that is
   simply quiet rather than idle. */
const EXECUTION_RATE_SERIES = [{ key: "executions_per_second", label: "Executions/sec" }];

const GRANT_SERIES = [
  { key: "granted_memory_mb", label: "Granted" },
  { key: "used_memory_mb", label: "Used" },
  { key: "available_memory_mb", label: "Available" },
];

const TEMPDB_SERIES = [
  { key: "total_reserved_mb", label: "Reserved" },
  { key: "user_objects_mb", label: "User objects" },
  { key: "internal_objects_mb", label: "Internal objects" },
  { key: "version_store_mb", label: "Version store" },
];

const HEALTH_CPU_SERIES = [
  { key: "sql_cpu_utilization", label: "SQL CPU %" },
  { key: "system_cpu_utilization", label: "System CPU %" },
];

/* ─────────────────────────── table columns ─────────────────────────── */

const WAIT_COLUMNS = [
  { key: "wait_type", label: "Wait Type" },
  { key: "total_wait_time_ms", label: "Total Wait", format: "ms" },
  { key: "resource_wait_ms", label: "Resource", format: "ms" },
  { key: "total_signal_wait_ms", label: "Signal", format: "ms" },
  { key: "waiting_tasks", label: "Tasks", format: "int" },
  { key: "signal_wait_pct", label: "Signal %", format: "num1" },
];

const WAITING_TASK_COLUMNS = [
  { key: "collection_time", label: "Time", format: "time" },
  { key: "session_id", label: "SPID", format: "int" },
  { key: "wait_type", label: "Wait" },
  { key: "wait_duration_ms", label: "Duration", format: "ms" },
  { key: "blocking_session_id", label: "Blocked by", format: "int" },
  { key: "database_name", label: "Database" },
];

const LATCH_COLUMNS = [
  { key: "latch_class", label: "Latch Class" },
  { key: "severity", label: "Severity", statusSev: true },
  { key: "total_delta_wait_time_ms", label: "Wait", format: "ms" },
  { key: "total_delta_waiting_requests", label: "Requests", format: "int" },
  { key: "avg_wait_ms_per_request", label: "Avg/req", format: "num2" },
  { key: "wait_ms_per_second", label: "ms/s", format: "num2" },
  { key: "description", label: "What it means", wrap: true },
];

const SPINLOCK_COLUMNS = [
  { key: "spinlock_name", label: "Spinlock" },
  { key: "total_delta_collisions", label: "Collisions", format: "int" },
  { key: "total_delta_spins", label: "Spins", format: "int" },
  { key: "total_delta_backoffs", label: "Backoffs", format: "int" },
  { key: "spins_per_collision", label: "Spins/coll", format: "num1" },
  { key: "collisions_per_second", label: "Coll/s", format: "num2" },
  { key: "description", label: "What it means", wrap: true },
];

/* #1949 ordering, which every query grid in both apps follows: the time/identity anchor, then the QUERY TEXT,
   then the metrics. Text pushed behind the metrics is text nobody scrolls to. */
const TOP_QUERY_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "query_text", label: "Query", render: (r) => codeDisclosure(r.query_text) },
  { key: "host_object", label: "Host object" },
  { key: "execution_count", label: "Execs", format: "int" },
  { key: "total_cpu_ms", label: "Total CPU", format: "ms" },
  { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
  { key: "total_elapsed_ms", label: "Total Elapsed", format: "ms" },
  { key: "avg_elapsed_ms", label: "Avg Elapsed", format: "ms" },
  { key: "max_cpu_ms", label: "Max CPU", format: "ms" },
  { key: "max_dop", label: "Max DOP", format: "int" },
  { key: "total_spills", label: "Spills", format: "int" },
  { key: "query_hash", label: "Query Hash", mono: true },
];

/* The per-collection snapshots get_query_trend returns, minus the two the chart already draws. Executions,
   DOP and the plan hash are here rather than on the chart because they are not milliseconds and one y-domain
   cannot hold two units honestly; the plan hash in particular is what answers "did it get worse, or did it get
   a different plan". All three come back null (never zero) when the hourly rollup answered, and the notice
   above the chart says why. */
const QUERY_TREND_COLUMNS = [
  { key: "collection_time", label: "Time", format: "time" },
  { key: "execution_count", label: "Execs", format: "int" },
  { key: "cpu_ms", label: "CPU", format: "ms" },
  { key: "elapsed_ms", label: "Elapsed", format: "ms" },
  { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
  { key: "avg_elapsed_ms", label: "Avg Elapsed", format: "ms" },
  { key: "spills", label: "Spills", format: "int" },
  { key: "min_dop", label: "Min DOP", format: "int" },
  { key: "max_dop", label: "Max DOP", format: "int" },
  { key: "query_plan_hash", label: "Plan Hash", mono: true },
];

const TOP_PROC_COLUMNS = [
  { key: "full_name", label: "Procedure" },
  { key: "database_name", label: "Database" },
  { key: "object_type", label: "Type" },
  { key: "execution_count", label: "Execs", format: "int" },
  { key: "total_cpu_ms", label: "Total CPU", format: "ms" },
  { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
  { key: "total_elapsed_ms", label: "Total Elapsed", format: "ms" },
  { key: "avg_elapsed_ms", label: "Avg Elapsed", format: "ms" },
  { key: "max_cpu_ms", label: "Max CPU", format: "ms" },
  { key: "total_spills", label: "Spills", format: "int" },
];

/* #2484: the regression grid. Baseline and recent sit BESIDE each other for each metric rather than being
   collapsed into the percent alone -- a 300% regression on a query that went from 1 ms to 4 ms is not the
   same finding as one that went from 1 s to 4 s, and the percent alone cannot tell them apart. Extra
   duration is the ranking key and the column that says whether the regression matters at all. */
const QUERY_STORE_REGRESSION_COLUMNS = [
  { key: "severity", label: "Severity" },
  { key: "database_name", label: "Database" },
  { key: "query_text", label: "Query", render: (r) => codeDisclosure(r.query_text) },
  { key: "query_id", label: "Query ID", format: "int" },
  { key: "additional_duration_ms", label: "Extra Duration", format: "ms" },
  { key: "duration_regression_percent", label: "Duration +%", format: "num1" },
  { key: "baseline_duration_ms", label: "Baseline Duration", format: "ms" },
  { key: "recent_duration_ms", label: "Recent Duration", format: "ms" },
  { key: "cpu_regression_percent", label: "CPU +%", format: "num1" },
  { key: "baseline_cpu_ms", label: "Baseline CPU", format: "ms" },
  { key: "recent_cpu_ms", label: "Recent CPU", format: "ms" },
  { key: "io_regression_percent", label: "Reads +%", format: "num1" },
  { key: "recent_exec_count", label: "Recent Execs", format: "int" },
  /* A plan count that moved between the two sides is the first thing to check: a query that regressed
     while gaining a plan is usually a plan-choice problem, not a data one. */
  { key: "baseline_plan_count", label: "Baseline Plans", format: "int" },
  { key: "recent_plan_count", label: "Recent Plans", format: "int" },
  { key: "last_execution_time", label: "Last Exec", format: "time" },
];

/* #2484: the heatmap grid, flattened. One row per cell, chronological. Query Count is DISTINCT QUERIES in
   the cell, not executions -- forty different queries running at 10-100ms is a different finding from one
   query running forty times, and the column label has to keep them apart. The top query is the most-executed
   one in the cell, which is what the desktop shows on hover. */
const QUERY_HEATMAP_COLUMNS = [
  { key: "time_bucket", label: "Time Bin", format: "time" },
  { key: "bucket_label", label: "Magnitude" },
  { key: "query_count", label: "Queries", format: "int" },
  { key: "top_query_text", label: "Most-Executed Query", render: (r) => codeDisclosure(r.top_query_text) },
  { key: "top_query_hash", label: "Query Hash" },
];

const QUERY_STORE_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "query_text", label: "Query", render: (r) => codeDisclosure(r.query_text) },
  { key: "query_id", label: "Query ID", format: "int" },
  { key: "plan_id", label: "Plan ID", format: "int" },
  { key: "execution_count", label: "Execs", format: "int" },
  { key: "avg_duration_ms", label: "Avg Duration", format: "ms" },
  { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
  { key: "avg_rowcount", label: "Avg Rows", format: "num1" },
  { key: "last_execution_time", label: "Last Exec", format: "time" },
  { key: "replica_role", label: "Replica" },
];

const LONG_QUERY_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "statement", label: "Statement", render: (r) => codeDisclosure(r.statement) },
  { key: "database_name", label: "Database" },
  { key: "object_name", label: "Object" },
  { key: "duration_ms", label: "Duration", format: "ms" },
  { key: "cpu_ms", label: "CPU", format: "ms" },
  { key: "row_count", label: "Rows", format: "int" },
  { key: "result", label: "Result" },
  { key: "client_app_name", label: "Application" },
  { key: "session_id", label: "SPID", format: "int" },
];

const PLAN_CORRECTION_COLUMNS = [
  { key: "collection_time", label: "Collected", format: "time" },
  { key: "query_text", label: "Query", render: (r) => codeDisclosure(r.query_text) },
  { key: "database_name", label: "Database" },
  { key: "query_id", label: "Query ID", format: "int" },
  { key: "recommendation_state", label: "State" },
  { key: "recommendation_reason", label: "Reason", wrap: true },
  { key: "score", label: "Score", format: "int" },
  { key: "estimated_gain_seconds", label: "Est. gain (s)", format: "num1" },
  { key: "last_good_plan_is_forced", label: "Forced", format: "bool" },
];

const AUTO_TUNING_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "force_last_good_plan_desired_state", label: "Desired" },
  { key: "force_last_good_plan_actual_state", label: "Actual" },
  { key: "force_last_good_plan_reason", label: "Reason", wrap: true },
  { key: "as_of", label: "As of", format: "time" },
];

const ACTIVE_COLUMNS = [
  { key: "collection_time", label: "Time", format: "time" },
  { key: "query_text", label: "Query", render: (r) => codeDisclosure(r.query_text) },
  { key: "session_id", label: "SPID", format: "int" },
  { key: "database_name", label: "Database" },
  { key: "status", label: "Status" },
  { key: "cpu_time_ms", label: "CPU", format: "ms" },
  { key: "elapsed_time_formatted", label: "Elapsed" },
  { key: "wait_type", label: "Wait" },
  { key: "blocking_session_id", label: "Blocked by", format: "int" },
  { key: "dop", label: "DOP", format: "int" },
  { key: "program_name", label: "Application" },
  { key: "login_name", label: "Login" },
];

const BLOCKING_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "blocked_sql_text", label: "Blocked SQL", render: (r) => codeDisclosure(r.blocked_sql_text) },
  { key: "blocking_sql_text", label: "Blocking SQL", render: (r) => codeDisclosure(r.blocking_sql_text) },
  { key: "database_name", label: "Database" },
  { key: "blocked_spid", label: "Blocked", format: "int" },
  { key: "blocking_spid", label: "Blocker", format: "int" },
  { key: "wait_time_ms", label: "Wait", format: "ms" },
  { key: "lock_mode", label: "Mode" },
  { key: "contentious_object", label: "Object" },
  { key: "blocked_client_app", label: "Blocked App" },
  { key: "blocking_client_app", label: "Blocking App" },
];

const DEADLOCK_COLUMNS = [
  { key: "deadlock_time", label: "Deadlock Time", format: "time" },
  { key: "victim_sql_text", label: "Victim SQL", render: (r) => codeDisclosure(r.victim_sql_text) },
  { key: "victim_process_id", label: "Victim" },
  { key: "process_summary", label: "Processes", wrap: true },
  { key: "has_deadlock_xml", label: "Graph", format: "bool" },
];

const DEADLOCK_XML_COLUMNS = [
  { key: "deadlock_time", label: "Deadlock Time", format: "time" },
  { key: "victim_process_id", label: "Victim" },
  { key: "deadlock_graph_xml", label: "Deadlock graph", render: (r) => xmlDisclosure(r.deadlock_graph_xml) },
];

const BPR_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "database_name", label: "Database" },
  { key: "blocked_spid", label: "Blocked", format: "int" },
  { key: "blocking_spid", label: "Blocker", format: "int" },
  { key: "wait_time_ms", label: "Wait", format: "ms" },
  { key: "blocked_process_report_xml", label: "Report", render: (r) => xmlDisclosure(r.blocked_process_report_xml) },
];

const OBJECT_LOCK_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "row_lock_wait_ms", label: "Row lock wait", format: "ms" },
  { key: "page_lock_wait_ms", label: "Page lock wait", format: "ms" },
  { key: "lock_escalations", label: "Escalations", format: "int" },
  { key: "page_latch_wait_ms", label: "Page latch", format: "ms" },
  { key: "page_io_latch_wait_ms", label: "Page IO latch", format: "ms" },
  { key: "total_rows", label: "Rows", format: "int" },
];

const FILE_IO_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "file_name", label: "File" },
  { key: "file_type", label: "Type" },
  { key: "size_mb", label: "Size", format: "mb" },
  { key: "avg_read_latency_ms", label: "Read latency", format: "num1" },
  { key: "avg_write_latency_ms", label: "Write latency", format: "num1" },
  { key: "delta_reads", label: "Reads", format: "int" },
  { key: "delta_writes", label: "Writes", format: "int" },
  { key: "physical_name", label: "Path", wrap: true },
];

const DB_SIZE_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "total_size_mb", label: "Total", format: "mb" },
  { key: "used_size_mb", label: "Used", format: "mb" },
];

const TABLE_SIZE_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "reserved_mb", label: "Reserved", format: "mb" },
  { key: "used_mb", label: "Used", format: "mb" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "index_count", label: "Indexes", format: "int" },
  { key: "growth_7d_mb", label: "7d growth", format: "mb" },
  { key: "growth_30d_mb", label: "30d growth", format: "mb" },
  { key: "growth_pct_30d", label: "30d %", format: "num1" },
];

const PVS_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "is_adr_on", label: "ADR", format: "bool" },
  { key: "pvs_size_mb", label: "PVS size", format: "mb" },
  { key: "pct_of_database", label: "% of DB", format: "num1" },
  { key: "database_data_size_mb", label: "Data size", format: "mb" },
  { key: "aborted_transaction_count", label: "Aborted txns", format: "int" },
  { key: "oldest_active_transaction_id", label: "Oldest active txn" },
];

const CLERK_COLUMNS = [
  { key: "clerk_type", label: "Clerk" },
  { key: "memory_mb", label: "Memory", format: "mb" },
];

const SEMAPHORE_COLUMNS = [
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

const CACHE_TYPE_COLUMNS = [
  { key: "cache_type", label: "Cache" },
  { key: "object_type", label: "Object type" },
  { key: "total_plans", label: "Plans", format: "int" },
  { key: "total_size_mb", label: "Size", format: "mb" },
  { key: "single_use_plans", label: "Single-use", format: "int" },
  { key: "single_use_size_mb", label: "Single-use size", format: "mb" },
  { key: "avg_use_count", label: "Avg uses", format: "num1" },
];

const SERVER_CONFIG_COLUMNS = [
  { key: "name", label: "Setting" },
  { key: "value_configured", label: "Configured" },
  { key: "value_in_use", label: "In use" },
  { key: "values_match", label: "Match", format: "bool" },
  { key: "is_dynamic", label: "Dynamic", format: "bool" },
  { key: "is_advanced", label: "Advanced", format: "bool" },
];

const DB_CONFIG_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "state", label: "State" },
  { key: "compatibility_level", label: "Compat", format: "int" },
  { key: "recovery_model", label: "Recovery" },
  { key: "rcsi", label: "RCSI", format: "bool" },
  { key: "snapshot_isolation", label: "SI", format: "bool" },
  { key: "auto_close", label: "Auto close", format: "bool" },
  { key: "auto_shrink", label: "Auto shrink", format: "bool" },
  { key: "auto_create_stats", label: "Auto create stats", format: "bool" },
  { key: "auto_update_stats", label: "Auto update stats", format: "bool" },
  { key: "query_store", label: "Query Store" },
  { key: "page_verify", label: "Page verify" },
  { key: "accelerated_database_recovery", label: "ADR", format: "bool" },
  { key: "optimized_locking", label: "Optimized locking", format: "bool" },
  { key: "log_reuse_wait", label: "Log reuse wait" },
];

const QS_HEALTH_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "actual_state", label: "Actual" },
  { key: "desired_state", label: "Desired" },
  { key: "state_matches_desired", label: "Match", format: "bool" },
  { key: "readonly_reason_decoded", label: "Read-only reason", wrap: true },
  { key: "current_storage_size_mb", label: "Used", format: "mb" },
  { key: "max_storage_size_mb", label: "Cap", format: "mb" },
  { key: "pct_of_cap", label: "% of cap", format: "num1" },
  { key: "size_based_cleanup_mode", label: "Cleanup" },
  { key: "stale_query_threshold_days", label: "Stale (days)", format: "int" },
];

const TRACE_FLAG_COLUMNS = [
  { key: "trace_flag", label: "Flag", format: "int" },
  { key: "enabled", label: "Enabled", format: "bool" },
  { key: "is_global", label: "Global", format: "bool" },
  { key: "is_session", label: "Session", format: "bool" },
];

const AUDIT_COLUMNS = [
  { key: "setting", label: "Setting" },
  { key: "status", label: "Status", statusSev: true },
  { key: "current_value", label: "Current" },
  { key: "suggested_value", label: "Suggested" },
  { key: "recommendation", label: "Why", wrap: true },
];

const SERVER_CHANGE_COLUMNS = [
  { key: "change_time", label: "Changed", format: "time" },
  { key: "configuration_name", label: "Setting" },
  { key: "old_value_configured", label: "Old (configured)" },
  { key: "new_value_configured", label: "New (configured)" },
  { key: "old_value_in_use", label: "Old (in use)" },
  { key: "new_value_in_use", label: "New (in use)" },
];

const DB_CHANGE_COLUMNS = [
  { key: "change_time", label: "Changed", format: "time" },
  { key: "database_name", label: "Database" },
  { key: "setting_name", label: "Setting" },
  { key: "old_value", label: "Old" },
  { key: "new_value", label: "New" },
];

const TRACE_FLAG_CHANGE_COLUMNS = [
  { key: "change_time", label: "Changed", format: "time" },
  { key: "trace_flag", label: "Flag", format: "int" },
  { key: "change_type", label: "Change" },
  { key: "previous_status", label: "Previous" },
  { key: "new_status", label: "New" },
  { key: "scope", label: "Scope" },
];

const APPLICATION_COLUMNS = [
  { key: "program_name", label: "Application" },
  { key: "connections", label: "Connections", format: "int" },
  { key: "running", label: "Running", format: "int" },
  { key: "sleeping", label: "Sleeping", format: "int" },
  { key: "dormant", label: "Dormant", format: "int" },
  { key: "total_cpu_time_ms", label: "CPU", format: "ms" },
];

const JOB_COLUMNS = [
  { key: "job_name", label: "Job" },
  { key: "job_enabled", label: "Enabled", format: "bool" },
  { key: "start_time", label: "Started", format: "time" },
  { key: "current_duration_formatted", label: "Running for" },
  { key: "avg_duration_formatted", label: "Average" },
  { key: "p95_duration_formatted", label: "p95" },
  { key: "percent_of_average", label: "% of avg", format: "num1" },
  { key: "is_running_long", label: "Long", format: "bool" },
  { key: "successful_run_count", label: "Successes", format: "int" },
];

const PERFMON_COLUMNS = [
  { key: "counter_name", label: "Counter" },
  { key: "instance_name", label: "Instance" },
  { key: "value", label: "Value", format: "num2" },
  { key: "delta_value", label: "Delta", format: "num2" },
];

const INDEX_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_type", label: "Type" },
  { key: "classification", label: "Classification" },
  { key: "reserved_mb", label: "Reserved", format: "mb" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "total_reads", label: "Reads", format: "int" },
  { key: "user_updates", label: "Updates", format: "int" },
  { key: "last_user_access", label: "Last access", format: "time" },
];

const FINDING_COLUMNS = [
  { key: "last_seen", label: "Last seen", format: "time" },
  { key: "category", label: "Category" },
  { key: "story_path", label: "Story", wrap: true },
  { key: "severity", label: "Severity", format: "num2" },
  { key: "confidence", label: "Confidence", format: "num2" },
  { key: "occurrences", label: "Occurrences", format: "int" },
  { key: "first_seen", label: "First seen", format: "time" },
];

const HEALTH_ENTRY_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "sql_cpu_utilization", label: "SQL CPU %", format: "int" },
  { key: "system_cpu_utilization", label: "System CPU %", format: "int" },
  { key: "non_yielding_tasks_reported", label: "Non-yielding", format: "int" },
  { key: "latch_warnings", label: "Latch warnings", format: "int" },
  { key: "spinlock_backoffs", label: "Spinlock backoffs", format: "int" },
  { key: "sick_spinlock_type", label: "Sick spinlock" },
  { key: "bad_pages_detected", label: "Bad pages", format: "int" },
  { key: "bad_pages_fixed", label: "Bad pages fixed", format: "int" },
  { key: "is_access_violation_occurred", label: "AV", format: "int" },
  { key: "total_dump_requests", label: "Dumps", format: "int" },
  { key: "page_faults", label: "Page faults", format: "int" },
];

const SEVERE_ERROR_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "error_number", label: "Error", format: "int" },
  { key: "severity", label: "Severity", format: "int" },
  { key: "state", label: "State", format: "int" },
  { key: "database_name", label: "Database" },
  { key: "message", label: "Message", wrap: true },
];

const SCHEDULER_ISSUE_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "scheduler_id", label: "Scheduler", format: "int" },
  { key: "cpu_id", label: "CPU", format: "int" },
  { key: "status", label: "Status" },
  { key: "is_online", label: "Online", format: "bool" },
  { key: "is_runnable", label: "Runnable", format: "bool" },
  { key: "non_yielding_time_ms", label: "Non-yielding", format: "ms" },
  { key: "thread_quantum_ms", label: "Quantum", format: "ms" },
];

const IO_ISSUE_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "state", label: "State" },
  { key: "io_latch_timeouts", label: "Latch timeouts", format: "int" },
  { key: "interval_long_ios", label: "Long I/Os (interval)", format: "int" },
  { key: "total_long_ios", label: "Long I/Os (total)", format: "int" },
  { key: "longest_pending_requests_duration_ms", label: "Longest pending", format: "ms" },
  { key: "longest_pending_requests_file_path", label: "File", wrap: true },
];

const CPU_TASK_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "state", label: "State" },
  { key: "max_workers", label: "Max workers", format: "int" },
  { key: "workers_created", label: "Created", format: "int" },
  { key: "workers_idle", label: "Idle", format: "int" },
  { key: "pending_tasks", label: "Pending", format: "int" },
  { key: "oldest_pending_task_waiting_time", label: "Oldest pending", format: "int" },
  { key: "tasks_completed_within_interval", label: "Completed", format: "int" },
  { key: "has_deadlocked_schedulers_occurred", label: "Deadlocked scheds", format: "bool" },
  { key: "did_blocking_occur", label: "Blocking", format: "bool" },
];

const MEMORY_CONDITION_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "last_notification", label: "Notification" },
  { key: "out_of_memory_exceptions", label: "OOM exceptions", format: "int" },
  { key: "available_physical_memory_gb", label: "Available", format: "num1" },
  { key: "working_set_gb", label: "Working set", format: "num1" },
  { key: "vm_committed_gb", label: "VM committed", format: "num1" },
  { key: "target_committed_gb", label: "Target committed", format: "num1" },
  { key: "current_committed_gb", label: "Current committed", format: "num1" },
  { key: "system_physical_memory_low", label: "System low", format: "int" },
  { key: "process_physical_memory_low", label: "Process low", format: "int" },
  { key: "last_oom_factor", label: "Last OOM factor" },
];

const MEMORY_BROKER_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "broker", label: "Broker" },
  { key: "notification", label: "Notification" },
  { key: "memory_ratio", label: "Ratio", format: "num2" },
  { key: "new_target", label: "New target", format: "int" },
  { key: "currently_allocated", label: "Allocated", format: "int" },
  { key: "previously_allocated", label: "Previously", format: "int" },
  { key: "currently_predicated", label: "Predicated", format: "int" },
  { key: "rate", label: "Rate", format: "num2" },
];

/* #2484: the Significant Waits grid. Signal duration sits beside the total on purpose -- both are
   milliseconds, so they share a column format honestly, and a signal close to the total is CPU pressure
   wearing a wait type's name. The statement goes through codeDisclosure like every other SQL column. */
const SIGNIFICANT_WAIT_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "wait_type", label: "Wait Type" },
  { key: "duration_ms", label: "Duration", format: "ms" },
  { key: "signal_duration_ms", label: "Signal", format: "ms" },
  { key: "wait_resource", label: "Resource", wrap: true },
  { key: "session_id", label: "Session", format: "int" },
  { key: "query_text", label: "Query", render: (r) => codeDisclosure(r.query_text) },
];

const MEMORY_OOM_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "memory_node_id", label: "Node", format: "int" },
  { key: "memory_utilization_pct", label: "Utilization", format: "pct" },
  { key: "failure_type", label: "Failure" },
  { key: "failure_value", label: "Value", format: "int" },
  { key: "available_physical_memory_kb", label: "Available", format: "int" },
  { key: "committed_kb", label: "Committed", format: "int" },
  { key: "target_kb", label: "Target", format: "int" },
  { key: "resources", label: "Resources", wrap: true },
  { key: "last_error", label: "Last error" },
];

const DEFAULT_TRACE_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  { key: "category", label: "Category" },
  { key: "event_name", label: "Event" },
  { key: "database_name", label: "Database" },
  { key: "object_name", label: "Object" },
  { key: "login_name", label: "Login" },
  { key: "application_name", label: "Application" },
  { key: "duration_ms", label: "Duration", format: "ms" },
  { key: "growth_mb", label: "Growth", format: "mb" },
  { key: "error_number", label: "Error", format: "int" },
  { key: "text_data", label: "Detail", wrap: true },
];

/* #2484: the raw log's columns. The duration SPLIT is the reason this table earns its place beside the
   rollup -- total time cannot separate a collector that is slow because the monitored server is slow from
   one that is slow because the store is, and that is the first question anyone asks of a slow collector. */
const COLLECTION_LOG_COLUMNS = [
  { key: "collection_time", label: "When", format: "time" },
  { key: "collector", label: "Collector" },
  { key: "status", label: "Status", statusSev: true },
  { key: "duration_ms", label: "Total", format: "ms" },
  { key: "sql_duration_ms", label: "On Server", format: "ms" },
  { key: "store_duration_ms", label: "On Store", format: "ms" },
  { key: "rows_collected", label: "Rows", format: "int" },
  { key: "error_message", label: "Error", wrap: true },
];

const COLLECTOR_COLUMNS = [
  { key: "collector", label: "Collector" },
  { key: "status", label: "Status", statusSev: true },
  { key: "total_runs", label: "Runs", format: "int" },
  { key: "errors", label: "Errors", format: "int" },
  { key: "yields", label: "Yields", format: "int" },
  { key: "failure_rate_pct", label: "Failure %", format: "num1" },
  { key: "avg_duration_ms", label: "Avg Dur", format: "ms" },
  { key: "p95_duration_ms", label: "p95 Dur", format: "ms" },
  { key: "last_success", label: "Last Success", format: "time" },
  { key: "last_error", label: "Last Error", wrap: true },
  /* #1837: what a NON-failing run reported (an enumeration that came back with 0 items). Blank for a
     plainly healthy collector; the same column the two WPF grids carry, so the web view is not the one
     Collection Health surface that still hides it. note_summary, not the raw last_note: it carries the
     "(all N runs)" qualifier that separates a persistently empty collector from an occasionally quiet
     one, composed server-side from the shared formatter so this table cannot render it a third way. */
  { key: "note_summary", label: "Note", wrap: true },
];

const HEAVIEST_COLUMNS = [
  { key: "collector", label: "Collector" },
  { key: "avg_duration_ms", label: "Avg", format: "ms" },
  { key: "p95_duration_ms", label: "p95", format: "ms" },
  { key: "max_duration_ms", label: "Max", format: "ms" },
  { key: "frequency_minutes", label: "Every (min)", format: "num1" },
  { key: "amortized_ms_per_minute", label: "ms/min", format: "num1" },
  { key: "pct_of_sweep_budget_per_run", label: "% of sweep", format: "num1" },
];

/* ─────────────────────────── PostgreSQL stat descriptors ─────────────────────────── */

/* Severity reaches these tiles and grids as the read's own token — `critical_far_past_threshold`,
   `warning_inactive_and_growing`, `ok`. It is NOT translated here. Two lexicons for one vocabulary drift, and
   the one that drifts is never the one being read; the tokens are also what the MCP surface already hands an
   agent, so an operator reading the screen and an agent reading the tool see the same word. */

const PG_WRAPAROUND_STATS = [
  { key: "worst_database", label: "Closest to wraparound", format: "text", small: true },
  { key: "worst_severity", label: "Severity", format: "text", small: true },
  { key: "worst_pct_toward_wraparound", label: "Toward wraparound (%)", format: "num2" },
];

/* PostgreSQL's own thresholds, shipped by the read as constants. They are here because a percentage with no
   scale is not a measurement: 74.5% sounds comfortable and is where the failsafe engages. */
const PG_WRAPAROUND_THRESHOLD_STATS = [
  { key: "thresholds.failsafe_engages_around_pct", label: "Failsafe engages (%)", format: "num1" },
  { key: "thresholds.server_warnings_begin_around_pct", label: "Server warns (%)", format: "num1" },
  { key: "thresholds.writes_stop_at_pct", label: "Writes stop (%)", format: "num2" },
  {
    key: "thresholds.anti_wraparound_vacuum_forced_at_pct_of_freeze_max_age",
    label: "Forced vacuum (% of freeze_max_age)",
    format: "num1",
  },
];

const PG_XMIN_STATS = [
  { key: "winning_source", label: "Holding the horizon", format: "text", small: true },
  { key: "winning_holder", label: "Holder", format: "text", small: true },
  { key: "winning_xmin_age", label: "xmin age (XIDs)", format: "int" },
];

const PG_SLOT_STATS = [
  { key: "slot_count", label: "Slots", format: "int" },
  { key: "inactive_count", label: "Inactive", format: "int" },
  { key: "total_retained_wal_gb", label: "Retained WAL (GB)", format: "num2" },
  { key: "worst_slot", label: "Worst slot", format: "text", small: true },
  { key: "worst_severity", label: "Severity", format: "text", small: true },
];

const PG_AUTOVACUUM_STATS = [
  { key: "table_count", label: "Tables returned", format: "int" },
  { key: "past_threshold_count", label: "Past threshold (of those)", format: "int" },
  { key: "growing_count", label: "Still growing (of those)", format: "int" },
  { key: "autovacuum_disabled_count", label: "Autovacuum off (of those)", format: "int" },
  { key: "worst_table", label: "Worst table", format: "text", small: true },
  { key: "worst_severity", label: "Severity", format: "text", small: true },
];

/* The denominator tiles. captures_total is the number of times the collector LOOKED; without it a zero-row
   chain grid cannot be told from a collector that never ran, and both are an absence of rows. */
const PG_BLOCKING_STATS = [
  { key: "captures_total", label: "Captures", format: "int" },
  { key: "captures_with_blocking", label: "With blocking", format: "int" },
  { key: "pct_of_captures_with_blocking", label: "Of captures (%)", format: "num1" },
  { key: "worst_chain_victims", label: "Worst chain victims", format: "int" },
  { key: "worst_chain_root_state", label: "Root state", format: "text", small: true },
  { key: "cycles_sampled", label: "Cycles", format: "int" },
];

/* Every total here is summed over the rows the read's LIMIT let through, and the read names that in the
   field itself (`cache_hit_pct_of_returned`), so the labels do too rather than promising a cluster figure the
   number structurally is not. `limit_reached` is what turns that caveat into something a reader can act on. */
const PG_DATABASE_STATS = [
  { key: "database_count", label: "Databases returned", format: "int" },
  { key: "total_temp_files", label: "Temp files", format: "int" },
  { key: "total_temp_bytes", label: "Temp bytes", format: "int" },
  { key: "top_spiller", label: "Biggest spiller", format: "text", small: true },
  { key: "total_deadlocks", label: "Deadlocks", format: "int" },
  { key: "cache_hit_pct_of_returned", label: "Cache hit % (of returned)", format: "num2" },
  { key: "limit_reached", label: "Limit reached", format: "bool" },
  /* Named on the tile rather than only per row: every total beside it is a LOWER BOUND when this is true,
     and a reader has to see that before drawing a conclusion from any of them. */
  { key: "statistics_were_reset_in_window", label: "Stats reset in window", format: "bool" },
];

/* The suppression count is a first-class tile rather than a per-row detail, because the usual cause is one
   instance-wide permissions gap: pg_stats is filtered by SELECT privilege and pg_monitor does not grant it,
   so either every row is publishable or almost none are. Naming it here is what gets it fixed.
   `estimated_bloat_*_over_trusted_rows` keeps the read's own name: it is a sum of ESTIMATES over the rows
   whose estimates were fit to publish, and a shorter label would promise reclaimable bytes. */
const PG_BLOAT_STATS = [
  { key: "table_count", label: "Tables returned", format: "int" },
  { key: "estimated_bloat_mb_over_trusted_rows", label: "Est. bloat (trusted rows)", format: "mb" },
  { key: "trusted_estimate_count", label: "Estimates published", format: "int" },
  { key: "suppressed_estimate_count", label: "Estimates suppressed", format: "int" },
  { key: "pgstattuple_available_anywhere", label: "pgstattuple installed", format: "bool" },
  { key: "limit_reached", label: "Limit reached", format: "bool" },
];

/* The capture tiles lead, and they are not decoration on this read: it is an EXCEPTION surface sampled once
   per collection cycle, so an empty grid is either "every transaction was short" or "nobody looked", and
   only the denominator separates them.

   `idle_in_transaction_pinning_nothing` is a first-class tile rather than a per-row detail because it is the
   correction this whole read exists to make: those sessions look exactly like the harmful ones and cost
   VACUUM nothing, and a reader who meets that count before the grid does not spend the grid looking for
   something to kill. `redacted_row_count` is here for the same reason `suppressed_estimate_count` is on the
   bloat tile - the usual cause is one missing GRANT on the whole instance. */
const PG_SESSION_STATE_STATS = [
  { key: "captures_in_window", label: "Captures", format: "int" },
  { key: "captures_with_sessions", label: "With reportable sessions", format: "int" },
  { key: "session_count", label: "Sessions returned", format: "int" },
  { key: "horizon_holder_count", label: "Pinned the xmin horizon", format: "int" },
  { key: "idle_in_transaction_pinning_nothing", label: "Idle in xact, pinning NOTHING", format: "int" },
  { key: "redacted_row_count", label: "Redacted rows", format: "int" },
  { key: "limit_reached", label: "Limit reached", format: "bool" },
];

/* "unscanned_without_a_structural_blocker" is deliberately not shortened to "droppable" on the tile either:
   the field name survived review for saying what it is, and a label that renamed it would undo that where a
   reader actually looks. */
const PG_INDEX_USAGE_STATS = [
  { key: "index_count", label: "Indexes returned", format: "int" },
  { key: "unscanned_without_a_structural_blocker", label: "Unscanned, nothing blocking a drop", format: "int" },
  { key: "mb_held_by_those_indexes", label: "Held by those indexes", format: "mb" },
  { key: "invalid_index_count", label: "Invalid indexes", format: "int" },
  { key: "statistics_were_reset_in_window", label: "Stats reset in window", format: "bool" },
  { key: "limit_reached", label: "Limit reached", format: "bool" },
];

const PG_IO_SUMMARY_STATS = [
  { key: "combination_count", label: "Combinations", format: "int" },
  { key: "total_reads", label: "Reads", format: "int" },
  { key: "total_read_time_ms", label: "Read time", format: "ms" },
  { key: "busiest_by_read_time", label: "Busiest", format: "text", small: true },
  { key: "write_counters_tracked_anywhere", label: "Write counters tracked", format: "bool" },
];

/* ─────────────────────────── PostgreSQL column descriptors ─────────────────────────── */

/* #2629: the stock-PostgreSQL wait grid. Samples FIRST and the estimate beside it, deliberately the
   opposite emphasis from PG_WAIT_COLUMNS above — that one reports measured time, this one reports how many
   times a periodic profiler caught a backend in a state. Leading with the estimate would present a derived
   number as the observation and hide that its error grows as the event gets rarer. */
/* #2629 column sets for the eight reads that reached the web only now. */

/* Density leads, not size: a huge index at 90% density is fine and a small one at 40% is the finding.
   skipped_reason is last and always present — a blank there is a measurement, a value there is an index
   nobody looked at, and the two must never be confused for one another. */
/* The checkpoint story in reading order: how many, how many were FORCED, and who paid for the writes.
   pct_checkpoints_requested leads the pair because the raw counts mean little apart — twenty checkpoints is
   healthy or alarming entirely depending on how many of them the server asked for.

   buffers_backend is here rather than buried with the other buffer counters because it is the one that
   lands on a user query: a backend writing its own dirty buffer is a query paying for the write. */
const PG_DEADLOCK_COLUMNS = [
  { key: "occurred_at", label: "When", format: "time" },
  { key: "victim_pid", label: "Victim PID", format: "int" },
  { key: "participant_count", label: "Sessions", format: "int" },
  { key: "lock_modes", label: "Lock Modes" },
  { key: "resources", label: "Resources" },
  { key: "victim_statement", label: "Victim Statement" },
  { key: "times_seen", label: "Sightings", format: "int", small: true },
];

const PG_SERVER_CONFIG_COLUMNS = [
  { key: "name", label: "Setting" },
  { key: "setting", label: "Value" },
  { key: "unit", label: "Unit", small: true },
  { key: "default_value", label: "Default", small: true },
  { key: "source", label: "Source", small: true },
  { key: "context", label: "Change needs", small: true },
  { key: "pending_restart", label: "Pending restart", format: "bool", small: true },
  { key: "category", label: "Category", small: true },
];

const PG_CONFIG_CHANGE_COLUMNS = [
  { key: "changed_at", label: "Seen at", format: "time" },
  { key: "name", label: "Setting" },
  { key: "old_value", label: "From" },
  { key: "new_value", label: "To" },
  { key: "unit", label: "Unit", small: true },
  { key: "source", label: "Source", small: true },
];

const PG_WRITE_STATS = [
  { key: "checkpoints_timed", label: "Timed", format: "int" },
  { key: "checkpoints_requested", label: "Requested", format: "int" },
  { key: "pct_checkpoints_requested", label: "% Requested", format: "num1" },
  { key: "checkpoint_write_time_ms", label: "Checkpoint write", format: "ms" },
  { key: "buffers_written_checkpoint", label: "Buffers (checkpoint)", format: "int" },
  { key: "buffers_clean", label: "Buffers (bgwriter)", format: "int" },
  { key: "buffers_backend", label: "Buffers (backend)", format: "int" },
  { key: "wal_records", label: "WAL records", format: "int" },
  { key: "wal_fpi", label: "WAL full-page images", format: "int" },
  { key: "counter_reset", label: "Counters reset", format: "bool", small: true },
];

const PG_INDEX_BLOAT_COLUMNS = [
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_mb", label: "Size", format: "mb" },
  { key: "avg_leaf_density", label: "Leaf Density %", format: "num1" },
  { key: "leaf_fragmentation", label: "Fragmentation %", format: "num1" },
  { key: "estimated_reclaimable_mb", label: "Reclaimable", format: "mb" },
  { key: "skipped_reason", label: "Not Measured" },
];

/* n_distinct and correlation are the two the planner actually turns into a row estimate, so they lead.
   num2 throughout: correlation lives between -1 and 1 and num1 would round most real values to 0.0 or 1.0,
   which is the difference between "clustered" and "not" rendered as the same number. */
const PG_COLUMN_STATS_COLUMNS = [
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "column_name", label: "Column" },
  { key: "n_distinct", label: "n_distinct", format: "num2" },
  { key: "correlation", label: "Correlation", format: "num2" },
  { key: "null_frac", label: "Null Frac", format: "num2" },
  { key: "top_value_frequency", label: "Top Value Freq", format: "num2" },
  { key: "avg_width", label: "Avg Width", format: "int" },
];

/* filtered_pct is the recommendation and worst_estimate_error_ratio is the counter-argument: high filtering
   says index this, a high error ratio says the planner is wrong about it and an index may not help. Both on
   the same row so neither is read alone. */
const PG_PREDICATE_COLUMNS = [
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "column_name", label: "Column" },
  { key: "operator", label: "Op" },
  { key: "rows_evaluated", label: "Rows Evaluated", format: "int" },
  { key: "rows_filtered", label: "Rows Filtered", format: "int" },
  { key: "filtered_pct", label: "Filtered %", format: "num1" },
  { key: "worst_estimate_error_ratio", label: "Worst Est. Error", format: "num2" },
  { key: "sample_rate", label: "Sample Rate", format: "num2" },
];

const PG_BUFFER_USAGE_COLUMNS = [
  { key: "relation_name", label: "Relation" },
  { key: "relation_kind", label: "Kind" },
  { key: "buffer_mb", label: "Resident", format: "mb" },
  { key: "pct_of_pool", label: "% of Pool", format: "num1" },
  { key: "dirty_buffers", label: "Dirty", format: "int" },
  { key: "pct_dirty", label: "% Dirty", format: "num1" },
  { key: "avg_usage_count", label: "Avg Usage", format: "num2" },
];

/* State first: it is the only column anyone scans for, and "available" is the one that means a one-line fix
   is waiting. */
const PG_EXTENSION_COLUMNS = [
  { key: "state", label: "State" },
  { key: "extension_name", label: "Extension" },
  { key: "database_name", label: "Database" },
  { key: "installed_version", label: "Installed" },
  { key: "default_version", label: "Default" },
  { key: "monitoring_relevant", label: "We Use It", format: "bool" },
];

/* granted is second, right beside the mode, because an ungranted lock is the entire finding and burying it
   among the counts would make a contended server look like a busy one. */
const PG_LOCK_STATS_COLUMNS = [
  { key: "mode", label: "Mode" },
  { key: "granted", label: "Granted", format: "bool" },
  { key: "lock_type", label: "Type" },
  { key: "relation_name", label: "Relation" },
  { key: "captures", label: "Captures", format: "int" },
  { key: "max_backends", label: "Max Backends", format: "int" },
  { key: "max_wait_ms", label: "Worst Wait", format: "ms" },
];

/* worst_* beside latest on every lag measure. Lag is spiky, a sample catches one instant, and a grid showing
   only the latest value reports a replica that fell an hour behind and caught up as perfectly healthy. */
const PG_REPLICATION_STATS_COLUMNS = [
  { key: "application_name", label: "Replica" },
  { key: "state", label: "State" },
  { key: "sync_state", label: "Sync" },
  { key: "replay_lag_ms", label: "Replay Lag", format: "ms" },
  { key: "worst_replay_lag_ms", label: "Worst Lag", format: "ms" },
  { key: "replay_bytes_behind", label: "Bytes Behind", format: "int" },
  { key: "worst_replay_bytes_behind", label: "Worst Behind", format: "int" },
  { key: "samples", label: "Samples", format: "int" },
];

const PG_WAIT_SAMPLING_COLUMNS = [
  { key: "event_type", label: "Type" },
  { key: "wait_event", label: "Event" },
  { key: "queryid", label: "Query ID" },
  { key: "samples", label: "Samples", format: "int" },
  { key: "estimated_wait_ms", label: "Est. Wait", format: "ms" },
  { key: "backends", label: "Backends", format: "int" },
  { key: "pct_of_samples", label: "% of Samples", format: "num1" },
];

/* #2629: OS CPU per query shape. Total first because it is the ranking, then the user/system split, because
   system time dominated by kernel work is a different finding from user time dominated by the planner.
   Device bytes are labelled "Device" rather than "Read"/"Write" — they are not logical I/O, and a zero here
   means the page cache served it. */
const PG_KERNEL_COLUMNS = [
  { key: "queryid", label: "Query ID" },
  { key: "database_name", label: "Database" },
  { key: "cpu_ms", label: "CPU", format: "ms" },
  { key: "user_cpu_ms", label: "User CPU", format: "ms" },
  { key: "system_cpu_ms", label: "System CPU", format: "ms" },
  { key: "pct_of_total_cpu", label: "% of CPU", format: "num1" },
  /* mb, from the _mb fields the tool emits beside the byte counts — "bytes" is not one of the renderer's
     formats, and an unrecognised one falls through to raw text rather than erroring. */
  { key: "device_read_mb", label: "Device Read", format: "mb" },
  { key: "device_write_mb", label: "Device Write", format: "mb" },
  { key: "major_faults", label: "Major Faults", format: "int" },
];

const PG_WAIT_COLUMNS = [
  { key: "wait_type", label: "Type" },
  { key: "wait_event", label: "Event" },
  { key: "total_wait_time_ms", label: "Total Wait", format: "ms" },
  { key: "waits", label: "Waits", format: "int" },
  /* num2 rather than ms: the average is routinely well under a millisecond, and fmtMs rounds those to "0 ms",
     which reads as "no wait" rather than as the small number it is. */
  { key: "avg_wait_time_ms", label: "Avg Wait (ms)", format: "num2" },
  { key: "pct_of_total_wait", label: "% of Wait", format: "num1" },
];

/* queryid, calls and the three time columns are the answer; the block counters are not on this grid. Temp
   blocks written IS, because a spill is the explanation for a statement whose time makes no sense from its
   row count, and it is the one counter PostgreSQL exposes that changes what you would do next. */
const PG_PLAN_COLUMNS = [
  { key: "queryid", label: "Query ID", mono: true },
  { key: "top_node_type", label: "Top Node" },
  { key: "node_count", label: "Nodes", format: "int" },
  { key: "total_duration_ms", label: "Total", format: "ms" },
  { key: "max_duration_ms", label: "Max", format: "ms" },
  { key: "avg_duration_ms", label: "Avg", format: "ms" },
  /* CAPTURES, not executions: the collector reads an overlapping tail of the server log, so one
     execution can be seen twice. The Calls column on the grid above is the authority on how often a
     statement actually ran, and this label has to keep the two apart. */
  { key: "captures", label: "Captures", format: "int" },
  { key: "plan_hash", label: "Plan Hash", mono: true },
];

const PG_TOP_QUERY_COLUMNS = [
  { key: "queryid", label: "Query ID", mono: true },
  { key: "calls", label: "Calls", format: "int" },
  { key: "total_exec_time_ms", label: "Total", format: "ms" },
  { key: "avg_exec_time_ms", label: "Avg", format: "ms" },
  /* A high-water mark over the whole retained history, not a window maximum — the read says so, and the label
     says so here, because a "Max" beside two window-scoped columns would otherwise be read as one of them. */
  { key: "max_exec_time_ms", label: "Max (all time)", format: "ms" },
  { key: "pct_of_total_time", label: "% of Time", format: "num1" },
  { key: "rows_returned", label: "Rows", format: "int" },
  { key: "temp_blks_written", label: "Temp Blocks Written", format: "int" },
  { key: "wal_bytes", label: "WAL (bytes)", format: "int" },
  { key: "max_exec_peakmem_bytes", label: "Peak Mem (bytes)", format: "int" },
  { key: "query_text", label: "Query", wrap: true, render: (row) => codeDisclosure(row.query_text) },
];

const PG_BLOCKING_CHAIN_COLUMNS = [
  { key: "captured_at", label: "When", format: "time" },
  { key: "root_pid", label: "Root PID", format: "int" },
  { key: "databases", label: "Databases", render: (row) => listCell(row.databases) },
  { key: "root_username", label: "User" },
  { key: "root_application", label: "Application", wrap: true },
  { key: "root_state", label: "Root State" },
  { key: "root_is_idle_in_transaction", label: "Idle in Txn", format: "bool" },
  { key: "root_xact_duration_ms", label: "Txn Age", render: sentinelDuration("root_xact_duration_ms") },
  { key: "total_victims", label: "Victims", format: "int" },
  { key: "direct_victims", label: "Direct", format: "int" },
  { key: "max_chain_depth", label: "Depth", format: "int" },
  { key: "worst_victim_wait_ms", label: "Worst Wait", render: sentinelDuration("worst_victim_wait_ms") },
  /* Null here means "cannot tell how many samples this root appeared in", not one — the read carries the
     reason in samples_as_root_note, which is why this renders as "—" rather than as 1. */
  { key: "samples_as_root", label: "Samples as Root", format: "int" },
  { key: "root_query", label: "Root Query", wrap: true, render: (row) => codeDisclosure(row.root_query) },
  { key: "recommended_action", label: "Action", wrap: true },
];

const PG_BLOCKING_CYCLE_COLUMNS = [
  { key: "captured_at", label: "When", format: "time" },
  { key: "participant_count", label: "Participants", format: "int" },
  { key: "pids", label: "PIDs", render: (row) => listCell(row.pids) },
  { key: "database", label: "Database" },
  { key: "application", label: "Application", wrap: true },
  { key: "blocked_behind_count", label: "Queued Behind", format: "int" },
  { key: "blocked_behind_pids", label: "Queued PIDs", render: (row) => listCell(row.blocked_behind_pids) },
  { key: "finding", label: "Finding", wrap: true },
];

const PG_AUTOVACUUM_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "table_name", label: "Table" },
  { key: "severity", label: "Severity" },
  { key: "dead_tuples", label: "Dead Tuples", format: "int" },
  { key: "vacuum_threshold", label: "Vacuum At", format: "int" },
  /* The headline number: 1.0 means autovacuum should be triggering right now. Null where the table has never
     been analyzed or the collector had no threshold to report — a ratio invented there would rank a table
     nothing is known about above tables that have been measured. */
  { key: "threshold_ratio", label: "× Vacuum Threshold", format: "num2" },
  { key: "dead_tuples_growing", label: "Growing", format: "bool" },
  { key: "dead_tuple_change", label: "Change", format: "int" },
  { key: "mods_since_analyze", label: "Mods Since Analyze", format: "int" },
  { key: "analyze_threshold_ratio", label: "× Analyze Threshold", format: "num2" },
  { key: "inserts_since_vacuum", label: "Inserts Since Vacuum", format: "int" },
  { key: "autovacuum_disabled", label: "AV Disabled", format: "bool" },
  { key: "never_autovacuumed", label: "Never AV'd", format: "bool" },
  { key: "total_gb", label: "Size (GB)", format: "num2" },
  { key: "last_autovacuum", label: "Last Autovacuum", format: "time" },
  { key: "last_autoanalyze", label: "Last Autoanalyze", format: "time" },
];

/* Both counters, side by side and never merged into a worst-of: XIDs and MultiXacts wrap independently, a
   database can be comfortable on one and in trouble on the other, and the remedies differ. */
const PG_WRAPAROUND_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "severity", label: "Severity" },
  { key: "frozen_xid_age", label: "Frozen XID Age", format: "int" },
  { key: "xids_remaining", label: "XIDs Left", format: "int" },
  { key: "pct_toward_wraparound", label: "% to Wraparound", format: "num2" },
  { key: "pct_toward_emergency_vacuum", label: "% to Emergency Vacuum", format: "num1" },
  { key: "window_peak_frozen_xid_age", label: "Peak Age (window)", format: "int" },
  { key: "freezing_is_keeping_up", label: "Keeping Up", format: "bool" },
  { key: "min_multixid_age", label: "MultiXact Age", format: "int" },
  { key: "multixids_remaining", label: "MultiXacts Left", format: "int" },
  { key: "pct_toward_multixact_wraparound", label: "% to MultiXact Wraparound", format: "num2" },
  { key: "pct_toward_multixact_emergency", label: "% to MultiXact Emergency", format: "num1" },
  { key: "allows_connections", label: "Connectable", format: "bool" },
  { key: "measured_at", label: "Measured", format: "time" },
];

/* Every cause, not only the current winner. Four unrelated things produce "autovacuum runs, reports success,
   reclaims nothing", each with a different fix, so `source` and `remedy` are the columns that matter and
   `xmin_age` on its own is the one that leaves the reader where they started. */
const PG_XMIN_COLUMNS = [
  { key: "source", label: "Source" },
  { key: "is_currently_winning", label: "Winning", format: "bool" },
  { key: "xmin_age", label: "xmin Age", format: "int" },
  { key: "peak_xmin_age", label: "Peak Age", format: "int" },
  { key: "holder", label: "Holder", wrap: true },
  { key: "detail", label: "Detail", wrap: true },
  { key: "samples_as_winner", label: "Samples Winning", format: "int" },
  { key: "samples", label: "Samples", format: "int" },
  { key: "pct_of_window_winning", label: "% of Window", format: "num1" },
  { key: "measured_at", label: "Measured", format: "time" },
  { key: "remedy", label: "Remedy", wrap: true },
];

const PG_IO_COLUMNS = [
  { key: "backend_type", label: "Backend" },
  { key: "object_type", label: "Object" },
  { key: "context", label: "Context" },
  { key: "context_meaning", label: "Means", wrap: true },
  { key: "reads", label: "Reads", format: "int" },
  { key: "read_time_ms", label: "Read Time", format: "ms" },
  { key: "avg_read_ms", label: "Avg Read (ms)", format: "num2" },
  { key: "pct_of_total_read_time", label: "% of Read Time", format: "num1" },
  { key: "hits", label: "Hits", format: "int" },
  { key: "hit_pct", label: "Hit %", format: "num1" },
  { key: "extends", label: "Extends", format: "int" },
  { key: "evictions", label: "Evictions", format: "int" },
  { key: "reuses", label: "Reuses", format: "int" },
  /* Blank rather than zero on Aurora, where backends do not write data files and pg_stat_io's whole write side
     is NULL. The read preserves the NULL and the "bool" beside it says which it is, so a blank cell here is
     "not measured" and never "measured zero". */
  { key: "writes", label: "Writes", format: "int" },
  { key: "write_time_ms", label: "Write Time", format: "ms" },
  { key: "write_counters_tracked", label: "Writes Tracked", format: "bool" },
  { key: "stats_reset", label: "Stats Reset", format: "time" },
];

/* The read's own `*_finding` prose travels beside each number, because none of these three is actionable
   from the figure alone — a 99% cache hit ratio is meaningless without knowing the working set, and a
   rollback ratio is a fault only against what the application is supposed to do. */
const PG_DATABASE_COLUMNS = [
  { key: "database", label: "Database" },
  { key: "temp_files", label: "Temp Files", format: "int" },
  { key: "temp_bytes", label: "Temp Bytes", format: "int" },
  { key: "avg_temp_file_bytes", label: "Avg Temp File (bytes)", format: "int" },
  { key: "spill_finding", label: "Spills", wrap: true },
  { key: "cache_hit_pct", label: "Cache Hit %", format: "num2" },
  { key: "cache_finding", label: "Cache", wrap: true },
  { key: "deadlocks", label: "Deadlocks", format: "int" },
  { key: "xact_commit", label: "Commits", format: "int" },
  { key: "xact_rollback", label: "Rollbacks", format: "int" },
  { key: "rollback_pct", label: "Rollback %", format: "num2" },
  { key: "rollback_finding", label: "Rollbacks", wrap: true },
  /* The reset evidence. Without it every total in the row is a lower bound and nothing on screen says so. */
  { key: "counters_were_reset", label: "Reset", format: "bool" },
  { key: "reset_note", label: "Reset Note", wrap: true },
  { key: "sample_count", label: "Samples", format: "int" },
];

/* bloat_pct_estimate and bloat_bytes_estimate are NULL on a suppressed row, which renders as an empty cell
   rather than a zero - and that is the point. estimate_suppression_reason is the column that says why, so it
   is wrapped and kept adjacent rather than pushed to the end of a wide grid. */
const PG_BLOAT_COLUMNS = [
  { key: "database", label: "Database" },
  { key: "schema", label: "Schema" },
  { key: "table", label: "Table" },
  /* sevKey, not statusSev: the band is SERVER-computed (the browser never re-derives one) and it is what
     gives this grid the cue the WPF grid gets from its row-style triggers. A suppressed row comes back
     "Unknown" rather than a severity, so it renders neutral - colouring it would assert the very thing the
     suppression denies. */
  { key: "severity", label: "Severity", sevKey: "severity" },
  { key: "heap_mb", label: "Heap", format: "mb" },
  { key: "bloat_mb_estimate", label: "Bloat (est.)", format: "mb" },
  { key: "bloat_pct_estimate", label: "Bloat % (est.)", format: "num2" },
  { key: "estimate_suppressed", label: "Suppressed", format: "bool" },
  { key: "estimate_suppression_reason", label: "Why Suppressed", wrap: true },
  /* Measured, from the server's own counters - what a suppressed estimate degrades TO rather than to
     nothing. */
  { key: "dead_tuple_pct", label: "Dead Tuple %", format: "num2" },
  { key: "dead_tuple_finding", label: "Dead Tuples", wrap: true },
  { key: "bloat_finding", label: "Finding", wrap: true },
  { key: "mods_since_analyze_pct_of_rows", label: "Modified Since Analyze %", format: "num1" },
  { key: "last_analyzed", label: "Last Analyzed", format: "time" },
  { key: "fillfactor", label: "Fill Factor", format: "int" },
  { key: "fillfactor_note", label: "Fill Factor Note", wrap: true },
  { key: "toast_bytes", label: "TOAST Bytes", format: "int" },
  { key: "toast_note", label: "TOAST", wrap: true },
  { key: "index_bytes", label: "Index Bytes", format: "int" },
  { key: "heap_bytes_growth_in_window", label: "Heap Growth", format: "int" },
  { key: "pgstattuple_available", label: "pgstattuple", format: "bool" },
  { key: "exact_measurement_command", label: "Measure It Exactly", wrap: true },
  { key: "sample_count", label: "Samples", format: "int" },
];

/* Both scan figures are shown. The lifetime count is what anyone querying the server directly would see, so
   omitting it would make this grid look like it disagreed with psql; the windowed one is the answer to the
   question actually being asked, and only stored history can produce it. */
const PG_INDEX_USAGE_COLUMNS = [
  { key: "database", label: "Database" },
  { key: "schema", label: "Schema" },
  { key: "table", label: "Table" },
  { key: "index", label: "Index" },
  /* Server-computed, same as the bloat grid: INVALID is Critical, an unscanned index with no structural
     blocker is Warning, and an index we have not watched for two samples is Healthy rather than Warning -
     too-early-to-say must not look like a finding. */
  { key: "severity", label: "Severity", sevKey: "severity" },
  { key: "index_mb", label: "Size", format: "mb" },
  { key: "scans_in_window", label: "Scans in Window", format: "int" },
  { key: "total_scans_since_stats_reset", label: "Lifetime Scans", format: "int" },
  { key: "last_scan", label: "Last Scan", format: "time" },
  { key: "last_scan_note", label: "Last Scan Note", wrap: true },
  { key: "droppability_finding", label: "Can It Go?", wrap: true },
  { key: "cost_finding", label: "What It Costs", wrap: true },
  /* The droppability facts, individually, so the grid can be sorted and filtered on them rather than only
     read as prose. supports_constraint is the one that most often makes a zero-scan index untouchable. */
  { key: "is_valid", label: "Valid", format: "bool" },
  { key: "is_primary_key", label: "Primary Key", format: "bool" },
  { key: "is_unique", label: "Unique", format: "bool" },
  { key: "supports_constraint", label: "Backs Constraint", format: "bool" },
  { key: "is_replica_identity", label: "Replica Identity", format: "bool" },
  { key: "is_partial", label: "Partial", format: "bool" },
  { key: "is_expression", label: "Expression", format: "bool" },
  { key: "index_method", label: "Method" },
  { key: "tuples_read", label: "Tuples Read", format: "int" },
  { key: "tuples_fetched", label: "Tuples Fetched", format: "int" },
  { key: "blocks_hit", label: "Blocks Hit", format: "int" },
  { key: "stats_were_reset_in_window", label: "Stats Reset", format: "bool" },
  { key: "sample_count", label: "Samples", format: "int" },
  { key: "index_definition", label: "Definition", wrap: true },
];

/* peak_horizon_age is rendered rather than formatted, and that is the one non-negotiable cell on this grid:
   -1 means the session pinned NOTHING - no snapshot and no transaction id in any sample - and it is a
   MEASURED finding rather than a missing value. `format: "int"` would print "-1", which reads as a small age
   next to a long duration and is precisely the misreading that gets a harmless backend killed. The WPF grid
   renders the same sentinel as the same words for the same reason. */
const PG_SESSION_STATE_COLUMNS = [
  { key: "pid", label: "PID", format: "int" },
  /* The collector's synthetic (backend_start, pid) identity. Shown because a pid alone is not one - pids are
     reused, and this is the id that matches the same backend on the blocking grid. */
  { key: "backend_id", label: "Backend", mono: true },
  /* sevKey, not statusSev, and the same shape the bloat and index grids use: the band is SERVER-computed
     (R1 - the browser never re-derives one) and it names one of the four house words the shared sev-*
     CSS defines. The read returned a private lower-case vocabulary in its first draft, which would have
     attached sev-critical / sev-info / sev-none - classes that match no rule, so the badge would have
     rendered UNSTYLED rather than failing. That is the kind of defect that ships, so the fix went into
     the read rather than being worked around here. */
  { key: "severity", label: "Severity", sevKey: "severity" },
  { key: "database", label: "Database" },
  { key: "username", label: "User" },
  { key: "application_name", label: "Application" },
  { key: "client_addr", label: "Client" },
  { key: "backend_type", label: "Backend Type" },
  { key: "last_state", label: "State" },
  {
    key: "peak_horizon_age",
    label: "Pinned Horizon (peak)",
    render: (row) =>
      document.createTextNode(
        row.peak_horizon_age == null
          ? "—"
          : Number(row.peak_horizon_age) < 0
            ? "pins nothing"
            : Number(row.peak_horizon_age).toLocaleString() + " transactions",
      ),
  },
  { key: "pinned_the_horizon", label: "Pinned Anything", format: "bool" },
  /* The sample counts, not a flag: a session that was the oldest holder once in a hundred captures was
     momentarily at the front of a queue every write transaction passes through, and one that held it in
     ninety-eight is why vacuum reclaims nothing. Those are opposite findings. */
  { key: "horizon_holder_samples", label: "Holder Samples", format: "int" },
  { key: "idle_in_transaction_samples", label: "Idle-In-Xact Samples", format: "int" },
  { key: "sample_count", label: "Samples", format: "int" },
  /* PEAKS, not averages - averaging a hundred samples of a transaction that grew monotonically reports
     about half of what happened.

     The read's own pre-formatted strings are used for the two it ships, rather than `format: "ms"` over the
     raw milliseconds: those columns carry the -1 sentinel, and the read already renders it as "not
     measured". The third has no string twin, so it goes through sentinelCell for the same reason. */
  { key: "peak_xact_duration", label: "Peak Xact" },
  { key: "peak_state_duration", label: "Peak State" },
  /* Backend age against transaction age: a connection created ten minutes ago that has been idle in
     transaction for all ten is a pool handing out a session nobody finished with, while a three-day-old
     worker holding one for ten minutes is a code path that forgot to commit. Same duration, different bug. */
  { key: "backend_age", label: "Backend Age" },
  {
    key: "peak_query_duration_ms",
    label: "Peak Query",
    render: (row) => sentinelCell(row.peak_query_duration_ms, fmtMs),
  },
  /* -1 means the session held no snapshot / no transaction id, which is an absence rather than a small age. */
  {
    key: "peak_xmin_age",
    label: "Peak xmin Age",
    render: (row) => sentinelCell(row.peak_xmin_age, (v) => Number(v).toLocaleString()),
  },
  {
    key: "peak_xid_age",
    label: "Peak XID Age",
    render: (row) => sentinelCell(row.peak_xid_age, (v) => Number(v).toLocaleString()),
  },
  { key: "last_wait_event_type", label: "Wait Type" },
  { key: "last_wait_event", label: "Wait Event" },
  /* The leading SQL keyword, whitelisted at collection - NOT a truncation of the statement. No raw query
     text is stored anywhere: pg_stat_activity.query carries literal parameter values. */
  { key: "last_command_tag", label: "Command" },
  { key: "last_query_id", label: "Query ID", mono: true },
  { key: "query_id_note", label: "Query ID Note", wrap: true },
  { key: "finding", label: "Finding", wrap: true },
  /* Instance context from this backend's most recent sample: two idle-in-transaction sessions out of six
     connections is a different server from two out of four thousand. */
  { key: "idle_in_transaction_on_instance", label: "Idle In Xact (instance)", format: "int" },
  { key: "active_sessions_on_instance", label: "Active (instance)", format: "int" },
  { key: "sessions_on_instance", label: "Sessions (instance)", format: "int" },
  { key: "reportable_sessions_on_instance", label: "Reportable (instance)", format: "int" },
  { key: "state_was_redacted", label: "Redacted", format: "bool" },
  { key: "capture_was_truncated", label: "Capture Truncated", format: "bool" },
  { key: "first_seen_at", label: "First Seen", format: "time" },
  { key: "last_seen_at", label: "Last Seen", format: "time" },
];

const PG_SLOT_COLUMNS = [
  { key: "slot_name", label: "Slot" },
  { key: "severity", label: "Severity" },
  { key: "slot_type", label: "Type" },
  { key: "plugin", label: "Plugin" },
  { key: "database_name", label: "Database" },
  { key: "is_active", label: "Active", format: "bool" },
  { key: "wal_status", label: "WAL Status" },
  { key: "retained_wal_gb", label: "Retained WAL (GB)", format: "num2" },
  { key: "retained_wal_growth_gb_per_hour", label: "Growth (GB/h)", format: "num2" },
  { key: "safe_wal_size_bytes", label: "Safe WAL (bytes)", format: "int" },
  { key: "xmin_age", label: "xmin Age", format: "int" },
  { key: "catalog_xmin_age", label: "Catalog xmin Age", format: "int" },
  { key: "inactive_since", label: "Inactive Since", format: "time" },
  { key: "invalidation_reason", label: "Invalidated", wrap: true },
  { key: "conflicting", label: "Conflicting", format: "bool" },
];
