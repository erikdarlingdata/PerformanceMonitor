/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, readToolWithinKeptHistory, keptWindowStrip, mount, truncate, loadingStrip, readErrorStrip, emptyStrip, disclosure, noticeStrip, chartNoteText, fmtInt, localTime, windowFromHours, queryWaitFilter, setQueryWaitFilter } from "../../util.js";
import { setPanelSignal, getPanelSignal, VIZ } from "../../panels.js";
import { activeQueriesSnapshotSlot, SNAPSHOT_ROW_CAP } from "../active-snapshots.js";
import { activeQueriesLiveBar } from "../active-live.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS } from "../../charts.js";
import { statsHistoryColumn } from "../stats-history.js";
import { windowParams } from "../../time-slicer.js";
import { activePlanColumns, planColumn } from "../plan-viewer.js";
import { comparePick, compareControl } from "../top-compare.js";
import { codeDisclosure, listCell, panelShell, discontinuityNotes, pickerControl, topRankingLabel, rankedParams, withTagPick, tagFilterPicker, tagColumn, compareDrawn, rankedCard, slicedSlot, tablePanel, linePanel, TOP_QUERY_COLUMNS, TOP_QUERY_GROUPS } from "./shared.js";

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
  return rankedCard("queries", (ranking, picker, w, onRow) => topQueriesCard(server, ctx, ranking, picker, w, onRow), { server, ctx });
}

/* One draw of the card for one ranking (#5226): rankedCard draws it again on a pick, so the title and the rows follow the choice. */
/* `ctx` is the page range (the per-query trend below reads it whole); `w` is the window the grid reads, the page range or, while the time slicer
   holds a selection (#6301), that selection; `onRow` is the slicer's overlay-on-select. */
function topQueriesCard(server, ctx, ranking, picker, w = ctx, onRow = null) {
  const { panel, body } = panelShell(
    "Top Queries by " + topRankingLabel(ranking),
    w.label + ", with a per-collection trend for the query you pick",
    2,
    "get_top_queries_by_cpu",
    null,
    picker
  );
  const signal = getPanelSignal();
  (async () => {
    const res = await readToolWithinKeptHistory(
      "get_top_queries_by_cpu",
      rankedParams(windowParams({ server, hours: w.hours, top: 20, detail: "full" }, w), ranking, "queries"),
      signal
    );
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, [keptWindowStrip(res), emptyStrip(res.message)]);

    const queries = res.data.queries || [];
    const parts = [
      VIZ.table(res.data, {
        rowsKey: "queries",
        columns: [...TOP_QUERY_COLUMNS, planColumn(server), statsHistoryColumn("queries", server, w)],
        groups: TOP_QUERY_GROUPS.groups,
        defaultGroups: TOP_QUERY_GROUPS.defaultGroups,
        ...(onRow ? { onRow } : {}),
        emptyText:
          "No query stats in this window. Delta-based collection needs at least two cycles (~30 minutes) " +
          "before it reports non-zero values.",
      }),
    ];
    /* #4231: this composite calls VIZ.table directly rather than going through tablePanel()/renderPanel, so it
       misses loadPanel's automatic noteKey handling (#3278) and has to render `truncation_note` itself —
       same field, same meaning, as the plain tablePanel() panels below it on this page. #5299: a reads ranking past what raw keeps carries
       `retention_notice` as well, and readTool already put that sentence in `truncation_note`'s place (util.js localizeWindowNote), so this
       draws the one window note and the retention notice is not drawn a second time. */
    if (typeof res.data.truncation_note === "string" && res.data.truncation_note.trim()) {
      parts.unshift(noticeStrip(res.data.truncation_note));
    }
    const kept = keptWindowStrip(res);
    if (kept) parts.unshift(kept);

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
      const picker = pickerControl("Query", options, (i) => drawQueryTrend(chartSlot, server, ctx, trendable[Number(i)].query, signal));
      parts.push(el("div", { class: "picker-row" }, [picker]), chartSlot);
      drawQueryTrend(chartSlot, server, ctx, trendable[0].query, signal);
    } else if (queries.length) {
      parts.push(
        emptyStrip(
          "None of these rows carries both a query hash and a database name, which is what the query trend " +
            "keys on, so there is nothing here to trend."
        )
      );
    }
    mount(body, parts);
  })();
  return panel;
}

async function drawQueryTrend(slot, server, ctx, query, signal) {
  mount(slot, loadingStrip());
  const trend = await readToolWithinKeptHistory("get_query_trend", {
    server,
    query_hash: query.query_hash,
    database_name: query.database_name,
    hours: ctx.hours,
  }, signal);
  if (trend.kind === "aborted") return;
  if (trend.kind !== "data") {
    mount(slot, trend.kind === "empty" ? [keptWindowStrip(trend), emptyStrip(trend.message)] : readErrorStrip(trend.message));
    return;
  }

  /* #2353: the read routes to the hourly rollup once the window reaches past the raw tier's four-day
     retention, and reports which tier answered and how far it really reached. Those sentences are rendered
     rather than dropped, because this page's range goes to 7 days, past that tier: without them the DOP and
     plan-hash columns simply go blank, and a blank column reads as "nothing to see" rather than "not measured
     here". */
  const notes = [];
  /* #6359: what a point covers, from the read's own width fields: its `aggregate_note` is written for an MCP caller. */
  const pointNote = chartNoteText(trend.data, "summed");
  if (pointNote) notes.push(pointNote);
  /* #3653 item 17: the window floor is `window_truncated` — the page dialect's `truncated` (a limit biting,
     which this read has none of) is a different fact with the opposite remedy, and the two used to share a
     spelling. Reading the old key here would silently drop this notice: undefined is falsy. */
  if (trend.data.window_truncated) {
    notes.push(
      "History for this query starts " +
        trend.data.effective_hours_back +
        " hours back, not the " +
        trend.data.hours_back +
        " the window asked for — earlier collections have aged out of the tier that answered."
    );
  }
  /* #3653 A5: the payload's baseline discontinuities, beside the #2353 coverage notes above. */
  notes.push(...discontinuityNotes(trend.data));

  mount(slot, [
    keptWindowStrip(trend),
    notes.length ? noticeStrip(notes.join(" ")) : null,
    zoomableLineChart({
      atTime: { server },
      points: trend.data.trend || [],
      xKey: "collection_time",
      series: [
        { key: "avg_cpu_ms", label: "Avg CPU", color: SERIES_COLORS[0] },
        { key: "avg_elapsed_ms", label: "Avg Elapsed", color: SERIES_COLORS[1] },
      ],
      formatValue: (v, o) => fmtInt(v, o) + " ms",
      unit: "ms",
      /* #2802: axis spans the requested window (ctx.hours ending now). When the read is #2353-truncated the data
         starts later than the window and plots toward the right; the truncation notice above already says so.
         A narrowed read spans the hours it answered for. */
      ...windowFromHours(trend.keptHours || ctx.hours),
    }, "query-trend|" + query.query_hash, chartZoomScope(ctx.hours)),
    VIZ.table(trend.data, {
      rowsKey: "trend",
      columns: QUERY_TREND_COLUMNS,
      emptyText: "No collections recorded this query in the window the read could reach.",
    }),
  ]);
}

/** A grid drawn again on a tag pick: `build(picker)` draws the card with the tag filter under its title. Each draw aborts the one before it. */
export function tagFilteredCard(kind, build, cmp = null) {
  const slot = el("div", {}, []);
  slot.style.display = "contents";
  let drawing = null;
  const draw = () => {
    if (drawing) drawing.abort();
    const mine = new AbortController();
    drawing = mine;
    const outer = getPanelSignal();
    if (outer) outer.addEventListener("abort", () => mine.abort(), { once: true });
    const picker = el("div", { class: "picker-row" }, [tagFilterPicker(kind, draw), ...(cmp ? [compareControl(draw, slot)] : [])]);
    setPanelSignal(mine.signal);
    const card = cmp && comparePick.value !== "off" ? compareDrawn(kind, cmp, null, "Query Store", picker, mine.signal, () => build(picker)) : build(picker);
    setPanelSignal(outer);
    mount(slot, [card]);
  };
  draw();
  return slot;
}

/**
 * The Active Queries and Query Store cards under a time slicer (#6301 slice 2a): `tagFilteredCard` with the slicer over it. `build(picker, w,
 * onRow)` draws the grid for `w`, the page range or the slicer's selection, and `onRow` is the Query Store row pick that overlays that query's
 * history on the strip (null for Active Queries). `compare` hands the Compare picker the narrowed window, as the ranked cards do.
 */
export function slicedTagCard(kind, cmp, build, compare = false) {
  return slicedSlot(kind, cmp, true, (w, slicer) =>
    tagFilteredCard(kind, (picker) => build(picker, w, slicer ? slicer.onRow(null) : null), compare ? { server: cmp.server, ctx: w } : null)
  );
}

/* The three Performance-Trends reads all return {time, value, execution_count, executions_per_second} and
   share this series: `value` is milliseconds per second. The execution rate is NOT charted beside it — a
   count and a millisecond on one y-domain is the mistake the CPU chart's dropped idle_cpu series exists to
   avoid — it gets its own panel off the same payload. */
export const DURATION_SERIES = [{ key: "value", label: "Avg duration" }];

/* #2484: executions/sec, the viewer's fourth Performance-Trends chart. Charts executions_per_second and
   not execution_count: the two are the same quantity, and the integer one reports ZERO on any server
   running under one execution a second, which would draw a flat line along the axis for a server that is
   simply quiet rather than idle. */
export const EXECUTION_RATE_SERIES = [{ key: "executions_per_second", label: "Executions/sec" }];

/* The Active Queries grid. With a wait filter set (a wait row or the wait trend chart opened this tab on a wait) it reads
   that wait only, at Lite's drill-down cap of 500 rows so a one-hour drill holds the clicked minute, and a strip above the
   grid names the wait as text and clears the filter. The heading stays "Active Queries" (the scroll finder matches it).
   Unfiltered, the read is the 50-row page it always was. */
export function activeQueriesPanel(server, ctx) {
  const wait = queryWaitFilter(server);
  /* #6302: the snapshot picker (active-snapshots.js) reads the window's snapshot times, under the tag pick and the wait filter the grid
     uses, and draws the grid for the latest one, or the one picked; "All snapshots in window" draws it as it was. */
  /* #6301: under the time slicer. `w` is the page range or the slicer's selection; the snapshot list and the grid both read it. */
  const card = slicedTagCard("active", { server, ctx }, (tagPicker, w) =>
    activeQueriesSnapshotSlot(server, w, wait, withTagPick({}, "active"), (snapshotTime, snapPicker) =>
      activeQueriesGrid(server, w, wait, snapshotTime, snapPicker ? el("div", {}, [tagPicker, snapPicker]) : tagPicker)
    )
  );
  /* #6386: the live read ("Run live now") sits above the stored snapshots, in the stored grid's columns. */
  const wrap = el("div", {}, [activeQueriesLiveBar(server, ACTIVE_COLUMNS), card]);
  wrap.style.display = "contents";
  return wrap;
}

function activeQueriesGrid(server, ctx, wait, snapshotTime, control) {
  /* The "All snapshots" grid is an event list, cut to the selection; a picked snapshot is one instant and is read as it is. */
  const base = windowParams(wait ? { server, hours: ctx.hours, limit: 500, wait_type: wait } : { server, hours: ctx.hours, limit: 50 }, ctx, !snapshotTime);
  const panel = tablePanel(
    "Active Queries",
    "get_active_queries",
    snapshotTime
      ? withTagPick({ ...base, limit: SNAPSHOT_ROW_CAP, collection_time: snapshotTime }, "active")
      : withTagPick(base, "active"),
    "queries",
    [...ACTIVE_COLUMNS, ...activePlanColumns(server)],
    snapshotTime ? "snapshot at " + localTime(snapshotTime) : wait ? ctx.label + ", waiting on " + wait : ctx.label,
    snapshotTime
      ? "No active queries" + (wait ? " waiting on " + wait : "") + " in this snapshot."
      : wait ? "No active queries waiting on " + wait + " were captured in this window." : "No active-query snapshots in this window.",
    2,
    null,
    null,
    null,
    control
  );
  if (wait) {
    const strip = el("div", { class: "strip notice wait-filter-strip" }, [
      el("span", { text: "Only requests waiting on " + wait + ". " }),
      el("button", {
        type: "button",
        class: "btn small",
        text: "Show all active queries",
        onClick: () => {
          setQueryWaitFilter(server, "");
          /* The same hash fires no hashchange, so the router is told to rebuild the tab. */
          window.dispatchEvent(new Event("hashchange"));
        },
      }),
    ]);
    panel.insertBefore(strip, panel.lastChild);
  }
  return panel;
}

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
  { key: "spills", label: "Spilled Pages", format: "int" },
  { key: "min_dop", label: "Min DOP", format: "int" },
  { key: "max_dop", label: "Max DOP", format: "int" },
  { key: "query_plan_hash", label: "Plan Hash", mono: true },
];

/* #2484: the regression grid. Baseline and recent sit BESIDE each other for each metric rather than being
   collapsed into the percent alone -- a 300% regression on a query that went from 1 ms to 4 ms is not the
   same finding as one that went from 1 s to 4 s, and the percent alone cannot tell them apart. Extra
   duration is the ranking key and the column that says whether the regression matters at all. */
/* Stays local: this page's column set differs from the catalog entry's. */
/* #5506: Aborted (timeouts, cancels) and Exception (errors) runs sit beside the Regular cost, as two more groups. Their
   count and the reason the row is listed stay in the always-visible columns, so a timeout row is never hidden. */
export const QUERY_STORE_REGRESSION_GROUPS = { groups: ["CPU", "Executions and plans", "Timeouts (Aborted)", "Errors (Exception)"], defaultGroups: [] };

export const QUERY_STORE_REGRESSION_COLUMNS = [
  { key: "severity", label: "Severity" },
  { key: "regression_reason", label: "Why" },
  { key: "database_name", label: "Database" },
  { key: "query_text", label: "Query", valueList: false, render: (r) => codeDisclosure(r.query_text) },
  { key: "query_id", label: "Query ID", format: "int" },
  { key: "recent_aborted_count", label: "Recent Timeouts", format: "int" },
  { key: "recent_exception_count", label: "Recent Errors", format: "int" },
  { key: "additional_duration_ms", label: "Extra Duration", format: "ms" },
  { key: "duration_regression_percent", label: "Duration +%", format: "num1" },
  { key: "baseline_duration_ms", label: "Baseline Duration", format: "ms" },
  { key: "recent_duration_ms", label: "Recent Duration", format: "ms" },
  { key: "cpu_regression_percent", label: "CPU +%", format: "num1" },
  { key: "baseline_cpu_ms", label: "Baseline CPU", group: "CPU", format: "ms" },
  { key: "recent_cpu_ms", label: "Recent CPU", group: "CPU", format: "ms" },
  { key: "baseline_exec_count", label: "Base Execs", group: "Executions and plans", format: "int" },
  { key: "recent_exec_count", label: "Recent Execs", group: "Executions and plans", format: "int" },
  /* A plan count that moved between the two sides is the first thing to check: a query that regressed
     while gaining a plan is usually a plan-choice problem, not a data one. */
  { key: "baseline_plan_count", label: "Baseline Plans", group: "Executions and plans", format: "int" },
  { key: "recent_plan_count", label: "Recent Plans", group: "Executions and plans", format: "int" },
  { key: "baseline_aborted_count", label: "Baseline Timeouts Execs", group: "Timeouts (Aborted)", format: "int" },
  { key: "recent_aborted_count", label: "Recent Timeouts Execs", group: "Timeouts (Aborted)", format: "int" },
  { key: "baseline_aborted_cpu_ms", label: "Baseline Timeouts CPU", group: "Timeouts (Aborted)", format: "ms" },
  { key: "recent_aborted_cpu_ms", label: "Recent Timeouts CPU", group: "Timeouts (Aborted)", format: "ms" },
  { key: "baseline_aborted_duration_ms", label: "Baseline Timeouts Duration", group: "Timeouts (Aborted)", format: "ms" },
  { key: "recent_aborted_duration_ms", label: "Recent Timeouts Duration", group: "Timeouts (Aborted)", format: "ms" },
  { key: "baseline_aborted_per_hour", label: "Baseline Timeouts Per Hour", group: "Timeouts (Aborted)", format: "num1" },
  { key: "recent_aborted_per_hour", label: "Recent Timeouts Per Hour", group: "Timeouts (Aborted)", format: "num1" },
  { key: "baseline_exception_count", label: "Baseline Errors Execs", group: "Errors (Exception)", format: "int" },
  { key: "recent_exception_count", label: "Recent Errors Execs", group: "Errors (Exception)", format: "int" },
  { key: "baseline_exception_cpu_ms", label: "Baseline Errors CPU", group: "Errors (Exception)", format: "ms" },
  { key: "recent_exception_cpu_ms", label: "Recent Errors CPU", group: "Errors (Exception)", format: "ms" },
  { key: "baseline_exception_duration_ms", label: "Baseline Errors Duration", group: "Errors (Exception)", format: "ms" },
  { key: "recent_exception_duration_ms", label: "Recent Errors Duration", group: "Errors (Exception)", format: "ms" },
  { key: "baseline_exception_per_hour", label: "Baseline Errors Per Hour", group: "Errors (Exception)", format: "num1" },
  { key: "recent_exception_per_hour", label: "Recent Errors Per Hour", group: "Errors (Exception)", format: "num1" },
  { key: "baseline_regular_per_hour", label: "Baseline Execs/Hour", group: "Executions and plans", format: "num1" },
  { key: "recent_regular_per_hour", label: "Recent Execs/Hour", group: "Executions and plans", format: "num1" },
  { key: "last_execution_time", label: "Last Exec", format: "time" },
];

/* #2484: the heatmap grid, flattened. One row per cell, chronological. Query Count is DISTINCT QUERIES in
   the cell, not executions -- forty different queries running at 10-100ms is a different finding from one
   query running forty times, and the column label has to keep them apart. The top query is the most-executed
   one in the cell, which is what the desktop shows on hover. */
export const QUERY_HEATMAP_COLUMNS = [
  { key: "time_bucket", label: "Time Bin", format: "time" },
  { key: "bucket_label", label: "Magnitude" },
  { key: "query_count", label: "Queries", format: "int" },
  { key: "top_query_text", label: "Most-Executed Query", valueList: false, render: (r) => codeDisclosure(r.top_query_text) },
  { key: "top_query_hash", label: "Query Hash" },
];

export const QUERY_STORE_COLUMNS = [
  { key: "database_name", label: "Database" },
  tagColumn(),
  { key: "query_text", label: "Query", valueList: false, render: (r) => codeDisclosure(r.query_text) },
  { key: "query_id", label: "Query ID", format: "int" },
  { key: "plan_id", label: "Plan ID", format: "int" },
  { key: "execution_count", label: "Execs", format: "int" },
  { key: "avg_duration_ms", label: "Avg Duration", format: "ms" },
  { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
  { key: "avg_rowcount", label: "Avg Rows", format: "num1" },
  { key: "last_execution_time", label: "Last Exec", format: "time" },
  { key: "replica_role", label: "Replica" },
];

/*
 * #3797: the Query Store CLUTTER rows, and the per-server overhead block beside them.
 *
 * The verdict is COLOURED, not re-derived: `sevKey` points the cell at the band the server already computed
 * (R1 — the browser never recomputes a threshold), and `verdict_reasons` is shown as the token list that
 * raised it so the colour is never the only evidence. `excluded_reason` is its own column rather than a
 * footnote: a replica reads Unknown/excluded by architecture, and a reader who cannot see WHY on the row
 * will file the absence as a defect. `recommendations` is the SERVER's prose (QueryStoreClutter.Recommendations
 * — the same sentences the MCP payload and the WPF grid carry), behind a disclosure because each one is a
 * paragraph; it is never re-worded here.
 */
export const QS_CLUTTER_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "verdict", label: "Verdict", sevKey: "verdict" },
  { key: "verdict_reasons", label: "Reasons", render: (r) => listCell(r.verdict_reasons) },
  /* Excluded is per-ROW and architectural (readonly_reason bit 8). The dash is the ordinary case. */
  { key: "excluded_reason", label: "Excluded Because" },
  { key: "read_cost.runs_slowest_pct", label: "Slowest on %", format: "num1" },
  { key: "read_cost.slowest_share_pct", label: "Share of Pass %", format: "num1" },
  { key: "read_cost.slowest_item_ms_p50", label: "Slowest ms p50", format: "ms" },
  { key: "read_cost.dominance_ratio", label: "vs Others", format: "num2" },
  { key: "plan_churn.plans_per_query_p95", label: "Plans/Query p95", format: "int" },
  { key: "plan_churn.plans_per_query_max", label: "Plans/Query Max", format: "int" },
  { key: "plan_churn.new_plans_per_day", label: "New Plans/Day", format: "num1" },
  { key: "plan_churn.never_seen_twice_fraction", label: "One-Shot Fraction", format: "num2" },
  { key: "plan_churn.distinct_plans", label: "Plans", format: "int" },
  { key: "config.actual_state", label: "State" },
  /* V137 (#3796). A dash here is a health capture older than the rung — never asked — which the payload's
     capture_mode_note spells out; it is NOT the engine's NONE, and the two must not look alike. */
  { key: "config.query_capture_mode", label: "Capture Mode" },
  { key: "config.max_plans_per_query", label: "Max Plans/Query", format: "int" },
  { key: "config.stale_query_threshold_days", label: "Stale Days", format: "int" },
  { key: "config.pct_of_cap", label: "% of Cap", format: "num1" },
  { key: "config.captured_at", label: "Options Captured", format: "time" },
  {
    key: "recommendations",
    label: "Recommendation",
    wrap: true,
    render: (r) => {
      const lines = Array.isArray(r.recommendations) ? r.recommendations : [];
      if (!lines.length) return document.createTextNode("—");
      return disclosure(lines[0], el("div", {}, lines.map((line) => el("p", { text: line }))), { max: 90 });
    },
  },
];

/* The per-server overhead proxy's wait rows. wait_ms_per_hour is the server's own rating over the seconds
   those rows measured; a null is an unknowable interval, which the generic formatter renders as a dash. */
export const QS_OVERHEAD_WAIT_COLUMNS = [
  { key: "wait_type", label: "Wait Type" },
  { key: "wait_ms_per_hour", label: "Wait ms/hour", format: "num1" },
  { key: "wait_ms_total", label: "Wait ms (window)", format: "int" },
  { key: "waiting_tasks_total", label: "Waiting Tasks", format: "int" },
  { key: "measured_seconds", label: "Measured s", format: "int" },
  { key: "rows_observed", label: "Rows", format: "int" },
  { key: "rows_unknowable", label: "Unrateable Rows", format: "int" },
  { key: "last_observed", label: "Last Seen", format: "time" },
];

/* The Query Store memory clerk, the second half of the per-server overhead proxy. All three keys are null
   together — the clerk never appeared in a capture — which is exactly when vizStat prefers the panel's
   sentence to a row of em-dashes, and the payload's own `note` (shown as the panel's notice) says whether
   that is "no captures at all" or "below the collector's top 25", which are different facts. */
export const QS_CLERK_STATS = [
  { key: "qs_overhead.memory_clerk.latest_memory_mb", label: "Latest MB", format: "num1" },
  { key: "qs_overhead.memory_clerk.max_memory_mb_in_window", label: "Window Max MB", format: "num1" },
  { key: "qs_overhead.memory_clerk.latest_clerk_captured_at", label: "Clerk Last Seen", format: "time" },
];

export const LONG_QUERY_GROUPS = { groups: ["I/O and rows", "Session"], defaultGroups: [] };

export const LONG_QUERY_COLUMNS = [
  { key: "event_time", label: "Time", format: "time" },
  tagColumn(),
  { key: "statement", label: "Statement", valueList: false, render: (r) => codeDisclosure(r.statement) },
  { key: "event_type", label: "Event Type" },
  { key: "duration_ms", label: "Duration", format: "ms" },
  { key: "cpu_ms", label: "CPU", format: "ms" },
  { key: "logical_reads", label: "Logical Reads", group: "I/O and rows", format: "int" },
  { key: "physical_reads", label: "Physical Reads", group: "I/O and rows", format: "int" },
  { key: "writes", label: "Writes", group: "I/O and rows", format: "int" },
  { key: "row_count", label: "Rows", group: "I/O and rows", format: "int" },
  { key: "result", label: "Result" },
  { key: "database_name", label: "Database" },
  { key: "object_name", label: "Object" },
  { key: "session_id", label: "SPID", group: "Session", format: "int" },
  { key: "client_app_name", label: "App", group: "Session" },
  { key: "server_principal_name", label: "Login", group: "Session" },
  { key: "query_hash", label: "Query Hash" },
];

/* The Script, Executable and Revertable columns the desktop grid adds are not here: get_plan_corrections
   returns no implementation script or action flags, and the page does not rebuild them. */
export const PLAN_CORRECTION_GROUPS = { groups: ["Plans", "Plan metrics", "Lifecycle"], defaultGroups: [] };

/* #5644 W21: the web read collapses the collector's per-cycle re-captures to one row per recommendation, so the old
   "Collected" column is the recommendation's LAST capture, with its first capture and how many captures the window held beside it. */
export const PLAN_CORRECTION_COLUMNS = [
  { key: "first_seen", label: "First seen", format: "time" },
  { key: "collection_time", label: "Last seen", format: "time" },
  { key: "capture_count", label: "Captures", format: "int" },
  { key: "query_text", label: "Query", valueList: false, render: (r) => codeDisclosure(r.query_text) },
  { key: "database_name", label: "Database" },
  { key: "recommendation_state", label: "State" },
  { key: "recommendation_state_reason", label: "State Reason", wrap: true },
  { key: "recommendation_reason", label: "Reason", wrap: true },
  { key: "score", label: "Score", format: "int" },
  { key: "estimated_gain_seconds", label: "Est. gain (s)", format: "num1" },
  { key: "query_id", label: "Query ID", format: "int" },
  { key: "regressed_plan_id", label: "Regressed Plan", group: "Plans", format: "int" },
  { key: "last_good_plan_id", label: "Last Good Plan", group: "Plans", format: "int" },
  { key: "last_good_plan_forcing_type", label: "Forcing Type", group: "Plans" },
  { key: "last_good_plan_is_forced", label: "Forced", group: "Plans", format: "bool" },
  { key: "last_good_plan_force_failure_reason", label: "Force Failure", group: "Plans", wrap: true },
  { key: "regressed_plan_execution_count", label: "Regressed Execs", group: "Plan metrics", format: "int" },
  { key: "regressed_plan_cpu_time_average_ms", label: "Regressed CPU (ms)", group: "Plan metrics", format: "num2" },
  { key: "last_good_plan_execution_count", label: "Last Good Execs", group: "Plan metrics", format: "int" },
  { key: "last_good_plan_cpu_time_average_ms", label: "Last Good CPU (ms)", group: "Plan metrics", format: "num2" },
  { key: "valid_since", label: "Valid Since", group: "Lifecycle", format: "time" },
  { key: "last_refresh", label: "Last Refresh", group: "Lifecycle", format: "time" },
  { key: "execute_action_initiated_by", label: "Executed By", group: "Lifecycle" },
  { key: "execute_action_initiated_time", label: "Executed At", group: "Lifecycle", format: "time" },
  { key: "revert_action_initiated_by", label: "Reverted By", group: "Lifecycle" },
  { key: "revert_action_initiated_time", label: "Reverted At", group: "Lifecycle", format: "time" },
];

export const AUTO_TUNING_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "force_last_good_plan_desired_state", label: "Desired" },
  { key: "force_last_good_plan_actual_state", label: "Actual" },
  { key: "force_last_good_plan_reason", label: "Reason", wrap: true },
  { key: "as_of", label: "As of", format: "time" },
];

/* The Active Queries grid, in the desktop viewer's column order and wording (Collected and Query Text lead, the anchor rule; the plan buttons are added where the grid is built: activePlanColumns). */
const ACTIVE_COLUMNS = [
  { key: "collection_time", label: "Collected", format: "time" },
  { key: "query_text", label: "Query Text", valueList: false, render: (r) => codeDisclosure(r.query_text) },
  { key: "session_id", label: "SPID", format: "int" },
  tagColumn(),
  { key: "database_name", label: "Database" },
  { key: "login_name", label: "Login" },
  { key: "host_name", label: "Host" },
  { key: "program_name", label: "Program" },
  { key: "status", label: "Status" },
  { key: "elapsed_time_formatted", label: "Elapsed" },
  { key: "cpu_time_ms", label: "CPU (ms)", format: "int" },
  { key: "logical_reads", label: "Logical Reads", format: "int" },
  { key: "reads", label: "Reads", format: "int" },
  { key: "writes", label: "Writes", format: "int" },
  { key: "wait_type", label: "Wait Type" },
  { key: "wait_time_ms", label: "Wait (ms)", format: "int" },
  { key: "wait_resource", label: "Wait Resource" },
  { key: "blocking_session_id", label: "Blocking", format: "int" },
  { key: "dop", label: "DOP", format: "int" },
  { key: "parallel_worker_count", label: "Workers", format: "int" },
  { key: "granted_query_memory_gb", label: "Memory (GB)", format: "num2" },
  { key: "transaction_isolation_level", label: "Isolation" },
  { key: "open_transaction_count", label: "Open Tran", format: "int" },
  { key: "percent_complete", label: "% Done", format: "num1" },
  { key: "query_hash", label: "Query Hash" },
];

