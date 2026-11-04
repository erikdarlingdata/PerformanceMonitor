/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { fmtMs, el, disclosure } from "../../util.js";
import { detailColumn, firstSentence, spillWord, cacheWord, rollbackWord } from "../../pg-grid-text.js";
import { codeDisclosure, listCell } from "./shared.js";

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

export const PG_EVICTION_STATS = [
  { key: "evictions.eviction_passes_in_window", label: "Eviction passes", format: "int" },
  { key: "evictions.max_entries", label: "pg_stat_statements.max", format: "int" },
];

/* The denominator tiles. captures_total is the number of times the collector LOOKED; without it a zero-row
   chain grid cannot be told from a collector that never ran, and both are an absence of rows. */
export const PG_BLOCKING_STATS = [
  { key: "captures_total", label: "Captures", format: "int" },
  { key: "captures_with_blocking", label: "With blocking", format: "int" },
  { key: "pct_of_captures_with_blocking", label: "Of captures (%)", format: "num1" },
  { key: "worst_chain_victims", label: "Worst chain victims", format: "int" },
  { key: "worst_chain_root_state", label: "Root state", format: "text", small: true },
  { key: "cycles_sampled", label: "Cycles", format: "int" },
];

/* Every total here is the WINDOW's (#3653): the read computes them in the same statement as the rows, above
   its LIMIT, so "Temp files" is every database that moved and not the twenty the grid below shows. Until
   #3653 these were sums over the returned rows and the labels said so ("Databases returned", "Cache hit %
   (of returned)") - the A7 census carried that as its one stated allowance, and relabelling here is half of
   what retired it. `database_count` is the window's count and `databases_returned` the page's; `truncated`
   is OBSERVED by the read (a limit + 1 fetch), not inferred from the cap. */
export const PG_DATABASE_STATS = [
  { key: "database_count", label: "Databases active", format: "int" },
  { key: "total_temp_files", label: "Temp files", format: "int" },
  { key: "total_temp_bytes", label: "Temp bytes", format: "int" },
  { key: "top_spiller", label: "Biggest spiller", format: "text", small: true },
  { key: "total_deadlocks", label: "Deadlocks", format: "int" },
  { key: "cache_hit_pct", label: "Cache hit %", format: "num2" },
  { key: "truncated", pageCut: "The window held more databases than the grid below lists.", label: "Truncated", format: "bool" },
  /* Named on the tile rather than only per row: every total beside it is a LOWER BOUND when this is true,
     and a reader has to see that before drawing a conclusion from any of them. */
  { key: "statistics_were_reset_in_window", label: "Stats reset in window", format: "bool" },
];

/* #7394: the Query Duration Trend as charts, the series groups of the desktop viewer's PgQueryDurationTrendHost: duration (mean beside the
   bucket's peak) and the call rate are two units, so two charts over the one read. A point with no calls has a null mean, which stays a gap. */
/* The bucket's single worst interval is drawn in the ramp's light grey: beside its pooled twin in the ramp's second blue, two lines of one hue cannot be told apart in the legend. */
const PEAK_COLOR = "#e0e0e0";
export function pgQueryDurationTrendSpecs(ctx) {
  const common = { viz: "line", rowsKey: "points", xKey: "collection_time", span: 2, hideEmptySeries: true };
  const empty = "No statement recorded execution time in this window, so there is nothing to follow.";
  return [
    {
      ...common,
      title: "Query Duration Trend",
      subtitle: ctx.label + ", the statement with the most execution time in this window; a step that persists is a plan or index change",
      series: [
        { key: "mean_exec_ms", label: "Mean ms/exec" },
        /* #3960: a point is a bucket of intervals, so its costliest single interval rides beside the pooled mean. */
        { key: "peak_mean_exec_ms", label: "Peak ms/exec", color: PEAK_COLOR },
      ],
      format: "num2",
      unit: "ms",
      emptyText: empty,
    },
    {
      ...common,
      hideWhenNoRows: true,
      title: "Statement Calls per Second",
      subtitle: ctx.label + ", the same statement",
      series: [{ key: "calls_per_second", label: "Calls/sec" }],
      format: "num2",
      unit: "calls/s",
      emptyText: empty,
    },
  ];
}

/* Temp FILES beside temp BYTES, because the split is the finding: many small spill files is a work_mem
   slightly under what a constant plan needs, a few enormous ones is a plan or index problem no work_mem
   makes acceptable. Deadlocks stay a count - they are discrete events, and a per-second rate would render
   every real one as four leading zeros. */
/* #7394: the Database Trend as charts, the four groups of the desktop viewer's PgDatabaseTrendHost. The percentages share one 0 to 100 axis, the
   rates and the byte counts each get their own because their magnitudes differ by orders. Temp FILES stay beside Deadlocks as counts, as before:
   discrete events, where a per-second rate would render every real one as four leading zeros. `counter_reset` marks an interval across a
   statistics reset (a vertical marker on each chart), since the step it leaves is the counters starting over, not the workload. */
export function pgDatabaseTrendSpecs(ctx) {
  const common = { viz: "line", rowsKey: "points", xKey: "collection_time", span: 2, hideEmptySeries: true, resetKey: "counter_reset", resetLabel: "Stats reset" };
  const empty = "No differenced intervals for this database in the window. A trend needs at least two snapshots, so a short window is legitimately empty here.";
  return [
    {
      ...common,
      title: "Database Trend",
      subtitle: ctx.label + ", the biggest temp-file spiller in this window, or the busiest by block access when nothing spilled; each point's hit ratio is its own intervals', not the lifetime average, and Worst Hit % is its worst single interval",
      series: [
        { key: "cache_hit_pct", label: "Cache Hit %" },
        /* #3897: a point is a bucket of intervals; the cliff this chart exists to show is its worst one. */
        { key: "worst_cache_hit_pct", label: "Worst Hit %", color: PEAK_COLOR },
        { key: "rollback_pct", label: "Rollback %" },
      ],
      format: "pct",
      unit: "%",
      emptyText: empty,
    },
    { ...common, hideWhenNoRows: true, title: "Database Transactions per Second", subtitle: ctx.label, series: [{ key: "transactions_per_second", label: "Xact/sec" }], format: "num2", unit: "xact/s", emptyText: empty },
    { ...common, hideWhenNoRows: true, title: "Database Temp Bytes", subtitle: ctx.label, series: [{ key: "temp_bytes", label: "Temp Bytes" }], format: "bytes", emptyText: empty },
    {
      ...common,
      hideWhenNoRows: true,
      title: "Database Temp Files and Deadlocks",
      subtitle: ctx.label,
      series: [
        { key: "temp_files", label: "Temp Files" },
        { key: "deadlocks", label: "Deadlocks" },
      ],
      format: "int",
      unit: "count",
      emptyText: empty,
    },
  ];
}

export const PG_DEADLOCK_GRAPH_COLUMNS = [
  { key: "occurred_at", label: "When", format: "time" },
  { key: "victim_pid", label: "Victim PID", format: "int" },
  { key: "graph", label: "Graph" },
];

export const PG_DEADLOCK_COLUMNS = [
  { key: "occurred_at", label: "When", format: "time" },
  { key: "victim_pid", label: "Victim PID", format: "int" },
  { key: "participant_count", label: "Sessions", format: "int" },
  { key: "lock_modes", label: "Lock Modes" },
  { key: "resources", label: "Resources" },
  { key: "victim_statement", label: "Victim Statement", valueList: false },
  { key: "times_seen", label: "Sightings", format: "int", small: true },
];

/* #3601 the classified log events. No column for statement text, because none is stored: the fingerprint
   is the statement's identity and get_pg_top_queries carries the shape's normalised text. */
export const PG_LOG_EVENT_COLUMNS = [
  { key: "occurred_at", label: "When", format: "time" },
  { key: "family", label: "Family", small: true },
  { key: "severity", label: "Severity", small: true },
  { key: "sqlstate", label: "SQLSTATE", small: true },
  { key: "database_name", label: "Database" },
  { key: "user_name", label: "User" },
  { key: "application_name", label: "App" },
  { key: "pid", label: "PID", format: "int", small: true },
  { key: "message", label: "Message" },
  { key: "detail", label: "Detail" },
  { key: "context", label: "Context" },
  { key: "times_seen", label: "Sightings", format: "int", small: true },
];

/* n_distinct and correlation are the two the planner actually turns into a row estimate, so they lead.
   num2 throughout: correlation lives between -1 and 1 and num1 would round most real values to 0.0 or 1.0,
   which is the difference between "clustered" and "not" rendered as the same number. */
export const PG_COLUMN_STATS_COLUMNS = [
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
export const PG_PREDICATE_COLUMNS = [
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

/* granted is second, right beside the mode, because an ungranted lock is the entire finding and burying it
   among the counts would make a contended server look like a busy one. */
export const PG_LOCK_STATS_COLUMNS = [
  { key: "mode", label: "Mode" },
  { key: "granted", label: "Granted", format: "bool" },
  { key: "lock_type", label: "Type" },
  { key: "relation_name", label: "Relation" },
  { key: "captures", label: "Captures", format: "int" },
  { key: "max_backends", label: "Max Backends", format: "int" },
  { key: "max_wait_ms", label: "Worst Wait", format: "ms" },
];

/* queryid, calls and the three time columns are the answer; the block counters are not on this grid. Temp
   blocks written IS, because a spill is the explanation for a statement whose time makes no sense from its
   row count, and it is the one counter PostgreSQL exposes that changes what you would do next. */
export const PG_PLAN_COLUMNS = [
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

/* Remedy last and deliberately widest. Facet, state and observed value are what the eye scans down; the
   remedy is the sentence somebody reads once they have found the row that is wrong, and it is the column
   this panel exists for — every other reader of these rows outside the Windows Viewer sees the facet name
   and its observed value and nothing about what to do. */
/* #7410: the collector's facet names are stable, so the grid says what each one is in words (the raw name stays in the cell's tooltip and
   in the Copy/CSV rows). A name this table does not know shows as the collector wrote it. `extension_available` is the one facet whose "no"
   is not a fault: auto_explain is preload-only and never appears in pg_available_extensions, so an unconfirmed answer reads "Can't tell". */
const PLAN_CAPTURE_FACET_LABELS = {
  library_loaded: "auto_explain is loaded",
  capture_threshold: "Capture threshold is set",
  extension_available: "auto_explain is available",
  plan_text_setting: "Plans are logged as JSON",
  plan_attribution: "Plans carry a query id",
  message_locale: "Log messages are in English",
};
const planFacetUnconfirmed = (row) => row.facet === "extension_available" && row.is_satisfied !== true;

export const PG_PLAN_CAPTURE_READINESS_COLUMNS = [
  { key: "facet", label: "Facet", display: (row) => PLAN_CAPTURE_FACET_LABELS[row.facet] || String(row.facet ?? "—"), sortValue: (row) => PLAN_CAPTURE_FACET_LABELS[row.facet] || row.facet },
  {
    key: "is_satisfied",
    label: "Ready",
    display: (row) => (row.is_satisfied === true ? "yes" : row.is_satisfied === false ? (planFacetUnconfirmed(row) ? "can't tell" : "no") : "—"),
    sortValue: (row) => (row.is_satisfied === true ? 2 : planFacetUnconfirmed(row) ? 1 : 0),
  },
  { key: "observed", label: "Observed", mono: true, display: (row) => (planFacetUnconfirmed(row) ? "not listed" : row.observed ?? "—"), sortValue: (row) => row.observed },
  detailColumn("detail", "What it means and the fix", (text) => firstSentence(text, 110) || null,
    "The first sentence of what this setting does to plan capture and how to change it. Click for the full text.",
    { wrap: true, detailMax: 120, cellClass: () => "flow-cell" }),
  { key: "last_observed", label: "Last Seen", format: "time", small: true },
];

/* Verdict beside the setting, then the value, then the prose columns widest and last - the same reading
   order as the readiness grid above it: scan down setting and verdict to find the row that is off, then
   read across for what it unlocks, what it costs, and what to type. remedy is the column the panel exists
   for and is worded for the server's hosting flavour by the read, not by this file. */
export const PG_LOGGING_AUDIT_COLUMNS = [
  { key: "setting", label: "Setting", mono: true },
  { key: "verdict", label: "Verdict", hint: "Describes the log lines each setting writes. Partial means a threshold is filtering, which is the recommended state for log_min_duration_statement." },
  /* #7410: the unit sits with its value and the source is its tooltip, so the long-text columns have the width. Unlocks and Recommended
     wrap to the width the page has (`flow-cell`), and the cost and the remedy open on a click under one cell. */
  {
    key: "value",
    label: "Value",
    mono: true,
    render: (row) => el("span", { title: row.source ? "From " + row.source : null }, [row.value == null || row.value === "" ? "—" : String(row.value) + (row.unit ? " " + row.unit : "")]),
    sortValue: (row) => row.value,
  },
  { key: "change_needs", label: "Change needs", small: true },
  { key: "recommended", label: "Recommended", wrap: true, cellClass: () => "flow-cell flow-narrow" },
  { key: "unlocks", label: "Unlocks", wrap: true, cellClass: () => "flow-cell flow-narrow" },
  {
    key: "remedy",
    label: "Cost and remedy",
    cellClass: () => "flow-cell",
    sortable: false,
    copyValue: (row) => row.remedy ?? "",
    render: (row) => {
      const parts = [];
      if (row.remedy) parts.push(el("p", { class: "cell-detail-text" }, [el("strong", {}, ["Remedy. "]), row.remedy]));
      if (row.cost_note) parts.push(el("p", { class: "cell-detail-text" }, [el("strong", {}, ["Cost. "]), row.cost_note]));
      if (!parts.length) return document.createTextNode("—");
      return disclosure(firstSentence(row.remedy || row.cost_note, 70), parts, { max: 70, cell: true });
    },
  },
  /* #7410: not drawn, still in Copy and the CSV: the fields the Value cell and the remedy cell fold in. */
  { key: "unit", label: "Unit", exportOnly: true },
  { key: "source", label: "Source", exportOnly: true },
  { key: "cost_note", label: "Cost", exportOnly: true },
];

/* #7410: the query text is the second column, with room to read (`query-cell`), and not the last one cut off after a few characters. The
   Query ID stays first. The three counters only some engines fill (temp blocks, WAL, peak memory) leave the grid when every row of the
   reply holds 0 or nothing, which the reply says, so no engine name is tested. */
export const PG_TOP_QUERY_COLUMNS = [
  { key: "queryid", label: "Query ID", mono: true },
  { key: "query_text", label: "Query", wrap: true, valueList: false, cellClass: () => "query-cell", render: (row) => codeDisclosure(row.query_text) },
  { key: "calls", label: "Calls", format: "int" },
  { key: "total_exec_time_ms", label: "Total", format: "ms" },
  { key: "avg_exec_time_ms", label: "Avg", format: "ms" },
  /* A high-water mark over the whole retained history, not a window maximum — the read says so, and the label
     says so here, because a "Max" beside two window-scoped columns would otherwise be read as one of them. */
  { key: "max_exec_time_ms", label: "Max (all time)", format: "ms" },
  { key: "pct_of_total_time", label: "% of Time", format: "num1" },
  { key: "rows_returned", label: "Rows", format: "int" },
  { key: "temp_blks_written", label: "Temp Blocks Written", format: "int", hideWhenZero: true },
  { key: "wal_bytes", label: "WAL", format: "bytes", hideWhenZero: true },
  { key: "max_exec_peakmem_bytes", label: "Peak Mem", format: "bytes", hideWhenZero: true },
];

export const PG_BLOCKING_CHAIN_COLUMNS = [
  { key: "captured_at", label: "When", format: "time" },
  { key: "root_pid", label: "Root PID", format: "int" },
  { key: "databases", label: "Databases", render: (row) => listCell(row.databases) },
  { key: "root_username", label: "User" },
  { key: "root_application", label: "App", wrap: true },
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
  { key: "root_query", label: "Root Query", wrap: true, valueList: false, render: (row) => codeDisclosure(row.root_query) },
  { key: "recommended_action", label: "Action", wrap: true },
];

export const PG_BLOCKING_CYCLE_COLUMNS = [
  { key: "captured_at", label: "When", format: "time" },
  { key: "participant_count", label: "Participants", format: "int" },
  { key: "pids", label: "PIDs", render: (row) => listCell(row.pids) },
  { key: "database", label: "Database" },
  { key: "application", label: "App", wrap: true },
  { key: "blocked_behind_count", label: "Queued Behind", format: "int" },
  { key: "blocked_behind_pids", label: "Queued PIDs", render: (row) => listCell(row.blocked_behind_pids) },
  { key: "finding", label: "Finding", clamp: true },
];

/* The read's own `*_finding` prose travels beside each number, because none of these three is actionable
   from the figure alone — a 99% cache hit ratio is meaningless without knowing the working set, and a
   rollback ratio is a fault only against what the application is supposed to do. */
export const PG_DATABASE_COLUMNS = [
  { key: "database", label: "Database" },
  { key: "temp_files", label: "Temp Files", format: "int" },
  { key: "temp_bytes", label: "Temp Bytes", format: "int" },
  { key: "avg_temp_file_bytes", label: "Avg Temp File (bytes)", format: "int" },
  /* #7410: each row used to carry three paragraphs that read the same on every database. The cell shows the verdict word and the
     paragraph opens on a click (the same pattern as the bloat grid), and the header tooltip says what the column means once. */
  detailColumn("spill_finding", "Spills", spillWord, "Whether sorts and hashes spilled to temp files in this window, and how big the files were. Click a verdict for the cause and the usual fix."),
  { key: "cache_hit_pct", label: "Cache Hit %", format: "num2" },
  detailColumn("cache_finding", "Cache", cacheWord, "How the cache hit percentage compares with the usual 99% target. It is an upper bound on the problem, not a disk measurement. Click a verdict for more."),
  { key: "deadlocks", label: "Deadlocks", format: "int" },
  { key: "xact_commit", label: "Commits", format: "int" },
  { key: "xact_rollback", label: "Rollbacks", format: "int" },
  { key: "rollback_pct", label: "Rollback %", format: "num2" },
  detailColumn("rollback_finding", "Rollback Share", rollbackWord, "Whether the share of transactions that rolled back is usual for a healthy application. Click a verdict for what counts as a rollback."),
  /* The reset evidence. Without it every total in the row is a lower bound and nothing on screen says so. */
  { key: "counters_were_reset", label: "Reset", format: "bool" },
  { key: "reset_note", label: "Reset Note", clamp: true },
  { key: "sample_count", label: "Samples", format: "int" },
  /* The one LEVEL in the row (V133): the window's peak of connected backends for this database, never
     differenced. Empty on a pre-V133 history rather than 0 — not sampled is not idle. Per-database peaks land
     at different instants, so there is deliberately no total tile for them above. */
  { key: "peak_numbackends", label: "Peak Backends", format: "int" },
];

