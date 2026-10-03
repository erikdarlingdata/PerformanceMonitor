/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Utilization" tab: the provisioning verdict, health score, cost cards, CPU and memory summary, reason
   sentence and 7-day trend from get_finops (view utilization, the fixed 24-hour window), the two top-database grids
   from get_finops (view database_resources, top 5 over 24 hours, as the desktop), then the raw panels: CPU over the
   last day (get_cpu_utilization), the latest memory snapshot (get_memory_stats) and allocated versus used size per
   database (get_database_sizes). Each section has its own container and the two get_finops reads run independently,
   so one failing never blanks the other. The sizes table's snapshot time is not shown (a table panel has no slot for
   a top-level field); a second panel would read get_database_sizes twice. A known gap. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, readErrorStrip, errorStrip, readTool, fmtInt } from "../../util.js";
const CPU_HOURS = 24;
const TOP_HOURS = 24;
const TOP_LIMIT = 5;

/* The service names the verdict and the band; the browser only picks a colour for the label it was sent. */
const VERDICT_SEV = { RIGHT_SIZED: "Healthy", OVER_PROVISIONED: "Warning", UNDER_PROVISIONED: "Critical" };
const BAND_SEV = { good: "Healthy", fair: "Warning", poor: "Critical" };

/* Both come from the shared catalog, so a label or format changed there changes here (the Server page does the same). */
const CPU_SERIES = READ_FIELDS.get_cpu_utilization.line.series;
const SIZE_COLUMNS = READ_FIELDS.get_database_sizes.table.columns;

/* The same predicate the Server page uses (server-tabs.js keeps its copy private): on an Azure SQL Database
   (engine_edition 5) the first two memory figures are the database's memory limit and the room left under it, not
   the host's RAM, so they are named that way and the read's own memory_note is shown. */
const AZURE_SQL_DATABASE = { key: "engine_edition", equals: 5 };

const MEMORY_STATS = [
  { key: "captured_at", label: "Collected", format: "reltime", small: true },
  { key: "total_physical_memory_mb", label: "Physical", format: "mb", hideWhen: AZURE_SQL_DATABASE },
  { key: "total_physical_memory_mb", label: "Memory limit", format: "mb", showWhen: AZURE_SQL_DATABASE },
  { key: "available_physical_memory_mb", label: "Available", format: "mb", hideWhen: AZURE_SQL_DATABASE },
  { key: "available_physical_memory_mb", label: "Available under limit", format: "mb", showWhen: AZURE_SQL_DATABASE },
  { key: "memory_utilization_pct", label: "Utilization", format: "pct" },
  { key: "total_server_memory_mb", label: "Total server", format: "mb" },
  { key: "target_server_memory_mb", label: "Target server", format: "mb" },
  { key: "buffer_pool_mb", label: "Buffer pool", format: "mb" },
  { key: "plan_cache_mb", label: "Plan cache", format: "mb" },
  { key: "system_memory_state", label: "System state", format: "text", small: true, nullKey: "system_memory_state_note" },
  { key: "sql_memory_model", label: "Memory model", format: "text", small: true },
];

/* utilization-keys:begin */
/* Keys computed in the browser from the payload (text only); every other key below is read from the view. */
const DERIVED_KEYS = ["verdict_label", "verdict_sev", "day_label", "monthly_cost_text", "annual_cost_text", "workers_in_use"];

const SUMMARY_STATS = [
  { key: "verdict_label", label: "Verdict", format: "text" },
  { key: "health_score", label: "Health", format: "int" },
  { key: "monthly_cost_text", label: "Budget", format: "text" },
  { key: "annual_cost_text", label: "Annual", format: "text" },
  { key: "cpu.avg_cpu_pct", label: "Avg CPU %", format: "num1" },
  { key: "cpu.p95_cpu_pct", label: "P95 CPU %", format: "num1" },
  { key: "cpu.max_cpu_pct", label: "Max CPU %", format: "num1" },
  { key: "cpu.cpu_count", label: "vCores", format: "int", nullKey: "cpu.cpu_count_reason", showWhen: { key: "cpu.cpu_count_unit", equals: "vcores" } },
  { key: "cpu.cpu_count", label: "CPUs", format: "int", nullKey: "cpu.cpu_count_reason", hideWhen: { key: "cpu.cpu_count_unit", equals: "vcores" } },
  { key: "cpu.max_workers", label: "Max workers", format: "int" },
  { key: "workers_in_use", label: "Workers in use", format: "text" },
  { key: "cpu.cpu_samples", label: "CPU samples (24h)", format: "int" },
  { key: "memory.stolen_memory_pct", label: "Stolen memory %", format: "num1" },
  { key: "memory.buffer_pool_pct", label: "Buffer pool %", format: "num1" },
  { key: "memory.physical_memory_mb", label: "Physical", format: "mb", hideWhen: { key: "memory.memory_basis", equals: "memory_limit" } },
  { key: "memory.physical_memory_mb", label: "Memory limit", format: "mb", showWhen: { key: "memory.memory_basis", equals: "memory_limit" } },
  { key: "memory.target_memory_mb", label: "Target memory", format: "mb" },
  { key: "memory.total_memory_mb", label: "Total memory", format: "mb" },
  { key: "memory.buffer_pool_mb", label: "Buffer pool", format: "mb" },
];

const TREND_COLUMNS = [
  { key: "day_label", label: "Day" },
  { key: "verdict_label", label: "Status", sevKey: "verdict_sev" },
  { key: "avg_cpu_pct", label: "Avg CPU %", format: "num1" },
  { key: "p95_cpu_pct", label: "P95 CPU %", format: "num1" },
  { key: "max_cpu_pct", label: "Max CPU %", format: "int" },
  { key: "memory_ratio", label: "Mem Ratio", format: "num2" },
];
/* utilization-keys:end */

/* top-keys:begin */
const TOP_TOTAL_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "cpu_share_pct", label: "CPU %", format: "num1" },
  { key: "io_share_pct", label: "IO %", format: "num1" },
  { key: "cpu_time_ms", label: "CPU (ms)", format: "int" },
  { key: "execution_count", label: "Execs", format: "int" },
];

const TOP_AVG_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "avg_cpu_ms", label: "Avg CPU (ms)", format: "int" },
  { key: "avg_io_mb", label: "Avg IO (MB)", format: "num2" },
  { key: "execution_count", label: "Execs", format: "int" },
];
/* top-keys:end */

function verdictLabel(v) {
  return v == null ? "No Data" : v === "NOT_APPLICABLE" ? "N/A" : String(v).replace(/_/g, " ");
}

function trendDay(s) {
  const d = new Date(s + "T00:00:00Z");
  if (Number.isNaN(d.getTime())) return s;
  return d.toLocaleDateString(undefined, { timeZone: "UTC", weekday: "short", month: "2-digit", day: "2-digit" });
}

function loadUtilization(body, server, ctx) {
  (async () => {
    try {
      const res = await readTool("get_finops", { server, view: "utilization" }, ctx && ctx.signal);
      if (res.kind === "aborted" || res.kind === "auth") return;
      if (res.kind === "error") return mount(body, readErrorStrip(res.message));
      if (res.kind === "empty") return mount(body, emptyStrip(res.message));
      const data = res.data || {};
      const hasCost = data.monthly_cost_usd != null;
      const view = {
        ...data,
        verdict_label: verdictLabel(data.verdict),
        monthly_cost_text: hasCost ? "$" + fmtInt(data.monthly_cost_usd) + "/mo" : null,
        annual_cost_text: data.annual_cost_usd != null ? "$" + fmtInt(data.annual_cost_usd) + "/yr" : null,
        workers_in_use: data.cpu && data.cpu.current_workers != null ? fmtInt(data.cpu.current_workers) : "n/a",
      };
      const stats = SUMMARY_STATS
        .filter((s) => hasCost || (s.key !== "monthly_cost_text" && s.key !== "annual_cost_text"))
        .map((s) =>
          s.key === "verdict_label" ? { ...s, sev: VERDICT_SEV[data.verdict] }
            : s.key === "health_score" ? { ...s, sev: BAND_SEV[data.health_band] }
            : s);
      const trend = (data.provisioning_trend || []).map((r) => ({
        ...r,
        day_label: trendDay(r.day),
        verdict_label: verdictLabel(r.verdict),
        verdict_sev: VERDICT_SEV[r.verdict],
      }));
      mount(body, [
        VIZ.stat(view, { stats }),
        data.health_score_note ? el("p", { class: "finops-note", title: data.health_score_note, text: data.health_score_note }) : null,
        el("p", { class: "finops-reason", text: data.verdict_reason }),
        el("details", {}, [
          el("summary", { text: "7-Day Provisioning Trend" }),
          VIZ.table({ rows: trend }, { rowsKey: "rows", columns: TREND_COLUMNS, emptyText: "No provisioning trend in the last 7 days." }),
        ]),
      ]);
    } catch (e) {
      if (e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
    }
  })();
}

function loadTopDatabases(body, server, ctx) {
  (async () => {
    try {
      const res = await readTool("get_finops", { server, view: "database_resources", hours: TOP_HOURS, limit: TOP_LIMIT }, ctx && ctx.signal);
      if (res.kind === "aborted" || res.kind === "auth") return;
      if (res.kind === "error") return mount(body, readErrorStrip(res.message));
      if (res.kind === "empty") return mount(body, emptyStrip(res.message));
      const data = res.data || {};
      mount(body, [
        el("h3", { text: "Top Databases by Total CPU" }),
        VIZ.table(data, { rowsKey: "top_by_total", columns: TOP_TOTAL_COLUMNS, emptyText: "No database used CPU in the last 24 hours." }),
        el("h3", { text: "Top Databases by Avg CPU / Execution" }),
        VIZ.table(data, { rowsKey: "top_by_avg", columns: TOP_AVG_COLUMNS, emptyText: "No database had executions in the last 24 hours." }),
      ]);
    } catch (e) {
      if (e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
    }
  })();
}

function section(id, children) {
  return el("div", { class: "finops-section", "data-section": id }, children);
}

export const tab = {
  id: "utilization",
  label: "Utilization",
  build(server, ctx) {
    const verdictBody = el("div", {}, [loadingStrip()]);
    const topBody = el("div", {}, [loadingStrip()]);
    loadUtilization(verdictBody, server, ctx);
    loadTopDatabases(topBody, server, ctx);
    return el("div", { class: "finops-utilization" }, [
      section("verdict", [verdictBody]),
      section("top", [topBody]),
      section("cpu", [
        renderPanel({
          title: "CPU Utilization",
          subtitle: "last " + CPU_HOURS + " hours",
          read: "get_cpu_utilization",
          params: { server, hours: CPU_HOURS },
          viz: "line",
          rowsKey: "samples",
          xKey: "sample_time",
          series: CPU_SERIES,
          format: "pct",
          unit: "%",
          emptyText: "No CPU samples in this window.",
          span: 2,
        }),
      ]),
      section("memory", [
        renderPanel({
          title: "Memory",
          subtitle: "latest snapshot",
          read: "get_memory_stats",
          params: { server },
          viz: "stat",
          stats: MEMORY_STATS,
          noteKey: "memory_note",
          span: 2,
        }),
      ]),
      section("sizes", [
        renderPanel({
          title: "Database Sizes",
          subtitle: "latest snapshot",
          read: "get_database_sizes",
          params: { server },
          viz: "table",
          rowsKey: "databases",
          columns: SIZE_COLUMNS,
          emptyText: "No database sizes in the latest snapshot.",
          noteKey: "note",
          span: 2,
        }),
      ]),
    ]);
  },
};
