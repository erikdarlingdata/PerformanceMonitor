/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Optimization" tab: idle databases, tempdb pressure, wait time by category, the most expensive statements by CPU
   and daily memory-grant efficiency, from one get_finops read (view optimization). The picker moves the wait categories, the expensive statements and the cost
   share; the other sections keep their own fixed windows. Each section reads its own status:
   ok shows its table, empty shows an empty strip, not_collected shows the section's message. */

import { VIZ } from "../../panels.js";
import { planColumn } from "../plan-viewer.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool, applyFormat } from "../../util.js";

// The default window.
const HOURS = 24;

// The desktop's window choices (FinOpsTab.xaml ~:791 Wait Stats and ~:818 Expensive Queries), as hours. The desktop has a
// picker for each of those two sections; the web keeps ONE picker that moves both. The other sections stay fixed, as on
// the desktop.
const WINDOWS = [
  { value: 1, label: "Last 1 hour" },
  { value: 4, label: "Last 4 hours" },
  { value: 12, label: "Last 12 hours" },
  { value: 24, label: "Last 24 hours" },
  { value: 168, label: "Last 7 days" },
];

// The chosen window per server, kept here so the 60 s rebuild of the tab does not put it back to 24 hours.
const chosenHours = new Map();

// A labelled <select> (the server-tabs.js pickerControl pattern); every value goes through el()'s text and attribute paths.
function pickerControl(label, options, selected, onPick) {
  const sel = el(
    "select",
    { class: "range-select-inline", "aria-label": label },
    options.map((o) => el("option", { value: o.value, text: o.label }))
  );
  sel.value = String(selected);
  sel.addEventListener("change", () => onPick(Number(sel.value)));
  return el("label", { class: "range-control" }, [el("span", { text: label }), sel]);
}
const LIMIT = 20;

const IDLE_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "total_size_mb", label: "Total size (MB)", format: "num2" },
  { key: "file_count", label: "Files", format: "int" },
  { key: "last_execution_server_local", label: "Last execution (server time)" },
];

const TEMPDB_COLUMNS = [
  { key: "metric", label: "Metric" },
  { key: "current_mb", label: "Current (MB)", format: "num2" },
  { key: "peak_24h_mb", label: "Peak 24h (MB)", format: "num2" },
  { key: "warning", label: "Warning" },
];

const WAIT_COLUMNS = [
  { key: "category", label: "Category" },
  { key: "total_wait_time_ms", label: "Total wait (ms)", format: "int" },
  { key: "waiting_tasks", label: "Waiting tasks", format: "int" },
  { key: "pct_of_total", label: "% of total", format: "num1" },
  { key: "top_wait_type", label: "Top wait type" },
  { key: "top_wait_time_ms", label: "Top wait (ms)", format: "int" },
  { key: "est_cost_usd", label: "Est. cost share ($)", format: "num2" },
];

const QUERY_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "query_preview", label: "Query preview" },
  { key: "total_cpu_ms", label: "Total CPU", format: "ms" },
  { key: "avg_cpu_ms_per_exec", label: "Avg CPU/exec (ms)", format: "num2" },
  { key: "total_reads", label: "Total reads", format: "int" },
  { key: "avg_reads_per_exec", label: "Avg reads/exec", format: "int" },
  { key: "executions", label: "Executions", format: "int" },
  { key: "has_plan", label: "Has plan", format: "bool" },
  { key: "est_cost_usd", label: "Est. cost share ($)", format: "num2" },
];

const GRANT_COLUMNS = [
  { key: "day", label: "Day (UTC)" },
  { key: "avg_granted_mb", label: "Avg granted (MB)", format: "num1" },
  { key: "avg_used_mb", label: "Avg used (MB)", format: "num1" },
  { key: "efficiency_pct", label: "Efficiency (%)", format: "num1" },
  { key: "peak_granted_mb", label: "Peak granted (MB)", format: "num1" },
  { key: "wasted_mb", label: "Wasted (MB)", format: "num1" },
  { key: "total_grantees", label: "Grantees", format: "int" },
  { key: "total_waiters", label: "Waiters", format: "int" },
  { key: "timeout_errors", label: "Timeouts", format: "int" },
  { key: "forced_grants", label: "Forced", format: "int" },
];

function idleNotice(s) {
  const n = (s.rows || []).length;
  let text = (n === 1 ? "1 idle database" : n + " idle databases") + " over the last " + (s.window_days ?? "?") + " days";
  text += s.truncated ? "; the top " + n + " of " + (s.database_count ?? "more") + "." : ".";
  return text;
}

function windowNotice(s) {
  return "last " + (s.window_hours ?? HOURS) + " hours";
}

function queryNotice(s) {
  const hours = s.window_hours ?? HOURS;
  const start = s.effective_start ? Date.parse(s.effective_start) : NaN;
  // The service cuts this section to the window query text is kept for, so a start later than the asked window began
  // means the window was shortened: say what is shown, with the length taken from the start.
  if (Number.isFinite(start) && start > Date.now() - hours * 3600000 + 60000) {
    const days = Math.max(1, Math.round((Date.now() - start) / 86400000));
    return "Top " + (s.rows || []).length + " by CPU, showing the last " + days + " days; query text and plans are not retained beyond that.";
  }
  let text = "Top " + (s.rows || []).length + " by CPU, last " + hours + " hours";
  if (s.effective_start) text += ", from " + applyFormat("time", s.effective_start) + " (local time)";
  return text + ".";
}

function costLine(data) {
  const hours = data.hours_back ?? HOURS;
  return data.monthly_cost_usd != null
    ? "Est. cost shares split the server's $" + applyFormat("num2", data.monthly_cost_usd) + " monthly cost pro-rated to this window (" + hours + " hours); they are an attribution, not a measured cost."
    : (data.cost_reason ?? "monthly cost not set") + ", so Est. cost share is blank.";
}

// One section: a heading, then the table, the empty strip or the notice, by the section's own status.
function sectionView(title, s, columns, emptyText, notice) {
  const section = s || {};
  let content;
  if (section.status === "ok") {
    content = [noticeStrip(notice(section)), VIZ.table(section, { rowsKey: "rows", columns, emptyText })];
  } else if (section.status === "empty") {
    content = [emptyStrip(emptyText)];
  } else {
    content = [noticeStrip(section.message ?? "This section was not collected.")];
  }
  return el("div", { class: "finops-section" }, [el("h3", { text: title }), ...content]);
}

export const tab = {
  id: "optimization",
  label: "Optimization",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    let seq = 0;
    const load = async (hours) => {
      const mine = ++seq;
      chosenHours.set(server, hours);
      mount(body, loadingStrip());
      try {
        const res = await readTool("get_finops", { server, view: "optimization", hours, limit: LIMIT }, ctx && ctx.signal);
        if (mine !== seq) return;
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        mount(body, [
          noticeStrip(costLine(data)),
          sectionView("Idle Databases", data.idle_databases, IDLE_COLUMNS, "No idle databases detected", idleNotice),
          sectionView("tempdb Pressure", data.tempdb_pressure, TEMPDB_COLUMNS, "No tempdb data available", windowNotice),
          sectionView("Wait Stats Summary", data.wait_categories, WAIT_COLUMNS, "No wait stats data available", windowNotice),
          sectionView("Expensive Queries (Top " + LIMIT + " by CPU)", data.expensive_queries, [...QUERY_COLUMNS, planColumn(server)], "No expensive queries found", queryNotice),
          sectionView("Memory Grant Efficiency", data.memory_grant_efficiency, GRANT_COLUMNS, "No memory grant data available", windowNotice),
        ]);
      } catch (e) {
        if (mine === seq && e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    };
    const start = chosenHours.get(server) ?? HOURS;
    const root = el("div", {}, [pickerControl("Window", WINDOWS, start, (hours) => load(hours)), body]);
    load(start);
    return root;
  },
};
