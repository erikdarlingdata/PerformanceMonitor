/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, readTool, readToolWithinKeptHistory, mount, errorStrip, readErrorStrip, emptyStrip, noticeStrip, localTime, dbFilteredEmptyText } from "../../util.js";
import { getPanelSignal, VIZ } from "../../panels.js";
import { panelShell } from "./shared.js";

export const SERVER_CONFIG_COLUMNS = [
  { key: "name", label: "Setting" },
  { key: "value_configured", label: "Configured" },
  { key: "value_in_use", label: "In use" },
  { key: "values_match", label: "Match", format: "bool" },
  { key: "is_dynamic", label: "Dynamic", format: "bool" },
  { key: "is_advanced", label: "Advanced", format: "bool" },
];

/* The WPF Database Configuration grid's columns in its order and wording, less its per-row Collected column:
   the read stamps one captured_at on the whole snapshot, so the panel subtitle carries it instead. */
export const DB_CONFIG_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "state", label: "State" },
  { key: "compatibility_level", label: "Compat Level", format: "int" },
  { key: "collation", label: "Collation" },
  { key: "recovery_model", label: "Recovery Model" },
  { key: "read_only", label: "Read Only", format: "bool" },
  { key: "rcsi", label: "RCSI", format: "bool" },
  { key: "snapshot_isolation", label: "Snapshot Isolation" },
  { key: "auto_create_stats", label: "Auto Create Stats", format: "bool" },
  { key: "auto_update_stats", label: "Auto Update Stats", format: "bool" },
  { key: "auto_update_stats_async", label: "Async Stats Update", format: "bool" },
  { key: "parameterization_forced", label: "Forced Parameterization", format: "bool" },
  { key: "query_store", label: "Query Store", format: "bool" },
  { key: "encrypted", label: "Encrypted", format: "bool" },
  { key: "trustworthy", label: "Trustworthy", format: "bool" },
  { key: "db_chaining", label: "DB Chaining", format: "bool" },
  { key: "broker_enabled", label: "Broker", format: "bool" },
  { key: "cdc_enabled", label: "CDC", format: "bool" },
  { key: "mixed_page_allocation", label: "Mixed Pages", format: "bool" },
  { key: "log_reuse_wait", label: "Log Reuse Wait" },
  { key: "auto_close", label: "Auto Close", format: "bool" },
  { key: "auto_shrink", label: "Auto Shrink", format: "bool" },
  { key: "page_verify", label: "Page Verify" },
  { key: "target_recovery_time_seconds", label: "Target Recovery (s)", format: "int" },
  { key: "delayed_durability", label: "Delayed Durability" },
  { key: "accelerated_database_recovery", label: "ADR", format: "bool" },
  { key: "memory_optimized", label: "Memory Optimized", format: "bool" },
  { key: "optimized_locking", label: "Optimized Locking", format: "bool" },
];

/* The WPF Scoped Configuration grid: one row per database setting; Collected is the snapshot's captured_at. */
const SCOPED_CONFIG_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "name", label: "Setting" },
  { key: "value", label: "Value" },
  { key: "value_for_secondary", label: "Value for Secondary" },
  { key: "captured_at", label: "Collected", format: "time" },
];

/* The scoped-configuration read groups settings under each database; the grid wants one row per setting. The
   rows live in the panel's closure, not module state, so the 60 s repaint rebuilds them from the read. */
export function scopedConfigPanel(server) {
  const { panel, body } = panelShell("Database Scoped Configuration", "sys.database_scoped_configurations, newest connect-time snapshot", 2, "get_database_scoped_config");
  const signal = getPanelSignal();
  (async () => {
    const res = await readTool("get_database_scoped_config", { server }, signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, emptyStrip(res.message));
    const rows = [];
    for (const db of res.data.databases || [])
      for (const st of db.settings || [])
        rows.push({ database_name: db.database_name, ...st, captured_at: res.data.captured_at });
    mount(body, VIZ.table({ rows }, { rowsKey: "rows", columns: SCOPED_CONFIG_COLUMNS, emptyText: dbFilteredEmptyText("get_database_scoped_config", "No database-scoped configuration snapshot yet.") }));
  })();
  return panel;
}

export const QS_HEALTH_COLUMNS = [
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
  /* V137 (#3796): the payload's two trailing fields. Formatless on purpose: the renderer prints a null as the
     page's "—", which is the right cell for a pre-rung row (or, for wait stats, a 2016 engine) — an absence,
     never OFF. ALL is the plan-churn factory the tool's description names; the verdict is #3797's, not this tile's. */
  { key: "query_capture_mode", label: "Capture mode" },
  { key: "wait_stats_capture_mode", label: "Wait stats capture" },
];

export const TRACE_FLAG_COLUMNS = [
  { key: "trace_flag", label: "Flag", format: "int" },
  { key: "enabled", label: "Enabled", format: "bool" },
  { key: "is_global", label: "Global", format: "bool" },
  { key: "is_session", label: "Session", format: "bool" },
];

export const AUDIT_COLUMNS = [
  { key: "setting", label: "Setting" },
  { key: "status", label: "Status", statusSev: true },
  { key: "current_value", label: "Current" },
  { key: "suggested_value", label: "Suggested" },
  { key: "recommendation", label: "Why", wrap: true },
];

/* #6875: the identity column capacity panels on the Configuration tab. The band, the percent, the values left and the days to
   exhaustion are the service's (IdentityCapacityMath through get_identity_capacity), the same figures the Viewer shows. values_remaining
   and the other bigint and decimal values arrive as text, so the figure is formatted from the digits and never through a JS number. */
function identityDigits(value) {
  if (value == null || value === "") return "—";
  try {
    return BigInt(value).toLocaleString();
  } catch (e) {
    return String(value);
  }
}
function identityTable(r) {
  return [r.schema, r.table].filter((x) => x != null && x !== "").join(".");
}
function identityType(r) {
  return (r.data_type === "decimal" || r.data_type === "numeric") && r.type_precision > 0 ? r.data_type + "(" + r.type_precision + ")" : r.data_type || "—";
}
function identityScanState(r) {
  if (!r || r.scanned !== true) return r && r.scan_note ? r.scan_note.charAt(0).toUpperCase() + r.scan_note.slice(1) : "Not yet scanned";
  const n = r.columns_above_10_percent;
  return n > 0 ? "Scanned, " + n + (n === 1 ? " column" : " columns") + " above 10 percent" : "Scanned, clean";
}
function identityLastScanned(r) {
  const at = r.scanned_at || r.last_scanned_at;
  return at ? localTime(at) : "—";
}
const IDENTITY_COLUMNS = [
  { key: "database", label: "Database" },
  { key: "table", label: "Table", render: (r) => el("span", { text: identityTable(r) }), sortValue: identityTable },
  { key: "column", label: "Column" },
  { key: "data_type", label: "Type", render: (r) => el("span", { text: identityType(r) }), sortValue: identityType },
  { key: "percent_used", label: "Percent used", format: "num1" },
  { key: "values_remaining", label: "Values remaining", render: (r) => el("span", { text: identityDigits(r.values_remaining) }), sortValue: (r) => (r.values_remaining == null ? -1 : Number(r.values_remaining)) },
  { key: "rate_per_day", label: "Rate per day", format: "num1" },
  { key: "days_to_exhaustion", label: "Days to exhaustion", format: "num1" },
  { key: "band", label: "Band", format: "text", small: true },
];
const IDENTITY_SCAN_COLUMNS = [
  { key: "database", label: "Database" },
  { key: "scan_state", label: "Scan state", render: (r) => el("span", { text: identityScanState(r) }), sortValue: identityScanState },
  /* A database not yet scanned has no scan in the window: its last scan before the window (if any) is shown instead. */
  { key: "scanned_at", label: "Last scanned", format: "time", small: true, render: (r) => el("span", { text: identityLastScanned(r) }), sortValue: (r) => r.scanned_at || r.last_scanned_at || "" },
];
export function identityCapacityPanels(server) {
  const columnsShell = panelShell("Identity Capacity", "identity_capacity collector, every 6 hours", 2, "get_identity_capacity");
  const scanShell = panelShell("Identity Scan State", "per database, newest scan", 2, "get_identity_capacity");
  const signal = getPanelSignal();
  (async () => {
    const res = await readToolWithinKeptHistory("get_identity_capacity", { server }, signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") {
      mount(columnsShell.body, readErrorStrip(res.message));
      mount(scanShell.body, readErrorStrip(res.message));
      return;
    }
    const data = res.data;
    try {
      /* "empty" is the one true negative: databases were scanned and none has a column above 10 percent, so the scan list still draws.
         unavailable, precondition and not_collected carry the read's own sentence in both panels. */
      if (res.kind === "empty" && res.status !== "empty") {
        mount(columnsShell.body, [emptyStrip(res.message)]);
        mount(scanShell.body, [emptyStrip(res.message)]);
        return;
      }
      const spec = { read: "get_identity_capacity" };
      const truncated = data && data.truncated === true ? noticeStrip("Showing the " + data.columns_returned + " fullest columns. Pick a database to narrow the list.") : null;
      mount(columnsShell.body, [
        truncated,
        res.kind === "empty"
          ? emptyStrip(res.message)
          : VIZ.table(data, { ...spec, rowsKey: "columns", columns: IDENTITY_COLUMNS, emptyText: "No identity column is above 10 percent used." }),
      ]);
      mount(scanShell.body, [VIZ.table(data, { ...spec, rowsKey: "databases", columns: IDENTITY_SCAN_COLUMNS, emptyText: "No database has been scanned yet." })]);
    } catch (e) {
      const message = "Could not render this panel: " + (e && e.message ? e.message : String(e));
      mount(columnsShell.body, errorStrip(message));
      mount(scanShell.body, errorStrip(message));
    }
  })();
  return [columnsShell.panel, scanShell.panel];
}
/* #6874: the Agent alert panels on the Configuration tab. The status words and the gap sentences are the service's (AgentAlertStatus),
   the same ones the Viewer shows. A gap names either the alerts it is about or the severities and errors that have no alert. */
function agentAlertGapWhich(r) {
  const alerts = Array.isArray(r && r.alerts) ? r.alerts : [];
  const missing = Array.isArray(r && r.missing) ? r.missing : [];
  return alerts.length ? alerts.join(", ") : missing.join(", ");
}
const AGENT_ALERT_STAT_COLUMNS = [
  { key: "instance_status", label: "Alert setup", format: "text", small: true },
  { key: "alert_count", label: "Alerts defined", format: "int" },
  { key: "captured_at", label: "Captured", format: "time", small: true },
];
const AGENT_ALERT_GAP_COLUMNS = [
  { key: "message", label: "What is wrong" },
  { key: "which", label: "Which", render: (r) => el("span", { text: agentAlertGapWhich(r) }), sortValue: agentAlertGapWhich },
];
const AGENT_ALERT_COLUMNS = [
  { key: "name", label: "Alert" },
  { key: "enabled", label: "Enabled", format: "bool" },
  { key: "severity", label: "Severity", format: "int" },
  { key: "message_id", label: "Error", format: "int" },
  { key: "database_name", label: "Database" },
  { key: "operator_count", label: "Operators", format: "int" },
  { key: "fired_in_window", label: "Fired in the window", format: "bool" },
  { key: "occurrence_count", label: "Times fired", format: "int" },
  { key: "last_occurrence", label: "Last fired (server clock)" },
];
export const AGENT_ALERT_SPECS = [
  {
    title: "Agent Alerts",
    subtitle: "msdb.dbo.sysalerts, collected every 15 minutes",
    span: 2,
    viz: "stat",
    stats: AGENT_ALERT_STAT_COLUMNS,
    emptyText: "No SQL Server Agent alert capture yet.",
  },
  {
    title: "Alert Setup Gaps",
    subtitle: "severities 19 to 25, errors 823 to 825, operators",
    span: 2,
    viz: "table",
    rowsKey: "gaps",
    columns: AGENT_ALERT_GAP_COLUMNS,
    emptyText: "No gaps. Every severity from 19 to 25 and every error from 823 to 825 has an enabled alert that tells an operator.",
  },
  {
    title: "Alert Definitions",
    subtitle: "newest capture, fired judged over the last 24 hours",
    span: 2,
    viz: "table",
    rowsKey: "alerts",
    columns: AGENT_ALERT_COLUMNS,
    emptyText: "No SQL Server Agent alert is defined on this instance.",
  },
];

/* #6873: the Health Checks panels on the Configuration tab. One fact is the age and the state word the service gives it. A platform-managed
   target (and a native-only target's two backup facts) shows the platform's label beside the age and is never overdue. Overdue databases sort first. */
function protectionFactText(f) {
  if (!f) return "-";
  const state = f.state === "overdue" || f.state === "ok" ? "" : String(f.state).replace(/_/g, " ");
  const overdue = f.overdue ? " - overdue" : "";
  if (f.age) return f.age + (state ? " (" + state + ")" : "") + overdue;
  return (state || "-") + overdue;
}
function protectionFact(key, label) {
  return {
    key,
    label,
    display: (r) => protectionFactText(r[key]),
    sortValue: (r) => (r[key] && r[key].overdue ? 0 : 1),
    cellClass: (r) => (r[key] && r[key].overdue ? "sev-High" : null),
  };
}
const DATABASE_PROTECTION_COLUMNS = [
  { key: "database", label: "Database" },
  { key: "state", label: "State", small: true },
  protectionFact("full_backup", "Last full backup"),
  protectionFact("log_backup", "Last log backup"),
  protectionFact("checkdb", "Last good CHECKDB"),
  { key: "platform", label: "Platform", display: (r) => (r.platform && r.platform !== "native" ? String(r.platform).replace(/_/g, " ") : ""), small: true },
  { key: "excluded_reason", label: "Not judged because", small: true },
];
const DATABASE_PROTECTION_STATS = [
  { key: "summary.text", label: "Overdue", format: "text", small: true },
  { key: "captured_at", label: "Captured", format: "time", small: true },
];
const overdueFirst = (rows) => [...rows].sort((a, b) => {
  const score = (r) => ["full_backup", "log_backup", "checkdb"].filter((k) => r[k] && r[k].overdue).length;
  return score(b) - score(a);
});
export const DATABASE_PROTECTION_SPECS = [
  {
    title: "Health Checks",
    subtitle: "full backup 7 days, log backup 60 minutes, CHECKDB 14 days, collected hourly",
    span: 2,
    viz: "stat",
    stats: DATABASE_PROTECTION_STATS,
    emptyText: "No database protection capture yet.",
  },
  {
    title: "Backups and CHECKDB by Database",
    subtitle: "newest capture, overdue first",
    span: 2,
    viz: "table",
    rowsKey: "databases",
    columns: DATABASE_PROTECTION_COLUMNS,
    orderRows: overdueFirst,
    emptyText: "No databases in the newest capture.",
  },
];

