/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The table definitions for reads, once: read name -> { rowsKey, columns, emptyText }. An alert notebook's read
 * cell names a read and `viz: "table"` and nothing else, so the page looks the table up here. The starter
 * dashboards spread the same entries into their table panels.
 */
export const READ_TABLES = {
  get_blocking: {
    rowsKey: "events",
    emptyText: "No blocking events in this window.",
    columns: [
      { key: "event_time", label: "Time", format: "time" },
      { key: "blocked_sql_text", label: "Blocked SQL", wrap: true },
      { key: "blocking_sql_text", label: "Blocking SQL", wrap: true },
      { key: "database_name", label: "Database" },
      { key: "blocked_spid", label: "Blocked", format: "int" },
      { key: "blocking_spid", label: "Blocker", format: "int" },
      { key: "wait_time_ms", label: "Wait", format: "ms" },
      { key: "lock_mode", label: "Mode" },
      { key: "contentious_object", label: "Object" },
    ],
  },
  get_top_queries_by_cpu: {
    rowsKey: "queries",
    emptyText: "No query stats in this window. Delta-based collection needs at least two cycles (~30 minutes).",
    /* #4231: raw query_stats is dropped at 4 days once the rollups are armed; `noteKey` (#3278)
       surfaces the read's own `truncation_note` when this panel's 24-hour default (or an edited,
       wider one) outran what the raw tier still holds. */
    noteKey: "truncation_note",
    columns: [
      { key: "database_name", label: "Database" },
      { key: "query_text", label: "Query", wrap: true },
      { key: "execution_count", label: "Execs", format: "int" },
      { key: "total_cpu_ms", label: "Total CPU", format: "ms" },
      { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
      { key: "max_cpu_ms", label: "Max CPU", format: "ms" },
      { key: "max_dop", label: "Max DOP", format: "int" },
    ],
  },
  get_top_procedures_by_cpu: {
    rowsKey: "procedures",
    emptyText: "No procedure stats in this window. Delta-based collection needs at least two cycles (~30 minutes).",
    noteKey: "truncation_note",
    columns: [
      { key: "full_name", label: "Procedure" },
      { key: "database_name", label: "Database" },
      { key: "execution_count", label: "Execs", format: "int" },
      { key: "total_cpu_ms", label: "Total CPU", format: "ms" },
      { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
      { key: "avg_elapsed_ms", label: "Avg Elapsed", format: "ms" },
    ],
  },
  get_wait_stats: {
    rowsKey: "waits",
    emptyText: "No waits accumulated in this window.",
    columns: [
      { key: "wait_type", label: "Wait Type" },
      { key: "total_wait_time_ms", label: "Total Wait", format: "ms" },
      { key: "resource_wait_ms", label: "Resource", format: "ms" },
      { key: "total_signal_wait_ms", label: "Signal", format: "ms" },
      { key: "waiting_tasks", label: "Tasks", format: "int" },
      { key: "signal_wait_pct", label: "Signal %", format: "num1" },
    ],
  },
  get_collection_health: {
    rowsKey: "collectors",
    emptyText: "No collection log rows for this server yet.",
    columns: [
      { key: "collector", label: "Collector" },
      { key: "status", label: "Status", statusSev: true },
      { key: "total_runs", label: "Runs", format: "int" },
      { key: "errors", label: "Errors", format: "int" },
      { key: "avg_duration_ms", label: "Avg Dur", format: "ms" },
      { key: "last_success", label: "Last Success", format: "time" },
      { key: "note_summary", label: "Note", wrap: true },
    ],
  },

  get_active_queries: {
    rowsKey: "queries",
    emptyText: "No active-query snapshots in this window.",
    columns: [
      { key: "collection_time", label: "Time", format: "time" },
      { key: "query_text", label: "Query", wrap: true },
      { key: "session_id", label: "SPID", format: "int" },
      { key: "database_name", label: "Database" },
      { key: "status", label: "Status" },
      { key: "cpu_time_ms", label: "CPU", format: "ms" },
      { key: "elapsed_time_formatted", label: "Elapsed" },
      { key: "wait_type", label: "Wait" },
      { key: "blocking_session_id", label: "Blocked by", format: "int" },
    ],
  },
  get_cpu_scheduler_pressure: {
    rowsKey: ".",
    emptyText: "No scheduler snapshot in this window.",
    columns: [
      { key: "pressure_level", label: "Pressure" },
      { key: "schedulers", label: "Schedulers", format: "int" },
      { key: "runnable_tasks", label: "Runnable tasks", format: "int" },
      { key: "runnable_percent", label: "Runnable %", format: "num1" },
      { key: "worker_utilization_percent", label: "Worker use %", format: "num1" },
      { key: "queued_requests", label: "Queued requests", format: "int" },
    ],
  },
  get_deadlock_detail: {
    rowsKey: "deadlocks",
    emptyText: "No deadlock graph XML captured in this window.",
    columns: [
      { key: "deadlock_time", label: "Deadlock Time", format: "time" },
      { key: "victim_process_id", label: "Victim" },
      { key: "deadlock_graph_xml", label: "Deadlock graph", wrap: true },
    ],
  },
  get_deadlock_trend: {
    rowsKey: "trend",
    emptyText: "No deadlocks in this window.",
    columns: [
      { key: "time", label: "Time", format: "time" },
      { key: "count", label: "Deadlocks", format: "int" },
    ],
  },
  get_pg_wait_stats: {
    rowsKey: "waits",
    emptyText: "No waits accumulated in this window.",
    columns: [
      { key: "wait_type", label: "Wait" },
      { key: "total_wait_time_ms", label: "Total Wait", format: "ms" },
      { key: "avg_wait_time_ms", label: "Avg Wait", format: "ms" },
      { key: "pct_of_total_wait", label: "% of Total", format: "num1" },
    ],
  },
  get_waiting_tasks: {
    rowsKey: "tasks",
    emptyText: "No waiting tasks were captured in this window.",
    columns: [
      { key: "collection_time", label: "Time", format: "time" },
      { key: "session_id", label: "SPID", format: "int" },
      { key: "wait_type", label: "Wait" },
      { key: "wait_duration_ms", label: "Duration", format: "ms" },
      { key: "blocking_session_id", label: "Blocked by", format: "int" },
      { key: "database_name", label: "Database" },
    ],
  },
  get_resource_semaphore: {
    rowsKey: "grants",
    emptyText: "No resource semaphore samples in this window.",
    columns: [
      { key: "collection_time", label: "Time", format: "time" },
      { key: "resource_semaphore_id", label: "Semaphore", format: "int" },
      { key: "target_memory_mb", label: "Target MB", format: "int" },
      { key: "granted_memory_mb", label: "Granted MB", format: "int" },
      { key: "available_memory_mb", label: "Available MB", format: "int" },
      { key: "grantee_count", label: "Grantees", format: "int" },
      { key: "waiter_count", label: "Waiters", format: "int" },
    ],
  },
  get_memory_grants: {
    rowsKey: "grants",
    emptyText: "No memory grant samples in this window.",
    columns: [
      { key: "collection_time", label: "Time", format: "time" },
      { key: "granted_memory_mb", label: "Granted MB", format: "int" },
      { key: "used_memory_mb", label: "Used MB", format: "int" },
      { key: "available_memory_mb", label: "Available MB", format: "int" },
      { key: "grantee_count", label: "Grantees", format: "int" },
      { key: "waiter_count", label: "Waiters", format: "int" },
    ],
  },
  get_wait_trend: {
    rowsKey: "trend",
    emptyText: "No samples of this wait in this window.",
    columns: [
      { key: "time", label: "Time", format: "time" },
      { key: "wait_time_ms_per_second", label: "Wait ms/s", format: "num1" },
    ],
  },
  get_pg_wraparound_risk: {
    rowsKey: "databases",
    emptyText: "No freeze-headroom samples in this window.",
    columns: [
      { key: "database_name", label: "Database" },
      { key: "severity", label: "Severity" },
      { key: "frozen_xid_age", label: "Frozen XID Age", format: "int" },
      { key: "xids_remaining", label: "XIDs Left", format: "int" },
      { key: "pct_toward_wraparound", label: "% to Wraparound", format: "num2" },
      { key: "pct_toward_emergency_vacuum", label: "% to Emergency Vacuum", format: "num1" },
    ],
  },
  get_pg_autovacuum_health: {
    rowsKey: "tables",
    emptyText: "No table has pending autovacuum work.",
    columns: [
      { key: "database_name", label: "Database" },
      { key: "table_name", label: "Table" },
      { key: "severity", label: "Severity" },
      { key: "dead_tuples", label: "Dead Tuples", format: "int" },
      { key: "vacuum_threshold", label: "Vacuum At", format: "int" },
      { key: "threshold_ratio", label: "× Vacuum Threshold", format: "num2" },
      { key: "dead_tuples_growing", label: "Growing", format: "bool" },
    ],
  },
  get_pg_xmin_horizon: {
    rowsKey: "holders",
    emptyText: "Nothing held the xmin horizon back in this window.",
    columns: [
      { key: "source", label: "Source" },
      { key: "is_currently_winning", label: "Winning", format: "bool" },
      { key: "xmin_age", label: "xmin Age", format: "int" },
      { key: "peak_xmin_age", label: "Peak Age", format: "int" },
      { key: "holder", label: "Holder", wrap: true },
      { key: "detail", label: "Detail", wrap: true },
    ],
  },
  get_pg_session_states: {
    rowsKey: "sessions",
    emptyText: "No sessions were captured in this window.",
    columns: [
      { key: "pid", label: "PID", format: "int" },
      { key: "backend_type", label: "Backend" },
      { key: "database", label: "Database" },
      { key: "application_name", label: "Application" },
      { key: "last_state", label: "State" },
      { key: "last_wait_event", label: "Wait" },
      { key: "peak_xact_duration_ms", label: "Peak Xact", format: "ms" },
    ],
  },
  get_pg_replication_slots: {
    rowsKey: "slots",
    emptyText: "This server has no replication slots.",
    columns: [
      { key: "slot_name", label: "Slot" },
      { key: "slot_type", label: "Type" },
      { key: "is_active", label: "Active", format: "bool" },
      { key: "wal_status", label: "WAL Status" },
      { key: "retained_wal_gb", label: "Retained WAL GB", format: "num2" },
      { key: "severity", label: "Severity" },
    ],
  },
  get_pg_replication_stats: {
    rowsKey: "replicas",
    emptyText: "No replica was connected in this window.",
    columns: [
      { key: "application_name", label: "Replica" },
      { key: "state", label: "State" },
      { key: "sync_state", label: "Sync" },
      { key: "replay_lag_ms", label: "Replay Lag", format: "ms" },
      { key: "worst_replay_lag_ms", label: "Worst Lag", format: "ms" },
      { key: "replay_bytes_behind", label: "Bytes Behind", format: "int" },
    ],
  },
  get_long_query_completions: {
    rowsKey: "completions",
    emptyText: "No long-running completions in this window.",
    columns: [
      { key: "event_time", label: "Time", format: "time" },
      { key: "statement", label: "Statement", wrap: true },
      { key: "database_name", label: "Database" },
      { key: "duration_ms", label: "Duration", format: "ms" },
      { key: "cpu_ms", label: "CPU", format: "ms" },
      { key: "row_count", label: "Rows", format: "int" },
      { key: "session_id", label: "SPID", format: "int" },
    ],
  },
  get_plan_corrections: {
    rowsKey: "recommendations",
    emptyText: "No tuning recommendations in this window.",
    columns: [
      { key: "collection_time", label: "Collected", format: "time" },
      { key: "query_text", label: "Query", wrap: true },
      { key: "database_name", label: "Database" },
      { key: "query_id", label: "Query ID", format: "int" },
      { key: "recommendation_state", label: "State" },
      { key: "recommendation_reason", label: "Reason", wrap: true },
      { key: "score", label: "Score", format: "int" },
    ],
  },
  get_collection_log: {
    rowsKey: "runs",
    emptyText: "No collector runs in the selected window.",
    columns: [
      { key: "collection_time", label: "When", format: "time" },
      { key: "collector", label: "Collector" },
      { key: "status", label: "Status", statusSev: true },
      { key: "duration_ms", label: "Total", format: "ms" },
      { key: "rows_collected", label: "Rows", format: "int" },
      { key: "error_message", label: "Error", wrap: true },
    ],
  },
  get_running_jobs: {
    rowsKey: "jobs",
    emptyText: "No SQL Agent jobs were running at the last collection.",
    columns: [
      { key: "job_name", label: "Job" },
      { key: "start_time", label: "Started", format: "time" },
      { key: "current_duration_formatted", label: "Running for" },
      { key: "avg_duration_formatted", label: "Average" },
      { key: "is_running_long", label: "Long", format: "bool" },
    ],
  },
  get_collector_cost: {
    rowsKey: "trend",
    emptyText: "No cost samples for this collector.",
    columns: [
      { key: "day", label: "Day" },
      { key: "run_count", label: "Runs", format: "int" },
      { key: "total_sql_ms", label: "Total SQL", format: "ms" },
      { key: "avg_sql_ms", label: "Avg SQL", format: "ms" },
      { key: "max_sql_ms", label: "Max SQL", format: "ms" },
    ],
  },
  get_collector_stall_probes: {
    rowsKey: "probes",
    emptyText: "No stall probes were recorded.",
    columns: [
      { key: "probe_time", label: "Time", format: "time" },
      { key: "collector_name", label: "Collector" },
      { key: "outcome", label: "Outcome" },
      { key: "query_ms", label: "Query", format: "ms" },
      { key: "top_wait_type", label: "Top Wait" },
      { key: "waiting_task_count", label: "Waiting Tasks", format: "int" },
    ],
  },
  get_analysis_findings: {
    rowsKey: "findings",
    emptyText: "No findings in this window.",
    columns: [
      { key: "last_seen", label: "Last seen", format: "time" },
      { key: "category", label: "Category" },
      { key: "story_path", label: "Story", wrap: true },
      { key: "severity", label: "Severity", format: "num2" },
      { key: "confidence", label: "Confidence", format: "num2" },
      { key: "occurrences", label: "Occurrences", format: "int" },
      { key: "first_seen", label: "First seen", format: "time" },
    ],
  },
};

/**
 * The cell with its table definition filled in from READ_TABLES. A cell that already carries its own columns, a
 * cell that is not a table, and a read with no entry come back as they were.
 */
export function resolveReadTable(cell) {
  if (!cell || cell.viz !== "table" || (Array.isArray(cell.columns) && cell.columns.length)) return cell;
  const entry = READ_TABLES[cell.read];
  return entry ? { ...cell, ...entry } : cell;
}
