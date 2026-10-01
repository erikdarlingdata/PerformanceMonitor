/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The field definitions for reads, once: read name -> { table, line, stat }, each the part of a panel descriptor that viz
 * needs and the read's payload decides (table: rowsKey + columns; line: rowsKey + xKey + series; stat: stats). An
 * alert notebook's read cell names a read and a viz and nothing else, so the page looks the fields up here. The
 * starter dashboards spread the same `table` entries into their panels, so a read's columns live in one place.
 */
export const READ_FIELDS = {
  get_blocking: {
    table: {
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
  },
  get_top_queries_by_cpu: {
    table: {
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
  },
  get_top_procedures_by_cpu: {
    table: {
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
  },
  get_wait_stats: {
    table: {
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
  },
  get_collection_health: {
    table: {
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
  },

  get_active_queries: {
    table: {
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
  },
  get_cpu_scheduler_pressure: {
    table: {
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
    stat: {
      emptyText: "No scheduler snapshot in this window.",
      stats: [
        { key: "pressure_level", label: "Pressure", format: "text", small: true },
        { key: "schedulers", label: "Schedulers", format: "int" },
        { key: "runnable_tasks", label: "Runnable tasks", format: "int" },
        { key: "runnable_percent", label: "Runnable %", format: "num1" },
        { key: "worker_utilization_percent", label: "Worker use %", format: "num1" },
        { key: "queued_requests", label: "Queued requests", format: "int" },
      ],
    },
  },
  get_deadlock_detail: {
    table: {
      rowsKey: "deadlocks",
      emptyText: "No deadlock graph XML captured in this window.",
      columns: [
        { key: "deadlock_time", label: "Deadlock Time", format: "time" },
        { key: "victim_process_id", label: "Victim" },
        { key: "deadlock_graph_xml", label: "Deadlock graph", wrap: true },
      ],
    },
  },
  get_deadlock_trend: {
    table: {
      rowsKey: "trend",
      emptyText: "No deadlocks in this window.",
      columns: [
        { key: "time", label: "Time", format: "time" },
        { key: "count", label: "Deadlocks", format: "int" },
      ],
    },
    line: {
      rowsKey: "trend",
      xKey: "time",
      format: "int",
      emptyText: "No deadlocks in this window — an empty trend here means none happened, not that nothing was collected.",
      series: [{ key: "count", label: "Deadlocks" }],
    },
  },
  get_pg_wait_stats: {
    table: {
      rowsKey: "waits",
      emptyText: "No waits accumulated in this window.",
      columns: [
        { key: "wait_type", label: "Wait" },
        { key: "total_wait_time_ms", label: "Total Wait", format: "ms" },
        { key: "avg_wait_time_ms", label: "Avg Wait", format: "ms" },
        { key: "pct_of_total_wait", label: "% of Total", format: "num1" },
      ],
    },
  },
  get_waiting_tasks: {
    table: {
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
  },
  get_resource_semaphore: {
    table: {
      rowsKey: "grants",
      emptyText: "No resource semaphore samples in this window.",
      columns: [
        { key: "collection_time", label: "Time", format: "time" },
        { key: "resource_semaphore_id", label: "Semaphore", format: "int" },
        { key: "target_memory_mb", label: "Target MB", format: "mb" },
        { key: "granted_memory_mb", label: "Granted MB", format: "mb" },
        { key: "available_memory_mb", label: "Available MB", format: "mb" },
        { key: "grantee_count", label: "Grantees", format: "int" },
        { key: "waiter_count", label: "Waiters", format: "int" },
      ],
    },
  },
  get_memory_grants: {
    table: {
      rowsKey: "grants",
      emptyText: "No memory grant samples in this window.",
      columns: [
        { key: "collection_time", label: "Time", format: "time" },
        { key: "granted_memory_mb", label: "Granted MB", format: "mb" },
        { key: "used_memory_mb", label: "Used MB", format: "mb" },
        { key: "available_memory_mb", label: "Available MB", format: "mb" },
        { key: "grantee_count", label: "Grantees", format: "int" },
        { key: "waiter_count", label: "Waiters", format: "int" },
      ],
    },
  },
  get_wait_trend: {
    table: {
      rowsKey: "trend",
      emptyText: "No samples of this wait in this window.",
      columns: [
        { key: "time", label: "Time", format: "time" },
        { key: "wait_time_ms_per_second", label: "Wait ms/s", format: "num1" },
      ],
    },
    line: {
      rowsKey: "trend",
      xKey: "time",
      format: "num1",
      emptyText: "No samples of this wait in this window.",
      series: [{ key: "wait_time_ms_per_second", label: "Wait ms/s" }],
    },
  },
  get_pg_wraparound_risk: {
    table: {
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
  },
  get_pg_autovacuum_health: {
    table: {
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
  },
  get_pg_xmin_horizon: {
    table: {
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
  },
  get_pg_session_states: {
    table: {
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
  },
  get_pg_replication_slots: {
    table: {
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
  },
  get_pg_replication_stats: {
    table: {
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
  },
  get_long_query_completions: {
    table: {
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
  },
  get_plan_corrections: {
    table: {
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
  },
  get_collection_log: {
    table: {
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
  },
  get_running_jobs: {
    table: {
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
  },
  get_collector_cost: {
    table: {
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
    /* Without a collector_name the read answers for the whole fleet: collectors[] ranked, not a day trend. */
    tableFleet: {
      rowsKey: "collectors",
      emptyText: "No collector cost samples in this window.",
      columns: [
        { key: "collector_name", label: "Collector" },
        { key: "run_count", label: "Runs", format: "int" },
        { key: "total_sql_ms", label: "Total SQL", format: "ms" },
        { key: "avg_sql_ms", label: "Avg SQL", format: "ms" },
        { key: "max_sql_ms", label: "Max SQL", format: "ms" },
      ],
    },
  },
  get_tempdb_trend: {
    table: {
      rowsKey: "trend",
      emptyText: "No tempdb samples in this window.",
      columns: [
        { key: "time", label: "Time", format: "time" },
        { key: "total_reserved_mb", label: "Reserved", format: "mb" },
        { key: "user_objects_mb", label: "User objects", format: "mb" },
        { key: "internal_objects_mb", label: "Internal objects", format: "mb" },
        { key: "version_store_mb", label: "Version store", format: "mb" },
      ],
    },
    line: {
      rowsKey: "trend",
      xKey: "time",
      format: "mb",
      emptyText: "No tempdb samples in this window.",
      series: [
        { key: "total_reserved_mb", label: "Reserved" },
        { key: "user_objects_mb", label: "User objects" },
        { key: "internal_objects_mb", label: "Internal objects" },
        { key: "version_store_mb", label: "Version store" },
      ],
    },
  },
  get_database_sizes: {
    table: {
      rowsKey: "databases",
      emptyText: "No database sizes in the latest snapshot.",
      noteKey: "note",
      columns: [
        { key: "database_name", label: "Database" },
        { key: "total_size_mb", label: "Total", format: "mb" },
        { key: "used_size_mb", label: "Used", format: "mb" },
        /* size_note: the read's own sentence for the row another database on an Azure SQL Database server gets (its log
           size is not reported). Only that row has the key, so the column is left out unless a row fills it. */
        { key: "size_note", label: "Note", wrap: true, hideWhenEmpty: true },
      ],
    },
  },
  get_file_io_stats: {
    table: {
      rowsKey: "files",
      emptyText: "No file I/O rows in the latest snapshot.",
      columns: [
        { key: "database_name", label: "Database" },
        { key: "file_name", label: "File" },
        { key: "file_type", label: "Type" },
        { key: "size_mb", label: "Size", format: "mb", nullKey: "size_note" },
        { key: "avg_read_latency_ms", label: "Read latency", format: "num1" },
        { key: "avg_write_latency_ms", label: "Write latency", format: "num1" },
        { key: "delta_reads", label: "Reads", format: "int" },
        { key: "delta_writes", label: "Writes", format: "int" },
        { key: "physical_name", label: "Path", wrap: true },
      ],
    },
  },
  get_pvs_stats: {
    table: {
      rowsKey: "databases",
      emptyText:
        "No PVS rows. The collector reads a SQL Server 2019+ DMV, and a server with Accelerated Database Recovery off has nothing to report.",
      columns: [
        { key: "database_name", label: "Database" },
        { key: "is_adr_on", label: "ADR", format: "bool" },
        { key: "pvs_size_mb", label: "PVS size", format: "mb" },
        { key: "pct_of_database", label: "% of DB", format: "num1" },
        { key: "database_data_size_mb", label: "Data size", format: "mb" },
        { key: "aborted_transaction_count", label: "Aborted txns", format: "int" },
        { key: "oldest_active_transaction_id", label: "Oldest active txn" },
      ],
    },
  },
  get_default_trace_events: {
    table: {
      rowsKey: "events",
      emptyText: "No significant default trace events in this window.",
      columns: [
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
      ],
    },
  },
  get_server_summary: {
    /* One object, so the table draws it as one row (rowsKey "."); the stat part is the same keys as tiles. */
    table: {
      rowsKey: ".",
      emptyText: "No server summary is available.",
      columns: [
        { key: "cpu_percent", label: "CPU", format: "pct" },
        { key: "memory_mb", label: "Memory", format: "mb" },
        { key: "blocking_count", label: "Blocking (recent)", format: "int" },
        { key: "deadlock_count", label: "Deadlocks (recent)", format: "int" },
        { key: "last_collection", label: "Last collection", format: "time" },
      ],
    },
    stat: {
      emptyText: "No server summary is available.",
      stats: [
        { key: "cpu_percent", label: "CPU", format: "pct" },
        { key: "memory_mb", label: "Memory", format: "mb" },
        { key: "blocking_count", label: "Blocking (recent)", format: "int" },
        { key: "deadlock_count", label: "Deadlocks (recent)", format: "int" },
        { key: "last_collection", label: "Last collection", format: "reltime", small: true },
      ],
    },
  },
  get_ag_health: {
    table: {
      rowsKey: "availability_groups",
      emptyText: "No availability groups reported.",
      columns: [
        { key: "server_name", label: "Reporting server" },
        { key: "ag_name", label: "Group" },
        { key: "primary_replica", label: "Primary" },
        { key: "severity_label", label: "Severity", statusSev: true },
        { key: "collection_time", label: "Snapshot", format: "time" },
      ],
    },
  },
  get_store_metrics: {
    /* objects[] holds byte rows and background-job rows; the columns the other kind lacks stay empty. */
    table: {
      rowsKey: "objects",
      emptyText: "No store metrics were recorded.",
      columns: [
        { key: "object_kind", label: "Kind" },
        { key: "object_name", label: "Object" },
        { key: "total_bytes", label: "Bytes", format: "int", hideWhenEmpty: true },
        { key: "growth_bytes", label: "Growth (bytes)", format: "int", hideWhenEmpty: true },
        { key: "delta_since", label: "Since", hideWhenEmpty: true },
        { key: "last_run_duration_ms", label: "Last run", format: "ms", hideWhenEmpty: true },
        { key: "schedule_interval_ms", label: "Cadence", format: "ms", hideWhenEmpty: true },
        { key: "duration_vs_cadence_percent", label: "Of cadence %", format: "num1", hideWhenEmpty: true },
        { key: "total_failures", label: "Failures", format: "int", hideWhenEmpty: true },
        { key: "failures_in_window", label: "Failures in window", format: "int", hideWhenEmpty: true },
      ],
    },
  },
  get_mute_rules: {
    table: {
      rowsKey: "mute_rules",
      emptyText: "No mute rules are configured.",
      columns: [
        { key: "id", label: "Id", format: "int" },
        { key: "enabled", label: "Enabled", format: "bool" },
        { key: "server_name", label: "Server" },
        { key: "metric_name", label: "Metric" },
        { key: "expires_at_utc", label: "Expires", format: "time" },
        { key: "reason", label: "Reason", wrap: true },
      ],
    },
  },
  get_alert_history: {
    table: {
      rowsKey: "alerts",
      emptyText: "No alerts in this window.",
      columns: [
        { key: "alert_time", label: "Time", format: "time" },
        { key: "server_name", label: "Server" },
        { key: "metric_name", label: "Metric" },
        { key: "current_value", label: "Value", format: "num2" },
        { key: "threshold_value", label: "Threshold", format: "num2" },
        { key: "notification_type", label: "Notification" },
        { key: "muted", label: "Muted", format: "bool" },
        { key: "detail_text", label: "Detail", wrap: true },
      ],
    },
  },
  get_cpu_utilization: {
    table: {
      rowsKey: "samples",
      emptyText: "No CPU samples in this window.",
      columns: [
        { key: "sample_time", label: "Time", format: "time" },
        { key: "sql_server_cpu", label: "SQL CPU %", format: "int" },
        { key: "other_process_cpu", label: "Other %", format: "int" },
        { key: "total_cpu", label: "Total %", format: "int" },
      ],
    },
    line: {
      rowsKey: "samples",
      xKey: "sample_time",
      format: "pct",
      unit: "%",
      emptyText: "No CPU samples in this window.",
      series: [
        { key: "sql_server_cpu", label: "SQL CPU %" },
        { key: "other_process_cpu", label: "Other %" },
        { key: "total_cpu", label: "Total %" },
      ],
    },
  },
  get_pg_cpu_utilization: {
    table: {
      rowsKey: "samples",
      emptyText: "No instance CPU samples in this window. Performance Insights CPU is collected for Aurora targets only.",
      columns: [
        { key: "sample_time", label: "Time", format: "time" },
        { key: "cpu_percent", label: "CPU %", format: "num1" },
        { key: "peak_cpu_percent", label: "Peak CPU %", format: "num1" },
        { key: "acu_utilization_percent", label: "ACU %", format: "num1" },
      ],
    },
    line: {
      rowsKey: "samples",
      xKey: "sample_time",
      format: "pct",
      unit: "%",
      emptyText: "No instance CPU samples in this window. Performance Insights CPU is collected for Aurora targets only.",
      series: [
        { key: "cpu_percent", label: "CPU %" },
        { key: "acu_utilization_percent", label: "ACU %" },
      ],
    },
  },
  get_blocking_stats: {
    table: {
      rowsKey: "blocking_duration",
      emptyText: "No blocking in this window.",
      columns: [
        { key: "time", label: "Time", format: "time" },
        { key: "event_count", label: "Events", format: "int" },
        { key: "total_duration_ms", label: "Total wait", format: "ms" },
        { key: "max_duration_ms", label: "Max wait", format: "ms" },
        { key: "avg_duration_ms", label: "Avg wait", format: "ms" },
      ],
    },
    line: {
      rowsKey: "blocking_duration",
      xKey: "time",
      format: "ms",
      emptyText: "No blocking in this window.",
      series: [
        { key: "total_duration_ms", label: "Total Wait (ms)" },
        { key: "max_duration_ms", label: "Max Wait (ms)" },
      ],
    },
  },
  get_lock_wait_trend: {
    table: {
      rowsKey: "trend",
      emptyText: "No lock waits in this window.",
      columns: [
        { key: "collection_time", label: "Time", format: "time" },
        { key: "wait_time_ms_per_second", label: "Lock wait ms/s", format: "num1" },
        { key: "peak_wait_time_ms_per_second", label: "Peak ms/s", format: "num1" },
      ],
    },
    line: {
      rowsKey: "trend",
      xKey: "collection_time",
      format: "num1",
      emptyText: "No lock waits in this window.",
      series: [{ key: "wait_time_ms_per_second", label: "Lock wait ms/s" }],
    },
  },
  get_deadlocks: {
    table: {
      rowsKey: "deadlocks",
      emptyText: "No deadlocks in this window.",
      columns: [
        { key: "deadlock_time", label: "Deadlock Time", format: "time" },
        { key: "victim_process_id", label: "Victim" },
        { key: "victim_sql_text", label: "Victim SQL", wrap: true },
        { key: "process_summary", label: "Processes", wrap: true },
        { key: "has_deadlock_xml", label: "Graph captured", format: "bool" },
      ],
    },
  },
  get_query_store_regressions: {
    table: {
      rowsKey: "regressions",
      emptyText: "No Query Store regressions in this window.",
      columns: [
        { key: "database_name", label: "Database" },
        { key: "query_id", label: "Query id" },
        { key: "severity", label: "Severity" },
        { key: "baseline_cpu_ms", label: "Baseline CPU", format: "ms" },
        { key: "recent_cpu_ms", label: "Recent CPU", format: "ms" },
        { key: "cpu_regression_percent", label: "CPU change %", format: "num1" },
        { key: "duration_regression_percent", label: "Duration change %", format: "num1" },
        { key: "additional_duration_ms", label: "Added time", format: "ms" },
        { key: "query_text", label: "Query", wrap: true },
      ],
    },
  },
  get_collector_stall_probes: {
    table: {
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
  },
  get_analysis_findings: {
    table: {
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
  },
};

/**
 * The cell with the fields its viz needs filled in from READ_FIELDS. A cell that already carries its own field
 * array, a viz the catalog has no part for, and a read with no entry come back as they were.
 */
export function resolveReadTable(cell) {
  if (!cell) return cell;
  const field = cell.viz === "line" ? "series" : cell.viz === "stat" ? "stats" : cell.viz === "table" ? "columns" : null;
  if (!field || (Array.isArray(cell[field]) && cell[field].length)) return cell;
  const entry = READ_FIELDS[cell.read];
  const fleet = cell.viz === "table" && entry && entry.tableFleet && !(cell.params && cell.params.collector_name);
  const part = entry && (fleet ? entry.tableFleet : entry[cell.viz]);
  return part ? { ...cell, ...part } : cell;
}
