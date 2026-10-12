/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * What each server-page tab reads, for its time-range reach (#5562 review r1 M4, ruling R2; moved out of server-tabs.js in r3).
 *
 * `reachReads` names the windowed reads a tab makes (names, never hours): the picker offers the tab the smallest `max_hours` the
 * catalog gives them. `collector` names the collector whose sample interval the picker shows for the tab. They live here, keyed
 * by tab id, and not on the tab objects in server-tabs.js, because that file is read as text by the pins that count how often a
 * tab FETCHES a read (a name in a reach list is not a fetch) and that find a tab's neighbouring fields by position. The registry
 * applies this table to its tabs once, at load (withTabReach), so a tab still carries `reachReads` and `collector` at run time.
 * WebServerPageRangeTests holds the lists: every name served, every windowed read a tab makes declared.
 */

/** The SQL Server registry's tabs, by id. */
export const SQL_TAB_REACH = {
  overview: {
    collector: "cpu_utilization",
    reachReads: [
      "get_analysis_findings", "get_blocking_trend", "get_cpu_utilization", "get_deadlock_trend",
      "get_file_io_trend", "get_memory_trend"
    ]
  },
  waits: {
    collector: "wait_stats",
    reachReads: [
      "get_latch_stats", "get_spinlock_stats", "get_wait_stats", "get_wait_trend", "get_waiting_tasks"
    ]
  },
  cpu: {
    collector: "cpu_utilization",
    reachReads: [
      "get_cpu_utilization", "get_server_trend", "get_top_procedures_by_cpu", "get_top_queries_by_cpu"
    ]
  },
  memory: {
    collector: "memory_stats",
    reachReads: [
      "get_memory_grants", "get_memory_pressure_events", "get_memory_trend", "get_plan_cache_bloat",
      "get_resource_semaphore", "get_server_trend"
    ]
  },
  blocking: {
    collector: "blocked_process_report",
    reachReads: [
      "get_blocked_process_xml", "get_blocking", "get_blocking_stats", "get_blocking_trend",
      "get_current_waits_trend", "get_deadlock_detail", "get_deadlock_trend", "get_deadlocks",
      "get_lock_wait_trend"
    ]
  },
  io: {
    collector: "file_io_stats",
    reachReads: [
      "get_file_io_trend", "get_tempdb_trend"
    ]
  },
  queries: {
    collector: "query_stats",
    reachReads: [
      "get_active_queries", "get_long_query_completions", "get_plan_corrections", "get_procedure_duration_trend",
      "get_query_duration_trend", "get_query_heatmap", "get_query_store_clutter",
      "get_query_store_duration_trend", "get_query_store_regressions", "get_query_store_top", "get_query_trend",
      "get_top_procedures_by_cpu", "get_top_queries_by_cpu"
    ]
  },
  changes: {
    reachReads: [
      "get_database_config_changes", "get_server_config_changes", "get_trace_flag_changes"
    ]
  },
  activity: {
    reachReads: [
      "get_job_history", "get_perfmon_trend", "get_server_trend"
    ]
  },
  events: {
    reachReads: [
      "get_default_trace_events", "get_health_parser_cpu_tasks", "get_health_parser_io_issues",
      "get_health_parser_memory_broker", "get_health_parser_memory_conditions",
      "get_health_parser_memory_node_oom", "get_health_parser_scheduler_issues",
      "get_health_parser_severe_errors", "get_health_parser_significant_waits", "get_health_parser_system_health"
    ]
  },
  health: {
    reachReads: [
      "get_collection_log"
    ]
  }
};

/** The PostgreSQL registry's tabs, by id. */
export const POSTGRES_TAB_REACH = {
  overview: {
    reachReads: [
      "get_analysis_findings", "get_collection_log", "get_pg_autovacuum_health", "get_pg_cpu_utilization",
      "get_pg_extensions", "get_pg_replication_slots", "get_pg_wraparound_risk", "get_pg_xmin_horizon"
    ]
  },
  activity: {
    reachReads: [
      "get_pg_blocking", "get_pg_column_stats", "get_pg_database_stats", "get_pg_database_trend",
      "get_pg_deadlocks", "get_pg_lock_stats", "get_pg_log_events", "get_pg_plan_capture_readiness",
      "get_pg_plans", "get_pg_predicate_stats", "get_pg_query_duration_trend", "get_pg_top_queries"
    ]
  },
  vacuum: {
    reachReads: [
      "get_pg_autovacuum_health", "get_pg_session_states", "get_pg_wraparound_risk", "get_pg_xmin_horizon"
    ]
  },
  waits: {
    collector: "pg_wait_sampling",
    reachReads: [
      "get_pg_kernel_stats", "get_pg_wait_sampling", "get_pg_wait_stats", "get_pg_wait_trend"
    ]
  },
  io: {
    collector: "pg_io_stats",
    reachReads: [
      "get_pg_buffer_usage", "get_pg_io_stats", "get_pg_io_trend", "get_pg_write_stats"
    ]
  },
  replication: {
    reachReads: [
      "get_pg_replication_slots", "get_pg_replication_stats"
    ]
  },
  storage: {
    reachReads: [
      "get_pg_index_bloat", "get_pg_index_usage", "get_pg_table_bloat"
    ]
  },
  config: {
    reachReads: [
      "get_pg_server_config_changes"
    ]
  }
};

/** Puts each tab's reach declaration on the tab object. A tab with no entry is left as it is (it reads at the common reach). */
export function withTabReach(tabs, reach) {
  for (const tab of tabs) {
    if (reach[tab.id]) Object.assign(tab, reach[tab.id]);
  }
  return tabs;
}
