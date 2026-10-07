/* Which reads the server page's database filter (#5244, #5245) applies to. Pure data: no imports, so util.js can read it
   from readTool and a test can read it as text. Every SQL Server read that a module reachable from pages/server.js names
   sits in exactly ONE of the four classes below, and WebDatabaseFilterReadsTests fails a read that sits in none or in two,
   so a new panel cannot default to "server-wide" by silence. PostgreSQL targets get no filter; their reads carry the
   get_pg_ prefix and are in no class.

   Keep every list below to plain quoted names, one string per read: the tests read this file as text.

   Moving a read into FILTERED is a three-part change, pinned together by WebDatabaseFilterReadsTests: the read's catalog
   row in DarlingWebEndpoints.cs carries PDatabases(), its dispatch entry calls DatabaseNames(c), and its name moves from
   UNFILTERED to here. */

/** Reads whose route takes the chosen databases: readTool adds one `database_name` key per chosen database to each. The six
 *  top-query reads are here; each later page moves its reads out of UNFILTERED into this list. */
export const FILTERED = new Set([
  // Top queries, procedures, active queries, Query Store and the heatmap.
  "get_top_queries_by_cpu", "get_top_procedures_by_cpu", "get_active_queries", "get_query_store_top",
  "get_query_store_regressions", "get_query_heatmap",
  // Object contention and index usage (#5231 PR2): the Locking page reads both. The Locking row's detail request
  // (the `detail_*` selector) names its own database and ignores the injected filter.
  "get_object_locking", "get_index_usage",
  // Blocking and waits (#5244 PR3): the Blocking tab's blocking, trend, XML and stats reads and the Current Waits reads.
  // get_blocking_stats limits only its blocking series (deadlock severity stays whole).
  "get_blocking_trend", "get_waiting_tasks", "get_blocking", "get_current_waits_trend", "get_blocking_stats",
  "get_blocked_process_xml",
  // Duration trends, Query Store clutter, long queries and plan corrections (#5244 PR4).
  "get_query_duration_trend", "get_procedure_duration_trend", "get_query_store_duration_trend",
  "get_query_store_clutter", "get_long_query_completions", "get_plan_corrections",
]);

/** Database-scoped reads that cannot take the filter yet, so they show every database and say so (the "All databases"
 *  chip). 34 database-scoped reads in all: this list shrinks as FILTERED grows, and what is left at the end is the three
 *  deadlock reads, which stay unfiltered on purpose (as on the desktop): a deadlock spans several databases, and they are
 *  inside each deadlock graph. The groups follow the page that moves each read into FILTERED. */
export const UNFILTERED = new Set([
  // File I/O, sizes and the persistent version store.
  "get_file_io_trend", "get_file_io_stats", "get_database_sizes", "get_table_index_sizes", "get_pvs_stats",
  // Configuration, Query Store health, configuration changes, severe errors and the default trace.
  "get_database_config", "get_database_scoped_config", "get_query_store_health", "get_database_config_changes",
  "get_health_parser_severe_errors", "get_default_trace_events",
  // The deadlock reads: they stay here.
  "get_deadlock_trend", "get_deadlocks", "get_deadlock_detail",
]);

/** Reads that take `database_name` as the identity of a row, not as a filter: the query drill, the seven plan-viewer
 *  reads (the four plan reads, the Blocking and Deadlocks grids' plan reads of #5236, and the repro script of #5233, which
 *  name one stored row and send that row's own database, or none) and the Query Store History panel's read (#5300: one Query
 *  Store query, keyed by its database and query_id). The page always sends the row's own database, so the filter is never
 *  injected into them and they show no chip. */
export const IDENTITY = new Set([
  "get_query_trend", "get_plan_xml", "get_query_store_plan_xml", "get_active_query_plan_xml", "get_procedure_plan_xml",
  "get_query_store_query_history", "get_blocking_plan_xml", "get_deadlock_plan_xml", "get_query_repro_script",
]);

/** Every other SQL Server read a module of the server page names: the data has no database, so the filter does not apply.
 *  That includes the fleet, FinOps, alert and self-monitoring reads the page's shared modules name. */
export const SERVER_WIDE = new Set([
  "audit_config", "get_alert_history", "get_analysis_findings", "get_collection_health", "get_collection_log",
  "get_cpu_scheduler_pressure", "get_cpu_utilization", "get_daily_summary_range", "get_finops", "get_fleet_overview",
  "get_health_parser_cpu_tasks", "get_health_parser_io_issues", "get_health_parser_memory_broker",
  "get_health_parser_memory_conditions", "get_health_parser_memory_node_oom", "get_health_parser_scheduler_issues",
  "get_health_parser_significant_waits", "get_health_parser_system_health", "get_job_history", "get_latch_stats",
  "get_lock_wait_trend", "get_memory_clerks", "get_memory_grants", "get_memory_pressure_events", "get_memory_stats",
  "get_memory_trend", "get_mute_rules", "get_perfmon_stats", "get_perfmon_trend", "get_plan_cache_bloat",
  "get_read_latency", "get_resource_semaphore", "get_running_jobs", "get_server_config", "get_server_config_changes",
  "get_server_properties", "get_server_summary", "get_server_trend", "get_session_stats", "get_slow_reads",
  "get_spinlock_stats", "get_store_query_history", "get_tempdb_trend", "get_trace_flag_changes", "get_trace_flags",
  "get_wait_stats", "get_wait_trend",
]);

/** The class a read is in: "filtered", "unfiltered", "identity" or "server" (server-wide); null for a read no class names
 *  (a PostgreSQL read, or one added without being classified: WebDatabaseFilterReadsTests fails the second). */
export function readScope(tool) {
  if (FILTERED.has(tool)) return "filtered";
  if (UNFILTERED.has(tool)) return "unfiltered";
  if (IDENTITY.has(tool)) return "identity";
  if (SERVER_WIDE.has(tool)) return "server";
  return null;
}
