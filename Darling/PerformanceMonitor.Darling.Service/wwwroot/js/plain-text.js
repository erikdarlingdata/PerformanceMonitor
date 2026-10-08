/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Plain words for text the web page shows that the service wrote for an MCP client.

   The service's messages are one text for every reader. An MCP client can pass `hours_back`, read `hints.captures` and call
   `get_collection_health`; a person on the web page cannot, so the same sentence reads as internal names there. The service
   text stays as it is (the client needs those names and many tests pin it); the page runs what it shows through plainText()
   and shows the result.

   Only a short list of known names is rewritten, never "every snake_case word": a PostgreSQL object such as
   pg_stat_statements is a real name the reader may need. */

/** Every MCP tool whose name starts with get_, the only names the tool-name rule rewrites. A user's own text can hold a
    get_ word that is no tool (a procedure dbo.get_orders, a server get_prod), and it must reach the page as written.
    WebPlainTextTests checks this list against the MCP tool registry both ways, so it cannot drift. */
export const MCP_TOOL_NAMES = [
  "get_active_queries", "get_active_query_plan_xml", "get_ag_health", "get_alert_history", "get_alert_settings",
  "get_analysis_facts", "get_analysis_findings", "get_blocked_process_xml", "get_blocking", "get_blocking_plan_xml",
  "get_blocking_stats", "get_blocking_trend", "get_collection_health", "get_collection_log", "get_collector_cost",
  "get_collector_stall_probes", "get_cpu_scheduler_pressure", "get_cpu_utilization", "get_current_waits_trend",
  "get_custom_alert_rule", "get_custom_view", "get_daily_summary", "get_daily_summary_range", "get_database_config",
  "get_database_config_changes", "get_database_scoped_config", "get_database_sizes", "get_deadlock_detail",
  "get_deadlock_plan_xml", "get_deadlock_trend", "get_deadlocks", "get_default_trace_events", "get_file_io_stats",
  "get_file_io_trend", "get_finops", "get_finops_inventory", "get_finops_recommendations", "get_fleet_overview",
  "get_health_parser_cpu_tasks", "get_health_parser_io_issues", "get_health_parser_memory_broker",
  "get_health_parser_memory_conditions", "get_health_parser_memory_node_oom", "get_health_parser_scheduler_issues",
  "get_health_parser_severe_errors", "get_health_parser_significant_waits", "get_health_parser_system_health",
  "get_index_usage", "get_job_history", "get_latch_stats", "get_lock_wait_trend", "get_long_query_completions",
  "get_memory_clerks", "get_memory_grants", "get_memory_pressure_events", "get_memory_stats", "get_memory_trend",
  "get_mute_rules", "get_notification_routes", "get_object_locking", "get_oversized_plan_backlog",
  "get_perfmon_stats", "get_perfmon_trend", "get_pg_autovacuum_health", "get_pg_blocking", "get_pg_buffer_usage",
  "get_pg_column_stats", "get_pg_cpu_utilization", "get_pg_database_stats", "get_pg_database_trend",
  "get_pg_deadlock_detail", "get_pg_deadlocks", "get_pg_extensions", "get_pg_index_bloat", "get_pg_index_usage",
  "get_pg_io_stats", "get_pg_io_trend", "get_pg_kernel_stats", "get_pg_lock_stats", "get_pg_log_events",
  "get_pg_logging_audit", "get_pg_plan_capture_readiness", "get_pg_plans", "get_pg_predicate_stats",
  "get_pg_query_duration_trend", "get_pg_replication_slots", "get_pg_replication_stats", "get_pg_server_config",
  "get_pg_server_config_changes", "get_pg_session_states", "get_pg_table_bloat", "get_pg_top_queries",
  "get_pg_wait_sampling", "get_pg_wait_stats", "get_pg_wait_trend", "get_pg_wraparound_risk", "get_pg_write_stats",
  "get_pg_xmin_horizon", "get_plan_cache_bloat", "get_plan_corrections", "get_plan_xml",
  "get_procedure_duration_trend", "get_procedure_plan_xml", "get_pvs_stats", "get_query_duration_trend",
  "get_query_heatmap", "get_query_repro_script", "get_query_store_clutter", "get_query_store_duration_trend",
  "get_query_store_health", "get_query_store_plan_xml", "get_query_store_query_history",
  "get_query_store_regressions", "get_query_store_top", "get_query_trend", "get_read_latency",
  "get_resource_semaphore", "get_running_jobs", "get_server_config", "get_server_config_changes",
  "get_server_properties", "get_server_summary", "get_server_trend", "get_session_stats", "get_slow_reads",
  "get_spinlock_stats", "get_store_host", "get_store_log", "get_store_metrics", "get_store_query_history",
  "get_store_query_stats", "get_sweep_reports", "get_table_index_sizes", "get_tempdb_trend", "get_tool_guide",
  "get_top_procedures_by_cpu", "get_top_queries_by_cpu", "get_trace_flag_changes", "get_trace_flags",
  "get_wait_stats", "get_wait_trend", "get_wait_types", "get_waiting_tasks",
];
const TOOL_NAME_SET = new Set(MCP_TOOL_NAMES);

/** The label=value measurement labels the collectors write (the CollectorMeasurement labels). A lone label=value pair is
    rewritten only when its label is one of these; WebPlainTextTests checks the list against the collectors' source. */
export const MEASUREMENT_LABELS = [
  "csv_records_discarded", "events_read", "events_stored", "foreign_zone_lines_skipped", "forged_captures_skipped",
  "identity_epoch_changes", "job_history_identity_regressions", "job_history_identity_store_watermark",
  "job_history_identity_target_row", "json_records_discarded", "log_bytes_skipped", "log_files_skipped_by_rotation",
  "log_matches_limited", "log_resume_file_missing", "log_resume_file_recycled", "postmaster_epoch_changes",
  "raise_shaped_deadlocks_skipped", "report_xml_empty", "report_xml_unparsed", "shred_gated", "statement_scrub_ms",
  "statement_scrub_named", "statement_scrub_timeouts", "statement_scrub_unjudged", "statements_dealloc",
  "statements_epoch_changes",
];
const LABEL_SET = new Set(MEASUREMENT_LABELS);

/** A tool name, in the words its page uses: get_collection_health -> Collection Health. */
function toolWords(name) {
  return name
    .split("_")
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(" ");
}

/** The shown form of `label=value` counts: a run of pairs becomes "label words: value, label words: value". */
function pairsToText(run) {
  return run
    .split(" ")
    .map((pair) => {
      const eq = pair.indexOf("=");
      return pair.slice(0, eq).replace(/_/g, " ") + ": " + pair.slice(eq + 1);
    })
    .join(", ");
}

/* The same counts with the "=" lost: shred_gated_1 events_read_0 report_xml_empty_0. Each token's last part is the value. */
function underscoreCountsToText(run) {
  return run
    .split(" ")
    .map((tok) => {
      const at = tok.lastIndexOf("_");
      return tok.slice(0, at).replace(/_/g, " ") + ": " + tok.slice(at + 1);
    })
    .join(", ");
}

/* Fields of the Collection Health row that its own sentences point at, named by the column that shows them. */
const FIELD_WORDS = [
  [/\blast_error\b/g, "Last Error"],
  [/\blast_note\b/g, "Note"],
  [/\bnote_count\b/g, "the note count"],
  [/\btotal_runs\b/g, "Runs"],
  [/\bsession_missing\b/g, "session missing"],
];

/* The rewrites that name one known internal token each (a hints pointer, a window parameter, a health field). They never
   touch anything else, so they are safe on text the service did not write, such as a collector's Last Error. */
function namedTokenText(text) {
  let t = text;

  /* A pointer into the MCP answer's own `hints`, which the page never shows. */
  t = t.replace(/\s*[—–-]+\s*see hints\.captures for which collectors ran and when/g, "");

  /* The health sentence's denial flag, said in words. The "true" form carries its own explanation after the dash. */
  t = t.replace(/, and denied_since_last_success is true - the newest denial postdates the newest success,/g, ", and the newest denial is newer than the newest success,");
  t = t.replace(/ \(denied_since_last_success is false\)/g, "");
  t = t.replace(/\bdenied_since_last_success is true\b/g, "a denial is newer than the last success");
  t = t.replace(/\bdenied_since_last_success is false\b/g, "no denial is newer than the last success");

  /* The window parameter an MCP client passes; the page calls it the time range. */
  t = t.replace(/\bwiden days_back\b/g, "widen the date range");
  t = t.replace(/\bmove as_of\b/g, "move the end date");
  t = t.replace(/\bdays_back\b/g, "the date range");
  t = t.replace(/\bwiden hours_back\b/g, "widen the time range");
  t = t.replace(/\bhours_back\b/g, "the time range");

  t = t.replace(/\bCheck list_servers\b/g, "Check the server list");
  t = t.replace(/\bpg_wraparound_stats\b/g, "the freeze-headroom collector");

  for (const [re, words] of FIELD_WORDS) t = t.replace(re, words);
  return t;
}

/**
 * `text` with only the named internal tokens put into words, nothing else. For text that may echo user data or a real
 * error message (a Last Error), where the count and tool-name rules could change what the reader needs to see.
 */
export function plainTextNamed(text) {
  if (typeof text !== "string" || text === "") return text;
  return namedTokenText(text);
}

/**
 * `text` with the internal names a web reader cannot use put into words. Text with none of them comes back unchanged.
 * Not a string (null, a number) comes back as it is.
 */
export function plainText(text) {
  if (typeof text !== "string" || text === "") return text;
  if (text.includes(ENGINE_GATE_MARK)) return NOT_COLLECTED_LINE;
  let t = text;
  t = namedTokenText(t);

  /* A collector's measurement counts: shred_gated=1 events_read=0 report_xml_empty=0. A run of two or more pairs, or a
     lone pair whose label a collector really writes; any other lone pair, such as timeout=30 in a driver message, stays. */
  t = t.replace(/(?<![\w.-])[a-z][a-z0-9]*(?:_[a-z0-9]+)*=\d+(?: [a-z][a-z0-9]*(?:_[a-z0-9]+)*=\d+)*(?![\w=])/g, (run) =>
    run.includes(" ") || LABEL_SET.has(run.slice(0, run.indexOf("="))) ? pairsToText(run) : run,
  );

  /* The same counts written with an underscore for the "=": a run of two or more name_digits words. A lone word such as
     pg_stat_statements_1 is left alone, so a real object name followed by a number is not rewritten. */
  t = t.replace(/(?<![\w.])[a-z][a-z0-9]*(?:_[a-z][a-z0-9]*)*_\d+(?: [a-z][a-z0-9]*(?:_[a-z][a-z0-9]*)*_\d+)+(?![\w])/g, underscoreCountsToText);

  /* A tool name: use get_collection_health -> use Collection Health. Only a real tool name; any other get_ word is the
     reader's own text (an object, a server) and stays as written. */
  t = t.replace(/(?<![\w.])get_([a-z0-9]+(?:_[a-z0-9]+)*)(?![\w])/g, (m, rest) => (TOOL_NAME_SET.has(m) ? toolWords(rest) : m));

  return t;
}

/** The one short line the page shows where the server says a collector can never run on this kind of server. The
    server's own sentence names the collector, the engine gate and the way it is decided, none of which helps a reader. */
export const NOT_COLLECTED_LINE = "This server does not collect this data (it does not apply to this kind of server).";

/** The words every engine-gate sentence ends on (CollectorEngineCapability's permanent-gap epilogue). A "not collected"
    answer that is something else, such as a plan that was never stored, does not carry them and keeps its own sentence. */
const ENGINE_GATE_MARK = "permanent engine capability gap";
