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
  "get_blocking_stats", "get_blocking_trend", "get_change_watches", "get_known_patterns", "get_client_timeouts", "get_collection_health", "get_collection_log", "get_collection_state", "get_collector_cost", "get_collector_schedules",
  "get_collector_stall_probes", "get_agent_changes", "get_command_result", "get_compiles_after_evict", "get_cpu_by_database", "get_cpu_scheduler_pressure", "get_cpu_utilization", "get_current_waits_trend",
  "get_custom_alert_rule", "get_custom_view", "get_daily_summary", "get_daily_summary_range", "get_database_config",
  "get_database_config_changes", "get_database_scoped_config", "get_database_state_expectations", "get_database_sizes", "get_deadlock_detail",
  "get_deadlock_plan_xml", "get_deadlock_trend", "get_deadlocks", "get_default_trace_events", "get_file_io_stats",
  "get_file_io_trend", "get_finops", "get_finops_inventory", "get_finops_recommendations", "get_fleet_overview", "get_forced_plans", "get_query_store_hints",
  "get_health_parser_cpu_tasks", "get_health_parser_io_issues", "get_health_parser_memory_broker",
  "get_health_parser_memory_conditions", "get_health_parser_memory_node_oom", "get_health_parser_scheduler_issues",
  "get_health_parser_severe_errors", "get_health_parser_significant_waits", "get_health_parser_system_health",
  "get_index_usage", "get_investigation_evidence", "get_investigation_reports", "get_job_history", "get_latch_stats", "get_load_by_caller", "get_lock_wait_trend", "get_long_query_completions",
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
  "get_pg_xmin_horizon", "get_plan_cache_bloat", "get_plan_corrections", "get_plan_force_precheck", "get_plan_xml",
  "get_procedure_duration_trend", "get_procedure_plan_xml", "get_pvs_stats", "get_query_duration_trend",
  "get_query_heatmap", "get_query_pileups", "get_query_repro_script", "get_query_store_aborts", "get_query_store_clutter", "get_query_store_duration_trend",
  "get_query_store_health", "get_query_store_plan_xml", "get_query_store_query_history",
  "get_query_store_regressions", "get_query_store_top", "get_query_trend", "get_read_latency",
  "get_resource_semaphore", "get_resource_use_by_tag", "get_running_jobs", "get_server_actions", "get_server_config", "get_server_config_changes", "get_server_settings",
  "get_server_properties", "get_server_summary", "get_server_trend", "get_session_stats", "get_slow_reads",
  "get_spinlock_stats", "get_store_host", "get_store_log", "get_store_metrics", "get_store_query_history",
  "get_store_query_stats", "get_sweep_reports", "get_table_index_sizes", "get_tempdb_trend", "get_tool_guide",
  "get_top_procedures_by_cpu", "get_top_queries_by_cpu", "get_trace_flag_changes", "get_trace_flags",
  "get_wait_stats", "get_wait_trend", "get_wait_types", "get_waiting_tasks",
];
const TOOL_NAME_SET = new Set(MCP_TOOL_NAMES);

/** Every other MCP tool the service registers (the ones whose name does not start with get_): reads that act, and the write
    tools. A message that names one tells a client what to call next; a web reader cannot call it. WebPlainTextTests checks
    this list against the registry, like the get_ list above. */
export const MCP_ACTION_TOOL_NAMES = [
  "add_servers", "analyze_now", "analyze_plan_xml", "analyze_procedure_plan", "analyze_query_plan", "analyze_query_store_plan",
  "analyze_server", "approve_investigation_note", "assign_query_tag", "assign_server_tag", "audit_config", "backtest_custom_alert_rule", "claim_investigation_request",
  "clear_quiet_window", "close_investigation_request", "compare_analysis", "create_custom_alert_rule", "create_custom_view", "create_known_pattern", "create_mute_rule",
  "create_notification_route", "create_query_tag", "create_query_tag_rule", "create_server_tag", "delete_custom_alert_rule",
  "delete_custom_view", "delete_known_pattern", "delete_mute_rule", "delete_notification_route", "delete_query_tag", "delete_query_tag_rule",
  "delete_server_tag", "describe_custom_view_catalog", "disable_collector", "disable_server", "dismiss_alert_history", "edit_server",
  "enable_collector", "enable_server", "evict_cached_plan", "find_database", "force_query_store_plan", "generate_webhook_signing_key", "kill_session",
  "list_custom_alert_rules", "list_custom_alert_templates", "list_custom_views", "list_investigation_requests", "list_query_tag_rules",
  "list_query_tags", "list_servers", "merge_deadlock_signatures", "mute_analysis_finding", "pause_collection", "purge_expired_mute_rules", "purge_now",
  "create_plan_guide", "drop_plan_guide", "recompile_module",
  "record_investigation_feedback", "reject_investigation_note", "reload_config", "remove_server", "remove_webhook_signing_key", "reset_collector_schedules", "restore_alert_history", "resume_collection", "run_collectors_now", "run_custom_view_panel",
  "save_environment_profile", "set_agent_actions_enabled", "set_alert_threshold_override", "set_collector_databases", "set_collector_frequency", "set_collector_retention", "set_collector_run_at", "set_database_state_expected", "set_mute_rule_enabled",
  "set_notification_route_enabled", "set_notification_route_fallback", "set_quiet_window", "set_server_action", "set_server_excluded_databases",
  "submit_investigation_report", "test_connection", "test_custom_alert_rule", "test_query_tag_rule", "start_practice_request", "unassign_query_tag",
  "unassign_server_tag", "undo_agent_change", "undo_plan_change", "unforce_query_store_plan", "unmute_analysis_finding", "update_alert_settings", "update_custom_alert_rule",
  "update_custom_view", "update_known_pattern", "update_mute_rule", "update_notification_channels", "update_notification_route", "update_query_tag", "update_query_tag_rule",
  "update_server_tag", "update_service_flags", "validate_custom_alert_rule", "validate_custom_view", "wait_for_investigation_requests", "write_investigation_note",
];
const ACTION_TOOL_SET = new Set(MCP_ACTION_TOOL_NAMES);

/** The page's own word for the place a read shows, keyed by tool. A message that tells a client to call one of these reads
    "the Replication tab" on the page. Only reads a tab really shows are here (server-tab-reach.js lists them); every other
    tool name is not something the page has, so a sentence that still names one is dropped (withoutToolNames). */
const PAGE_FOR_TOOLS = [
  ["Collection Health", ["get_collection_health", "get_collection_log"]],
  ["the Blocking tab", ["get_blocking", "get_blocking_stats", "get_blocking_trend", "get_blocked_process_xml", "get_deadlocks", "get_deadlock_detail", "get_deadlock_trend", "get_lock_wait_trend"]],
  ["the Queries tab", ["get_active_queries", "get_query_store_top", "get_query_store_regressions", "get_top_queries_by_cpu", "get_top_procedures_by_cpu", "get_query_trend", "get_query_heatmap"]],
  ["the Wait Stats tab", ["get_wait_stats", "get_wait_trend", "get_waiting_tasks", "get_latch_stats", "get_spinlock_stats"]],
  ["the File I/O tab", ["get_file_io_trend", "get_tempdb_trend"]],
  ["the CPU tab", ["get_cpu_utilization"]],
  ["the Query Store Health panel", ["get_query_store_health"]],
  ["the Memory tab", ["get_memory_grants", "get_memory_trend", "get_resource_semaphore"]],
  ["the System Events tab", ["get_default_trace_events"]],
  ["the Replication tab", ["get_pg_replication_slots", "get_pg_replication_stats"]],
  ["the Vacuum tab", ["get_pg_autovacuum_health", "get_pg_session_states", "get_pg_wraparound_risk", "get_pg_xmin_horizon"]],
  ["the Waits tab", ["get_pg_wait_sampling", "get_pg_wait_stats", "get_pg_wait_trend", "get_pg_kernel_stats"]],
  ["the I/O tab", ["get_pg_io_stats", "get_pg_io_trend", "get_pg_buffer_usage", "get_pg_write_stats"]],
  ["the Activity tab", ["get_pg_blocking", "get_pg_deadlocks", "get_pg_database_stats", "get_pg_database_trend", "get_pg_top_queries", "get_pg_plans", "get_pg_plan_capture_readiness", "get_pg_log_events", "get_pg_lock_stats"]],
  ["the Storage tab", ["get_pg_table_bloat", "get_pg_index_bloat", "get_pg_index_usage"]],
  ["the Configuration tab", ["get_pg_server_config", "get_pg_server_config_changes"]],
];
const PAGE_FOR_TOOL = new Map(PAGE_FOR_TOOLS.flatMap(([page, tools]) => tools.map((t) => [t, page])));

/** The label=value measurement labels the collectors write (the CollectorMeasurement labels). A lone label=value pair is
    rewritten only when its label is one of these; WebPlainTextTests checks the list against the collectors' source. */
export const MEASUREMENT_LABELS = [
  "csv_records_discarded", "event_file_buffer_cut", "event_file_error", "event_file_events", "event_file_fallback", "event_file_wildcard",
  "events_read", "events_stored", "foreign_zone_lines_skipped", "forged_captures_skipped",
  "identity_epoch_changes", "job_history_identity_regressions", "job_history_identity_store_watermark",
  "job_history_identity_target_row", "json_records_discarded", "log_bytes_skipped", "log_files_skipped_by_rotation",
  "log_matches_limited", "log_resume_file_missing", "log_resume_file_recycled", "newest_visible_event_age_s",
  "postmaster_epoch_changes", "raise_shaped_deadlocks_skipped", "report_xml_empty", "report_xml_unparsed",
  "ring_buffer_events_hidden", "ring_buffer_truncated", "shred_gated", "statement_scrub_ms",
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

/* The rewrites that name one known internal token each (a hints pointer, a window parameter, a denial flag). They are the
   parameter names the service's own sentences write, never a field or table name: those can be real object names in a
   store error (pg_wraparound_stats, last_error), so they stay out of this list and are rewritten only by plainText. */
/* #6831: what each system_health event type the health parser reads is called on the page. */
const SYSTEM_HEALTH_EVENT_WORDS = {
  sp_server_diagnostics_component_result: "server diagnostics",
  scheduler_monitor_system_health_ring_buffer_recorded: "scheduler monitor",
  memory_broker_ring_buffer_recorded: "memory broker",
  memory_node_oom_ring_buffer_recorded: "memory node out-of-memory",
  error_reported: "error",
  wait_info: "wait",
};
/* The two rules in namedTokenText match exactly the names the table above holds, so a name is added in one place. */
const SYSTEM_HEALTH_EVENT_NAMES = Object.keys(SYSTEM_HEALTH_EVENT_WORDS).join("|");
const SYSTEM_HEALTH_EVENT_COUNTED = new RegExp("(?<![\\w.])(\\d[\\d,]*) (" + SYSTEM_HEALTH_EVENT_NAMES + ") event\\(s\\)( were)?", "g");
const SYSTEM_HEALTH_EVENT_BARE = new RegExp("\\b(" + SYSTEM_HEALTH_EVENT_NAMES + ")\\b", "g");

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
 * A collector's measurement counts put into words (shred_gated=1 events_read=0 -> "shred gated: 1, events read: 0"), and the
 * underscore form of the same counts. Only these two rules apply to text that may echo a real error message (a collection
 * log's Error cell holds a failed run's error and a successful run's note in the same column).
 */
function measurementCountsText(text) {
  let t = text;

  /* A collector's measurement counts: shred_gated=1 events_read=0 report_xml_empty=0. A run of two or more pairs, or a
     lone pair whose label a collector really writes; any other lone pair, such as timeout=30 in a driver message, stays. */
  t = t.replace(/(?<![\w.-])[a-z][a-z0-9]*(?:_[a-z0-9]+)*=-?\d+(?:\.\d+)?(?: [a-z][a-z0-9]*(?:_[a-z0-9]+)*=-?\d+(?:\.\d+)?)*(?![\w=])/g, (run) =>
    run.includes(" ") || LABEL_SET.has(run.slice(0, run.indexOf("="))) ? pairsToText(run) : run,
  );

  /* The same counts written with an underscore for the "=": a run of two or more name_digits words. A lone word such as
     pg_stat_statements_1 is left alone, so a real object name followed by a number is not rewritten. */
  t = t.replace(/(?<![\w.])[a-z][a-z0-9]*(?:_[a-z][a-z0-9]*)*_\d+(?: [a-z][a-z0-9]*(?:_[a-z][a-z0-9]*)*_\d+)+(?![\w])/g, underscoreCountsToText);
  return t;
}

/**
 * `text` with the named internal tokens and the measurement counts put into words, nothing else (#6649). For a collection
 * log's message cell: a successful run leaves its counts there, and a failed run leaves a real error message.
 */
export function plainTextCounts(text) {
  if (typeof text !== "string" || text === "") return text;
  return measurementCountsText(namedTokenText(text));
}

/** Whether a word is a tool the service registers, get_ or otherwise. */
function isToolName(word) {
  return TOOL_NAME_SET.has(word) || ACTION_TOOL_SET.has(word);
}
const TOOL_WORD = /(?<![\w.])[a-z][a-z0-9]*(?:_[a-z0-9]+)+(?![\w])/g;

/** Splits at ". ", "! " and "? " when a capital letter, a quote or a bracket follows. */
function sentencesOf(text) {
  return text.split(/(?<=[.!?])\s+(?=[A-Z"'(\[])/);
}

/** Where the first registered tool name sits in `s`, or -1. */
function firstToolAt(s) {
  for (const m of s.matchAll(TOOL_WORD)) if (isToolName(m[0])) return m.index;
  return -1;
}

/**
 * Tool names, said the way the page says them (#6156). A person on the page has no tools to call, so a name never reaches
 * them: a read a tab shows becomes that tab ("the Replication tab"), a collector becomes its display name ("the Trace Flags
 * collector"), and a sentence that still points at any other tool (analyze_server, a get_ read no tab shows) is dropped,
 * or only its trailing clause when the sentence says something before it ("...; audit_config reports those.").
 * If that would leave nothing, the names are said as words instead, so the strip is never blank.
 */
function withoutToolNames(text) {
  let t = text;

  /* A collector name followed by the word "collector": its display name is its words, title-cased, as the tab titles are. */
  t = t.replace(/(?<![\w.])((?:[Tt]he )?)([a-z][a-z0-9]*(?:_[a-z0-9]+)+) collector(?![\w])/g, (m, the, name) => the + toolWords(name) + " collector");
  t = t.replace(/\bcollector '([a-z][a-z0-9]*(?:_[a-z0-9]+)+)'/g, (m, name) => "collector '" + toolWords(name) + "'");

  /* A read a tab shows. Any other tool name stays here and is dropped below. */
  t = t.replace(/(?<![\w.])get_[a-z0-9]+(?:_[a-z0-9]+)*(?![\w])/g, (m) => PAGE_FOR_TOOL.get(m) ?? m);

  /* Two reads on one tab, named one after the other, are that tab once: "try the Queries tab or the Queries tab". */
  t = t.replace(/\b(the (?:[A-Z][\w/]*\s)+\btab)(?:, | or | and )\1/g, "$1");

  if (firstToolAt(t) < 0) return t;

  const kept = [];
  for (const sentence of sentencesOf(t)) {
    const at = firstToolAt(sentence);
    if (at < 0) {
      kept.push(sentence);
      continue;
    }
    /* The clause the name sits in starts at the last "; " or dash before it. What stands before that clause is still worth
       showing when it does not name a tool itself and is long enough to be a statement. */
    const before = sentence.slice(0, at);
    const cut = Math.max(before.lastIndexOf("; "), before.lastIndexOf(" — "), before.lastIndexOf(" – "), before.lastIndexOf(" - "));
    const head = cut > 0 ? before.slice(0, cut).trimEnd() : "";
    if (head.split(/\s+/).length >= 4 && firstToolAt(head) < 0) kept.push(head.replace(/[,;:]$/, "") + (/[.!?]$/.test(head) ? "" : "."));
  }
  const out = kept.join(" ").trim();
  if (out !== "") return out;
  return t.replace(TOOL_WORD, (m) => (isToolName(m) ? toolWords(m.replace(/^get_/, "")) : m));
}

/** Ordinary English words a message may write in capitals for emphasis ("DID execute", "NOT an all-clear"). Only these are
    lower-cased. An acronym (SQL, CPU, WAL, XID, TLS), a state or setting (READ_ONLY, FATAL), a wait type, a SQL keyword and
    a name the reader typed (a server, a database) are not here, so they are never touched; a word with an underscore or a
    digit in it is not even looked at. */
const EMPHASIS_WORDS = new Set((
  "NOT AND OR BUT NO ANY ALL ONLY ALWAYS EVER NEVER EVERY NOTHING ONE TWO THREE FOUR FIRST LATEST NEWEST OLDEST OLDER LAST NEXT SAME " +
  "OWN THIS THAT THESE THOSE WHOLE WITHOUT WITHIN BEFORE AFTER BOTH EITHER NEITHER OTHER ANOTHER STILL YET ALREADY ONCE THEN WHEN " +
  "WHICH WHAT WHY WHO HAS HAVE HAD WAS WERE ARE BE DID DOES CAN CANNOT COULD WOULD MAY MUST SHOULD WILL ALSO TOO MORE MOST LESS " +
  "FEWER WIDER NARROWER HIGHER LOWER WORSE WORST VERY ACTUALLY EXACTLY ITSELF ITS YOU YOUR OUR INSTEAD ABOVE BELOW BETWEEN INTO " +
  "UNDER PAST EACH MEASURED ESTIMATED SAMPLED SAMPLE SAMPLES SUPPOSED DAILY HOURLY CAUSAL DIFFERENCE DIFFERENCES DIFFERENT " +
  "SEPARATE RANK RANKED LINES REAL WRONG CORRECT NORMAL PARTIAL EXPECTED HERE ZERO SHORT BAD"
).split(" "));

/** Whether a token (a run without spaces) is all capitals, digits and underscores: ALTER, READ_ONLY, SQL. */
function isCapsToken(token) {
  return /^[A-Z][A-Z0-9_]+$/.test(token.replace(/^[^A-Za-z0-9_]+|[^A-Za-z0-9_]+$/g, ""));
}

/** A whole word of capitals that is an ordinary English word, said in lower case: "runs DID execute" -> "runs did execute".
    A word that opens a sentence keeps its first capital. A word that stands next to another all-capitals word is part of a
    statement or a permission name (ALTER ANY EVENT SESSION, IS NOT NULL) and is left as written. */
function lowerEmphasisWords(text) {
  return text.replace(/(?<![\w$.\/\\-])[A-Z]{2,}(?![\w$\/\\-])/g, (word, at) => {
    if (!EMPHASIS_WORDS.has(word)) return word;
    const prev = /(\S+) $/.exec(text.slice(0, at));
    const next = /^ (\S+)/.exec(text.slice(at + word.length));
    if ((prev && isCapsToken(prev[1])) || (next && isCapsToken(next[1]))) return word;
    const lower = word.toLowerCase();
    return at === 0 || /[.!?]\s+$/.test(text.slice(0, at)) ? lower[0].toUpperCase() + lower.slice(1) : lower;
  });
}

/**
 * `text` with the internal names a web reader cannot use put into words. Text with none of them comes back unchanged.
 * Not a string (null, a number) comes back as it is.
 */
export function plainText(text) {
  if (typeof text !== "string" || text === "") return text;
  if (text.includes(ENGINE_GATE_MARK)) return AZURE_SQL_DATABASE_MARK.test(text) ? AZURE_SQL_DATABASE_NOT_COLLECTED_LINE : NOT_COLLECTED_LINE;
  let t = text;
  if (text.includes(RUNTIME_PRECONDITION_MARK)) t = runtimePreconditionText(text);
  t = namedTokenText(t);

  /* The health sentence's collector and field names, said as the page's own words (#5492). Not in the named path: a Last
     Error can quote a real relation or column called the same thing. */
  t = t.replace(/\bpg_wraparound_stats\b/g, "the freeze-headroom collector");
  for (const [re, words] of FIELD_WORDS) t = t.replace(re, words);

  t = measurementCountsText(t);

  /* The index-bloat "answered only" read says it in the read's own parameter name. The page shows the sentence under the
     answered-ranking panel, so it says why that ranking is empty, in words, and points at the table above that lists each
     index with its reason (release walk, PG18 Storage). */
  t = t.replace(/answered_only excluded every row: /g, "Nothing to rank: ");
  t = t.replace(
    /the suppression breakdown below says why each index has no answer and which reasons are remediable\. Re-run without answered_only to see the rows and their reasons\./g,
    "the Index Bloat table above says why each index has no answer."
  );
  t = t.replace(/\banswered_only: true\b/g, "the answered-only ranking");
  t = t.replace(/\banswered_only\b/g, "the answered-only ranking");

  /* #5644 W8: clauses that tell an MCP client which parameter to pass. The page has no such control, so a clause that points
     at one is dropped (the sentence before it still stands) and a filter name is said as the page names it. hours_back and
     days_back are already put into words by namedTokenText, which runs first. */
  /* A trend's width note: "...near 1500 points; pass bucket_minutes (1-1440) for another width, or narrow hours_back for finer
     points." The page picks the width, so only the narrower range is left to say. */
  t = t.replace(/pass bucket_minutes \(1-\d+\) for another width, or /g, "");
  /* #5644 W8: a trend note's closing advice, and the field names it explains. The CPU and Memory pages have no bucket control (the
     page picks the width), so "narrow the bucket" is said as the control that exists, the time range; the Waits, Latch/Spinlock
     and Activity notes name their fields by the column or tile the page shows. */
  t = t.replace(/\bnarrow the bucket to see it\b/g, "narrow the time range to see it");
  t = t.replace(/\bwait_time_ms_per_second is recomputed\b/g, "Wait ms/s is recomputed");
  t = t.replace(/\bcollisions_per_second is recomputed\b/g, "Coll/s is recomputed");
  t = t.replace(
    /\btop_application_name, top_application_connections, top_host_name and top_host_connections are\b/g,
    "the top application, its connection count, the top host and its connection count are",
  );
  /* #6831: the system_health event names ("1431 scheduler_monitor_system_health_ring_buffer_recorded event(s) were captured") in words, with
     the count's own plural. The same words say the name inside "No ... events were captured". */
  t = t.replace(
    SYSTEM_HEALTH_EVENT_COUNTED,
    (m, n, type, were) => n + " " + SYSTEM_HEALTH_EVENT_WORDS[type] + (n === "1" ? " event" + (were ? " was" : "") : " events" + (were || "")),
  );
  t = t.replace(SYSTEM_HEALTH_EVENT_BARE, (m, type) => SYSTEM_HEALTH_EVENT_WORDS[type]);
  /* #6831: the empty cut-window sentence's pointer at hints.effective_start, when the page has no instant to put there (util.js names it when it has one). */
  t = t.replace(/\bstarts at effective_start \(see hints\)/g, "starts partway into the window");
  /* "24 hour(s)", "288 collector run(s)": the count is known, so the word agrees with it ("1 hour", "24 hours", "2 queries"). */
  t = t.replace(/(?<![\w.])(\d[\d,]*) ((?:[a-z]+ )?[a-z]+)\(s\)/g, (m, n, word) => n + " " + (n === "1" ? word : /[^aeiou]y$/.test(word) ? word.slice(0, -1) + "ies" : word + "s"));

  /* "raise limit": the pages have no row-limit control. "...filter - raise limit to see more." and "...returned; raise limit
     (now 50) for more." lose the clause; ", or raise limit," and ", and raise limit if none ... shows one" lose theirs. */
  t = t.replace(/\s*[;—–-]+\s*raise (?:the )?limit(?: \(now [\d,]+\))?(?: (?:to see|for) (?:more|the rest(?: of the rows)?))?(?=\.)/g, "");
  t = t.replace(/Scope by server_name(?:, or raise limit,)? (to see the rest|to see them)\./g, "Select a single server $1.");
  t = t.replace(/, or raise (?:the )?limit(?=[.,])/g, "");
  t = t.replace(/, and raise limit if none of the databases returned shows one/g, "");
  t = t.replace(/\bpass database_name to ask about one\b/g, "filter by database to ask about one");
  t = t.replace(/\bpass database_name to chart one database per file\b/g, "choose a database to chart its files");
  t = t.replace(/\bomit database_name for every database\b/g, "clear the database filter for every database");
  /* The filters a "matched ..." sentence names: "matched database_name 'x' with blocking_only with wait_type 'y'". */
  t = t.replace(/\bdatabase_name '/g, "database '");
  t = t.replace(/\bwait_type '/g, "wait type '");
  t = t.replace(/\bcollector_name '/g, "collector '");
  t = t.replace(/\bmin_duration_ms (?=\d)/g, "minimum duration (ms) ");
  t = t.replace(/\bblocking_only\b/g, "blocking queries only");
  /* #6649: field names inside the service's own sentences. The left-out databases' names live in the MCP answer's
     `databases_left_out` list, which the page never shows, so the pointer goes and the count before it stays. */
  t = t.replace(/ \(see databases_left_out\)/g, "");
  t = t.replace(
    /Here the window starts at baseline_start, so the baseline holds only the part from effective_start to baseline_end\./g,
    "Here the window starts before the baseline period, so the baseline holds only the part the store has.",
  );
  /* #6750: the empty-baseline sentence and the cut-recent-window sentences that follow it. */
  t = t.replace(
    /Here effective_start is at or after baseline_end, so the baseline holds no hours: timeouts and errors are flagged on recent runs alone, and a cost flag can come only from an interval that ran before baseline_end and was collected after it\./g,
    "Here the store starts at or after the end of the baseline period, so the baseline holds no hours: timeouts and errors are flagged on recent runs alone, and a cost flag can come only from a run before the baseline period ended that was collected later.",
  );
  t = t.replace(
    /effective_start is also after baseline_end, so the recent window holds only the part from effective_start\./g,
    "The store also starts after the recent window began, so the recent window holds only the part the store has.",
  );
  /* The memory clerk note has four arms (memory_clerk.note): two name latest_memory_mb, the Queries tab's "Latest MB" column,
     and two name the collector's own relation, memory_clerks. */
  t = t.replace(/\blatest_memory_mb\b/g, "Latest MB");
  t = t.replace(/\bmemory_clerks capture\b/g, "memory clerk capture");
  t = t.replace(/\bmemory_clerks stores\b/g, "The memory clerks collector stores");
  /* The system-health "events were captured, none qualified" closing. */
  t = t.replace(/so this is the healthy answer for this read rather than missing data/g, "so this is a normal result, not missing data");

  /* A tool name is not something the page has (#6156): a read a tab shows becomes that tab, any other sentence that points at
     a tool is dropped. Only a registered tool name counts; any other get_ word is the reader's own text (an object, a server)
     and stays as written. Capitals used for emphasis come last, once the tool sentences are gone. */
  t = withoutToolNames(t);
  t = lowerEmphasisWords(t);

  return t;
}

/** The words every runtime-precondition answer opens its closing sentence with (CollectorRuntimePrecondition.cs: the
    PreconditionEpilogue and ConnectScopedEpilogue constants). */
const RUNTIME_PRECONDITION_MARK = "This is a runtime PRECONDITION";

/** How CollectorRuntimePrecondition.GatedOffMessage opens its "never ran" answer. */
const NEVER_RAN_OPENING = /^The ([a-z][a-z0-9]*(?:_[a-z0-9]+)*) collector has never run against .+?, while the server itself is collecting normally/s;

/** How CollectorRuntimePrecondition.GatedOffMessage opens its "no longer invoked" answer, with the number of hours it names. */
const GONE_DARK_OPENING = /^The ([a-z][a-z0-9]*(?:_[a-z0-9]+)*) collector is no longer being invoked against .+? by more than (\d+) hours/s;

/** What GatedOffMessage puts between its fixed sentences and the closing sentence: the text its caller passes for what could switch the collector off. */
const SWITCHED_OFF_CAUSES = /(?:never|no longer) permitted to look\.\s*(.*?)\s*This is a runtime PRECONDITION/s;

/** What the two "the collector is not being invoked" answers say in their closing sentence that a reader can act on: the fact
    that gates them is read once when the service connects (ConnectScopedEpilogue), so fixing a cause is not enough by itself. */
const RECONNECT_STEP = " After you fix the cause, the service must reconnect to this server before collection starts again. Restart the service, or change this server's settings.";

/**
 * A runtime-precondition answer (CollectorRuntimePrecondition.cs) the way the page shows it. The server's text is written for
 * an MCP client: it labels itself a runtime PRECONDITION and says to retry, which a reader of the page cannot use. Without
 * that closing sentence the rest is kept as written, because it names the statement, grant or step that satisfies it.
 * The two "the collector is not being invoked" answers (never ran, gone dark) are longer than the page needs: each becomes a
 * line saying which data is missing, followed by the possible causes the server names, as written, and the reconnect step the server's closing sentence carries.
 */
function runtimePreconditionText(text) {
  const causes = SWITCHED_OFF_CAUSES.exec(text);
  const tail = (causes && causes[1] ? " " + causes[1] : "") + RECONNECT_STEP;
  const never = NEVER_RAN_OPENING.exec(text);
  if (never) return "The " + toolWords(never[1]) + " collector has never run for this server, so there is nothing to show." + tail;
  const dark = GONE_DARK_OPENING.exec(text);
  if (dark) return "The " + toolWords(dark[1]) + " collector has not run for this server for over " + dark[2] + " hours, so there is nothing current to show." + tail;
  return text.slice(0, text.indexOf(RUNTIME_PRECONDITION_MARK)).trimEnd();
}

/** The one short line the page shows where the server says a collector can never run on this kind of server. The
    server's own sentence names the collector, the engine gate and the way it is decided, none of which helps a reader. */
export const NOT_COLLECTED_LINE = "This server does not collect this data (it does not apply to this kind of server).";

/** The line for the same answer when the server's engine is Azure SQL Database (#6178): it names the engine, as the Memory tab does. */
export const AZURE_SQL_DATABASE_NOT_COLLECTED_LINE = "Azure SQL Database does not have this data.";

/** How the server's engine-gate sentence names an Azure SQL Database ("<server> runs on Azure SQL Database (EngineEdition 5)."). */
const AZURE_SQL_DATABASE_MARK = /runs on Azure SQL Database \(EngineEdition 5\)/;

/** The words every engine-gate sentence ends on (CollectorEngineCapability's permanent-gap epilogue). A "not collected"
    answer that is something else, such as a plan that was never stored, does not carry them and keeps its own sentence.
    The mark opens with "This is a" because a runtime-precondition sentence (an extension that is not installed, a skipped
    source) says "not a permanent engine capability gap": matching the bare phrase turned that fixable state into "does not
    apply to this kind of server" (release walk, PG18 OS CPU by Query). */
const ENGINE_GATE_MARK = "This is a permanent engine capability gap";
