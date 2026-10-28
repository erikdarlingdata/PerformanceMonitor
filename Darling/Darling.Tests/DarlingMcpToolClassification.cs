/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Service.Mcp;

namespace Darling.Tests;

/// <summary>
/// The explicit read-only list for every Darling MCP tool. A tool is either here or in
/// <see cref="DarlingMcpWritePolicy.WriteTools"/>; the allowlist test in the write-policy tests lists every registered tool and fails
/// on one that is neither, so a new tool (whatever its name begins with) fails until someone decides which it is and
/// adds it to the right set. Add a name here only for a tool that changes nothing in the monitoring store's
/// configuration: <c>analyze_server</c> persists the findings it derives but is a read.
/// </summary>
internal static class DarlingMcpToolClassification
{
    internal static readonly IReadOnlySet<string> ReadOnlyTools = new HashSet<string>(StringComparer.Ordinal)
    {
        "analyze_plan_xml", "analyze_procedure_plan", "analyze_query_plan", "analyze_query_store_plan",
        "analyze_server", "audit_config", "backtest_custom_alert_rule", "compare_analysis", "describe_custom_view_catalog", "get_active_queries",
        "get_active_query_plan_xml", "get_ag_health", "get_alert_history", "get_alert_settings",
        "get_analysis_facts", "get_analysis_findings", "get_blocked_process_xml", "get_blocking",
        "find_database", "get_blocking_plan_xml", "get_blocking_stats", "get_blocking_trend", "get_change_watches", "get_known_patterns", "get_client_timeouts", "get_collection_health",
        "get_collection_log", "get_collection_state", "get_collector_cost", "get_collector_schedules", "get_server_actions", "get_server_settings", "get_command_result", "get_agent_changes", "get_collector_stall_probes", "get_cpu_by_database", "get_cpu_scheduler_pressure",
        "get_cpu_utilization", "get_current_waits_trend", "get_custom_alert_rule", "get_custom_view",
        "get_daily_summary", "get_daily_summary_range", "get_database_config", "get_database_config_changes",
        "get_database_scoped_config", "get_database_state_expectations", "get_database_sizes", "get_deadlock_detail", "get_deadlock_plan_xml",
        "get_deadlock_trend", "get_deadlocks", "get_default_trace_events", "get_file_io_stats", "get_file_io_trend",
        "get_finops", "get_finops_inventory", "get_finops_recommendations", "get_fleet_overview", "get_forced_plans", "get_identity_capacity", "get_query_store_hints",
        "get_health_parser_cpu_tasks", "get_health_parser_io_issues", "get_health_parser_memory_broker",
        "get_health_parser_memory_conditions", "get_health_parser_memory_node_oom",
        "get_health_parser_scheduler_issues", "get_health_parser_severe_errors",
        "get_health_parser_significant_waits", "get_health_parser_system_health", "get_index_usage",
        "get_compiles_after_evict", "get_investigation_evidence", "get_investigation_reports", "get_job_history", "get_latch_stats", "get_load_by_caller", "get_lock_wait_trend", "get_long_query_completions",
        "get_memory_clerks", "get_memory_grants", "get_memory_pressure_events", "get_memory_stats",
        "get_memory_trend", "get_mute_rules", "get_notification_routes", "get_object_locking",
        "get_oversized_plan_backlog", "get_perfmon_stats", "get_perfmon_trend", "get_pg_autovacuum_health",
        "get_pg_blocking", "get_pg_buffer_usage", "get_pg_column_stats", "get_pg_cpu_utilization",
        "get_pg_database_stats", "get_pg_database_trend", "get_pg_deadlock_detail", "get_pg_deadlocks",
        "get_pg_extensions", "get_pg_index_bloat", "get_pg_index_usage", "get_pg_io_stats", "get_pg_io_trend",
        "get_pg_kernel_stats", "get_pg_lock_stats", "get_pg_log_events", "get_pg_logging_audit",
        "get_pg_plan_capture_readiness", "get_pg_plans", "get_pg_predicate_stats", "get_pg_query_duration_trend",
        "get_pg_replication_slots", "get_pg_replication_stats", "get_pg_server_config",
        "get_pg_server_config_changes", "get_pg_session_states", "get_pg_table_bloat", "get_pg_top_queries",
        "get_pg_wait_sampling", "get_pg_wait_stats", "get_pg_wait_trend", "get_pg_wraparound_risk",
        "get_pg_write_stats", "get_pg_xmin_horizon", "get_plan_cache_bloat", "get_plan_corrections", "get_plan_force_precheck", "get_plan_xml",
        "get_procedure_duration_trend", "get_procedure_plan_xml", "get_pvs_stats", "get_query_duration_trend",
        "get_query_heatmap", "get_query_pileups", "get_query_repro_script", "get_query_store_aborts", "get_query_store_clutter", "get_query_store_duration_trend",
        "get_query_store_health", "get_query_store_plan_xml", "get_query_store_query_history",
        "get_query_store_regressions", "get_query_store_top", "get_query_trend", "get_read_latency",
        "get_resource_semaphore", "get_resource_use_by_tag", "get_running_jobs", "get_server_config", "get_server_config_changes",
        "get_server_properties", "get_server_summary", "get_server_trend", "get_session_stats", "get_slow_reads",
        "get_spinlock_stats", "get_store_host", "get_store_log", "get_store_metrics", "get_store_query_history",
        "get_store_query_stats", "get_sweep_reports", "get_table_index_sizes", "get_tempdb_trend", "get_tool_guide",
        "get_top_procedures_by_cpu", "get_top_queries_by_cpu", "get_trace_flag_changes", "get_trace_flags",
        "get_wait_stats", "get_wait_trend", "get_wait_types", "get_waiting_tasks", "list_custom_alert_rules",
        "list_custom_alert_templates", "list_custom_views", "list_investigation_requests", "list_query_tag_rules", "list_query_tags", "list_servers", "run_custom_view_panel",
        "test_query_tag_rule",
        "test_custom_alert_rule", "validate_custom_alert_rule", "validate_custom_view", "wait_for_investigation_requests",
    };
}
