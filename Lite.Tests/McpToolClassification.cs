/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitorLite.Mcp;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The explicit read-only list for every Lite MCP tool. A tool is either here or in
/// <see cref="McpWritePolicy.WriteTools"/>; the allowlist test in the write-policy tests lists every registered tool and fails
/// on one that is neither, so a new tool (whatever its name begins with) fails until someone decides which it is and
/// adds it to the right set. Add a name here only for a tool that changes nothing in the monitoring store's
/// configuration: <c>analyze_server</c> persists the findings it derives but is a read.
/// </summary>
internal static class McpToolClassification
{
    internal static readonly IReadOnlySet<string> ReadOnlyTools = new HashSet<string>(StringComparer.Ordinal)
    {
        "analyze_plan_xml", "analyze_procedure_plan", "analyze_query_plan", "analyze_query_store_plan",
        "analyze_server", "audit_config", "compare_analysis", "find_database", "get_active_queries", "get_alert_history",
        "get_alert_settings", "get_analysis_facts", "get_analysis_findings", "get_blocked_process_reports",
        "get_blocked_process_xml", "get_blocking_stats", "get_blocking_trend", "get_client_timeouts", "get_collection_health",
        "get_collection_log", "get_cpu_by_database", "get_cpu_scheduler_pressure", "get_cpu_utilization", "get_current_waits_trend",
        "get_daily_summary", "get_daily_summary_range", "get_database_config", "get_database_config_changes",
        "get_database_scoped_config", "get_database_sizes", "get_deadlock_detail", "get_deadlock_trend",
        "get_deadlocks", "get_default_trace_events", "get_file_io_stats", "get_file_io_trend",
        "get_health_parser_cpu_tasks", "get_health_parser_io_issues", "get_health_parser_memory_broker",
        "get_health_parser_memory_conditions", "get_health_parser_memory_node_oom",
        "get_health_parser_scheduler_issues", "get_health_parser_severe_errors",
        "get_health_parser_significant_waits", "get_health_parser_system_health", "get_index_usage",
        "get_investigation_reports", "get_latch_stats", "get_load_by_caller", "get_lock_wait_trend", "get_long_query_completions", "get_memory_clerks",
        "get_memory_grants", "get_memory_pressure_events", "get_memory_stats", "get_memory_trend", "get_mute_rules",
        "get_notification_routes", "get_object_locking", "get_perfmon_stats", "get_perfmon_trend",
        "get_plan_cache_bloat", "get_plan_corrections", "get_plan_xml", "get_procedure_duration_trend",
        "get_pvs_stats", "get_query_duration_trend", "get_query_heatmap", "get_query_pileups", "get_query_store_duration_trend",
        "get_query_store_health", "get_query_store_regressions", "get_query_store_top", "get_query_trend",
        "get_resource_semaphore", "get_resource_use_by_tag", "get_running_jobs", "get_server_config", "get_server_config_changes",
        "get_server_properties", "get_server_summary", "get_session_stats", "get_spinlock_stats",
        "get_table_index_sizes", "get_tempdb_trend", "get_tool_guide", "get_top_procedures_by_cpu",
        "get_top_queries_by_cpu", "get_trace_flag_changes", "get_trace_flags", "get_wait_stats", "get_wait_trend",
        "get_wait_types", "get_waiting_tasks", "list_investigation_requests", "list_query_tag_rules", "list_query_tags", "list_servers",
        "test_query_tag_rule",
    };
}
