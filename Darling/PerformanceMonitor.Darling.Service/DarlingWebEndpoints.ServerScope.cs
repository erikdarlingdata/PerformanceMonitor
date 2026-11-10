/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingWebEndpoints
{
    // Each admitted reader resolves its server through DarlingServerResolver and queries by that ID.
    // New readers must be checked before being added here.
    internal static readonly IReadOnlySet<string> ScopedServerReadPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "/api/read/audit_config",
        "/api/read/compare_analysis",
        "/api/read/get_analysis_facts",
        "/api/read/get_analysis_findings",
        "/api/read/get_active_queries",
        "/api/read/get_session_stats",
        "/api/read/get_waiting_tasks",
        "/api/read/get_blocked_process_xml",
        "/api/read/get_blocking",
        "/api/read/get_block_chain",
        "/api/read/get_blocking_trend",
        "/api/read/get_deadlock_detail",
        "/api/read/get_deadlock_trend",
        "/api/read/get_deadlocks",
        "/api/read/get_lock_wait_trend",
        "/api/read/get_plan_corrections",
        "/api/read/get_database_config",
        "/api/read/get_server_config",
        "/api/read/get_trace_flags",
        "/api/read/get_database_protection",
        "/api/read/get_identity_capacity",
        "/api/read/get_database_config_changes",
        "/api/read/get_database_scoped_config",
        "/api/read/get_query_store_health",
        "/api/read/get_server_config_changes",
        "/api/read/get_trace_flag_changes",
        "/api/read/get_collection_health",
        "/api/read/get_collection_log",
        "/api/read/get_current_waits_trend",
        "/api/read/get_client_timeouts",
        "/api/read/get_blocking_stats",
        "/api/read/get_cpu_utilization",
        "/api/read/get_file_io_stats",
        "/api/read/get_memory_clerks",
        "/api/read/get_memory_stats",
        "/api/read/get_perfmon_stats",
        "/api/read/get_query_heatmap",
        "/api/read/get_query_store_regressions",
        "/api/read/get_query_store_clutter",
        "/api/read/get_resource_use_by_tag",
        "/api/read/get_query_store_query_history",
        "/api/read/get_forced_plans",
        "/api/read/get_query_store_hints",
        "/api/read/get_plan_force_precheck",
        "/api/read/get_query_store_aborts",
        "/api/read/get_cpu_by_database",
        "/api/read/get_load_by_caller",
        "/api/read/get_query_pileups",
        "/api/read/get_query_store_top",
        "/api/read/get_long_query_completions",
        "/api/read/get_server_properties",
        "/api/read/get_tempdb_trend",
        "/api/read/get_top_procedures_by_cpu",
        "/api/read/get_top_queries_by_cpu",
        "/api/read/get_pg_top_queries",
        "/api/read/get_pg_plans",
        "/api/read/get_pg_plan_capture_readiness",
        "/api/read/get_pg_logging_audit",
        "/api/read/get_pg_wraparound_risk",
        "/api/read/get_pg_xmin_horizon",
        "/api/read/get_pg_replication_slots",
        "/api/read/get_pg_autovacuum_health",
        "/api/read/get_pg_io_stats",
        "/api/read/get_pg_wait_stats",
        "/api/read/get_pg_cpu_utilization",
        "/api/read/get_pg_wait_sampling",
        "/api/read/get_pg_kernel_stats",
        "/api/read/get_pg_predicate_stats",
        "/api/read/get_pg_index_bloat",
        "/api/read/get_pg_column_stats",
        "/api/read/get_pg_buffer_usage",
        "/api/read/get_pg_extensions",
        "/api/read/get_pg_lock_stats",
        "/api/read/get_pg_write_stats",
        "/api/read/get_pg_server_config",
        "/api/read/get_pg_server_config_changes",
        "/api/read/get_pg_deadlocks",
        "/api/read/get_pg_deadlock_detail",
        "/api/read/get_pg_log_events",
        "/api/read/get_pg_wait_trend",
        "/api/read/get_pg_query_duration_trend",
        "/api/read/get_pg_io_trend",
        "/api/read/get_pg_database_trend",
        "/api/read/get_pg_replication_stats",
        "/api/read/get_pg_blocking",
        "/api/read/get_pg_database_stats",
        "/api/read/get_pg_index_usage",
        "/api/read/get_pg_table_bloat",
        "/api/read/get_pg_session_states",
        "/api/read/get_wait_stats",
        "/api/read/get_wait_trend",
        "/api/read/get_wait_types",
        "/api/read/get_file_io_trend",
        "/api/read/get_memory_trend",
        "/api/read/get_server_trend",
        "/api/read/get_perfmon_trend",
        "/api/read/get_procedure_duration_trend",
        "/api/read/get_query_duration_trend",
        "/api/read/get_query_store_duration_trend",
        "/api/read/get_query_trend",
        "/api/read/get_server_summary",
        "/api/read/get_overview_baselines",
        "/api/read/get_daily_summary",
        "/api/read/get_daily_summary_range",
        "/api/read/get_collector_stall_probes",
        "/api/read/get_oversized_plan_backlog",
        "/api/read/get_latch_stats",
        "/api/read/get_spinlock_stats",
        "/api/read/get_memory_grants",
        "/api/read/get_memory_pressure_events",
        "/api/read/get_resource_semaphore",
        "/api/read/get_database_sizes",
        "/api/read/get_pvs_stats",
        "/api/read/get_index_usage",
        "/api/read/get_object_locking",
        "/api/read/get_table_index_sizes",
        "/api/read/get_cpu_scheduler_pressure",
        "/api/read/get_plan_cache_bloat",
        "/api/read/get_running_jobs",
        "/api/read/get_agent_alerts",
        "/api/read/get_plan_xml",
        "/api/read/get_query_store_plan_xml",
        "/api/read/get_procedure_plan_xml",
        "/api/read/get_active_query_plan_xml",
        "/api/read/get_query_repro_script",
        "/api/read/get_blocking_plan_xml",
        "/api/read/get_deadlock_plan_xml",
        "/api/read/get_default_trace_events",
        "/api/read/get_health_parser_cpu_tasks",
        "/api/read/get_health_parser_io_issues",
        "/api/read/get_health_parser_memory_broker",
        "/api/read/get_health_parser_memory_conditions",
        "/api/read/get_health_parser_memory_node_oom",
        "/api/read/get_health_parser_scheduler_issues",
        "/api/read/get_health_parser_severe_errors",
        "/api/read/get_health_parser_significant_waits",
        "/api/read/get_health_parser_system_health",
        "/api/read/get_change_watches",
        "/api/read/get_finops_recommendations",
        "/api/read/get_finops",
        "/api/read/get_alert_details",
        "/api/server-databases",
        "/api/server-data-start",
    };

    internal static bool IsScopedFleetRead(string name) =>
        name is "list_servers" or "get_alert_history" or "get_fleet_overview" or "get_query_fleet_summary"
            or "get_finops_inventory" or "list_investigation_requests" or "get_investigation_reports" or "get_sweep_reports"
            or "list_query_tags" or "get_job_history";

    internal static bool SupportsScopedRead(PathString path) =>
        SupportsScopedServerRead(path) ||
        (path.Value is { } value && value.StartsWith("/api/read/", StringComparison.OrdinalIgnoreCase) &&
         IsScopedFleetRead(value["/api/read/".Length..].ToLowerInvariant())) ||
        path.Equals("/api/ping", StringComparison.OrdinalIgnoreCase) ||
        path.Equals("/api/catalog", StringComparison.OrdinalIgnoreCase) ||
        path.Equals("/api/agent-changes", StringComparison.OrdinalIgnoreCase) ||
        path.Equals("/api/compose/run", StringComparison.OrdinalIgnoreCase) ||
        path.StartsWithSegments("/api/sweeps", StringComparison.OrdinalIgnoreCase);

    internal static bool SupportsScopedServerRead(PathString path) =>
        path.Value is { } value && ScopedServerReadPaths.Contains(value);


    internal static async Task<int[]> AllowedWebServerIdsAsync(
        NpgsqlDataSource postgres, HttpContext context, System.Threading.CancellationToken cancellationToken)
    {
        var seat = PerformanceMonitor.Darling.Service.Hosting.DarlingWebSeat.FromContext(context);
        return await AllowedWebServerIdsAsync(postgres, seat.AllowedTagIds ?? Array.Empty<int>(), cancellationToken);
    }

    internal static async Task<int[]> AllowedWebServerIdsAsync(
        NpgsqlDataSource postgres, IReadOnlyList<int> allowedTagIds, System.Threading.CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand("""
            WITH RECURSIVE allowed_tags(id) AS (
                SELECT id FROM config.server_tags WHERE id = ANY($1)
                UNION
                SELECT child.id FROM config.server_tags child
                JOIN allowed_tags parent ON child.parent_id = parent.id
            )
            SELECT DISTINCT assignment.server_id FROM config.server_tag_map assignment
            JOIN allowed_tags tag ON tag.id = assignment.tag_id
            JOIN collect.servers server ON server.server_id = assignment.server_id
            """);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer,
            System.Linq.Enumerable.ToArray(allowedTagIds));
        var ids = new List<int>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ids.Add(reader.GetInt32(0));
        }

        return ids.ToArray();
    }

    internal static async Task<int?> AuthorizedWebServerAsync(
        NpgsqlDataSource postgres, HttpContext context, IReadOnlyList<int> allowedTagIds, int[]? allowedServerIds = null)
    {
        var query = context.Request.Query;
        // Refuse ambiguous or missing selectors instead of letting the tool choose a server.
        if (context.Request.Path.Equals("/api/read/get_alert_details", StringComparison.OrdinalIgnoreCase))
        {
            if (query["server_id"].Count != 1 || query.ContainsKey("server") || query.ContainsKey("server_name") ||
                !int.TryParse(query["server_id"], out var id))
            {
                return null;
            }

            var ids = allowedServerIds ?? await AllowedWebServerIdsAsync(postgres, allowedTagIds, context.RequestAborted);
            return Array.IndexOf(ids, id) >= 0 ? id : null;
        }

        if (query.ContainsKey("server_id") ||
            query["server"].Count > 1 || query["server_name"].Count > 1 ||
            (query.ContainsKey("server") && query.ContainsKey("server_name")) ||
            string.IsNullOrWhiteSpace(Server(context)) ||
            string.Equals(Server(context)?.Trim(), "*", StringComparison.Ordinal) ||
            DarlingServerResolver.IsFleetSentinelName(Server(context)))
        {
            return null;
        }

        var serverIds = allowedServerIds ?? await AllowedWebServerIdsAsync(postgres, allowedTagIds, context.RequestAborted);
        var (resolved, error) = await DarlingServerResolver.ReadWebScopeAsync(serverIds, allowedTagIds,
            () => DarlingServerResolver.ResolveOrErrorAsync(postgres, Server(context), context.RequestAborted));
        if (error is not null)
        {
            return null;
        }

        /* From here the request names the server by its canonical server_name, so a read's own resolution is an exact match inside
           the grant and the resolver's pinned id is a second check rather than the only one. */
        var canonical = new List<KeyValuePair<string, string?>>();
        foreach (var pair in query)
        {
            if (string.Equals(pair.Key, "server", StringComparison.OrdinalIgnoreCase) || string.Equals(pair.Key, "server_name", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            foreach (var value in pair.Value)
            {
                canonical.Add(new KeyValuePair<string, string?>(pair.Key, value));
            }
        }

        canonical.Add(new KeyValuePair<string, string?>("server", resolved.ServerName));
        context.Request.QueryString = QueryString.Create(canonical);
        return resolved.ServerId;
    }
}
