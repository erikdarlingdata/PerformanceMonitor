/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The session diagnostic-depth MCP tools — get_session_stats, get_active_queries, get_waiting_tasks —
/// served over Darling's Postgres store. Each tool body mirrors LITE's <c>McpSessionTools</c> /
/// <c>McpWaitTools</c> field-for-field (the store-faithful shape Darling's collector-mirror schema can
/// serve), keeping the tool names + parameter contracts. Reads flow through
/// <see cref="DarlingSessionReader"/> — STORED reads (no live monitored-server hit).
///
/// <para>
/// get_session_stats follows LITE's per-application shape (the store's <c>session_stats</c> table), not the
/// Dashboard's server-wide <c>session_summary_stats</c> summary — the two tools diverge and slices 1+2
/// follow Lite. get_waiting_tasks is a Lite-only tool (the Dashboard has no equivalent); it surfaces the
/// always-NULL <c>resource_description</c> exactly as Lite does.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpSessionTools
{
    [McpServerTool(Name = "get_session_stats"), Description("Gets connection and session statistics grouped by application. Shows connection counts, running/sleeping/dormant breakdown, and aggregate resource usage per application.")]
    public static async Task<string> GetSessionStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingSessionReader.GetLatestSessionStatsAsync(postgres, resolved.ServerId);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "session_stats")
                    ?? McpHelpers.Status("unavailable", "No session statistics available. The session collector may not have run yet.");

            var totalConnections = rows.Sum(r => r.ConnectionCount);
            var totalRunning = rows.Sum(r => r.RunningCount);
            var totalSleeping = rows.Sum(r => r.SleepingCount);
            var totalDormant = rows.Sum(r => r.DormantCount);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                collection_time = rows[0].CollectionTime.ToString("o"),
                summary = new
                {
                    total_connections = totalConnections,
                    total_running = totalRunning,
                    total_sleeping = totalSleeping,
                    total_dormant = totalDormant,
                    distinct_applications = rows.Count
                },
                applications = rows.Select(r => new
                {
                    program_name = r.ProgramName,
                    connections = r.ConnectionCount,
                    running = r.RunningCount,
                    sleeping = r.SleepingCount,
                    dormant = r.DormantCount,
                    total_cpu_time_ms = r.TotalCpuTimeMs,
                    total_logical_reads = r.TotalLogicalReads
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_session_stats", ex);
        }
    }

    [McpServerTool(Name = "get_active_queries"), Description("Gets active query snapshots captured from sys.dm_exec_requests. Shows what queries were running at each collection point: session ID, query text, wait type, CPU time, elapsed time, blocking info, DOP, and memory grants. Use hours_back to look at a specific time window — critical for finding what was running during a CPU spike or blocking event.")]
    public static async Task<string> GetActiveQueries(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to retrieve. Default 1.")] int hours_back = 1,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description("Show only queries involved in blocking (blocking_session_id > 0 or is a head blocker).")] bool blocking_only = false,
        [Description("Maximum number of rows to return. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingSessionReader.GetActiveQueriesAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_snapshots")
                    ?? McpHelpers.Status("empty", "No active query snapshots found in the requested time range.");

            IEnumerable<DarlingSessionReader.ActiveQueryRow> filtered = rows;

            if (!string.IsNullOrEmpty(database_name))
                filtered = filtered.Where(r => (r.DatabaseName ?? "").Equals(database_name, StringComparison.OrdinalIgnoreCase));

            if (blocking_only)
                filtered = filtered.Where(r => r.BlockingSessionId > 0
                    || rows.Any(other => other.BlockingSessionId == r.SessionId));

            var result = filtered.Take(limit).Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                session_id = r.SessionId,
                database_name = r.DatabaseName,
                status = r.Status,
                cpu_time_ms = r.CpuTimeMs,
                elapsed_time_ms = r.TotalElapsedTimeMs,
                elapsed_time_formatted = r.ElapsedTimeFormatted,
                logical_reads = r.LogicalReads,
                reads = r.Reads,
                writes = r.Writes,
                wait_type = string.IsNullOrEmpty(r.WaitType) ? null : r.WaitType,
                wait_time_ms = r.WaitTimeMs > 0 ? r.WaitTimeMs : (long?)null,
                blocking_session_id = r.BlockingSessionId > 0 ? r.BlockingSessionId : (int?)null,
                dop = r.Dop > 0 ? r.Dop : (int?)null,
                parallel_worker_count = r.ParallelWorkerCount > 0 ? r.ParallelWorkerCount : (int?)null,
                granted_query_memory_gb = r.GrantedQueryMemoryGb > 0 ? r.GrantedQueryMemoryGb : (double?)null,
                transaction_isolation_level = string.IsNullOrEmpty(r.TransactionIsolationLevel) ? null : r.TransactionIsolationLevel,
                open_transaction_count = r.OpenTransactionCount > 0 ? r.OpenTransactionCount : (int?)null,
                login_name = r.LoginName,
                host_name = r.HostName,
                program_name = r.ProgramName,
                query_text = McpHelpers.Truncate(r.QueryText, 2000)
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                total_snapshots = rows.Count,
                shown = result.Count,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_active_queries", ex);
        }
    }

    [McpServerTool(Name = "get_waiting_tasks"), Description("Gets recently captured waiting tasks — queries that were actively waiting on a resource at collection time. Shows session ID, wait type, duration, blocking session, and database. Complements get_wait_stats by showing individual waiting queries rather than aggregated stats.")]
    public static async Task<string> GetWaitingTasks(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1.")] int hours_back = 1,
        [Description("Maximum rows. Default 30.")] int limit = 30,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingSessionReader.GetWaitingTasksAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "waiting_tasks")
                    ?? McpHelpers.Status("empty", "No waiting tasks captured in the specified time range.");

            var result = rows.Take(limit).Select(r => new
            {
                session_id = r.SessionId,
                wait_type = r.WaitType,
                wait_duration_ms = r.WaitDurationMs,
                blocking_session_id = r.BlockingSessionId,
                database_name = r.DatabaseName,
                resource_description = r.ResourceDescription,
                collection_time = r.CollectionTime.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                tasks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_waiting_tasks", ex);
        }
    }
}
