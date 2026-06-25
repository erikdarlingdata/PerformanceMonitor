using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpQueryTools
{
    [McpServerTool(Name = "get_top_queries_by_cpu"), Description("Gets expensive queries from sys.dm_exec_query_stats (plan cache). Best for: currently cached queries with detailed per-execution stats, DOP, spills, and query_hash for trending. Returns query_hash, query_plan_hash, sql_handle, plan_handle. Supports database and parallelism filtering.")]
    public static async Task<string> GetTopQueriesByCpu(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description("If true, only return queries that used parallelism (max_dop > 1).")] bool parallel_only = false,
        [Description("Minimum DOP to filter on. Implies parallel filtering.")] int min_dop = 0)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            var rows = await resolved.Value.Service.GetQueryStatsForMcpAsync(hours_back, top, database_name, parallel_only, min_dop);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No query stats available for the specified time range.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                query_hash = r.QueryHash,
                query_plan_hash = r.QueryPlanHash,
                sql_handle = r.SqlHandle,
                plan_handle = r.PlanHandle,
                object_name = r.ObjectName,
                object_type = r.ObjectType,
                execution_count = r.ExecutionCount,
                total_cpu_ms = r.TotalCpuTimeMs,
                avg_cpu_ms = r.AvgCpuTimeMs,
                total_elapsed_ms = r.TotalElapsedTimeMs,
                avg_elapsed_ms = r.AvgElapsedTimeMs,
                min_cpu_ms = r.MinWorkerTimeMs,
                max_cpu_ms = r.MaxWorkerTimeMs,
                min_elapsed_ms = r.MinElapsedTimeMs,
                max_elapsed_ms = r.MaxElapsedTimeMs,
                min_dop = r.MinDop,
                max_dop = r.MaxDop,
                is_parallel = r.MaxDop > 1,
                total_logical_reads = r.TotalLogicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_physical_reads = r.TotalPhysicalReads,
                total_rows = r.TotalRows,
                total_spills = r.TotalSpills,
                avg_reads = r.AvgLogicalReads,
                query_text = McpHelpers.Truncate(r.QueryText, 2000)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_top_queries_by_cpu", ex);
        }
    }

    [McpServerTool(Name = "get_top_procedures_by_cpu"), Description("Gets the most expensive stored procedures ranked by total CPU time. Useful for identifying procedure-level optimization targets. Returns execution counts, CPU/elapsed time stats, and I/O metrics including spills.")]
    public static async Task<string> GetTopProceduresByCpu(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top procedures. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            var rows = await resolved.Value.Service.GetProcedureStatsForMcpAsync(hours_back, top, database_name);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No procedure stats available for the specified time range.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                full_name = r.FullObjectName,
                object_type = r.TypeDesc,
                sql_handle = r.SqlHandle,
                plan_handle = r.PlanHandle,
                execution_count = r.ExecutionCount,
                total_cpu_ms = r.TotalCpuTimeMs,
                avg_cpu_ms = r.AvgCpuTimeMs,
                total_elapsed_ms = r.TotalElapsedTimeMs,
                avg_elapsed_ms = r.AvgElapsedTimeMs,
                min_cpu_ms = r.MinWorkerTimeMs,
                max_cpu_ms = r.MaxWorkerTimeMs,
                min_elapsed_ms = r.MinElapsedTimeMs,
                max_elapsed_ms = r.MaxElapsedTimeMs,
                total_logical_reads = r.TotalLogicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_physical_reads = r.TotalPhysicalReads,
                avg_reads = r.AvgLogicalReads,
                total_spills = r.TotalSpills
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                procedures = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_top_procedures_by_cpu", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_top"), Description("Gets expensive queries from Query Store (persistent, survives restarts). Best for: historical analysis, forced plans, queries no longer in plan cache. Requires Query Store enabled on target databases. Supports database and parallelism filtering.")]
    public static async Task<string> GetQueryStoreTop(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description("If true, only return queries that used parallelism (max_dop > 1).")] bool parallel_only = false,
        [Description("Minimum DOP to filter on. Implies parallel filtering.")] int min_dop = 0)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            var rows = await resolved.Value.Service.GetQueryStoreDataForMcpAsync(hours_back, top, database_name, parallel_only, min_dop);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No Query Store data available. Query Store may not be enabled on target databases.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                query_id = r.QueryId,
                query_plan_hash = r.QueryPlanHash,
                execution_type = r.ExecutionTypeDesc,
                execution_count = r.ExecutionCount,
                plan_count = r.PlanCount,
                avg_duration_ms = r.AvgDurationMs,
                avg_cpu_ms = r.AvgCpuTimeMs,
                avg_logical_reads = r.AvgLogicalReads,
                avg_logical_writes = r.AvgLogicalWrites,
                avg_physical_reads = r.AvgPhysicalReads,
                min_dop = r.MinDop,
                max_dop = r.MaxDop,
                avg_rowcount = r.AvgRowcount,
                avg_memory_mb = r.AvgMemoryMb,
                is_forced_plan = r.IsForcedPlan,
                last_execution_time = r.LastExecutionTime?.ToString("o"),
                query_text = McpHelpers.Truncate(r.QuerySqlText, 2000)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_top", ex);
        }
    }

    [McpServerTool(Name = "get_expensive_queries"), Description("Gets expensive queries combining multiple sources (plan cache + Query Store) into a single ranked view. Best for: quick overview when you don't know which source to check. Use get_top_queries_by_cpu or get_query_store_top for more detail from a specific source.")]
    public static async Task<string> GetExpensiveQueries(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            var rows = await resolved.Value.Service.GetExpensiveQueriesAsync(hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No expensive query data available.");
            }

            IEnumerable<ExpensiveQueryItem> filtered = rows;
            if (!string.IsNullOrEmpty(database_name))
                filtered = filtered.Where(r => string.Equals(r.DatabaseName, database_name, StringComparison.OrdinalIgnoreCase));

            var result = filtered.Take(top).Select(r => new
            {
                source = r.Source,
                database_name = r.DatabaseName,
                object_name = r.ObjectName,
                execution_count = r.ExecutionCount,
                total_cpu_sec = r.TotalWorkerTimeSec,
                avg_cpu_ms = r.AvgWorkerTimeMs,
                total_elapsed_sec = r.TotalElapsedTimeSec,
                avg_elapsed_ms = r.AvgElapsedTimeMs,
                total_logical_reads = r.TotalLogicalReads,
                avg_logical_reads = r.AvgLogicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_physical_reads = r.TotalPhysicalReads,
                max_grant_mb = r.MaxGrantMb,
                first_execution = r.FirstExecutionTime?.ToString("o"),
                last_execution = r.LastExecutionTime?.ToString("o"),
                query_text = McpHelpers.Truncate(r.QueryTextSample, 2000)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_expensive_queries", ex);
        }
    }

    [McpServerTool(Name = "get_query_trend"), Description("Gets a time-series of performance metrics for a specific query identified by its query_hash. Use this after identifying a problematic query from get_top_queries_by_cpu or get_query_store_top to see how it has changed over time.")]
    public static async Task<string> GetQueryTrend(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("The query_hash value from get_top_queries_by_cpu.")] string query_hash,
        [Description("The database name the query belongs to.")] string database_name,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var rows = await resolved.Value.Service.GetQueryStatsHistoryAsync(database_name, query_hash);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("empty", $"No history found for query_hash '{query_hash}' in database '{database_name}'.");
            }

            var result = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                execution_count = r.ExecutionCount,
                execution_count_delta = r.ExecutionCountDelta,
                total_cpu_ms = r.TotalWorkerTimeMs,
                cpu_delta_ms = r.TotalWorkerTimeDeltaMs,
                avg_cpu_ms = r.AvgWorkerTimeMs,
                total_elapsed_ms = r.TotalElapsedTimeMs,
                elapsed_delta_ms = r.TotalElapsedTimeDeltaMs,
                avg_elapsed_ms = r.AvgElapsedTimeMs,
                total_logical_reads = r.TotalLogicalReads,
                reads_delta = r.TotalLogicalReadsDelta,
                avg_reads = r.AvgLogicalReads,
                total_physical_reads = r.TotalPhysicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_rows = r.TotalRows,
                min_dop = r.MinDop,
                max_dop = r.MaxDop,
                total_spills = r.TotalSpills,
                max_grant_mb = r.MaxGrantMb,
                max_used_grant_mb = r.MaxUsedGrantMb
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                database_name,
                query_hash,
                data_points = rows.Count,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_trend", ex);
        }
    }
}
