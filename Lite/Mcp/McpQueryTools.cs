using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpQueryTools
{
    /// <summary>
    /// #2320 (review catches, both — mirrors Darling): the attribution reads must never fail the
    /// tool call; any exception here collapses to "no cpu_window" per the omitted-never-fabricated
    /// contract. The two reads are independent (each opens its own DuckDB connection) and run
    /// concurrently.
    /// </summary>
    private static async Task<CpuAttribution.CpuWindow?> TryComputeCpuWindowAsync(
        LocalDataService dataService, int serverId, double attributedCpuMs, int hoursBack)
    {
        try
        {
            var averageTask = dataService.GetCpuWindowAverageAsync(serverId, hoursBack);
            var coresTask = dataService.GetLatestCpuCountAsync(serverId);
            await Task.WhenAll(averageTask, coresTask);
            var (avgSqlCpu, samples) = averageTask.Result;
            return CpuAttribution.Compute(attributedCpuMs, avgSqlCpu, samples, coresTask.Result, hoursBack);
        }
        catch (Exception)
        {
            return null;
        }
    }

    [McpServerTool(Name = "get_top_queries_by_cpu"), Description("Gets expensive queries from sys.dm_exec_query_stats (plan cache). Best for: currently cached queries with detailed per-execution stats, DOP, spills, and query_hash for trending. Returns query_hash, query_plan_hash, sql_handle, plan_handle, and host_object (the hosting procedure/function for proc-hosted statements, null for ad-hoc) — groups key on (database, query_hash, host_object), so INSERT...EXEC callers in different procedures report separately with their own text. distinct_texts counts statement texts merged into a group (>1 = ad-hoc literal variants or pre-upgrade history; query_text is one representative, 0 means no stored text for the group). Supports database and parallelism filtering. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes for the plan's time in cache (same semantics as max_dop), not windowed — totals and avgs are windowed deltas; rows where an extreme provably predates the window carry extremes_note. cpu_window reports what fraction of the box's MEASURED SQL CPU the returned rows explain (attributed_ratio, from the collected utilization series x core count); null when that series cannot support the denominator - omitted, never fabricated.")]
    public static async Task<string> GetTopQueriesByCpu(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description("If true, only return queries whose cached plan has EVER run at DOP > 1. Note: max_dop comes from sys.dm_exec_query_stats and is a lifetime-max for the plan's time in cache, so a plan compiled before MAXDOP was lowered keeps reporting the old higher value until it is evicted or recompiled. Confirm current parallelism with analyze_query_plan, which reads the actual plan.")] bool parallel_only = false,
        [Description("Minimum DOP to filter on. Implies parallel filtering. Filters the same lifetime-max value as parallel_only, not current parallelism.")] int min_dop = 0)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            var rows = await dataService.GetTopQueriesByCpuAsync(resolved.ServerId, hours_back, top, databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name });
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No query stats available for the specified time range.");
            }

            IEnumerable<QueryStatsRow> filtered = rows;
            if (parallel_only || min_dop > 1)
                filtered = filtered.Where(r => r.MaxDop > 1 && r.MaxDop >= (min_dop > 1 ? min_dop : 2));
            var kept = filtered.ToList();

            /* #2320: the denominator the ranking never handed the caller — mirrors Darling. Null
               (omitted) whenever the utilization series or core count can't support it. */
            var attribution = await TryComputeCpuWindowAsync(
                dataService, resolved.ServerId, kept.Sum(r => r.TotalCpuMs), hours_back);

            var result = kept.Select(r => new
            {
                database_name = r.DatabaseName,
                query_hash = r.QueryHash,
                query_plan_hash = r.QueryPlanHash,
                sql_handle = r.SqlHandle,
                plan_handle = r.PlanHandle,
                execution_count = r.TotalExecutions,
                total_cpu_ms = r.TotalCpuMs,
                total_elapsed_ms = r.TotalElapsedMs,
                avg_cpu_ms = r.AvgCpuMs,
                avg_elapsed_ms = r.AvgElapsedMs,
                min_cpu_ms = r.MinCpuMs,
                max_cpu_ms = r.MaxCpuMs,
                min_elapsed_ms = r.MinElapsedMs,
                max_elapsed_ms = r.MaxElapsedMs,
                /* #2235: min/max are lifetime extremes (see QueryStatExtremes) — flagged only on
                   the provable case, an extreme exceeding the whole window's total. */
                extremes_note = QueryStatExtremes.LifetimeExtremeNote(
                    r.TotalCpuMs, r.MaxCpuMs, r.TotalElapsedMs, r.MaxElapsedMs),
                min_dop = r.MinDop,
                max_dop = r.MaxDop,
                is_parallel = r.MaxDop > 1,
                total_logical_reads = r.TotalLogicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_physical_reads = r.TotalPhysicalReads,
                total_rows = r.TotalRows,
                total_spills = r.TotalSpills,
                avg_reads = r.AvgReads,
                // #2012 stage 2: same annotations as Darling's twin — the host object joins the
                // grouping key, so proc-hosted INSERT...EXEC callers sharing a hash land in
                // separate, correctly-labeled rows; null = ad-hoc/prepared text (or pre-upgrade
                // history, which ages out with retention).
                host_object = r.HostObjectName,
                query_text = McpHelpers.Truncate(r.QueryText, 2000),
                distinct_texts = r.DistinctTexts,
                text_note = r.DistinctTexts > 1
                    ? $"this group blends {r.DistinctTexts} distinct statement texts (ad-hoc literal variants; or history predating the host-object split for INSERT...EXEC callers); query_text is one representative"
                    : null
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #2320: null means the utilization series or core count could not support the
                   denominator — omitted, never fabricated. Mirrors Darling. */
                cpu_window = attribution is { } a
                    ? new
                    {
                        measured_sql_cpu_seconds = Math.Round(a.MeasuredSqlCpuSeconds),
                        attributed_cpu_seconds = Math.Round(a.AttributedCpuSeconds),
                        attributed_ratio = Math.Round(a.AttributedRatio, 3),
                        note = a.Note,
                    }
                    : null,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_top_queries_by_cpu", ex);
        }
    }

    [McpServerTool(Name = "get_top_procedures_by_cpu"), Description("Gets the most expensive stored procedures ranked by total CPU time. Shows execution counts, CPU/elapsed times, and I/O metrics. Delta-based: requires ~30 minutes after adding a new server before data appears. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes for the plan's time in cache (same semantics as max_dop), not windowed — totals and avgs are windowed deltas; rows where an extreme provably predates the window carry extremes_note. cpu_window reports what fraction of the box's MEASURED SQL CPU the returned rows explain (attributed_ratio, from the collected utilization series x core count); null when that series cannot support the denominator - omitted, never fabricated.")]
    public static async Task<string> GetTopProceduresByCpu(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top procedures. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            var rows = await dataService.GetTopProceduresByCpuAsync(resolved.ServerId, hours_back, top, databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name });
            if (rows.Count == 0)
            {
                return McpHelpers.Status(
                    "unavailable",
                    "No procedure stats available. Delta-based collection requires at least two collection cycles (~30 minutes) to produce non-zero values.");
            }

            /* #2320: same attribution denominator as the queries tool. Mirrors Darling. */
            var attribution = await TryComputeCpuWindowAsync(
                dataService, resolved.ServerId, rows.Sum(r => r.TotalCpuMs), hours_back);

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                full_name = r.FullName,
                object_type = r.ObjectType,
                sql_handle = r.SqlHandle,
                plan_handle = r.PlanHandle,
                execution_count = r.TotalExecutions,
                total_cpu_ms = r.TotalCpuMs,
                total_elapsed_ms = r.TotalElapsedMs,
                avg_cpu_ms = r.AvgCpuMs,
                avg_elapsed_ms = r.AvgElapsedMs,
                min_cpu_ms = r.MinCpuMs,
                max_cpu_ms = r.MaxCpuMs,
                min_elapsed_ms = r.MinElapsedMs,
                max_elapsed_ms = r.MaxElapsedMs,
                /* #2235: same lifetime-extremes flag as the queries tool. Mirrors Darling. */
                extremes_note = QueryStatExtremes.LifetimeExtremeNote(
                    r.TotalCpuMs, r.MaxCpuMs, r.TotalElapsedMs, r.MaxElapsedMs),
                avg_reads = r.AvgReads,
                total_logical_reads = r.TotalLogicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_physical_reads = r.TotalPhysicalReads,
                total_spills = r.TotalSpills
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #2320: null means the utilization series or core count could not support the
                   denominator — omitted, never fabricated. Mirrors Darling. */
                cpu_window = attribution is { } a
                    ? new
                    {
                        measured_sql_cpu_seconds = Math.Round(a.MeasuredSqlCpuSeconds),
                        attributed_cpu_seconds = Math.Round(a.AttributedCpuSeconds),
                        attributed_ratio = Math.Round(a.AttributedRatio, 3),
                        note = a.Note,
                    }
                    : null,
                procedures = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_top_procedures_by_cpu", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_top"), Description("Gets expensive queries from Query Store (persistent, survives restarts). Best for: historical analysis, queries no longer in plan cache. Requires Query Store enabled on target databases. Supports database filtering.")]
    public static async Task<string> GetQueryStoreTop(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            var rows = await dataService.GetQueryStoreTopQueriesAsync(resolved.ServerId, hours_back, top, databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name });
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No Query Store data available. Query Store may not be enabled on target databases.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                query_id = r.QueryId,
                plan_id = r.PlanId,
                query_hash = r.QueryHash,
                query_plan_hash = r.QueryPlanHash,
                execution_count = r.TotalExecutions,
                avg_duration_ms = r.AvgDurationMs,
                avg_cpu_ms = r.AvgCpuTimeMs,
                avg_logical_reads = r.AvgLogicalReads,
                avg_logical_writes = r.AvgLogicalWrites,
                avg_physical_reads = r.AvgPhysicalReads,
                avg_rowcount = r.AvgRowcount,
                last_execution_time = r.LastExecutionTime?.ToString("o"),
                query_text = McpHelpers.Truncate(r.QueryText, 2000)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_top", ex);
        }
    }

    [McpServerTool(Name = "get_query_duration_trend"), Description("Gets a time-series of average query duration over time. Useful for spotting overall performance degradation or improvement trends across all queries.")]
    public static async Task<string> GetQueryDurationTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var points = await dataService.GetQueryDurationTrendAsync(resolved.ServerId, hours_back);
            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                value = p.Value,
                execution_count = p.ExecutionCount
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_query_trend"), Description("Gets a time-series of performance metrics for a specific query identified by its query_hash. Use this after identifying a problematic query from get_top_queries_by_cpu or get_query_store_top to see how it has changed over time.")]
    public static async Task<string> GetQueryTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The query_hash value from get_top_queries_by_cpu or get_query_store_top.")] string query_hash,
        [Description("The database name the query belongs to.")] string database_name,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetQueryStatsHistoryAsync(resolved.ServerId, database_name, query_hash, hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("empty", $"No history found for query_hash '{query_hash}' in database '{database_name}' within the last {hours_back} hours.");
            }

            var result = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                execution_count = r.DeltaExecutions,
                cpu_ms = Math.Round(r.DeltaCpuMs, 2),
                elapsed_ms = Math.Round(r.DeltaElapsedMs, 2),
                avg_cpu_ms = Math.Round(r.AvgCpuMs, 2),
                avg_elapsed_ms = Math.Round(r.AvgElapsedMs, 2),
                logical_reads = r.DeltaLogicalReads,
                logical_writes = r.DeltaLogicalWrites,
                physical_reads = r.DeltaPhysicalReads,
                rows = r.DeltaRows,
                spills = r.DeltaSpills,
                min_dop = r.MinDop,
                max_dop = r.MaxDop,
                query_plan_hash = r.QueryPlanHash
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                database_name,
                query_hash,
                hours_back,
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
