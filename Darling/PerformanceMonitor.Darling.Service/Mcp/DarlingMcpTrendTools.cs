/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// The windowed-trend data-read MCP tools — get_memory_trend / get_perfmon_trend / get_file_io_trend /
/// get_query_trend / get_query_duration_trend — served over Darling's Postgres store. These are the trend
/// siblings of the merged core data-read tools (<see cref="DarlingMcpDataTools"/>): each is a per-second /
/// per-collection time-series over the respective collected table, the SAME shape a client already sees on
/// Lite / the Dashboard. Every tool body mirrors LITE's <c>Mcp*Tools</c> trend tools field-for-field
/// (get_query_duration_trend has no Dashboard twin; the other four names + params match the Dashboard, the
/// shape follows Lite where the SKUs diverge — the same rule <see cref="DarlingMcpDataTools"/> follows), so
/// an MCP client sees one consistent product across all three SKUs.
///
/// <para>
/// Reads flow through <see cref="DarlingTrendReader"/> — STORED reads (no live monitored-server hit),
/// windowed BOTH-sides on the naive-UTC <c>collection_time</c>, byte-identical to the viewer's proven
/// chart reads. get_perfmon_trend reproduces Lite's miss vocabulary (the intentionally-uncollected Page
/// Life Expectancy special-case + the collected-counters hint); get_query_trend reproduces Lite's per-key
/// "empty" miss; the three unkeyed trends return the #1224 "unavailable" miss on an empty window, matching
/// the merged get_tempdb_trend sibling. A response-shape change here must land in Lite's Mcp*Tools too, and
/// vice versa.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpTrendTools
{
    [McpServerTool(Name = "get_memory_trend"), Description("Gets memory usage trend over time: total server memory, target memory, buffer pool, plan cache, and granted memory. Useful for identifying memory growth patterns or pressure periods.")]
    public static async Task<string> GetMemoryTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var points = await DarlingTrendReader.GetMemoryTrendAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (points.Count == 0)
                return McpHelpers.Status("unavailable", "No memory trend data available.");

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                total_server_memory_mb = p.TotalServerMemoryMb,
                target_server_memory_mb = p.TargetServerMemoryMb,
                buffer_pool_mb = p.BufferPoolMb,
                plan_cache_mb = p.PlanCacheMb,
                /* Lite carries total_granted_mb on its MemoryTrendPoint but GetMemoryTrendAsync (a
                   memory_stats-only read) leaves it unset — the grant overlay is a separate chart series.
                   Reproduced here as the same 0 placeholder for field-for-field parity with Lite's tool. */
                total_granted_mb = 0.0
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
            return McpHelpers.FormatError("get_memory_trend", ex);
        }
    }

    [McpServerTool(Name = "get_perfmon_trend"), Description("Gets a time-series trend for a specific performance counter. Use get_perfmon_stats first to see available counter names.")]
    public static async Task<string> GetPerfmonTrend(
        NpgsqlDataSource postgres,
        [Description("The exact counter name, e.g. 'Batch Requests/sec'.")] string counter_name,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var start = now.AddHours(-hours_back);
            var points = await DarlingTrendReader.GetPerfmonTrendAsync(postgres, resolved.ServerId, counter_name, start, now);
            if (points.Count == 0)
            {
                /* No points can mean three different things to a caller. Distinguish them so an LLM
                   doesn't read a bad counter name as "this metric looks fine" — Lite's get_perfmon_trend
                   miss vocabulary. */
                var collected = await DarlingTrendReader.GetDistinctPerfmonCountersAsync(postgres, resolved.ServerId, start, now);

                /* Page Life Expectancy is the counter people reach for by habit; it is intentionally
                   not collected, so an empty trend would otherwise be misread as "PLE looks fine." */
                if (IsPageLifeExpectancy(counter_name))
                    return McpHelpers.Status(
                        "not_collected",
                        $"No trend data for counter '{counter_name}'. Page Life Expectancy is a legacy metric and is intentionally not collected. " +
                        "Use get_memory_stats for buffer pool / memory pressure instead.",
                        new { collected_counters = collected });

                /* Nothing collected at all for this server in the window: the collector likely hasn't
                   produced perfmon data yet (delta counters need two cycles). Not retrievable now. */
                if (collected.Count == 0)
                    return McpHelpers.Status(
                        "unavailable",
                        $"No trend data for counter '{counter_name}'. No perfmon counters have been collected for this server in the last {hours_back}h yet " +
                        "(the collector may not have run, or delta counters need a second collection cycle).");

                /* Other counters exist but not this one: the name is almost certainly wrong. Hand back
                   the collected names so the caller can correct it. */
                return McpHelpers.Status(
                    "not_collected",
                    $"No trend data for counter '{counter_name}'. It may not be a counter this server collects — see hints.collected_counters for the {collected.Count} that are.",
                    new { collected_counters = collected });
            }

            /* sample_interval_seconds is the delta's denominator, and the only way a caller can tell a
               fabricated zero from an idle interval: 0 means no delta was knowable (first sighting,
               counter reset, or a gap past the policy), so delta_value = 0 with an interval of 0 must
               NOT be read as "no activity". Derive rates as delta_value / sample_interval_seconds
               rather than assuming a fixed cadence — fleet gaps run p50 299 s, p99 830 s, so dividing
               by the configured 60 s is wrong by whatever the jitter was (#2233, #2234). */
            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                value = p.Value,
                delta_value = p.DeltaValue,
                sample_interval_seconds = p.SampleIntervalSeconds
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                counter_name,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_perfmon_trend", ex);
        }
    }

    [McpServerTool(Name = "get_file_io_trend"), Description("Gets I/O latency trend over time per database, useful for spotting degradation in storage performance.")]
    public static async Task<string> GetFileIoTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var points = await DarlingTrendReader.GetFileIoLatencyTrendAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (points.Count == 0)
                return McpHelpers.Status("unavailable", "No I/O trend data available.");

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                database_name = p.DatabaseName,
                avg_read_latency_ms = Math.Round(p.AvgReadLatencyMs, 2),
                avg_write_latency_ms = Math.Round(p.AvgWriteLatencyMs, 2)
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
            return McpHelpers.FormatError("get_file_io_trend", ex);
        }
    }

    [McpServerTool(Name = "get_query_trend"), Description("Gets a time-series of performance metrics for a specific query identified by its query_hash. Use this after identifying a problematic query from get_top_queries_by_cpu or get_query_store_top to see how it has changed over time.")]
    public static async Task<string> GetQueryTrend(
        NpgsqlDataSource postgres,
        [Description("The query_hash value from get_top_queries_by_cpu or get_query_store_top.")] string query_hash,
        [Description("The database name the query belongs to.")] string database_name,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingTrendReader.GetQueryHistoryAsync(postgres, resolved.ServerId, database_name, query_hash, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return McpHelpers.Status("empty", $"No history found for query_hash '{query_hash}' in database '{database_name}' within the last {hours_back} hours.");

            var result = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                execution_count = r.DeltaExecutions,
                cpu_ms = Math.Round(r.DeltaCpuUs / 1000.0, 2),
                elapsed_ms = Math.Round(r.DeltaElapsedUs / 1000.0, 2),
                avg_cpu_ms = Math.Round(r.DeltaExecutions > 0 ? r.DeltaCpuUs / 1000.0 / r.DeltaExecutions : 0, 2),
                avg_elapsed_ms = Math.Round(r.DeltaExecutions > 0 ? r.DeltaElapsedUs / 1000.0 / r.DeltaExecutions : 0, 2),
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

    [McpServerTool(Name = "get_query_duration_trend"), Description("Gets a time-series of average query duration over time. Useful for spotting overall performance degradation or improvement trends across all queries.")]
    public static async Task<string> GetQueryDurationTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var points = await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (points.Count == 0)
                return McpHelpers.Status("unavailable", "No query duration trend data available.");

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

    /// <summary>
    /// True when the caller asked for Page Life Expectancy by any common spelling. Matches the full
    /// counter name (case-insensitive) or an exact "PLE" — but not "PLE" as a substring, so counters
    /// like "samples" don't false-positive. Lite's <c>McpPerfmonTools.IsPageLifeExpectancy</c>.
    /// </summary>
    private static bool IsPageLifeExpectancy(string counterName) =>
        counterName.Contains("page life expectancy", StringComparison.OrdinalIgnoreCase) ||
        counterName.Trim().Equals("PLE", StringComparison.OrdinalIgnoreCase);
}
