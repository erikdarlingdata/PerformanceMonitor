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
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The core data-read MCP tools — the SAME tool surface Lite and the Dashboard expose (resource
/// metrics, query performance, discovery/health), served over Darling's Postgres store. Each tool body
/// mirrors Lite's <c>Mcp*Tools</c> field-for-field (same tool names, same parameters, same response
/// fields, the #1224 miss vocabulary via <see cref="McpHelpers.Status"/>) so an MCP client sees one
/// consistent product across all three SKUs — the same reason the existing <see cref="DarlingMcpTools"/>
/// mirror Lite's analysis tools. These are the tools the analysis engine's <c>next_tools</c>
/// recommendations already point at (get_cpu_utilization / get_wait_stats / get_top_queries_by_cpu / …),
/// so a client following a finding's advice can now resolve them on this same server.
///
/// <para>
/// THE SEAM: where Lite's tools read its local DuckDB via <c>LocalDataService</c> and resolve a name
/// through its in-memory <c>ServerManager</c>, these read the Postgres store via
/// <see cref="DarlingDataReader"/> — a STORED read (no live monitored-server hit), consistent with
/// Darling's read-from-collected-data posture — and resolve through the Postgres <c>servers</c> registry
/// (<see cref="DarlingServerResolver"/>). Every read is Lite's / the viewer's proven read adapted to
/// Postgres. Where Lite and the Dashboard's result shapes diverge (Lite's get_cpu_utilization carries
/// idle_cpu, its get_file_io_stats the raw deltas rather than the Dashboard's SQL-Server-view-computed
/// latency assessment), this follows Lite — the store-faithful shape Darling's collector-mirror schema
/// can serve, matching the viewer's own port. A response-shape change here must land in Lite's
/// Mcp*Tools too, and vice versa.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpDataTools
{
    /* ═══════════════════════════ resource metrics ═══════════════════════════ */

    [McpServerTool(Name = "get_cpu_utilization"), Description("Gets CPU utilization over time showing SQL Server CPU %, other process CPU %, total CPU %, and idle %. Data is downsampled to 1-minute averages. Use this to identify CPU pressure periods, then use get_top_queries_by_cpu to find the culprit queries.")]
    public static async Task<string> GetCpuUtilization(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingDataReader.GetCpuUtilizationAsync(postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "cpu_utilization")
                    ?? McpHelpers.Status("unavailable", "No CPU utilization data available.");

            /* Downsample to 1-minute buckets to avoid overwhelming LLM context (Lite's projection). */
            var bucketed = rows
                .GroupBy(r => new DateTime(r.SampleTime.Year, r.SampleTime.Month, r.SampleTime.Day,
                    r.SampleTime.Hour, r.SampleTime.Minute, 0, r.SampleTime.Kind))
                .OrderBy(g => g.Key)
                .Select(g => new
                {
                    sample_time = g.Key.ToString("o"),
                    sql_server_cpu = (int)Math.Round(g.Average(r => r.SqlServerCpu)),
                    other_process_cpu = (int)Math.Round(g.Average(r => r.OtherProcessCpu)),
                    total_cpu = (int)Math.Round(g.Average(r => r.SqlServerCpu + r.OtherProcessCpu)),
                    idle_cpu = (int)Math.Round(g.Average(r => Math.Max(0, 100 - (r.SqlServerCpu + r.OtherProcessCpu)))),
                    samples_in_bucket = g.Count()
                });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                note = "Values are 1-minute averages of 15-second ring buffer samples.",
                samples = bucketed
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_cpu_utilization", ex);
        }
    }

    [McpServerTool(Name = "get_wait_stats"), Description("Gets the top SQL Server wait types aggregated over a time period. Wait stats reveal what SQL Server spends time waiting on — high signal waits indicate CPU pressure, high resource waits indicate I/O or lock contention. Use this first to identify the dominant wait category, then drill into specific tools based on the wait type.")]
    public static async Task<string> GetWaitStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return. Default 20.")] int limit = 20,
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
            var rows = await DarlingDataReader.GetWaitStatsAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "wait_stats")
                    ?? McpHelpers.Status("unavailable", "No wait stats data available for the specified time range.");

            var result = rows.Take(limit).Select(r =>
            {
                var signalPct = r.TotalWaitTimeMs > 0 ? (double)r.TotalSignalWaitTimeMs / r.TotalWaitTimeMs * 100 : 0;
                return new
                {
                    wait_type = r.WaitType,
                    total_wait_time_ms = r.TotalWaitTimeMs,
                    total_signal_wait_ms = r.TotalSignalWaitTimeMs,
                    resource_wait_ms = r.TotalWaitTimeMs - r.TotalSignalWaitTimeMs,
                    waiting_tasks = r.TotalWaitingTasks,
                    signal_wait_pct = Math.Round(signalPct, 1)
                };
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                waits = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_stats", ex);
        }
    }

    [McpServerTool(Name = "get_wait_types"), Description("Lists the distinct wait types observed on a server in the given time period, heaviest first. Useful for discovering which exact wait type to drill into with get_wait_trend.")]
    public static async Task<string> GetWaitTypes(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var types = await DarlingDataReader.GetDistinctWaitTypesAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            if (types.Count == 0)
            {
                /*
                    An empty list said nothing about which nothing this is. A server that collected and was
                    quiet in THIS window wants the window widened; a server nothing has been stored for
                    wants somebody to look at collection, and widening will never fill it. Probed only here,
                    against the SAME source the read walks.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "wait_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await DarlingDataReader.HasAnyWaitStatAsync(postgres, resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No wait types recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected wait stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No wait stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — nothing has been stored for this server at all. Delta wait stats need a SECOND collection cycle before the first row exists, so on a newly added server this clears itself; otherwise check that collection is running and that the server is enabled.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                wait_types = types
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_types", ex);
        }
    }

    [McpServerTool(Name = "get_wait_trend"), Description("Gets a time-series trend for a specific wait type, showing how wait time changes over time. Use get_wait_stats first to discover the dominant wait types.")]
    public static async Task<string> GetWaitTrend(
        NpgsqlDataSource postgres,
        [Description("The exact wait type name, e.g. CXPACKET, PAGEIOLATCH_SH.")] string wait_type,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var start = now.AddHours(-hours_back);
            var points = await DarlingDataReader.GetWaitTrendAsync(postgres, resolved.ServerId, wait_type, start, now);
            if (points.Count == 0)
            {
                /* The engine question comes BEFORE the distinct-values probe, not after it. Both are on
                   the miss path, so either order keeps the property that matters — but a permanently gated
                   engine takes this branch on every call, forever, and the probe below could never tell it
                   anything. Asking first makes that case one query instead of two. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "wait_stats");
                if (gated != null)
                {
                    return gated;
                }

                /* Distinguish "unknown wait type here" from "nothing collected at all", handing back the
                   ones that do have data — Lite's get_wait_trend miss vocabulary. */
                var collected = await DarlingDataReader.GetDistinctWaitTypesAsync(postgres, resolved.ServerId, start, now);
                if (collected.Count == 0)
                    return McpHelpers.Status(
                        "unavailable",
                        $"No trend data for wait type '{wait_type}'. No wait stats have been collected for this server in the last {hours_back}h yet " +
                        "(the collector may not have run, or delta wait stats need a second collection cycle).");

                return McpHelpers.Status(
                    "not_collected",
                    $"No trend data for wait type '{wait_type}'. It may not be a wait type observed on this server in this window — see hints.collected_wait_types for the {collected.Count} that have data.",
                    new { collected_wait_types = collected });
            }

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                wait_time_ms_per_second = p.WaitTimeMsPerSecond,
                signal_wait_time_ms_per_second = p.SignalWaitTimeMsPerSecond
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                wait_type,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_wait_trend", ex);
        }
    }

    [McpServerTool(Name = "get_memory_stats"), Description("Gets the latest memory statistics snapshot: physical memory, buffer pool size, plan cache size, memory utilization %, and SQL Server memory model. Use this for a quick memory health check; use get_memory_clerks to see detailed breakdown by component.")]
    public static async Task<string> GetMemoryStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var stats = await DarlingDataReader.GetLatestMemoryStatsAsync(postgres, resolved.ServerId);
            if (stats == null)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_stats")
                    ?? McpHelpers.Status("unavailable", "No memory stats available.");

            var utilization = stats.TotalPhysicalMemoryMb > 0
                ? (stats.TotalPhysicalMemoryMb - stats.AvailablePhysicalMemoryMb) / stats.TotalPhysicalMemoryMb * 100
                : 0;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                collection_time = stats.CollectionTime.ToString("o"),
                total_physical_memory_mb = stats.TotalPhysicalMemoryMb,
                available_physical_memory_mb = stats.AvailablePhysicalMemoryMb,
                memory_utilization_pct = Math.Round(utilization, 1),
                system_memory_state = stats.SystemMemoryState,
                sql_memory_model = stats.SqlMemoryModel,
                target_server_memory_mb = stats.TargetServerMemoryMb,
                total_server_memory_mb = stats.TotalServerMemoryMb,
                buffer_pool_mb = stats.BufferPoolMb,
                plan_cache_mb = stats.PlanCacheMb
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_stats", ex);
        }
    }

    [McpServerTool(Name = "get_memory_clerks"), Description("Gets the top memory consumers by memory clerk type — shows which SQL Server components are using the most memory.")]
    public static async Task<string> GetMemoryClerks(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingDataReader.GetLatestMemoryClerksAsync(postgres, resolved.ServerId);

            if (rows.Count == 0)
                /*
                    ONE branch here, deliberately, and it is the reason this read gets no existence probe.
                    The read is "every clerk at MAX(collection_time)", so zero rows back is logically the
                    same statement as zero rows in the table — any probe against that source would agree
                    with the read by construction and tell the caller nothing it did not already have. What
                    the caller does need is to be told that an empty clerk list is NEVER a quiet period,
                    because on a live SQL Server it cannot be: the DMV always has clerks.
                */
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_clerks")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No memory-clerk snapshot is available for {resolved.ServerName}. This read returns the LATEST snapshot rather than a window, so an empty result is never a quiet period — a live SQL Server always has memory clerks. It means nothing the memory_clerks collector stored is still retained, either because it has not run for this server or because its rows have aged out. Check get_collection_health and get_collection_log for the memory_clerks collector.");

            var result = rows.Select(r => new
            {
                clerk_type = r.ClerkType,
                memory_mb = Math.Round(r.MemoryMb, 2)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                clerks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_clerks", ex);
        }
    }

    [McpServerTool(Name = "get_file_io_stats"), Description("Gets the latest file I/O statistics per database file: read/write counts, bytes, stall times, and calculated latency. High read latency (>20ms) or write latency (>10ms for data, >2ms for log) often indicates storage bottlenecks.")]
    public static async Task<string> GetFileIoStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingDataReader.GetLatestFileIoStatsAsync(postgres, resolved.ServerId);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "file_io_stats")
                    ?? McpHelpers.Status("unavailable", "No file I/O stats available.");

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                file_name = r.FileName,
                file_type = r.FileType,
                physical_name = r.PhysicalName,
                size_mb = Math.Round(r.SizeMb, 1),
                delta_reads = r.DeltaReads,
                delta_writes = r.DeltaWrites,
                delta_read_bytes = r.DeltaReadBytes,
                delta_write_bytes = r.DeltaWriteBytes,
                delta_stall_read_ms = r.DeltaStallReadMs,
                delta_stall_write_ms = r.DeltaStallWriteMs,
                avg_read_latency_ms = Math.Round(r.DeltaReads > 0 ? (double)r.DeltaStallReadMs / r.DeltaReads : 0, 2),
                avg_write_latency_ms = Math.Round(r.DeltaWrites > 0 ? (double)r.DeltaStallWriteMs / r.DeltaWrites : 0, 2)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                files = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_file_io_stats", ex);
        }
    }

    [McpServerTool(Name = "get_tempdb_trend"), Description("Gets TempDB space usage over time: user objects, internal objects, version store, total reserved, and unallocated space. Also shows top TempDB consumer session. High version store can indicate long-running transactions under RCSI/SNAPSHOT isolation.")]
    public static async Task<string> GetTempDbTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingDataReader.GetTempDbTrendAsync(postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "tempdb_stats")
                    ?? McpHelpers.Status("unavailable", "No TempDB data available.");

            var result = rows.Select(r => new
            {
                time = r.CollectionTime.ToString("o"),
                user_objects_mb = Math.Round(r.UserObjectReservedMb, 1),
                internal_objects_mb = Math.Round(r.InternalObjectReservedMb, 1),
                version_store_mb = Math.Round(r.VersionStoreReservedMb, 1),
                total_reserved_mb = Math.Round(r.TotalReservedMb, 1),
                unallocated_mb = Math.Round(r.UnallocatedMb, 1),
                sessions_using_tempdb = r.TotalSessionsUsingTempDb,
                top_consumer_session_id = r.TopSessionId,
                top_consumer_mb = Math.Round(r.TopSessionTempDbMb, 1)
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
            return McpHelpers.FormatError("get_tempdb_trend", ex);
        }
    }

    [McpServerTool(Name = "get_perfmon_stats"), Description("Gets the latest SQL Server performance counter values: batch requests/sec, compilations/sec, deadlocks/sec, and more. Provides throughput context to distinguish a busy server from a sick one. Use counter_name or instance_name to filter results.")]
    public static async Task<string> GetPerfmonStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Filter to a specific counter name, e.g. 'Batch Requests/sec'.")] string? counter_name = null,
        [Description("Filter to a specific instance name, e.g. a database name.")] string? instance_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingDataReader.GetLatestPerfmonStatsAsync(postgres, resolved.ServerId);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "perfmon_stats")
                    ?? McpHelpers.Status("unavailable", "No perfmon stats available.");

            IEnumerable<DarlingDataReader.PerfmonRow> filtered = rows;
            if (!string.IsNullOrEmpty(counter_name))
                filtered = filtered.Where(r => r.CounterName.Contains(counter_name, StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrEmpty(instance_name))
                filtered = filtered.Where(r => r.InstanceName.Contains(instance_name, StringComparison.OrdinalIgnoreCase));

            var result = filtered.Select(r => new
            {
                counter_name = r.CounterName,
                instance_name = r.InstanceName,
                value = r.Value,
                delta_value = r.DeltaValue
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                counters = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_perfmon_stats", ex);
        }
    }

    /* ═══════════════════════════ query performance ═══════════════════════════ */

    [McpServerTool(Name = "get_top_queries_by_cpu"), Description("Gets expensive queries from sys.dm_exec_query_stats (plan cache). Best for: currently cached queries with detailed per-execution stats, DOP, spills, and query_hash for trending. Returns query_hash, query_plan_hash, sql_handle, plan_handle, and host_object (the hosting procedure/function for proc-hosted statements, null for ad-hoc) — groups key on (database, query_hash, host_object), so INSERT...EXEC callers in different procedures report separately with their own text. distinct_texts counts statement texts merged into a group (>1 = ad-hoc literal variants or pre-upgrade history; query_text is one representative, 0 means only rows predating the text dimension). Set group_by='host_object' to roll all of a procedure's statements into one row — necessary when dynamic SQL with per-value literals fragments one statement across many hashes, which no top-N-by-hash ranking can surface. Supports database and parallelism filtering. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes for the plan's time in cache (same semantics as max_dop), not windowed — totals and avgs are windowed deltas; rows where an extreme provably predates the window carry extremes_note. Also returns cpu_attribution: the returned rows' summed CPU-seconds against the SQL process's measured CPU-seconds for the window (avg cpu_utilization % x core count x window) - attributed_cpu_ratio says how much of the box the ranking explains; when the CPU series or core count is missing, or covers too little of the window, the ratio is omitted rather than invented.")]
    public static async Task<string> GetTopQueriesByCpu(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description("If true, only return queries whose cached plan has EVER run at DOP > 1. Note: max_dop comes from sys.dm_exec_query_stats and is a lifetime-max for the plan's time in cache, so a plan compiled before MAXDOP was lowered keeps reporting the old higher value until it is evicted or recompiled. Confirm current parallelism with analyze_query_plan, which reads the actual plan.")] bool parallel_only = false,
        [Description("Minimum DOP to filter on. Implies parallel filtering. Filters the same lifetime-max value as parallel_only, not current parallelism.")] int min_dop = 0,
        [Description("Grouping. 'query_hash' (default) is one row per (database, query_hash, host_object). 'host_object' rolls every statement of a hosting procedure/function into ONE row — use it when dynamic SQL built with per-value literals fragments one logical statement across many query_hash values, which makes top-N-by-hash structurally unable to surface it (measured at 21 fragments for one statement, whose combined CPU was the largest on the instance while no single fragment ranked). Ad-hoc statements have no host object and stay grouped per hash in both modes. distinct_query_hashes reports how many hashes a row rolled up.")] string group_by = "query_hash",
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        /* #2235: an unrecognised value must not silently fall back to the default grouping — a caller who
           asked for a rollup and got a per-hash ranking would read it as "this proc is not hot", which is
           the exact wrong conclusion this option exists to prevent. */
        var rollUp = string.Equals(group_by, "host_object", StringComparison.OrdinalIgnoreCase);
        if (!rollUp && !string.Equals(group_by, "query_hash", StringComparison.OrdinalIgnoreCase))
        {
            return McpHelpers.Status("invalid",
                $"group_by must be 'query_hash' or 'host_object' (got '{group_by}').");
        }

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, top, database_name, rollUpByHostObject: rollUp);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? McpHelpers.Status("unavailable", "No query stats available for the specified time range.");

            var filtered = rows
                .Where(r => !(parallel_only || min_dop > 1) || (r.MaxDop > 1 && r.MaxDop >= (min_dop > 1 ? min_dop : 2)))
                .ToList();

            /* #2320: what fraction of the box's measured CPU the RETURNED rows explain — numerator is
               the caller-visible ranking (post top-N, post filters), denominator is measured, and the
               ratio is omitted rather than invented when a denominator piece is missing. The two reads
               are independent, so they run concurrently (review catch). */
            var cpuAggregateTask = DarlingDataReader.GetCpuWindowAggregateAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            var propertiesTask = DarlingDataReader.GetLatestServerPropertiesAsync(postgres, resolved.ServerId);
            await Task.WhenAll(cpuAggregateTask, propertiesTask);
            var cpuAggregate = await cpuAggregateTask;
            var properties = await propertiesTask;
            var attribution = CpuAttribution.Compute(
                filtered.Sum(r => r.TotalCpuUs) / 1_000_000.0,
                now.AddHours(-hours_back), now,
                cpuAggregate.SampleCount, cpuAggregate.FirstSample, cpuAggregate.LastSample, cpuAggregate.AvgSqlCpuPercent,
                properties?.CpuCount ?? 0);

            var result = filtered.Select(r => new
            {
                database_name = r.DatabaseName,
                query_hash = r.QueryHash,
                query_plan_hash = r.QueryPlanHash,
                sql_handle = r.SqlHandle,
                plan_handle = r.PlanHandle,
                execution_count = r.TotalExecutions,
                total_cpu_ms = r.TotalCpuUs / 1000.0,
                total_elapsed_ms = r.TotalElapsedUs / 1000.0,
                avg_cpu_ms = r.TotalExecutions > 0 ? r.TotalCpuUs / 1000.0 / r.TotalExecutions : 0,
                avg_elapsed_ms = r.TotalExecutions > 0 ? r.TotalElapsedUs / 1000.0 / r.TotalExecutions : 0,
                min_cpu_ms = r.MinCpuUs / 1000.0,
                max_cpu_ms = r.MaxCpuUs / 1000.0,
                min_elapsed_ms = r.MinElapsedUs / 1000.0,
                max_elapsed_ms = r.MaxElapsedUs / 1000.0,
                /* #2235: min/max are lifetime extremes (see QueryStatExtremes) — flagged only on
                   the provable case, an extreme exceeding the whole window's total. */
                extremes_note = QueryStatExtremes.LifetimeExtremeNote(
                    r.TotalCpuUs, r.MaxCpuUs, r.TotalElapsedUs, r.MaxElapsedUs),
                min_dop = r.MinDop,
                max_dop = r.MaxDop,
                is_parallel = r.MaxDop > 1,
                total_logical_reads = r.TotalLogicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_physical_reads = r.TotalPhysicalReads,
                total_rows = r.TotalRows,
                total_spills = r.TotalSpills,
                avg_reads = r.TotalExecutions > 0 ? (double)r.TotalLogicalReads / r.TotalExecutions : 0,
                // #2012 stage 2: the statement's host object joins the GROUPING key, so proc-hosted
                // INSERT...EXEC callers sharing a hash now land in separate, correctly-labeled rows;
                // null = ad-hoc/prepared text (literal-collapse behavior unchanged). History rows
                // predating the column read as null and age out with raw retention.
                host_object = r.HostObjectName,
                query_text = McpHelpers.Truncate(r.QueryText, 2000),
                // #2012 stage 1's disclosure, now the residual: with proc-hosted callers split by
                // host_object, distinct_texts > 1 marks ad-hoc literal blends (or pre-stage-2
                // history where the split can't apply yet).
                distinct_texts = r.DistinctTexts,
                text_note = r.DistinctTexts > 1
                    ? $"this group blends {r.DistinctTexts} distinct statement texts (ad-hoc literal variants; or history predating the host-object split for INSERT...EXEC callers); query_text is one representative"
                    : null,
                // #2235: under host_object rollup this is the finding, not a decoration — it is the number
                // that explains why a per-hash ranking could not surface this statement. query_hash is one
                // member of the group when it is > 1, exactly as query_text already is for distinct_texts.
                distinct_query_hashes = r.DistinctQueryHashes,
                rollup_note = r.DistinctQueryHashes > 1
                    ? $"rolled up {r.DistinctQueryHashes} query_hash values belonging to {r.HostObjectName} — dynamic SQL with per-value literals fragments one statement across hashes, so none of these would rank individually; query_hash and query_text are one representative fragment"
                    : null
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #2235: echoed so a stored or pasted payload cannot be misread as the other grouping —
                   the two answer different questions and the rows look alike. */
                group_by = rollUp ? "host_object" : "query_hash",
                cpu_attribution = new
                {
                    ranked_cpu_seconds = attribution.RankedCpuSeconds,
                    sql_cpu_seconds_in_window = attribution.SqlCpuSecondsInWindow,
                    attributed_cpu_ratio = attribution.AttributedCpuRatio,
                    note = attribution.Note
                },
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_top_queries_by_cpu", ex);
        }
    }

    [McpServerTool(Name = "get_top_procedures_by_cpu"), Description("Gets the most expensive stored procedures ranked by total CPU time. Shows execution counts, CPU/elapsed times, and I/O metrics. Delta-based: requires ~30 minutes after adding a new server before data appears. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes for the plan's time in cache (same semantics as max_dop), not windowed — totals and avgs are windowed deltas; rows where an extreme provably predates the window carry extremes_note. Also returns cpu_attribution: the returned rows' summed CPU-seconds against the SQL process's measured CPU-seconds for the window (avg cpu_utilization % x core count x window) - attributed_cpu_ratio says how much of the box the ranking explains; when the CPU series or core count is missing, or covers too little of the window, the ratio is omitted rather than invented.")]
    public static async Task<string> GetTopProceduresByCpu(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top procedures. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingDataReader.GetTopProceduresByCpuAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, top, database_name);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "procedure_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No procedure stats available. Delta-based collection requires at least two collection cycles (~30 minutes) to produce non-zero values.");

            /* #2320: same attributed-CPU disclosure as the queries tool — one shared computation,
               same concurrent independent reads. */
            var cpuAggregateTask = DarlingDataReader.GetCpuWindowAggregateAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            var propertiesTask = DarlingDataReader.GetLatestServerPropertiesAsync(postgres, resolved.ServerId);
            await Task.WhenAll(cpuAggregateTask, propertiesTask);
            var cpuAggregate = await cpuAggregateTask;
            var properties = await propertiesTask;
            var attribution = CpuAttribution.Compute(
                rows.Sum(r => r.TotalCpuUs) / 1_000_000.0,
                now.AddHours(-hours_back), now,
                cpuAggregate.SampleCount, cpuAggregate.FirstSample, cpuAggregate.LastSample, cpuAggregate.AvgSqlCpuPercent,
                properties?.CpuCount ?? 0);

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                full_name = string.IsNullOrEmpty(r.SchemaName) ? r.ObjectName : $"{r.SchemaName}.{r.ObjectName}",
                object_type = r.ObjectType,
                sql_handle = r.SqlHandle,
                plan_handle = r.PlanHandle,
                execution_count = r.TotalExecutions,
                total_cpu_ms = r.TotalCpuUs / 1000.0,
                total_elapsed_ms = r.TotalElapsedUs / 1000.0,
                avg_cpu_ms = r.TotalExecutions > 0 ? r.TotalCpuUs / 1000.0 / r.TotalExecutions : 0,
                avg_elapsed_ms = r.TotalExecutions > 0 ? r.TotalElapsedUs / 1000.0 / r.TotalExecutions : 0,
                min_cpu_ms = r.MinCpuUs / 1000.0,
                max_cpu_ms = r.MaxCpuUs / 1000.0,
                min_elapsed_ms = r.MinElapsedUs / 1000.0,
                max_elapsed_ms = r.MaxElapsedUs / 1000.0,
                /* #2235: same lifetime-extremes flag as the queries tool. */
                extremes_note = QueryStatExtremes.LifetimeExtremeNote(
                    r.TotalCpuUs, r.MaxCpuUs, r.TotalElapsedUs, r.MaxElapsedUs),
                avg_reads = r.TotalExecutions > 0 ? (double)r.TotalLogicalReads / r.TotalExecutions : 0,
                total_logical_reads = r.TotalLogicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_physical_reads = r.TotalPhysicalReads,
                total_spills = r.TotalSpills
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                cpu_attribution = new
                {
                    ranked_cpu_seconds = attribution.RankedCpuSeconds,
                    sql_cpu_seconds_in_window = attribution.SqlCpuSecondsInWindow,
                    attributed_cpu_ratio = attribution.AttributedCpuRatio,
                    note = attribution.Note
                },
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
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var requestedStart = now.AddHours(-hours_back);
            var rows = await DarlingDataReader.GetQueryStoreTopAsync(postgres, resolved.ServerId, requestedStart, now, top, database_name);

            /* #2364: what the window ACTUALLY holds. The rows above are the top N by COST, so their timestamps
               say nothing about how far back the read reached -- the most expensive query in a month may have
               run this morning. raw query_store_stats is dropped at 4 days on a store with the rollups armed,
               and this tool has no rollup to fall back to (the corrected CAGGs carry no query_id or plan_id,
               and plan identity is the whole point of this tool). So the honest move is to report the window
               that was served rather than echo the one that was asked for. */
            var floor = await DarlingDataReader.GetQueryStoreWindowFloorAsync(postgres, resolved.ServerId, requestedStart, now);
            var effectiveStart = floor ?? requestedStart;
            var truncated = floor is DateTime f && f > requestedStart.AddMinutes(90);

            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store")
                    /* #2546: the sentence below GUESSES ("may not be enabled"), and it has to, because the
                       read had no way to find out. The store has known all along -- query_store_health
                       records actual_state per database every hour for exactly this purpose. Asking it turns
                       a hedge into a fact plus the ALTER DATABASE that fixes it, and it answers for the
                       database this read was scoped to rather than for the server's most flattering one. */
                    ?? await DarlingRuntimePrecondition.QueryStoreStatusAsync(postgres, resolved.ServerId, resolved.ServerName, database_name)
                    /* And the collector's own last run, for the case Query Store is on and the collector is
                       the thing that cannot read it. */
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No Query Store rows for this server in the {hours_back}-hour window searched. Query Store " +
                        "may not be enabled on the target databases -- or the window reaches past what the raw tier " +
                        "retains (query_store_stats is dropped at 4 days when the rollups are armed), in which case " +
                        "nothing was read for the older part of it. Try a shorter window before concluding the " +
                        "queries did not run.");

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
                query_text = McpHelpers.Truncate(r.QueryText, 2000),
                /* Emitted because it is a grouping key: on a 2022+ AG the same query can appear once per
                   replica role, and without this the caller would see duplicate-looking rows with no way
                   to tell them apart. NULL when the server did not attribute the row. */
                replica_role = r.ReplicaRole
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #2364: what was served, beside what was asked for. hours_back alone was a request echoed
                   back as though it described the data. */
                effective_start = effectiveStart.ToString("o"),
                effective_hours_back = Math.Round((now - effectiveStart).TotalHours, 1),
                truncated,
                truncation_note = truncated
                    ? "The window reaches further back than this server's raw query_store_stats retains, so the "
                      + "older part of it was not read. This tool reads the raw tier only: the corrected rollups "
                      + "carry no query_id or plan_id, and plan identity is what it exists to return."
                    : null,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_top", ex);
        }
    }

    /* ═══════════════════════════ discovery / health ═══════════════════════════ */

    [McpServerTool(Name = "list_servers"), Description("Lists all monitored SQL Server instances with their collection freshness status and last collection time. Use this first to see available servers before calling other tools. The service has no live connection to the monitored servers, so status is derived from how recently each server was collected (Online = fresh, Warning = stale, Offline = no recent collection). The peer_fleets block names the SIBLING Darling stores that monitor the rest of a split fleet, with what each one covers — this server can only NAME them (no cross-store reads), and peer_note says what an empty peer_fleets does and does not prove.")]
    public static async Task<string> ListServers(
        NpgsqlDataSource postgres)
    {
        try
        {
            List<DarlingDataReader.ServerListRow> servers;
            try
            {
                servers = await DarlingDataReader.GetServerListAsync(postgres);
            }
            catch (Exception ex)
            {
                return $"Could not read the servers registry from the Postgres store: {ex.Message}";
            }

            /* #2339: the empty-registry answer is prose, not the JSON envelope, so it carries the peer
               disclosure explicitly — otherwise it is the one path where the declaration silently vanishes,
               and it is the worst one to lose it on: a store with nothing registered is a fresh or
               just-restarted box, where "no servers here" with no mention of the siblings is the strongest
               version of the wrong conclusion. */
            if (servers.Count == 0)
                return "No servers are registered yet. The service registers each monitored server on its first successful connection."
                    + DarlingPeerDirectory.EmptyRegistryDisclosure(DarlingPeerDirectory.Current);

            return RenderServerList(servers, DateTime.UtcNow, DarlingPeerDirectory.Current);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("list_servers", ex);
        }
    }

    /// <summary>
    /// The <c>list_servers</c> envelope, pure over (registry rows, now, declared peers) — separated from the
    /// store read so the response SHAPE, including the #2339 peer disclosure, pins without a live store.
    ///
    /// <para><b>Why the peer block lives on THIS tool.</b> <c>list_servers</c> is the discovery read: it is
    /// where an agent forms its model of "who is monitored", so it is where the fact that SIBLING stores hold
    /// the rest of a split fleet has to appear. <c>peer_fleets</c> is therefore always present and
    /// <c>peer_note</c> is always populated — an EMPTY peer list has two very different meanings (this really
    /// is the only store, or the operator never declared its siblings) and this server cannot tell them
    /// apart, so it says so rather than letting an empty array read as "this is the whole fleet".</para>
    /// </summary>
    internal static string RenderServerList(
        IReadOnlyList<DarlingDataReader.ServerListRow> servers,
        DateTime nowUtc,
        DarlingPeerDirectory.Snapshot peers)
    {
        var result = servers.Select(s => new
        {
            server_name = s.ServerName,
            display_name = string.IsNullOrEmpty(s.DisplayName) ? s.ServerName : s.DisplayName,
            sql_version = SqlVersionLabel(s.SqlMajorVersion),
            status = FreshnessStatus(s.LastCollection, nowUtc),
            read_only = s.ServerName.EndsWith(":RO", StringComparison.Ordinal),
            last_collection = s.LastCollection?.ToString("o")
        });

        return JsonSerializer.Serialize(new
        {
            server_count = servers.Count,
            this_store_covers = peers.ThisStoreCovers.Length == 0 ? null : peers.ThisStoreCovers,
            peer_fleets = peers.Peers.Select(p => new
            {
                name = p.Name,
                covers = p.Covers,
                matches = p.Matches
            }),
            peer_note = peers.Peers.Count == 0
                ? DarlingPeerDirectory.NoPeersDeclaredNote
                : DarlingPeerDirectory.PeersDeclaredNote,
            servers = result
        }, McpHelpers.JsonOptions);
    }

    [McpServerTool(Name = "get_collection_health"), Description("Shows the health status of all data collectors for a server — whether they're running successfully, failing, or stale. A collector reads STOPPED rather than FAILING when it has attempted nothing at all — no success, no error, nothing — for longer than the FAILING cutoff, despite a history of runs: that is a collector whose gate (AppliesTo) flipped off for this target rather than one that keeps running and erroring, and it does not count toward a server's failing-collector total. Check this before investigating data to ensure collectors are working properly. Each row also carries last_note/note_count: what a NON-failing run reported, e.g. an enumeration that came back with 0 items. note_count equal to total_runs means the collector has been collecting nothing all window — not a fault (the target may be legitimately empty), but the reason a HEALTHY collector can still have no data. target_has_user_databases tells those two apart: true means the target DID have user databases in the same window, so an all-window empty enumeration is worth investigating (a login that cannot enter them, an exclusion filter that matched everything); false means either no user databases or no inventory to go on. Each row also carries abandoned and abandon_rate_pct: cycles the 120-second whole-server wall-clock budget gave up on, which stored nothing and advanced no watermark. Unlike a yield, which retries, an abandoned cycle is collected data you do not have. A rate above 0.5% bands the collector WARNING, so a WARNING here may have nothing to do with errors - read abandoned beside errors to attribute it. CRITICAL for reading last_error: it is a single slot carrying the newest ERROR or PERMISSIONS message in the whole window, and a message in it is NOT evidence that the condition is current. Read last_error_at for when it happened, last_denied_at for when the newest DENIAL specifically happened, and denied_since_last_success for the derived answer - true means a denial is the collector's current state, false means every denial in the window predates a later success and the collector is reading fine now. A fault recorded before a code path changed will sit in last_error for the rest of the window while every cycle since succeeds: pg_deadlocks moved from an in-database route to an AWS API route, and six days later this tool still showed HEALTHY, errors 0, a reassuring note and a stale permission denial together - a combination that describes a state which cannot occur, and which produced a bug report claiming a fleet-wide denial when the collector had been succeeding on all 50 targets. Do not infer a live condition from last_error alone. Total abandonment still reads FAILING through staleness; the rate exists for the partial case, where a collector abandons some cycles and succeeds often enough to stay fresh, which otherwise read HEALTHY with errors 0 indefinitely. The sweep_pressure block is the server-level roll-up: it compares the collectors' combined execution demand (average duration amortized by cadence) against the minute the fastest cadence holds. SATURATED means the collection body cannot fit inside its cadence, so relaunches are skipped and the server collects at a multiple of its configured interval while every collector still reads healthy — heaviest_collectors names where that budget goes. That verdict is the SUSTAINED answer only. peak_cycle_risk is the separate single-sweep answer: peak_cycle_ms is what the body costs on the cycle where every scheduled cadence comes due together, and BODY_OVERRUN means that one body cannot fit the budget even when the verdict reads OK — the signature of one infrequent heavy collector, which amortization hides and heaviest_collectors therefore ranks out of sight. peak_collector names it, and peak_cycle_note explains it. Read both fields: a server can be OK/BODY_OVERRUN (a schedule-shape problem, fix by moving or splitting that collector) or SATURATED/BODY_OVERRUN (a capacity problem). Every collector row carries avg_duration_ms, p95_duration_ms and max_duration_ms, because a collector's runs are not always one population: query_store on one dogfood server averaged 13,834 ms over 1,155 runs of which 958 yielded nothing and cost about 36 ms, which puts the other 197 at roughly 80,900 ms EACH - each one, on its own, larger than the whole sweep budget. Read the three together: avg close to p95 close to max is one population, avg far below p95 is two, and p95 far below max is one pathological run. peak_cycle_ms is built from p95 (floored at the mean, so it can never read lower than a mean-based figure) for exactly that reason, and peak_collector carries peak_run_ms beside avg_duration_ms so the gap is visible. Those three still describe RUNS, and a collector that runs once per DATABASE writes one blended row, so no run-level statistic can say which database cost what. Five fan out from an enumeration on any SQL Server target (query_store, plan_correction, query_store_health, index_object_stats, database_scoped_config); separately, eight more fan out over a per-database connection loop when the target is Azure SQL DB, and pg_autovacuum_stats always does on PostgreSQL. The per-collector `fanout` block is that answer, null for a collector that does not fan out: `items` is how wide the fan-out was, `slowest`/`slowest_ms` name the dearest database and its cost on the window's worst run, `run_ms` is that whole run, and `dominance` is slowest_ms * items / run_ms — 1.0 for a perfectly even fan-out, rising with concentration. It matters because the remedies diverge there: near 1.0 the cost is the fan-out's WIDTH and bounded parallelism is the lever, while around 2.0 or above one database dominates and a per-database schedule override or a stagger is what helps. Do not try to infer this from p95 versus avg — on a per-database collector that ratio is usually saturated by empty-versus-productive runs and says nothing about databases. Every field named so far describes what a collector SPENT; rows_stored, runs_with_rows and productive_run_pct are what it BOUGHT, counted over the same window as total_runs and the durations, so cost and output on a row always describe the same runs. Read them together for the three readings that need different actions: rows_stored above zero is expensive AND productive; rows_stored zero with denied_since_last_success false is a collector that read and found nothing, which for one that stores a row only when an event occurs (e.g. deadlocks, blocked_process_report, pg_blocking, pg_xmin_horizon) is the correct resting state and needs no action; rows_stored zero with denied_since_last_success true is a collector that could not read and needs a grant. output_finding says which of the two zero readings applies and is null whenever rows_stored is positive. This is deliberately NOT a band: pg_deadlocks was the single most expensive collector on one managed store, 49,258,335 ms over 79,333 runs in seven days, and stored zero rows - and that zero was CORRECT, because the reader was working on all 50 targets and there were no deadlocks to find. A verdict keyed on cost-plus-zero-rows would fire on the healthy quiet install rather than the blind one. These are NOT the hourly per-collector series Darling's get_collector_cost reports as total_rows - a separate series over that caller's own days_back and across every server at once, and Darling-only, so Lite has no twin of it; the top-level output_note names both windows and disclaims that one. rows_stored is also what a run STORED, never what the monitored engine counted, so a zero cannot tell a genuinely quiet source apart from a reader capturing nothing off a busy one - nothing on this surface measures that. One block on this response is deliberately NOT on the seven-day window: alert_read_health, which counts the alerting layer's OWN store reads that failed and were swallowed. A condition check that cannot read the store logs one line and skips - correctly, because firing on absent evidence would fabricate an alert and resolving on it would fabricate a recovery - and that skip is not a collector run, so it writes no collection_log row and reaches no other health surface: only a grep of the service log found the class. It matters out of proportion to the count because the alert pass runs on a much shorter store deadline than the collection sweep, so as store latency rises the alerting layer is the FIRST thing to fail and collection is the last - during one measured episode of store lock contention the service log's error rate rose 41 to 61 per hour, every line an alerting-side read, while collector failures over the same hours FELL from 23 to 2. Read server_read_failures beside server_alert_passes for this server, instance_read_failures for the whole service (which also covers the fleet-scoped store self-alerts - disk pressure, compression-job health, store-job cadence, retention holds - that belong to no server and so appear in no per-server count), last_failure_read for which condition went blind most recently, and last_failure_at to tell a healed episode from a live one: this count never ages out of a window, so a nonzero value with a stamp from days ago is history. counting_since is when this process began counting, early in its own startup - these are in-memory counts and a restart takes them to zero, so a zero means \"none since counting_since\" and NOT \"none in seven days\"; check the stamp before reading the zero as reassurance. Deliberately not persisted, because what it counts is a failure to READ the store. It does NOT count alerts that failed to DELIVER and makes no claim about them - that is get_alert_history's question. And deliberately not a band, for the same reason the output figures are not: any threshold over it would have to guess how many blind reads make alerting unhealthy, and a wrong guess on this particular surface fails by saying nothing is wrong.")]
    public static async Task<string> GetCollectionHealth(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var rows = await DarlingDataReader.GetCollectionHealthAsync(postgres, resolved.ServerId, DateTime.UtcNow.AddDays(-7));
            if (rows.Count == 0)
                return McpHelpers.Status("unavailable", "No collection health data available.");

            var result = rows.Select(r => new
            {
                collector = r.CollectorName,
                status = r.HealthStatus,
                total_runs = r.TotalRuns,
                errors = r.ErrorCount,
                /* Deliberate 1s lock-timeout yields (#1805) — benign, distinct from errors; clustering
                   here is a lock-contention signal about the monitored server. */
                yields = r.YieldCount,
                /* #2804: runs the #2673 wall-clock budget gave up on. Unlike a yield, which retries, an
                   abandoned cycle stored nothing and advanced no watermark — it is data LOSS, and it is
                   the reason a WARNING here may have nothing to do with `errors`. Before this it reached
                   the surface only inside note_summary's prose, so there was no number to threshold,
                   alert or trend on. */
                abandoned = r.AbandonedCount,
                abandon_rate_pct = Math.Round(r.AbandonRatePercent, 2),
                failure_rate_pct = Math.Round(r.FailureRatePercent, 1),
                avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
                /* #2460: the mean above is a blend whenever a collector's runs come in two sizes, and
                   on this fleet one of them plainly does — query_store averaged 13,834 ms over 1,155
                   runs where 958 yielded nothing at ~36 ms, which puts the other 197 at ~80,900 ms
                   each. p95 is what a HEAVY run of this collector costs and is what the peak-cycle
                   arithmetic below is built from; max is carried beside it so a routine tail can be
                   told from a single pathological cycle, which is the one thing a max alone cannot
                   say about itself. Read the three together: avg ~= p95 ~= max is one population,
                   avg << p95 is two, and p95 << max is one bad run. */
                p95_duration_ms = Math.Round(r.P95DurationMs, 0),
                max_duration_ms = Math.Round(r.MaxDurationMs, 0),
                /* #3017: what the spend BOUGHT, beside what it cost. Every field above this line
                   describes cost — the run count, the three durations, and the sweep-pressure roll-up
                   built from them — and none of them said whether any of it bought anything. The rows
                   figure lived on get_collector_cost, a different tool over a different (hourly,
                   fleet-wide) series, so correlating spend against output was a join a caller had to
                   know to make.

                   Measured: pg_deadlocks was the single dearest collector on a managed store —
                   49,258,335 ms over 79,333 runs in seven days, about 13.7 h/week — and stored zero
                   rows. THAT ZERO WAS CORRECT: the reader was working on all 50 targets and there were
                   no deadlocks to find. Which is exactly why this is a fact placed beside the cost and
                   NOT a band — a verdict keyed on cost-plus-zero-rows fires on the healthy quiet
                   install rather than the blind one, the cry-wolf failure #1852 exists to prevent.

                   Flat rather than a nested block like `fanout`: the denominator these are read against
                   is total_runs, which is already flat on this row, and nesting the numerator away from
                   its denominator would be the half-a-ratio shape the block would have existed to
                   prevent. runs_with_rows is get_pg_blocking's captures_with_blocking move — 12 rows
                   over 3 of 79,333 runs is a different collector from 12 rows over all of them. */
                rows_stored = r.RowsStored,
                runs_with_rows = r.RunsWithRows,
                productive_run_pct = Math.Round(r.ProductiveRunPercent, 1),
                last_success = r.LastSuccessTime?.ToString("o"),
                last_error = r.LastError,
                /* #3010: WHEN that error was, which the field never carried. `last_error` is a single slot
                   holding the newest ERROR/PERMISSIONS message in the window, and it was served with no
                   timestamp beside it — so a condition from six days ago, on a code path the collector no
                   longer takes, reads exactly like one from the last cycle.

                   That is not hypothetical. `pg_deadlocks` moved from the in-database pg_read_file route
                   to the RDS log API; its PERMISSIONS rows stop dead at the cutover and every cycle since
                   has been a SUCCESS on all 50 targets. Six days later this tool still reported HEALTHY,
                   errors 0, a reassuring note, AND `permission denied for function pg_read_file`. Every
                   element was individually true and together they described a server being refused right
                   now, which was false. A bug report was filed on exactly that reading.

                   So all three ride together: the instant of the newest failure of either class, the
                   instant of the newest DENIAL specifically, and the derived answer to the only question a
                   reader actually has — is this current, or a fossil. */
                last_error_at = r.LastErrorTime?.ToString("o"),
                last_denied_at = r.LastDeniedTime?.ToString("o"),
                denied_since_last_success = r.DeniedSinceLastSuccess,
                /* #3017's third term, and the whole reason this waited for #3010. rows_stored = 0 spans
                   two collectors that want opposite actions: one that read and found nothing, and one
                   that could not read. denied_since_last_success is what separates them, so the finding
                   sits directly beneath it and names which reading applies. Null when the collector
                   stored something — a note that fires on the healthy case is how a signal teaches
                   people to ignore it (FormatPeakCycleNote's own reasoning).

                   Composed from the shared formatter, like note_summary above, so the web table and any
                   other consumer cannot re-derive the sentence differently. Reading the predicate here
                   does not band on it: this is display text and HealthStatus never sees it. */
                output_finding = r.OutputFinding,
                /* #1837: what a NON-failing run reported — an enumeration that came back with 0 items,
                   items whose enumeration probe failed. note_count == total_runs means every run in the
                   window came back that way, which is the "collecting nothing for weeks" case that reads
                   as HEALTHY (correctly — an empty target is not a fault) and needs saying out loud. */
                last_note = r.LastNote,
                note_count = r.NoteCount,
                /* #1852: whether the store saw user databases on this target in the same window. The
                   fact that separates "nothing to collect" from "collecting nothing" — a caller
                   diagnosing an empty collector gets it as a boolean instead of parsing it out of the
                   sentence below. False also means "no inventory to go on", never "no databases". */
                target_has_user_databases = r.TargetHasUserDatabases,
                /* The same string both WPF grids render, composed on this side so the web dashboard and
                   any other consumer cannot re-derive it differently. */
                note_summary = CollectorHealthClassifier.FormatCollectionNote(
                    r.LastNote, r.NoteCount, r.TotalRuns, r.CollectorName, r.TargetHasUserDatabases),
                /* #2472: the per-database breakdown of a collector that fans out, null for one that does
                   not. Emitted as a nested object rather than four sibling fields so a consumer cannot
                   read a slowest item without the width it has to be judged against — the parts only mean
                   something together, and `dominance` is that meaning, computed here so every consumer
                   gets the same arithmetic instead of three of them inventing it.

                   This is the thing avg/p95/max structurally cannot say: they aggregate over runs, and one
                   run is one blended row however many databases it covered. */
                fanout = r.FanoutDominance is null ? null : new
                {
                    items = r.FanoutItems,
                    slowest = r.SlowestItem,
                    slowest_ms = r.SlowestItemMs,
                    run_ms = r.SlowestRunDurationMs,
                    dominance = Math.Round(r.FanoutDominance.Value, 2)
                }
            });

            /* #2296: the roll-up that makes half-rate collection visible. Every collector on a saturated
               server reads HEALTHY — from each one's own seat nothing is wrong — so the condition only
               existed as a service-log warning ("collection body has not completed … skipping relaunch").
               The verdict compares the collectors' combined execution demand (average duration amortized
               by cadence) against the minute the fastest cadence holds; heaviest_collectors names where
               the budget goes, which is the actionable half of the answer. */
            var pressure = SweepPressureClassifier.Compute(
                rows.Select(r => (r.CollectorName, r.AvgDurationMs, r.P95DurationMs, r.FrequencyMinutes)));
            var heaviest = rows
                .Where(r => r.FrequencyMinutes > 0 && r.AvgDurationMs > 0)
                .OrderByDescending(r => r.AvgDurationMs / r.FrequencyMinutes)
                .Take(3)
                .Select(r => new
                {
                    collector = r.CollectorName,
                    avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
                    p95_duration_ms = Math.Round(r.P95DurationMs, 0),
                    max_duration_ms = Math.Round(r.MaxDurationMs, 0),
                    frequency_minutes = r.FrequencyMinutes,
                    /* #2446: the ranking key said out loud, beside the single-run cost it is derived from.
                       The list still ranks by amortized contribution, because that is what explains
                       busy_percent — but an operator reading it to find the collector that overran a body
                       was reading the wrong column with nothing on the row to say so. */
                    amortized_ms_per_minute = Math.Round(r.AvgDurationMs / r.FrequencyMinutes, 0),
                    /* #2460: "% of the budget PER RUN" now comes from the run that actually costs
                       something — PeakRunMs, the p95 floored at the mean — rather than from a mean that
                       on a bimodal collector describes no run at all. It is the same number the peak
                       cycle charges this collector, so the column and the cycle reconcile by hand;
                       taken from the mean, this row said query_store cost 23% of a body when its heavy
                       run costs 135% of one. Through the shared helper rather than re-derived here, so
                       the floor rule cannot drift between the two SKUs' tools. */
                    pct_of_sweep_budget_per_run = Math.Round(
                        SweepPressureClassifier.PeakRunMs(r.AvgDurationMs, r.P95DurationMs) / SweepPressureClassifier.SweepBudgetMs * 100.0, 1)
                });

            /* #2446: the collector that owns the most of ONE sweep, which is a different collector from
               the ones above whenever it is infrequent enough for amortization to hide it. Named on every
               server, not only on BODY_OVERRUN — knowing where a body's time concentrates is worth having
               before it is a problem, and this is exactly the row heaviest_collectors ranks out of sight. */
            var peakCollector = pressure.PeakCollectorName == null ? null : new
            {
                collector = pressure.PeakCollectorName,
                /* #2460: what one aligned body is charged for this collector — its p95, floored at its
                   mean — with the mean kept beside it, because on a bimodal collector the GAP between
                   the two is the finding. amortized_ms_per_minute stays derived from the mean: that is
                   what amortization means, and a rate built from a tail would claim work the server
                   never sustains. */
                peak_run_ms = Math.Round(pressure.PeakCollectorPeakRunMs, 0),
                avg_duration_ms = Math.Round(pressure.PeakCollectorAvgDurationMs, 0),
                frequency_minutes = pressure.PeakCollectorFrequencyMinutes,
                amortized_ms_per_minute = Math.Round(pressure.PeakCollectorAvgDurationMs / pressure.PeakCollectorFrequencyMinutes, 0),
                pct_of_sweep_budget_per_run = Math.Round(pressure.PeakCollectorPeakRunMs / SweepPressureClassifier.SweepBudgetMs * 100.0, 1)
            };
            var peakCycleNote = SweepPressureClassifier.FormatPeakCycleNote(pressure);

            /* #3013: the alerting layer's own store reads, which appear on no other health surface. A
               condition check that cannot read the store logs one line and skips — correctly, since firing
               on absent evidence fabricates an alert — but the skip is not a collector run, so it writes no
               collection_log row and every field above this line stays green while the alert pass goes
               blind one condition at a time. The key is derived the way THIS SKU's alert pass derives it
               (invariant), so the read and the write land in the same bucket; AlertReadFailureSurfaceTests
               pins that agreement from source rather than trusting it. */
            var alertReads = AlertReadFailureCounter.Shared.ReadFor(
                resolved.ServerId.ToString(CultureInfo.InvariantCulture));
            var alertReadFinding = AlertReadFailureCounter.FormatFinding(alertReads);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3013: a BLOCK rather than flat fields, unlike #3017's row-level output figures. There the
                   denominator (total_runs) was already on the row, so nesting the numerator away from it
                   would have split a ratio; here neither number exists on the response yet, so the block is
                   what keeps them together. Deliberately not a band and not a status input: any threshold
                   over it would have to guess how many blind reads make alerting unhealthy, and a wrong
                   guess on THIS surface fails in the direction #3013 is about. */
                alert_read_health = new
                {
                    /* Both scopes, because the two answer different questions and neither substitutes. The
                       per-server number is the actionable unit and matches this tool's scope; the instance
                       number is the only home the FLEET-scoped store self-alerts have — disk pressure,
                       compression-job health, store-job cadence, retention holds belong to no server, so a
                       per-server-only figure would have left them exactly as invisible as #3013 found the
                       whole class. */
                    server_read_failures = alertReads.ServerReadFailures,
                    server_alert_passes = alertReads.ServerAlertPasses,
                    instance_read_failures = alertReads.InstanceReadFailures,
                    /* The currency term, and the reason a count alone would be misread: this figure never
                       ages out of a window, so without a stamp beside it a healed episode from days ago and
                       one still in progress read identically. Exactly last_error's #2966 lesson. */
                    last_failure_at = alertReads.LastFailureAtUtc,
                    last_failure_read = alertReads.LastFailureRead,
                    /* The floor under the zero. A restart resets these counts, so counting_since is what
                       says whether a zero covers weeks or ninety seconds. */
                    counting_since = alertReads.CountingSinceUtc,
                    finding = alertReadFinding,
                    note = AlertReadFailureCounter.WindowNote
                },
                sweep_pressure = new
                {
                    busy_ms_per_minute = Math.Round(pressure.BusyMsPerMinute, 0),
                    busy_percent = Math.Round(pressure.BusyPercent, 1),
                    verdict = pressure.Verdict,
                    /* #2446: the second dimension, and deliberately NOT folded into verdict. verdict
                       answers "does sustained demand fit the cadence on average"; this answers "does one
                       scheduled body fit at all". They disagree exactly when an infrequent heavy collector
                       owns most of a single sweep — which an amortized number cannot see by construction,
                       since dividing by that collector's own long cadence is what makes it small. Its own
                       vocabulary (FITS / BODY_OVERRUN) so it can never be read as a fourth verdict band,
                       and its own field so a fleet scan can filter on it. */
                    peak_cycle_ms = Math.Round(pressure.PeakCycleMs, 0),
                    peak_cycle_percent = Math.Round(pressure.PeakCyclePercent, 1),
                    peak_cycle_risk = pressure.PeakCycleRisk,
                    peak_collector = peakCollector,
                    peak_cycle_note = string.IsNullOrEmpty(peakCycleNote) ? null : peakCycleNote,
                    heaviest_collectors = heaviest,
                    note = pressure.Verdict switch
                    {
                        SweepPressureClassifier.Saturated =>
                            "The collection body cannot finish inside its cadence: relaunches are skipped every cycle and this server collects at a multiple of its configured interval, while each collector above correctly reads healthy from its own seat. The lever is capacity or placement (lighter or fewer scheduled collectors, a longer cadence, or a collector closer to the target), not collector repair.",
                        SweepPressureClassifier.AtRisk =>
                            "The collection body's average demand is close to its cadence; variance will intermittently push it over, skipping relaunches and stretching the delivered interval.",
                        _ => null
                    }
                },
                /* #3017: the windows, said once for the whole array rather than repeated on all ~41
                   rows. It names the window rows_stored/runs_with_rows were counted over — the same
                   fixed trailing seven days as total_runs and the durations, out of one aggregate, so
                   cost and output on a row can never describe different runs — and DISCLAIMS the one it
                   did not read: get_collector_cost's hourly series over the caller's own days_back and
                   across every server at once. That disclaiming is #3027's discipline one level down; a
                   sentence claiming both windows were read here would be the same defect it was written
                   to avoid. It also says outright that rows are what a run STORED and never what the
                   monitored engine counted, because nothing on this surface measures the second. */
                output_note = CollectorHealthClassifier.OutputWindowNote,
                collectors = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_collection_health", ex);
        }
    }

    [McpServerTool(Name = "get_server_properties"), Description("Gets SQL Server instance properties: edition, version, CPU count, physical memory, socket/core topology, HADR status, and clustering. Use for capacity planning and edition-aware recommendations.")]
    public static async Task<string> GetServerProperties(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var row = await DarlingDataReader.GetLatestServerPropertiesAsync(postgres, resolved.ServerId);
            if (row == null)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "server_properties")
                    ?? McpHelpers.Status("unavailable", "No server properties available. The properties collector may not have run yet.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                collection_time = row.CollectionTime.ToString("o"),
                edition = row.Edition,
                engine_edition = row.EngineEdition,
                product_version = row.ProductVersion,
                product_level = row.ProductLevel,
                product_update_level = string.IsNullOrEmpty(row.ProductUpdateLevel) ? null : row.ProductUpdateLevel,
                cpu_count = row.CpuCount,
                hyperthread_ratio = row.HyperthreadRatio,
                socket_count = row.SocketCount,
                cores_per_socket = row.CoresPerSocket,
                physical_memory_mb = row.PhysicalMemoryMb,
                is_hadr_enabled = row.IsHadrEnabled,
                is_clustered = row.IsClustered,
                enterprise_features = string.IsNullOrEmpty(row.EnterpriseFeatures) ? null : row.EnterpriseFeatures,
                service_objective = string.IsNullOrEmpty(row.ServiceObjective) ? null : row.ServiceObjective
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_properties", ex);
        }
    }

    /* ─────────────────────────── list_servers helpers ─────────────────────────── */

    /// <summary>
    /// The freshness-derived status this tool reports: Fresh → Online, Stale → Warning, long-dead → Offline,
    /// never-collected → AwaitingFirstCollection (the service hasn't reached the server yet — a bootstrap
    /// state, not an outage). Both instants are UTC.
    ///
    /// <para>It used to classify freshness itself, against its OWN copies of the 2-minute and 15-minute
    /// thresholds — so <c>ServerHealthThresholds</c> could move and <c>list_servers</c> would silently keep
    /// answering with the old numbers. It now shares the ladder with every other status surface (#2473). What
    /// it does NOT share is the vocabulary: <see cref="ServerCollectionStatusRules.McpToken"/> spells the
    /// never-collected state as one word because that value was published to MCP clients, and a status value
    /// a client keys on is a consumer API.</para>
    /// </summary>
    private static string FreshnessStatus(DateTime? lastCollectionUtc, DateTime nowUtc) =>
        ServerCollectionStatusRules
            .FromFreshness(ServerHealthClassifier.ClassifyFreshness(lastCollectionUtc, nowUtc))
            .McpToken();

    /// <summary>Product-name label for a sql_major_version (the viewer's <c>SqlVersionLabel</c>); 2016+ is
    /// what the product supports, older/unknown majors fall back to a bare version tag, null to empty.</summary>
    private static string SqlVersionLabel(int? sqlMajorVersion) => sqlMajorVersion switch
    {
        null => "",
        11 => "SQL Server 2012",
        12 => "SQL Server 2014",
        13 => "SQL Server 2016",
        14 => "SQL Server 2017",
        15 => "SQL Server 2019",
        16 => "SQL Server 2022",
        17 => "SQL Server 2025",
        _ => $"SQL Server v{sqlMajorVersion}",
    };

    [McpServerTool(Name = "get_collection_log"), Description("Gets the RAW per-run collection log for a server, newest first: one row per collector run with its total duration, the part spent querying the monitored server, the part spent writing to the store, rows collected, status and any error. get_collection_health rolls seven days of these into a per-collector verdict; this is the underlying runs, which is what you need when the rollup says healthy and collection still looks wrong, or when you want to see what a collector was doing during a specific incident window. Also carries the phase decomposition where the run recorded one, as nested blocks that are null when the run took a path that does not report them — and a row carries at most ONE family. Server-scoped collectors fill sql_phases (open_ms, drain_ms, other_ms which is derived, watermark_ms) and drain (rows_read, bytes_read, last_read_ms, target_session_id). Per-database collectors that perform a deferred plan or statement-text fetch instead fill plan_fetch and/or text_fetch, each carrying probe_ms, target_ms, write_ms, ids_attempted and probe_ids summed across that run's databases. sweep_peer_max_ms is flat and present on every row: it is the slowest peer collector in the same sweep, the denominator for asking whether a slow run was slow alone or the whole sweep was. A null block means the run took the other path, not that the phase was free — most runs perform no deferred fetch at all. Divide target_ms by ids_attempted for the per-id target cost, probe_ms by probe_ids for the per-reference probe cost.")]
    public static async Task<string> GetCollectionLog(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, newest first. Default 200.")] int limit = 200,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        /* The shared row-cap contract every sibling read uses: rejects out of range rather than
           silently clamping, so a caller asking for 5000 is told no instead of quietly given 1000. */
        var invalidLimit = McpHelpers.ValidateTop(limit);
        if (invalidLimit != null) return invalidLimit;

        /* ResolveAsOf here, deliberately NOT ValidateWindow. These three reads have never capped
           hours_back -- they Math.Abs() it and window on the result -- so routing them through the
           shared validator would impose the 168-hour ceiling every other read carries, and take reach
           away from exactly the read whose premise is looking FURTHER back than the default. The anchor
           is validated because it is new; the span keeps the behaviour callers already have. */
        var anchorError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-Math.Abs(hours_back));

            /* Over-fetch by one so truncation is OBSERVED rather than inferred. Comparing count to the
               cap cannot tell a window holding exactly `limit` runs from one holding more, and this
               read's whole premise is that the cap announces itself instead of being guessed at. */
            var rows = await DarlingDataReader.GetCollectionLogAsync(
                postgres, resolved.ServerId, start, end, limit + 1);
            var truncated = rows.Count > limit;
            if (truncated) rows = rows.Take(limit).ToList();

            if (rows.Count == 0)
            {
                /*
                    Zero rows is two completely different facts and they need different answers.
                    A server that has collected before and simply did nothing in THIS window is a
                    true negative -- the caller narrowed to a quiet period, and widening the window
                    is the move. A server with no log rows at all has never collected, which is a
                    fault, and telling that caller "nothing in the last 24 hours" would send them
                    off widening a window that will never fill. So we ask which one it is rather
                    than emitting one sentence that is true of both.
                */
                var everCollected = await DarlingDataReader.HasAnyCollectionLogAsync(postgres, resolved.ServerId);
                return everCollected
                    ? McpHelpers.Status(
                        "empty",
                        $"No collector runs recorded for {resolved.ServerName} in the last {Math.Abs(hours_back)} hour(s). This server HAS collected before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent runs.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No collector runs have EVER been recorded for {resolved.ServerName}. This is not an empty window — collection has not run at all for this server. Check that the service is running and that the server is enabled for collection; get_collection_health will be equally empty until it does.");
            }

            var result = rows.Select(r => new
            {
                collector = r.CollectorName,
                collection_time = r.CollectionTime.ToString("o"),
                duration_ms = r.DurationMs is null ? (double?)null : Math.Round(r.DurationMs.Value, 0),
                /*
                    The split matters more than the total. A collector slow because the monitored
                    server is slow needs work on that server; one slow because the store is slow
                    needs work here. The total alone cannot tell those apart, and it is the
                    question people actually ask of this log.
                */
                sql_duration_ms = r.SqlDurationMs is null ? (double?)null : Math.Round(r.SqlDurationMs.Value, 0),
                store_duration_ms = r.StoreDurationMs is null ? (double?)null : Math.Round(r.StoreDurationMs.Value, 0),
                rows_collected = r.RowsCollected,
                status = r.Status,
                error_message = r.ErrorMessage,
                /*
                    The phase decomposition, emitted here rather than only SELECTed because persisting a
                    column nothing reports is half a feature. V108 and V109 both widened CollectionLogSql
                    and CollectionLogEntry and stopped: the eight columns below were read off the row into
                    the record and dropped on the floor by this projection, so the only way to them was psql
                    on the monitoring box -- the exact reachability problem V108 was filed to fix. Found
                    while adding V110's ten (#2860) and fixed in the same pass, because a projection
                    carrying the fetch split but not the open/drain one would read as "the server-scoped
                    split is not stored".

                    GROUPED into blocks that collapse to a single null, rather than nineteen flat fields.
                    That is a measurement, not a preference: flat, a 200-row read went from 41,221 to
                    138,481 characters -- 3.36x, ~97KB of it the literal text "null" -- because a row
                    carries at most ONE of these blocks and most carry none. The nesting is not arbitrary
                    either: it is exactly the mutual exclusivity, which was previously only a comment. The
                    open/drain figures come from the SERVER-scoped path, the fetch figures from the
                    ENUMERATED one which never sets V108's measured flag and is the only path performing a
                    deferred fetch, so a null block means "this run took the other path" and NULL
                    throughout is the ordinary case rather than a fault.

                    sweep_peer_max_ms deliberately stays FLAT: V109 records it on every row on purpose,
                    because a ratio needs a denominator drawn from ordinary bodies rather than only from
                    failures, so it is not part of any conditional block and grouping it would imply it
                    shares their fate.
                */
                sql_phases = r.SqlOpenMs is null && r.SqlDrainMs is null && r.WatermarkMs is null ? null : new
                {
                    open_ms = r.SqlOpenMs is null ? (double?)null : Math.Round(r.SqlOpenMs.Value, 0),
                    drain_ms = r.SqlDrainMs is null ? (double?)null : Math.Round(r.SqlDrainMs.Value, 0),
                    /* Derived, not stored -- V108 keeps no other_ms column so the terms cannot drift from
                       the parent they decompose. Reported because a large residual is itself the finding:
                       it means the cost sits in our own code between the phases, in neither database. */
                    other_ms = r.SqlOtherMs is null ? (double?)null : Math.Round(r.SqlOtherMs.Value, 0),
                    watermark_ms = r.WatermarkMs is null ? (double?)null : Math.Round(r.WatermarkMs.Value, 0),
                },
                /* V109: what the drain DELIVERED, as against what the run STORED. rows_collected above is 0
                   for every abandoned cycle by definition, so it could never separate a target that sent
                   nothing from one that sent rows and went silent. */
                drain = r.DrainRowsRead is null && r.DrainBytesRead is null
                        && r.DrainLastReadMs is null && r.TargetSessionId is null ? null : new
                {
                    rows_read = r.DrainRowsRead,
                    bytes_read = r.DrainBytesRead,
                    last_read_ms = r.DrainLastReadMs is null ? (double?)null : Math.Round(r.DrainLastReadMs.Value, 0),
                    target_session_id = r.TargetSessionId,
                },
                sweep_peer_max_ms = r.SweepPeerMaxMs is null ? (double?)null : Math.Round(r.SweepPeerMaxMs.Value, 0),
                /*
                    V110 (#2860): the deferred plan/text fetch split, SUMMED across the run's fan-out. The
                    store probe is the largest single term on this fleet -- 55.4% of plan_fetch and 80.6% of
                    text_fetch measured over 38h -- which inverts the shape the sub-split was originally
                    written against, so getting it in front of a reader is the whole point.

                    Two blocks rather than one, because the halves are independently nullable: a run that
                    fetched text but no plans has one block and not the other, matching how the log line
                    emits its two sub-lines. Emitted raw rather than pre-divided into ms-per-id -- the rates
                    are what the counts are for, but there are three useful ones over these five figures (ms
                    per attempted id, ms per probed reference, and attempted / probed, which is the #2902
                    backlog signal), and blessing one here would hide the other two.
                */
                plan_fetch = r.PlanFetchProbeMs is null ? null : new
                {
                    probe_ms = Math.Round(r.PlanFetchProbeMs.Value, 0),
                    target_ms = r.PlanFetchTargetMs is null ? (double?)null : Math.Round(r.PlanFetchTargetMs.Value, 0),
                    write_ms = r.PlanFetchWriteMs is null ? (double?)null : Math.Round(r.PlanFetchWriteMs.Value, 0),
                    ids_attempted = r.PlanFetchIdsAttempted,
                    probe_ids = r.PlanFetchProbeIds,
                },
                text_fetch = r.TextFetchProbeMs is null ? null : new
                {
                    probe_ms = Math.Round(r.TextFetchProbeMs.Value, 0),
                    target_ms = r.TextFetchTargetMs is null ? (double?)null : Math.Round(r.TextFetchTargetMs.Value, 0),
                    write_ms = r.TextFetchWriteMs is null ? (double?)null : Math.Round(r.TextFetchWriteMs.Value, 0),
                    ids_attempted = r.TextFetchIdsAttempted,
                    probe_ids = r.TextFetchProbeIds,
                },
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back = Math.Abs(hours_back),
                run_count = rows.Count,
                /* Observed by the over-fetch above, not inferred from the row count. */
                truncated,
                runs = result,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_collection_log", ex);
        }
    }

    [McpServerTool(Name = "get_current_waits_trend"), Description("Gets the two Current Waits series over time for a server: waiting-task total wait duration per wait type per collection, and blocked-session counts per database per collection. get_waiting_tasks answers 'what is waiting right now' and can never say whether it is worse than an hour ago — this is that question. Use it to tell a server that is always mildly blocked from one that just started, and to see which database owns the blocking over the window rather than in one snapshot.")]
    public static async Task<string> GetCurrentWaitsTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4,
        [Description("Limit the blocked-session series to one database. Omit for all databases.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        /* ResolveAsOf here, deliberately NOT ValidateWindow. These three reads have never capped
           hours_back -- they Math.Abs() it and window on the result -- so routing them through the
           shared validator would impose the 168-hour ceiling every other read carries, and take reach
           away from exactly the read whose premise is looking FURTHER back than the default. The anchor
           is validated because it is new; the span keeps the behaviour callers already have. */
        var anchorError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-Math.Abs(hours_back));

            var waits = await DarlingDataReader.GetWaitingTaskTrendAsync(postgres, resolved.ServerId, start, end);
            var blocked = await DarlingDataReader.GetBlockedSessionTrendAsync(
                postgres, resolved.ServerId, start, end, database_name);

            if (waits.Count == 0 && blocked.Count == 0)
            {
                /*
                    Both series empty is two facts again, and here the wrong one is actively reassuring:
                    "nothing was waiting" reads as an all-clear, while the truth may be that the
                    waiting_tasks collector never ran. A caller told all-clear stops looking.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "waiting_tasks");
                if (gated != null)
                {
                    return gated;
                }

                var everCollected = await DarlingDataReader.HasAnyWaitingTaskSampleAsync(postgres, resolved.ServerId);
                return everCollected
                    ? McpHelpers.Status(
                        "empty",
                        $"Nothing was waiting on {resolved.ServerName} in the last {Math.Abs(hours_back)} hour(s). The collector HAS sampled this server, so this is a genuine all-clear for the window rather than missing data.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No waiting-task samples have EVER been recorded for {resolved.ServerName}, so this is NOT an all-clear — there is nothing to read. Check that collection is running for this server before concluding it was quiet.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back = Math.Abs(hours_back),
                database_name,
                /*
                    Two series in one payload because they are read together: a wait-type spike with no
                    blocked sessions is a resource wait, and the same spike WITH them is contention. Split
                    across two tools a caller can fetch one and draw the wrong conclusion.
                */
                waiting_tasks = waits.Select(w => new
                {
                    collection_time = w.CollectionTime.ToString("o"),
                    wait_type = w.WaitType,
                    total_wait_ms = w.TotalWaitMs,
                }),
                blocked_sessions = blocked.Select(b => new
                {
                    collection_time = b.CollectionTime.ToString("o"),
                    database_name = b.DatabaseName,
                    blocked_count = b.BlockedCount,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_current_waits_trend", ex);
        }
    }

    [McpServerTool(Name = "get_blocking_stats"), Description("Gets blocking SEVERITY over time for a server: per-minute blocking duration (event count, total, max and average wait) and per-minute deadlock severity (victim count plus total, max and average wait across every process in the graphs). get_blocking_trend and get_deadlock_trend count incidents; this is how BAD they were. Ten one-second blocks and one ten-minute block are the same count and are not the same problem, which is the distinction this read exists to make.")]
    public static async Task<string> GetBlockingStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        /* ResolveAsOf here, deliberately NOT ValidateWindow. These three reads have never capped
           hours_back -- they Math.Abs() it and window on the result -- so routing them through the
           shared validator would impose the 168-hour ceiling every other read carries, and take reach
           away from exactly the read whose premise is looking FURTHER back than the default. The anchor
           is validated because it is new; the span keeps the behaviour callers already have. */
        var anchorError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-Math.Abs(hours_back));

            var blocking = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, resolved.ServerId, start, end);

            /* Parsed and bucketed by the shared aggregator rather than re-derived here: a second copy of
               "what counts as a victim" is how two surfaces end up disagreeing about one deadlock. */
            var graphs = await DarlingDataReader.GetDeadlockGraphsAsync(postgres, resolved.ServerId, start, end);
            var deadlocks = DeadlockSeverityAggregator.Aggregate(graphs);

            if (blocking.Count == 0 && deadlocks.Count == 0)
            {
                /*
                    The denominator is whether we LOOKED, not whether we ever FOUND anything. Blocking and
                    deadlocks are edge tables: a server collected perfectly for months that simply never
                    blocked has no rows at all, so asking "was an event ever captured" answers no and
                    reports a healthy server as uncollected -- the reassuring-answer failure inverted, and
                    a false alarm sends someone to fix collection that is working.

                    So this asks collection_log for a SUCCESSFUL run of either capture path. Both are
                    checked because either can be off alone, and the deadlock collector is separate from
                    both -- the verdict covers its series too.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "blocked_process_report");
                if (gated != null)
                {
                    return gated;
                }

                var everRan =
                    await DarlingBlockingTrendReader.HasAnyBlockingCollectorRunAsync(postgres, resolved.ServerId)
                    || await DarlingBlockingTrendReader.HasAnyDeadlockCollectorRunAsync(postgres, resolved.ServerId);
                return everRan
                    ? McpHelpers.Status(
                        "empty",
                        $"No blocking or deadlocks recorded for {resolved.ServerName} in the last {Math.Abs(hours_back)} hour(s). The blocking collectors HAVE run successfully for this server, so the window is genuinely clear rather than blind.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"The blocking collectors have NEVER run successfully for {resolved.ServerName}, so this is NOT a clean bill of health — nothing looked. Blocked-process reports need the XE session running, or the DMV blocking snapshot collector enabled; check those before concluding this server does not block.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back = Math.Abs(hours_back),
                /*
                    Severity, not counts. get_blocking_trend already answers how OFTEN; ten one-second
                    blocks and one ten-minute block share a count and are different problems.
                */
                blocking_duration = blocking.Select(b => new
                {
                    time = b.Time.ToString("o"),
                    event_count = b.EventCount,
                    total_duration_ms = b.TotalDurationMs,
                    max_duration_ms = b.MaxDurationMs,
                    avg_duration_ms = Math.Round(b.AvgDurationMs, 0),
                }),
                deadlock_severity = deadlocks.Select(d => new
                {
                    time = d.Time.ToString("o"),
                    victim_count = d.VictimCount,
                    /* Every process's wait, not just the victims' -- the Dashboard analyzer's semantics. */
                    total_wait_ms = d.TotalWaitMs,
                    max_wait_ms = d.MaxWaitMs,
                    avg_wait_ms = Math.Round(d.AvgWaitMs, 0),
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocking_stats", ex);
        }
    }
}
