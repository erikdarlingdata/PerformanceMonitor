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
        [Description("Hours of history. Default 4.")] int hours_back = 4)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingDataReader.GetCpuUtilizationAsync(postgres, resolved.ServerId, DateTime.UtcNow.AddHours(-hours_back));
            if (rows.Count == 0)
                return McpHelpers.Status("unavailable", "No CPU utilization data available.");

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
        [Description("Maximum rows to return. Default 20.")] int limit = 20)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingDataReader.GetWaitStatsAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return McpHelpers.Status("unavailable", "No wait stats data available for the specified time range.");

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
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var types = await DarlingDataReader.GetDistinctWaitTypesAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

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
            var points = await DarlingDataReader.GetWaitTrendAsync(postgres, resolved.ServerId, wait_type, start, now);
            if (points.Count == 0)
            {
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
                return McpHelpers.Status("unavailable", "No memory stats available.");

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
                return McpHelpers.Status("unavailable", "No file I/O stats available.");

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
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await DarlingDataReader.GetTempDbTrendAsync(postgres, resolved.ServerId, DateTime.UtcNow.AddHours(-hours_back));
            if (rows.Count == 0)
                return McpHelpers.Status("unavailable", "No TempDB data available.");

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
                return McpHelpers.Status("unavailable", "No perfmon stats available.");

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
        [Description("Grouping. 'query_hash' (default) is one row per (database, query_hash, host_object). 'host_object' rolls every statement of a hosting procedure/function into ONE row — use it when dynamic SQL built with per-value literals fragments one logical statement across many query_hash values, which makes top-N-by-hash structurally unable to surface it (measured at 21 fragments for one statement, whose combined CPU was the largest on the instance while no single fragment ranked). Ad-hoc statements have no host object and stay grouped per hash in both modes. distinct_query_hashes reports how many hashes a row rolled up.")] string group_by = "query_hash")
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

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, top, database_name, rollUpByHostObject: rollUp);
            if (rows.Count == 0)
                return McpHelpers.Status("unavailable", "No query stats available for the specified time range.");

            var filtered = rows
                .Where(r => !(parallel_only || min_dop > 1) || (r.MaxDop > 1 && r.MaxDop >= (min_dop > 1 ? min_dop : 2)))
                .ToList();

            /* #2320: what fraction of the box's measured CPU the RETURNED rows explain — numerator is
               the caller-visible ranking (post top-N, post filters), denominator is measured, and the
               ratio is omitted rather than invented when a denominator piece is missing. */
            var cpuAggregate = await DarlingDataReader.GetCpuWindowAggregateAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            var properties = await DarlingDataReader.GetLatestServerPropertiesAsync(postgres, resolved.ServerId);
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
        [Description("Filter to a specific database.")] string? database_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingDataReader.GetTopProceduresByCpuAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, top, database_name);
            if (rows.Count == 0)
                return McpHelpers.Status(
                    "unavailable",
                    "No procedure stats available. Delta-based collection requires at least two collection cycles (~30 minutes) to produce non-zero values.");

            /* #2320: same attributed-CPU disclosure as the queries tool — one shared computation. */
            var cpuAggregate = await DarlingDataReader.GetCpuWindowAggregateAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            var properties = await DarlingDataReader.GetLatestServerPropertiesAsync(postgres, resolved.ServerId);
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
        [Description("Filter to a specific database.")] string? database_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;

        try
        {
            var now = DateTime.UtcNow;
            var rows = await DarlingDataReader.GetQueryStoreTopAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, top, database_name);
            if (rows.Count == 0)
                return McpHelpers.Status("unavailable", "No Query Store data available. Query Store may not be enabled on target databases.");

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
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_top", ex);
        }
    }

    /* ═══════════════════════════ discovery / health ═══════════════════════════ */

    [McpServerTool(Name = "list_servers"), Description("Lists all monitored SQL Server instances with their collection freshness status and last collection time. Use this first to see available servers before calling other tools. The service has no live connection to the monitored servers, so status is derived from how recently each server was collected (Online = fresh, Warning = stale, Offline = no recent collection).")]
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

            if (servers.Count == 0)
                return "No servers are registered yet. The service registers each monitored server on its first successful connection.";

            var now = DateTime.UtcNow;
            var result = servers.Select(s => new
            {
                server_name = s.ServerName,
                display_name = string.IsNullOrEmpty(s.DisplayName) ? s.ServerName : s.DisplayName,
                sql_version = SqlVersionLabel(s.SqlMajorVersion),
                status = FreshnessStatus(s.LastCollection, now),
                read_only = s.ServerName.EndsWith(":RO", StringComparison.Ordinal),
                last_collection = s.LastCollection?.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server_count = servers.Count,
                servers = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("list_servers", ex);
        }
    }

    [McpServerTool(Name = "get_collection_health"), Description("Shows the health status of all data collectors for a server — whether they're running successfully, failing, or stale. Check this before investigating data to ensure collectors are working properly. Each row also carries last_note/note_count: what a NON-failing run reported, e.g. an enumeration that came back with 0 items. note_count equal to total_runs means the collector has been collecting nothing all window — not a fault (the target may be legitimately empty), but the reason a HEALTHY collector can still have no data. target_has_user_databases tells those two apart: true means the target DID have user databases in the same window, so an all-window empty enumeration is worth investigating (a login that cannot enter them, an exclusion filter that matched everything); false means either no user databases or no inventory to go on. The sweep_pressure block is the server-level roll-up: it compares the collectors' combined execution demand (average duration amortized by cadence) against the minute the fastest cadence holds. SATURATED means the collection body cannot fit inside its cadence, so relaunches are skipped and the server collects at a multiple of its configured interval while every collector still reads healthy — heaviest_collectors names where that budget goes.")]
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
                failure_rate_pct = Math.Round(r.FailureRatePercent, 1),
                avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
                last_success = r.LastSuccessTime?.ToString("o"),
                last_error = r.LastError,
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
                    r.LastNote, r.NoteCount, r.TotalRuns, r.CollectorName, r.TargetHasUserDatabases)
            });

            /* #2296: the roll-up that makes half-rate collection visible. Every collector on a saturated
               server reads HEALTHY — from each one's own seat nothing is wrong — so the condition only
               existed as a service-log warning ("collection body has not completed … skipping relaunch").
               The verdict compares the collectors' combined execution demand (average duration amortized
               by cadence) against the minute the fastest cadence holds; heaviest_collectors names where
               the budget goes, which is the actionable half of the answer. */
            var pressure = SweepPressureClassifier.Compute(
                rows.Select(r => (r.CollectorName, r.AvgDurationMs, r.FrequencyMinutes)));
            var heaviest = rows
                .Where(r => r.FrequencyMinutes > 0 && r.AvgDurationMs > 0)
                .OrderByDescending(r => r.AvgDurationMs / r.FrequencyMinutes)
                .Take(3)
                .Select(r => new
                {
                    collector = r.CollectorName,
                    avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
                    frequency_minutes = r.FrequencyMinutes
                });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                sweep_pressure = new
                {
                    busy_ms_per_minute = Math.Round(pressure.BusyMsPerMinute, 0),
                    busy_percent = Math.Round(pressure.BusyPercent, 1),
                    verdict = pressure.Verdict,
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
                return McpHelpers.Status("unavailable", "No server properties available. The properties collector may not have run yet.");

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

    /// <summary>Older than twice the ~1-minute collector cadence = the collection has visibly lagged.</summary>
    private static readonly TimeSpan StaleThreshold = TimeSpan.FromMinutes(2);

    /// <summary>Older than this (or no collection at all) = the server is treated as Offline.</summary>
    private static readonly TimeSpan OfflineThreshold = TimeSpan.FromMinutes(15);

    /// <summary>
    /// The freshness-derived status the headless viewer's cards use (<c>ServerSummaryItem.ClassifyFreshness</c>):
    /// Fresh → Online, Stale → Warning, long-dead → Offline, never-collected → AwaitingFirstCollection
    /// (the service hasn't reached the server yet — a bootstrap state, not an outage; additive status
    /// value, existing values unchanged). Both instants are UTC.
    /// </summary>
    private static string FreshnessStatus(DateTime? lastCollectionUtc, DateTime nowUtc)
    {
        if (!lastCollectionUtc.HasValue) return "AwaitingFirstCollection";
        var age = nowUtc - lastCollectionUtc.Value;
        if (age > OfflineThreshold) return "Offline";
        if (age > StaleThreshold) return "Warning";
        return "Online";
    }

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
}
