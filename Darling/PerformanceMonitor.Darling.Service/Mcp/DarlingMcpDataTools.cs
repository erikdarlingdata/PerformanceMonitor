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
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Analysis;

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

    [McpServerTool(Name = "get_cpu_utilization"), Description("Gets CPU utilization over time in time buckets: SQL Server CPU %, other process CPU %, total CPU % and idle %, with each bucket's busiest sample. Use this to identify CPU pressure periods, then use get_top_queries_by_cpu to find the culprit queries.")]
    public static Task<string> GetCpuUtilization(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        CancellationToken cancellationToken = default) =>
        GetCpuUtilization(postgres, server_name, hours_back, as_of, bucket_minutes, TrendBudget.Mcp(TrendBuckets.CpuMaxPoints), cancellationToken);

    /// <summary>
    /// get_cpu_utilization under an explicit <paramref name="budget"/> (#3960): the MCP tool passes its own, the web
    /// viewer's <c>/api/read</c> mirror <see cref="TrendBudget.Chart"/>. The samples are bucketed in SQL — the tool
    /// used to read every sample of the window and average them to the minute here, which at a week and an Azure SQL
    /// DB source's 15-second cadence was 40,000 rows for 10,000 points.
    /// </summary>
    internal static async Task<string> GetCpuUtilization(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, int? bucket_minutes, TrendBudget budget,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
        if (bucketError != null) return bucketError;

        try
        {
            var points = await DarlingDataReader.GetCpuBucketsAsync(postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, bucketMinutes, cancellationToken);
            if (points.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "cpu_utilization", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No CPU utilization data available.");

            /* #3653 A15/A16: the source-cadence sentence both SKUs publish is TrendPayloads.CpuCadenceNote, the one
               place it is spelled (the ring buffer writes one record a minute on-prem, MI and RDS; Azure SQL DB's
               sys.dm_db_resource_stats one every 15 seconds; samples_in_bucket is the measured count). */
            return TrendPayloads.CpuUtilization(resolved.ServerName, hours_back, points, bucketMinutes, bucket_minutes is not null, budget.AutoPoints);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_cpu_utilization", ex);
        }
    }

    [McpServerTool(Name = "get_wait_stats"), Description("Gets the top SQL Server wait types over a time period ending at as_of, heaviest total wait first. Bounded by limit: wait_types_returned is how many you got; truncated means the window held more. The page holds only the heaviest waits — a sum over the page is not a sum over the server. <<GUIDE>> Gets the top SQL Server wait types aggregated over a time period, heaviest total wait first. Wait stats reveal what SQL Server spends time waiting on — high signal waits indicate CPU pressure, high resource waits indicate I/O or lock contention. Use this first to identify the dominant wait category, then drill into specific tools based on the wait type. THE PAGE IS BOUNDED BY limit: wait_types_returned is how many wait types you got and truncated says the window observed more than limit — the rows you have are the heaviest, and the ones past the cap are lighter, but a sum over the page is a sum over the page rather than over the server.")]
    public static async Task<string> GetWaitStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum wait types to return, heaviest first. Default 20. This is what bounds the page — read truncated to know whether the window observed more.")] int limit = 20,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            /* #3541 A3: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal. The reader's LIMIT 50 sat under a limit the tool accepts up to 1,000. */
            var rows = await DarlingDataReader.GetWaitStatsAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "wait_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No wait stats data available for the specified time range.");

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            var result = page.Select(r =>
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
                /* #3541 A3: the page described as a page. No time bounds here — the rows are per-type
                   aggregates over the whole window, so there is no page reach to report, only a cap. */
                wait_types_returned = page.Count,
                truncated,
                order = "total_wait_time_ms_desc",
                waits = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_wait_stats", ex);
        }
    }

    [McpServerTool(Name = "get_wait_types"), Description("Lists the distinct wait types observed on a server in the given time period, heaviest first. Useful for discovering which exact wait type to drill into with get_wait_trend.")]
    public static async Task<string> GetWaitTypes(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var types = await DarlingDataReader.GetDistinctWaitTypesAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, cancellationToken);

            if (types.Count == 0)
            {
                /*
                    An empty list said nothing about which nothing this is. A server that collected and was
                    quiet in THIS window wants the window widened; a server nothing has been stored for
                    wants somebody to look at collection, and widening will never fill it. Probed only here,
                    against the SAME source the read walks.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "wait_stats", cancellationToken);
                if (gated != null)
                {
                    return gated;
                }

                return await DarlingDataReader.HasAnyWaitStatAsync(postgres, resolved.ServerId, cancellationToken)
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_wait_types", ex);
        }
    }

    [McpServerTool(Name = "get_wait_trend"), Description("Gets one wait type's wait time per second over time, in time buckets. Use get_wait_stats first to discover the dominant wait types." + BaselineDiscontinuities.DescriptionSentence)]
    public static Task<string> GetWaitTrend(
        NpgsqlDataSource postgres,
        [Description("The exact wait type name, e.g. CXPACKET, PAGEIOLATCH_SH.")] string wait_type,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        CancellationToken cancellationToken = default) =>
        GetWaitTrend(postgres, wait_type, server_name, hours_back, as_of, bucket_minutes, TrendBudget.Mcp(TrendBuckets.WaitMaxPoints), cancellationToken);

    /// <summary>get_wait_trend under an explicit <paramref name="budget"/> (#3960): the MCP tool passes its own, the
    /// web viewer's <c>/api/read</c> mirror <see cref="TrendBudget.Chart"/>.</summary>
    internal static async Task<string> GetWaitTrend(
        NpgsqlDataSource postgres, string wait_type, string? server_name, int hours_back, string? as_of, int? bucket_minutes, TrendBudget budget,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
        if (bucketError != null) return bucketError;

        try
        {
            var now = windowEnd;
            var start = now.AddHours(-hours_back);
            var points = await DarlingDataReader.GetWaitBucketsAsync(postgres, resolved.ServerId, wait_type, start, now, bucketMinutes, cancellationToken);
            if (points.Count == 0)
            {
                /* The engine question comes BEFORE the distinct-values probe, not after it. Both are on
                   the miss path, so either order keeps the property that matters — but a permanently gated
                   engine takes this branch on every call, forever, and the probe below could never tell it
                   anything. Asking first makes that case one query instead of two. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "wait_stats", cancellationToken);
                if (gated != null)
                {
                    return gated;
                }

                /* Distinguish "unknown wait type here" from "nothing collected at all", handing back the
                   ones that do have data — Lite's get_wait_trend miss vocabulary. */
                var collected = await DarlingDataReader.GetDistinctWaitTypesAsync(postgres, resolved.ServerId, start, now, cancellationToken);
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

            /* #3653 A5: wait_stats is the FIRST identity-epoch carrier (#3705), so this is the trend whose
               step a restart or failover most directly manufactures; the markers ride the payload's trailing
               key exactly as on the DarlingMcpTrendTools family (see that class's remarks), over the same
               window as the points. */
            var discontinuities = await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, start, now, cancellationToken);

            return TrendPayloads.WaitTrend(
                resolved.ServerName, wait_type, hours_back, points, bucketMinutes, bucket_minutes is not null,
                budget.AutoPoints, BaselineDiscontinuities.ToPayload(discontinuities));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_wait_trend", ex);
        }
    }

    [McpServerTool(Name = "get_memory_stats"), Description("Gets the latest memory statistics snapshot: physical memory, buffer pool size, plan cache size, memory utilization %, and SQL Server memory model. Use this for a quick memory health check; use get_memory_clerks to see detailed breakdown by component. LATEST IS A TIME: this reads one snapshot, not a window, and captured_at is the instant that snapshot was collected - read it before treating any figure as current, because the newest row a store holds can be minutes or days old.")]
    public static async Task<string> GetMemoryStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var stats = await DarlingDataReader.GetLatestMemoryStatsAsync(postgres, resolved.ServerId, cancellationToken);
            if (stats == null)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No memory stats available.");

            var utilization = stats.TotalPhysicalMemoryMb > 0
                ? (stats.TotalPhysicalMemoryMb - stats.AvailablePhysicalMemoryMb) / stats.TotalPhysicalMemoryMb * 100
                : 0;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3541 A10: the one stamp every latest-snapshot read publishes, under the one name. */
                captured_at = stats.CollectionTime.ToString("o"),
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_memory_stats", ex);
        }
    }

    [McpServerTool(Name = "get_memory_clerks"), Description("Gets the top memory consumers by memory clerk type — shows which SQL Server components are using the most memory. LATEST IS A TIME: this reads the newest clerk snapshot, not a window, and captured_at is the instant it was collected.")]
    public static async Task<string> GetMemoryClerks(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var snapshot = await DarlingDataReader.GetLatestMemoryClerksAsync(postgres, resolved.ServerId, cancellationToken);

            if (snapshot.IsEmpty)
                /*
                    ONE branch here, deliberately, and it is the reason this read gets no existence probe.
                    The read is "every clerk at MAX(collection_time)", so zero rows back is logically the
                    same statement as zero rows in the table — any probe against that source would agree
                    with the read by construction and tell the caller nothing it did not already have. What
                    the caller does need is to be told that an empty clerk list is NEVER a quiet period,
                    because on a live SQL Server it cannot be: the DMV always has clerks.
                */
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_clerks", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No memory-clerk snapshot is available for {resolved.ServerName}. This read returns the LATEST snapshot rather than a window, so an empty result is never a quiet period — a live SQL Server always has memory clerks. It means nothing the memory_clerks collector stored is still retained, either because it has not run for this server or because its rows have aged out. Check get_collection_health and get_collection_log for the memory_clerks collector.");

            var result = snapshot.Rows.Select(r => new
            {
                clerk_type = r.ClerkType,
                memory_mb = Math.Round(r.MemoryMb, 2)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                captured_at = snapshot.CapturedAt!.Value.ToString("o"),
                clerks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_memory_clerks", ex);
        }
    }

    [McpServerTool(Name = "get_file_io_stats"), Description("Gets the latest per-database-file I/O stats: read/write counts, bytes, stall times, calculated latency. LATEST IS A TIME: the newest snapshot, not a window; captured_at is when it was collected, and the deltas cover the sample_interval_seconds ending there. sample_interval_seconds 0 means no delta was knowable for that file (first sighting, counter reset, a gap) and that row's latencies are null, not 0. <<GUIDE>> Gets the latest file I/O statistics per database file: read/write counts, bytes, stall times, and calculated latency. High read latency (>20ms) or write latency (>10ms for data, >2ms for log) often indicates storage bottlenecks. Each row carries sample_interval_seconds, the measured seconds its deltas accrued over; a 0 means no delta was knowable for that file at this collection (first sighting, counter reset, or a gap past the delta policy — typically a restart) and its latencies are null rather than 0. LATEST IS A TIME: this reads the newest file-I/O snapshot, not a window, and captured_at is the instant it was collected; the deltas cover the sample_interval_seconds ending there.")]
    public static async Task<string> GetFileIoStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var snapshot = await DarlingDataReader.GetLatestFileIoStatsAsync(postgres, resolved.ServerId, cancellationToken);
            if (snapshot.IsEmpty)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "file_io_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No file I/O stats available.");

            var result = snapshot.Rows.Select(r => new
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
                /* #3540: the measured seconds the deltas accrued over, handed to the caller as the perfmon
                   tools hand theirs. 0 means no delta on this row was knowable — the collector's first
                   sighting of the file, a counter reset, or a gap past the policy — and the latencies below
                   are null for it rather than the "0.00 ms" a restart used to read as. null on the interval
                   itself is a pre-V127 row that never recorded one. */
                sample_interval_seconds = r.SampleIntervalSeconds,
                avg_read_latency_ms = r.IsUnknowable ? (double?)null : Math.Round(r.DeltaReads > 0 ? (double)r.DeltaStallReadMs / r.DeltaReads : 0, 2),
                avg_write_latency_ms = r.IsUnknowable ? (double?)null : Math.Round(r.DeltaWrites > 0 ? (double)r.DeltaStallWriteMs / r.DeltaWrites : 0, 2)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                captured_at = snapshot.CapturedAt!.Value.ToString("o"),
                files = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_file_io_stats", ex);
        }
    }

    [McpServerTool(Name = "get_tempdb_trend"), Description("Gets TempDB space usage over time in time buckets: user objects, internal objects, version store, total reserved and unallocated space, and the top consumer session. High version store can indicate long-running transactions under RCSI/SNAPSHOT isolation.")]
    public static Task<string> GetTempDbTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        CancellationToken cancellationToken = default) =>
        GetTempDbTrend(postgres, server_name, hours_back, as_of, bucket_minutes, TrendBudget.Mcp(TrendBuckets.TempDbMaxPoints), cancellationToken);

    /// <summary>get_tempdb_trend under an explicit <paramref name="budget"/> (#3960): the MCP tool passes its own, the
    /// web viewer's <c>/api/read</c> mirror <see cref="TrendBudget.Chart"/>.</summary>
    internal static async Task<string> GetTempDbTrend(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, int? bucket_minutes, TrendBudget budget,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
        if (bucketError != null) return bucketError;

        try
        {
            var points = await DarlingDataReader.GetTempDbBucketsAsync(postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, bucketMinutes, cancellationToken);
            if (points.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "tempdb_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No TempDB data available.");

            return TrendPayloads.TempDbTrend(resolved.ServerName, hours_back, points, bucketMinutes, bucket_minutes is not null, budget.AutoPoints);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_tempdb_trend", ex);
        }
    }

    [McpServerTool(Name = "get_perfmon_stats"), Description("Gets the latest SQL Server performance counter values (batch requests/sec, compilations/sec, deadlocks/sec, and more). LATEST IS A TIME: the newest snapshot, not a window; captured_at is when it was collected; use get_perfmon_trend for history. counter_kind: gauge = value IS the reading, delta_value null; rate = value is cumulative, delta_value its per-interval change; other = a non-rate per-interval change; null counter_kind predates the column, classify by name (ends in /sec = rate). <<GUIDE>> Gets the latest SQL Server performance counter values: batch requests/sec, compilations/sec, deadlocks/sec, and more. Provides throughput context to distinguish a busy server from a sick one. Use counter_name or instance_name to filter results. LATEST IS A TIME: this reads the newest counter snapshot, not a window, and captured_at is the instant it was collected; use get_perfmon_trend for a counter over time. Each row carries counter_kind from the stored cntr_type: 'gauge' means value IS the reading (a level such as Total Server Memory (KB); delta_value is null because a level has no delta), 'rate' means value is a cumulative count and delta_value is its change over the last collection interval (get_perfmon_trend carries the sample_interval_seconds to divide it by for a per-second figure), 'other' means an average/fraction numerator whose delta_value is a per-interval change and not a rate; null counter_kind is a row written before the type was stored — classify it by name (a counter whose name ends in /sec is a rate).")]
    public static async Task<string> GetPerfmonStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Filter to a specific counter name, e.g. 'Batch Requests/sec'.")] string? counter_name = null,
        [Description("Filter to a specific instance name, e.g. a database name.")] string? instance_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var snapshot = await DarlingDataReader.GetLatestPerfmonStatsAsync(postgres, resolved.ServerId, cancellationToken);
            if (snapshot.IsEmpty)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "perfmon_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No perfmon stats available.");

            IEnumerable<DarlingDataReader.PerfmonRow> filtered = snapshot.Rows;
            if (!string.IsNullOrEmpty(counter_name))
                filtered = filtered.Where(r => r.CounterName.Contains(counter_name, StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrEmpty(instance_name))
                filtered = filtered.Where(r => r.InstanceName.Contains(instance_name, StringComparison.OrdinalIgnoreCase));

            /* counter_kind is the stored type's three-way reading (V132, #3653 A7) through the one shared
               vocabulary; delta_value is null on a gauge because the collector writes none — the reading is
               value — and null on nothing else. Twin of Lite's McpPerfmonTools. */
            var result = filtered.Select(r => new
            {
                counter_name = r.CounterName,
                instance_name = r.InstanceName,
                value = r.Value,
                delta_value = r.DeltaValue,
                cntr_type = r.CntrType,
                counter_kind = PerfmonCounterTypes.Word(r.CntrType)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                captured_at = snapshot.CapturedAt!.Value.ToString("o"),
                counters = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_perfmon_stats", ex);
        }
    }

    /* ═══════════════════════════ query performance ═══════════════════════════ */

    [McpServerTool(Name = "get_top_queries_by_cpu"), Description("Gets expensive cached queries from sys.dm_exec_query_stats, ranked by CPU over a window ending at as_of. Filters (database_name, parallel_only, min_dop) apply before the top-N cap: filter_applied names the floor in force, and an empty page under it is the window's real answer, not a miss. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes, not windowed; cpu_attribution's ratio is omitted, not invented, when its inputs are missing. window_truncated marks a window floor, not a page cut; effective_start / effective_hours_back give the reach actually served. <<GUIDE>> Gets expensive queries from sys.dm_exec_query_stats (plan cache). Best for: currently cached queries with detailed per-execution stats, DOP, spills, and query_hash for trending. Returns query_hash, query_plan_hash, sql_handle, plan_handle, and host_object (the hosting procedure/function for proc-hosted statements, null for ad-hoc) — groups key on (database, query_hash, host_object), so INSERT...EXEC callers in different procedures report separately with their own text. distinct_texts counts statement texts merged into a group (>1 = ad-hoc literal variants or pre-upgrade history; query_text is one representative, 0 means only rows predating the text dimension). 'host_object' rolls all of a procedure's statements into one row — use it when dynamic SQL with per-value literals fragments one statement across many hashes, which no top-N-by-hash ranking can surface. Ad-hoc statements have no host object and stay grouped per hash in both modes. distinct_query_hashes reports how many hashes a row rolled up. Set group_by='host_object' to roll all of a procedure's statements into one row — necessary when dynamic SQL with per-value literals fragments one statement across many hashes, which no top-N-by-hash ranking can surface. Supports database and parallelism filtering; every filter is applied IN the query before the ranking and the cap, so the page is the top-N of the FILTERED population (filter_applied names the parallelism floor in force, null when none), and an empty page under parallel_only/min_dop is the window's answer rather than a page artefact. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes for the plan's time in cache (same semantics as max_dop), not windowed — totals and avgs are windowed deltas; rows where an extreme provably predates the window carry extremes_note. max_dop comes from sys.dm_exec_query_stats and is a lifetime-max for the plan's time in cache, so a plan compiled before MAXDOP was lowered keeps reporting the old higher value until it is evicted or recompiled; confirm current parallelism with analyze_query_plan, which reads the actual plan." + McpHelpers.WindowTruncatedDescription + " " + McpToolGuideTopics.CpuTimeExtremesAndAttribution)]
    public static async Task<string> GetTopQueriesByCpu(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description("If true, only return queries whose cached plan has EVER run at DOP > 1 (a LIFETIME max_dop; can read stale after a MAXDOP change). See the tool's reading guide.")] bool parallel_only = false,
        [Description("Minimum DOP to filter on. Implies parallel filtering. Filters the same lifetime-max value as parallel_only, not current parallelism.")] int min_dop = 0,
        [Description("Grouping. 'query_hash' (default): one row per (database, query_hash, host_object). 'host_object': rolls a proc's statements into ONE row. See the tool's reading guide.")] string group_by = "query_hash",
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        /* #2235: an unrecognised value must not silently fall back to the default grouping — a caller who
           asked for a rollup and got a per-hash ranking would read it as "this proc is not hot", which is
           the exact wrong conclusion this option exists to prevent. */
        var rollUp = string.Equals(group_by, "host_object", StringComparison.OrdinalIgnoreCase);
        if (!rollUp && !string.Equals(group_by, "query_hash", StringComparison.OrdinalIgnoreCase))
        {
            return McpHelpers.Refusal("group_by",
                $"group_by must be 'query_hash' or 'host_object' (got '{group_by}').");
        }

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;

        /* #3541 A13: the parallelism filter goes INTO the read as a lifetime max_dop floor on the grouped
           population, applied before the CPU ranking and the cap (see TopQueriesSql's HAVING note). It used
           to be a .Where over the returned top-N page: parallel_only=true on a box whose twenty hottest plans
           were serial came back EMPTY while the window held parallel plans, and the engine's own CXPACKET
           advice steers agents to exactly that call. The floor is 2 for parallel_only (the smallest DOP that
           is parallel), min_dop when the caller set one above that, 0 (admit all) otherwise; min_dop implies
           parallel filtering, as its description has always said. */
        var minMaxDop = min_dop > 1 ? min_dop : parallel_only ? 2 : 0;
        var filterApplied = minMaxDop > 0
            ? $"lifetime max_dop >= {minMaxDop} (applied in SQL before the top-{top} ranking; the page is the top-{top} of the parallel population)"
            : null;

        try
        {
            var now = windowEnd;
            var requestedStart = now.AddHours(-hours_back);
            var routed = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                postgres, resolved.ServerId, requestedStart, now, top, database_name, rollUpByHostObject: rollUp, minMaxDop: minMaxDop, cancellationToken: cancellationToken);
            var rows = routed.Rows;
            var tierUsed = routed.Tier == RetentionTier.Hourly ? "hourly" : "raw";

            /* #4231 stage 3: hourly-routed rows have no host_object split and no per-row text dimension —
               query_text is resolved with a separate follow-up, not the raw LATERAL's, and host_object/
               distinct_texts are always null/0 at that tier (see GetTopQueriesByCpuHourlyAsync). Stated once
               here rather than per row, since it is a property of the tier, not the row. */
            var precisionNote = tierUsed == "hourly"
                ? "hourly-rollup rows have no host-object split; proc-hosted callers that share a query_hash are combined"
                : null;

            /* #4231: what the raw tier actually held, beside what was asked for. Rows above are top-N by CPU,
               not by time, so their timestamps say nothing about how far back the window reached — raw
               query_stats is dropped at 4 days on a store with the rollups armed, and a 7-day ask silently
               got ~4. Same probe and the same #2364 disclosure get_query_store_top already makes.
               #4231 stage 3: only meaningful for the RAW tier — an hourly-routed read did not touch
               query_stats at all, so the raw floor probe would answer a fact about a table this read never
               consulted; skip it and report window_truncated = false there. */
            DateTime? floor = tierUsed == "raw"
                ? await DarlingDataReader.GetQueryStatsWindowFloorAsync(postgres, resolved.ServerId, requestedStart, now, cancellationToken)
                : null;
            var effectiveStart = tierUsed == "raw" ? RawWindowFloor.EffectiveStart(floor, requestedStart) : requestedStart;
            var windowTruncated = tierUsed == "raw" && RawWindowFloor.IsTruncated(floor, requestedStart);

            if (rows.Count == 0)
            {
                /* A filtered miss is not a collection miss: with the floor in the query, an empty page under
                   parallel_only means the window held no group whose plan ever ran parallel, and saying
                   "no query stats available" for that would send the caller to collection health. */
                if (minMaxDop > 0)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No query-stats group on {resolved.ServerName} in the last {hours_back} hour(s) has a cached plan with lifetime max_dop >= {minMaxDop}. The filter was applied in SQL over the whole window, so this is the window's answer rather than a page artefact — drop parallel_only / min_dop to see the unfiltered ranking, or confirm current parallelism with analyze_query_plan.",
                        new { filter_applied = filterApplied });
                }

                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No query stats available for the specified time range.");
            }

            /* #2320: what fraction of the box's measured CPU the RETURNED rows explain — numerator is
               the caller-visible ranking (post top-N, post filters), denominator is measured, and the
               ratio is omitted rather than invented when a denominator piece is missing. The two reads
               are independent, so they run concurrently (review catch). */
            var cpuAggregateTask = DarlingDataReader.GetCpuWindowAggregateAsync(postgres, resolved.ServerId, requestedStart, now, cancellationToken);
            var propertiesTask = DarlingDataReader.GetLatestServerPropertiesAsync(postgres, resolved.ServerId, cancellationToken);
            await Task.WhenAll(cpuAggregateTask, propertiesTask);
            var cpuAggregate = await cpuAggregateTask;
            var properties = await propertiesTask;
            var attribution = CpuAttribution.Compute(
                rows.Sum(r => r.TotalCpuUs) / 1_000_000.0,
                requestedStart, now,
                cpuAggregate.SampleCount, cpuAggregate.FirstSample, cpuAggregate.LastSample, cpuAggregate.AvgSqlCpuPercent,
                properties?.CpuCount ?? 0);

            var result = rows.Select(r => new
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
                /* #4231: what was served, beside what was asked for — the same disclosure get_query_store_top
                   makes (#2364), over query_stats instead of query_store_stats. */
                effective_start = effectiveStart.ToString("o"),
                effective_hours_back = Math.Round((now - effectiveStart).TotalHours, 1),
                /* #2235: echoed so a stored or pasted payload cannot be misread as the other grouping —
                   the two answer different questions and the rows look alike. */
                group_by = rollUp ? "host_object" : "query_hash",
                /* #3541 A13: the filter that shaped the population, stated on the payload; null when none. */
                filter_applied = filterApplied,
                /* #4231 stage 3: which tier answered — "raw" or "hourly" (never bare truncated/degraded
                   vocabulary; see McpHelpers.WindowTruncatedDescription's own rule). precision_note explains
                   what an hourly-routed row is missing versus what raw would have returned. */
                tier_used = tierUsed,
                precision_note = precisionNote,
                cpu_attribution = new
                {
                    ranked_cpu_seconds = attribution.RankedCpuSeconds,
                    sql_cpu_seconds_in_window = attribution.SqlCpuSecondsInWindow,
                    attributed_cpu_ratio = attribution.AttributedCpuRatio,
                    note = attribution.Note
                },
                /* #4231: the WINDOW floor, spelled the way #3653 item 17 fixed the vocabulary — never bare
                   `truncated`, which on every paged tool in this file means a limit bit. Nothing the caller
                   sends changes it: the raw tier is where the rows were, and it stops where it stops. */
                window_truncated = windowTruncated,
                truncation_note = windowTruncated
                    ? "The window reaches further back than this server's raw query_stats retains (or this "
                      + "server has been monitored for less time than that), so the older part of it was not read."
                    : null,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_top_queries_by_cpu", ex);
        }
    }

    [McpServerTool(Name = "get_top_procedures_by_cpu"), Description("Gets the most expensive stored procedures ranked by total CPU time over a window ending at as_of. Delta-based: requires ~30 minutes after adding a new server before data appears. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes, not windowed (extremes_note flags a provably stale one); cpu_attribution's ratio is omitted, not invented, when its inputs are missing. window_truncated marks a window floor, not a page cut; effective_start / effective_hours_back give the reach actually served. <<GUIDE>> Shows execution counts, CPU/elapsed times, and I/O metrics. Delta-based: requires ~30 minutes after adding a new server before data appears." + McpHelpers.WindowTruncatedDescription + " " + McpToolGuideTopics.CpuTimeExtremesAndAttribution)]
    public static async Task<string> GetTopProceduresByCpu(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top procedures. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var requestedStart = now.AddHours(-hours_back);
            var routed = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(postgres, resolved.ServerId, requestedStart, now, top, database_name, cancellationToken);
            var rows = routed.Rows;
            var tierUsed = routed.Tier == RetentionTier.Hourly ? "hourly" : "raw";

            /* #4231 stage 3b: hourly-routed rows have no object_type, sql_handle or plan_handle — stated once
               here rather than per row, since it is a property of the tier, not the row. */
            var precisionNote = tierUsed == "hourly"
                ? "hourly-rollup rows have no object_type, sql_handle, or plan_handle"
                : null;

            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "procedure_stats", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No procedure stats available. Delta-based collection requires at least two collection cycles (~30 minutes) to produce non-zero values.");

            /* #4231: what the raw tier actually held, beside what was asked for — same probe and disclosure as
               get_top_queries_by_cpu and get_query_store_top (#2364), over procedure_stats.
               #4231 stage 3b: only meaningful for the RAW tier — an hourly-routed read did not touch
               procedure_stats at all, so the raw floor probe would answer a fact about a table this read
               never consulted; skip it and report window_truncated = false there. */
            var floor = tierUsed == "raw"
                ? await DarlingDataReader.GetProcedureStatsWindowFloorAsync(postgres, resolved.ServerId, requestedStart, now, cancellationToken)
                : null;
            var effectiveStart = tierUsed == "raw" ? RawWindowFloor.EffectiveStart(floor, requestedStart) : requestedStart;
            var windowTruncated = tierUsed == "raw" && RawWindowFloor.IsTruncated(floor, requestedStart);

            /* #2320: same attributed-CPU disclosure as the queries tool — one shared computation,
               same concurrent independent reads. */
            var cpuAggregateTask = DarlingDataReader.GetCpuWindowAggregateAsync(postgres, resolved.ServerId, requestedStart, now, cancellationToken);
            var propertiesTask = DarlingDataReader.GetLatestServerPropertiesAsync(postgres, resolved.ServerId, cancellationToken);
            await Task.WhenAll(cpuAggregateTask, propertiesTask);
            var cpuAggregate = await cpuAggregateTask;
            var properties = await propertiesTask;
            var attribution = CpuAttribution.Compute(
                rows.Sum(r => r.TotalCpuUs) / 1_000_000.0,
                requestedStart, now,
                cpuAggregate.SampleCount, cpuAggregate.FirstSample, cpuAggregate.LastSample, cpuAggregate.AvgSqlCpuPercent,
                properties?.CpuCount ?? 0);

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                full_name = string.IsNullOrEmpty(r.SchemaName) ? r.ObjectName : $"{r.SchemaName}.{r.ObjectName}",
                /* #4231 stage 3b: procedure_stats_hourly carries no object_type column, so an hourly-routed
                   row reports null here rather than the empty string the reader's default carries —
                   precision_note says so. */
                object_type = tierUsed == "hourly" ? null : r.ObjectType,
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
                /* #4231: what was served, beside what was asked for. */
                effective_start = effectiveStart.ToString("o"),
                effective_hours_back = Math.Round((now - effectiveStart).TotalHours, 1),
                /* #4231 stage 3b: which tier answered — "raw" or "hourly" (never bare truncated/degraded
                   vocabulary; see McpHelpers.WindowTruncatedDescription's own rule). precision_note explains
                   what an hourly-routed row is missing versus what raw would have returned. */
                tier_used = tierUsed,
                precision_note = precisionNote,
                cpu_attribution = new
                {
                    ranked_cpu_seconds = attribution.RankedCpuSeconds,
                    sql_cpu_seconds_in_window = attribution.SqlCpuSecondsInWindow,
                    attributed_cpu_ratio = attribution.AttributedCpuRatio,
                    note = attribution.Note
                },
                /* #4231: the WINDOW floor (#3653 item 17 vocabulary) — never bare `truncated`. */
                window_truncated = windowTruncated,
                truncation_note = windowTruncated
                    ? "The window reaches further back than this server's raw procedure_stats retains (or this "
                      + "server has been monitored for less time than that), so the older part of it was not read."
                    : null,
                procedures = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_top_procedures_by_cpu", ex);
        }
    }

    /// <summary>
    /// #4198: the default page's <c>query_text</c> preview length -- the wide FIELD, not the row count (top
    /// stays 20 here; unlike get_plan_corrections and get_query_store_regressions, this row is narrow enough
    /// on its own that only the text needed cutting). At the old blanket 2,000-character truncation with no
    /// opt-in, twenty rows on a busy production store measured 48 KB, over the shared 32 KB budget
    /// (<see cref="McpResponseBudget.DefaultBytes"/>). Unlike get_deadlock_detail's deadlock_graph_xml, this
    /// field already HAD a cap (2,000) before #4198, so the web viewer's <c>/api/read</c> mirror keeps that
    /// exact number through the <paramref name="previewLength"/> overload below rather than switching to
    /// full text.
    /// </summary>
    private const int QueryTextPreviewLength = 400;

    [McpServerTool(Name = "get_query_store_top"), Description("Cost-ranked top Query Store queries (heaviest first), not time-ordered. Requires Query Store enabled on target databases. window_truncated marks a window floor, not a page cut — no limit changes it — because stored history can be shorter than asked; effective_start / effective_hours_back give the reach actually served. <<GUIDE>> Gets expensive queries from Query Store (persistent, survives restarts). Best for: historical analysis, queries no longer in plan cache. Requires Query Store enabled on target databases. Supports database and module filtering. Reads the raw tier only (the corrected rollups carry no query_id or plan_id), which on a store with the rollups armed is dropped at 4 days. Rows are per Query Store execution outcome (execution_type: Regular, Aborted, Exception): a plan with aborted executions returns one row per outcome, each with its own counts and averages. The execution_type filter keeps one outcome, and module_name keeps one module: the exact, case-sensitive schema-qualified name the collector records (get_top_procedures_by_cpu's full_name; Adhoc for ad-hoc statements, Unknown for an object it could not resolve), applied after interval deduplication and before ranking. When a filter matches nothing but the same read without the filters has rows, the answer is empty (a measured zero), not a Query Store precondition; a module_name miss also carries the window read (effective_start, effective_hours_back, window_truncated) as hints. query_text is a 400-character preview by default (query_text_truncated marks a cut row); full_text=true returns each row's whole statement." + McpHelpers.WindowTruncatedDescription)]
    public static Task<string> GetQueryStoreTop(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Filter by Query Store execution outcome: Regular, Aborted, or Exception.")] string? execution_type = null,
        [Description("Exact schema-qualified module name, as get_top_procedures_by_cpu returns it in full_name (e.g. dbo.usp_ProcessOrder). Case-sensitive; applied before ranking. Ad-hoc statements are Adhoc.")] string? module_name = null,
        [Description("Return each row's full query_text instead of a 400-character preview. Default false.")] bool full_text = false,
        CancellationToken cancellationToken = default) =>
        GetQueryStoreTop(postgres, server_name, hours_back, top, database_name, as_of, execution_type, module_name, full_text, QueryTextPreviewLength, cancellationToken);

    /// <summary>
    /// get_query_store_top under an explicit <paramref name="previewLength"/> (#4198): the MCP tool passes
    /// <see cref="QueryTextPreviewLength"/>, the web viewer's <c>/api/read</c> mirror passes 2000 -- the cap
    /// query_text already had before this opt-in existed, so the viewer's page does not change. Same overload
    /// shape #3897's trend tools use <c>TrendBudget.Chart</c> for.
    /// </summary>
    internal static async Task<string> GetQueryStoreTop(
        NpgsqlDataSource postgres, string? server_name, int hours_back, int top, string? database_name, string? as_of,
        string? execution_type, string? module_name, bool full_text, int previewLength, CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top, "top");
        if (validation != null) return validation;
        /* A closed set, refused by name rather than applied (#3541 A13): an unknown outcome can never match, and
           the empty answer under it would read as "no such executions". Downstream filters on the canonical
           spelling, which is how the collector stores it. */
        validation = McpHelpers.ValidateChoice(execution_type, McpHelpers.QueryStoreExecutionTypes, "execution_type");
        if (validation != null) return validation;
        execution_type = string.IsNullOrWhiteSpace(execution_type)
            ? null
            : McpHelpers.QueryStoreExecutionTypes.First(t => string.Equals(t, execution_type.Trim(), StringComparison.OrdinalIgnoreCase));
        module_name = string.IsNullOrWhiteSpace(module_name) ? null : module_name;

        try
        {
            var now = windowEnd;
            var requestedStart = now.AddHours(-hours_back);
            var rows = await DarlingDataReader.GetQueryStoreTopAsync(postgres, resolved.ServerId, requestedStart, now, top, database_name, execution_type, module_name, cancellationToken);

            /* #2364: what the window ACTUALLY holds. The rows above are the top N by COST, so their timestamps
               say nothing about how far back the read reached -- the most expensive query in a month may have
               run this morning. raw query_store_stats is dropped at 4 days on a store with the rollups armed,
               and this tool has no rollup to fall back to (the corrected CAGGs carry no query_id or plan_id,
               and plan identity is the whole point of this tool). So the honest move is to report the window
               that was served rather than echo the one that was asked for. */
            var floor = await DarlingDataReader.GetQueryStoreWindowFloorAsync(postgres, resolved.ServerId, requestedStart, now, cancellationToken);
            var effectiveStart = floor ?? requestedStart;
            /* #4231: the shared helper's own boundary, not a bare 90-minute literal restated here. */
            var truncated = RawWindowFloor.IsTruncated(floor, requestedStart);

            if (rows.Count == 0)
            {
                /* A filter that matched nothing is an answer, not a missing collection. Most queries never abort,
                   so an Aborted or Exception filter is empty far more often than not, and falling through to the
                   chain below ended at "Query Store may not be enabled" -- false, whenever the same read without
                   the filter has rows. One unfiltered top-1 read tells the two apart; it runs only on this path.
                   module_name (#4057) is the same case and takes the same test: a module that did not run in the
                   window is a measured zero whenever the read without the filters has rows. */
                if ((execution_type != null || module_name != null)
                    && (await DarlingDataReader.GetQueryStoreTopAsync(postgres, resolved.ServerId, requestedStart, now, 1, database_name, cancellationToken)).Count > 0)
                    return module_name is null
                        ? McpHelpers.QueryStoreExecutionTypeEmpty(execution_type!, hours_back, database_name)
                        /* The module miss hands back the window it read: "did not run" is a claim about that window,
                           and the raw tier may not reach the whole of the one asked for. */
                        : McpHelpers.QueryStoreModuleEmpty(module_name, execution_type, hours_back, database_name, truncated, new
                        {
                            effective_start = effectiveStart.ToString("o"),
                            effective_hours_back = Math.Round((now - effectiveStart).TotalHours, 1),
                            window_truncated = truncated
                        });

                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store", cancellationToken)
                    /* #2546: the sentence below GUESSES ("may not be enabled"), and it has to, because the
                       read had no way to find out. The store has known all along -- query_store_health
                       records actual_state per database every hour for exactly this purpose. Asking it turns
                       a hedge into a fact plus the ALTER DATABASE that fixes it, and it answers for the
                       database this read was scoped to rather than for the server's most flattering one. */
                    ?? await DarlingRuntimePrecondition.QueryStoreStatusAsync(postgres, resolved.ServerId, resolved.ServerName, database_name, cancellationToken)
                    /* And the collector's own last run, for the case Query Store is on and the collector is
                       the thing that cannot read it. */
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store", cancellationToken)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No Query Store rows for this server in the {hours_back}-hour window searched. Query Store " +
                        "may not be enabled on the target databases -- or the window reaches past what the raw tier " +
                        "retains (query_store_stats is dropped at 4 days when the rollups are armed), in which case " +
                        "nothing was read for the older part of it. Try a shorter window before concluding the " +
                        "queries did not run.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                query_id = r.QueryId,
                plan_id = r.PlanId,
                query_hash = r.QueryHash,
                query_plan_hash = r.QueryPlanHash,
                execution_type = r.ExecutionTypeDesc,
                module_name = r.ModuleName,
                execution_count = r.TotalExecutions,
                avg_duration_ms = r.AvgDurationMs,
                avg_cpu_ms = r.AvgCpuTimeMs,
                avg_logical_reads = r.AvgLogicalReads,
                avg_logical_writes = r.AvgLogicalWrites,
                avg_physical_reads = r.AvgPhysicalReads,
                avg_rowcount = r.AvgRowcount,
                last_execution_time = r.LastExecutionTime?.ToString("o"),
                query_text = full_text ? r.QueryText : McpHelpers.Truncate(r.QueryText, previewLength),
                query_text_truncated = !full_text && r.QueryText != null && r.QueryText.Length > previewLength,
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
                /* #3653 item 17: the WINDOW floor under its own key. This tool has no page cut to disclose (top
                   is a rank, not a cap the window overflowed), and the flag was spelled `truncated` anyway — the
                   page dialect's word, which on every neighbour in this file means "limit bit, raise it". Here
                   nothing the caller sends changes it: the raw tier is where the rows were, and it stops where
                   it stops. The trend family renamed the same fact the same way (DarlingMcpTrendTools, Lite's
                   WriteDisclosure); the census fails a bare `truncated` beside `effective_hours_back`. The note
                   beside it keeps its name: it is the prose for THIS flag, and `*_note` is the house idiom. */
                window_truncated = truncated,
                truncation_note = truncated
                    ? "The window reaches further back than this server's raw query_store_stats retains, so the "
                      + "older part of it was not read. This tool reads the raw tier only: the corrected rollups "
                      + "carry no query_id or plan_id, and plan identity is what it exists to return."
                    : null,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_store_top", ex);
        }
    }

    /* ═══════════════════════════ discovery / health ═══════════════════════════ */

    [McpServerTool(Name = "list_servers"), Description("Lists monitored servers with collection freshness status and last collection time. Darling has no live connection to monitored servers: status is derived from how recently each was collected (Online = fresh, Warning = stale, Offline = no recent collection). Lite's status IS a live connection check, independent of whether anything is being collected. <<GUIDE>> Use this first to see available servers before calling other tools. Lists all monitored servers — SQL Server and PostgreSQL — with their collection freshness status and last collection time. Each row says which engine it describes: engine_kind is the raw registry token (sqlserver, postgres, aurora-postgres; null when no connect has stamped the row — the same vocabulary get_fleet_overview uses) and engine_version is the engine-aware version label (\"SQL Server 2022\", \"PostgreSQL 18\"; empty when no version has been collected). sql_version is a DEPRECATED legacy alias carrying the same value as engine_version, kept so existing consumers keep working — read engine_version instead, and never infer the engine from that key's name: a PostgreSQL row's sql_version reads \"PostgreSQL 18\". The service has no live connection to the monitored servers, so status is derived from how recently each server was collected (Online = fresh, Warning = stale, Offline = no recent collection). The peer_fleets block names the SIBLING Darling stores that monitor the rest of a split fleet, with what each one covers — this server can only NAME them (no cross-store reads), and peer_note says what an empty peer_fleets does and does not prove.")]
    public static async Task<string> ListServers(
        NpgsqlDataSource postgres,
        CancellationToken cancellationToken = default)
    {
        try
        {
            List<DarlingDataReader.ServerListRow> servers;
            try
            {
                servers = await DarlingDataReader.GetServerListAsync(postgres, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* #3653 (errors one shape): through the shared sentence helper like every other SQL Server-family
                   tool on this SKU, rather than an interpolated sentence of this tool's own. The operation names
                   what was being read so the message loses nothing the ad-hoc one said. */
                return McpHelpers.FormatError("list_servers (reading the servers registry from the store)", ex);
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
        catch (Exception ex) when (ex is not OperationCanceledException)
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
        var result = servers.Select(s =>
        {
            /* Engine-aware (#3145): the label for whichever engine the row describes, so a PostgreSQL
               target reads "PostgreSQL 18" instead of the "SQL Server v0" its 0-valued sql_major_version
               used to produce. Computed once because it rides under two keys below. */
            var engineVersion = MonitoredEngineVersion.DescribeEngineVersion(s.EngineKind, s.SqlMajorVersion, s.PostgresMajorVersion);

            return new
            {
                server_name = s.ServerName,
                display_name = string.IsNullOrEmpty(s.DisplayName) ? s.ServerName : s.DisplayName,
                /* #3245: the row says which engine it describes, machine-readably. engine_kind is the raw
                   registry token — sqlserver / postgres / aurora-postgres, null when no connect has stamped
                   the row — the same key and vocabulary get_fleet_overview's FleetServerCard publishes, so
                   a consumer keys on a field instead of parsing the label. */
                engine_kind = s.EngineKind,
                engine_version = engineVersion,
                /* Legacy alias of engine_version, kept because a field an MCP client keys on is a consumer
                   API (#3149). The NAME is the misdirection #3245 fixes — a PostgreSQL row reads
                   sql_version: "PostgreSQL 18" — so it is documented as deprecated everywhere the payload
                   is described, and retiring it belongs to a later major of the MCP contract, not here. */
                sql_version = engineVersion,
                status = FreshnessStatus(s.LastCollection, s.RegisteredAt, nowUtc),
                read_only = s.ServerName.EndsWith(":RO", StringComparison.Ordinal),
                last_collection = s.LastCollection?.ToString("o")
            };
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

    [McpServerTool(Name = "get_collection_health"), Description("Per-collector 7-day health for a server. STOPPED (gate off) does not count as failing; rows_stored=0 is NOT a fault by itself — output_finding says whether it is a resting event-collector or a denial needing a grant. last_error is a sticky slot, not necessarily current: check last_error_at/denied_since_last_success. regressed_from_productive floors WARNING on an axis SEPARATE from failed_collector_count — never add them. alert_read_health uses a DIFFERENT since-restart window, not this one; zero there means none since restart, not none in 7 days. <<GUIDE>> Shows the health status of all data collectors for a server — whether they're running successfully, failing, or stale. A collector reads STOPPED rather than FAILING when it has attempted nothing at all — no success, no error, nothing — for longer than the FAILING cutoff, despite a history of runs: that is a collector whose gate (AppliesTo) flipped off for this target rather than one that keeps running and erroring, and it does not count toward a server's failing-collector total. A collector reads EXTENSION_MISSING when every attempt in the window was skipped because a PostgreSQL extension it declares is not installed on the target - last_error names the extension, CREATE EXTENSION (plus shared_preload_libraries and a restart where the message says so) is the remedy, never a grant, and an optional extension left uninstalled is a legitimate resting state rather than a fault. That last reading holds ONLY for a collector that never produced on this target, and regressed_from_productive is the field that tells you which one you are looking at: it is true when this collector WAS storing rows and has reported a named skip - EXTENSION_MISSING, PERMISSIONS or SESSION_MISSING - on every cycle since. That is a restart or an upgrade changing what the collector can read, which is a regression rather than rest, and it is not something the band can tell you on its own: the skip bands are reached only when the whole window holds no success, so a collector that regressed inside the last day still has a fresh success, reads HEALTHY on the ordinary staleness ladder, and is floored to WARNING by this flag alone. last_productive_at is when this collector last stored anything, rows_in_prior_7d is how much it stored over this seven-day window and is served only when the flag is true (only then is that figure entirely pre-regression, because a named skip stores nothing), and regression_finding is the sentence that puts the three facts in the order they happened. get_fleet_overview counts the same rows per server as regressed_collector_count. That count is a SEPARATE AXIS from failed_collector_count and the two must never be added: one regression reads WARNING on its first day and FAILING once its success clock runs out, and is regressed on both. Check this before investigating data to ensure collectors are working properly. Each row also carries last_note/note_count: what a NON-failing run reported, e.g. an enumeration that came back with 0 items. note_count equal to total_runs means the collector has been collecting nothing all window — not a fault (the target may be legitimately empty), but the reason a HEALTHY collector can still have no data. target_has_user_databases tells those two apart: true means the target DID have user databases in the same window, so an all-window empty enumeration is worth investigating (a login that cannot enter them, an exclusion filter that matched everything, a databases scope on the collector's schedule row that admits nothing - the last a Darling-only schedule knob); false means either no user databases or no inventory to go on. Each row also carries abandoned and abandon_rate_pct: cycles the 120-second whole-server wall-clock budget gave up on, which stored nothing and advanced no watermark. Unlike a yield, which retries, an abandoned cycle is collected data you do not have. A rate above 0.5% bands the collector WARNING, so a WARNING here may have nothing to do with errors - read abandoned beside errors to attribute it. CRITICAL for reading last_error: it is a single slot carrying the newest ERROR, PERMISSIONS, or EXTENSION_MISSING message in the whole window, and a message in it is NOT evidence that the condition is current. Read last_error_at for when it happened, last_denied_at for when the newest DENIAL specifically happened, and denied_since_last_success for the derived answer - true means a denial is the collector's current state, false means every denial in the window predates a later success and the collector is reading fine now. A fault recorded before a code path changed will sit in last_error for the rest of the window while every cycle since succeeds. Do not infer a live condition from last_error alone. Total abandonment still reads FAILING through staleness; the rate exists for the partial case, where a collector abandons some cycles and succeeds often enough to stay fresh, which otherwise read HEALTHY with errors 0 indefinitely. The sweep_pressure block is the server-level roll-up: it compares the collectors' combined execution demand (average duration amortized by cadence) against the minute the fastest cadence holds. SATURATED means the collection body cannot fit inside its cadence, so relaunches are skipped and the server collects at a multiple of its configured interval while every collector still reads healthy — heaviest_collectors names where that budget goes. That verdict is the SUSTAINED answer only. peak_cycle_risk is the separate single-sweep answer: peak_cycle_ms is what the body costs on the cycle where every scheduled cadence comes due together, and BODY_OVERRUN means that one body cannot fit the budget even when the verdict reads OK — the signature of one infrequent heavy collector, which amortization hides and heaviest_collectors therefore ranks out of sight. peak_collector names it, and peak_cycle_note explains it. Read both fields: a server can be OK/BODY_OVERRUN (a schedule-shape problem, fix by moving or splitting that collector) or SATURATED/BODY_OVERRUN (a capacity problem). Every collector row carries avg_duration_ms, p95_duration_ms and max_duration_ms, because a collector's runs are not always one population. Read the three together: avg close to p95 close to max is one population, avg far below p95 is two, and p95 far below max is one pathological run. peak_cycle_ms is built from p95 (floored at the mean, so it can never read lower than a mean-based figure) for exactly that reason, and peak_collector carries peak_run_ms beside avg_duration_ms so the gap is visible. Those three still describe RUNS, and a collector that runs once per DATABASE writes one blended row, so no run-level statistic can say which database cost what. Five fan out from an enumeration on any SQL Server target (query_store, plan_correction, query_store_health, index_object_stats, database_scoped_config); separately, eleven more fan out over a per-database connection loop when the target is Azure SQL DB, and pg_autovacuum_stats always does on PostgreSQL. The per-collector `fanout` block is that answer, null for a collector that does not fan out: `items` is how wide the fan-out was, `slowest`/`slowest_ms` name the dearest database and its cost on the window's worst run, `run_ms` is that whole run, `slowest_share_pct` is slowest_ms / run_ms as a percentage — the slowest item's share of the whole pass — and `dominance` is slowest_ms * items / run_ms, the slowest item against the MEAN item: 1.0 for a perfectly even fan-out, rising with concentration. The remediation decision routes through the SHARE, not through dominance: a low share means the cost is the fan-out's WIDTH — no single database is worth chasing, and bounded parallelism is the lever — while a high share means one database dominates the pass and a per-database schedule override or a stagger is what helps. Dominance is NOT that verdict, because its ceiling is items. A fixed threshold on dominance misreads exactly the widest fan-outs, where the cost lives. Dominance only reads as dominance when it approaches a meaningful fraction of items (share = dominance / items); it stays published as the evenness ratio, for continuity. Do not try to infer any of this from p95 versus avg — on a per-database collector that ratio is usually saturated by empty-versus-productive runs and says nothing about databases. Every field named so far describes what a collector SPENT; rows_stored, runs_with_rows and productive_run_pct are what it BOUGHT, counted over the same window as total_runs and the durations, so cost and output on a row always describe the same runs. Read them together for the readings that need different actions: rows_stored above zero is expensive AND productive; rows_stored zero with denied_since_last_success false, no faulted run and no note is a collector that read and found nothing, which for one that stores a row only when an event occurs (e.g. deadlocks, blocked_process_report, pg_blocking, pg_xmin_horizon) is the correct resting state and needs no action; rows_stored zero with denied_since_last_success true is a collector that could not read and needs a grant. output_finding says which zero reading applies and is null whenever rows_stored is positive. There are five, in the order they are decided: a current denial is the grant case; errors plus session_missing above zero means that many runs recorded a fault rather than a result, so on those runs the collector was UNABLE to read and the resting-state reading is withheld for the whole window (every run faulted is nothing read at all and needs action - the case an Azure SQL DB target produced when two collectors failed on every database every sweep and were recorded SUCCESS with a sentence saying they had read and found nothing); note_count above zero means the runs themselves recorded what they found, so the finding defers to last_note instead of assuming a category - which is what keeps a DELIBERATE zero distinguishable from a collector that quietly stopped storing rows; nothing on the row explaining the zero from an event collector is that collector at rest; and the same from a collector that is NOT event-triggered - a configuration or snapshot read such as database_scoped_config, which returns a row per setting per database - is its source coming back empty on every run, which is not a resting state and the finding says needs a look. The event-collector set is a closed list on the shared classifier, so a collector left off it gets the non-reassuring sentence rather than the reassuring one. query_store on a read-replica target is the deliberate case: it is not an event collector, and every run notes an empty enumeration because Query Store on a readable secondary is excluded by design. This is deliberately NOT a band. A verdict keyed on cost-plus-zero-rows would fire on the healthy quiet install rather than the blind one. These are NOT the hourly per-collector series Darling's get_collector_cost reports as total_rows - a separate series over that caller's own days_back and across every server at once, and Darling-only, so Lite has no twin of it; the top-level output_note names both windows and disclaims that one. rows_stored is also what a run STORED, never what the monitored engine counted, so a zero cannot tell a genuinely quiet source apart from a reader capturing nothing off a busy one - nothing on this surface measures that. One block on this response is deliberately NOT on the seven-day window: alert_read_health, which counts the alerting layer's OWN store reads that failed and were swallowed. A condition check that cannot read the store logs one line and skips - correctly, because firing on absent evidence would fabricate an alert and resolving on it would fabricate a recovery - and that skip is not a collector run, so it writes no collection_log row and reaches no other health surface: only a grep of the service log found the class. It matters out of proportion to the count because the alert pass runs on a much shorter store deadline than the collection sweep, so as store latency rises the alerting layer is the FIRST thing to fail and collection is the last - during one measured episode of store lock contention the service log's error rate rose 41 to 61 per hour, every line an alerting-side read, while collector failures over the same hours FELL from 23 to 2. Read server_read_failures beside server_alert_passes for this server (a pass is one alert evaluation pass containing many reads, so more failures than passes is ordinary and the pair is NOT a ratio; a Darling sweep runs two passes for a SQL Server target, three for a PostgreSQL one, and Lite runs one, so the denominator is comparable within a host and engine but not across them), instance_read_failures for the whole service (which also covers the fleet-scoped conditions that belong to no server and so appear in no per-server count: " + AlertReadFailureCounter.FleetScopedReads + "), fleet_read_failures for how many of that service total belong to no server at all - the figure that makes a nonzero service count readable from a server whose own count is zero, because those two populations take opposite actions: a blind fleet-scoped read means the store's own self-alerts went quiet and two of them are the reads whose alerts would say the store is in trouble, while one on another server is answered by reading this same block there, last_failure_read for which condition went blind most recently, last_failure_elapsed_ms for how long that read ran before it faulted - which is the term that says whose deadline ended it ONLY WHERE THE ALERT PASS SETS ONE. The Darling service does, on every store read; Lite's alerting reads go into its local store with no command deadline at all, so on Lite this is a plain duration that says a read became slow and nothing about who ended it. Where there is a deadline: an elapsed at or about it means this process stopped waiting while the statement was still running on the store, one well below that bound means the store returned a fault, and the exception text cannot make that distinction because a client-side deadline renders as a torn stream with no SQLSTATE exactly like a dropped connection. A figure well ABOVE the bound is a third reading: the failure was not a single bounded read, which each site's clock restart between consecutive awaits makes rare and which is expected only on the shared engine sweep entry, whose awaited operation is a whole alert pass - and last_failure_at to tell a healed episode from a live one: this count never ages out of a window, so a nonzero value with a stamp from days ago is history. Every count on this block carries its own newest-failure stamp, read name and elapsed, so none of them sits undated: last_failure_* are THIS server's, fleet_last_failure_* are the fleet-scoped conditions' and are attributable by construction, and instance_last_failure_* are the newest failure anywhere in this service whatever its scope - that last trio may therefore name a read on a server you did not ask about and makes no claim about which, which is why the fleet trio is reported separately rather than inferred from it. The third population is a subtraction: instance_read_failures minus server_read_failures minus fleet_read_failures is how many failed on OTHER servers, and that one has no stamp here by design - nothing holds a newest failure for it, and a count with nothing to date or name it is the gap the three trios close, so read this block on those servers to attribute it. retried_reads is the SECOND population and on the Darling service is where most of what this block used to count now lands: reads that crossed the 10 s alert-pass deadline once and succeeded on the single retry two seconds later - the store's write bands' cost, counted rather than blinding an alert. Every one of those would once have been a swallowed failure, so read the two together: retries rising with failures at zero is a store under write pressure whose alerting is intact, and both rising is a store where a second attempt twelve seconds later still found the band on, which wants the store's write schedule looked at rather than the reader's. A read that failed twice counts in both. It carries no stamp and no read name, deliberately - a retried read did not go blind, so there is no episode to date or attribute, and a stamp would invite reading a retry as a soft failure. instance_retried_reads is the same figure across the whole service, with no fleet part because nothing records a fleet-scoped retry today. On Lite both read zero as a property of the SKU and not of a quiet store: Lite's alerting reads carry no command deadline, so there is no deadline to cross and nothing to retry. counting_since is when this process began counting, early in its own startup - these are in-memory counts and a restart takes them to zero, so a zero means \"none since counting_since\" and NOT \"none in seven days\"; check the stamp before reading the zero as reassurance. Deliberately not persisted, because what it counts is a failure to READ the store. It does NOT count alerts that failed to DELIVER and makes no claim about them - that is get_alert_history's question. And deliberately not a band, for the same reason the output figures are not: any threshold over it would have to guess how many blind reads make alerting unhealthy, and a wrong guess on this particular surface fails by saying nothing is wrong. The service block beside alert_read_health is the build-attribution read: nothing else on this surface says WHAT build is answering, so the question an operator watching an install actually asks - \"is the running service the build that carries fix X\" - is answered by version directly instead of by restart inference plus a merge-list lookup the watcher has no way to perform. version is the running build's informational version - the same read the Darling service's --version verb prints, with any SemVer build-metadata suffix stripped and the prerelease suffix KEPT, because the nightly stamp lives in the prerelease and one nightly differs from the next in nothing else; Lite has no version verb and reads the same attribute off its own app assembly, stamped from the same single declaration. counting_since remains the restart detector, and a restart is NOT a build: a crash-restart moves counting_since exactly the way an install does, which is why inferring a build from it misattributes. started_at is the SAME instant counting_since carries, deliberately - both mean \"when this process came up\", and a second clock for one fact would put two near-identical stamps on one payload whose skew a reader would have to explain away. compiled_schema_version is the schema rung this BUILD expects - the compiled constant, not a read of the store's migrated rung - so beside version it says what this build requires of the store it serves, whether or not that store has caught up. When a recent analysis pass could not read one of its fact families for this server, the payload also carries analysis_caveats (analysis_time, families_failed, families_total, entries[{family, read, outcome, message, failed_in_last_passes}]) — the analysis pass's own reads of the collectors' tables, a different layer from the collector rows; absent when the last 24 remembered passes were clean. Process memory: a service restart forgets. full_detail (#4198): every collector row above defaults to a compact shape - collector, status, compact, total_runs, rows_stored, avg_duration_ms, last_success - for a collector that is HEALTHY with zero errors, session_missing, extension_missing, permission_denied and abandoned runs this window, not regressed, and either stored rows or is a known event collector reading zero at rest. Any other row - failing, stale, stopped, erroring, denied, regressed, or a non-event collector's unexplained zero - is never dropped and never silently shortened to that seven-field shape: it gets partial_detail: true instead of compact, and a leaner shape naming what is wrong and since when - collector, status, partial_detail, total_runs, errors, session_missing, abandoned, rows_stored, last_success, last_error, last_error_truncated, last_error_at, last_denied_at, denied_since_last_success, regressed_from_productive, output_finding, output_finding_truncated - rather than every field named earlier in this guide. Pass full_detail=true for every field on every row regardless of health or tier. collector_detail_note on the envelope names how many rows compacted, how many took the leaner shape, and how full_detail restores each.")]
    public static async Task<string> GetCollectionHealth(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Return every field for every collector. Default compacts a HEALTHY collector with nothing to report; failing, stale, stopped, erroring, denied or regressed collectors always keep every field.")] bool full_detail = false,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            /* #3856: through the single-flight memo, never the direct read. This statement is 29 columns per
               collector over a four-window-function subquery across seven days of collection_log, and it was
               the one surface of that scan #3738 left unmemoized when it gave the fleet rollup's twin one scan
               per minute per host (#3735). On 2026-09-21 21:3xZ the mcp role's 15 s statement_timeout cancelled
               it (57014) inside the hourly materializations' write band — first occurrence on record, and the
               retry succeeded, which is the band's signature rather than the plan's.

               nowUtc is read ONCE and used twice, deliberately: it is the instant the trailing-7-day window is
               cut from AND the reference collection_health_age_seconds is measured against, and two
               DateTime.UtcNow reads would put a payload's age on a different clock from the window it
               describes. The memo keys on (server, window start) and this call site's window start is the same
               fixed now - 7 days get_fleet_overview's rollup cuts — which is what #3856 asked to be verified
               before relying on the windows matching. It does not make the two reads INTERCHANGEABLE (the
               rollup's memo holds per-server counts off a twelve-column aggregate, not these rows; see
               GetCollectionHealthMemoizedAsync), and that is why this is its own memo rather than a filter over
               that one. */
            var nowUtc = DateTime.UtcNow;
            var (rows, collectionHealthAgeSeconds) = await DarlingDataReader.GetCollectionHealthMemoizedAsync(
                postgres, resolved.ServerId, nowUtc.AddDays(-7), nowUtc, cancellationToken);
            if (rows.Count == 0)
                return McpHelpers.Status("unavailable", "No collection health data available.");

            var compactCount = full_detail ? 0 : rows.Count(IsCollectionHealthCompactEligible);
            /* #4198's second cut: a row that fails the predicate above used to keep the full ~30-field shape
               below regardless of full_detail. Measured on both SKUs' #4198 fixtures that alone could not clear
               the response budget -- the weight is 30+ small fields times every row that needs a look, which
               previewing free text cannot touch -- so that row now gets PartialCollectionHealthRow instead,
               unless full_detail=true asks for everything. */
            var partialCount = full_detail ? 0 : rows.Count - compactCount;
            var result = rows.Select(r => full_detail
                ? (object)FullCollectionHealthRow(r)
                : IsCollectionHealthCompactEligible(r)
                    ? (object)CompactCollectionHealthRow(r)
                    : (object)PartialCollectionHealthRow(r));

            static object FullCollectionHealthRow(CollectorHealth r) => new
            {
                collector = r.CollectorName,
                status = r.HealthStatus,
                total_runs = r.TotalRuns,
                errors = r.ErrorCount,
                /* #3754: runs whose XE session was missing or could not be created (SESSION_MISSING). Its
                   own number rather than folded into errors, because it is not one: it feeds no band (the
                   capture-down story is the self-alert's) and it is not a grant. Until now the status reached
                   this surface as total_runs and nothing else, so a collector whose every run was
                   SESSION_MISSING read `errors 0` beside a FAILING band and an output_finding that said it had
                   read and found nothing. It is the second count output_finding reads as "could not read".
                   0 for every collector that reads no XE session. */
                session_missing = r.SessionMissingCount,
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
                /* #3819: whether this collector STOPPED producing, and the three facts behind it. A named
                   skip means one thing on a collector that never produced here (the Aurora
                   optional-extension class, a legitimate resting state) and the opposite on one that was
                   producing until a restart or an upgrade — and until now the row read the same both
                   ways. Worse, the band agreed with the benign reading for a full day: the ladder's skip
                   arms need a window with NO success in it, so a regressed collector never reaches them
                   and reads HEALTHY until its last productive cycle ages past the FAILING cutoff.

                   rows_in_prior_7d and the finding are null unless the flag is true. The rows figure is
                   rows_stored above, which on a regressed row is entirely pre-regression because a named
                   skip stores nothing; served under its own name only where that reading holds, so the
                   name cannot claim a scope the row does not support. last_productive_at rides
                   unconditionally — "when did this last store anything" is worth answering on every row.

                   Composed from the shared classifier, like output_finding and note_summary above, so no
                   consumer re-derives either the predicate or the sentence differently. */
                /* #3885 widened the flag from the skip class to EITHER regression class: a collector
                   recording SUCCESS with zero rows on three consecutive runs after a productive history
                   has stopped doing what it used to do just as surely as one reporting a skip word, and
                   that class is strictly harder to see — its successes are fresh, so no staleness reading
                   moves and the row read HEALTHY for a fortnight on 41 of 43 servers of the largest
                   production store. One field, one finding slot, one fleet count, because the two classes
                   are disjoint by construction and an operator asks one question of them.

                   zero_row_success_runs rides unconditionally, like last_productive_at: "how many runs
                   have stored nothing" is worth answering on every row, and on a HEALTHY row below the
                   N=3 boundary it is exactly what says how close this collector is to it. */
                regressed_from_productive = r.AnyRegression,
                last_productive_at = r.LastProductiveTime?.ToString("o"),
                rows_in_prior_7d = r.AnyRegression ? r.RowsStored : (long?)null,
                regression_finding = r.AnyRegressionFinding,
                zero_row_success_runs = r.TrailingZeroRowSuccessRuns,
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
                   not. Emitted as a nested object rather than sibling fields so a consumer cannot read a
                   slowest item without the width it has to be judged against — the parts only mean
                   something together, computed here so every consumer gets the same arithmetic instead of
                   three of them inventing it.

                   #3502: `slowest_share_pct` is the verdict — the slowest item's share of the whole pass,
                   which is what the width-versus-concentration decision turns on. `dominance` stays for
                   continuity as the evenness ratio (slowest against the MEAN item); its ceiling is `items`,
                   so at width it reads high with no concentration behind it, which is exactly how it got
                   read as a verdict it never was. Share is non-null wherever dominance is: it is derived
                   from it, so the two cannot drift apart or describe different runs.

                   This is the thing avg/p95/max structurally cannot say: they aggregate over runs, and one
                   run is one blended row however many databases it covered. */
                fanout = r.FanoutDominance is null ? null : new
                {
                    items = r.FanoutItems,
                    slowest = r.SlowestItem,
                    slowest_ms = r.SlowestItemMs,
                    run_ms = r.SlowestRunDurationMs,
                    slowest_share_pct = Math.Round(r.FanoutSlowestSharePercent!.Value, 2),
                    dominance = Math.Round(r.FanoutDominance.Value, 2)
                }
            };

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

            /* #3691: wraps the whole payload rather than sitting in it as a property — McpHelpers.JsonOptions
               WRITES nulls, so a property would ship analysis_caveats: null on every clean answer — and it reads
               the process-wide ledger because the scheduled sweep builds a fresh analysis service per pass.
               A different layer from the collector rows above: 43 healthy collectors above a family the pass timed out reading. */
            return JsonSerializer.Serialize(CollectionCaveatLedger.Shared.Attach(new
            {
                server = resolved.ServerName,
                /* #3453: the build-attribution read. Nothing else on the MCP surface says WHAT build is
                   answering: counting_since moving proves a RESTART (a crash-restart reads identically to
                   an install), and a capability fingerprint identifies a build only when the change in
                   question happens to move one. The version is this assembly's informational version
                   through the SAME method the CLI's --version verb prints, so the two surfaces cannot
                   drift; the nightly stamp lives in its prerelease suffix, which is exactly what
                   distinguishes one nightly from the next and why this is ProductVersion() and not a
                   display-normalized form that would strip it. */
                service = new
                {
                    version = DarlingCliCommands.ProductVersion(),
                    /* The SAME instant alert_read_health.counting_since carries below, deliberately: both
                       mean "when this process came up", and a second clock for one fact would put two
                       near-identical stamps on one payload whose skew a reader would have to explain
                       away. Round-trip "o", matching every other stamp on this response. */
                    started_at = alertReads.CountingSinceUtc.ToString("o"),
                    /* The rung this BUILD expects - the compiled constant, never a literal, so the field
                       moves with every future migration by construction. Deliberately NOT the store's
                       migrated rung: that is a store read with its own failure modes, and the pair
                       version-plus-expectation is the build's half of "what is actually running". The
                       startup migration pass is what makes the two rungs agree on a healthy install. */
                    compiled_schema_version = StorageVersion.SchemaVersion
                },
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
                       one still in progress read identically. Exactly last_error's #3010 lesson.

                       Round-trip "o", matching last_success / last_error_at / last_denied_at on the
                       collector rows of this same response. A raw DateTime would serialize to an ISO string
                       too, but with trailing zeros trimmed, so two timestamps on one payload would carry
                       different precision guarantees for no reason. */
                    last_failure_at = alertReads.LastFailureAtUtc?.ToString("o"),
                    last_failure_read = alertReads.LastFailureRead,
                    /* The classification term. The name says WHICH read went blind; this says whose deadline
                       ended it — at or about DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds means
                       this service stopped waiting while the statement was still running on the store, and
                       well below that bound means the store returned a fault. The exception text cannot
                       separate those two, because Npgsql renders a client-side deadline as a torn stream
                       with no SQLSTATE, identically to a dropped connection.

                       Null exactly when last_failure_at is null, from the counter's own single currency
                       test, so this response can never carry a duration belonging to no event. */
                    last_failure_elapsed_ms = alertReads.LastFailureElapsedMs,
                    /* The fleet scope, whose failures belong to NO server and so land in no per-server
                       bucket. Without this count a nonzero instance total beside a zero server count spans
                       two populations that take opposite actions and cannot be told apart — a blind read on
                       another server, answered by this same block there, and a blind read on a condition
                       that belongs to no server, which no per-server block answers at all. Two of the
                       conditions in that set are the store background-job health reads, so this count is
                       most likely to be nonzero exactly when the population it isolates is the one that
                       matters. The failures on OTHER servers are the subtraction of these two from the
                       instance total, and are not a field: nothing holds a newest failure for that
                       population, and a count with no stamp to date it is what the three trios here fix. */
                    fleet_read_failures = alertReads.FleetReadFailures,
                    /* The fleet count's own currency and identity terms, and the only attributable read
                       name on the block — a failure recorded under a null key belongs to the fleet bucket
                       by construction, where the instance-wide newest below may be on any server. */
                    fleet_last_failure_at = alertReads.FleetLastFailureAtUtc?.ToString("o"),
                    fleet_last_failure_read = alertReads.FleetLastFailureRead,
                    /* Read against what the named read actually is rather than against the alert pass's
                       deadline: the store background-job health reads in this set swallow their own faults
                       one level down and run on their own budget, so for that entry this is a duration for
                       whatever faulted and not evidence about who ended it. */
                    fleet_last_failure_elapsed_ms = alertReads.FleetLastFailureElapsedMs,
                    /* The instance count's currency term — the newest failure anywhere in this service,
                       whatever its scope. It is the only one that covers the other-server population, and
                       it makes no claim about scope: it may name a read on a server the caller did not ask
                       about, which is why the fleet trio is reported separately rather than inferred from
                       this one. */
                    instance_last_failure_at = alertReads.InstanceLastFailureAtUtc?.ToString("o"),
                    instance_last_failure_read = alertReads.InstanceLastFailureRead,
                    instance_last_failure_elapsed_ms = alertReads.InstanceLastFailureElapsedMs,
                    /* The floor under the zero. A restart resets these counts, so counting_since is what
                       says whether a zero covers weeks or ninety seconds. */
                    counting_since = alertReads.CountingSinceUtc.ToString("o"),
                    /* #3848: the second population, and on this SKU it is where most of what the counts
                       above used to hold now lands — reads that crossed the 10 s deadline once and
                       succeeded on the single retry two seconds later. The write bands' cost, counted
                       rather than blinding an alert. Trailing fields deliberately: the two counts they are
                       READ against sit at the top of this block, and a reader reaches these after the
                       failure figures rather than instead of them.

                       No stamp and no read name beside them, unlike every count above, and that asymmetry
                       is the design: those three date and attribute a condition that went BLIND, and a
                       retried read did not — it was judged on evidence that arrived late. A stamp here
                       would invite reading a retry as a soft failure. counting_since is their currency
                       term like everything else on the block. */
                    retried_reads = alertReads.ServerRetriedReads,
                    instance_retried_reads = alertReads.InstanceRetriedReads,
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
                /* #4198: what the default cut left out and how to get it back, read fresh every call rather
                   than a static sentence — full_detail=true changes compactCount to 0, so the note always
                   describes what THIS response actually did instead of what the argument nominally requested. */
                collector_detail_note = full_detail
                    ? "full_detail=true: every field is served for every collector below."
                    : $"{compactCount} of {rows.Count} collector(s) below are HEALTHY with nothing to report (no errors, denials, session/extension-missing runs or abandoned cycles this window) and are compacted to collector/status/compact/total_runs/rows_stored/avg_duration_ms/last_success. {partialCount} need a look (failing, stale, stopped, erroring, denied, regressed, or an unexplained zero) and carry a leaner partial_detail shape instead of full detail. Pass full_detail=true for every field on every collector.",
                collectors = result,
                /* #3856: how old the collector half of this payload is, in whole seconds — the same field, the
                   same name and the same semantics get_fleet_overview has carried since #3735, because it is
                   now the same kind of reading: one 7-day scan per minute per host per server, shared by every
                   get_collection_health call of that minute. 0 means this call's own statement produced it.

                   Published UNCONDITIONALLY, including on the 0, for the self-proving-flag reason (#3574's
                   visibility, #3735's own): a memo whose hit is indistinguishable from a fresh read has not
                   reported, and a caller watching a collector it just fixed needs to know whether it is reading
                   a minute-old answer before concluding the fix did not take. Trailing, after collectors, so
                   nothing an existing consumer indexes by position moved — the same placement rule every
                   column of the statement behind it follows. */
                collection_health_age_seconds = collectionHealthAgeSeconds
            }, resolved.ServerId, McpHelpers.JsonOptions), McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_collection_health", ex);
        }
    }

    /* #4198: the default-argument size cut for get_collection_health, which unlike a row-limited tool has no
       row to drop — every collector on the server is one row, and a health read must never hide one that is
       failing, stale, disabled or erroring by leaving it off the page. So the cut is per-FIELD instead: a
       collector this predicate calls boring gets the seven-field CompactCollectionHealthRow shape instead of
       the ~30-field full one. Every check here is a fact this window's aggregate already computed, not a new
       read, and each one guards against exactly the "erroring collector went quiet" failure #4198 warns about:
       HealthStatus alone is not enough, because Classify() bands WARNING only above a 20% error rate or a 0.5%
       abandon rate, so a collector could carry a handful of errors, session-missing runs or abandoned cycles
       and still read HEALTHY. RowsStored > 0 (or a known event collector reading zero at rest) rules out the
       "non-event collector came back empty and needs a look" reading FormatOutputFinding would otherwise carry
       — dropped here specifically because it is the one non-obvious way a HEALTHY-banded row can still be
       worth a second look. HealthStatus == Healthy already implies AnyRegression is false (HealthStatus floors
       regressed rows to WARNING, #3819) and PermissionDeniedCount == 0 already implies
       DeniedSinceLastSuccess is false (it requires a PERMISSIONS run to exist), so neither is re-checked here
       — this predicate tests only what HealthStatus does NOT already cover. YieldCount is deliberately absent:
       a yield retries and is benign by design (#1805), unlike the counts checked here. */
    private static bool IsCollectionHealthCompactEligible(CollectorHealth r) =>
        r.HealthStatus == CollectorHealthClassifier.Healthy
        && r.ErrorCount == 0
        && r.SessionMissingCount == 0
        && r.ExtensionMissingCount == 0
        && r.PermissionDeniedCount == 0
        && r.AbandonedCount == 0
        && (r.RowsStored > 0 || CollectorHealthClassifier.IsEventCollector(r.CollectorName));

    /// <summary>The compact shape <see cref="IsCollectionHealthCompactEligible"/> rows get by default: enough
    /// to confirm the collector is fine and cheap (it ran, stored what it should, and did not cost much)
    /// without the ~30 fields a row with nothing to report does not need. <c>compact: true</c> is the caller's
    /// signal that this row was shortened — a full row never carries the property, so its mere presence is
    /// unambiguous without a second lookup against <c>full_detail</c>.</summary>
    private static object CompactCollectionHealthRow(CollectorHealth r) => new
    {
        collector = r.CollectorName,
        status = r.HealthStatus,
        compact = true,
        total_runs = r.TotalRuns,
        rows_stored = r.RowsStored,
        avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
        last_success = r.LastSuccessTime?.ToString("o"),
    };

    /// <summary>#4198: the shape a row that FAILS <see cref="IsCollectionHealthCompactEligible"/> gets by
    /// default -- neither the full ~30-field shape nor <see cref="CompactCollectionHealthRow"/>'s "nothing to
    /// report" seven. Previewing every free-text field alone could not clear the response budget on either
    /// SKU's #4198 fixture (measured, not assumed), so the row itself is cut down to what says a collector is
    /// wrong and since when. <c>partial_detail: true</c> is the caller's signal -- deliberately NOT
    /// <c>compact</c>, whose meaning is "healthy, nothing to report": CollectionHealthPayloadBudgetLiveTests and
    /// its Lite twin assert a row that needs a look never carries <c>compact</c>, specifically so a caller
    /// scanning for <c>compact != true</c> never skips it, and reusing that marker here would defeat that check
    /// silently. full_detail=true restores every field named in the guide above, same as it does for a compact
    /// row. Field-for-field Lite's twin.</summary>
    private static object PartialCollectionHealthRow(CollectorHealth r)
    {
        /* OutputFinding recomputes FormatOutputFinding's sentence on every access -- read it once rather than
           twice (preview, then the truncated comparison). */
        var outputFinding = r.OutputFinding;
        return new
        {
            collector = r.CollectorName,
            status = r.HealthStatus,
            partial_detail = true,
            total_runs = r.TotalRuns,
            errors = r.ErrorCount,
            /* #4198 follow-up: IsCollectionHealthCompactEligible fails a HEALTHY row on session-missing runs,
               abandoned cycles, or a denial even after a later success -- and until now none of the three had a
               field here, so a row failing the predicate ONLY on one of them carried partial_detail: true with
               nothing in the shape saying why. Same names and formats FullCollectionHealthRow uses, so a caller
               already reading the full shape does not learn a second vocabulary for the same facts. Always
               emitted, not conditional on being the reason this row is partial -- McpHelpers.JsonOptions keeps
               nulls, and a reader comparing partial rows across collectors needs the same keys on every one. */
            session_missing = r.SessionMissingCount,
            abandoned = r.AbandonedCount,
            rows_stored = r.RowsStored,
            last_success = r.LastSuccessTime?.ToString("o"),
            last_error = McpHelpers.Truncate(r.LastError, ErrorMessagePreviewLength),
            last_error_truncated = r.LastError is not null && r.LastError.Length > ErrorMessagePreviewLength,
            last_error_at = r.LastErrorTime?.ToString("o"),
            last_denied_at = r.LastDeniedTime?.ToString("o"),
            denied_since_last_success = r.DeniedSinceLastSuccess,
            regressed_from_productive = r.AnyRegression,
            output_finding = McpHelpers.Truncate(outputFinding, OutputFindingPreviewLength),
            output_finding_truncated = outputFinding is not null && outputFinding.Length > OutputFindingPreviewLength,
        };
    }

    [McpServerTool(Name = "get_server_properties"), Description("Gets SQL Server instance properties: edition, version, CPU count, memory, socket/core topology, HADR, clustering, and the clock (utc_offset_minutes, time_zone_id). LATEST IS A TIME: the newest snapshot, not a window; captured_at is when it was collected, and on a stalled collector it is the only sign of staleness. time_zone_id is CURRENT_TIMEZONE_ID() (SQL Server 2022+/Azure SQL only); null means a pre-2022 engine, so only the offset in force at captured_at is known, and an instant across a DST transition from it can read an hour off. <<GUIDE>> Gets SQL Server instance properties: edition, version, CPU count, physical memory, socket/core topology, HADR status, clustering, and the server's clock: utc_offset_minutes is the UTC offset in force when the snapshot was collected, and time_zone_id is the engine's own time-zone name (CURRENT_TIMEZONE_ID(), SQL Server 2022+ and Azure SQL only) - a null time_zone_id means a pre-2022 engine, where only the offset is known and any instant on the far side of a DST transition from the snapshot is placed an hour off by that offset. Use for capacity planning and edition-aware recommendations. LATEST IS A TIME: this reads the newest properties snapshot, not a window, and captured_at is the instant it was collected - a core count or memory figure here is what the server reported AT that stamp, and on a server whose collector has stalled the stamp is the only thing that says how stale it is.")]
    public static async Task<string> GetServerProperties(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var row = await DarlingDataReader.GetLatestServerPropertiesAsync(postgres, resolved.ServerId, cancellationToken);
            if (row == null)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "server_properties", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No server properties available. The properties collector may not have run yet.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3653: captured_at, the #3637 census's one spelling for a latest read's stamp. This tool
                   stamped itself as collection_time before that vocabulary existed and was carried as a
                   named allowance; the web surface read none of its keys by that name, so the cut-over is
                   clean - no alias, because the census is the contract and a second key for one instant is
                   the drift it exists to refuse. */
                captured_at = row.CollectionTime.ToString("o"),
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
                service_objective = string.IsNullOrEmpty(row.ServiceObjective) ? null : row.ServiceObjective,
                /* V134 (#3653 item 13, Q8): the clock pair. The offset is the one IN FORCE at captured_at,
                   which is exact for an instant on the same side of a DST transition and an hour wrong for
                   one on the other (#3231); the zone is what can tell the two apart. NULL is a real answer
                   for the zone - CURRENT_TIMEZONE_ID() is SQL Server 2022+ / Azure SQL only - and the note
                   says what it means rather than leaving a caller to read it as "not collected". Byte-for-byte
                   the keys Lite's tool emits. */
                utc_offset_minutes = row.UtcOffsetMinutes,
                time_zone_id = string.IsNullOrEmpty(row.TimeZoneId) ? null : row.TimeZoneId,
                time_zone_note = string.IsNullOrEmpty(row.TimeZoneId)
                    ? "time_zone_id is null: a pre-2022 engine (CURRENT_TIMEZONE_ID() is SQL Server 2022+ / Azure SQL only), so only the offset in force at captured_at is known."
                    : "time_zone_id is the engine's own zone (CURRENT_TIMEZONE_ID()); utc_offset_minutes is the offset that zone had at captured_at."
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
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
    ///
    /// <para>#3967: the read has no window, but the collection log's retention bounds what it can see, so a
    /// server whose whole history retention has dropped comes back null. Its registration tells that server
    /// (Offline) from one that has never collected (AwaitingFirstCollection), through the same rule the fleet
    /// card applies to its 48-hour window.</para>
    /// </summary>
    internal static string FreshnessStatus(DateTime? lastCollectionUtc, DateTime? registeredAtUtc, DateTime nowUtc) =>
        ServerCollectionStatusRules
            .FromFreshness(ServerHealthClassifier.ClassifyFreshness(
                lastCollectionUtc, registeredAtUtc, DarlingRetentionHorizons.CollectionLogHorizon(nowUtc), nowUtc))
            .McpToken();

    /// <summary>
    /// The empty-read sentence for the FLEET SENTINEL's log, which is a different population from a
    /// monitored server's and needs different words (#3399).
    ///
    /// <para><b>The unfiltered quiet-window sentence is the one that matters.</b> For a monitored server
    /// "this window is genuinely quiet rather than broken" is true and useful. For a maintenance pass it is
    /// backwards: the passes run on a FIXED cadence, so a window wider than the cadence with no row in it
    /// means a pass stopped running — which is the whole reason the row is written on every tick. Emitting
    /// the server sentence here would answer the question this population was added to answer with a
    /// reassurance, in the one direction a wrong answer costs something.</para>
    ///
    /// <para>The filtered sentence is wrong a second way: it tells a caller to check the name against what
    /// <c>get_collection_health</c> lists, and that read is scoped to monitored servers, so it lists neither
    /// of these names and never will.</para>
    ///
    /// <para>Pure, so all three branches are pinnable without a store.</para>
    /// </summary>
    internal static (string State, string Message) FleetMaintenanceLogMiss(
        bool everRecorded,
        string? collectorName,
        double? minDurationMs,
        int hoursBack,
        string? status = null)
    {
        var hours = hoursBack.ToString(CultureInfo.InvariantCulture);

        if (!everRecorded)
        {
            return ("unavailable",
                "No fleet-maintenance run-records have EVER been recorded. Both passes write one on their "
                + "FIRST pass after the service starts — data_retention for the daily purge, "
                + "oversized_plan_sweep for the fifteen-minute oversized-plan backlog drain — so this is a service "
                + "that has not completed a maintenance pass, not an empty window. Check that the service is "
                + "running, and check its log: these writes are failure-isolated and report at Debug.");
        }

        var cadence =
            "The passes run on a FIXED cadence — every fifteen minutes for oversized_plan_sweep, daily for data_retention — "
            + "and every tick writes a row whatever it found, including one that found nothing to do. So an "
            + "absent row over a window wider than the cadence means the pass did not RUN, which is the "
            + "reading these rows exist for. Do NOT check get_collection_health for these names: it is scoped "
            + "to monitored servers and lists neither of them.";

        /* #3869 joins this gate rather than only the server-shaped one below: the maintenance passes write
           SUCCESS and WARNING (a pass whose tables partly failed), so a status filter is a real question to
           ask of this population, and an unnamed filter here would let the cadence sentence claim the pass
           did not RUN when it ran and merely did not match. */
        if (!string.IsNullOrWhiteSpace(collectorName) || minDurationMs is not null || !string.IsNullOrWhiteSpace(status))
        {
            return ("empty",
                $"No fleet-maintenance run-records in the last {hours} hour(s) matched "
                + $"{McpHelpers.DescribeCollectionLogFilters(collectorName, minDurationMs, status)}. "
                + cadence);
        }

        return ("empty",
            $"No fleet-maintenance run-records at all in the last {hours} hour(s). " + cadence);
    }

    /// <summary>
    /// #4198: error_message is this tool's one wide field — DarlingObservability.LogCollectionAsync caps it
    /// at 4000 characters at WRITE time, so a page of failing runs (the exact "show me the failures"
    /// incident-window ask this tool's guide leads with) can still carry a large multiple of that per row.
    /// Previewed to this length per row at default (<c>full_text: true</c> opts back in), the same
    /// preview-plus-opt-in shape <c>get_store_query_stats</c> uses for its own <c>full_text</c>. Also covers
    /// the (fleet) sentinel's SUCCESS rows, where error_message carries the tick's counts rather than a
    /// fault — a long summary line is previewed the same as a long fault.
    /// </summary>
    private const int ErrorMessagePreviewLength = 500;

    /// <summary>#4198: PartialCollectionHealthRow.output_finding's preview length -- shorter than
    /// <see cref="ErrorMessagePreviewLength"/> because it previews a template sentence (FormatOutputFinding),
    /// not a driver/engine error string, and because #4198 measured it as the single dominant cost on a fixture
    /// where every row needs a look and the finding is populated on all of them (~600 B untruncated, on
    /// average). Field-for-field Lite's twin.</summary>
    private const int OutputFindingPreviewLength = 200;

    [McpServerTool(Name = "get_collection_log"), Description("Raw per-run collector log: duration split into monitored-server and store-write time, rows, status, error. NEWEST FIRST by default; min_duration_ms flips it to SLOWEST FIRST, ranked by cost. hours_back is the ask; oldest/newest_returned_collection_time bound what you actually got — under a min_duration_ms floor that is the cost-ranked sample's age, not reach. Filters apply before the cap; truncated/run_count reflect matches. status is the failure filter: an unknown value is refused, never silently empty. get_collection_health is the rollup; this is the underlying runs. <<GUIDE>> Gets the RAW per-run collection log for a server, NEWEST FIRST by default and SLOWEST FIRST whenever min_duration_ms is supplied: one row per collector run with its total duration, the part spent querying the monitored server, the part spent writing to the store, rows collected, status and any error. error_message is a preview by default (ErrorMessagePreviewLength characters, error_message_truncated marks a cut); full_text returns it whole (#4198). get_collection_health rolls seven days of these into a per-collector verdict; this is the underlying runs, which is what you need when the rollup says healthy and collection still looks wrong, or when you want to see what a collector was doing during a specific incident window. READ THE PAGE-SPAN FIELDS BEFORE CONCLUDING ANYTHING FROM THE ROWS. hours_back is the span you ASKED for; oldest_returned_collection_time and newest_returned_collection_time bound the page you GOT, and on a busy fleet those are wildly different — roughly 500 log rows a minute across 50 servers means a 24-hour request at the 200-row default is satisfied by about the last 25 seconds of activity. truncated says the cap bit; the two timestamps say what the page holds. THE TWO FIELDS MEAN DIFFERENT THINGS UNDER THE TWO ORDERINGS and the difference matters: under the default newest-first ordering the page is a contiguous slice of the window's tail, so oldest_returned_collection_time IS how far back this read reached; under a min_duration_ms floor the page is a cost-RANKED sample drawn from the whole window, so it tells you how old the slowest matching runs are and NOTHING about reach. Read order to know which you have. Neither field is a window floor: nothing here probes for the oldest row the window could have held. A read whose newest and oldest are seconds apart has told you nothing about the window you named, and raising limit does NOT fix it under the default ordering because the slow runs are not the recent ones — min_duration_ms is the knob for that, because supplying it ranks by duration instead of by time. All THREE filters are applied in SQL, BEFORE the cap, so truncated and run_count describe the MATCHING rows rather than the unfiltered window. order names which ordering you got, so a caller never has to infer it from the filters it sent. status IS THE FAILURE-HUNTING FILTER and the reason to reach for this tool during an incident: 'show me the failures' is the most common question asked of this log, and without it a caller pages the newest-first tail eyeballing status — which the page-span contract above explains cannot work, because a 200-row page on a busy fleet covers seconds and raising limit does not reach a failure that happened twenty minutes ago. Pass one of SUCCESS, SKIPPED, YIELDED, ABANDONED, ERROR, PERMISSIONS, EXTENSION_MISSING, SESSION_MISSING, WARNING (case-insensitive); an unknown value is REFUSED and the refusal names the whole set, rather than being applied as an equality filter that returns an empty page a caller would read as 'no failures'. get_collection_health is not this question's answer either: it carries one last_error per collector over a seven-day rollup, not the runs, their timestamps or their sequence — which is what says whether every collector failed at 03:41 or one collector failed all night. A status filter changes the page from a contiguous tail to a filtered one, so read the two page-span timestamps the same way you would under a duration floor. The filter you sent is echoed back as status_filter (not status, which on an empty result is the miss word instead), in the stored UPPERCASE spelling whatever case you sent. Also carries the phase decomposition where the run recorded one, as nested blocks that are null when the run took a path that does not report them — and a row carries at most ONE family. Server-scoped collectors fill sql_phases (open_ms, drain_ms, other_ms which is derived, watermark_ms) and drain (rows_read, bytes_read, last_read_ms, target_session_id). Per-database collectors that perform a deferred plan or statement-text fetch instead fill plan_fetch and/or text_fetch, each carrying probe_ms, target_ms, write_ms, ids_attempted and probe_ids summed across that run's databases. sweep_peer_max_ms is flat and present on every row: it is the slowest peer collector in the same sweep, the denominator for asking whether a slow run was slow alone or the whole sweep was. A null block means the run took the other path, not that the phase was free — most runs perform no deferred fetch at all. Divide target_ms by ids_attempted for the per-id target cost, probe_ms by probe_ids for the per-reference probe cost. CRITICAL for reading sql_duration_ms on a fetching collector: it is NOT purely target-side there. The deferred fetches run inside the driver's per-item SQL stopwatch and each one round-trips the MONITORING STORE to decide what plan XML and statement text are already held before writing back what came off the target, so the store's probe and write are billed to the column documented as the monitored server's. The probe is the largest single term in both fetches on this fleet — 55.4% of plan_fetch and 80.6% of text_fetch — and on one production run it was 107,334 ms of a 124,972 ms sql_duration_ms, 86%, against a plan-plus-text target time of 6,494 ms. sql_store_ms is that store share, derived from the two fetch blocks (probe_ms + write_ms of each) and null when no fetch ran. It is a FLOOR, not the whole: the per-item watermark refresh is also a store read inside the same stopwatch, the enumerated path records no watermark_ms, and that component is stored nowhere — so sql_duration_ms minus sql_store_ms is an UPPER bound on target-side time rather than the target-side time. store_duration_ms is not where the probe went either: it is the binary COPY of the collected rows and nothing else. Do NOT conclude a monitored server is slow from a large sql_duration_ms on query_store without reading sql_store_ms beside it. THE RESERVED server_name (fleet) READS THE FLEET-MAINTENANCE RUN-RECORDS instead of a monitored server's collector runs: the passes that iterate the whole fleet have no one server to attribute a run to, so they log under a sentinel that is not in the server list — data_retention for the daily purge, oversized_plan_sweep for the fifteen-minute oversized-plan backlog drain. Read those rows by their ABSENCE as much as their contents: every tick writes one whatever it found, including a tick that found an empty backlog and captured nothing, so rows_collected = 0 means the pass ran and had nothing to fetch while a MISSING row past the pass's cadence means the pass did not run at all. That is the only way to tell those two apart. error_message carries the tick's counts on a SUCCESS row (servers swept, plans claimed, captured, expired, fetch failures); sql_duration_ms is the time inside the monitored-server fetches and store_duration_ms the rest of the tick. These rows are excluded from get_collection_health and from get_fleet_overview by design — they are maintenance passes, not collectors, so a per-server staleness ladder does not apply to them. Five parameters carry more guidance than their 200-character cap allows; the rest of each below. collector_name: A name this server has never run returns the no-matches status rather than a quiet-window one. min_duration_ms: Applied in SQL before the cap. 0 is a real value: it admits every run and is how you ask for the whole window ranked by cost. A negative is refused. Omit for no floor and newest-first order. status: THE FAILURE FILTER — 'show me the failures' is what this log exists to answer, and paging the newest-first tail cannot reach a failure that is not recent. An unknown value is REFUSED, naming the accepted set, rather than applied as a filter that matches nothing. Omit for every status. server_name: Omitted, blank, or \"*\" reads the WHOLE FLEET (#4199) — every enabled server's runs, merged and ranked together, each row carrying server_name — instead of one server; this is different from the reserved name (fleet), which still reads the fleet-MAINTENANCE run-records and still requires being named exactly. limit: Default McpResponseBudget.CollectionLogPerServerDefaultLimit (58) for one server (#4198: sized so a default call stays under the shared response-size target on the wider SQL Server-target row shape). The fleet-wide form (server_name omitted or \"*\") defaults instead to McpResponseBudget.CollectionLogFleetDefaultLimit, sized the same way; pass limit explicitly for more rows either way.")]
    public static async Task<string> GetCollectionLog(
        NpgsqlDataSource postgres,
        [Description("Server name or display name. Omit or pass \"*\" for the WHOLE FLEET (every enabled server's runs, merged; see tool guide) — differs from the reserved name (fleet).")] string? server_name = null,
        [Description("Hours of history. Default 24. No upper bound (this read exists to look further back than the 168-hour reads allow); a negative or zero value is refused rather than read as its absolute value.")] int hours_back = 24,
        [Description("Maximum rows to return, applied after the filters. Default 58 for one server; the fleet-wide form (server_name omitted or \"*\") defaults lower — see tool guide.")] int? limit = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        /*
            APPENDED after as_of rather than grouped beside `limit`, and this is not tidiness deferred.
            as_of is `string?` and collector_name is `string?`, so a C# call site passing the anchor
            POSITIONALLY -- which one shipped test does -- would silently rebind it to the collector filter
            and compile without a word. The read would then refuse to match any collector and return the
            no-matches status for a call that asked about a past incident: a silently-different answer, which
            is the failure class these filters were added to remove. MCP invokes by name, so the position
            costs a client nothing; a positional C# caller is the only observer, and appending is what keeps
            every existing one meaning what it already meant.
        */
        [Description("Limit to one collector, matched EXACTLY (query_store, plan_correction, wait_stats — the names get_collection_health lists). Omit for every collector.")] string? collector_name = null,
        [Description("Return only runs whose total duration_ms is at or above this floor, AND rank the page SLOWEST FIRST rather than newest first — a floor under newest-first ordering still cannot reach the tail.")] double? min_duration_ms = null,
        /*
            #3869, APPENDED for the reason collector_name was: every filter joins the end of this list so no
            positional C# caller changes meaning. This one is the log's own stored vocabulary, so it is a
            ValidateChoice parameter rather than free text.
        */
        [Description("Limit to runs with this status, matched case-insensitively against the log's own vocabulary: SUCCESS, SKIPPED, YIELDED, ABANDONED, ERROR, PERMISSIONS, EXTENSION_MISSING, SESSION_MISSING, WARNING.")] string? status = null,
        /*
            #4198, APPENDED for the reason collector_name and status were: every filter joins the end of
            this list so no positional C# caller changes meaning. error_message is the tool's one wide
            field (up to 4000 characters at write time, DarlingObservability.LogCollectionAsync's own
            ceiling) and is previewed at default the same shape get_store_query_stats already uses for
            full_text.
        */
        [Description("Return each run's error_message in full instead of a preview. Default false.")] bool full_text = false,
        CancellationToken cancellationToken = default)
    {
        /* #4199: server_name OMITTED, blank, or "*" means the WHOLE FLEET rather than "auto-select the
           lone server" or "which server did you mean" -- the read whose subject is the log itself is also
           the one whose most common fleet-shaped question ("which servers' runs of collector X were slow
           this hour") has no single server to name, and today costs one call per server. Checked BEFORE
           the resolve below and deliberately NOT built into DarlingServerResolver: every other tool's
           omitted name still means "the one server, or tell me which", and only this read's omission gets
           a fleet-wide reading. The (fleet) SENTINEL is a different, explicit ask -- the fleet-MAINTENANCE
           run-records, not a fleet-wide page of every server's ordinary runs -- so it is untouched by this
           check and falls through to the resolve below exactly as it always has. */
        if (string.IsNullOrWhiteSpace(server_name) || server_name.Trim() == "*")
        {
            return await GetCollectionLogFleetAsync(
                postgres, hours_back, limit ?? McpResponseBudget.CollectionLogFleetDefaultLimit,
                as_of, collector_name, min_duration_ms, status, cancellationToken);
        }

        /* limit is nullable so the fleet branch above and this per-server branch can default it
           differently (#4199 wants a smaller fleet-wide default; #4198 sizes the per-server default
           against the wider of the two target-kind row shapes — see CollectionLogPerServerDefaultLimit)
           — a caller omitting the argument is otherwise indistinguishable from one passing the C#
           default explicitly. Resolved once, here, so everything below reads one concrete int exactly
           as it did before this became nullable. */
        var effectiveLimit = limit ?? McpResponseBudget.CollectionLogPerServerDefaultLimit;

        /* The SENTINEL-AWARE resolve, and this read is the only one that takes it (#3399): its subject is
           the log itself, so the fleet-maintenance run-records have to be nameable here or they answer
           nothing. Every other tool keeps the registry-only resolve, where "(fleet)" is correctly a miss. */
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorWithFleetSentinelAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        /* The shared row-cap contract every sibling read uses: rejects out of range rather than
           silently clamping, so a caller asking for 5000 is told no instead of quietly given 1000. */
        var invalidLimit = McpHelpers.ValidateTop(effectiveLimit);
        if (invalidLimit != null) return invalidLimit;

        /* Same contract for the floor, and for the same reason one step further on: supplying it also
           switches the ORDERING, so a negative quietly read as "no floor" would hand back a
           duration-ranked full page with nothing to say the filter was ignored. */
        var invalidFloor = McpHelpers.ValidateMinMs(min_duration_ms, "min_duration_ms");
        if (invalidFloor != null) return invalidFloor;

        /*
            #3869: the status VALUE is validated as loudly as #3870 makes an unknown ARGUMENT NAME a hard
            error, and deliberately so — the two halves of the same promise. An unknown key and an unknown
            value are the same caller mistake seen from two angles, and the quiet failure mode is identical:
            a read that looks like it answered. An equality filter on a misspelled status returns an empty
            page, and on THIS tool that page reads as "no failures in the window" — the single most
            dangerous false negative this surface can produce, since the caller asked the question during an
            incident. So the closed set is refused by name rather than applied, following #3541 A13's
            get_analysis_facts ruling, and the refusal prints the whole vocabulary because a caller cannot
            otherwise discover a set the store's writers define.
        */
        var invalidStatus = McpHelpers.ValidateChoice(
            status, EnumeratedCollectorDriver.CollectionLogStatuses, "status");
        if (invalidStatus != null) return invalidStatus;

        /* ValidateUncappedWindow, deliberately NOT ValidateWindow. These three reads have never capped
           hours_back, so routing them through the shared validator would impose the 168-hour ceiling every
           other read carries and take reach away from exactly the read whose premise is looking FURTHER back
           than the default. What they no longer do is Math.Abs() a negative span (#3541 A13): a window that
           ends before it starts is a caller error, and flipping the sign answered a different question with
           nothing to say so. Refused, like every other unusable parameter here. */
        var anchorError = McpHelpers.ValidateUncappedWindow(hours_back, as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);

            /* Over-fetch by one so truncation is OBSERVED rather than inferred. Comparing count to the
               cap cannot tell a window holding exactly `limit` runs from one holding more, and this
               read's whole premise is that the cap announces itself instead of being guessed at.

               The filters go INTO the read, so the over-fetch is of the FILTERED set and `truncated`
               keeps meaning "more rows match than you were given". Filtering the returned page instead
               would make it mean "more rows were in the window", which is a different sentence under the
               same field name. */
            var rows = await DarlingDataReader.GetCollectionLogAsync(
                postgres, resolved.ServerId, start, end, effectiveLimit + 1, collector_name, min_duration_ms, status, cancellationToken);
            var truncated = rows.Count > effectiveLimit;
            if (truncated) rows = rows.Take(effectiveLimit).ToList();

            var filtered = !string.IsNullOrWhiteSpace(collector_name)
                || min_duration_ms is not null
                || !string.IsNullOrWhiteSpace(status);

            if (rows.Count == 0)
            {
                /*
                    Zero rows is now THREE completely different facts and they need different answers.
                    A server that has collected before and simply did nothing in THIS window is a
                    true negative -- the caller narrowed to a quiet period, and widening the window
                    is the move. A server with no log rows at all has never collected, which is a
                    fault, and telling that caller "nothing in the last 24 hours" would send them
                    off widening a window that will never fill. So we ask which one it is rather
                    than emitting one sentence that is true of both.

                    The third is a filter that matched nothing, and it is the one that would have been
                    filed as a defect the day after the filters shipped: a misspelled collector_name or a
                    floor above every run in the window reaches the same zero, and the quiet-window
                    sentence would then assert the window is quiet on a read that never looked at the
                    whole window. It also names the filters back, because the caller cannot otherwise tell
                    a rejected value from an honestly empty match.

                    THE NEVER-COLLECTED CHECK COMES FIRST, ahead of the filter branch, and the order is the
                    whole correctness of this block. A fault outranks a miss: on a server that has never
                    collected, "the filters were applied, so unfiltered runs may well exist -- drop them to
                    see what the window holds" is FALSE, and it sends the caller to widen and unfilter a
                    window that will never fill. That is the same defect the two original branches exist to
                    prevent, reintroduced by the branch added to prevent it -- so the filters can only ever
                    narrow the answer given to a server that HAS collected. It costs one LIMIT 1 probe on a
                    path that already returned no rows.
                */
                var everCollected = await DarlingDataReader.HasAnyCollectionLogAsync(postgres, resolved.ServerId, cancellationToken);

                /* The sentinel gets its OWN three sentences, ahead of the server-shaped ones, because all
                   three of those are false about a maintenance pass — and one is false in the reassuring
                   direction on exactly the question this population exists to answer. See
                   FleetMaintenanceLogMiss. The server branches below are untouched. */
                if (resolved.ServerId == DarlingObservability.FleetServerId)
                {
                    var (state, text) = FleetMaintenanceLogMiss(
                        everCollected, collector_name, min_duration_ms, hours_back, status);

                    return McpHelpers.Status(state, text);
                }

                if (!everCollected)
                {
                    return McpHelpers.Status(
                        "unavailable",
                        $"No collector runs have EVER been recorded for {resolved.ServerName}. This is not an empty window — collection has not run at all for this server. Check that the service is running and that the server is enabled for collection; get_collection_health will be equally empty until it does.");
                }

                if (filtered)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No collector runs on {resolved.ServerName} in the last {hours_back} hour(s) matched {McpHelpers.DescribeCollectionLogFilters(collector_name, min_duration_ms, status)}. This says nothing about the window as a whole — the filters were applied, so unfiltered runs may well exist. Drop them to see what the window holds, and check collector_name against the names get_collection_health lists, since it is matched exactly.");
                }

                return McpHelpers.Status(
                    "empty",
                    $"No collector runs recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent runs.");
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

                    #3192: and on the ENUMERATED path that split does not fall where these two columns
                    put it. sql_duration_ms is the driver's per-item stopwatch, which wraps the whole
                    readItem closure -- and for query_store that closure round-trips the STORE to decide
                    what plan XML and statement text are already held, then writes back what came off the
                    target. So the store's probe and write are billed to the target's column, and on this
                    fleet the probe is the largest single term in both fetches (55.4% of plan_fetch, 80.6%
                    of text_fetch; 107,334 of a 124,972 ms run). store_duration_ms is NOT where that time
                    went either -- it is the binary COPY of the collected rows and nothing else, which is
                    the measurement ServiceCommandDeadlines derives the COPY deadline from.

                    So sql_store_ms names it instead, derived from the fetch blocks below rather than
                    stored (the SqlOtherMs / #2859 rule), which also makes it RETROACTIVE to every row
                    written since V110 instead of only to rows written after this change. Deliberately
                    NOT a correction applied to sql_duration_ms itself: that column feeds
                    collect.collector_cost, a 90-day hourly series that carries no phase split and is
                    written from an in-memory accumulator rather than re-aggregated from this table, so
                    the past could not be corrected to match and a re-based column would make the series
                    a step function across the deploy -- under a self-alert whose baseline window is 14
                    days. The number stays; the attribution arrives beside it.
                */
                sql_duration_ms = r.SqlDurationMs is null ? (double?)null : Math.Round(r.SqlDurationMs.Value, 0),
                store_duration_ms = r.StoreDurationMs is null ? (double?)null : Math.Round(r.StoreDurationMs.Value, 0),
                /* Flat and nullable rather than inside a block, like sweep_peer_max_ms: it decomposes
                   sql_duration_ms (the sql_ prefix carries that, V108's convention) and belongs to neither
                   fetch half, being the sum of both halves' store terms. NULL means no deferred fetch ran,
                   so nothing is attributable -- never "the store share was zero". */
                sql_store_ms = r.SqlStoreMs is null ? (double?)null : Math.Round(r.SqlStoreMs.Value, 0),
                rows_collected = r.RowsCollected,
                status = r.Status,
                /* #4198: a preview by default — see ErrorMessagePreviewLength's doc comment above this
                   method — with full_text opting back into the whole (up to 4000-character) field. */
                error_message = full_text ? r.ErrorMessage : McpHelpers.Truncate(r.ErrorMessage, ErrorMessagePreviewLength),
                error_message_truncated = !full_text && r.ErrorMessage is not null && r.ErrorMessage.Length > ErrorMessagePreviewLength,
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
                /* The span REQUESTED. Kept under its shipped name, and no longer the only span reported --
                   see the two timestamps below. */
                hours_back = hours_back,
                run_count = rows.Count,
                /* Observed by the over-fetch above, not inferred from the row count. */
                truncated,
                /*
                    #3287: the span this PAGE covers, beside the span requested.

                    `hours_back = 24` next to `truncated = true` told a caller the cap bit and then showed
                    them the 24 hours as if that were the window -- an instrument reporting a span it did
                    not measure. On the production fleet a 24-hour request at the default cap is satisfied
                    by roughly the last 25 SECONDS of activity, and nothing in the payload said so, which
                    is how a 114,331 ms run went unfound behind a page whose maximum was 19,064 ms.

                    The _returned_ in these names is load-bearing and is the whole reason the unqualified
                    spellings are not used. (Not written out here: a pin forbids them across this file, and
                    a correction that names what it corrected would be the one occurrence that defeats it.)
                    Under the DEFAULT ordering the page is a contiguous slice of the window's tail, so its
                    oldest row IS how far back the read reached -- the #3287 figure. Under a duration floor
                    it is a cost-RANKED sample drawn from the whole window, so its oldest row says how old
                    the slowest runs happen to be and says NOTHING about reach. QueryStoreTopWindowTests
                    states the general form of that trap for get_query_store_top, where the rows are ALWAYS
                    cost-ranked and the window therefore has to come from a bounded probe; here the rows are
                    reach under one ordering and a sample under the other, so the field NAMES what it is and
                    the tool description says which case is which, rather than one number quietly meaning
                    two things depending on a parameter the caller may not have sent.

                    Both ends, not just the oldest, because under either ordering the newest row is not
                    necessarily recent (an as_of anchor or a stalled collector both move it), so a caller
                    cannot bound the page from the oldest alone.

                    Computed over the rows rather than from rows[0] / rows[^1]: those are the same thing
                    only under time ordering, and the ranked page would silently report the wrong ends
                    while the time-ordered test kept passing.
                */
                oldest_returned_collection_time = rows.Min(r => r.CollectionTime).ToString("o"),
                newest_returned_collection_time = rows.Max(r => r.CollectionTime).ToString("o"),
                /* Which ordering the page actually came back in. Stated rather than left to be inferred
                   from whether min_duration_ms was sent, because the first sentence of this tool's
                   description is the only other place that coupling is written down. */
                order = min_duration_ms is null
                    ? McpHelpers.CollectionLogOrderNewestFirst
                    : McpHelpers.CollectionLogOrderSlowestFirst,
                /* Echoed back so the filters that produced this page are on the page. run_count and
                   truncated describe the MATCHING rows, and that sentence is unreadable without them. */
                collector_name = string.IsNullOrWhiteSpace(collector_name) ? null : collector_name.Trim(),
                min_duration_ms,
                /*
                    #3869. Echoed in the STORED spelling rather than the caller's, because the filter matched
                    case-insensitively and a page echoing "error" beside rows whose status field reads "ERROR"
                    would invite a client to compare the two and conclude the filter had not applied.

                    Spelled status_FILTER, breaking the convention its two neighbours follow of echoing a
                    filter under the parameter's own name, and the exception is the point: `status` is already
                    a key on this tool's OTHER branch, where McpHelpers.Status puts the miss word (empty /
                    unavailable). A client reading `.status` off this tool would get "empty" on a miss and
                    "ERROR" on a hit -- one key meaning two unrelated things depending on a branch, which is
                    the defect the _returned_ names and the truncated/window_truncated split were both made
                    to remove. Each ROW still carries its own `status`, where there is nothing to collide
                    with.
                */
                status_filter = string.IsNullOrWhiteSpace(status) ? null : status.Trim().ToUpperInvariant(),
                runs = result,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_collection_log", ex);
        }
    }

    /// <summary>
    /// The FLEET-WIDE form of <see cref="GetCollectionLog"/> (#4199): server_name omitted, blank or "*"
    /// lands here instead of the per-server branch above. Same four validations, same filters, same
    /// truncated/ordering contract — merged across every enabled server rather than scoped to one, with
    /// each run carrying which server it came from. Kept as a SEPARATE method rather than a branch woven
    /// through the per-server one above: the two response shapes differ (no single `server`, an added
    /// `server_name` per row), and a shared method threading both through the same JSON builder would be
    /// harder to read than the small validation duplication costs.
    /// </summary>
    private static async Task<string> GetCollectionLogFleetAsync(
        NpgsqlDataSource postgres,
        int hours_back,
        int limit,
        string? as_of,
        string? collector_name,
        double? min_duration_ms,
        string? status,
        CancellationToken cancellationToken = default)
    {
        var invalidLimit = McpHelpers.ValidateTop(limit);
        if (invalidLimit != null) return invalidLimit;

        var invalidFloor = McpHelpers.ValidateMinMs(min_duration_ms, "min_duration_ms");
        if (invalidFloor != null) return invalidFloor;

        var invalidStatus = McpHelpers.ValidateChoice(
            status, EnumeratedCollectorDriver.CollectionLogStatuses, "status");
        if (invalidStatus != null) return invalidStatus;

        var anchorError = McpHelpers.ValidateUncappedWindow(hours_back, as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);

            /* Over-fetch by one, exactly like the per-server read, so truncated is OBSERVED across the
               merged fleet page rather than inferred. */
            var rows = await DarlingDataReader.GetCollectionLogFleetAsync(
                postgres, start, end, limit + 1, collector_name, min_duration_ms, status, cancellationToken);
            var truncated = rows.Count > limit;
            if (truncated) rows = rows.Take(limit).ToList();

            var filtered = !string.IsNullOrWhiteSpace(collector_name)
                || min_duration_ms is not null
                || !string.IsNullOrWhiteSpace(status);

            if (rows.Count == 0)
            {
                /* Same three-way split as the per-server branch, over the whole enabled fleet instead of
                   one server: never-collected (every server newly added, or the service never started)
                   outranks a filter miss, which outranks a genuinely quiet fleet-wide window. */
                var everCollected = await DarlingDataReader.HasAnyCollectionLogFleetAsync(postgres, cancellationToken);

                if (!everCollected)
                {
                    return McpHelpers.Status(
                        "unavailable",
                        "No collector runs have EVER been recorded for any enabled server. This is not an empty window — either no server is enabled yet, or collection has not run at all. Check that the service is running and that at least one server is enabled for collection.");
                }

                if (filtered)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No collector runs fleet-wide in the last {hours_back} hour(s) matched {McpHelpers.DescribeCollectionLogFilters(collector_name, min_duration_ms, status)}. This says nothing about the window as a whole — the filters were applied, so unfiltered runs may well exist. Drop them to see what the window holds.");
                }

                return McpHelpers.Status(
                    "empty",
                    $"No collector runs recorded fleet-wide in the last {hours_back} hour(s). At least one enabled server has collected before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent runs.");
            }

            var result = rows.Select(fr => new
            {
                /* The one field this shape adds over the per-server page: which server each merged row
                   came from. Everything else below is the per-server row verbatim, so a client that
                   already parses one page's `runs` needs only this one extra field to parse the other. */
                server_name = fr.ServerName,
                collector = fr.Entry.CollectorName,
                collection_time = fr.Entry.CollectionTime.ToString("o"),
                duration_ms = fr.Entry.DurationMs is null ? (double?)null : Math.Round(fr.Entry.DurationMs.Value, 0),
                sql_duration_ms = fr.Entry.SqlDurationMs is null ? (double?)null : Math.Round(fr.Entry.SqlDurationMs.Value, 0),
                store_duration_ms = fr.Entry.StoreDurationMs is null ? (double?)null : Math.Round(fr.Entry.StoreDurationMs.Value, 0),
                sql_store_ms = fr.Entry.SqlStoreMs is null ? (double?)null : Math.Round(fr.Entry.SqlStoreMs.Value, 0),
                rows_collected = fr.Entry.RowsCollected,
                status = fr.Entry.Status,
                error_message = fr.Entry.ErrorMessage,
                sql_phases = fr.Entry.SqlOpenMs is null && fr.Entry.SqlDrainMs is null && fr.Entry.WatermarkMs is null ? null : new
                {
                    open_ms = fr.Entry.SqlOpenMs is null ? (double?)null : Math.Round(fr.Entry.SqlOpenMs.Value, 0),
                    drain_ms = fr.Entry.SqlDrainMs is null ? (double?)null : Math.Round(fr.Entry.SqlDrainMs.Value, 0),
                    other_ms = fr.Entry.SqlOtherMs is null ? (double?)null : Math.Round(fr.Entry.SqlOtherMs.Value, 0),
                    watermark_ms = fr.Entry.WatermarkMs is null ? (double?)null : Math.Round(fr.Entry.WatermarkMs.Value, 0),
                },
                drain = fr.Entry.DrainRowsRead is null && fr.Entry.DrainBytesRead is null
                        && fr.Entry.DrainLastReadMs is null && fr.Entry.TargetSessionId is null ? null : new
                {
                    rows_read = fr.Entry.DrainRowsRead,
                    bytes_read = fr.Entry.DrainBytesRead,
                    last_read_ms = fr.Entry.DrainLastReadMs is null ? (double?)null : Math.Round(fr.Entry.DrainLastReadMs.Value, 0),
                    target_session_id = fr.Entry.TargetSessionId,
                },
                sweep_peer_max_ms = fr.Entry.SweepPeerMaxMs is null ? (double?)null : Math.Round(fr.Entry.SweepPeerMaxMs.Value, 0),
                plan_fetch = fr.Entry.PlanFetchProbeMs is null ? null : new
                {
                    probe_ms = Math.Round(fr.Entry.PlanFetchProbeMs.Value, 0),
                    target_ms = fr.Entry.PlanFetchTargetMs is null ? (double?)null : Math.Round(fr.Entry.PlanFetchTargetMs.Value, 0),
                    write_ms = fr.Entry.PlanFetchWriteMs is null ? (double?)null : Math.Round(fr.Entry.PlanFetchWriteMs.Value, 0),
                    ids_attempted = fr.Entry.PlanFetchIdsAttempted,
                    probe_ids = fr.Entry.PlanFetchProbeIds,
                },
                text_fetch = fr.Entry.TextFetchProbeMs is null ? null : new
                {
                    probe_ms = Math.Round(fr.Entry.TextFetchProbeMs.Value, 0),
                    target_ms = fr.Entry.TextFetchTargetMs is null ? (double?)null : Math.Round(fr.Entry.TextFetchTargetMs.Value, 0),
                    write_ms = fr.Entry.TextFetchWriteMs is null ? (double?)null : Math.Round(fr.Entry.TextFetchWriteMs.Value, 0),
                    ids_attempted = fr.Entry.TextFetchIdsAttempted,
                    probe_ids = fr.Entry.TextFetchProbeIds,
                },
            });

            return JsonSerializer.Serialize(new
            {
                /* No single `server`: `scope` is the fleet-form's own signal, so a client branching on
                   shape does not have to infer it from a null/absent field. */
                scope = "fleet",
                hours_back,
                run_count = rows.Count,
                truncated,
                oldest_returned_collection_time = rows.Min(r => r.Entry.CollectionTime).ToString("o"),
                newest_returned_collection_time = rows.Max(r => r.Entry.CollectionTime).ToString("o"),
                order = min_duration_ms is null
                    ? McpHelpers.CollectionLogOrderNewestFirst
                    : McpHelpers.CollectionLogOrderSlowestFirst,
                collector_name = string.IsNullOrWhiteSpace(collector_name) ? null : collector_name.Trim(),
                min_duration_ms,
                status_filter = string.IsNullOrWhiteSpace(status) ? null : status.Trim().ToUpperInvariant(),
                runs = result,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_collection_log", ex);
        }
    }

    [McpServerTool(Name = "get_current_waits_trend"), Description("Gets the two Current Waits series over time for a server: waiting-task total wait duration per wait type per collection, and blocked-session counts per database per collection. get_waiting_tasks answers 'what is waiting right now' and can never say whether it is worse than an hour ago — this is that question. Use it to tell a server that is always mildly blocked from one that just started, and to see which database owns the blocking over the window rather than in one snapshot.")]
    public static async Task<string> GetCurrentWaitsTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4. No upper bound (this read exists to look further back than the 168-hour reads allow); a negative or zero value is refused rather than read as its absolute value.")] int hours_back = 4,
        [Description("Limit the blocked-session series to one database. Omit for all databases.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        /* ValidateUncappedWindow, deliberately NOT ValidateWindow. These three reads have never capped
           hours_back, so routing them through the shared validator would impose the 168-hour ceiling every
           other read carries and take reach away from exactly the read whose premise is looking FURTHER back
           than the default. What they no longer do is Math.Abs() a negative span (#3541 A13): a window that
           ends before it starts is a caller error, and flipping the sign answered a different question with
           nothing to say so. Refused, like every other unusable parameter here. */
        var anchorError = McpHelpers.ValidateUncappedWindow(hours_back, as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);

            var waits = await DarlingDataReader.GetWaitingTaskTrendAsync(postgres, resolved.ServerId, start, end, cancellationToken);
            var blocked = await DarlingDataReader.GetBlockedSessionTrendAsync(
                postgres, resolved.ServerId, start, end, database_name, cancellationToken);

            if (waits.Count == 0 && blocked.Count == 0)
            {
                /*
                    Both series empty is two facts again, and here the wrong one is actively reassuring:
                    "nothing was waiting" reads as an all-clear, while the truth may be that the
                    waiting_tasks collector never ran. A caller told all-clear stops looking.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "waiting_tasks", cancellationToken);
                if (gated != null)
                {
                    return gated;
                }

                var everCollected = await DarlingDataReader.HasAnyWaitingTaskSampleAsync(postgres, resolved.ServerId, cancellationToken);
                return everCollected
                    ? McpHelpers.Status(
                        "empty",
                        $"Nothing was waiting on {resolved.ServerName} in the last {hours_back} hour(s). The collector HAS sampled this server, so this is a genuine all-clear for the window rather than missing data.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No waiting-task samples have EVER been recorded for {resolved.ServerName}, so this is NOT an all-clear — there is nothing to read. Check that collection is running for this server before concluding it was quiet.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back = hours_back,
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_current_waits_trend", ex);
        }
    }

    [McpServerTool(Name = "get_blocking_stats"), Description("Gets blocking SEVERITY over time for a server: per-minute blocking duration (event count, total, max and average wait) and per-minute deadlock severity (victim count plus total, max and average wait across every process in the graphs). get_blocking_trend and get_deadlock_trend count incidents; this is how BAD they were. Ten one-second blocks and one ten-minute block are the same count and are not the same problem, which is the distinction this read exists to make.")]
    public static async Task<string> GetBlockingStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24. No upper bound (this read exists to look further back than the 168-hour reads allow); a negative or zero value is refused rather than read as its absolute value.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        /* ValidateUncappedWindow, deliberately NOT ValidateWindow. These three reads have never capped
           hours_back, so routing them through the shared validator would impose the 168-hour ceiling every
           other read carries and take reach away from exactly the read whose premise is looking FURTHER back
           than the default. What they no longer do is Math.Abs() a negative span (#3541 A13): a window that
           ends before it starts is a caller error, and flipping the sign answered a different question with
           nothing to say so. Refused, like every other unusable parameter here. */
        var anchorError = McpHelpers.ValidateUncappedWindow(hours_back, as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);

            var blocking = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, resolved.ServerId, start, end, cancellationToken);

            /* Parsed and bucketed by the shared aggregator rather than re-derived here: a second copy of
               "what counts as a victim" is how two surfaces end up disagreeing about one deadlock. */
            var graphs = await DarlingDataReader.GetDeadlockGraphsAsync(postgres, resolved.ServerId, start, end, cancellationToken);
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
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "blocked_process_report", cancellationToken);
                if (gated != null)
                {
                    return gated;
                }

                var everRan =
                    await DarlingBlockingTrendReader.HasAnyBlockingCollectorRunAsync(postgres, resolved.ServerId, cancellationToken)
                    || await DarlingBlockingTrendReader.HasAnyDeadlockCollectorRunAsync(postgres, resolved.ServerId, cancellationToken);
                return everRan
                    ? McpHelpers.Status(
                        "empty",
                        $"No blocking or deadlocks recorded for {resolved.ServerName} in the last {hours_back} hour(s). The blocking collectors HAVE run successfully for this server, so the window is genuinely clear rather than blind.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"The blocking collectors have NEVER run successfully for {resolved.ServerName}, so this is NOT a clean bill of health — nothing looked. Blocked-process reports need the XE session running, or the DMV blocking snapshot collector enabled; check those before concluding this server does not block.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back = hours_back,
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_blocking_stats", ex);
        }
    }
}
