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
/// The windowed-trend data-read MCP tools — get_memory_trend / get_perfmon_trend / get_file_io_trend /
/// get_query_trend / get_query_duration_trend / get_procedure_duration_trend /
/// get_query_store_duration_trend — served over Darling's Postgres store. These are the trend
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
/// "empty" miss; the three unkeyed trends now distinguish the two kinds of nothing (#2485) rather than
/// collapsing both into one "unavailable" — a quiet window answers "empty" and tells the caller to widen
/// it, while a server the collector has never sampled answers "unavailable" and says so outright. A
/// response-shape change here must land in Lite's Mcp*Tools too, and vice versa.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpTrendTools
{
    [McpServerTool(Name = "get_memory_trend"), Description("Gets memory usage trend over time: total server memory, target memory, buffer pool, plan cache, and granted memory. Useful for identifying memory growth patterns or pressure periods.")]
    public static async Task<string> GetMemoryTrend(
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
            var points = await DarlingTrendReader.GetMemoryTrendAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (points.Count == 0)
            {
                /*
                    "No memory trend data available" was true of two opposite states and told the caller
                    neither. A server that collected fine and was simply quiet in THIS window wants the
                    window widened; a server the collector has never touched wants somebody to go look at
                    collection, and widening will never fill it. Probed only here, on the path that already
                    found nothing, against the SAME source the trend read.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await DarlingTrendReader.HasAnyMemoryStatAsync(postgres, resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No memory samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected memory stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No memory stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the memory_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_memory_stats will be equally empty until it does.");
            }

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
            var points = await DarlingTrendReader.GetPerfmonTrendAsync(postgres, resolved.ServerId, counter_name, start, now);
            if (points.Count == 0)
            {
                /* The engine question comes BEFORE the distinct-counter probe, not after it. Both are on
                   the miss path, so either order keeps the property that matters — but a permanently gated
                   engine takes this branch on every call, forever, and neither the probe nor the PLE branch
                   below could tell it anything. Asking first makes that case one query instead of two. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "perfmon_stats");
                if (gated != null)
                {
                    return gated;
                }

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
            var points = await DarlingTrendReader.GetFileIoLatencyTrendAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (points.Count == 0)
            {
                /* Same two states as the memory trend, same probe discipline. The quiet-window sentence
                   carries one extra clause the others do not need: this read's top_files CTE requires
                   delta_reads or delta_writes above zero, so a genuinely idle file set is empty here even
                   on a server whose file_io_stats collector ran every cycle. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "file_io_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await DarlingTrendReader.HasAnyFileIoStatAsync(postgres, resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No file I/O samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected file I/O stats before, so this window is genuinely quiet rather than broken — widen hours_back, or read it as no measurable read or write activity on any file in this window.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No file I/O stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the file_io_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_file_io_stats will be equally empty until it does.");
            }

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
            var history = await DarlingTrendReader.GetQueryHistoryAsync(postgres, resolved.ServerId, database_name, query_hash, now.AddHours(-hours_back), now);
            var rows = history.Points;
            if (rows.Count == 0)
            {
                /* #2353: say what was READ, never what happened. This message used to assert "no history in the
                   last N hours" over a span the read never covered — for a query whose history had aged out of
                   the raw tier that is a false statement, not an incomplete one, and an agent acts on it by
                   concluding the query did not run. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No history found for query_hash '{query_hash}' in database '{database_name}' in the " +
                        $"{history.Source} tier over the last {hours_back} hours. This means nothing was recorded " +
                        "for that query_hash in that window in the tier searched — confirm the hash and database " +
                        "with get_top_queries_by_cpu before concluding the query did not run.");
            }

            /* The hourly rollup keeps executions, CPU and elapsed and nothing else. Those columns arrive as
               NULL and the mapper floors them to 0, so they are emitted as null HERE rather than as zero: on an
               aggregate row a zero would read as "none observed", which is a measurement we did not make. */
            var aggregated = history.Source != "raw";

            var result = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                execution_count = r.DeltaExecutions,
                cpu_ms = Math.Round(r.DeltaCpuUs / 1000.0, 2),
                elapsed_ms = Math.Round(r.DeltaElapsedUs / 1000.0, 2),
                avg_cpu_ms = Math.Round(r.DeltaExecutions > 0 ? r.DeltaCpuUs / 1000.0 / r.DeltaExecutions : 0, 2),
                avg_elapsed_ms = Math.Round(r.DeltaExecutions > 0 ? r.DeltaElapsedUs / 1000.0 / r.DeltaExecutions : 0, 2),
                logical_reads = aggregated ? (long?)null : r.DeltaLogicalReads,
                logical_writes = aggregated ? (long?)null : r.DeltaLogicalWrites,
                physical_reads = aggregated ? (long?)null : r.DeltaPhysicalReads,
                rows = aggregated ? (long?)null : r.DeltaRows,
                spills = aggregated ? (long?)null : r.DeltaSpills,
                min_dop = aggregated ? (int?)null : r.MinDop,
                max_dop = aggregated ? (int?)null : r.MaxDop,
                query_plan_hash = aggregated ? null : r.QueryPlanHash
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                database_name,
                query_hash,
                hours_back,
                /* #2353: what was actually served, alongside what was asked for. hours_back on its own was a
                   request echoed back as if it were a description of the data. */
                source = history.Source,
                effective_start = history.EffectiveStartUtc.ToString("o"),
                effective_hours_back = Math.Round((now - history.EffectiveStartUtc).TotalHours, 1),
                truncated = history.Truncated,
                bucket = history.Source == "raw" ? "per-collection" : "1 hour",
                aggregate_note = aggregated
                    ? "Served from the hourly rollup because the requested window reaches past the raw tier's "
                      + "4-day retention. Executions, CPU and elapsed time are summed per hour; logical_reads, "
                      + "logical_writes, physical_reads, rows, spills, min_dop, max_dop and query_plan_hash are "
                      + "null because the rollup does not carry them - null means not measured, not zero."
                    : null,
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
            var points = await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (points.Count == 0)
            {
                /* Same two states again. The probe reads the BASE query_stats table because this trend
                   does — v_query_stats is the payload-resolving view on a V38+ store, and probing a
                   different relation from the one the read walks is how an existence probe ends up
                   reporting the wrong branch. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await DarlingTrendReader.HasAnyQueryStatAsync(postgres, resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No query samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected query stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No query stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the query_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_top_queries_by_cpu will be equally empty until it does.");
            }

            /* The two siblings below serialize through the SAME helper, so the three Performance-Trends
               reads cannot advertise three different field sets for one shape. */
            return SerializeTrend(resolved.ServerName, hours_back, points);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_procedure_duration_trend"), Description("Gets a time-series of stored-procedure elapsed time per second and executions per second over time, summed across every procedure. The sibling of get_query_duration_trend, and NOT a duplicate of it: query_stats attributes a procedure's work to the individual statements inside it, so a procedure that got slower is smeared across however many statements it runs. This charges the whole call to the procedure. Read the two together to tell an ad-hoc SQL regression from a procedure regression.")]
    public static async Task<string> GetProcedureDurationTrend(
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
            var points = await DarlingTrendReader.GetProcedureDurationTrendAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            if (points.Count == 0)
            {
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "procedure_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await EmptyTrendAsync(
                    DarlingTrendReader.HasAnyProcedureStatAsync(postgres, resolved.ServerId),
                    resolved.ServerName, hours_back, "stored-procedure",
                    "Check that collection is running and that the server is enabled. A server that genuinely runs no stored procedures also lands here, and that is a real answer rather than a fault.");
            }

            return SerializeTrend(resolved.ServerName, hours_back, points);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_procedure_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_duration_trend"), Description("Gets a time-series of Query Store duration per second and executions per second over time, summed across every query. Where get_query_duration_trend reads the plan cache and loses everything an eviction or a restart takes with it, this reads Query Store, which persists per interval - so it is the series that survives a failover and the one to reach for when a regression is older than the cache. Each interval is counted once, at the hour the work ran.")]
    public static async Task<string> GetQueryStoreDurationTrend(
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
            var points = await DarlingTrendReader.GetQueryStoreDurationTrendAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            if (points.Count == 0)
            {
                /*
                    The one empty answer here that is NOT about the collector: Query Store can be off on
                    every database on the instance. A server with no Query Store data is not a server with
                    no slow queries, so the message names that cause first.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store");
                if (gated != null)
                {
                    return gated;
                }

                return await EmptyTrendAsync(
                    DarlingTrendReader.HasAnyQueryStoreStatAsync(postgres, resolved.ServerId),
                    resolved.ServerName, hours_back, "Query Store",
                    "Query Store may be OFF on this server's databases — that, not an absence of slow queries, is the usual cause. Check QUERY_STORE = ON per database, then that collection is running for this server.");
            }

            return SerializeTrend(resolved.ServerName, hours_back, points);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_duration_trend", ex);
        }
    }

    /// <summary>
    /// The one payload shape the three Performance-Trends siblings share, so a caller can chart them on one
    /// axis without learning three field names.
    /// <para><c>execution_count</c> and <c>executions_per_second</c> are the SAME quantity. The first
    /// shipped truncated to an integer, which on a quiet server turns 0.4 executions a second into a
    /// reported ZERO - an idle server, when the truth was a slow one. It is kept so a consumer reading it
    /// does not break; read <c>executions_per_second</c>.</para>
    /// </summary>
    private static string SerializeTrend(
        string serverName, int hours_back, List<DarlingTrendReader.QueryDurationTrendPoint> points) =>
        JsonSerializer.Serialize(new
        {
            server = serverName,
            hours_back,
            trend = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                value = p.Value,
                execution_count = p.ExecutionCount,
                executions_per_second = p.ExecutionsPerSecond,
            }),
        }, McpHelpers.JsonOptions);

    /// <summary>
    /// The two-branch empty answer the two new Performance-Trends siblings share (#2484), phrased to match
    /// the one get_query_duration_trend already ships (#2485) so the three reads tell one story.
    /// <para>Zero points is two facts wanting opposite responses. A server that HAS been sampled and was
    /// quiet in this window wants the window widened; a server that has never been sampled wants somebody
    /// to go look at why. Both are literally "no trend data", and the probe — one LIMIT 1 against the same
    /// table the trend reads, run only on this path — is what separates them.</para>
    /// </summary>
    private static async Task<string> EmptyTrendAsync(
        Task<bool> probe, string serverName, int hours_back, string what, string checkThis)
    {
        var everSampled = await probe;
        return everSampled
            ? McpHelpers.Status(
                "empty",
                $"No {what} samples were recorded for {serverName} in the last {hours_back} hour(s). This server HAS been sampled before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
            : McpHelpers.Status(
                "unavailable",
                $"No {what} samples have EVER been recorded for {serverName}. This is not an empty window — nothing at all has been stored for this server, so it is NOT a quiet server. {checkThis}");
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
