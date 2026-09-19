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
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

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
///
/// <para>
/// <b>The four reads over ROLLED tables route by retention tier and say what they served (#2353, #3541
/// A2).</b> <c>query_stats</c>, <c>procedure_stats</c> and <c>query_store_stats</c> have their raw rows
/// dropped at <see cref="TimescaleSupport.RawRetentionSpan"/> on a TimescaleDB store while the tools accept
/// <c>hours_back</c> up to seven days, so get_query_trend, get_query_duration_trend and
/// get_procedure_duration_trend read the hourly rollup for a window raw cannot hold and
/// get_query_store_duration_trend reads the corrected rollup for the region it has materialized (#2736) —
/// and every one of them publishes <c>source</c>, <c>effective_start</c>, <c>effective_hours_back</c>,
/// <c>truncated</c> and <c>bucket</c>, on the data path and on the empty one. "Quiet, widen hours_back" is
/// said only where widening can help: a window whose head the store no longer holds says that instead,
/// because the rows were dropped, not absent, and a wider window cannot recover them. Lite's twins publish
/// the same fields with Lite's truth (raw, unbounded within its retention), so the contract is one shape
/// across SKUs even where the depth differs.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpTrendTools
{
    [McpServerTool(Name = "get_memory_trend"), Description("Gets memory usage trend over time: total server memory, target memory, buffer pool, plan cache, and granted memory joined per point from the memory-grant series. total_granted_mb is null on points the grants series does not cover — a granted_note explains any gap; use get_memory_grants for grant detail. Useful for identifying memory growth patterns or pressure periods.")]
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

            var grants = await DarlingTrendReader.GetMemoryGrantTrendAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            var granted = AlignGrantSeries(
                points.Select(p => p.CollectionTime).ToArray(),
                grants.Select(g => (g.CollectionTime, g.TotalGrantedMb)).ToArray());

            var result = points.Select((p, i) => new
            {
                time = p.CollectionTime.ToString("o"),
                total_server_memory_mb = p.TotalServerMemoryMb,
                target_server_memory_mb = p.TargetServerMemoryMb,
                buffer_pool_mb = p.BufferPoolMb,
                plan_cache_mb = p.PlanCacheMb,
                /* Joined from the memory-grant series (#3548): the nearest memory_grant_stats snapshot
                   within 30 seconds of this memory sample, SUM(granted_memory_mb) across pools — the same
                   series the viewer's Memory Overview overlay charts. null when no snapshot aligns, never
                   a fabricated 0: a literal zero read as "granted was 0 all window" and steered callers
                   away from memory grants at exactly the wrong moment (#3529). A genuine 0.0 still appears
                   when a snapshot exists with nothing granted. Field-for-field parity with Lite's tool,
                   which joins it the same way. */
                total_granted_mb = granted[i]
            });

            /* The note exists to explain null points; a fully covered window gets no note at all rather
               than a null-valued key (JsonOptions writes nulls). */
            return granted.Any(v => v is null)
                ? JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    granted_note = GrantGapNote,
                    trend = result
                }, McpHelpers.JsonOptions)
                : JsonSerializer.Serialize(new
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

    /// <summary>
    /// Half the 1-minute cadence floor both collectors share (<c>CollectorScheduleDefaults</c>). The two
    /// series each stamp their own capture clock per collector run, so same-cycle rows sit seconds
    /// apart and can never be equality-joined — while a grants series on a slower cadence must NOT smear
    /// onto every memory point. Within half the finest cadence, at most one snapshot can claim a point.
    /// </summary>
    private static readonly TimeSpan GrantJoinTolerance = TimeSpan.FromSeconds(30);

    /// <summary>Why a point is null, stated once per payload — and only when a null point exists.</summary>
    private const string GrantGapNote =
        "total_granted_mb is null where no memory-grant snapshot lies within 30 seconds of the memory sample — the memory_grant_stats series is collected on its own schedule, so a gap means no grant measurement at that moment, not zero granted. Use get_memory_grants for the full grant picture.";

    /// <summary>
    /// Nearest-match join of the memory-grant series onto the memory-trend points (#3548): for each trend
    /// point, the closest grants snapshot within <see cref="GrantJoinTolerance"/>, else null — no grant
    /// measurement at that moment, which is not the same claim as a genuine 0.0 from a snapshot with
    /// nothing granted. Both inputs are time-ascending (both reads ORDER BY collection_time), so one
    /// forward pointer finds every nearest neighbor. Twin of Lite's
    /// <c>McpMemoryTools.AlignGrantSeries</c> — the two must stay in step so both SKUs join the same way.
    /// </summary>
    private static double?[] AlignGrantSeries(
        DateTime[] trendTimes,
        (DateTime Time, double TotalGrantedMb)[] grants)
    {
        var aligned = new double?[trendTimes.Length];
        if (grants.Length == 0) return aligned;

        var g = 0;
        for (var t = 0; t < trendTimes.Length; t++)
        {
            var target = trendTimes[t];
            while (g + 1 < grants.Length && (grants[g + 1].Time - target).Duration() <= (grants[g].Time - target).Duration())
            {
                g++;
            }

            if ((grants[g].Time - target).Duration() <= GrantJoinTolerance)
            {
                aligned[t] = grants[g].TotalGrantedMb;
            }
        }

        return aligned;
    }

    [McpServerTool(Name = "get_perfmon_trend"), Description("Gets a time-series trend for a specific performance counter. Use get_perfmon_stats first to see available counter names. counter_kind (from the stored cntr_type) says what each point's number is: 'gauge' — value IS the reading (a level such as Memory Grants Pending), delta_value and sample_interval_seconds are null because a level has no delta; 'rate' — the per-second figure is delta_value divided by sample_interval_seconds, never delta_value alone (a collection interval is minutes, not a second) and never a point whose sample_interval_seconds is 0 (no delta was knowable there); 'other' — delta_value is a per-interval change of an average/fraction numerator, not a rate and not a level; null — the rows predate the stored type or the counter's instances mix types, so classify by name (a name ending in /sec is a rate) as every reader did before the type was stored.")]
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
               by the configured 60 s is wrong by whatever the jitter was (#2233, #2234).

               counter_kind is the series' stored type read three ways (V132, #3653 A7): the type of any
               point that has one, because a counter's type does not change and the read reports a type
               only where the point's instance rows agree; null when no point has one. A gauge's points
               publish null delta_value and null sample_interval_seconds — the collector writes neither for a
               level — and value is the reading. Twin of Lite's McpPerfmonTools. */
            var seriesType = points.Select(p => p.CntrType).LastOrDefault(t => t.HasValue);
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
                cntr_type = seriesType,
                counter_kind = PerfmonCounterTypes.Word(seriesType),
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

            /* #3541 A2: the store's measured shape rides along so the tier decision degrades to what exists
               and to what has materialized (see DarlingTrendReader.ResolveTier) — the age-only #2353 rule
               answered 42P01 on a plain-PostgreSQL store for any window past four days, and read an empty
               rollup while raw still held the rows on a never-backfilled one. Cached per data source and
               shared with the composer, so this is not a probe per call. */
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, CancellationToken.None);
            var history = await DarlingTrendReader.GetQueryHistoryAsync(
                postgres, resolved.ServerId, database_name, query_hash, now.AddHours(-hours_back), now,
                hourlyAvailable: rollups.QueryGrainHourly,
                coverage: coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));
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

    [McpServerTool(Name = "get_query_duration_trend"), Description("Gets a time-series of average query duration over time. Useful for spotting overall performance degradation or improvement trends across all queries. On the per-collection (raw) route each point is a rate over the collection's STORED sample interval (sample_interval_seconds, the seconds the collector measured between its two snapshots), so a collection whose interval was unknowable - a restart or counter reset, stored as 0 - carries null rates rather than a fabricated 0.00; a collection that recorded no interval is rated over the gap since the PREVIOUS collection, so the window's first such collection - which has no previous one to difference against - carries null rates too. Unknowable is never reported as 0 (unrated_points counts them, unrated_note says why). The hourly rollup route divides by the bucket width and has no unrated point.")]
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
            var startUtc = now.AddHours(-hours_back);

            /*
                #3541 A2: route by the tier that can actually serve the window. This read went to raw
                query_stats only, whose rows a TimescaleDB store drops at four days, while hours_back
                accepts 168 — so a 7-day request came back as 4 days under a label saying 7, and when
                nothing survived the empty branch called the window "genuinely quiet" and advised widening
                it, which cannot help with rows that were dropped. Same ladder as get_query_trend
                (DarlingTrendReader.ResolveTier), measured by the reader against the WALL CLOCK because
                retention drops by age, never by where a point sits inside the requested window — the
                as_of anchor decides the window, not how old its rows are.
            */
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, CancellationToken.None);
            var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(startUtc, rollups, coverage);
            var result = await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, resolved.ServerId, startUtc, now, route);

            if (result.Points.Count == 0)
            {
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats");
                if (gated != null)
                {
                    return gated;
                }

                /* The raw probe reads the BASE query_stats table because the raw trend does — v_query_stats
                   is the payload-resolving view on a V38+ store, and probing a different relation from the
                   one the read walks is how an existence probe ends up reporting the wrong branch. On the
                   hourly route the rollup is probed too, because a server whose raw rows have all aged out
                   is not a server nothing was ever stored for. */
                return await EmptyRoutedTrendAsync(
                    DarlingTrendReader.HasAnyQueryStatAsync(postgres, resolved.ServerId),
                    postgres, resolved.ServerId, resolved.ServerName, hours_back, startUtc, now, route, "query",
                    "Check that collection is running and that the server is enabled; get_top_queries_by_cpu will be equally empty until it does.");
            }

            /* The two siblings below serialize through the SAME helper, so the three Performance-Trends
               reads cannot advertise three different field sets for one shape. */
            return SerializeTrend(resolved.ServerName, hours_back, result.Points, DescribeRoute(result, now));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_procedure_duration_trend"), Description("Gets a time-series of stored-procedure elapsed time per second and executions per second over time, summed across every procedure. The sibling of get_query_duration_trend, and NOT a duplicate of it: query_stats attributes a procedure's work to the individual statements inside it, so a procedure that got slower is smeared across however many statements it runs. This charges the whole call to the procedure. Read the two together to tell an ad-hoc SQL regression from a procedure regression. On the per-collection (raw) route each point is a rate over the collection's STORED sample interval (sample_interval_seconds), so a collection whose interval was unknowable - a restart or counter reset, stored as 0 - carries null rates rather than a fabricated 0.00; a collection that recorded no interval is rated over the gap since the PREVIOUS collection, so the window's first such collection - which has no previous one to difference against - carries null rates too. Unknowable is never reported as 0 (unrated_points counts them, unrated_note says why). The hourly rollup route divides by the bucket width and has no unrated point.")]
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
            var startUtc = now.AddHours(-hours_back);

            /* #3541 A2 — the same routing as get_query_duration_trend, over the procedure pair. */
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, CancellationToken.None);
            var route = DarlingTrendReader.ResolveProcedureDurationTrendRoute(startUtc, rollups, coverage);
            var result = await DarlingTrendReader.GetProcedureDurationTrendAsync(postgres, resolved.ServerId, startUtc, now, route);

            if (result.Points.Count == 0)
            {
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "procedure_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await EmptyRoutedTrendAsync(
                    DarlingTrendReader.HasAnyProcedureStatAsync(postgres, resolved.ServerId),
                    postgres, resolved.ServerId, resolved.ServerName, hours_back, startUtc, now, route, "stored-procedure",
                    "Check that collection is running and that the server is enabled. A server that genuinely runs no stored procedures also lands here, and that is a real answer rather than a fault.");
            }

            return SerializeTrend(resolved.ServerName, hours_back, result.Points, DescribeRoute(result, now));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_procedure_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_duration_trend"), Description("Gets a time-series of Query Store duration per second and executions per second over time, summed across every query. Where get_query_duration_trend reads the plan cache and loses everything an eviction or a restart takes with it, this reads Query Store, which persists per interval - so it is the series that survives a failover and the one to reach for when a regression is older than the cache. Each interval is counted once, at the hour the work ran. A rollup point (an hourly bucket the corrected rollup has materialized) is rated over its bucket width, so every rollup point is rated, the window's first bucket included; a raw point (a Query Store interval placed at its start, or a legacy row at its collection time) is rated over the gap since the PREVIOUS point, so a raw point that is first in the window - with no previous one to difference against - carries null rates: unknowable, never reported as 0 (unrated_points counts them, unrated_note says why).")]
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
            var startUtc = now.AddHours(-hours_back);

            /*
                #2736: route through the corrected rollup where the store has one. The raw read ranks the
                whole Query Store slab per call, which exceeds the mcp role's statement_timeout at ANY
                window width on a large store — so the materialized window portion is served from
                query_store_stats_corrected_hourly and only the unmaterialized tail is ranked raw. The
                payload discloses the routing (grain and boundary) rather than presenting the two regions
                as one estimator. The same route is what keeps this sibling honest about DEPTH (#3541 A2):
                raw query_store_stats is dropped at four days only once its rollups cover it (the #1680
                arming gate), so wherever raw is short the rollup is the tier holding the history, and a
                raw-only route means raw is complete.
            */
            var route = await QueryStoreTrendRouting.ResolveAsync(postgres);
            var points = await DarlingTrendReader.GetQueryStoreDurationTrendAsync(
                postgres, resolved.ServerId, startUtc, now, route);
            var disclosure = DescribeQueryStoreRoute(route, points, startUtc, now);

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

                /*
                    A window that ends before the rollup has materialized anything is a coverage gap, not a
                    quiet server: the work may well be in raw rows this read deliberately no longer ranks
                    (that rank IS the #2736 timeout), or in history only a backfill can materialize. Saying
                    "widen hours_back" here would send the caller in the wrong direction.
                */
                if (route.UseRollup && route.RollupFloorUtc is DateTime floor && now < floor)
                {
                    return EmptyStatus(
                        "empty",
                        $"The requested window ends before {floor:o}, the oldest hour the corrected Query Store rollup has materialized. This read serves history from query_store_stats_corrected_hourly rather than ranking the raw Query Store slab (#2736), so windows before that floor come back empty even when rows were collected — run --backfill-rollups to materialize deeper history.",
                        disclosure);
                }

                var everSampled = await DarlingTrendReader.HasAnyQueryStoreStatAsync(postgres, resolved.ServerId);
                if (!everSampled)
                {
                    return EmptyStatus(
                        "unavailable",
                        NeverSampledMessage(resolved.ServerName, "Query Store",
                            "Query Store may be OFF on this server's databases — that, not an absence of slow queries, is the usual cause. Check QUERY_STORE = ON per database, then that collection is running for this server."),
                        disclosure);
                }

                /*
                    Sampled, nothing in the window — but a window whose HEAD sits below the rollup's floor is
                    only quiet in the part the rollup has reached. The unserved head is named so "widen" is
                    read for what it can do (find the most recent samples) and not for what it cannot (reach
                    history nothing has materialized).
                */
                if (route.UseRollup && route.RollupFloorUtc is DateTime head && head > startUtc)
                {
                    return EmptyStatus(
                        "empty",
                        $"No Query Store samples were recorded for {resolved.ServerName} between {head:o} — the oldest hour the corrected rollup has materialized — and the end of the window. This server HAS been sampled before, so that stretch is genuinely quiet; the part of the window before {head:o} is unserved rather than quiet (the rollup has not materialized it and this read no longer ranks the raw slab for it, #2736) — run --backfill-rollups to materialize it. Widening hours_back finds newer samples only; it cannot reach the unserved head.",
                        disclosure);
                }

                return EmptyStatus("empty", QuietWindowMessage(resolved.ServerName, hours_back, "Query Store"), disclosure);
            }

            return SerializeTrend(resolved.ServerName, hours_back, points, disclosure);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_duration_trend", ex);
        }
    }

    /// <summary>
    /// What a tiered trend says about itself beside its points (#2353's vocabulary, applied to the trio by
    /// #3541 A2): which tier served (<c>source</c>), where the served series actually begins
    /// (<c>effective_start</c>, <c>effective_hours_back</c>), whether that head sits later than asked
    /// (<c>truncated</c>), the grain of a point (<c>bucket</c>), and the prose a degraded tier owes the reader
    /// (<c>aggregate_note</c>, null on raw). <c>Routing</c> is the Query Store sibling's #2736 seam detail and
    /// is emitted only when the rollup route was taken — the other two have no seam to describe.
    /// </summary>
    private sealed record TrendDisclosure(
        string Source, DateTime EffectiveStartUtc, DateTime WindowEndUtc, bool Truncated, string Bucket,
        string? AggregateNote, Dictionary<string, object>? Routing = null)
    {
        /// <summary>The disclosure keys in the order they are written, so the data envelope and the empty
        /// envelope carry the same block in the same shape.</summary>
        public void WriteTo(Dictionary<string, object?> envelope)
        {
            envelope["source"] = Source;
            /* Written in the store's own frame (naive UTC, Kind=Unspecified) so it prints exactly like the
               points' `time` beside it. The requested start arrives Kind=Utc from ValidateWindow and would
               otherwise carry a trailing Z the points do not, which reads as two frames in one payload. */
            envelope["effective_start"] = DateTime.SpecifyKind(EffectiveStartUtc, DateTimeKind.Unspecified).ToString("o");
            envelope["effective_hours_back"] = Math.Round((WindowEndUtc - EffectiveStartUtc).TotalHours, 1);
            envelope["truncated"] = Truncated;
            envelope["bucket"] = Bucket;
            envelope["aggregate_note"] = AggregateNote;
            if (Routing is not null)
            {
                envelope["routing"] = Routing;
            }
        }
    }

    /// <summary>The prose the hourly tier owes a reader of the query-stats and procedure-stats trends.</summary>
    private static string HourlyAggregateNote(DarlingTrendReader.DurationTrendRoute route) =>
        $"Served from the hourly rollup ({route.HourlyView}) because the requested window reaches past the raw tier's "
        + $"{TimescaleSupport.RawRetentionSpan.TotalDays:0}-day retention. Each point is one hour's summed work divided by "
        + "3,600 seconds, so an hour the collector covered only partly reads LOW, never high; the rollup trails the "
        + "clock by up to two hours (the current hour is never materialized and the previous one lands on the next refresh).";

    /// <summary>The routed trio's disclosure, from the route and what the read returned.</summary>
    private static TrendDisclosure DescribeRoute(DarlingTrendReader.DurationTrendResult result, DateTime windowEndUtc) =>
        new(
            result.Route.Source,
            result.EffectiveStartUtc,
            windowEndUtc,
            result.Truncated,
            result.Route.Tier == RetentionTier.Raw ? "per-collection" : "1 hour",
            result.Route.Tier == RetentionTier.Raw ? null : HourlyAggregateNote(result.Route));

    /// <summary>
    /// The routed trio's disclosure for an EMPTY answer: the tier is described from its floor rather than from
    /// a first point it does not have. Where the tier's oldest instant is measured and sits above the requested
    /// start, that instant is what the read could reach — <c>effective_start</c> says so and <c>truncated</c>
    /// follows <see cref="DarlingTrendReader.TruncationSlack"/>, exactly as it would had a point been there.
    /// Unmeasured, the requested start stands (#2353's rule: an empty result narrows nothing it cannot describe).
    /// A floor beyond the window's END is clamped to the end: the tier held none of the window, and
    /// <c>effective_hours_back</c> reads 0 rather than a negative span.
    /// </summary>
    private static TrendDisclosure DescribeEmptyRoute(DarlingTrendReader.DurationTrendRoute route, DateTime startUtc, DateTime windowEndUtc)
    {
        var reach = route.Tier == RetentionTier.Raw ? route.Coverage.RawOldestUtc : route.Coverage.HourlyFloorUtc;
        var (effectiveStart, truncated) = DescribeEmptyCoverage(reach, startUtc, windowEndUtc);

        return new TrendDisclosure(
            route.Source, effectiveStart, windowEndUtc, truncated,
            route.Tier == RetentionTier.Raw ? "per-collection" : "1 hour",
            route.Tier == RetentionTier.Raw ? null : HourlyAggregateNote(route));
    }

    /// <summary>
    /// Coverage for an EMPTY answer, from the tier's measured floor rather than from a first point it does not
    /// have. Three shapes: a floor at or before the start (or unmeasured) reached the whole window, so the
    /// requested start stands and nothing is truncated (#2353's rule — an empty result narrows nothing it
    /// cannot describe); a floor inside the window is where the tier could first have answered, and
    /// <see cref="DarlingTrendReader.DescribeCoverage"/> judges it by the shared slack exactly as it would a
    /// first point; a floor BEYOND the window's end means the tier held none of the window, so the served
    /// span is honestly zero (<c>effective_start</c> clamped to the end) and the answer is truncated outright
    /// — the slack is for a head that arrived late, not for a window that never arrived at all.
    /// </summary>
    private static (DateTime EffectiveStartUtc, bool Truncated) DescribeEmptyCoverage(
        DateTime? tierFloorUtc, DateTime startUtc, DateTime windowEndUtc)
    {
        if (tierFloorUtc is not DateTime floor || floor <= startUtc)
        {
            return (startUtc, false);
        }

        return floor > windowEndUtc
            ? (windowEndUtc, true)
            : DarlingTrendReader.DescribeCoverage(floor, startUtc);
    }

    /// <summary>
    /// The one payload shape the three Performance-Trends siblings share, so a caller can chart them on one
    /// axis without learning three field names.
    /// <para><c>execution_count</c> and <c>executions_per_second</c> are the SAME quantity. The first
    /// shipped truncated to an integer, which on a quiet server turns 0.4 executions a second into a
    /// reported ZERO - an idle server, when the truth was a slow one. It is kept so a consumer reading it
    /// does not break; read <c>executions_per_second</c>.</para>
    /// <para><c>value</c> and <c>elapsed_ms_per_second</c> are the same quantity too (#3541): a bare
    /// <c>value</c> named no unit, and a reasoning agent charted it as whatever it guessed. The named field is
    /// the one to read; <c>value</c> stays for the consumer already reading it, on the precedent above.</para>
    /// <para>The disclosure block sits between the request echo and the points on every sibling, and the
    /// empty envelope (<see cref="EmptyStatus"/>) carries the same block — the same six keys in the same
    /// order whichever branch answered, which is what lets a caller read <c>source</c> without first checking
    /// whether it got data.</para>
    /// </summary>
    private static string SerializeTrend(
        string serverName, int hours_back, List<DarlingTrendReader.QueryDurationTrendPoint> points,
        TrendDisclosure disclosure)
    {
        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hours_back,
        };
        disclosure.WriteTo(envelope);
        /* #3541 A12: a point with no rate is published as null, never as 0, and the envelope says how many
           and why. On the raw route a collection with no denominator is unrated: the window's first
           collection of a stretch that recorded no interval (its LAG has no previous one to difference
           against), and — since the plan-cache trends read the STORED interval (#3540 V128 for procedures,
           #3695 / #3653 for query_stats) — a restart collection whose interval the calculator could not
           measure (stored 0 → NULL). The hourly route divides by the bucket width and produces none, and a
           Query Store rollup bucket is rated over its width too (#3695); only a raw Query Store point is
           LAG-rated. The note below is the trio's shared sentence on BOTH SKUs (Lite's McpQueryTools carries
           it byte-identical, pinned by McpMissMessageParityPinTests) and names BOTH ways a denominator goes
           unknowable — the stored-0 restart (#3695 / #3700) and the first-in-window LAG (#3541 A12) — in one
           sentence, because the three tools serialize through this one helper and a per-tool note would put
           three sentences on one shape. The Query Store trend can only hit the second arm (it stores no
           interval), and the sentence stays true there: every one of its points is "a collection where no
           interval was stored". */
        var unrated = points.Count(p => !p.HasRate);
        envelope["unrated_points"] = unrated;
        envelope["unrated_note"] = unrated == 0
            ? null
            : $"{unrated} point(s) carry null rates: a rate is the point's work divided by the seconds it accrued over, and that denominator is unknowable two ways — the collection's STORED sample interval is 0 (a restart or counter reset: the collector could not difference its two snapshots, so the zeros beside it were never measured), or the point is rated against the PREVIOUS one and has none inside the window (the window's first collection where no interval was stored, or one landing in the same second as its predecessor). Unknowable is not 0 — the point is kept so effective_start is the first collection the store held, and its rates are null.";
        envelope["trend"] = points.Select(p => new
        {
            time = p.CollectionTime.ToString("o"),
            value = p.Value,
            elapsed_ms_per_second = p.Value,
            execution_count = p.ExecutionCount,
            executions_per_second = p.ExecutionsPerSecond,
        });

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>
    /// <see cref="McpHelpers.Status"/> with the trend's disclosure block beside <c>status</c> and
    /// <c>message</c>: an empty answer still says which tier it read and how far that tier reached, because
    /// "nothing here" means different things from a four-day raw tier and a ninety-day rollup.
    /// </summary>
    private static string EmptyStatus(string status, string message, TrendDisclosure disclosure)
    {
        var envelope = new Dictionary<string, object?>
        {
            ["status"] = status,
            ["message"] = message,
        };
        disclosure.WriteTo(envelope);
        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>
    /// The routing disclosure get_query_store_duration_trend attaches (#2736): which relation served which
    /// region, at which grain, and (when the requested window reaches below the rollup's materialized floor)
    /// what was NOT served and why. A degraded or partial answer must label itself. Since #3541 A2 the block
    /// speaks the shared vocabulary — <c>source</c> is the tier word (<c>rollup+raw</c> or <c>raw</c>),
    /// <c>aggregate_note</c> the grain prose, and the seam's instants live under <c>routing</c> — so the three
    /// siblings' envelopes carry the same keys with the same types. The raw-only route attaches no
    /// <c>routing</c> block because it is the original single-estimator read.
    /// </summary>
    private static TrendDisclosure DescribeQueryStoreRoute(
        QueryStoreTrendRouting.QueryStoreTrendRoute route, List<DarlingTrendReader.QueryDurationTrendPoint> points,
        DateTime windowStartUtc, DateTime windowEndUtc)
    {
        /* Coverage from the first point; for an EMPTY rollup-routed answer whose head sits below the floor,
           from the floor — the instant the tier could first have answered, the same rule DescribeEmptyRoute
           applies to the other two siblings. */
        var (effectiveStart, truncated) = points.Count > 0
            ? DarlingTrendReader.DescribeCoverage(points[0].CollectionTime, windowStartUtc)
            : DescribeEmptyCoverage(route.UseRollup ? route.RollupFloorUtc : null, windowStartUtc, windowEndUtc);

        /* The tier word and the unserved-head rule are the Storage definitions the viewer's chart title also
           reads (#3653) — QueryStoreTrendRouting.SourceWord / UnservedBefore — so the payload and the desktop
           cannot disagree about what served a window or whether its head was reached. */
        if (!route.UseRollup)
        {
            return new TrendDisclosure(QueryStoreTrendRouting.SourceWord(route), effectiveStart, windowEndUtc, truncated, "per-interval", null);
        }

        var routing = new Dictionary<string, object>
        {
            ["rollup"] = TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
            ["raw_from"] = route.RawStartUtc.ToString("o"),
        };

        if (QueryStoreTrendRouting.UnservedBefore(route, windowStartUtc) is DateTime floor)
        {
            routing["unserved_before"] = floor.ToString("o");
            routing["unserved_note"] =
                "The rollup has not materialized history before unserved_before, and this read no longer " +
                "falls back to ranking the raw slab for it (that rank is the #2736 timeout) — points " +
                "before that instant are missing, not zero. Run --backfill-rollups to materialize deeper " +
                "history.";
        }

        return new TrendDisclosure(
            QueryStoreTrendRouting.SourceWord(route), effectiveStart, windowEndUtc, truncated,
            "1 hour before routing.raw_from, per-interval from it",
            "Points before routing.raw_from are 1-hour buckets from the corrected Query Store rollup (#1849): " +
            "bucketed on the COLLECTION hour, deduped at interval grain, with an interval whose " +
            "snapshots straddle an hour boundary contributing to both adjacent buckets. Points at or " +
            "after routing.raw_from are raw Query Store intervals deduped to their final snapshot and placed " +
            "at interval_start_time_utc.",
            routing);
    }

    /// <summary>The two-state sentence pair the trio shares with Lite's twins, word for word (#2484, #2485).</summary>
    private static string QuietWindowMessage(string serverName, int hours_back, string what) =>
        $"No {what} samples were recorded for {serverName} in the last {hours_back} hour(s). This server HAS been sampled before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.";

    private static string NeverSampledMessage(string serverName, string what, string checkThis) =>
        $"No {what} samples have EVER been recorded for {serverName}. This is not an empty window — nothing at all has been stored for this server, so it is NOT a quiet server. {checkThis}";

    /// <summary>
    /// The empty answer for the two ROUTED Performance-Trends siblings (#2484, #2485, #3541 A2), in three
    /// states rather than the two the pre-routing version knew.
    /// <para>Zero points is still two facts wanting opposite responses — sampled-and-quiet wants the window
    /// widened, never-sampled wants somebody to look at collection — and the probe (one LIMIT 1 against the
    /// relation the trend reads, run only on this path; on the hourly route the rollup too, see
    /// <see cref="DarlingTrendReader.HasAnySampleOnRouteAsync"/>) still separates them. The third state is
    /// the one this fix exists for: sampled, nothing in the window, and the tier that was read does NOT
    /// reach the window's start. That is neither quiet nor broken; the rows are DROPPED (raw past its
    /// retention) or NOT MATERIALIZED (a rollup whose floor sits above the start), and "widen hours_back"
    /// is exactly the wrong advice, because a wider window reaches further into what the tier does not
    /// hold. The message names the tier, its measured reach, and the remedy that can work.</para>
    /// <para>"Quiet, widen" is kept word for word with Lite's twin for the state where it is true: the tier
    /// reaches the whole window (a plain-PostgreSQL store, where nothing drops raw; a rollup whose floor
    /// covers the start; a measured raw oldest at or before the start).</para>
    /// </summary>
    private static async Task<string> EmptyRoutedTrendAsync(
        Task<bool> rawProbe, NpgsqlDataSource postgres, int serverId, string serverName, int hours_back,
        DateTime startUtc, DateTime windowEndUtc, DarlingTrendReader.DurationTrendRoute route,
        string what, string checkThis)
    {
        var disclosure = DescribeEmptyRoute(route, startUtc, windowEndUtc);

        if (!await DarlingTrendReader.HasAnySampleOnRouteAsync(postgres, rawProbe, route, serverId))
        {
            return EmptyStatus("unavailable", NeverSampledMessage(serverName, what, checkThis), disclosure);
        }

        if (route.Tier == RetentionTier.Hourly)
        {
            var floor = route.Coverage.HourlyFloorUtc;
            var reach = floor is DateTime f
                ? (f <= startUtc
                    ? $"The rollup has materialized history from {f:o}, which covers the whole window, so nothing was recorded for this server in it in the tier searched — a quiet stretch, or a refresh gap inside the rollup (check get_collection_health)."
                    : $"The rollup has materialized history only from {f:o}; the part of the window before that is UNSERVED rather than quiet, and the raw rows for it were dropped by retention. Run --backfill-rollups to materialize deeper history.")
                : "The rollup has materialized NOTHING yet, so this is a coverage gap rather than a quiet server: the raw rows for this span were dropped by retention and only --backfill-rollups can materialize them.";

            return EmptyStatus(
                "empty",
                $"No {what} samples in the hourly rollup ({route.HourlyView}) for {serverName} over the last {hours_back} hour(s), from {startUtc:o}. The window reaches past the raw tier's {TimescaleSupport.RawRetentionSpan.TotalDays:0}-day retention, so this read served the rollup, not raw. {reach} Widening hours_back cannot help here.",
                disclosure);
        }

        /*
            Raw route. Honest "quiet" needs raw to reach the window's start. Raw's oldest row is MEASURED
            (the coverage probe reads min(collection_time) on every rolled table, rollups or not), so where it
            sits at or before the start the window is fully served and quiet is the truth. Where it is later,
            the head is unserved for one of two reasons the reader must not confuse: the window predates the
            store's own history (a young store, any engine — nothing was dropped, nothing ever existed), or
            retention dropped it (this grain's rollup exists, so its raw purge can be armed; the window is past
            the raw horizon; and coverage routed here because the rollup has materialized LESS than raw holds
            — the only way a past-horizon window routes to raw when the rollup exists). Unmeasured (an empty
            table), the horizon decides, and only where retention applies to this grain at all: a grain whose
            rollup is missing has its purge held by the arming gate, so raw is complete there.
        */
        var pastRawHorizon = route.RawRetentionApplies && startUtc < route.ResolvedAtUtc - TimescaleSupport.RawRetentionSpan;
        var rawReaches = route.RawReaches(startUtc) ?? !pastRawHorizon;
        if (rawReaches)
        {
            return EmptyStatus("empty", QuietWindowMessage(serverName, hours_back, what), disclosure);
        }

        if (!pastRawHorizon && route.Coverage.RawOldestUtc is DateTime storeOldest)
        {
            return EmptyStatus(
                "empty",
                $"No {what} samples were recorded for {serverName} between {storeOldest:o} — the oldest {route.RawTable} row this store holds for any server — and the end of the window. This server HAS been sampled before, so that stretch is genuinely quiet; the part of the window before {storeOldest:o} predates the store's history rather than being quiet, and widening hours_back cannot reach it.",
                disclosure);
        }

        /* Past the horizon on the raw route with the rollup present: coverage put the read here because the
           rollup has materialized less than raw holds (the #1759 held-purge shape), so raw's reach is the
           store's reach and the rollup is the remedy. */
        var oldest = route.Coverage.RawOldestUtc is DateTime o
            ? $"The raw tier's oldest row for any server is {o:o}"
            : $"The raw tier keeps about {TimescaleSupport.RawRetentionSpan.TotalDays:0} days";

        return EmptyStatus(
            "empty",
            $"No {what} samples were recorded for {serverName} in the part of the last {hours_back} hour(s) that the raw tier still holds. {oldest}, and the window as requested starts at {startUtc:o} — the part before raw's reach is UNSERVED rather than quiet, because the hourly rollup ({route.HourlyView}) that would serve deeper history has materialized less than raw holds. This server HAS been sampled before. Widening hours_back reaches further into what raw no longer holds and cannot help; run --backfill-rollups to materialize the rollup, which is what serves deeper history.",
            disclosure);
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
