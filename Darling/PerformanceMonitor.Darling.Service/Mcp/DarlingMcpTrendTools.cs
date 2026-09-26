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
using PerformanceMonitor.Collectors;
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
/// <c>window_truncated</c> and <c>bucket</c>, on the data path and on the empty one. "Quiet, widen hours_back" is
/// said only where widening can help: a window whose head the store no longer holds says that instead,
/// because the rows were dropped, not absent, and a wider window cannot recover them. Lite's twins publish
/// the same fields with Lite's truth (raw, unbounded within its retention), so the contract is one shape
/// across SKUs even where the depth differs.
/// </para>
///
/// <para>
/// <b>The window floor is spelled <c>window_truncated</c>, not <c>truncated</c> (#3653 item 17).</b> Until
/// #3653 the flag above rode under the page dialect's key, and #3703's vocabulary census found the homonym:
/// on twenty-odd paged tools <c>truncated</c> is the caller's <c>limit</c> biting (observed off a
/// <c>cap + 1</c> fetch, beside <c>*_returned</c>, remedied by a bigger <c>limit</c>); here it was the store's
/// REACH — the head of the served series sits later than the requested start because the tier that answered
/// no longer holds the window's head — beside <c>effective_start</c> / <c>effective_hours_back</c>, and no
/// <c>limit</c> changes it. One key, two facts, opposite remedies. The window-floor fact now takes #3703's
/// <c>&lt;bound&gt;_truncated</c> dialect on both SKUs (Lite's <c>McpQueryTools.WriteDisclosure</c> is the twin)
/// and on <c>get_query_store_top</c> in <see cref="DarlingMcpDataTools"/>; every tool that publishes it carries
/// <see cref="McpHelpers.WindowTruncatedDescription"/>, which names the wire change. The C# members that
/// carry the fact (<c>DurationTrendResult.Truncated</c>, <c>QueryHistoryResult.Truncated</c>,
/// <see cref="DarlingTrendReader.TruncationSlack"/>, <see cref="DarlingTrendReader.DescribeCoverage"/>) keep
/// their names — the rename is the WIRE key, and <c>McpPayloadContractCensusTests</c> holds it: a bare
/// <c>truncated</c> published beside <c>effective_hours_back</c> on either SKU is red by name.
/// </para>
///
/// <para>
/// <b>Every trend here ends with <c>discontinuities[]</c> (#3653 A5).</b> The identity-epoch carriers (#3694,
/// #3705) forget a server's delta baselines when the target restarts, fails over, is renamed or has its
/// statistics reset, and mark the carrier run's <c>collection_log</c> row; a trend read over such a window
/// shows a step that is the instrument re-baselining, not the workload. So each tool reads the markers inside
/// the SAME window as its points (<see cref="DarlingTrendReader.GetBaselineDiscontinuitiesAsync"/>) and
/// publishes them as the payload's trailing key — <see cref="BaselineDiscontinuities.PayloadKey"/>, an empty
/// array when none, each entry <c>{ at, reason, detail }</c> through the shared
/// <see cref="BaselineDiscontinuities.ToPayload"/> so Lite's twins spell the same three keys — and its
/// description carries the shared sentence naming it. On the data path only: an empty answer is a status
/// envelope with no series to mark. <c>get_wait_trend</c> in <see cref="DarlingMcpDataTools"/> carries the same
/// block; the census in Darling.Tests enumerates all eight on both SKUs.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpTrendTools
{
    [McpServerTool(Name = "get_memory_trend"), Description("Gets memory usage over time in time buckets: total server, target, buffer pool and plan cache memory, with granted memory from the memory-grant series joined per bucket. total_granted_mb is null where that series has no snapshot (granted_note says why); use get_memory_grants for grant detail." + BaselineDiscontinuities.DescriptionSentence)]
    public static Task<string> GetMemoryTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        CancellationToken cancellationToken = default) =>
        GetMemoryTrend(postgres, server_name, hours_back, as_of, bucket_minutes, TrendBudget.Mcp(TrendBuckets.MemoryMaxPoints), cancellationToken);

    /// <summary>get_memory_trend under an explicit <paramref name="budget"/> (#3960): the MCP tool passes its own, the
    /// web viewer's <c>/api/read</c> mirror <see cref="TrendBudget.Chart"/>. The memory samples and the memory-grant
    /// snapshots are bucketed at the same width and joined on the bucket (<see cref="TrendPayloads.MemoryTrend"/>).</summary>
    internal static async Task<string> GetMemoryTrend(
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
            var now = windowEnd;
            var points = await DarlingTrendReader.GetMemoryBucketsAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, bucketMinutes, cancellationToken);
            if (points.Count == 0)
            {
                /*
                    "No memory trend data available" was true of two opposite states and told the caller
                    neither. A server that collected fine and was simply quiet in THIS window wants the
                    window widened; a server the collector has never touched wants somebody to go look at
                    collection, and widening will never fill it. Probed only here, on the path that already
                    found nothing, against the SAME source the trend read.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_stats", cancellationToken);
                if (gated != null)
                {
                    return gated;
                }

                return await DarlingTrendReader.HasAnyMemoryStatAsync(postgres, resolved.ServerId, cancellationToken)
                    ? McpHelpers.Status(
                        "empty",
                        $"No memory samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected memory stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No memory stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the memory_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_memory_stats will be equally empty until it does.");
            }

            /* Joined from the memory-grant series (#3548), SUM(granted_memory_mb) across pools per snapshot — the
               series the viewer's Memory Overview overlay charts — bucketed at the SAME width and joined on the
               bucket (#3960), where it used to be the nearest snapshot within 30 seconds of each memory sample. A
               bucket no grant snapshot fell into publishes null, never a fabricated 0 (#3529); a genuine 0.0 still
               appears where snapshots exist with nothing granted. Lite's tool builds the same payload. */
            var grants = await DarlingTrendReader.GetGrantBucketsAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, bucketMinutes, cancellationToken);
            var discontinuities = await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, cancellationToken);

            return TrendPayloads.MemoryTrend(
                resolved.ServerName, hours_back, points, grants, bucketMinutes, bucket_minutes is not null,
                budget.AutoPoints, BaselineDiscontinuities.ToPayload(discontinuities));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_memory_trend", ex);
        }
    }

    [McpServerTool(Name = "get_perfmon_trend"), Description("Gets one performance counter over time in buckets, ending at as_of. counter_kind says the unit: gauge - value is the bucket average, delta_value and sample_interval_seconds null (no delta for a level); rate - per-second is delta_value divided by sample_interval_seconds, never delta_value alone or when the interval is 0; other - delta_value is a non-rate change; null - classify by name (ends in /sec = rate). No points never returns empty: not_collected covers a gated engine, Page Life Expectancy, or an unknown counter name; unavailable: no counter at all collected in the window. <<GUIDE>> Gets one performance counter over time in time buckets. Use get_perfmon_stats first to see available counter names. counter_kind (from the stored cntr_type) says what a point's number is: 'gauge' — value is the bucket's average reading and peak_value its highest, delta_value and sample_interval_seconds are null because a level has no delta; 'rate' — the per-second figure is delta_value divided by sample_interval_seconds, never delta_value alone, and never where sample_interval_seconds is 0 (no delta was knowable); 'other' — delta_value is the change of an average/fraction numerator, not a rate and not a level; null — the rows predate the stored type or the instances mix types, so classify by name (a name ending in /sec is a rate)." + BaselineDiscontinuities.DescriptionSentence)]
    public static Task<string> GetPerfmonTrend(
        NpgsqlDataSource postgres,
        [Description("The exact counter name, e.g. 'Batch Requests/sec'.")] string counter_name,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null) =>
        GetPerfmonTrend(postgres, counter_name, server_name, hours_back, as_of, bucket_minutes, TrendBudget.Mcp(TrendBuckets.PerfmonMaxPoints));

    /// <summary>get_perfmon_trend under an explicit <paramref name="budget"/> (#3960): the MCP tool passes its own,
    /// the web viewer's <c>/api/read</c> mirror <see cref="TrendBudget.Chart"/>.</summary>
    internal static async Task<string> GetPerfmonTrend(
        NpgsqlDataSource postgres, string counter_name, string? server_name, int hours_back, string? as_of, int? bucket_minutes,
        TrendBudget budget)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
        if (bucketError != null) return bucketError;

        try
        {
            var now = windowEnd;
            var start = now.AddHours(-hours_back);
            var points = await DarlingTrendReader.GetPerfmonBucketsAsync(postgres, resolved.ServerId, counter_name, start, now, bucketMinutes);
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
               by the configured 60 s is wrong by whatever the jitter was (#2233, #2234). A bucket sums its
               deltas and the seconds they accrued over (#3960), so that division still holds per point.

               counter_kind is the series' stored type read three ways (V132, #3653 A7): the type of any
               point that has one, because a counter's type does not change and the read reports a type
               only where the point's instance rows agree; null when no point has one. A gauge's points
               publish null delta_value and null sample_interval_seconds — the collector writes neither for a
               level — and value is the bucket's average reading. Lite's McpPerfmonTools builds the same
               payload (TrendPayloads.PerfmonTrend). */
            var discontinuities = await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, start, now);

            return TrendPayloads.PerfmonTrend(
                resolved.ServerName, counter_name, hours_back, points, bucketMinutes, bucket_minutes is not null,
                budget.AutoPoints, BaselineDiscontinuities.ToPayload(discontinuities));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_perfmon_trend", ex);
        }
    }

    [McpServerTool(Name = "get_file_io_trend"), Description("Gets file I/O read and write latency over time per database, data and log files pooled, heaviest I/O stall first; past the top five the rest fold into one (other) line. database_name charts one database per file. Useful for spotting degradation in storage performance." + BaselineDiscontinuities.DescriptionSentence)]
    public static Task<string> GetFileIoTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        [Description("One database, charted per file. Omit for every database.")] string? database_name = null) =>
        GetFileIoTrend(postgres, server_name, hours_back, as_of, bucket_minutes, database_name, TrendBudget.Mcp(TrendBuckets.FileIoMaxPoints));

    /// <summary>
    /// get_file_io_trend under an explicit <paramref name="budget"/> (#3897): the MCP tool above passes its own,
    /// sized for a model's context, and the web viewer's <c>/api/read</c> mirror passes
    /// <see cref="TrendBudget.Chart"/>, sized for a chart. One body, so the two surfaces differ in point count
    /// and nothing else.
    ///
    /// <para>Two reads, deliberately. The first ranks the window's active series, and its length decides the
    /// bucket width — five lines and an "(other)" line need coarser buckets than two lines do to stay inside the
    /// same budget. The second buckets only those lines; it re-derives the same ranking in SQL rather than taking
    /// the list back as parameters, which keeps the two statements one shared CTE and the two SKUs one shape.</para>
    /// </summary>
    internal static async Task<string> GetFileIoTrend(
        NpgsqlDataSource postgres, string? server_name, int hours_back, string? as_of, int? bucket_minutes,
        string? database_name, TrendBudget budget)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        /* An unusable width is a fault in the request whatever the window holds, so it is refused before anything
           is read; the cap waits for the ranking, because it depends on how many lines there are to draw. */
        var widthError = TrendBuckets.ValidateWidth(bucket_minutes);
        if (widthError != null) return widthError;

        var scope = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();

        try
        {
            var now = windowEnd;
            var start = now.AddHours(-hours_back);
            var series = await DarlingTrendReader.GetFileIoSeriesAsync(postgres, resolved.ServerId, start, now, scope);
            if (series.Count == 0)
            {
                /* Same two states as the memory trend, same probe discipline. The quiet-window sentence
                   carries one extra clause the others do not need: the ranking counts only series that read
                   or wrote, so a genuinely idle file set is empty here even on a server whose file_io_stats
                   collector ran every cycle. */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "file_io_stats");
                if (gated != null)
                {
                    return gated;
                }

                var everCollected = await DarlingTrendReader.HasAnyFileIoStatAsync(postgres, resolved.ServerId);

                /* A scoped read that found nothing is first a question about the NAME: a misspelt database
                   and an idle one land here alike, and only the collected names tell them apart. */
                if (scope is not null && everCollected)
                {
                    return McpHelpers.Status("empty", TrendPayloads.FileIoScopeEmptyMessage(resolved.ServerName, scope, hours_back));
                }

                return everCollected
                    ? McpHelpers.Status(
                        "empty",
                        $"No file I/O samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected file I/O stats before, so this window is genuinely quiet rather than broken — widen hours_back, or read it as no measurable read or write activity on any file in this window.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No file I/O stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the file_io_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_file_io_stats will be equally empty until it does.");
            }

            var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, TrendPayloads.LinesFor(series.Count), budget, out var bucketMinutes);
            if (bucketError != null) return bucketError;

            var points = await DarlingTrendReader.GetFileIoTrendAsync(
                postgres, resolved.ServerId, start, now, scope, TrendPayloads.ChartedFor(series.Count), bucketMinutes);
            var discontinuities = await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, start, now);

            return TrendPayloads.FileIoTrend(
                resolved.ServerName, hours_back, scope, series, points, bucketMinutes, bucket_minutes is not null,
                budget.AutoPoints, BaselineDiscontinuities.ToPayload(discontinuities));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_file_io_trend", ex);
        }
    }

    [McpServerTool(Name = "get_query_trend"), Description("Gets a time-series of performance metrics for a specific query identified by its query_hash. Use this after identifying a problematic query from get_top_queries_by_cpu or get_query_store_top to see how it has changed over time." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetQueryTrend(
        NpgsqlDataSource postgres,
        [Description("The query_hash value from get_top_queries_by_cpu or get_query_store_top.")] string query_hash,
        [Description("The database name the query belongs to.")] string database_name,
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

            /* #3541 A2: the store's measured shape rides along so the tier decision degrades to what exists
               and to what has materialized (see DarlingTrendReader.ResolveTier) — the age-only #2353 rule
               answered 42P01 on a plain-PostgreSQL store for any window past four days, and read an empty
               rollup while raw still held the rows on a never-backfilled one. Cached per data source and
               shared with the composer, so this is not a probe per call. */
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
            var history = await DarlingTrendReader.GetQueryHistoryAsync(
                postgres, resolved.ServerId, database_name, query_hash, now.AddHours(-hours_back), now,
                hourlyAvailable: rollups.QueryGrainHourly,
                coverage: coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView),
                /* #3653 A6: the hourly FROM-clause item for THIS window (RollupCoverage.StitchedRelationSql) —
                   the legacy alone with no successor (byte-identical to "collect.<legacy> AS f", the SAME
                   text HourlyRelationFor plus the bare-name alias produced), or a stitch splicing in the
                   successor past its floor. The tier above is still decided over the legacy pair, the deeper
                   of the two. */
                hourlyRelation: coverage.StitchedRelationSql(TimescaleSupport.QueryStatsHourlyView, "f", now.AddHours(-hours_back), RollupCoverage.StitchTier.Hourly),
                cancellationToken: cancellationToken);
            var rows = history.Points;
            if (rows.Count == 0)
            {
                /* #2353: say what was READ, never what happened. This message used to assert "no history in the
                   last N hours" over a span the read never covered — for a query whose history had aged out of
                   the raw tier that is a false statement, not an incomplete one, and an agent acts on it by
                   concluding the query did not run. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats", cancellationToken)
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
            var discontinuities = await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, now.AddHours(-hours_back), now, cancellationToken);

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
                /* #3653 item 17: the WINDOW floor, under its own key. `truncated` on this surface is the page
                   cut (limit bit); this is the tier's reach falling short of the requested start, and the two
                   want opposite remedies — see the class remarks and McpHelpers.WindowTruncatedDescription. */
                window_truncated = history.Truncated,
                bucket = history.Source == "raw" ? "per-collection" : "1 hour",
                /* #3897: the width beside the word, as every trend carrying `bucket` now publishes it — null on
                   the per-collection history, the rollup's hour otherwise. Lite's twin writes it through
                   WriteDisclosure. */
                bucket_minutes = history.Source == "raw" ? (int?)null : 60,
                aggregate_note = aggregated
                    ? "Served from the hourly rollup because the requested window reaches past the raw tier's "
                      + "4-day retention. Executions, CPU and elapsed time are summed per hour; logical_reads, "
                      + "logical_writes, physical_reads, rows, spills, min_dop, max_dop and query_plan_hash are "
                      + "null because the rollup does not carry them - null means not measured, not zero."
                    : null,
                data_points = rows.Count,
                trend = result,
                discontinuities = BaselineDiscontinuities.ToPayload(discontinuities)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_trend", ex);
        }
    }

    [McpServerTool(Name = "get_query_duration_trend"), Description("Gets a time-series of query elapsed_ms_per_second and executions_per_second across all queries, points ending at as_of. A collection whose interval was unknowable is excluded, not counted as 0: unrated_collections counts it, and a point left with nothing else carries null rates (unrated_points), never zero. No points: unavailable means query_stats was never collected here; empty means it was (the message says quiet window or rollup coverage gap). window_truncated is the store's retention floor, not a page cut: effective_start / effective_hours_back say where the answer begins. <<GUIDE>> Gets a time-series of average query duration over time. Useful for spotting overall performance degradation or improvement trends across all queries. Points are time buckets (bucket, aggregate_note); rates are over each collection's STORED sample interval, and a collection whose interval was unknowable - a restart or counter reset, or the window's first collection when none was stored - is left out rather than counted as 0 (unrated_collections counts them; a point with nothing else carries null rates, unrated_points). The hourly rollup route divides by the bucket width and has no unrated point." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static Task<string> GetQueryDurationTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        CancellationToken cancellationToken = default) =>
        GetQueryDurationTrend(postgres, server_name, hours_back, as_of, bucket_minutes, TrendBudget.Mcp(TrendBuckets.DurationMaxPoints), cancellationToken);

    /// <summary>get_query_duration_trend under an explicit <paramref name="budget"/> (#3897): the MCP tool passes
    /// its own, the web viewer's <c>/api/read</c> mirror <see cref="TrendBudget.Chart"/>.</summary>
    internal static async Task<string> GetQueryDurationTrend(
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
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
            var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(startUtc, rollups, coverage, windowEndUtc: now);

            /* #3897: the hourly rollup's points are whole hours, so a width it cannot serve is refused rather than
               quietly answered at another, and an automatic width is never finer than the rollup's hour. */
            if (route.Tier == RetentionTier.Hourly)
            {
                var hourlyError = TrendBuckets.RequireWholeHours(bucket_minutes, RawRetentionDays);
                if (hourlyError != null) return hourlyError;

                bucketMinutes = TrendBuckets.OnHourlyTier(bucket_minutes, bucketMinutes);
            }

            var result = await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, resolved.ServerId, startUtc, now, route, bucketMinutes, cancellationToken);

            if (result.Points.Count == 0)
            {
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats", cancellationToken);
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
                    DarlingTrendReader.HasAnyQueryStatAsync(postgres, resolved.ServerId, cancellationToken),
                    postgres, resolved.ServerId, resolved.ServerName, hours_back, startUtc, now, route, "query",
                    "Check that collection is running and that the server is enabled; get_top_queries_by_cpu will be equally empty until it does.",
                    new BucketChoice(bucketMinutes, bucket_minutes is not null, budget.AutoPoints), cancellationToken);
            }

            /* The two siblings below serialize through the SAME helper, so the three Performance-Trends
               reads cannot advertise three different field sets for one shape. */
            return SerializeTrend(resolved.ServerName, hours_back, result.Points,
                DescribeRoute(result, now, new BucketChoice(bucketMinutes, bucket_minutes is not null, budget.AutoPoints)),
                await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, startUtc, now, cancellationToken));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_procedure_duration_trend"), Description("Gets a time-series of stored-procedure elapsed_ms_per_second and executions_per_second, summed across every procedure and charged to the whole call rather than smeared across its statements the way query_stats attributes work. Points end at as_of. unrated_points, unrated_collections, the empty/unavailable split and window_truncated (a retention floor, not a page cut) all follow get_query_duration_trend exactly. <<GUIDE>> Gets a time-series of stored-procedure elapsed time per second and executions per second over time, summed across every procedure. The sibling of get_query_duration_trend, and NOT a duplicate of it: query_stats attributes a procedure's work to the individual statements inside it, so a procedure that got slower is smeared across however many statements it runs. This charges the whole call to the procedure. Read the two together to tell an ad-hoc SQL regression from a procedure regression. Points, rates and unrated collections (unrated_points, unrated_collections) follow get_query_duration_trend exactly." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static Task<string> GetProcedureDurationTrend(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        CancellationToken cancellationToken = default) =>
        GetProcedureDurationTrend(postgres, server_name, hours_back, as_of, bucket_minutes, TrendBudget.Mcp(TrendBuckets.DurationMaxPoints), cancellationToken);

    /// <summary>get_procedure_duration_trend under an explicit <paramref name="budget"/> (#3897) — the query
    /// trend's twin, over the procedure pair.</summary>
    internal static async Task<string> GetProcedureDurationTrend(
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
            var now = windowEnd;
            var startUtc = now.AddHours(-hours_back);

            /* #3541 A2 — the same routing as get_query_duration_trend, over the procedure pair, and #3897 the same
               width rule on the hourly tier. */
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
            var route = DarlingTrendReader.ResolveProcedureDurationTrendRoute(startUtc, rollups, coverage, windowEndUtc: now);
            if (route.Tier == RetentionTier.Hourly)
            {
                var hourlyError = TrendBuckets.RequireWholeHours(bucket_minutes, RawRetentionDays);
                if (hourlyError != null) return hourlyError;

                bucketMinutes = TrendBuckets.OnHourlyTier(bucket_minutes, bucketMinutes);
            }

            var result = await DarlingTrendReader.GetProcedureDurationTrendAsync(postgres, resolved.ServerId, startUtc, now, route, bucketMinutes, cancellationToken);

            if (result.Points.Count == 0)
            {
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "procedure_stats", cancellationToken);
                if (gated != null)
                {
                    return gated;
                }

                return await EmptyRoutedTrendAsync(
                    DarlingTrendReader.HasAnyProcedureStatAsync(postgres, resolved.ServerId, cancellationToken),
                    postgres, resolved.ServerId, resolved.ServerName, hours_back, startUtc, now, route, "stored-procedure",
                    "Check that collection is running and that the server is enabled. A server that genuinely runs no stored procedures also lands here, and that is a real answer rather than a fault.",
                    new BucketChoice(bucketMinutes, bucket_minutes is not null, budget.AutoPoints), cancellationToken);
            }

            return SerializeTrend(resolved.ServerName, hours_back, result.Points,
                DescribeRoute(result, now, new BucketChoice(bucketMinutes, bucket_minutes is not null, budget.AutoPoints)),
                await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, startUtc, now, cancellationToken));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_procedure_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_duration_trend"), Description("Gets a time-series of Query Store duration and executions per second, summed across every query, each interval counted once, at the hour it ran. not_collected: engine cannot run Query Store. unavailable: never sampled here. empty: quiet on Lite always; on Darling, empty can also be a rollup coverage gap (window predates the corrected rollup, run --backfill-rollups). A point with no earlier point to rate against has null rates, never 0 (unrated_points, unrated_note says why). window_truncated marks the retention floor, not a page cut; effective_start says where the answer begins. <<GUIDE>> Gets a time-series of Query Store duration per second and executions per second over time, summed across every query. Where get_query_duration_trend reads the plan cache and loses everything an eviction or a restart takes with it, this reads Query Store, which persists per interval - so it is the series that survives a failover and the one to reach for when a regression is older than the cache. Each interval is counted once, at the hour the work ran. A rollup point (an hourly bucket the corrected rollup has materialized) is rated over its bucket width, so every rollup point is rated, the window's first bucket included; a raw point (a Query Store interval placed at its start, or a legacy row at its collection time) is rated over the gap since the PREVIOUS point, so a raw point that is first in the window - with no previous one to difference against - carries null rates: unknowable, never reported as 0 (unrated_points counts them, unrated_note says why)." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetQueryStoreDurationTrend(
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
            var route = await QueryStoreTrendRouting.ResolveAsync(postgres, cancellationToken);
            var points = await DarlingTrendReader.GetQueryStoreDurationTrendAsync(
                postgres, resolved.ServerId, startUtc, now, route, cancellationToken);
            var disclosure = DescribeQueryStoreRoute(route, points, startUtc, now);

            if (points.Count == 0)
            {
                /*
                    The one empty answer here that is NOT about the collector: Query Store can be off on
                    every database on the instance. A server with no Query Store data is not a server with
                    no slow queries, so the message names that cause first.
                */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_store", cancellationToken);
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

                var everSampled = await DarlingTrendReader.HasAnyQueryStoreStatAsync(postgres, resolved.ServerId, cancellationToken);
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

            return SerializeTrend(resolved.ServerName, hours_back, points, disclosure,
                await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, startUtc, now, cancellationToken));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_store_duration_trend", ex);
        }
    }

    /// <summary>
    /// What a tiered trend says about itself beside its points (#2353's vocabulary, applied to the trio by
    /// #3541 A2): which tier served (<c>source</c>), where the served series actually begins
    /// (<c>effective_start</c>, <c>effective_hours_back</c>), whether that head sits later than asked
    /// (<c>window_truncated</c> — the window floor, not the page dialect's <c>truncated</c>; #3653 item 17), the
    /// grain of a point (<c>bucket</c>), and the prose a degraded tier owes the reader
    /// (<c>aggregate_note</c>, null on raw). <c>Routing</c> is the Query Store sibling's #2736 seam detail and
    /// is emitted only when the rollup route was taken — the other two have no seam to describe.
    /// </summary>
    private sealed record TrendDisclosure(
        string Source, DateTime EffectiveStartUtc, DateTime WindowEndUtc, bool Truncated, string Bucket,
        string? AggregateNote, Dictionary<string, object>? Routing = null, int? BucketMinutes = null)
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
            /* The window floor under its own key (#3653 item 17). This was `truncated` until #3653, the same
               spelling every paged tool uses for its limit biting, and a client that had learned that meaning
               here read "raise the limit" off a fact no limit changes. Lite's WriteDisclosure writes the same
               key at the same position; McpPayloadContractCensusTests fails a bare `truncated` beside
               `effective_hours_back` on either SKU. The C# member stays `Truncated` — the wire key renamed. */
            envelope["window_truncated"] = Truncated;
            envelope["bucket"] = Bucket;
            /* #3897: the width in minutes beside its word, null on a series that is not time-bucketed (the Query
               Store trend's per-interval points). Lite's WriteDisclosure writes it at the same position. */
            envelope["bucket_minutes"] = BucketMinutes;
            envelope["aggregate_note"] = AggregateNote;
            if (Routing is not null)
            {
                envelope["routing"] = Routing;
            }
        }
    }

    /// <summary>The raw tier's retention in whole days, for the sentences that name it.</summary>
    private static int RawRetentionDays => (int)TimescaleSupport.RawRetentionSpan.TotalDays;

    /// <summary>The bucket width a duration trend served (#3897), whether the caller chose it, and the auto target
    /// it was sized for — what the disclosure needs to say what a point is.</summary>
    private readonly record struct BucketChoice(int Minutes, bool Requested, int AutoPoints);

    /// <summary>The prose the hourly tier owes a reader of the query-stats and procedure-stats trends: why the
    /// rollup served, what one point is at the served width (#3897: an hour, or the hours gathered into a wider
    /// bucket), and why there is no per-collection peak on this tier.</summary>
    private static string HourlyAggregateNote(DarlingTrendReader.DurationTrendRoute route, BucketChoice bucket) =>
        $"Served from the hourly rollup ({route.HourlyView}) because the requested window reaches past the raw tier's "
        + $"{TimescaleSupport.RawRetentionSpan.TotalDays:0}-day retention. "
        + (bucket.Minutes == 60
            ? "Each point is one hour's summed work divided by 3,600 seconds"
            : $"Each point gathers the rollup's hours in one {TrendBuckets.Adjective(bucket.Minutes)} bucket, stamped at its start: their summed work over 3,600 seconds per hour the rollup holds in it")
        + ", so an hour the collector covered only partly reads LOW, never high; the rollup trails the "
        + "clock by up to two hours (the current hour is never materialized and the previous one lands on the next refresh). "
        + "peak_elapsed_ms_per_second is the bucket's busiest HOUR on this tier: the rollup keeps hourly sums, not the collections inside them."
        + (bucket.Requested ? " The width is the bucket_minutes you passed." : string.Empty);

    /// <summary>What one point of a routed trio answer is, on the tier that served it (#3897).</summary>
    private static string RoutedAggregateNote(DarlingTrendReader.DurationTrendRoute route, BucketChoice bucket) =>
        route.Tier == RetentionTier.Raw
            ? TrendBuckets.AggregateNote(bucket.Minutes, bucket.Requested, bucket.AutoPoints)
            : HourlyAggregateNote(route, bucket);

    /// <summary>The routed trio's disclosure, from the route and what the read returned.</summary>
    private static TrendDisclosure DescribeRoute(DarlingTrendReader.DurationTrendResult result, DateTime windowEndUtc, BucketChoice bucket) =>
        new(
            result.Route.Source,
            result.EffectiveStartUtc,
            windowEndUtc,
            result.Truncated,
            TrendBuckets.Word(bucket.Minutes),
            RoutedAggregateNote(result.Route, bucket),
            BucketMinutes: bucket.Minutes);

    /// <summary>
    /// The routed trio's disclosure for an EMPTY answer: the tier is described from its floor rather than from
    /// a first point it does not have. Where the tier's oldest instant is measured and sits above the requested
    /// start, that instant is what the read could reach — <c>effective_start</c> says so and <c>window_truncated</c>
    /// follows <see cref="DarlingTrendReader.TruncationSlack"/>, exactly as it would had a point been there.
    /// Unmeasured, the requested start stands (#2353's rule: an empty result narrows nothing it cannot describe).
    /// A floor beyond the window's END is clamped to the end: the tier held none of the window, and
    /// <c>effective_hours_back</c> reads 0 rather than a negative span.
    /// </summary>
    private static TrendDisclosure DescribeEmptyRoute(
        DarlingTrendReader.DurationTrendRoute route, DateTime startUtc, DateTime windowEndUtc, BucketChoice bucket)
    {
        var reach = route.Tier == RetentionTier.Raw ? route.Coverage.RawOldestUtc : route.Coverage.HourlyFloorUtc;
        var (effectiveStart, truncated) = DescribeEmptyCoverage(reach, startUtc, windowEndUtc);

        return new TrendDisclosure(
            route.Source, effectiveStart, windowEndUtc, truncated,
            TrendBuckets.Word(bucket.Minutes),
            RoutedAggregateNote(route, bucket),
            BucketMinutes: bucket.Minutes);
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
        TrendDisclosure disclosure, IReadOnlyList<BaselineDiscontinuity> discontinuities)
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
        var unratedCollections = points.Sum(p => p.UnratedCollections);
        envelope["unrated_points"] = unrated;
        /* #3897: a bucket leaves an unknowable collection out of its rates, so the collections are counted
           apart from the points — a restart inside a rated bucket is still reported. */
        envelope["unrated_collections"] = unratedCollections;
        envelope["unrated_note"] = unratedCollections == 0
            ? null
            : $"{unratedCollections} collection(s) had no knowable rate: a rate is a collection's work divided by the seconds it accrued over, and that denominator is unknowable two ways — the collection's STORED sample interval is 0 (a restart or counter reset: the collector could not difference its two snapshots, so the zeros beside it were never measured), or the collection is rated against the PREVIOUS one and has none inside the window (the window's first collection where no interval was stored, or one landing in the same second as its predecessor). Unknowable is not 0 — such a collection is left out of its point's rates rather than counted as zero, and a point that held nothing else carries null rates ({unrated} here).";
        envelope["trend"] = points.Select(p => new
        {
            time = p.CollectionTime.ToString("o"),
            value = p.Value,
            elapsed_ms_per_second = p.Value,
            execution_count = p.ExecutionCount,
            executions_per_second = p.ExecutionsPerSecond,
            /* #3897: the bucket's worst single collection (its busiest hour on the hourly tier), so a spike
               survives the bucket; null on a Query Store point, which is not a bucket. */
            peak_elapsed_ms_per_second = p.PeakElapsedMsPerSecond,
        });
        /* #3653 A5: trailing, after the points, on the data envelope only — the class remarks. */
        envelope[BaselineDiscontinuities.PayloadKey] = BaselineDiscontinuities.ToPayload(discontinuities);

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
            return new TrendDisclosure(QueryStoreTrendRouting.SourceWord(route), effectiveStart, windowEndUtc, truncated, "per-interval", null, BucketMinutes: null);
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
        string what, string checkThis, BucketChoice bucket, CancellationToken cancellationToken = default)
    {
        var disclosure = DescribeEmptyRoute(route, startUtc, windowEndUtc, bucket);

        if (!await DarlingTrendReader.HasAnySampleOnRouteAsync(postgres, rawProbe, route, serverId, cancellationToken))
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
