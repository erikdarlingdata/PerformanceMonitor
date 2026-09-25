/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One memory-stats trend point — a mirror of Lite's <c>MemoryTrendPoint</c>
/// (<c>LocalDataService.Memory.cs</c>). The Overview lanes plot only <see cref="BufferPoolMb"/>, but the
/// full four-metric shape is carried for parity with Lite's row (the future Memory tab port reuses it),
/// exactly as the wait picker's <see cref="WaitStatsTrendPoint"/> carries its signal-wait field.</summary>
public sealed record MemoryTrendPoint(
    DateTime CollectionTime,
    double TotalServerMemoryMb,
    double TargetServerMemoryMb,
    double BufferPoolMb,
    double PlanCacheMb);

/// <summary>
/// The Overview inner tab's correlated-timeline-lanes reads (W1d viewer copy-parity) that don't already
/// exist from earlier waves: the single-line TOTAL wait trend (Lite's <c>GetTotalWaitTrendAsync</c>) and
/// the memory trend (Lite's <c>GetMemoryTrendAsync</c>), plus the per-lane baseline lookup that wraps the
/// shared <see cref="PgBaselineProvider"/>. The lanes' other four feeds are REUSED as-is:
/// <c>GetCpuUtilizationAsync</c> (W1a, raw CPU samples), and <c>GetBlockingTrendAsync</c> /
/// <c>GetDeadlockTrendAsync</c> / <c>GetFileIoLatencyTrendAsync</c> (W1c). Both new reads run against the
/// <c>v_wait_stats</c> / <c>v_memory_stats</c> passthrough views (the Darling-analysis convention) with
/// the naive-UTC window bound Kind-Unspecified, like every other viewer read.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Total wait time across ALL wait types as one per-second series — Lite's <c>GetTotalWaitTrendAsync</c>
    /// ported to Postgres. A <c>per_collection</c> CTE SUMs <c>delta_wait_time_ms</c> per collection and
    /// takes the collection's stored interval (MAX of its rows' <c>sample_interval_seconds</c>, #3540),
    /// deriving it from the LAG of the prior collection_time (the truncate-then-diff epoch idiom proven
    /// value-identical DuckDB↔Postgres) only for pre-V127 collections; the outer query divides the delta by
    /// the interval for a per-second rate (delta CAST to double precision for the typed reader), NULL — and
    /// the point dropped — when the whole collection was unknowable. Lite's
    /// per-user <c>IgnoredWaitTypes</c> exclusion clause is deliberately DROPPED — the viewer has no
    /// per-user ignore config, matching the W1b wait-picker read.
    ///
    /// <para><b>#4234: BUCKETED</b> — a rate is the summed delta over the summed interval (ruling item 2),
    /// the same time-weighted rule <c>WaitTrendsSql</c> uses for the per-type picker trend, generalized to
    /// one aggregate series. A bucket with no rated collection is dropped (<c>HAVING</c>), matching the old
    /// per-collection read's own drop of an unrated collection. <c>first_collection_time</c> /
    /// <c>collection_count</c> let the C# reader stamp a bucket that merged nothing at its one collection's
    /// own raw time (ruling item 3) rather than the bucket grid. $4 the bucket width in minutes.</para>
    /// </summary>
    public static readonly string TotalWaitTrendSql = $"""
        WITH per_collection AS
        (
            SELECT
                collection_time,
                SUM(delta_wait_time_ms) AS total_delta_ms,
                /* #3540: the collection's STORED interval — MAX over its rows, because a wait type first
                   seen in an otherwise steady pass carries 0 beside its siblings' real interval and adds 0
                   to the sum; MAX is 0 only when EVERY row was unknowable (a restart), and that 0 becomes
                   NULL through NULLIF so the point is dropped rather than rendered as 0.00. NULL (pre-V127
                   rows) falls back to the LAG this read always used. */
                CASE WHEN MAX(sample_interval_seconds) IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                     ELSE NULLIF(MAX(sample_interval_seconds), 0)
                END AS interval_seconds
            FROM v_wait_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time
        ),
        rated AS
        (
            SELECT
                collection_time,
                CASE WHEN interval_seconds > 0 THEN total_delta_ms END AS rated_delta_ms,
                CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds
            FROM per_collection
        )
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            CAST(SUM(rated_delta_ms) AS double precision) / SUM(rated_seconds) AS wait_time_ms_per_second,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM rated
        GROUP BY 1
        HAVING COUNT(rated_seconds) > 0
        ORDER BY 1
        """;

    /// <summary>
    /// The memory trend — Lite's <c>GetMemoryTrendAsync</c> ported to Postgres. Reads the four MB metrics
    /// from <c>v_memory_stats</c>; each is <c>numeric(18,2)</c> in the store, so all four are CAST to
    /// double precision for the typed GetDouble reader (the same numeric→double adaptation the CPU/File-IO
    /// reads make).
    ///
    /// <para><b>#4234: BUCKETED</b> — all four are gauges (ruling item 2): each bucket's value is the
    /// average of its collections, with no MAX/peak column since the Overview's buffer-pool lane (the only
    /// consumer, along with the Memory tab, both plain lines) draws none. <c>first_collection_time</c> /
    /// <c>collection_count</c> let the C# reader stamp a bucket that merged nothing at its one collection's
    /// own raw time (ruling item 3). $4 the bucket width in minutes.</para>
    /// </summary>
    public static readonly string MemoryTrendSql = $"""
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            AVG(CAST(total_server_memory_mb AS double precision)) AS total_server_memory_mb,
            AVG(CAST(target_server_memory_mb AS double precision)) AS target_server_memory_mb,
            AVG(CAST(buffer_pool_mb AS double precision)) AS buffer_pool_mb,
            AVG(CAST(plan_cache_mb AS double precision)) AS plan_cache_mb,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM v_memory_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>
    /// Total wait ms/sec across all types for one server over the window (the Overview's wait lane),
    /// bucketed to <see cref="TrendBudget.Chart"/>'s width (#4234; single series, so <c>seriesCount</c> is
    /// always 1 into <see cref="TrendBuckets.AutoMinutes"/>, matching <c>GetWaitStatsTrendsByTypesAsync</c>).
    /// The read produces only <c>CollectionTime</c> + <c>WaitTimeMsPerSecond</c>; the reused
    /// <see cref="WaitStatsTrendPoint"/>'s signal-wait / avg-per-wait fields are left 0 (mirroring Lite,
    /// whose <c>GetTotalWaitTrendAsync</c> returns the same record shape with those two unset). A bucket
    /// holding exactly one physical collection is stamped at that collection's own raw time rather than the
    /// bucket grid when EVERY bucket this call returned is such a singleton (ruling item 3).
    /// </summary>
    public async Task<List<WaitStatsTrendPoint>> GetTotalWaitTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(TotalWaitTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(DateTime BucketStart, DateTime FirstCollectionTime, double Rate)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* A NULL rate is an unknowable interval (#3540): the row is dropped, not read as 0. The HAVING
               clause already excludes an all-unrated bucket, so this is a defensive guard, not a live path. */
            if (reader.IsDBNull(1))
            {
                continue;
            }

            if (reader.GetInt64(3) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((reader.GetDateTime(0), reader.GetDateTime(2), reader.GetDouble(1)));
        }

        var items = new List<WaitStatsTrendPoint>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new WaitStatsTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.Rate,
                0,
                0));
        }

        return items;
    }

    /// <summary>
    /// The memory trend (four MB metrics per bucket) for one server over the window; the Overview's
    /// buffer-pool lane uses <see cref="MemoryTrendPoint.BufferPoolMb"/>. Bucketed to
    /// <see cref="TrendBudget.Chart"/>'s width (#4234); a bucket holding exactly one physical collection is
    /// stamped at that collection's own raw time rather than the bucket grid when EVERY bucket this call
    /// returned is such a singleton (ruling item 3).
    /// </summary>
    public async Task<List<MemoryTrendPoint>> GetMemoryTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(MemoryTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(DateTime BucketStart, DateTime FirstCollectionTime, double Total, double Target, double BufferPool, double PlanCache)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(6) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.GetDateTime(5),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4)));
        }

        var items = new List<MemoryTrendPoint>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new MemoryTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.Total,
                row.Target,
                row.BufferPool,
                row.PlanCache));
        }

        return items;
    }

    /// <summary>Lazily-built baseline provider over this service's pooled data source (Lite's lazy
    /// <c>GetBaselineProvider</c> mirrored). The provider holds only an in-memory cache and a borrowed
    /// reference to <c>_dataSource</c> — it does not own the data source, so it needs no disposal.</summary>
    private PgBaselineProvider? _baselineProvider;

    private PgBaselineProvider GetBaselineProvider()
        => _baselineProvider ??= new PgBaselineProvider(_dataSource);

    /// <summary>
    /// The baseline (mean ± stddev bucket) for a chart-unit metric at a reference time, for the lanes'
    /// baseline bands + anomaly markers — Lite's <c>GetBaselineForLaneAsync</c> mirrored over the shared
    /// <see cref="PgBaselineProvider"/> (in-memory compute from 30-day rolling history, no persistence).
    /// Returns <see cref="BaselineBucket.Empty"/> when the bucket has no samples, so the lane renderers'
    /// <c>SampleCount &gt; 0</c> guard simply skips the band. <paramref name="referenceTime"/> is naive UTC
    /// (the baseline buckets key on hour-of-day / day-of-week of the store's naive-UTC timestamps).
    /// </summary>
    public async Task<BaselineBucket> GetBaselineForLaneAsync(
        int serverId, string metricName, DateTime referenceTime)
    {
        var baseline = await GetBaselineProvider().GetBaselineAsync(serverId, metricName, referenceTime);
        return baseline.SampleCount > 0 ? baseline : BaselineBucket.Empty;
    }
}
