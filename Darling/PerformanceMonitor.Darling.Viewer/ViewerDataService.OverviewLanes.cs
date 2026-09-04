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
using PerformanceMonitor.Darling.Analysis;

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
    /// derives the collection interval from the LAG of the prior collection_time (the truncate-then-diff
    /// epoch idiom proven value-identical DuckDB↔Postgres); the outer query divides the delta by the
    /// interval for a per-second rate (delta CAST to double precision for the typed reader). Lite's
    /// per-user <c>IgnoredWaitTypes</c> exclusion clause is deliberately DROPPED — the viewer has no
    /// per-user ignore config, matching the W1b wait-picker read. $1 server_id, $2 window start, $3 window
    /// end (naive UTC).
    /// </summary>
    public const string TotalWaitTrendSql = """
        WITH per_collection AS
        (
            SELECT
                collection_time,
                SUM(delta_wait_time_ms) AS total_delta_ms,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
            FROM v_wait_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time
        )
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN CAST(total_delta_ms AS double precision) / interval_seconds ELSE 0 END AS wait_time_ms_per_second
        FROM per_collection
        ORDER BY collection_time
        """;

    /// <summary>
    /// The memory trend — Lite's <c>GetMemoryTrendAsync</c> ported to Postgres. Reads the four MB metrics
    /// from <c>v_memory_stats</c>; each is <c>numeric(18,2)</c> in the store, so all four are CAST to
    /// double precision for the typed GetDouble reader (the same numeric→double adaptation the CPU/File-IO
    /// reads make). $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string MemoryTrendSql = """
        SELECT
            collection_time,
            CAST(total_server_memory_mb AS double precision) AS total_server_memory_mb,
            CAST(target_server_memory_mb AS double precision) AS target_server_memory_mb,
            CAST(buffer_pool_mb AS double precision) AS buffer_pool_mb,
            CAST(plan_cache_mb AS double precision) AS plan_cache_mb
        FROM v_memory_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY collection_time
        """;

    /// <summary>
    /// Total wait ms/sec across all types for one server over the window (the Overview's wait lane). The
    /// read produces only <c>CollectionTime</c> + <c>WaitTimeMsPerSecond</c>; the reused
    /// <see cref="WaitStatsTrendPoint"/>'s signal-wait / avg-per-wait fields are left 0 (mirroring Lite,
    /// whose <c>GetTotalWaitTrendAsync</c> returns the same record shape with those two unset).
    /// </summary>
    public async Task<List<WaitStatsTrendPoint>> GetTotalWaitTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<WaitStatsTrendPoint>();

        await using var command = _dataSource.CreateCommand(TotalWaitTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new WaitStatsTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                0,
                0));
        }

        return items;
    }

    /// <summary>
    /// The memory trend (four MB metrics per collection) for one server over the window; the Overview's
    /// buffer-pool lane uses <see cref="MemoryTrendPoint.BufferPoolMb"/>.
    /// </summary>
    public async Task<List<MemoryTrendPoint>> GetMemoryTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<MemoryTrendPoint>();

        await using var command = _dataSource.CreateCommand(MemoryTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new MemoryTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4)));
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
