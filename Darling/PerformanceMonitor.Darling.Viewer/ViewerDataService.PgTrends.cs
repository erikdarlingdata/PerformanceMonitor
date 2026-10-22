/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The four PostgreSQL trend reads the web serves (#6385): <c>get_pg_database_trend</c>,
/// <c>get_pg_query_duration_trend</c>, <c>get_pg_wait_trend</c> and <c>get_pg_io_trend</c>. Every point comes from
/// <c>DarlingPgTrendReader</c> in the Storage project, the SAME reader the MCP tools and the web's
/// <c>/api/read</c> call - no query is copied here. What this file adds is only the two steps the MCP tool wraps
/// around that reader and the viewer cannot reach (the tool lives in the service): pick the subject the tool picks
/// when none is named (the busiest database / statement / wait event / I/O pair of the window), and size the bucket
/// the way the web's chart does (<see cref="TrendBudget.Chart"/>, one series, auto width).
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>One trend read: the subject that was followed (null when the window held nothing to follow), the
    /// bucket width the read used (minutes; 0 for the wait trend, which the web reads per collection), and its points.</summary>
    public sealed record PgTrend<TPoint>(string? Subject, int BucketMinutes, IReadOnlyList<TPoint> Points);

    /// <summary>A trend with nothing to follow in the window.</summary>
    public static PgTrend<TPoint> NoPgTrend<TPoint>(int bucketMinutes = 0) => new(null, bucketMinutes, Array.Empty<TPoint>());

    /// <summary>
    /// The bucket width the web's chart asks for over this window: the MCP tools' own resolver with no width
    /// requested and the chart budget, over the window's whole hours (the web sends whole <c>hours_back</c>).
    /// </summary>
    internal static int PgTrendBucketMinutes(DateTime startUtc, DateTime endUtc)
    {
        var hours = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalHours));
        TrendBuckets.Resolve(hours, null, 1, TrendBudget.Chart, out var bucketMinutes);
        return bucketMinutes;
    }

    /// <summary>The wait trend of the window's dominant wait event (the web's default subject).</summary>
    public async Task<PgTrend<DarlingPgTrendReader.PgWaitTrendPoint>> GetPgWaitTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var chosen = await DarlingPgTrendReader.GetDominantWaitEventAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);
        if (string.IsNullOrWhiteSpace(chosen))
        {
            return NoPgTrend<DarlingPgTrendReader.PgWaitTrendPoint>(0);
        }

        var points = await DarlingPgTrendReader.GetWaitTrendAsync(_dataSource, serverId, chosen, startUtc, endUtc, cancellationToken);
        return new(chosen, 0, points);
    }

    /// <summary>The duration trend of the window's costliest statement, bucketed as the web's chart is.</summary>
    public async Task<PgTrend<DarlingPgTrendReader.PgQueryDurationTrendPoint>> GetPgQueryDurationTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var bucketMinutes = PgTrendBucketMinutes(startUtc, endUtc);
        var top = await DarlingPgTrendReader.GetTopQueryIdAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);
        if (top is null)
        {
            return NoPgTrend<DarlingPgTrendReader.PgQueryDurationTrendPoint>(bucketMinutes);
        }

        var points = await DarlingPgTrendReader.GetQueryDurationBucketsAsync(
            _dataSource, serverId, top.Value, startUtc, endUtc, bucketMinutes, cancellationToken);
        return new(top.Value.ToString(System.Globalization.CultureInfo.InvariantCulture), bucketMinutes, points);
    }

    /// <summary>The I/O trend plus the two facts the web's payload applies before it shows latency and bytes:
    /// whether the server times I/O (<c>track_io_timing</c>, from its collected setting, else whether any interval
    /// shows time) and whether the byte rates are measured or estimated. A zero latency with timing off is a fact
    /// about the configuration, not the disk, so the chart draws no latency line then.</summary>
    public sealed record PgIoTrend(
        string? Subject, int BucketMinutes, IReadOnlyList<DarlingPgTrendReader.PgIoTrendPoint> Points,
        bool TimingTracked, bool BytesShown, bool WriteCountersTracked = true)
    {
        /// <summary>The write rate to plot for <paramref name="point"/>. Null when the server has no write counters
        /// (Amazon Aurora: backends do not write data files, the reader coalesces the NULL side to 0), exactly as
        /// the web's <c>get_pg_io_trend</c> nulls <c>writes_per_second</c> on <c>write_counters_tracked</c> false,
        /// so the chart draws no line instead of a flat zero that reads as "idle".</summary>
        public double? WritesPerSecondOf(DarlingPgTrendReader.PgIoTrendPoint point) =>
            WriteCountersTracked ? point.WritesPerSecond : null;
    }

    /// <summary>The I/O trend of the window's busiest (backend type, context) pair, bucketed as the web's chart is.
    /// The subject reads as "backend / context".</summary>
    public async Task<PgIoTrend> GetPgIoTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var bucketMinutes = PgTrendBucketMinutes(startUtc, endUtc);
        var fromEndpoints = await DarlingPgTrendReader.IsTimescaleDbStoreAsync(_dataSource, cancellationToken);
        var dominant = await DarlingPgTrendReader.GetDominantIoSubjectAsync(
            _dataSource, serverId, startUtc, endUtc, null, null, fromEndpoints, cancellationToken);
        if (dominant is null)
        {
            return new(null, bucketMinutes, Array.Empty<DarlingPgTrendReader.PgIoTrendPoint>(), false, false);
        }

        var points = await DarlingPgTrendReader.GetIoTrendAsync(
            _dataSource, serverId, dominant.Value.BackendType, dominant.Value.Context, startUtc, endUtc, bucketMinutes, cancellationToken);
        if (points.Count == 0)
        {
            return new(null, bucketMinutes, points, false, false);
        }

        var timingSetting = await DarlingPgTrendReader.GetIoTimingTrackedAsync(_dataSource, serverId, endUtc, cancellationToken);
        var timingObserved = points.Any(p => p.ReadTimeMs > 0 || p.WriteTimeMs > 0);
        var bytesMeasured = points.Any(p => p.BytesMeasured);
        var bytesShown = bytesMeasured || points.Any(p => p.BytesEstimable);
        return new($"{dominant.Value.BackendType} / {dominant.Value.Context}", bucketMinutes, points,
            timingSetting ?? timingObserved, bytesShown, points.Any(p => p.WriteCountersTracked));
    }

    /// <summary>The database trend of the window's busiest database, bucketed as the web's chart is.</summary>
    public async Task<PgTrend<DarlingPgTrendReader.PgDatabaseTrendPoint>> GetPgDatabaseTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var bucketMinutes = PgTrendBucketMinutes(startUtc, endUtc);
        var chosen = await DarlingPgTrendReader.GetTopDatabaseAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);
        if (chosen is null)
        {
            return NoPgTrend<DarlingPgTrendReader.PgDatabaseTrendPoint>(bucketMinutes);
        }

        var points = await DarlingPgTrendReader.GetDatabaseTrendAsync(
            _dataSource, serverId, chosen, startUtc, endUtc, bucketMinutes, cancellationToken);
        return new(chosen, bucketMinutes, points);
    }
}
