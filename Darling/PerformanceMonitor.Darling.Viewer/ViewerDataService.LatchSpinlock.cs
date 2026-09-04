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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One point on a latch class's per-second wait trend (this interval's
/// <c>delta_wait_time_ms</c> divided by the seconds since the previous collection of the SAME
/// class, via a per-class <c>LAG</c> window — the wait-stats idiom, minus the avg-per-wait metric).</summary>
public sealed record LatchStatsTrendPoint(string LatchClass, DateTime CollectionTime, double WaitTimeMsPerSecond);

/// <summary>One row of the Latch Stats latest-snapshot grid: the cumulative counters plus the last
/// interval's deltas for one latch class at the most recent collection in the window.</summary>
public sealed record LatchStatsSnapshotRow(
    string LatchClass,
    long WaitingRequestsCount,
    long WaitTimeMs,
    long MaxWaitTimeMs,
    long DeltaWaitingRequestsCount,
    long DeltaWaitTimeMs);

/// <summary>One point on a spinlock's per-second collision trend (this interval's
/// <c>delta_collisions</c> divided by the seconds since the previous collection of the SAME
/// spinlock, via a per-name <c>LAG</c> window).</summary>
public sealed record SpinlockStatsTrendPoint(string SpinlockName, DateTime CollectionTime, double CollisionsPerSecond);

/// <summary>One row of the Spinlock Stats latest-snapshot grid: the cumulative counters plus the
/// last interval's deltas for one spinlock at the most recent collection in the window.</summary>
public sealed record SpinlockStatsSnapshotRow(
    string SpinlockName,
    long Collisions,
    long Spins,
    double SpinsPerCollision,
    long SleepTime,
    long Backoffs,
    long DeltaCollisions,
    long DeltaSpins);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Latch Stats trend read: the per-second wait rate for the TOP 5 latch classes (by total delta
    /// wait time over the window), mirroring the Dashboard's <c>GetLatchStatsTopNAsync</c> top-5 grouping
    /// but normalizing to ms/sec in SQL via the per-class <c>LAG</c> interval (Darling's cumulative-delta
    /// tables have no stored <c>sample_interval_seconds</c>, so the seconds come from the same
    /// truncate-then-diff epoch idiom the wait-stats trend uses). Runs on the <c>v_latch_stats</c>
    /// passthrough view. $1 server_id, $2 window start, $3 window end (all naive UTC).
    /// </summary>
    public const string LatchTrendSql = """
        WITH top_latches AS
        (
            SELECT latch_class
            FROM v_latch_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY latch_class
            ORDER BY SUM(delta_wait_time_ms) DESC
            LIMIT 5
        ),
        raw AS
        (
            SELECT
                latch_class,
                collection_time,
                delta_wait_time_ms,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY latch_class ORDER BY collection_time)))) AS interval_seconds
            FROM v_latch_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   latch_class IN (SELECT latch_class FROM top_latches)
        )
        SELECT
            latch_class,
            collection_time,
            CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS wait_time_ms_per_second
        FROM raw
        ORDER BY latch_class, collection_time
        """;

    /// <summary>
    /// The Latch Stats latest-snapshot grid read: every latch class captured at the most recent
    /// collection in the window, ordered by the last interval's delta wait time (recent contention)
    /// then cumulative wait time, capped at 20 rows. $1 server_id, $2 start, $3 end (naive UTC).
    /// </summary>
    public const string LatchSnapshotSql = """
        WITH latest AS
        (
            SELECT MAX(collection_time) AS mx
            FROM v_latch_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            latch_class,
            waiting_requests_count,
            wait_time_ms,
            max_wait_time_ms,
            delta_waiting_requests_count,
            delta_wait_time_ms
        FROM v_latch_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT mx FROM latest)
        ORDER BY delta_wait_time_ms DESC, wait_time_ms DESC
        LIMIT 20
        """;

    /// <summary>
    /// The Spinlock Stats trend read: the per-second collision rate for the TOP 5 spinlocks (by total
    /// delta collisions over the window), the collision analog of <see cref="LatchTrendSql"/> — top-5
    /// grouping mirrors the Dashboard's <c>GetSpinlockStatsTopNAsync</c>, collisions/sec via the per-name
    /// <c>LAG</c> interval. Runs on <c>v_spinlock_stats</c>. $1 server_id, $2 start, $3 end (naive UTC).
    /// </summary>
    public const string SpinlockTrendSql = """
        WITH top_spinlocks AS
        (
            SELECT spinlock_name
            FROM v_spinlock_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY spinlock_name
            ORDER BY SUM(delta_collisions) DESC
            LIMIT 5
        ),
        raw AS
        (
            SELECT
                spinlock_name,
                collection_time,
                delta_collisions,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY spinlock_name ORDER BY collection_time)))) AS interval_seconds
            FROM v_spinlock_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   spinlock_name IN (SELECT spinlock_name FROM top_spinlocks)
        )
        SELECT
            spinlock_name,
            collection_time,
            CASE WHEN interval_seconds > 0 THEN CAST(delta_collisions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS collisions_per_second
        FROM raw
        ORDER BY spinlock_name, collection_time
        """;

    /// <summary>
    /// The Spinlock Stats latest-snapshot grid read: every spinlock captured at the most recent
    /// collection in the window, ordered by the last interval's delta collisions then cumulative
    /// collisions, capped at 20 rows. $1 server_id, $2 start, $3 end (naive UTC).
    /// </summary>
    public const string SpinlockSnapshotSql = """
        WITH latest AS
        (
            SELECT MAX(collection_time) AS mx
            FROM v_spinlock_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            spinlock_name,
            collisions,
            spins,
            spins_per_collision,
            sleep_time,
            backoffs,
            delta_collisions,
            delta_spins
        FROM v_spinlock_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT mx FROM latest)
        ORDER BY delta_collisions DESC, collisions DESC
        LIMIT 20
        """;

    /// <summary>The per-second wait trend for the top-5 latch classes over the window (empty when none).</summary>
    public async Task<List<LatchStatsTrendPoint>> GetLatchStatsTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<LatchStatsTrendPoint>();

        await using var command = _dataSource.CreateCommand(LatchTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new LatchStatsTrendPoint(
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2)));
        }

        return result;
    }

    /// <summary>The latest-snapshot latch-class rows (top 20 by recent delta wait) for the window.</summary>
    public async Task<List<LatchStatsSnapshotRow>> GetLatchStatsSnapshotAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<LatchStatsSnapshotRow>();

        await using var command = _dataSource.CreateCommand(LatchSnapshotSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new LatchStatsSnapshotRow(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5)));
        }

        return result;
    }

    /// <summary>The per-second collision trend for the top-5 spinlocks over the window (empty when none).</summary>
    public async Task<List<SpinlockStatsTrendPoint>> GetSpinlockStatsTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<SpinlockStatsTrendPoint>();

        await using var command = _dataSource.CreateCommand(SpinlockTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new SpinlockStatsTrendPoint(
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2)));
        }

        return result;
    }

    /// <summary>The latest-snapshot spinlock rows (top 20 by recent delta collisions) for the window.</summary>
    public async Task<List<SpinlockStatsSnapshotRow>> GetSpinlockStatsSnapshotAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<SpinlockStatsSnapshotRow>();

        await using var command = _dataSource.CreateCommand(SpinlockSnapshotSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new SpinlockStatsSnapshotRow(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7)));
        }

        return result;
    }
}
