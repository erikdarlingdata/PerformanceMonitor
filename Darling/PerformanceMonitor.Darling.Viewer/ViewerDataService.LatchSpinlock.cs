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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One point on a latch class's per-second wait trend (this interval's
/// <c>delta_wait_time_ms</c> divided by the seconds it accrued over — the row's stored
/// <c>sample_interval_seconds</c>, or for pre-V127 rows the per-class <c>LAG</c> window — the wait-stats
/// idiom, minus the avg-per-wait metric). A row whose interval is unknowable is not a point (#3540).</summary>
public sealed record LatchStatsTrendPoint(string LatchClass, DateTime CollectionTime, double WaitTimeMsPerSecond);

/// <summary>One row of the Latch Stats latest-snapshot grid: the cumulative counters plus the last
/// interval's deltas for one latch class at the most recent collection in the window.
/// <para>The two deltas are nullable (#3653 A7): <c>null</c> when <see cref="SampleIntervalSeconds"/> is the
/// calculator's 0 marker — the stored (0, 0) of a restart or first sighting, whose zeros are fabricated, not
/// measured (#3540) — so the grid renders "—" where it used to render a confident 0. <see cref="IntervalDisplay"/>
/// says why beside them ("restart / first sample"), or gives the seconds the deltas accrued over, or "not
/// stored" for a pre-V127 row whose deltas are real and kept. The rule is <see cref="DeltaSeriesShaping.ReadableDelta"/>,
/// shared with Lite's twin row.</para></summary>
public sealed record LatchStatsSnapshotRow(
    string LatchClass,
    long WaitingRequestsCount,
    long WaitTimeMs,
    long MaxWaitTimeMs,
    long? DeltaWaitingRequestsCount,
    long? DeltaWaitTimeMs,
    int? SampleIntervalSeconds)
{
    /// <summary>True when the row's deltas are the unknowable marker — a stored interval of exactly 0 (the
    /// <c>FileIoStatsRow</c> / resource-semaphore idiom).</summary>
    public bool IsUnknowable => SampleIntervalSeconds == 0;

    /// <summary>The grid's Interval (sec) cell — see <see cref="DeltaSeriesShaping.IntervalDisplay"/>.</summary>
    public string IntervalDisplay => DeltaSeriesShaping.IntervalDisplay(SampleIntervalSeconds);
}

/// <summary>One point on a spinlock's per-second collision trend (this interval's
/// <c>delta_collisions</c> divided by the seconds it accrued over — the stored interval, or for pre-V127
/// rows the per-name <c>LAG</c> window). A row whose interval is unknowable is not a point (#3540).</summary>
public sealed record SpinlockStatsTrendPoint(string SpinlockName, DateTime CollectionTime, double CollisionsPerSecond);

/// <summary>One row of the Spinlock Stats latest-snapshot grid: the cumulative counters plus the
/// last interval's deltas for one spinlock at the most recent collection in the window. The deltas are
/// nullable for the reason <see cref="LatchStatsSnapshotRow"/> gives (#3653 A7).</summary>
public sealed record SpinlockStatsSnapshotRow(
    string SpinlockName,
    long Collisions,
    long Spins,
    double SpinsPerCollision,
    long SleepTime,
    long Backoffs,
    long? DeltaCollisions,
    long? DeltaSpins,
    int? SampleIntervalSeconds)
{
    /// <summary>True when the row's deltas are the unknowable marker — a stored interval of exactly 0.</summary>
    public bool IsUnknowable => SampleIntervalSeconds == 0;

    /// <summary>The grid's Interval (sec) cell — see <see cref="DeltaSeriesShaping.IntervalDisplay"/>.</summary>
    public string IntervalDisplay => DeltaSeriesShaping.IntervalDisplay(SampleIntervalSeconds);
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Latch Stats trend read: the per-second wait rate for the TOP 5 latch classes (by total delta
    /// wait time over the window), mirroring the Dashboard's <c>GetLatchStatsTopNAsync</c> top-5 grouping
    /// but normalizing to ms/sec in SQL via each row's stored <c>sample_interval_seconds</c> (V127, #3540),
    /// falling back to the per-class <c>LAG</c> interval — the same truncate-then-diff epoch idiom the
    /// wait-stats trend uses — only for pre-V127 rows that never recorded one. Runs on the <c>v_latch_stats</c>
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
                /* #3540: the STORED interval where the row has one; 0 (no delta knowable) becomes NULL through NULLIF
                   and the reader drops the row rather than reading 0.00. NULL (a pre-V127 row) falls back to the LAG. */
                CASE WHEN sample_interval_seconds IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY latch_class ORDER BY collection_time))))
                     ELSE NULLIF(sample_interval_seconds, 0)
                END AS interval_seconds
            FROM v_latch_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   latch_class IN (SELECT latch_class FROM top_latches)
        )
        SELECT
            latch_class,
            collection_time,
            CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds END AS wait_time_ms_per_second
        FROM raw
        ORDER BY latch_class, collection_time
        """;

    /// <summary>
    /// The Latch Stats latest-snapshot grid read: every latch class captured at the most recent
    /// collection in the window, ordered by the last interval's delta wait time (recent contention)
    /// then cumulative wait time, capped at 20 rows. Carries the row's stored <c>sample_interval_seconds</c>
    /// (V127, #3595) so the reader can tell a restart's (0, 0) from an idle interval's (0, n) (#3653 A7);
    /// the deltas themselves are selected as stored and the nulling happens in the reader, so the ORDER BY
    /// keeps ranking the stored numbers. $1 server_id, $2 start, $3 end (naive UTC).
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
            delta_wait_time_ms,
            sample_interval_seconds
        FROM v_latch_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT mx FROM latest)
        ORDER BY delta_wait_time_ms DESC, wait_time_ms DESC
        LIMIT 20
        """;

    /// <summary>
    /// The Spinlock Stats trend read: the per-second collision rate for the TOP 5 spinlocks (by total
    /// delta collisions over the window), the collision analog of <see cref="LatchTrendSql"/> — top-5
    /// grouping mirrors the Dashboard's <c>GetSpinlockStatsTopNAsync</c>, collisions/sec via the stored
    /// interval (per-name <c>LAG</c> for pre-V127 rows). Runs on <c>v_spinlock_stats</c>. $1 server_id, $2 start, $3 end (naive UTC).
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
                /* #3540: the STORED interval where the row has one; 0 (no delta knowable) becomes NULL through NULLIF
                   and the reader drops the row rather than reading 0.00. NULL (a pre-V127 row) falls back to the LAG. */
                CASE WHEN sample_interval_seconds IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY spinlock_name ORDER BY collection_time))))
                     ELSE NULLIF(sample_interval_seconds, 0)
                END AS interval_seconds
            FROM v_spinlock_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   spinlock_name IN (SELECT spinlock_name FROM top_spinlocks)
        )
        SELECT
            spinlock_name,
            collection_time,
            CASE WHEN interval_seconds > 0 THEN CAST(delta_collisions AS DOUBLE PRECISION) / interval_seconds END AS collisions_per_second
        FROM raw
        ORDER BY spinlock_name, collection_time
        """;

    /// <summary>
    /// The Spinlock Stats latest-snapshot grid read: every spinlock captured at the most recent
    /// collection in the window, ordered by the last interval's delta collisions then cumulative
    /// collisions, capped at 20 rows. Carries <c>sample_interval_seconds</c> for the reason
    /// <see cref="LatchSnapshotSql"/> gives. $1 server_id, $2 start, $3 end (naive UTC).
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
            delta_spins,
            sample_interval_seconds
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
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* A NULL rate is an unknowable interval (#3540): the row is dropped, not read as 0. */
            if (reader.IsDBNull(2))
            {
                continue;
            }

            result.Add(new LatchStatsTrendPoint(
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDouble(2)));
        }

        return result;
    }

    /// <summary>The latest-snapshot latch-class rows (top 20 by recent delta wait) for the window.</summary>
    public async Task<List<LatchStatsSnapshotRow>> GetLatchStatsSnapshotAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<LatchStatsSnapshotRow>();

        await using var command = _dataSource.CreateCommand(LatchSnapshotSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* #3653 A7: the stored interval decides whether the two deltas are readable. 0 is the calculator's
               marker and the deltas beside it become null (the grid shows "—"); NULL is a pre-V127 row and its
               deltas stand. The interval column is integer in the store (V127), read as such. */
            var interval = reader.IsDBNull(6) ? (int?)null : Convert.ToInt32(reader.GetValue(6));
            result.Add(new LatchStatsSnapshotRow(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                DeltaSeriesShaping.ReadableDelta(reader.IsDBNull(4) ? 0 : reader.GetInt64(4), interval),
                DeltaSeriesShaping.ReadableDelta(reader.IsDBNull(5) ? 0 : reader.GetInt64(5), interval),
                interval));
        }

        return result;
    }

    /// <summary>The per-second collision trend for the top-5 spinlocks over the window (empty when none).</summary>
    public async Task<List<SpinlockStatsTrendPoint>> GetSpinlockStatsTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<SpinlockStatsTrendPoint>();

        await using var command = _dataSource.CreateCommand(SpinlockTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* A NULL rate is an unknowable interval (#3540): the row is dropped, not read as 0. */
            if (reader.IsDBNull(2))
            {
                continue;
            }

            result.Add(new SpinlockStatsTrendPoint(
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDouble(2)));
        }

        return result;
    }

    /// <summary>The latest-snapshot spinlock rows (top 20 by recent delta collisions) for the window.</summary>
    public async Task<List<SpinlockStatsSnapshotRow>> GetSpinlockStatsSnapshotAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<SpinlockStatsSnapshotRow>();

        await using var command = _dataSource.CreateCommand(SpinlockSnapshotSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* #3653 A7 — the same three-state read as the latch snapshot. */
            var interval = reader.IsDBNull(8) ? (int?)null : Convert.ToInt32(reader.GetValue(8));
            result.Add(new SpinlockStatsSnapshotRow(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                DeltaSeriesShaping.ReadableDelta(reader.IsDBNull(6) ? 0 : reader.GetInt64(6), interval),
                DeltaSeriesShaping.ReadableDelta(reader.IsDBNull(7) ? 0 : reader.GetInt64(7), interval),
                interval));
        }

        return result;
    }
}
