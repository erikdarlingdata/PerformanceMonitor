/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetBaselineProvider
{
    /// <summary>
    /// <c>pg_io_read_latency</c>: ms per data-file read per HOUR, from the reset-aware <c>pg_io_stats</c> counter differences (<c>read_time_ms</c> / <c>reads</c> deltas, per (backend_type, context) identity, <c>object_type = 'relation'</c>), summed over identities and rated only for hours that clear the operations floor.
    /// <para>/* filled by lane 11 of #3691. The arm is a CTE chain ending in <c>clean(collection_time, v)</c>
    /// followed by the ONE <c>PgBaselineProvider.RobustTierScaffold</c>, window bounds <c>&gt;= $2 AND &lt; $3</c>,
    /// <c>::DOUBLE PRECISION</c> on the value (the io-arm rule), <c>server_id = $1</c>, no bare <c>now()</c>; the
    /// baseline census in <c>PgTargetAnomalyTests</c> gained the metric's (name, table) row in the same PR. */</para>
    ///
    /// <para><b>The hour, not the collection — and what that costs in tier.</b> The quantity is the one the
    /// calibration read distributed (§B1: per-server HOURLY ms/read) and the one
    /// <c>PgTargetAnomalyDetector.IoLatencyWindowSql</c> takes its peak from; a one-minute quotient would be a
    /// noisier series of the same thing and the operations floor could not be applied to it. The cost: the
    /// scaffold keys each <c>clean</c> row by hour-of-day × day-of-week, so one row per hour is ONE sample per
    /// hour-of-week bucket per week — four or five in a 30-day window, under <c>BaselineMath.RestoreThreshold</c>
    /// (15) — and <c>BaselineMath.SelectBucket</c> collapses to the hour-of-DAY tier (~30 samples, one per day,
    /// confidence 0.85) for every pass. The buckets are therefore hour-of-day in practice until a finer grain is
    /// chosen (a 15-minute bucket with a 250-read floor would put ~17 samples in each hour-of-week bucket; stated
    /// on #3691 for the coordinator's call, not decided in this lane), and the detector's confidence multiplier
    /// says so on every fact it fires.</para>
    ///
    /// <para><b>The differencing.</b> <c>MAX(counter)</c> per identity per hour is that identity's last value in the
    /// hour (cumulative within an epoch); the hour-over-hour difference clamped at 0 is the hour's operations, a
    /// reset dropping one hour's contribution rather than the series (the same posture as the collector's
    /// per-sample <c>GREATEST(raw, 0)</c>, at the coarser grain). The first hour of each identity has no
    /// predecessor, is NULL, and is not in <c>per_hour</c>. Hours under the floor (the literal 1000 —
    /// <c>PgTargetScorer.IoMinimumOps</c>; <c>PgTargetIoTests</c> pins them equal) are not rated: a quotient over
    /// four reads is noise on either side of the comparison. With <c>track_io_timing</c> off every rated hour is
    /// 0 ms; the bucket then describes nothing and the detector's floor keeps it silent.</para>
    /// </summary>
    private static partial string? IoReadLatencyBaselineQuery() => @"
WITH hourly AS (
    SELECT backend_type, context,
           date_trunc('hour', collection_time) AS hour_start,
           MAX(reads)        AS reads,
           MAX(read_time_ms) AS read_ms
    FROM pg_io_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   object_type = 'relation'
    GROUP BY backend_type, context, date_trunc('hour', collection_time)
),
deltas AS (
    SELECT hour_start,
           reads   - LAG(reads)   OVER series AS raw_reads,
           read_ms - LAG(read_ms) OVER series AS raw_read_ms
    FROM hourly
    WINDOW series AS (PARTITION BY backend_type, context ORDER BY hour_start)
),
per_hour AS (
    SELECT hour_start,
           SUM(GREATEST(raw_reads, 0))::DOUBLE PRECISION   AS reads,
           SUM(GREATEST(raw_read_ms, 0))::DOUBLE PRECISION AS read_ms
    FROM deltas
    WHERE raw_reads IS NOT NULL
    GROUP BY hour_start
),
clean AS (
    SELECT hour_start AS collection_time, read_ms / reads AS v
    FROM per_hour
    WHERE reads >= 1000 /* PgTargetScorer.IoMinimumOps — the per-hour operations floor */
)," + RobustTierScaffold;
}
