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
    /// <c>pg_io_read_latency</c>: ms per data-file read per FIFTEEN-MINUTE sample, from the reset-aware <c>pg_io_stats</c> counter differences (<c>read_time_ms</c> / <c>reads</c> deltas, per (backend_type, context) identity, <c>object_type = 'relation'</c>), summed over identities and rated only for samples that clear the reads floor.
    /// <para>/* filled by lane 11 of #3691. The arm is a CTE chain ending in <c>clean(collection_time, v)</c>
    /// followed by the ONE <c>PgBaselineProvider.RobustTierScaffold</c>, window bounds <c>&gt;= $2 AND &lt; $3</c>,
    /// <c>::DOUBLE PRECISION</c> on the value (the io-arm rule), <c>server_id = $1</c>, no bare <c>now()</c>; the
    /// baseline census in <c>PgTargetAnomalyTests</c> gained the metric's (name, table) row in the same PR. */</para>
    ///
    /// <para><b>The quarter-hour, not the hour — and why (#3691 between waves, lane 11's own proposal).</b> The
    /// quantity is the one the calibration read distributed (§B1: ms per read) and the one
    /// <c>PgTargetAnomalyDetector.IoLatencyWindowSql</c> takes its peak from; a one-minute quotient would be a
    /// noisier series of the same thing and no floor could be applied to it. Lane 11 shipped it at the HOUR grain,
    /// and the scaffold keys each <c>clean</c> row by hour-of-day × day-of-week, so one row per hour was ONE sample
    /// per hour-of-week bucket per week — four or five in a 30-day window, under <c>BaselineMath.RestoreThreshold</c>
    /// (15) — and <c>BaselineMath.SelectBucket</c> collapsed to the hour-of-DAY tier (~30 samples, confidence 0.85)
    /// on EVERY pass; the live e2e pinned that collapse. Four samples an hour puts ~17–20 in each hour-of-week
    /// bucket, so the tier the operator reads ("routine for this hour of the week") is the one that answers. The
    /// ms-per-read quotient survives re-bucketing (a ratio of two sums is the same ratio over any partition of the
    /// same rows); what changes is the per-sample reads population, which is why the floor moved with the grain.
    /// <c>date_bin('15 minutes', collection_time, TIMESTAMP '2000-01-01')</c> is the bucket: a fixed origin on a
    /// quarter-hour boundary, so the four samples of an hour are :00 / :15 / :30 / :45 for every server.</para>
    ///
    /// <para><b>The differencing.</b> <c>MAX(counter)</c> per identity per sample is that identity's last value in
    /// the quarter-hour (cumulative within an epoch); the sample-over-sample difference clamped at 0 is the sample's
    /// operations, a reset dropping one sample's contribution rather than the series (the same posture as the
    /// collector's per-sample <c>GREATEST(raw, 0)</c>, at the coarser grain). The first sample of each identity has
    /// no predecessor, is NULL, and is not in <c>per_sample</c>. Samples under the floor (the literal 250 —
    /// <c>PgTargetScorer.IoBaselineBucketMinimumReads</c>; <c>PgTargetIoTests</c> pins them equal) are not rated: a
    /// quotient over a handful of reads is noise on either side of the comparison, and the low-reads tail is what
    /// the calibration read saw at 1–17 reads an hour. With <c>track_io_timing</c> off every rated sample is 0 ms;
    /// the bucket then describes nothing and the detector's floor keeps it silent.</para>
    /// </summary>
    private static partial string? IoReadLatencyBaselineQuery() => @"
WITH sampled AS (
    SELECT backend_type, context,
           date_bin('15 minutes', collection_time, TIMESTAMP '2000-01-01') AS sample_start,
           MAX(reads)        AS reads,
           MAX(read_time_ms) AS read_ms
    FROM pg_io_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   object_type = 'relation'
    GROUP BY backend_type, context, date_bin('15 minutes', collection_time, TIMESTAMP '2000-01-01')
),
deltas AS (
    SELECT sample_start,
           reads   - LAG(reads)   OVER series AS raw_reads,
           read_ms - LAG(read_ms) OVER series AS raw_read_ms
    FROM sampled
    WINDOW series AS (PARTITION BY backend_type, context ORDER BY sample_start)
),
per_sample AS (
    SELECT sample_start,
           SUM(GREATEST(raw_reads, 0))::DOUBLE PRECISION   AS reads,
           SUM(GREATEST(raw_read_ms, 0))::DOUBLE PRECISION AS read_ms
    FROM deltas
    WHERE raw_reads IS NOT NULL
    GROUP BY sample_start
),
clean AS (
    SELECT sample_start AS collection_time, read_ms / reads AS v
    FROM per_sample
    WHERE reads >= 250 /* PgTargetScorer.IoBaselineBucketMinimumReads — the per-sample reads floor */
)," + RobustTierScaffold;
}
