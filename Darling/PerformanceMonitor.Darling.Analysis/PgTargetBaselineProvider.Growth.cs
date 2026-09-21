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
    /// <c>pg_database_growth_bytes_per_day</c>: the instance total's growth between consecutive hourly
    /// <c>pg_database_size_stats</c> samples, rated per DAY — the WAL arm's differencing shape on a LEVEL series
    /// rather than a counter (no reset trap: a size is not cumulative, and a drop is a real shrink, not a restart).
    /// One <c>total_bytes</c> per <c>collection_time</c> (the collector denormalises the total onto every database's
    /// row; <c>DISTINCT ON</c> takes one), NULL totals excluded (the collector writes NULL on every row when any
    /// database could not be sized — the R5 rule; a sum over the sized databases is NOT the instance total and none
    /// is made here), shrinks clamped to zero because the series is GROWTH and a VACUUM FULL's reclaim is not a
    /// negative growth event an operator wants a baseline of.
    ///
    /// <para>/* filled by lane 38 of #3691 — declared and filled by the content lane (no stub existed). The arm is a
    /// CTE chain ending in <c>clean(collection_time, v)</c> followed by the ONE <see cref="PgBaselineProvider.RobustTierScaffold"/>,
    /// window bounds <c>&gt;= $2 AND &lt; $3</c>, <c>::DOUBLE PRECISION</c> on the value (the io-arm rule),
    /// <c>server_id = $1</c>, no bare <c>now()</c>, no <c>EXTRACT(HOUR|DOW FROM collection_time)</c> of its own (the
    /// scaffold keys on the root's local-time expression through <c>$4..$6</c>); <c>pg_database_size_stats</c> joined
    /// <c>DarlingRetentionHorizons.BaselineServingRawCollectors</c> in the same PR (BaselineSupplyTests derives that set
    /// from the arms' text and fails without the floor). */</para>
    ///
    /// <para><b>The bucket is thin by construction, and the detector says so.</b> The size collector is hourly, so
    /// thirty days hold ~720 samples: about four per hour-of-week bucket and thirty per hour. <c>BaselineMath.SelectBucket</c>
    /// will collapse the full bucket (under its restore threshold) to the hour-only sentinel — or, on a young store,
    /// to the flat one — for months; the detector's advice states the tier the grade rested on and leans on the
    /// magnitude floor (<c>AnomalyThresholds.PgDatabaseGrowthFloorBytesPerDay</c>) until the bucket matures. The
    /// alternative — a day-of-week series with one sample a day — is not something the provider's scaffold offers
    /// and would have 30 samples in total; the hour-of-week keying with the tier fallback is the same information,
    /// honestly graded.</para>
    ///
    /// <para><b>Cost, stated.</b> Thirty days of hourly rows for one server's databases — a few thousand rows through
    /// the table's <c>(server_id, collection_time)</c> index; milliseconds, once an hour per server (the provider's
    /// <c>CacheTtl</c>), on the analysis path and never the alerting one.</para>
    /// </summary>
    private static partial string? DatabaseGrowthBytesPerDayBaselineQuery() => @"
WITH totals AS (
    SELECT DISTINCT ON (collection_time) collection_time, total_bytes
    FROM pg_database_size_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   total_bytes IS NOT NULL
    ORDER BY collection_time
),
sampled AS (
    SELECT collection_time,
           total_bytes - LAG(total_bytes) OVER series AS raw_growth_bytes,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER series))) AS interval_sec
    FROM totals
    WINDOW series AS (ORDER BY collection_time)
),
clean AS (
    SELECT collection_time, GREATEST(raw_growth_bytes, 0)::DOUBLE PRECISION * 86400.0 / interval_sec AS v
    FROM sampled
    WHERE raw_growth_bytes IS NOT NULL
    AND   interval_sec > 0
)," + PgBaselineProvider.RobustTierScaffold;
}
