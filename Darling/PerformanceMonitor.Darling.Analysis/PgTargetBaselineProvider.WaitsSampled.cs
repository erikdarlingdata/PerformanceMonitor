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
    /// <c>pg_sampled_wait_ms_per_sec</c> (lane 24 of #3691): stock PostgreSQL's all-types SAMPLED wait rate per
    /// collection — <c>Δsample_count × profile_period_ms</c> summed over every (event_type, event, query_id) series
    /// of the collection, CPU/Running excluded, over the milliseconds that collection's sampler was WATCHING —
    /// the same quantity, from the same rows, under the same rules as the wait partial's stock read
    /// (<c>PgTargetFactCollector.PgWaitSamplingSql</c>) and the sampled detector's window read
    /// (<c>PgTargetAnomalyDetector.SampledWaitRateWindowSql</c>), so the number a window is judged against is the
    /// number the facts state. Its OWN metric name, never <c>pg_wait_ms_per_sec</c>: a per-backend-sample count
    /// quantised at the sampling period has a different noise distribution from the engine's measured microsecond
    /// sum, and a bucket that pooled the two would be the unit error #3689 §5 refused (the reason v1 shipped this
    /// arm empty).
    ///
    /// <para><b>The denominator is <c>sampled_ms</c> (V133), and NULL is the whole interval.</b> The #3604 service
    /// sampler watches 30 s of each 300 s cycle and stores <c>sampled_ms = 30000</c> on every row of a collection;
    /// the <c>pg_wait_sampling</c> extension arm watches the whole interval and stores NULL; every pre-V133 row is
    /// NULL and came from EITHER arm. So per collection the observed time is <c>coalesce(sampled_ms / 1000,
    /// LAG interval)</c> — the honest rate on the sampler, lane 5's arithmetic on the extension arm and on history,
    /// never a guessed 30 s. A window that straddles the V133 install therefore holds two kinds of collection: the
    /// pre-install ones read ~10× LOW on the sampler arm. Stated here and on the fact (<c>sampled_ms_known</c>);
    /// the bucket describes what the rows disclosed, and a rate the detector judges against it is read the same
    /// way, so the two agree on the arithmetic even where the arithmetic was blind.</para>
    ///
    /// <para><b>The differencing is the reader's.</b> Per series, per consecutive collection, <c>LAG(sample_count)</c>;
    /// a count that went backwards is taken WHOLE (everything since the reset — <c>DarlingPgWaitSamplingReader</c>'s
    /// rule, never <c>GREATEST</c>'s silent zero); a series' first in-window sighting contributes nothing. The
    /// window functions run after the <c>GROUP BY collection_time</c>, so the collection's own gap is the
    /// <c>LAG</c> over the grouped collection times exactly as the Aurora arm derives it; the first collection has
    /// no predecessor and drops out of <c>clean</c>. <c>::DOUBLE PRECISION</c> on the value (the io-arm rule).</para>
    ///
    /// <para>The arm ends in <c>clean(collection_time, v)</c> + the ONE <c>RobustTierScaffold</c>, so it inherits
    /// the scaffold's bucket keying (and any local-clock keying #3653 gives the scaffold) with zero edits; window
    /// bounds <c>&gt;= $2 AND &lt; $3</c>, <c>server_id = $1</c>, no bare <c>now()</c>. <c>pg_wait_sampling</c> joins
    /// <c>DarlingRetentionHorizons.BaselineServingRawCollectors</c> with this arm (D10): its 30-day schedule default
    /// was the only thing covering the window, and it is user-editable.</para>
    /// </summary>
    private static partial string? SampledWaitBaselineQuery() => @"
WITH series AS (
    SELECT collection_time, event_type, sample_count, profile_period_ms, sampled_ms,
           LAG(sample_count) OVER (PARTITION BY event_type, event, query_id ORDER BY collection_time) AS prev_count
    FROM pg_wait_sampling
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
per_collection AS (
    SELECT collection_time,
           MAX(sampled_ms) AS sampled_ms,
           CAST(coalesce(SUM(
               CASE WHEN prev_count IS NULL THEN NULL
                    WHEN sample_count < prev_count THEN sample_count
                    ELSE sample_count - prev_count
               END * profile_period_ms) FILTER (WHERE lower(event_type) IS DISTINCT FROM 'cpu'), 0) AS DOUBLE PRECISION) AS total_wait_ms,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec
    FROM series
    GROUP BY collection_time
),
clean AS (
    SELECT collection_time,
           total_wait_ms / CAST(coalesce(sampled_ms / 1000.0, interval_sec) AS DOUBLE PRECISION) AS v
    FROM per_collection
    WHERE interval_sec > 0
    AND   coalesce(sampled_ms / 1000.0, interval_sec) > 0
)," + RobustTierScaffold;
}
