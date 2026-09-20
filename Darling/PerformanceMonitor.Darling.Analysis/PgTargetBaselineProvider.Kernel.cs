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
    /// <c>pg_cpu_burn_cores</c>: cores busy per collection from <c>pg_kernel_stats</c> — the reset-aware
    /// <c>exec_user_time_ms + exec_system_time_ms + plan_cpu_time_ms</c> differences (<c>stats_since</c> the reset
    /// witness) summed across statements, over the collection's own gap. /* filled by lane 28 of #3691 — the marker
    /// stays, as v1's did. The arm is a CTE chain ending in <c>clean(collection_time, v)</c> followed by the ONE
    /// <see cref="PgBaselineProvider.RobustTierScaffold"/> — the #3653 Q6 contract: the scaffold binds the hour-of-week
    /// key through the ROOT's clock parameters (<c>$4..$6</c> after <c>$3</c>), never its own <c>EXTRACT</c>
    /// (LocalClockBucketKeyTests reads every arm) — window bounds <c>&gt;= $2 AND &lt; $3</c>, <c>::DOUBLE PRECISION</c>
    /// on the value, <c>server_id = $1</c>, no bare <c>now()</c>; the baseline census in <c>PgTargetAnomalyTests</c>
    /// gained the metric's (name, table) row in the same PR, and <c>pg_kernel_stats</c> joined
    /// <c>DarlingRetentionHorizons.BaselineServingRawCollectors</c> the day this arm first read it (BaselineSupplyTests
    /// derives that set from the arms' text and fails without the floor). */
    ///
    /// <para><b>One differencing, three readers.</b> This is <c>PgTargetFactCollector.PgTargetKernelCpuSql</c>'s
    /// differencing with the half-open upper bound (<c>&lt; $3</c>, every arm's shape): <c>LAG</c> per (database,
    /// <c>query_id</c>) identity, a reset (<c>stats_since</c> moved, or the counter went backwards) taking the current
    /// value WHOLE (the table's own reader's rule — the counters restarted from zero inside the interval, so the current
    /// value is the CPU since), a delta admitted only when the identity's previous row is the immediately preceding
    /// collection (<c>prev_k = k - 1</c> on the window's <c>DENSE_RANK</c> ordinal — the top-500 capture can drop and
    /// re-admit a statement; a re-entry's lump would spike one sample),
    /// summed per collection over the series' own second-truncated gap. The fact's peak and mean, the detector's window
    /// statistics and this bucket are therefore one unit — cores busy per collection — and a rate the fact states is a
    /// rate the bucket can be asked about.</para>
    ///
    /// <para><b>A collection with rows and zero delta is a ZERO sample; a collection with no rows is missing.</b> The
    /// collector writes a row per tracked statement whenever the function answers, so a quiet minute on an installed
    /// extension is rows with zero deltas — a legitimate small value that stays in — while no rows at all (the extension
    /// absent, or the collector off) yields an EMPTY bucket (<c>SampleCount</c> 0, the "no baseline" answer) rather than
    /// a thirty-day distribution of zeros with a zero MAD against which any first core would be infinitely deviant. The
    /// detector reads the window before it asks for this bucket, so on the measured fleet (no <c>pg_stat_kcache</c>)
    /// this query does not run at all; the shape here is the belt to that brace.</para>
    /// </summary>
    private static partial string? CpuBurnCoresBaselineQuery() => @"
WITH ranked AS (
    SELECT
        collection_time,
        database_name,
        query_id,
        coalesce(exec_user_time_ms, 0)   AS user_ms_total,
        coalesce(exec_system_time_ms, 0) AS system_ms_total,
        coalesce(plan_cpu_time_ms, 0)    AS plan_ms_total,
        stats_since,
        DENSE_RANK() OVER (ORDER BY collection_time) AS k
    FROM pg_kernel_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   query_id IS NOT NULL
),
series AS (
    SELECT
        collection_time,
        user_ms_total,
        system_ms_total,
        plan_ms_total,
        stats_since,
        k,
        LAG(k)               OVER identity AS prev_k,
        LAG(collection_time) OVER identity AS prev_time,
        LAG(user_ms_total)   OVER identity AS prev_user,
        LAG(system_ms_total) OVER identity AS prev_system,
        LAG(plan_ms_total)   OVER identity AS prev_plan,
        LAG(stats_since)     OVER identity AS prev_since
    FROM ranked
    WINDOW identity AS (PARTITION BY database_name, query_id ORDER BY collection_time)
),
deltas AS (
    SELECT
        collection_time,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', prev_time))) AS interval_sec,
        CASE WHEN stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user
             THEN user_ms_total + system_ms_total + plan_ms_total
             ELSE (user_ms_total - prev_user) + (system_ms_total - prev_system) + GREATEST(plan_ms_total - prev_plan, 0) END AS cpu_ms
    FROM series
    WHERE prev_k = k - 1
),
clean AS (
    SELECT collection_time,
           (SUM(cpu_ms) / (interval_sec * 1000.0))::DOUBLE PRECISION AS v
    FROM deltas
    WHERE interval_sec > 0
    GROUP BY collection_time, interval_sec
)," + PgBaselineProvider.RobustTierScaffold;
}
