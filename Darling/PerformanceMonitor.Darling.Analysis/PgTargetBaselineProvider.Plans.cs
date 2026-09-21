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
    /// <c>pg_statement_mean_ms</c>: the SERVER-WIDE mean execution ms per statement call, per collection, from
    /// <c>pg_statement_stats</c> — Σ <c>delta_total_exec_time_ms</c> over Σ <c>delta_calls</c> across every statement
    /// row stamped with one <c>collection_time</c>, the collector's STORED deltas (never a re-differencing of the
    /// cumulative columns — <c>PgTargetFactCollector.Queries.cs</c>'s discipline) and so already reset-aware.
    ///
    /// <para><b>THE CONSTRAINT THAT CHOSE THIS ARM IS LIFTED; the arm is now the COLD FALLBACK.</b>
    /// When this arm was written <c>PgBaselineProvider.GetBaselineAsync</c> keyed one series on (<c>server_id</c>, metric)
    /// with no per-statement dimension, so a per-<c>queryid</c> baseline needed either a key dimension on the shared
    /// seam (a shared-file change, reported to the coordinator as an out-of-lane item, never made inside the lane) or
    /// ONE series for the whole server — and v3 took the second. The key dimension landed in #3810
    /// (<c>ResolveKeyedBaselineQuery</c>, the third seam) with a keyed arm of this very metric
    /// (<c>PgTargetBaselineProvider.Statements.cs</c>), and calibration D then measured both series over the same
    /// 50 Aurora PostgreSQL clusters (§D5, 28 days fenced at 2026-09-19 16:40Z): the server-wide mean's hour-of-week
    /// ratio never reached the 3.0 multiple in a week (p99 2.05, maximum 4.4) while the keyed series' tail runs to
    /// 90× — a per-statement step is diluted to nothing in the server's mean. So <c>ANOMALY_PG_PLAN_REGRESSION</c>
    /// grades on the KEYED series (lane 39) and this arm is retained for exactly one case: a server with no
    /// trustworthy keyed bucket for any flipped statement, where a blunt reading beats no reading and the fact
    /// stamps <c>series = 0</c> to say which it is. Blunt is the word: the anomaly it feeds on that path says
    /// "this server's statements got slower per call than this hour usually sees", not "this statement did" — a
    /// heavier parameter mix, a colder cache or one new expensive statement all move this series too, and the
    /// detector's doc and the advice both say so. The per-statement flip is <c>PG_PLAN_REGRESSION</c>'s (the
    /// collector, with its own bars), which the anomaly folds onto either way
    /// (<c>PgTargetFactKeys.AnomalyToFamilies</c>).</para>
    ///
    /// <para><b>Why a per-call mean and not a total.</b> Σ exec time per collection is throughput-shaped — it rises
    /// with calls — and lane 9's <c>pg_tps</c> already baselines throughput. Dividing by calls gives the quantity a
    /// plan change moves: what one call costs. A collection with no calls (<c>HAVING SUM(delta_calls) &gt; 0</c>) is
    /// not a sample — a mean over no calls is undefined, not zero — so an idle minute contributes nothing, which is
    /// the same rule the fact family applies to its own means.</para>
    ///
    /// <para><b>Cost, stated.</b> Thirty days of one-minute <c>pg_statement_stats</c> for one server is every statement
    /// shape's row per minute — tens of millions of rows on a busy server — read once through the (server_id,
    /// collection_time) index and aggregated to one row per collection (~43,000), once an hour per server (the
    /// provider's <c>CacheTtl</c>), on the analysis path and never the alerting one. It is the family's heaviest read
    /// by an order of magnitude and the reason <c>pg_statement_stats</c> joins
    /// <c>DarlingRetentionHorizons.BaselineServingRawCollectors</c> in this PR: its 30-day schedule default was the only
    /// thing covering the window, and it is user-editable (D10, the #1757 shape).</para>
    ///
    /// <para>/* filled by lane 27 — the marker stays, as v1's did. The arm is a CTE chain ending in
    /// <c>clean(collection_time, v)</c> followed by the ONE <see cref="PgBaselineProvider.RobustTierScaffold"/> — the
    /// #3653 Q6 contract: the scaffold binds the hour-of-week key through the ROOT's clock parameters (<c>$4..$6</c>
    /// after <c>$3</c>), never its own <c>EXTRACT</c> (LocalClockBucketKeyTests reads every arm) — window bounds
    /// <c>&gt;= $2 AND &lt; $3</c>, <c>::DOUBLE PRECISION</c> on the value (the io-arm rule: STDDEV_SAMP over numeric
    /// can overflow System.Decimal at materialisation), <c>server_id = $1</c>, no bare <c>now()</c>; the baseline
    /// census in <c>PgTargetAnomalyTests</c> gains the metric's (name, table) row in the same PR. */</para>
    /// </summary>
    private static partial string? StatementMeanMsBaselineQuery() => @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_total_exec_time_ms)::DOUBLE PRECISION / SUM(delta_calls) AS mean_ms
    FROM pg_statement_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY collection_time
    HAVING SUM(delta_calls) > 0
),
clean AS (
    SELECT collection_time, mean_ms AS v
    FROM per_collection
)," + RobustTierScaffold;
}
