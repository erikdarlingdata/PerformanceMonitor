/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// The statement family's KEYED baseline arms (#3691 lane 33): one hour×day-of-week series per (server, metric,
/// <c>queryid</c>), reached through the five-argument
/// <see cref="PgBaselineProvider.GetBaselineAsync(int, string, string?, DateTime, CancellationToken)"/> with the
/// <c>queryid</c> as text. The seam exists for Erik's 2026-09-20 ruling: the bad-actor share is graded as deviation
/// from the statement's OWN hour-of-week share baseline, which one series per (server, metric) cannot express.
/// Both arms are consumed now: the share by lane 34's <c>ANOMALY_PG_BAD_ACTOR_SHARE</c>, the per-call mean by lane 39's
/// switch of <c>ANOMALY_PG_PLAN_REGRESSION</c> onto the keyed series (calibration D §D5 measured the server-wide mean
/// to be blind to a per-statement step). This file is the SQL they stand on, pinned by
/// <c>PgBaselineProviderKeyedTests</c> and the two-armed census in <c>LocalClockBucketKeyTests</c>.
///
/// <para><b>Both arms read the collector's STORED deltas</b> (<c>delta_total_exec_time_ms</c>, <c>delta_calls</c>)
/// and never re-difference the cumulative columns — lane 7's discipline (<c>PgTargetFactCollector.Queries.cs</c>), so
/// the series a statement is judged against is built from the same numbers the <c>PG_BAD_ACTOR_&lt;queryid&gt;</c>
/// fact states. Both are one row per <c>collection_time</c>: a collection is the unit the bucket counts as a sample,
/// exactly as lane 27's server-wide arm counts it, so a statement's bucket and the server's bucket over the same
/// window have the same sample count and the same days.</para>
///
/// <para><b>The key is <c>$7</c>, cast, after the six the base always binds.</b> The base binds the key as text (the
/// seam is dimension-neutral); <c>queryid</c> is <c>bigint</c>, so each arm casts the parameter once
/// (<c>$7::BIGINT</c>) rather than casting the column on every row (<c>queryid::text = $7</c> would run the cast
/// over tens of millions of rows for no index gain — <c>idx_pg_statement_stats_time</c> is (server_id,
/// collection_time), so the statement predicate is a heap filter either way). A key that is not a number fails the
/// cast at execution and lands in the base's one classified catch as "no baseline this pass" — the same posture as
/// any other failed compute, never a wrong bucket.</para>
///
/// <para><b>Cardinality.</b> One entry and one 30-day scan per (server, queryid) per cache period. The consumer bounds
/// the population (the base's cardinality note says how); these arms bound nothing themselves, because they cannot
/// know which statements matter.</para>
/// </summary>
public sealed partial class PgTargetBaselineProvider
{
    /// <summary>
    /// <c>pg_statement_share</c>, keyed: the statement's share of the collection's TOTAL execution time, per
    /// collection — Σ <c>delta_total_exec_time_ms</c> for the keyed <c>queryid</c> (a <c>FILTER</c> over the same
    /// rows, so the numerator and denominator are read once) over Σ for every statement row stamped with that
    /// <c>collection_time</c>, a fraction in [0, 1]. This is the decision variable the <c>PG_BAD_ACTOR</c> fact
    /// carries (<c>share_of_window_time</c>) taken per collection instead of per window, so the bucket's mean is the
    /// hour-of-week share this statement USUALLY takes and a window share far above it is a deviation from the
    /// statement's own habit — a top statement at its usual 40% is the workload, not a bad actor; a 40% statement that
    /// usually takes 4% is the story (Erik's ruling: own-baseline deviation, with the absolute share as context).
    ///
    /// <para>A collection whose statements ran nothing (<c>HAVING SUM(delta_total_exec_time_ms) &gt; 0</c>) is not
    /// a sample — a share of nothing is undefined, not zero — the same rule lane 27's mean applies to a collection
    /// with no calls. A collection where OTHER statements ran and this one did not IS a sample, at 0: the statement's
    /// habit includes the hours it is idle, and <c>coalesce(stmt_ms, 0)</c> says so (a NULL filtered sum would drop
    /// the row and inflate the statement's usual share). No unkeyed arm exists for this name: the server's share of
    /// its own total is 1.0 by construction.</para>
    /// </summary>
    private static partial string? StatementShareKeyedBaselineQuery() => @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_total_exec_time_ms)::DOUBLE PRECISION AS total_ms,
           CAST(SUM(delta_total_exec_time_ms) FILTER (WHERE queryid = $7::BIGINT) AS DOUBLE PRECISION) AS stmt_ms
    FROM pg_statement_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY collection_time
    HAVING SUM(delta_total_exec_time_ms) > 0
),
clean AS (
    SELECT collection_time, coalesce(stmt_ms, 0) / total_ms AS v
    FROM per_collection
)," + RobustTierScaffold;

    /// <summary>
    /// <c>pg_statement_mean_ms</c>, keyed: ONE statement's mean execution ms per call, per collection — Σ
    /// <c>delta_total_exec_time_ms</c> over Σ <c>delta_calls</c> across the keyed <c>queryid</c>'s rows stamped with one
    /// <c>collection_time</c> (several rows when the shape ran under more than one database, user or toplevel flag —
    /// the collector's full series identity — pooled to the <c>queryid</c> grain the fact family, the text store and
    /// <c>pg_plan_capture</c> share). Lane 27's UNKEYED arm of the same name (<c>PgTargetBaselineProvider.Plans.cs</c>)
    /// is the server-wide mean and stays exactly as it is — as the COLD FALLBACK, since lane 39; this is the
    /// per-statement series it said it could not have, and <c>ANOMALY_PG_PLAN_REGRESSION</c> now says "THIS statement
    /// got slower per call than its own hour usually sees" instead of "this server's statements did" whenever one
    /// flipped statement has a trustworthy bucket here. A collection in which the statement made no
    /// calls is not a sample (<c>HAVING SUM(delta_calls) &gt; 0</c>) — the same rule as the server-wide arm, applied
    /// to one statement, so an idle hour contributes nothing rather than a zero that would drag the bucket down.
    /// </summary>
    private static partial string? StatementMeanMsKeyedBaselineQuery() => @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_total_exec_time_ms)::DOUBLE PRECISION / SUM(delta_calls) AS mean_ms
    FROM pg_statement_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   queryid = $7::BIGINT
    GROUP BY collection_time
    HAVING SUM(delta_calls) > 0
),
clean AS (
    SELECT collection_time, mean_ms AS v
    FROM per_collection
)," + RobustTierScaffold;
}
