/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// The statement family's KEYED baseline arms (#3691 lane 33): one hour×day-of-week series per (server, metric,
/// <c>queryid</c>), reached through <see cref="PgBaselineProvider.GetBaselinesAsync"/> for a detector's whole candidate
/// set (#3901), or one <c>queryid</c> through the five-argument
/// <see cref="PgBaselineProvider.GetBaselineAsync(int, string, string?, DateTime, System.Threading.CancellationToken)"/>,
/// the key as text. The seam exists for Erik's 2026-09-20 ruling: the bad-actor share is graded as deviation
/// from the statement's OWN hour-of-week share baseline, which one series per (server, metric) cannot express.
/// Both arms are consumed now: the share by lane 34's <c>ANOMALY_PG_BAD_ACTOR_SHARE</c>, the per-call mean by lane 39's
/// switch of <c>ANOMALY_PG_PLAN_REGRESSION</c> onto the keyed series (calibration D §D5 measured the server-wide mean
/// to be blind to a per-statement step). This file is the SQL they stand on, pinned by
/// <c>PgBaselineProviderKeyedTests</c>, the two-armed census in <c>LocalClockBucketKeyTests</c> and, against the
/// per-statement arms it replaced, <c>PgStatementKeyedSetLiveTests</c>.
///
/// <para><b>Both arms read the collector's STORED deltas</b> (<c>delta_total_exec_time_ms</c>, <c>delta_calls</c>)
/// and never re-difference the cumulative columns — lane 7's discipline (<c>PgTargetFactCollector.Queries.cs</c>), so
/// the series a statement is judged against is built from the same numbers the <c>PG_BAD_ACTOR_&lt;queryid&gt;</c>
/// fact states. Both are one row per <c>collection_time</c>: a collection is the unit the bucket counts as a sample,
/// exactly as lane 27's server-wide arm counts it, so a statement's bucket and the server's bucket over the same
/// window have the same sample count and the same days.</para>
///
/// <para><b>One read of <c>pg_statement_stats</c> per metric, not per statement (#3901).</b> The keys are <c>$7</c>, a
/// <c>text[]</c> of up to <see cref="PgBaselineProvider.KeyedSetWidth"/> statements, after the six the base always
/// binds. Until #3901 each arm took one <c>queryid</c> and the two detectors asked for their candidates one at a time,
/// so every statement re-read the server's whole 30-day slice — <c>idx_pg_statement_stats_time</c> is (server_id,
/// collection_time) and the statement predicate is a heap filter — and the share arm recomputed the identical
/// per-collection total inside each read: on the production PostgreSQL store (50 Aurora clusters,
/// <c>pg_statement_stats</c> ~72 % of it) that was ~70 % of a cold <c>analyze_server</c>. Now each arm makes the
/// per-statement arm's own read — one <c>GROUP BY collection_time</c> over the slice, the plan that stays parallel and
/// streaming — with one <c>FILTER</c> per member SLOT where it had one for its single key (<see cref="MemberSlots"/>),
/// and hands every member's column of that one result to the one robust scaffold through
/// <see cref="PgBaselineProvider.PerMemberScaffold"/>. Each slot's aggregate is the per-statement arm's aggregate over
/// the same rows, so a member's samples are the samples its own read took, and its buckets come from the same scaffold
/// text. (Measured, not assumed: grouping by <c>(collection_time, queryid)</c> instead forfeits the parallel streaming
/// aggregate for a serial hash one — on 4.3M compressed rows it was no faster than the five reads it replaced.) The keys
/// are cast from the array (<c>($7::BIGINT[])[slot]</c>, folded to constants at planning), never the column —
/// <c>queryid::text</c> would cast tens of millions of rows. A key that is not a number fails that cast and lands in
/// the base's one classified catch as "no baseline this pass" for the set; the detectors only ever pass a <c>long</c>
/// rendered invariant, so no set of theirs meets one.</para>
///
/// <para><b>Cardinality.</b> One cache entry per (server, queryid) per cache period, and one 30-day read per (server,
/// metric) per compute of up to <see cref="PgBaselineProvider.KeyedSetWidth"/> statements. The consumer bounds the
/// population (the base's cardinality note says how); these arms bound nothing themselves, because they cannot know
/// which statements matter.</para>
/// </summary>
public sealed partial class PgTargetBaselineProvider
{
    /// <summary>
    /// The statement arms' member set (#3901): one row per <c>$7</c> key, numbered by its 1-based position — the
    /// <c>member</c> <see cref="PgBaselineProvider.PerMemberScaffold"/> returns, the provider maps back to its key, and
    /// the arm uses as the key's slot in the arrays <see cref="MemberSlots"/> builds.
    /// </summary>
    private const string StatementMembers = @"
WITH members AS (
    SELECT n::INT AS member
    FROM unnest($7::TEXT[]) WITH ORDINALITY AS k(member_key, n)
),";

    /// <summary>
    /// One aggregate per member SLOT (#3901), <c>1..</c><see cref="PgBaselineProvider.KeyedSetWidth"/>, as the elements
    /// of an <c>ARRAY[…]</c>: slot <c>i</c> is <paramref name="aggregate"/> over the predicate naming the statement
    /// <c>($7::BIGINT[])[i]</c>.
    /// A set narrower than the width leaves its last slots on a NULL key, whose <c>FILTER</c> matches no row and which no
    /// member reads. Built, not typed out, so the arms cannot carry a different number of slots than the provider
    /// packs keys into.
    /// </summary>
    private static string MemberSlots(Func<string, string> aggregate)
        => string.Join(",\r\n               ", Enumerable.Range(1, KeyedSetWidth).Select(i => aggregate($"queryid = ($7::BIGINT[])[{i}]")));

    /// <summary>
    /// <c>pg_statement_share</c>, keyed: the statement's share of the collection's TOTAL execution time, per
    /// collection — Σ <c>delta_total_exec_time_ms</c> for the keyed <c>queryid</c> (a <c>FILTER</c> over the same
    /// rows, so the numerator and denominator are read once) over Σ for every statement row stamped with that
    /// <c>collection_time</c>, a fraction in [0, 1]. This is the decision variable the <c>PG_BAD_ACTOR</c> fact
    /// carries (<c>share_of_window_time</c>) taken per collection instead of per window, so the bucket's mean is the
    /// hour-of-week share this statement USUALLY takes and a window share far above it is a deviation from the
    /// statement's own habit — a top statement at its usual 40% is the workload, not a bad actor; a 40% statement that
    /// usually takes 4% is the story (Erik's ruling: own-baseline deviation, with the absolute share as context).
    /// Since #3901 the numerator is one <c>FILTER</c> per member slot beside the ONE total, so the total is summed once
    /// for the set instead of once per statement.
    ///
    /// <para>A collection whose statements ran nothing (<c>HAVING SUM(delta_total_exec_time_ms) &gt; 0</c>) is not
    /// a sample — a share of nothing is undefined, not zero — the same rule lane 27's mean applies to a collection
    /// with no calls. A collection where OTHER statements ran and this one did not IS a sample, at 0: the statement's
    /// habit includes the hours it is idle, and <c>coalesce(…, 0)</c> says so (a NULL filtered sum would drop
    /// the row and inflate the statement's usual share). No unkeyed arm exists for this name: the server's share of
    /// its own total is 1.0 by construction.</para>
    /// </summary>
    private static partial string? StatementShareKeyedBaselineQuery() => StatementMembers + @"
per_collection AS MATERIALIZED (
    SELECT collection_time,
           SUM(delta_total_exec_time_ms)::DOUBLE PRECISION AS total_ms,
           ARRAY[
               " + MemberSlots(isMember => $"CAST(SUM(delta_total_exec_time_ms) FILTER (WHERE {isMember}) AS DOUBLE PRECISION)") + @"
           ] AS stmt_ms
    FROM pg_statement_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY collection_time
    HAVING SUM(delta_total_exec_time_ms) > 0
)" + PerMemberScaffold(@"
    SELECT collection_time, coalesce(stmt_ms[mem.member], 0) / total_ms AS v
    FROM per_collection");

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
    /// calls is not a sample (<c>SUM(delta_calls) &gt; 0</c>) — the same rule as the server-wide arm, applied
    /// to one statement, so an idle hour contributes nothing rather than a zero that would drag the bucket down.
    ///
    /// <para><b>The rule, per slot (#3901).</b> One read of the set's rows yields every member's per-collection sums,
    /// so the per-statement arm's <c>HAVING SUM(delta_calls) &gt; 0</c> is applied per member instead: its calls sum
    /// rides beside its mean (<c>calls</c>) and a member's <c>clean</c> keeps a collection only where that sum is
    /// positive — which is also why each mean is guarded by the same test (a member with no calls in a collection
    /// another member ran in would otherwise divide by zero).</para>
    /// </summary>
    private static partial string? StatementMeanMsKeyedBaselineQuery() => StatementMembers + @"
per_collection AS MATERIALIZED (
    SELECT collection_time,
           ARRAY[
               " + MemberSlots(isMember => $"CASE WHEN SUM(delta_calls) FILTER (WHERE {isMember}) > 0 THEN CAST(SUM(delta_total_exec_time_ms) FILTER (WHERE {isMember}) AS DOUBLE PRECISION) / SUM(delta_calls) FILTER (WHERE {isMember}) END") + @"
           ] AS mean_ms,
           ARRAY[
               " + MemberSlots(isMember => $"SUM(delta_calls) FILTER (WHERE {isMember})") + @"
           ] AS calls
    FROM pg_statement_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   queryid = ANY($7::BIGINT[])
    GROUP BY collection_time
)" + PerMemberScaffold(@"
    SELECT collection_time, mean_ms[mem.member] AS v
    FROM per_collection
    WHERE calls[mem.member] > 0");
}
