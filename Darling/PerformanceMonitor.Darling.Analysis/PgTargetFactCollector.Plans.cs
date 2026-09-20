/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// How many flipped statements the plan reads return, at most — a PAGE SIZE, not a threshold: it bounds the
    /// rows one pass sifts for the one <c>PG_PLAN_REGRESSION</c> / <c>PG_PARAMETER_SENSITIVITY</c> fact each key
    /// carries (the keys are static, so a pass emits one of each — the worst — and counts the rest in metadata).
    /// Fifty flipped statements in one window is already a server re-planning wholesale, which the count says.
    /// </summary>
    internal const int PlanFlipCandidateCount = 50;

    /// <summary>
    /// How far before the window's START the state reads look for their latest row: <c>pg_plan_capture_readiness</c>
    /// (hourly), <c>pg_extension_availability</c> and <c>pg_column_stats</c> (daily) and <c>pg_predicate_stats</c>
    /// (hourly) describe a STATE — is the library loaded, is the extension installed, how skewed is this column, which
    /// columns does this statement filter on — not a rate over the window, so a four-hour window may hold no row of
    /// theirs at all and the latest row before it is the honest answer. Two days is two runs of the daily collectors:
    /// one missed run survives, a collector silent for longer reads as "unknown" and the family says nothing rather
    /// than reading last month's state as today's. A STALENESS horizon for a state read, not a bar.
    /// </summary>
    internal const int PlanStateLookbackDays = 2;

    /// <summary>
    /// The two trackedness facets this family needs, as ONE row: whether <c>auto_explain</c> is loaded (the
    /// <c>library_loaded</c> facet of <c>pg_plan_capture_readiness</c> — the one precondition without which no plan is
    /// ever written to the log; the other facets refine a capture that can happen at all) and the state of
    /// <c>pg_qualstats</c> in <c>pg_extension_availability</c> (installed in ANY database counts — the extension is
    /// per-database and the predicate rows carry their database). Each column is NULL when the collector has no row
    /// inside <see cref="PlanStateLookbackDays"/> — "unknown", which the caller treats as silence, never as "off".
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the lookback days.
    /// </summary>
    public const string PgTargetPlanTrackednessSql = @"
WITH readiness AS (
    SELECT is_satisfied
    FROM pg_plan_capture_readiness
    WHERE server_id = $1
    AND   collection_time <= $3
    AND   collection_time >= $2 - make_interval(days => $4)
    AND   facet = 'library_loaded'
    ORDER BY collection_time DESC
    LIMIT 1
),
qualstats AS (
    SELECT state
    FROM pg_extension_availability
    WHERE server_id = $1
    AND   collection_time <= $3
    AND   collection_time >= $2 - make_interval(days => $4)
    AND   extension_name = 'pg_qualstats'
    ORDER BY (state IN ('installed', 'outdated')) DESC, collection_time DESC
    LIMIT 1
)
SELECT (SELECT is_satisfied FROM readiness) AS auto_explain_loaded,
       (SELECT state FROM qualstats)        AS pg_qualstats_state";

    /// <summary>
    /// Every statement captured under at least two distinct <c>plan_hash</c> values in the window, with its stored
    /// <c>pg_statement_stats</c> deltas split at the flip — the read behind <c>PG_PLAN_REGRESSION</c>. <c>$1</c>
    /// server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the row cap (<see cref="PlanFlipCandidateCount"/>).
    ///
    /// <para><b>The plan the window opened with against the plan it closed with.</b> Per (<c>query_id</c>,
    /// <c>plan_hash</c>) the captures collapse to a first-seen time; ordered by first-seen, the FIRST hash is
    /// "before" and the LAST is "after". <c>pg_statement_stats</c> is then read for the same statements over the same
    /// window and its STORED deltas (<c>delta_calls</c>, <c>delta_total_exec_time_ms</c> — the collector's, never a
    /// re-differencing of the cumulative columns; <c>PgTargetTopStatementsSql</c>'s discipline, same reasons) are
    /// summed on each side: rows stamped before the SECOND hash first appeared are "before" (the opening plan's
    /// period, pure), rows stamped at or after the LAST hash first appeared are "after" (the closing plan's period,
    /// pure). With two hashes the two moments coincide and the split is the flip; with three or more, the middle
    /// plans' minutes belong to neither side — counted (<c>hash_count</c>), never averaged into a plan they were not
    /// run under. A statement bouncing between plans is <c>PG_PARAMETER_SENSITIVITY</c>'s shape, read by
    /// <see cref="PgTargetPlanSensitivitySql"/> from the same captures. The one snapshot whose interval straddles a
    /// moment lands on the later side whole — stated, not corrected: the interval is a minute and the sides are
    /// hours, and pulling the straddle apart would need the interval the row may not carry (pre-V128).</para>
    ///
    /// <para><b>The hashes travel as their leading 48 bits.</b> <c>plan_hash</c> is 32 hex characters of a SHA-256
    /// (<c>PgPlanLogParser.Hash</c>) and <c>Fact.Metadata</c> is doubles-only; the first 12 characters are 48 bits,
    /// exact in a double, and a prefix a reader can match against <c>get_pg_plans</c>' full hashes. A hash that is
    /// not 12 hex characters (a planted or foreign row) yields NULL rather than a cast error, and the advice omits
    /// the clause.</para>
    ///
    /// <para><b>Orphans are excluded.</b> <c>query_id = 0</c> is a plan whose <c>log_line_prefix</c> carried no
    /// <c>%Q</c> — the collector's documented orphan — and joins no statement; it is dropped here rather than
    /// becoming a statement with a hash count.</para>
    /// </summary>
    public const string PgTargetPlanFlipSql = @"
WITH captures AS (
    SELECT query_id,
           plan_hash,
           MIN(collection_time) AS first_seen,
           COUNT(*)             AS captures,
           MAX(node_count)      AS node_count,
           MIN(top_node_type)   AS top_node_type,
           CASE WHEN plan_hash ~ '^[0-9A-Fa-f]{12}'
                THEN ('x' || left(plan_hash, 12))::bit(48)::bigint
           END                  AS hash_prefix
    FROM pg_plan_capture
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   query_id IS NOT NULL
    AND   query_id <> 0
    AND   plan_hash IS NOT NULL
    GROUP BY query_id, plan_hash
),
ordered AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY first_seen, plan_hash) AS rn,
           COUNT(*)     OVER (PARTITION BY query_id)                                AS hash_count
    FROM captures
),
flipped AS (
    SELECT f.query_id,
           f.hash_count,
           f.hash_prefix   AS hash_before,
           l.hash_prefix   AS hash_after,
           f.top_node_type AS top_node_before,
           l.top_node_type AS top_node_after,
           f.node_count    AS nodes_before,
           l.node_count    AS nodes_after,
           n.first_seen    AS first_flip_time,
           l.first_seen    AS flip_time
    FROM ordered AS f
    JOIN ordered AS n
      ON n.query_id = f.query_id
     AND n.rn = 2
    JOIN ordered AS l
      ON l.query_id = f.query_id
     AND l.rn = f.hash_count
    WHERE f.rn = 1
    AND   f.hash_count >= 2
),
sides AS (
    SELECT fl.query_id,
           SUM(s.delta_calls)              FILTER (WHERE s.collection_time <  fl.first_flip_time) AS calls_before,
           SUM(s.delta_total_exec_time_ms) FILTER (WHERE s.collection_time <  fl.first_flip_time) AS ms_before,
           SUM(s.delta_calls)              FILTER (WHERE s.collection_time >= fl.flip_time)       AS calls_after,
           SUM(s.delta_total_exec_time_ms) FILTER (WHERE s.collection_time >= fl.flip_time)       AS ms_after
    FROM flipped AS fl
    JOIN pg_statement_stats AS s
      ON s.queryid = fl.query_id
    WHERE s.server_id = $1
    AND   s.collection_time >= $2
    AND   s.collection_time <= $3
    GROUP BY fl.query_id
)
SELECT fl.query_id,
       fl.hash_count,
       fl.hash_before,
       fl.hash_after,
       fl.flip_time,
       fl.first_flip_time,
       (fl.top_node_before IS DISTINCT FROM fl.top_node_after) AS top_node_changed,
       fl.nodes_before,
       fl.nodes_after,
       CAST(COALESCE(sd.calls_before, 0) AS bigint) AS calls_before,
       CAST(COALESCE(sd.ms_before, 0) AS bigint)    AS ms_before,
       CAST(COALESCE(sd.calls_after, 0) AS bigint)  AS calls_after,
       CAST(COALESCE(sd.ms_after, 0) AS bigint)     AS ms_after,
       COUNT(*) OVER ()                             AS flipped_statements
FROM flipped AS fl
LEFT JOIN sides AS sd
  ON sd.query_id = fl.query_id
ORDER BY fl.query_id
LIMIT $4";

    /// <summary>
    /// Every statement captured under at least <c>$6</c> distinct <c>plan_hash</c> values in the window, with the most
    /// skewed of the predicate columns <c>pg_qualstats</c> recorded for it — the read behind
    /// <c>PG_PARAMETER_SENSITIVITY</c>. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the
    /// state lookback days (<see cref="PlanStateLookbackDays"/>), <c>$5</c> the row cap
    /// (<see cref="PlanFlipCandidateCount"/>), <c>$6</c> the hash floor
    /// (<see cref="PgTargetScorer.ParameterSensitivityMinHashes"/>), <c>$7</c> the skew bar
    /// (<see cref="PgTargetScorer.ParameterSensitivitySkewFrequency"/>) — both bars are the scorer's constants passed
    /// through, so the read and the grade cannot disagree about where the line is.
    ///
    /// <para><b>Predicates are joined to columns by NAME, per database.</b> <c>pg_predicate_stats</c> is per database
    /// (its OIDs mean nothing outside it — V98) and so is <c>pg_column_stats</c> (<c>pg_stats</c> describes the
    /// connected database — V91), so the join is on (<c>database_name</c>, <c>schema_name</c>, <c>table_name</c>,
    /// <c>column_name</c>), the latest column-stats row per column inside the lookback. A predicate whose column has
    /// no stats row (a table the monitoring role cannot read — V91's <c>has_column_privilege</c> filter) is a
    /// predicate with an unknown frequency, counted but never skewed.</para>
    ///
    /// <para><b>One row per statement, the skewed column's figures on it.</b> <c>predicate_columns</c> and
    /// <c>skewed_columns</c> are the statement's counts (window aggregates over the joined rows); the frequency,
    /// <c>n_distinct</c>, database and worst estimate error are the most-skewed column's. A statement with hashes
    /// and no predicate row at all returns with <c>predicate_columns = 0</c> and NULL figures, which the caller
    /// reads against the trackedness row: <c>pg_qualstats</c> absent, or installed and never sampled this statement
    /// (its default <c>sample_rate</c> is <c>1 / max_connections</c> — V98's own warning).</para>
    /// </summary>
    public const string PgTargetPlanSensitivitySql = @"
WITH hashes AS (
    SELECT query_id,
           COUNT(DISTINCT plan_hash) AS hash_count
    FROM pg_plan_capture
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   query_id IS NOT NULL
    AND   query_id <> 0
    AND   plan_hash IS NOT NULL
    GROUP BY query_id
    HAVING COUNT(DISTINCT plan_hash) >= $6
),
predicates AS (
    SELECT p.query_id,
           p.database_name,
           p.schema_name,
           p.table_name,
           p.column_name,
           MAX(p.worst_estimate_error_ratio) AS worst_estimate_error_ratio
    FROM pg_predicate_stats AS p
    JOIN hashes AS h
      ON h.query_id = p.query_id
    WHERE p.server_id = $1
    AND   p.collection_time <= $3
    AND   p.collection_time >= $2 - make_interval(days => $4)
    AND   p.column_name IS NOT NULL
    GROUP BY p.query_id, p.database_name, p.schema_name, p.table_name, p.column_name
),
columns AS (
    SELECT DISTINCT ON (database_name, schema_name, table_name, column_name)
           database_name,
           schema_name,
           table_name,
           column_name,
           top_value_frequency,
           n_distinct
    FROM pg_column_stats
    WHERE server_id = $1
    AND   collection_time <= $3
    AND   collection_time >= $2 - make_interval(days => $4)
    AND   column_name IS NOT NULL
    ORDER BY database_name, schema_name, table_name, column_name, collection_time DESC
),
joined AS (
    SELECT pr.query_id,
           pr.database_name,
           pr.worst_estimate_error_ratio,
           c.top_value_frequency,
           c.n_distinct
    FROM predicates AS pr
    LEFT JOIN columns AS c
      ON c.database_name = pr.database_name
     AND c.schema_name   = pr.schema_name
     AND c.table_name    = pr.table_name
     AND c.column_name   = pr.column_name
),
picked AS (
    SELECT DISTINCT ON (h.query_id)
           h.query_id,
           h.hash_count,
           COUNT(j.query_id) OVER (PARTITION BY h.query_id)                                            AS predicate_columns,
           COUNT(j.query_id) FILTER (WHERE j.top_value_frequency >= $7) OVER (PARTITION BY h.query_id) AS skewed_columns,
           j.database_name,
           j.top_value_frequency,
           j.n_distinct,
           j.worst_estimate_error_ratio
    FROM hashes AS h
    LEFT JOIN joined AS j
      ON j.query_id = h.query_id
    ORDER BY h.query_id, j.top_value_frequency DESC NULLS LAST
)
SELECT query_id,
       hash_count,
       predicate_columns,
       skewed_columns,
       database_name,
       top_value_frequency,
       n_distinct,
       worst_estimate_error_ratio
FROM picked
ORDER BY (top_value_frequency IS NULL), top_value_frequency DESC, hash_count DESC, query_id
LIMIT $5";

    /// <summary>
    /// <c>PG_PLAN_REGRESSION</c> from the window's <c>pg_statement_stats</c> (the STORED per-statement deltas — copy of
    /// <c>PgTargetFactCollector.Queries.cs</c>'s discipline, never a re-differencing) split at the moment
    /// <c>pg_plan_capture</c> first saw the statement's last <c>plan_hash</c>; <c>PG_PARAMETER_SENSITIVITY</c> from
    /// <c>plan_hash</c> variance per <c>query_id</c> beside <c>pg_column_stats</c>' <c>top_value_frequency</c> on the
    /// predicate columns <c>pg_predicate_stats</c> names; and (lane 30) <c>PG_SEQ_SCAN_ADVISORY</c> from <c>plan_json</c>
    /// Seq Scan nodes beside <c>pg_predicate_stats</c>. The statement travels through the <c>ObjectName</c> seam as its
    /// <c>query_id</c> (<c>Fact.Metadata</c> is doubles-only and a 64-bit id is exact in one only to 2^53 — lane 7's
    /// reason); a fact naming an index candidate carries evidence, never DDL (D8).
    ///
    /// <para><b>One fact per key, the worst of the window.</b> Both keys are static (the reconciler, the occurrence
    /// history and the graph key on them), so a pass emits the ONE regression that cleared the scorer's bars by the
    /// largest ratio — the bars are <see cref="PgTargetScorer"/>'s constants, referenced here for the pick and applied
    /// again there for the grade, so the collector and the scorer cannot disagree — and, when none cleared them, the
    /// largest-ratio flip that had enough calls on both sides (the scorer grades it 0 and the fact still shows the
    /// flip). The count of flipped statements rides along. Same for sensitivity: the most-skewed statement with a
    /// skewed column; when statements flipped three ways or more but no predicate could be read, ONE
    /// <c>unavailable</c> fact with the reason (<c>pg_qualstats</c> absent, or installed and never sampled).</para>
    ///
    /// <para><b>Absence of a capture is "not slow enough to log", never "no plan".</b> <c>auto_explain</c> writes only
    /// executions over <c>auto_explain.log_min_duration</c>, so a window with no captured plan says nothing about plans
    /// and this method emits nothing — except when the readiness collector says the library is NOT loaded, when the
    /// honest fact is <c>PG_PLAN_REGRESSION</c> in its <c>unavailable</c> shape with <c>reason_auto_explain_off</c>
    /// (the write family's shape for a counter the engine does not report). Unknown readiness (no row in the
    /// lookback) is silence, not "off".</para>
    ///
    /// <para><b>No fact without an observed window.</b> <c>ObservedDurationMs</c> of 0 means the coverage witness
    /// stamped nothing (#3538 A2); the pass reports <c>unavailable</c> anyway and this method contributes nothing.</para>
    ///
    /// <para>/* filled by lane 27 (lane 30 adds the Seq-Scan read in the same partial — the marked region at the end
    /// of this method). Every command sets <c>CommandTimeout = FactCommandTimeoutSeconds</c>, every store call passes
    /// <c>context.CancellationToken</c>, <c>$N</c> positional, no bare <c>now()</c> / <c>CURRENT_TIMESTAMP</c>
    /// (StoreSqlClockDisciplineTests), the catch is the <c>when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> shape that calls <see cref="ReportCollectionFailure"/>, and no rate is taken
    /// here at all — the family's quantities are per-call means and counts, which need no window divisor. The six
    /// tables are CollectorCatalog targets already, so the FROM/JOIN census admits them (pinned by name in
    /// PgTargetFactCollectorTests). */</para>
    /// </summary>
    private async partial Task CollectPlanFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0)
            return;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            var (autoExplainLoaded, qualstatsState) = await ReadPlanTrackednessAsync(context, connection);
            var flips = await ReadPlanFlipsAsync(context, connection);

            /* The regression: the worst flip that cleared the bars, else the worst flip with enough calls to be
               compared (graded 0, shown), else — when the library is KNOWN off — the unavailable shape. */
            var regression = PickRegression(flips);
            if (regression is { } pick)
            {
                facts.Add(RegressionFact(context, pick, flips.Count == 0 ? 0 : flips[0].FlippedStatements));
            }
            else if (flips.Count == 0 && autoExplainLoaded == false)
            {
                facts.Add(new Fact
                {
                    Source = PgTargetSources.PlansSource,
                    Key = PgTargetFactKeys.PlanRegression,
                    Value = 0,
                    ServerId = context.ServerId,
                    Metadata =
                    {
                        [PgTargetScorer.PlanUnavailableKey] = 1,
                        [PgTargetScorer.PlanReasonAutoExplainOffKey] = 1,
                        /* 1, not 0: no bar was chosen for this fact — nothing is graded off the unavailable shape — so
                           there is no unmeasured number for the flag to disclose (the write family's rule). */
                        ["threshold_lineage"] = 1,
                    },
                });
            }

            /* The sensitivity: read only when something flipped — a statement with one plan has no variance to explain. */
            if (flips.Count > 0)
            {
                var sensitivity = await ReadPlanSensitivityAsync(context, connection);
                var qualstatsAbsent = qualstatsState is "absent" or "available";
                if (PickSensitivity(sensitivity, qualstatsAbsent, context.ServerId) is { } fact)
                    facts.Add(fact);
            }

            /* lane 30: Seq-Scan read — PG_SEQ_SCAN_ADVISORY from plan_json Seq Scan nodes beside pg_predicate_stats,
               on this connection, after the two facts above so it can read them by key. Evidence only, never DDL (D8). */
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_plan_capture arrived in V99, pg_predicate_stats in V98, pg_column_stats in V91, the readiness table in
               V87; a store without any of them raises 42P01 here, which the reporter classifies quiet. Degrades to "no
               facts" so one unavailable input cannot cost this server its other facts, and WHY is reported, not
               assumed (#2826). An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>One flipped statement, as <see cref="PgTargetPlanFlipSql"/> returns it.</summary>
    internal sealed record PlanFlipRow(
        long QueryId, long HashCount, long? HashBefore, long? HashAfter, DateTime FlipTime, DateTime FirstFlipTime, bool TopNodeChanged,
        long? NodesBefore, long? NodesAfter, long CallsBefore, long MsBefore, long CallsAfter, long MsAfter, long FlippedStatements)
    {
        /// <summary>The per-call means, absent (null) over no calls — a mean over no calls is not 0 ms.</summary>
        public double? MeanBefore => CallsBefore > 0 ? (double)MsBefore / CallsBefore : null;
        public double? MeanAfter => CallsAfter > 0 ? (double)MsAfter / CallsAfter : null;
        public double? Ratio => MeanBefore is { } b && b > 0 && MeanAfter is { } a ? a / b : null;
    }

    /// <summary>One multi-plan statement, as <see cref="PgTargetPlanSensitivitySql"/> returns it.</summary>
    internal sealed record PlanSensitivityRow(
        long QueryId, long HashCount, long PredicateColumns, long SkewedColumns, string? DatabaseName,
        double? TopValueFrequency, double? NDistinct, double? WorstEstimateErrorRatio);

    /// <summary>
    /// The regression to emit, or null: the largest-ratio flip that clears every scorer bar (both call floors, the
    /// absolute delta, the concerning ratio) — else the largest-ratio flip with enough calls on both sides, which the
    /// scorer grades 0 and the fact still shows — else nothing. The bars are <see cref="PgTargetScorer"/>'s, referenced.
    /// Pure, so the pick is testable without a store.
    /// </summary>
    internal static PlanFlipRow? PickRegression(IReadOnlyList<PlanFlipRow> flips)
    {
        PlanFlipRow? cleared = null;
        PlanFlipRow? comparable = null;
        foreach (var flip in flips)
        {
            if (flip.CallsBefore < PgTargetScorer.PlanRegressionMinCallsPerSide || flip.CallsAfter < PgTargetScorer.PlanRegressionMinCallsPerSide)
                continue;
            if (flip.Ratio is not { } ratio || flip.MeanBefore is not { } before || flip.MeanAfter is not { } after)
                continue;

            if (comparable is null || ratio > comparable.Ratio)
                comparable = flip;

            if (after - before >= PgTargetScorer.PlanRegressionMinDeltaMs && ratio >= PgTargetScorer.PlanRegressionRatioConcerning
                && (cleared is null || ratio > cleared.Ratio))
                cleared = flip;
        }

        return cleared ?? comparable;
    }

    /// <summary>
    /// The sensitivity fact to emit, or null: the most-skewed statement whose skewed column clears the bar — the rows
    /// arrive most-skewed first, so the first row with a skewed column is it — else, when statements flipped but NO
    /// predicate row exists for any of them, one <c>unavailable</c> fact on the most-flipped statement with the reason
    /// (<paramref name="qualstatsAbsent"/> → the extension is not installed; otherwise installed and never sampled) —
    /// else nothing (predicates were read and none is skewed: variance without the mechanism's evidence is not a card).
    /// Pure.
    /// </summary>
    internal static Fact? PickSensitivity(IReadOnlyList<PlanSensitivityRow> rows, bool qualstatsAbsent, int serverId)
    {
        if (rows.Count == 0)
            return null;

        PlanSensitivityRow? mostFlipped = null;
        var anyPredicate = false;
        foreach (var row in rows)
        {
            anyPredicate |= row.PredicateColumns > 0;
            if (mostFlipped is null || row.HashCount > mostFlipped.HashCount)
                mostFlipped = row;

            if (row.SkewedColumns > 0 && row.TopValueFrequency is { } frequency && frequency >= PgTargetScorer.ParameterSensitivitySkewFrequency)
            {
                var fact = new Fact
                {
                    Source = PgTargetSources.PlansSource,
                    Key = PgTargetFactKeys.ParameterSensitivity,
                    Value = frequency,
                    ServerId = serverId,
                    ObjectName = row.QueryId.ToString(CultureInfo.InvariantCulture),
                    DatabaseName = row.DatabaseName,
                    Metadata =
                    {
                        [PgTargetScorer.PlanHashCountKey] = row.HashCount,
                        [PgTargetScorer.PlanTopValueFrequencyKey] = frequency,
                        ["predicate_columns"] = row.PredicateColumns,
                        ["skewed_columns"] = row.SkewedColumns,
                        ["sensitive_statements"] = rows.Count,
                    },
                };
                if (row.NDistinct is { } n) fact.Metadata["n_distinct"] = n;
                if (row.WorstEstimateErrorRatio is { } e) fact.Metadata["worst_estimate_error_ratio"] = e;
                return fact;
            }
        }

        if (anyPredicate)
            return null;

        return new Fact
        {
            Source = PgTargetSources.PlansSource,
            Key = PgTargetFactKeys.ParameterSensitivity,
            Value = 0,
            ServerId = serverId,
            ObjectName = mostFlipped!.QueryId.ToString(CultureInfo.InvariantCulture),
            Metadata =
            {
                [PgTargetScorer.PlanHashCountKey] = mostFlipped.HashCount,
                [PgTargetScorer.PlanUnavailableKey] = 1,
                [qualstatsAbsent ? PgTargetScorer.PlanReasonQualstatsAbsentKey : PgTargetScorer.PlanReasonNoPredicateSampledKey] = 1,
                ["sensitive_statements"] = rows.Count,
                /* 1, not 0: nothing is graded off the unavailable shape (the write family's rule). */
                ["threshold_lineage"] = 1,
            },
        };
    }

    /// <summary>The <c>PG_PLAN_REGRESSION</c> fact for a picked flip. <see cref="Fact.Value"/> is the after ÷ before
    /// ratio (the decision variable); the metadata carries both means, both call counts, the hash count and the two
    /// 48-bit hash prefixes, the node-count move and whether the top node changed, the ages of the first and the
    /// last plan change against the window's end, and how many statements flipped. <c>query_id</c> rides on <see cref="Fact.ObjectName"/>;
    /// <see cref="Fact.DatabaseName"/> stays null (the statement rolls up across databases, lane 7's grain).</summary>
    private static Fact RegressionFact(AnalysisContext context, PlanFlipRow flip, long flippedStatements)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.PlansSource,
            Key = PgTargetFactKeys.PlanRegression,
            Value = flip.Ratio ?? 0,
            ServerId = context.ServerId,
            ObjectName = flip.QueryId.ToString(CultureInfo.InvariantCulture),
            Metadata =
            {
                [PgTargetScorer.PlanMeanMsBeforeKey] = flip.MeanBefore ?? 0,
                [PgTargetScorer.PlanMeanMsAfterKey] = flip.MeanAfter ?? 0,
                ["mean_ms_delta"] = (flip.MeanAfter ?? 0) - (flip.MeanBefore ?? 0),
                [PgTargetScorer.PlanCallsBeforeKey] = flip.CallsBefore,
                [PgTargetScorer.PlanCallsAfterKey] = flip.CallsAfter,
                [PgTargetScorer.PlanHashCountKey] = flip.HashCount,
                ["top_node_changed"] = flip.TopNodeChanged ? 1 : 0,
                ["flip_age_s"] = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(flip.FlipTime)).TotalSeconds),
                ["first_flip_age_s"] = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(flip.FirstFlipTime)).TotalSeconds),
                ["flipped_statements"] = flippedStatements,
            },
        };

        /* Absent rather than zero: a hash the cast could not read is not hash 0; a node count the capture did not
           carry is not 0 nodes. */
        if (flip.HashBefore is { } hb) fact.Metadata["plan_hash_before_prefix"] = hb;
        if (flip.HashAfter is { } ha) fact.Metadata["plan_hash_after_prefix"] = ha;
        if (flip.NodesBefore is { } nb && flip.NodesAfter is { } na)
        {
            fact.Metadata["nodes_before"] = nb;
            fact.Metadata["nodes_after"] = na;
            fact.Metadata["node_count_delta"] = na - nb;
        }

        return fact;
    }

    private async Task<(bool? AutoExplainLoaded, string? QualstatsState)> ReadPlanTrackednessAsync(AnalysisContext context, NpgsqlConnection connection)
    {
        using var cmd = new NpgsqlCommand(PgTargetPlanTrackednessSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(PlanStateLookbackDays);

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken)) return (null, null);
        return (
            reader.IsDBNull(0) ? null : reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    private async Task<List<PlanFlipRow>> ReadPlanFlipsAsync(AnalysisContext context, NpgsqlConnection connection)
    {
        using var cmd = new NpgsqlCommand(PgTargetPlanFlipSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(PlanFlipCandidateCount);

        var rows = new List<PlanFlipRow>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            rows.Add(new PlanFlipRow(
                QueryId: ToInt64(reader.GetValue(0)),
                HashCount: ToInt64(reader.GetValue(1)),
                HashBefore: reader.IsDBNull(2) ? null : ToInt64(reader.GetValue(2)),
                HashAfter: reader.IsDBNull(3) ? null : ToInt64(reader.GetValue(3)),
                FlipTime: reader.GetDateTime(4),
                FirstFlipTime: reader.GetDateTime(5),
                TopNodeChanged: !reader.IsDBNull(6) && reader.GetBoolean(6),
                NodesBefore: reader.IsDBNull(7) ? null : ToInt64(reader.GetValue(7)),
                NodesAfter: reader.IsDBNull(8) ? null : ToInt64(reader.GetValue(8)),
                CallsBefore: ToInt64(reader.GetValue(9)),
                MsBefore: ToInt64(reader.GetValue(10)),
                CallsAfter: ToInt64(reader.GetValue(11)),
                MsAfter: ToInt64(reader.GetValue(12)),
                FlippedStatements: ToInt64(reader.GetValue(13))));
        }

        return rows;
    }

    private async Task<List<PlanSensitivityRow>> ReadPlanSensitivityAsync(AnalysisContext context, NpgsqlConnection connection)
    {
        using var cmd = new NpgsqlCommand(PgTargetPlanSensitivitySql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(PlanStateLookbackDays);
        cmd.Parameters.AddWithValue(PlanFlipCandidateCount);
        cmd.Parameters.AddWithValue((long)PgTargetScorer.ParameterSensitivityMinHashes);
        cmd.Parameters.AddWithValue(PgTargetScorer.ParameterSensitivitySkewFrequency);

        var rows = new List<PlanSensitivityRow>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            rows.Add(new PlanSensitivityRow(
                QueryId: ToInt64(reader.GetValue(0)),
                HashCount: ToInt64(reader.GetValue(1)),
                PredicateColumns: reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2)),
                SkewedColumns: reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                DatabaseName: reader.IsDBNull(4) ? null : reader.GetString(4),
                TopValueFrequency: reader.IsDBNull(5) ? null : Convert.ToDouble(reader.GetValue(5)),
                NDistinct: reader.IsDBNull(6) ? null : Convert.ToDouble(reader.GetValue(6)),
                WorstEstimateErrorRatio: reader.IsDBNull(7) ? null : Convert.ToDouble(reader.GetValue(7))));
        }

        return rows;
    }
}
