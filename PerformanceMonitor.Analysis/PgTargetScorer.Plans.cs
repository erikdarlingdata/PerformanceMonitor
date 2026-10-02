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

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_plans</c> — the plan family (filled by lane 27 of #3691, design §6; lane 30 adds the Seq-Scan advisory):
/// <c>PG_PLAN_REGRESSION</c> from a statement's per-call mean-ms step in <c>pg_statement_stats</c> coinciding
/// with a <c>plan_hash</c> flip in <c>pg_plan_capture</c>, <c>PG_PARAMETER_SENSITIVITY</c> from <c>plan_hash</c>
/// variance across the window beside a skewed <c>top_value_frequency</c> in <c>pg_column_stats</c>, and
/// <c>PG_SEQ_SCAN_ADVISORY</c> from a <c>plan_json</c> Seq Scan over a large relation under a selective predicate
/// (<c>pg_predicate_stats</c>). Bars carry their lineage marker within six lines; unmeasured ones carry
/// <c>threshold_lineage = 0</c>; gates are rates or fractions of OBSERVED time, never absolute totals. No bar may
/// be a SQL Server plan-regression constant reused by value. The Seq-Scan card is evidence first: an index
/// suggestion appears in its advice only where the predicate columns are KNOWN from <c>pg_qualstats</c> and every
/// gate holds, and then with its cost beside it (the maintainer withdrew the "never DDL" reading of D8 on
/// 2026-09-20 — the analysis engine recommends DDL elsewhere, RCSI being the precedent).
///
/// <para><b>The Seq-Scan advisory grades a SHAPE, not a cost.</b> A sequential scan is the right plan for a small
/// relation, for an unselective predicate, and for a statement that runs once a day; the card fires only when all
/// three are false at once — the relation is large (<c>pg_table_bloat_stats.heap_bytes</c>), the predicate is
/// selective (<c>pg_qualstats</c>' <c>rows_filtered / rows_evaluated</c>), and the plan was captured often enough
/// per observed hour to be a recurring path rather than one slow execution. Even then it is an ADVISORY-band card
/// (D5: 0.5, lifted to 0.65 by a same-statement co-fire, never CRITICAL by itself): it does not know the write rate
/// of the relation or the operator's storage budget, which is what decides whether an index pays for itself. Every
/// bar is <b>unmeasured</b>: the 2026-09-19 calibration read no plan-node or predicate-selectivity distribution
/// (no cluster on the measured population captures plans, and the bloat floor it did read — 64 MiB, the size at
/// which the bloat ESTIMATE becomes trustworthy — answers a different question than "large enough that a scan
/// hurts").</para>
///
/// <para><b>Why the ratio and not the SQL Server pair (per-execution CPU delta × plan-hash count).</b> The SQL
/// Server family (<c>FactScorer.ScorePlanRegressionFact</c>) grades a Query Store regression by the CPU-per-execution
/// delta between two plans the store itself attributes runtime to, and its constants (the 2 ms / 10 ms deltas, the
/// 14-day skew window) are that engine's. PostgreSQL offers neither: <c>pg_stat_statements</c> has no per-plan
/// attribution and no CPU, and the only plan evidence is <c>auto_explain</c>'s threshold-gated capture, whose
/// ABSENCE means "not slow enough to log", never "no plan". So the two readings are joined by TIME — the moment the
/// captured hash changed splits the statement's stored deltas into before and after — and the decision variable is
/// the per-call mean's RATIO across that split, with an absolute per-call delta beside it so a 1 ms → 2 ms step
/// (2×, nothing) cannot fire and a call floor on both sides so a mean over three calls cannot. Every one of those
/// numbers is <b>unmeasured</b>: the 2026-09-19 fleet calibration read no plan-hash distribution (no cluster on the
/// measured population captures plans — Aurora has no readable log) and no per-statement mean-step distribution.</para>
///
/// <para><b>Parameter sensitivity is a convention-class card (D5).</b> It names a MECHANISM — the planner sees
/// different values and picks different plans, and one of the predicate columns is skewed enough for that to be
/// the reason — not a fire; it roots at the advisory base and reaches ≥ 0.5 only when the regression co-fires on
/// the same statement, which is the workload evidence that the mechanism cost something.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>
    /// The ratio of the per-call mean AFTER the plan flip to the mean BEFORE it at which the step is CONCERNING
    /// (severity 0.5 — the story threshold) and at which it is CRITICAL (1.0). Below the concerning ratio the fact
    /// scores 0: a step under 2× is inside what a warm-vs-cold cache or a busier hour does to a mean without any
    /// plan change, so "the plan flipped and the mean doubled" is the smallest claim this family makes.
    /// unmeasured: chosen, not measured — calibrate against the per-statement mean-ms step across plan_hash flips in
    /// pg_statement_stats × pg_plan_capture (p50 / p95 of after ÷ before over the flips the fleet captures) before the
    /// next release; the 2026-09-19 calibration had no plan-hash distribution to read. The fact carries
    /// threshold_lineage = 0.
    /// </summary>
    public const double PlanRegressionRatioConcerning = 2.0;
    public const double PlanRegressionRatioCritical = 5.0;

    /// <summary>
    /// The smallest absolute per-call step (mean after − mean before, ms) that can be a regression. A ratio alone
    /// admits 0.4 ms → 1 ms — 2.5×, and invisible to every caller — so the step must also be large in the unit a
    /// caller feels. unmeasured: chosen, not measured — calibrate against the same flip read as the ratio bars
    /// (the distribution of mean_after − mean_before in ms) before the next release. The fact carries
    /// threshold_lineage = 0.
    /// </summary>
    public const double PlanRegressionMinDeltaMs = 50.0;

    /// <summary>
    /// The fewest calls on EACH side of the flip for the two means to be compared: a mean over a handful of calls
    /// is one slow execution, not a plan. A COUNT, not a rate — the window's stored deltas are summed per side, and
    /// what makes a mean trustworthy is how many calls it averages over, at any <c>hours_back</c>. unmeasured:
    /// chosen, not measured — calibrate against calls per side across the captured flips in pg_statement_stats
    /// before the next release. The fact carries threshold_lineage = 0.
    /// </summary>
    public const int PlanRegressionMinCallsPerSide = 20;

    /// <summary>The boost a co-firing sibling adds to a plan regression — see <see cref="PlanAmplifiers"/>. Two
    /// corroborators (the statement's own bad-actor share and the server-wide mean anomaly) lift a critical
    /// regression from 1.0 to 1.6, over the 1.5 notify floor; one lifts it to 1.3, under it — a plan flip that is
    /// neither the workload's head nor visible in the server's own mean is a card, not a page. unmeasured: chosen,
    /// not measured — calibrate against analysis_findings co-fire rates before the next release.</summary>
    public const double PlanCoFireBoost = 0.3;

    /// <summary>
    /// How many distinct <c>plan_hash</c> values one <c>query_id</c> must have been captured under in the window to
    /// be parameter-SENSITIVE rather than merely re-planned once: two hashes is a flip (the regression's shape);
    /// three or more is the planner choosing among plans as the values change. unmeasured: chosen, not measured —
    /// calibrate against COUNT(DISTINCT plan_hash) per query_id per window in pg_plan_capture before the next
    /// release. The fact carries threshold_lineage = 0.
    /// </summary>
    public const int ParameterSensitivityMinHashes = 3;

    /// <summary>
    /// The <c>top_value_frequency</c> (<c>most_common_freqs[1]</c> of <c>pg_stats</c>, stored by <c>pg_column_stats</c>
    /// — the frequency alone, never the value, V91) at or above which a predicate column is SKEWED: one value covers
    /// half the table, so a parameter equal to it and a parameter equal to anything else are two different
    /// selectivities and the planner is right to pick two plans. unmeasured: chosen, not measured — calibrate against
    /// the distribution of top_value_frequency over the predicate columns pg_predicate_stats names before the next
    /// release. The fact carries threshold_lineage = 0.
    /// </summary>
    public const double ParameterSensitivitySkewFrequency = 0.5;

    /// <summary>
    /// The advisory base a parameter-sensitivity card roots at (D5: a convention-class reading — a mechanism, not a
    /// fire — sits under the 0.5 story threshold on its own) and the boost that lifts it when <c>PG_PLAN_REGRESSION</c>
    /// co-fires on the same statement: 0.4 × (1 + 0.5) = 0.6, over the threshold — the plan variance cost something.
    /// unmeasured: the base is the shared advisory line (the config family's <c>KnobAdvisoryBase</c> shape, stated
    /// here on its own so the two can move apart); the boost is chosen, not measured — calibrate against
    /// analysis_findings co-fire rates before the next release. The fact carries threshold_lineage = 0.
    /// </summary>
    public const double ParameterSensitivityBase = 0.4;
    public const double ParameterSensitivityCoFireBoost = 0.5;

    /* ── lane 30: the Seq-Scan advisory's three gates, its base and its one boost. ── */

    /// <summary>
    /// The <c>pg_qualstats</c> selectivity (<c>rows_filtered / rows_evaluated</c> on the most selective predicate
    /// column the statement applies to the scanned relation, newest sample) at or above which the predicate is
    /// SELECTIVE: nine rows in ten evaluated are thrown away, so the scan reads the relation to keep a tenth of it
    /// or less. Under it a Seq Scan is reading rows it mostly wants and no access path would read much less.
    /// unmeasured: chosen, not measured — calibrate against the distribution of rows_filtered / rows_evaluated over
    /// pg_predicate_stats rows whose (query_id, table) has a captured Seq Scan before the next release; the
    /// 2026-09-19 calibration read no predicate selectivity. The fact carries threshold_lineage = 0.
    /// </summary>
    public const double SeqScanSelectiveFraction = 0.9;

    /// <summary>
    /// The relation's <c>heap_bytes</c> (<c>pg_table_bloat_stats</c>, hourly, newest sample) at or above which it is
    /// LARGE for this card: a quarter gigabyte is past what a sequential read returns from in a few tens of
    /// milliseconds on any store this product monitors, and past what a warm shared_buffers holds for one relation
    /// on a default-sized host. unmeasured: chosen, not measured — the calibration's 64 MiB bloat floor
    /// (<see cref="BloatSizeFloorBytes"/>) is the size at which the bloat ESTIMATE becomes trustworthy, a different
    /// question, so it is not reused here; calibrate against heap_bytes over the relations captured Seq Scans name
    /// before the next release. The fact carries threshold_lineage = 0.
    /// </summary>
    public const long SeqScanLargeRelationBytes = 256L * 1024 * 1024;

    /// <summary>
    /// The fewest captured plans per OBSERVED hour (<c>captures / (ObservedDurationMs / 3 600 000)</c>, computed by
    /// the collector and stored as <c>captures_per_hour_N</c>) for the (relation, statement) pair to be a recurring
    /// path rather than one slow execution. A LOWER BOUND on the true scan rate, and the advice says so:
    /// <c>auto_explain</c> logs only executions slower than <c>auto_explain.log_min_duration</c>, so every scan that
    /// finished under the threshold is invisible here. A rate over observed time, so it scales with
    /// <c>hours_back</c> (#3538 A7). unmeasured: chosen, not measured — calibrate against captures per hour per
    /// (query_id, relation) in pg_plan_capture before the next release. The fact carries threshold_lineage = 0.
    /// </summary>
    public const double SeqScanMinCapturesPerHour = 3.0;

    /// <summary>
    /// The advisory base the Seq-Scan card roots at when at least one carried pair clears all three gates (D5: a
    /// shape without its cost sits at the story threshold, not over it — the card is a question, not a page), and
    /// the boost that lifts it to 0.65 when the top pair's statement is ALSO the window's bad actor
    /// (<c>PG_BAD_ACTOR_&lt;queryid&gt;</c> fired) or regressed (<c>PG_PLAN_REGRESSION</c> fired on it) — the
    /// workload evidence that the scan costs something this window. One boost, either corroborator: 0.5 × 1.3 =
    /// 0.65, inside the advisory band; two would take it to 0.8, which the brief does not want a shape to reach
    /// alone. unmeasured: the base is the D5 advisory line; the boost is chosen, not measured — calibrate against
    /// analysis_findings co-fire rates before the next release. The fact carries threshold_lineage = 0.
    /// </summary>
    public const double SeqScanAdvisoryBase = 0.5;
    public const double SeqScanCoFireBoost = 0.3;

    /// <summary>How many (relation, statement) pairs one Seq-Scan fact carries, ranked by
    /// <c>captures_per_hour × rows_removed_share</c> — a PAGE SIZE for the metadata (each pair is a dozen doubles
    /// under a <c>_N</c> suffix), not a bar. The count of pairs seen rides on <c>candidate_pairs</c>.</summary>
    public const int SeqScanCarriedPairs = 3;

    /* ── metadata keys the collector writes and the scorer, advice and graph read — declared once. ── */

    /// <summary>The Seq-Scan fact's per-pair keys, suffixed <c>_1</c> … <c>_3</c> by rank: <see cref="SeqScanPairKey"/>
    /// builds them, so the collector, the scorer and the advice spell one name. <c>query_id_hi</c> / <c>query_id_lo</c>
    /// are the statement's 64-bit id as two 32-bit halves (each exact in a double — the whole id is not, lane 7's
    /// reason; <see cref="SeqScanQueryId"/> rejoins them). Absent, never 0, when the source had no row: no
    /// <c>heap_bytes</c> when <c>pg_table_bloat_stats</c> never sampled the relation, no <c>selectivity</c> when
    /// <c>pg_qualstats</c> recorded no predicate for the pair, no <c>rows_removed_share</c> when the captured plans
    /// carried no <c>Actual Rows</c> (<c>auto_explain.log_analyze</c> off).</summary>
    public const string SeqScanQueryIdHiKey = "query_id_hi";
    public const string SeqScanQueryIdLoKey = "query_id_lo";
    public const string SeqScanCapturesKey = "captures";
    public const string SeqScanCapturesPerHourKey = "captures_per_hour";
    public const string SeqScanRowsRemovedShareKey = "rows_removed_share";
    public const string SeqScanHeapBytesKey = "heap_bytes";
    public const string SeqScanSelectivityKey = "selectivity";
    public const string SeqScanEstimateErrorKey = "estimate_error_ratio";
    /// <summary>How many carried pairs the fact holds (≤ <see cref="SeqScanCarriedPairs"/>) and how many the walk saw.</summary>
    public const string SeqScanPairsKey = "pairs";
    public const string SeqScanCandidatePairsKey = "candidate_pairs";

    /// <summary><c>{key}_{rank}</c>, rank 1-based.</summary>
    public static string SeqScanPairKey(string key, int rank) => key + "_" + rank.ToString(CultureInfo.InvariantCulture);

    /// <summary>The two 32-bit halves of a statement id, as the doubles the metadata can carry exactly.</summary>
    public static (double Hi, double Lo) SeqScanSplitQueryId(long queryId) => unchecked(((double)(int)(queryId >> 32), (double)(uint)queryId));

    /// <summary>The statement id of the fact's <paramref name="rank"/>-th pair, rejoined from its halves; null when
    /// the pair is not carried.</summary>
    public static long? SeqScanQueryId(Fact fact, int rank)
    {
        if (!fact.Metadata.TryGetValue(SeqScanPairKey(SeqScanQueryIdHiKey, rank), out var hi)
            || !fact.Metadata.TryGetValue(SeqScanPairKey(SeqScanQueryIdLoKey, rank), out var lo))
            return null;
        return unchecked(((long)(int)hi << 32) | (uint)lo);
    }

    /// <summary>Per-call mean ms of the statement before / after the flip, and the calls each mean averages over.</summary>
    public const string PlanMeanMsBeforeKey = "mean_ms_before";
    public const string PlanMeanMsAfterKey = "mean_ms_after";
    public const string PlanCallsBeforeKey = "calls_before";
    public const string PlanCallsAfterKey = "calls_after";
    /// <summary>Distinct <c>plan_hash</c> values the statement was captured under in the window.</summary>
    public const string PlanHashCountKey = "plan_hash_count";

    /// <summary>Which baseline series <c>ANOMALY_PG_PLAN_REGRESSION</c> was graded on (#3691 lane 39): <c>1</c> the
    /// KEYED per-<c>queryid</c> per-call mean — the fact names its statement on <see cref="Fact.ObjectName"/> — or
    /// <c>0</c> the server-wide mean, the cold fallback for a server with no trustworthy keyed bucket, which names
    /// none. Absent reads as 0, which is what every such fact written before the switch was.</summary>
    public const string PlanAnomalySeriesKey = "series";
    /// <summary>The skewed predicate column's <c>top_value_frequency</c> — the sensitivity fact's value, named.</summary>
    public const string PlanTopValueFrequencyKey = "top_value_frequency";
    /// <summary>The family's <c>unavailable</c> flag: the fact is emitted at 0 with exactly one <c>reason_*</c> flag
    /// beside it and is graded nothing; the advice reads the reason and says which precondition is missing.</summary>
    public const string PlanUnavailableKey = "unavailable";
    /// <summary><c>pg_plan_capture_readiness</c>' <c>library_loaded</c> facet is NOT satisfied: <c>auto_explain</c> is
    /// not in <c>shared_preload_libraries</c>, so no plan can be captured and no flip can be seen.</summary>
    public const string PlanReasonAutoExplainOffKey = "reason_auto_explain_off";
    /// <summary><c>pg_extension_availability</c> says <c>pg_qualstats</c> is not installed in any database, so the
    /// predicate-column half of parameter sensitivity cannot be read.</summary>
    public const string PlanReasonQualstatsAbsentKey = "reason_pg_qualstats_absent";
    /// <summary><c>pg_qualstats</c> is installed but recorded no predicate for this statement — its default
    /// <c>sample_rate</c> is <c>1 / max_connections</c>, so a statement can run for hours and never be sampled.</summary>
    public const string PlanReasonNoPredicateSampledKey = "reason_no_predicate_sampled";

    /// <summary>
    /// Layer-1 base severity for the plan family. <c>PG_PLAN_REGRESSION</c>: the after ÷ before ratio through the
    /// shared formula between the two ratio bars, self-gated to 0 under the concerning ratio, under the absolute
    /// per-call delta, and under the call floor on either side; an <c>unavailable</c> fact is graded nothing.
    /// <c>PG_PARAMETER_SENSITIVITY</c>: the advisory base when the hash count and the skew both clear their bars,
    /// else 0. <c>PG_SEQ_SCAN_ADVISORY</c>: the advisory base when at least one carried pair clears all three gates
    /// (selective, large, recurring), else 0; an <c>unavailable</c> fact is graded nothing. Stamps
    /// <c>threshold_lineage = 0</c> on every fact it grades, including the ones a gate zeroes — every bar in this
    /// family is unmeasured.
    /// </summary>
    /* filled by lane 27 — the marker stays, as v1's did; lane 30 added the Seq-Scan arm below it. */
    private static partial double ScorePlanFact(Fact fact) => fact.Key switch
    {
        PgTargetFactKeys.PlanRegression => ScorePlanRegression(fact),
        PgTargetFactKeys.ParameterSensitivity => ScoreParameterSensitivity(fact),
        /* lane 30: PG_SEQ_SCAN_ADVISORY — a shape graded on three gates, advisory band. */
        PgTargetFactKeys.SeqScanAdvisory => ScoreSeqScanAdvisory(fact),
        _ => 0.0,
    };

    /// <summary>The Seq-Scan arm, documented on <see cref="ScorePlanFact"/>. The gates are applied per carried pair
    /// (the collector ranks by <c>captures_per_hour × rows_removed_share</c>, which is not the gate order, so the
    /// top pair may fail a gate a lower one clears); a pair whose selectivity or heap size is ABSENT fails that gate
    /// — an unknown is not a pass. Which pairs cleared rides back on <c>pairs_clearing</c> so the advice can say
    /// "the second pair, not the first".</summary>
    private static double ScoreSeqScanAdvisory(Fact fact)
    {
        if (fact.Metadata.GetValueOrDefault(PlanUnavailableKey) >= 1.0)
            return 0.0;
        if (fact.Metadata.GetValueOrDefault(SeqScanPairsKey) < 1)
            return 0.0;

        /* unmeasured: every bar below (see the declarations) — the fact says so. */
        fact.Metadata["threshold_lineage"] = 0;

        var clearing = 0;
        for (var rank = 1; rank <= SeqScanCarriedPairs; rank++)
        {
            if (SeqScanPairClearsEveryGate(fact, rank))
                clearing++;
        }

        fact.Metadata["pairs_clearing"] = clearing;
        /* unmeasured: SeqScanAdvisoryBase — the D5 advisory line; the one co-fire lifts it, nothing else does. */
        return clearing > 0 ? SeqScanAdvisoryBase : 0.0;
    }

    /// <summary>Whether the fact's <paramref name="rank"/>-th pair is selective, large AND recurring — the three
    /// gates, each against its declared bar; shared by the scorer and the advice so "cleared" is decided once.</summary>
    public static bool SeqScanPairClearsEveryGate(Fact fact, int rank)
    {
        if (!fact.Metadata.TryGetValue(SeqScanPairKey(SeqScanSelectivityKey, rank), out var selectivity)
            || !fact.Metadata.TryGetValue(SeqScanPairKey(SeqScanHeapBytesKey, rank), out var heapBytes)
            || !fact.Metadata.TryGetValue(SeqScanPairKey(SeqScanCapturesPerHourKey, rank), out var perHour))
            return false;

        /* unmeasured: SeqScanSelectiveFraction / SeqScanLargeRelationBytes / SeqScanMinCapturesPerHour — the three
           gates, declared above with their calibrating reads. */
        return selectivity >= SeqScanSelectiveFraction
            && heapBytes >= SeqScanLargeRelationBytes
            && perHour >= SeqScanMinCapturesPerHour;
    }

    /// <summary>The regression ramp, documented on <see cref="ScorePlanFact"/>.</summary>
    private static double ScorePlanRegression(Fact fact)
    {
        if (fact.Metadata.GetValueOrDefault(PlanUnavailableKey) >= 1.0)
            return 0.0;
        if (!fact.Metadata.TryGetValue(PlanMeanMsBeforeKey, out var before) || before <= 0
            || !fact.Metadata.TryGetValue(PlanMeanMsAfterKey, out var after))
            return 0.0;

        /* unmeasured: every bar below (see the declarations) — the fact says so. */
        fact.Metadata["threshold_lineage"] = 0;

        /* unmeasured: PlanRegressionMinCallsPerSide — a mean over a handful of calls is one execution. */
        if (fact.Metadata.GetValueOrDefault(PlanCallsBeforeKey) < PlanRegressionMinCallsPerSide
            || fact.Metadata.GetValueOrDefault(PlanCallsAfterKey) < PlanRegressionMinCallsPerSide)
            return 0.0;

        /* unmeasured: PlanRegressionMinDeltaMs — the step must be large in the unit a caller feels. */
        if (after - before < PlanRegressionMinDeltaMs)
            return 0.0;

        var ratio = after / before;
        /* unmeasured: PlanRegressionRatioConcerning — under it the fact is not a regression at all (self-gate). */
        if (ratio < PlanRegressionRatioConcerning)
            return 0.0;

        /* unmeasured: both ratio bars, declared above with their calibrating read. */
        return FactScorer.ApplyThresholdFormula(ratio, PlanRegressionRatioConcerning, PlanRegressionRatioCritical);
    }

    /// <summary>The sensitivity arm, documented on <see cref="ScorePlanFact"/>.</summary>
    private static double ScoreParameterSensitivity(Fact fact)
    {
        if (fact.Metadata.GetValueOrDefault(PlanUnavailableKey) >= 1.0)
            return 0.0;
        if (!fact.Metadata.TryGetValue(PlanHashCountKey, out var hashes)
            || !fact.Metadata.TryGetValue(PlanTopValueFrequencyKey, out var frequency))
            return 0.0;

        /* unmeasured: every bar below (see the declarations) — the fact says so. */
        fact.Metadata["threshold_lineage"] = 0;

        /* unmeasured: ParameterSensitivityMinHashes / ParameterSensitivitySkewFrequency — both halves of the shape. */
        if (hashes < ParameterSensitivityMinHashes || frequency < ParameterSensitivitySkewFrequency)
            return 0.0;

        /* unmeasured: ParameterSensitivityBase — the D5 advisory line; a co-fire lifts it, nothing else does. */
        return ParameterSensitivityBase;
    }

    /// <summary>
    /// Layer-2 amplifiers for the plan family. Each predicate asks only whether the sibling FIRED (its own scorer
    /// put <see cref="Fact.BaseSeverity"/> above zero) and, where the statement can be matched, that it is the SAME
    /// statement — the <c>query_id</c> travels on <see cref="Fact.ObjectName"/> (a 64-bit id is exact in a double only
    /// to 2^53, lane 7's reason for keeping it out of the metadata), so the match is a string comparison against
    /// lane 7's <see cref="PgTargetFactKeys.BadActorKey"/> shape.
    /// <list type="bullet">
    /// <item><description><c>PG_PLAN_REGRESSION</c>: <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> fired for the same
    /// <c>query_id</c> — the statement that regressed is also one holding the window's execution time, so the
    /// regression is the workload's, not a corner's; and <c>ANOMALY_PG_PLAN_REGRESSION</c> fired — the server-wide
    /// per-call mean moved against this hour's baseline, the statistical reading of the same step. The anomaly is a
    /// context fact whose <c>anomaly</c> source is in <c>FactScorer.ScoreAll</c>'s lookup, so its verdict is readable
    /// here exactly as a regular fact's (lane 15's pattern) — declared as an amplifier, never as an edge from the
    /// anomaly, because a base-0 fact cannot open one.</description></item>
    /// <item><description><c>PG_PARAMETER_SENSITIVITY</c>: <c>PG_PLAN_REGRESSION</c> fired on the same statement —
    /// the plan variance the skew explains cost something this window (D5: the co-fire that lifts a convention card
    /// over the story threshold).</description></item>
    /// <item><description><c>PG_SEQ_SCAN_ADVISORY</c>: ONE amplifier, either corroborator — the top pair's statement
    /// (its id rejoined from the <c>_1</c> halves, since <see cref="Fact.ObjectName"/> carries the RELATION on this
    /// fact) is the window's <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> or the statement <c>PG_PLAN_REGRESSION</c> named,
    /// and that sibling fired. The scan shape then has a cost beside it this window. Only the top pair: the card's
    /// headline names it, and lifting the card for a lower pair's statement would put the lift under the wrong
    /// name.</description></item>
    /// </list>
    /// </summary>
    /* filled by lane 27 — the marker stays; lane 30 added the Seq-Scan case. */
    private static partial List<AmplifierDefinition> PlanAmplifiers(string key)
    {
        switch (key)
        {
            case PgTargetFactKeys.PlanRegression:
                return
                [
                    new()
                    {
                        Description = "PG_BAD_ACTOR fired for the same queryid — the statement that regressed also holds a large share of the window's execution time",
                        /* unmeasured: PlanCoFireBoost, chosen, not measured — see its declaration. */
                        Boost = PlanCoFireBoost,
                        Predicate = facts => SameStatementFired(facts, PgTargetFactKeys.PlanRegression, PgTargetFactKeys.BadActorKeyPrefix),
                    },
                    new()
                    {
                        Description = "ANOMALY_PG_PLAN_REGRESSION fired — this statement's per-call time moved against its own hour-of-week baseline this window (or, on the cold fallback series, the server's did)",
                        /* unmeasured: PlanCoFireBoost, chosen, not measured — see its declaration. */
                        Boost = PlanCoFireBoost,
                        Predicate = PlanRegressionAnomalyCorroborates,
                    },
                ];

            case PgTargetFactKeys.ParameterSensitivity:
                return
                [
                    new()
                    {
                        Description = "PG_PLAN_REGRESSION fired on the same queryid — the plan variance the skewed column explains stepped the statement's per-call time this window",
                        /* unmeasured: ParameterSensitivityCoFireBoost — 0.4 × 1.5 = 0.6, the D5 line. */
                        Boost = ParameterSensitivityCoFireBoost,
                        Predicate = facts => SameStatementFired(facts, PgTargetFactKeys.ParameterSensitivity, PgTargetFactKeys.PlanRegression),
                    },
                ];

            case PgTargetFactKeys.SeqScanAdvisory:
                return
                [
                    new()
                    {
                        Description = "PG_BAD_ACTOR or PG_PLAN_REGRESSION fired for the statement behind the top scanned pair — the scan shape has a measured cost this window",
                        /* unmeasured: SeqScanCoFireBoost — 0.5 × 1.3 = 0.65, inside the advisory band. */
                        Boost = SeqScanCoFireBoost,
                        Predicate = SeqScanStatementCoFired,
                    },
                ];

            default:
                return [];
        }
    }

    /// <summary>
    /// Whether the fact under <paramref name="selfKey"/> names a statement (<see cref="Fact.ObjectName"/> is its
    /// <c>query_id</c>) and the sibling FIRED for the same statement: <paramref name="siblingKeyOrPrefix"/> is either
    /// an exact key whose fact must carry the same <c>ObjectName</c> (<c>PG_PLAN_REGRESSION</c>), or lane 7's
    /// dynamic-key prefix, completed with the id (<c>PG_BAD_ACTOR_&lt;queryid&gt;</c>). Shared by the amplifiers and
    /// the graph's predicates so "the same statement" is decided once.
    /// </summary>
    internal static bool SameStatementFired(IReadOnlyDictionary<string, Fact> facts, string selfKey, string siblingKeyOrPrefix)
    {
        if (!facts.TryGetValue(selfKey, out var self) || string.IsNullOrEmpty(self.ObjectName))
            return false;

        if (siblingKeyOrPrefix.EndsWith('_'))
            return facts.TryGetValue(siblingKeyOrPrefix + self.ObjectName, out var dynamic) && dynamic.BaseSeverity > 0;

        return facts.TryGetValue(siblingKeyOrPrefix, out var sibling) && sibling.BaseSeverity > 0
            && string.Equals(sibling.ObjectName, self.ObjectName, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether <c>ANOMALY_PG_PLAN_REGRESSION</c> fired AND its verdict is about the statement
    /// <c>PG_PLAN_REGRESSION</c> named — the one predicate behind the regression card's anomaly amplifier and the
    /// anomaly's edge into it, so the two cannot disagree about what "corroborates" means.
    ///
    /// <para>Two series answer to the anomaly's key since #3691 lane 39 (<c>series</c> in its metadata). On the
    /// KEYED series (1) the anomaly names ONE <c>queryid</c> on <see cref="Fact.ObjectName"/>, and corroboration
    /// means the SAME statement: a different flipped statement's deviation, however large, is that statement's
    /// story and lifting this card with it would put the lift under the wrong name (the rule the bad-actor alias
    /// edge above already follows). On the COLD FALLBACK series (0) the anomaly names no statement at all — it is
    /// the server's mean per-call time, which is all a server with no per-statement history can offer — so it
    /// corroborates whatever statement the regular fact named, exactly as it did before the switch. A fact with no
    /// <c>series</c> stamp reads as the fallback: that is what every ANOMALY_PG_PLAN_REGRESSION written before the
    /// switch was, and a stored fact re-scored later must not change meaning.</para>
    /// </summary>
    internal static bool PlanRegressionAnomalyCorroborates(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.AnomalyPlanRegression, out var anomaly) || anomaly.BaseSeverity <= 0)
            return false;

        if (anomaly.Metadata.GetValueOrDefault(PlanAnomalySeriesKey) < 1.0)
            return facts.TryGetValue(PgTargetFactKeys.PlanRegression, out var regression) && regression.BaseSeverity > 0;

        return SameStatementFired(facts, PgTargetFactKeys.PlanRegression, PgTargetFactKeys.AnomalyPlanRegression);
    }

    /// <summary>Whether the Seq-Scan fact's top pair names a statement whose bad actor or plan regression FIRED — the
    /// one predicate behind the card's amplifier and both of its edges.</summary>
    internal static bool SeqScanStatementCoFired(IReadOnlyDictionary<string, Fact> facts) =>
        SeqScanTopStatementBadActorFired(facts) || SeqScanTopStatementRegressed(facts);

    /* Block bodies, not expression bodies: a `is { } id` property pattern in an expression-bodied member stops the
       TsqlConventionGuardTests member walk at its braces (#3799 reshaped the buffer drill-down's Round for the same
       reason), and a block the walk reads whole needs no KnownTruncatedRanges line. */
    internal static bool SeqScanTopStatementBadActorFired(IReadOnlyDictionary<string, Fact> facts)
    {
        var id = SeqScanTopStatement(facts);
        if (id is null)
            return false;
        return facts.TryGetValue(PgTargetFactKeys.BadActorKey(id.Value), out var actor) && actor.BaseSeverity > 0;
    }

    internal static bool SeqScanTopStatementRegressed(IReadOnlyDictionary<string, Fact> facts)
    {
        var id = SeqScanTopStatement(facts);
        if (id is null)
            return false;
        return facts.TryGetValue(PgTargetFactKeys.PlanRegression, out var regression) && regression.BaseSeverity > 0
            && string.Equals(regression.ObjectName, id.Value.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
    }

    /// <summary>The statement id of the Seq-Scan fact's top pair, or null when the fact is absent, unavailable or
    /// carries no pair.</summary>
    internal static long? SeqScanTopStatement(IReadOnlyDictionary<string, Fact> facts) =>
        facts.TryGetValue(PgTargetFactKeys.SeqScanAdvisory, out var scan) && scan.Metadata.GetValueOrDefault(PlanUnavailableKey) < 1.0
            ? SeqScanQueryId(scan, 1)
            : null;
}
