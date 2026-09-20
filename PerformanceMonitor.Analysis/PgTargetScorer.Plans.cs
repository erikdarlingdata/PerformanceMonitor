/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_plans</c> — the plan family (filled by lane 27 of #3691, design §6; lane 30 adds the Seq-Scan advisory):
/// <c>PG_PLAN_REGRESSION</c> from a statement's per-call mean-ms step in <c>pg_statement_stats</c> coinciding
/// with a <c>plan_hash</c> flip in <c>pg_plan_capture</c>, <c>PG_PARAMETER_SENSITIVITY</c> from <c>plan_hash</c>
/// variance across the window beside a skewed <c>top_value_frequency</c> in <c>pg_column_stats</c>, and
/// <c>PG_SEQ_SCAN_ADVISORY</c> from a <c>plan_json</c> Seq Scan over a large relation under a selective predicate
/// (<c>pg_predicate_stats</c>). Bars carry their lineage marker within six lines; unmeasured ones carry
/// <c>threshold_lineage = 0</c>; gates are rates or fractions of OBSERVED time, never absolute totals. No bar may
/// be a SQL Server plan-regression constant reused by value, and no fact or amplifier in this family may carry
/// <c>CREATE INDEX</c> text (D8).
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

    /* ── metadata keys the collector writes and the scorer, advice and graph read — declared once. ── */

    /// <summary>Per-call mean ms of the statement before / after the flip, and the calls each mean averages over.</summary>
    public const string PlanMeanMsBeforeKey = "mean_ms_before";
    public const string PlanMeanMsAfterKey = "mean_ms_after";
    public const string PlanCallsBeforeKey = "calls_before";
    public const string PlanCallsAfterKey = "calls_after";
    /// <summary>Distinct <c>plan_hash</c> values the statement was captured under in the window.</summary>
    public const string PlanHashCountKey = "plan_hash_count";
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
    /// else 0. <c>PG_SEQ_SCAN_ADVISORY</c> is lane 30's and scores 0 here. Stamps <c>threshold_lineage = 0</c> on
    /// every fact it grades, including the ones a gate zeroes — every bar in this family is unmeasured.
    /// </summary>
    /* filled by lane 27 — the marker stays, as v1's did; lane 30 adds the Seq-Scan arm below it. */
    private static partial double ScorePlanFact(Fact fact) => fact.Key switch
    {
        PgTargetFactKeys.PlanRegression => ScorePlanRegression(fact),
        PgTargetFactKeys.ParameterSensitivity => ScoreParameterSensitivity(fact),
        /* lane 30: PG_SEQ_SCAN_ADVISORY — evidence-only card, its own arm when the lane lands. */
        _ => 0.0,
    };

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
    /// <item><description><c>PG_SEQ_SCAN_ADVISORY</c>: none here — lane 30's.</description></item>
    /// </list>
    /// </summary>
    /* filled by lane 27 — the marker stays; lane 30 adds the Seq-Scan case. */
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
                        Description = "ANOMALY_PG_PLAN_REGRESSION fired — the server's per-call statement time moved against its own hour-of-week baseline this window",
                        /* unmeasured: PlanCoFireBoost, chosen, not measured — see its declaration. */
                        Boost = PlanCoFireBoost,
                        Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AnomalyPlanRegression, out var anomaly) && anomaly.BaseSeverity > 0,
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

            default:
                /* lane 30: PG_SEQ_SCAN_ADVISORY's amplifiers, when the lane declares any. */
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
}
