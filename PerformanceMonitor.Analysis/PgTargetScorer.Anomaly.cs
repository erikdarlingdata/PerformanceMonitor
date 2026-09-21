/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The <c>ANOMALY_PG_*</c> arm: the membership predicates the shared anomaly scorer routes on, the ratio ramp,
/// and the load-family co-fire table (lane 9 of #3542; the predicates were declared by the plumbing lane because
/// they follow from each detector's SHAPE rather than from any bar).
///
/// <para><b>Why the shared anomaly scorer needs these two predicates at all (#3584).</b>
/// <c>FactScorer.IsDeviationScoredAnomalyKey</c> is a literal list of the seven SQL Server z-score families,
/// and <c>IsExtremeAnomaly</c> / <c>ScoreAnomalyFact</c> both route through it. A PostgreSQL anomaly keyed
/// <c>ANOMALY_PG_*</c> would fall past every arm, score 0 unless it happened to carry <c>ratio</c> metadata,
/// and — because <c>IsTuningClassKey</c> catches every <c>ANOMALY_</c> prefix — never be released from the
/// 1.49 tuning-class cap: the exact notification inertness #3584 fixed for SQL Server, shipping again for
/// PostgreSQL on day one. The shared scorer therefore asks this class first.</para>
///
/// <para>The z-score families (<see cref="PgTargetFactKeys.AnomalyTps"/>,
/// <see cref="PgTargetFactKeys.AnomalySessionSpike"/>, <see cref="PgTargetFactKeys.AnomalyCpuSpike"/>) then
/// grade on the SAME <c>deviation_sigma</c> / <c>fire_threshold</c> / <c>baseline_low_quality</c> /
/// <c>fallback_exceedance</c> metadata the shared <c>AnomalyGate</c> writes, through the shared ramp,
/// unchanged — and through the shared extremity escape: a PostgreSQL session spike at 3× its fire cutoff with
/// two corroborating siblings leaves the 1.49 tuning-class cap exactly as its SQL Server twin does. The ratio
/// families need their own ramp because the SQL Server one recognises <c>ANOMALY_BLOCKING_SPIKE</c> /
/// <c>ANOMALY_DEADLOCK_SPIKE</c> / <c>ANOMALY_WAIT_PROFILE</c> by literal prefix: <see cref="PgTargetFactKeys.AnomalyDeadlockRate"/>
/// (a per-hour rate over its bucket mean) and <see cref="PgTargetFactKeys.AnomalyWaitProfile"/> (the Aurora
/// all-types ms/sec, on <c>modified_z</c> when the bucket is robust and on its ratio otherwise — the statistic
/// the SQL Server profile fact lands on, so it is registered here and not as deviation-scored). Neither ratio
/// family is released from the cap: none is graded in sigmas the escape's multiple was calibrated for, and
/// their impact lives in the never-capped regular facts they fold into (<c>PG_DEADLOCK_RATE</c>; the
/// <c>PG_WAIT_*</c> standouts).</para>
///
/// <para><b>A first occurrence is not a ratio.</b> When the detector could not trust the bucket (<c>is_new</c>),
/// it fired on the absolute bar and carries <c>fallback_exceedance</c> (rate ÷ bar, ≥ 1) and NO sentinel ratio;
/// the ramp grades that exceedance — 0.5 at the bar, 1.0 at twice it — the deviation family's low-quality shape,
/// and the advice renders "first occurrence, no baseline yet", never "100× its baseline".</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>The PostgreSQL z-score families — graded off <c>deviation_sigma</c> against
    /// <c>fire_threshold</c> by the shared ramp, and eligible for the extremity escape from the cap.</summary>
    public static bool IsDeviationScoredAnomalyKey(string? key) =>
        key is PgTargetFactKeys.AnomalyTps
            or PgTargetFactKeys.AnomalySessionSpike
            or PgTargetFactKeys.AnomalyCpuSpike
            /* #3691 v2 plumbing: registered by detector SHAPE ahead of their lanes (11, 12, 15) — each is a
               peak-vs-own-baseline z-score on one series (read latency ms, replay lag bytes, WAL bytes/sec),
               so the shared deviation ramp and the extremity escape grade them off AnomalyGate's metadata
               the day their detector lands. Membership is vocabulary, not a bar; the lanes write no ramp. */
            or PgTargetFactKeys.AnomalyIoLatency
            or PgTargetFactKeys.AnomalyReplicationLag
            or PgTargetFactKeys.AnomalyWalVolume
            /* #3691 wave-3 plumbing (between waves): the blocking anomaly is a peak-vs-own-baseline z-score on one
               series (blocked sessions per capture) — registered by shape ahead of lane 17, as the v2 three were. */
            or PgTargetFactKeys.AnomalyBlocking
            /* #3691 v3 plumbing: the plan-regression and CPU-burn anomalies are each a peak-vs-own-baseline z-score on
               one series (a statement's mean ms; cores busy) — registered by shape ahead of lanes 27 and 28. */
            or PgTargetFactKeys.AnomalyPlanRegression
            or PgTargetFactKeys.AnomalyCpuBurn
            /* lane 34 (#3691, ruled 2026-09-20): one statement's window share against its OWN hour-of-week share bucket
               (lane 33's keyed pg_statement_share arm) — a peak-AND-mean z-score on one keyed series, graded by the
               shared deviation ramp off AnomalyGate's metadata like every z family here. The membership is the one edit
               this lane makes to a scorer root; the detector, the card's context band and the advice are the family's. */
            or PgTargetFactKeys.AnomalyBadActorShare;

    /// <summary>The PostgreSQL ratio-vs-own-baseline families — <see cref="ScoreRatioAnomaly"/> grades these.</summary>
    public static bool IsPgRatioAnomalyKey(string? key) =>
        key is PgTargetFactKeys.AnomalyDeadlockRate
            or PgTargetFactKeys.AnomalyWaitProfile
            /* lane 24 (#3691): the stock SAMPLED wait profile — the Aurora profile's shape (modified z when robust,
               ratio otherwise) on another instrument, so it is a ratio family and never deviation-scored. */
            or PgTargetFactKeys.AnomalySampledWaitProfile;

    /// <summary>
    /// The ratio at which a PostgreSQL ratio-family anomaly saturates at 1.0 — three times the firing multiple
    /// (<see cref="AnomalyThresholds.PgRatioAnomalyThreshold"/>), so the ramp runs 3× → 0.5 to 9× → 1.0: the same
    /// "saturate at a multiple of the anchor" METHOD the deviation ramp uses (2× its anchor), on the ratio axis.
    /// unmeasured: chosen, not measured — the 2026-09-20 calibration placed the firing multiple (see
    /// <see cref="AnomalyThresholds.PgRatioAnomalyThreshold"/>: TPS ratio p99.99 24.4, wait-rate ratio p99.9 31)
    /// but nobody has asked where a ratio family should saturate, so the 9× stays a judgment — calibrate the ramp's
    /// top against those same distributions before the next release; the fact carries threshold_lineage = 0.
    /// </summary>
    public const double RatioAnomalySaturation = 3.0 * AnomalyThresholds.PgRatioAnomalyThreshold;

    /// <summary>
    /// The wait profile's robust ramp span: sigmas ABOVE the heavy-tail cutoff at which it saturates — 0.5 at
    /// <see cref="AnomalyThresholds.HeavyTailModifiedZThreshold"/> (5.0), 1.0 ten sigmas past it (15σ), the SQL
    /// Server profile fact's shape. unmeasured: the cutoff is the SQL Server fleet's calibrated robust statistic
    /// used by reference, and the span is chosen, not measured — calibrate against the modified-z distribution of
    /// pg_wait_stats peaks before the next release; the fact carries threshold_lineage = 0.
    /// </summary>
    public const double WaitProfileModifiedZSpan = 10.0;

    /// <summary>
    /// #3691 (v1 residue, #3689 §5): whether an <see cref="PgTargetFactKeys.AnomalyWaitProfile"/> fact's deviation
    /// is extreme enough to leave the Layer-3 tuning-class cap on its own evidence — the PostgreSQL arm of
    /// <c>FactScorer.IsExtremeAnomaly</c>, routed off the SAME metadata <see cref="ScoreRatioAnomaly"/> grades
    /// from, so the fact can never be extreme on one statistic while scored on another. Never for <c>is_new</c>
    /// (a first occurrence has no baseline to be extreme against). Robust bucket (<c>modified_z &gt; 0</c>):
    /// <paramref name="extremeMultiple"/> × <see cref="AnomalyThresholds.HeavyTailModifiedZThreshold"/> — the SQL
    /// Server profile's own bar, 15σ at the shipped 3×. Ratio trigger otherwise: <paramref name="extremeMultiple"/> ×
    /// <see cref="AnomalyThresholds.PgRatioAnomalyThreshold"/> — the PostgreSQL profile's OWN firing multiple
    /// (3× → 9×), where the SQL Server arm reads 3 × its 4.0 ratio floor; the multiple is the caller's so the
    /// two engines share one "extreme" and differ only in the anchor each fired at. In v1 this family could
    /// never leave the 1.49 cap however far the profile moved ("by design tonight" in #3689) — so a Lock-storm
    /// profile at 40σ with every corroborator lit sat one hundredth under the page line. Since lane 24 the stock
    /// SAMPLED profile (<see cref="PgTargetFactKeys.AnomalySampledWaitProfile"/>) takes the same door on the same
    /// statistic — <see cref="PgTargetFactKeys.IsWaitProfileAnomaly"/> is the key check.
    /// </summary>
    public static bool IsExtremeWaitProfileAnomaly(Fact fact, double extremeMultiple)
    {
        ArgumentNullException.ThrowIfNull(fact);
        if (!PgTargetFactKeys.IsWaitProfileAnomaly(fact.Key)) return false;
        if (fact.Metadata.GetValueOrDefault("is_new") > 0) return false;

        var modifiedZ = fact.Metadata.GetValueOrDefault("modified_z");
        if (modifiedZ > 0)
            return modifiedZ >= extremeMultiple * AnomalyThresholds.HeavyTailModifiedZThreshold;
        return fact.Metadata.GetValueOrDefault("ratio") >= extremeMultiple * AnomalyThresholds.PgRatioAnomalyThreshold;
    }

    /// <summary>
    /// A first-occurrence (<c>is_new</c>) ratio anomaly's ramp: <c>fallback_exceedance</c> of 1.0 (AT the absolute
    /// bar the detector fired on) scores 0.5, saturating to 1.0 at 1 + this span (twice the bar) — the deviation
    /// family's low-quality shape (<c>FactScorer.LowQualityFallbackSpan</c>), restated here because that constant
    /// is private and this is a different family. unmeasured: chosen, not measured — calibrate against the
    /// distribution of first-occurrence exceedances in analysis_findings before the next release.
    /// </summary>
    public const double FirstOccurrenceExceedanceSpan = 1.0;

    /// <summary>
    /// The boost a corroborating sibling adds to a PostgreSQL load-family anomaly. unmeasured: derived, not
    /// measured — from the notify floor (1.5) and the deviation ramp's 1.0 ceiling rather than from any
    /// distribution: an extreme anomaly with ONE corroborator reads 1.3 (WARNING) and with TWO reads 1.6 (pages),
    /// which is #3584's rule that two independent corroborators are the bar for CRITICAL. The SQL Server load
    /// arm uses the same figure for the same derivation; this is not an alias of it, so the two can move apart.
    /// </summary>
    public const double LoadAnomalyCoFireBoost = 0.3;

    /// <summary>
    /// Base severity for a ratio-family PostgreSQL anomaly. Three readings, routed off the metadata the detector
    /// wrote so a fact can never be extreme on one statistic while scored on another:
    /// <list type="bullet">
    /// <item><description><c>is_new</c>: the first-occurrence ramp off <c>fallback_exceedance</c>.</description></item>
    /// <item><description>Either wait profile (Aurora or sampled) with a robust bucket (<c>modified_z &gt; 0</c>): the modified-z ramp.</description></item>
    /// <item><description>Otherwise: the ratio ramp, 0.5 at <see cref="AnomalyThresholds.PgRatioAnomalyThreshold"/>
    /// and 1.0 at <see cref="RatioAnomalySaturation"/>.</description></item>
    /// </list>
    /// Stamps <c>threshold_lineage = 0</c> on every fact it grades. Unlike the deviation ramp this does not
    /// multiply by <c>confidence</c>: the SQL Server ratio arms never did, and a ratio family's trust gate is
    /// binary (trustworthy bucket or <c>is_new</c>) rather than graded.
    /// </summary>
    public static double ScoreRatioAnomaly(Fact fact)
    {
        ArgumentNullException.ThrowIfNull(fact);
        if (!IsPgRatioAnomalyKey(fact.Key)) return 0.0;

        fact.Metadata["threshold_lineage"] = 0;

        if (fact.Metadata.GetValueOrDefault("is_new") >= 1.0)
        {
            var exceedance = fact.Metadata.GetValueOrDefault("fallback_exceedance");
            if (exceedance < 1.0) return 0.0;
            /* unmeasured: FirstOccurrenceExceedanceSpan, chosen, not measured — see its declaration. Floored at
               0.5 so a fact the detector fired on its bar is always rootable (InferenceEngine's 0.5 line). */
            return Math.Max(0.5, 0.5 + 0.5 * Math.Min((exceedance - 1.0) / FirstOccurrenceExceedanceSpan, 1.0));
        }

        if (PgTargetFactKeys.IsWaitProfileAnomaly(fact.Key))
        {
            var modifiedZ = fact.Metadata.GetValueOrDefault("modified_z");
            if (modifiedZ > 0)
            {
                /* unmeasured: the heavy-tail cutoff by reference and WaitProfileModifiedZSpan — see its declaration. */
                if (modifiedZ < AnomalyThresholds.HeavyTailModifiedZThreshold) return 0.0;
                return 0.5 + 0.5 * Math.Min((modifiedZ - AnomalyThresholds.HeavyTailModifiedZThreshold) / WaitProfileModifiedZSpan, 1.0);
            }
        }

        var ratio = fact.Metadata.GetValueOrDefault("ratio");
        /* measured (2026-09-20, per family): PgRatioAnomalyThreshold; unmeasured: RatioAnomalySaturation — see their
           declarations. The span is why the stamp above stays 0 even where the multiple is well placed. */
        if (ratio < AnomalyThresholds.PgRatioAnomalyThreshold) return 0.0;
        return 0.5 + 0.5 * Math.Min(
            (ratio - AnomalyThresholds.PgRatioAnomalyThreshold) / (RatioAnomalySaturation - AnomalyThresholds.PgRatioAnomalyThreshold), 1.0);
    }

    /// <summary>
    /// Layer-2 amplifiers for the <c>ANOMALY_PG_*</c> keys. The LOAD family — TPS, session count, CPU capacity
    /// (Aurora) and, since the third between-waves batch of #3691, CPU burn (<c>pg_stat_kcache</c>, stock and
    /// Aurora alike) — corroborate one another (a real surge moves more than one of them; the root's own key is
    /// omitted so a family never corroborates itself) and are confirmed by the MEASURED <c>PG_CPU_PERCENT</c> fact
    /// at the capacity warning bar. The confirmer reads the capacity-measured flag first: a fact whose value is the
    /// raw percent-of-allocated reading confirms nothing, however high it reads (#3281). On stock PostgreSQL there
    /// is no CPU capacity fact at all and that arm is inert — the advice never implies capacity was checked (D6).
    ///
    /// <para><b>Why the burn anomaly joined the load family (lane 36's measurement, 2026-09-20).</b> On the stock
    /// rig every reachable family cleared the 1.5 notify line except the load pair: a planted storm fired
    /// <c>ANOMALY_PG_TPS</c> and <c>ANOMALY_PG_SESSION_SPIKE</c> at 1.30 exactly — extreme, plus ONE corroborator
    /// (each other) — while <c>ANOMALY_PG_CPU_BURN</c> fired at 25σ (12.55 cores) beside them and nothing read it,
    /// because both CPU confirmers were Aurora-only. Cores busy above this server's own hour-of-week routine is the
    /// same statement about a surge that the capacity anomaly makes on Aurora, from the kernel's counters instead of
    /// Performance Insights', so it is the load family's second independent corroborator on stock: an extreme TPS or
    /// session anomaly with the burn beside it now reads 1.6 and pages, which is #3584's two-corroborator bar. The
    /// burn anomaly takes the load arms in return (symmetry: a family never corroborates itself, everyone else
    /// corroborates it), and <c>PgTargetFactKeys.AnomalyToFamilies</c> folds it onto <c>PG_CPU_DECOMPOSITION</c>
    /// beside the base-0 <c>PG_CPU_BURN_CORES</c> so the fold has a positive-base parent to land on.</para>
    ///
    /// <para>The wait profile is corroborated by the load family moving (a surge is driving the shift) and by a
    /// <c>PG_WAIT_*</c> standout crossing its own threshold (the shift is a nameable wait, not diffuse noise). The
    /// deadlock-rate anomaly has no arm: it folds into <c>PG_DEADLOCK_RATE</c> through the reconciler and its
    /// impact lives in that never-capped fact.</para>
    /// </summary>
    private static partial List<AmplifierDefinition> AnomalyAmplifiers(string key)
    {
        if (key is PgTargetFactKeys.AnomalyTps or PgTargetFactKeys.AnomalySessionSpike or PgTargetFactKeys.AnomalyCpuSpike
            or PgTargetFactKeys.AnomalyCpuBurn)
            return LoadAnomalyAmplifiers(key);

        /* Both wait profiles (Aurora exact; lane 24's stock sampled) take the load arms plus the named-standout arm:
           on stock the CPU confirmer is inert (no PG_CPU_PERCENT fact) and the standout arm reads sampled facts. */
        if (PgTargetFactKeys.IsWaitProfileAnomaly(key))
        {
            var amplifiers = LoadAnomalyAmplifiers(key);
            amplifiers.Add(new()
            {
                Description = "A named PostgreSQL wait crossed its own threshold in the same window — the profile shift is a nameable wait, not diffuse noise",
                /* unmeasured: LoadAnomalyCoFireBoost, derived, not measured — see its declaration. */
                Boost = LoadAnomalyCoFireBoost,
                Predicate = facts =>
                {
                    foreach (var (factKey, fact) in facts)
                    {
                        if (factKey.StartsWith(PgTargetFactKeys.WaitKeyPrefix, StringComparison.Ordinal) && fact.BaseSeverity > 0)
                            return true;
                    }
                    return false;
                },
            });
            return amplifiers;
        }

        return [];
    }

    /// <summary>A sibling anomaly family fired in the same window against its own baseline (its scorer put its
    /// base above zero). The callers skip the self-key.</summary>
    private static bool AnomalyCoFired(Dictionary<string, Fact> facts, string siblingKey) =>
        facts.TryGetValue(siblingKey, out var sibling) && sibling.BaseSeverity > 0;

    private static List<AmplifierDefinition> LoadAnomalyAmplifiers(string selfKey)
    {
        var amplifiers = new List<AmplifierDefinition>();
        void Sibling(string siblingKey, string description)
        {
            if (selfKey == siblingKey) return;
            amplifiers.Add(new()
            {
                Description = description,
                /* unmeasured: LoadAnomalyCoFireBoost, derived, not measured — see its declaration. */
                Boost = LoadAnomalyCoFireBoost,
                Predicate = facts => AnomalyCoFired(facts, siblingKey),
            });
        }

        Sibling(PgTargetFactKeys.AnomalySessionSpike, "Session-count anomaly co-fired — the surge is visible in connections too");
        Sibling(PgTargetFactKeys.AnomalyTps, "Transaction-rate anomaly co-fired — the surge is visible in throughput too");
        Sibling(PgTargetFactKeys.AnomalyCpuSpike, "Capacity anomaly co-fired — the surge is consuming CPU far above this instance's norm");
        /* The stock confirmer (lane 36's measurement): cores busy from pg_stat_kcache against this server's own
           hour-of-week routine — the same surge statement as the capacity anomaly, available where there is no
           Performance Insights. See the summary above for why it joined. */
        Sibling(PgTargetFactKeys.AnomalyCpuBurn, "CPU-burn anomaly co-fired — cores busy ran far above this server's own routine for the hour, so the surge is burning real CPU");
        amplifiers.Add(new()
        {
            Description = "Instance CPU at or past the capacity warning bar (Aurora, percent of the configured ceiling) — the surge is consuming real capacity, not just moving a counter",
            /* unmeasured: LoadAnomalyCoFireBoost (derived) and CpuCapacityWarningPercent (the fleet ladder, repeated) — see their declarations. */
            Boost = LoadAnomalyCoFireBoost,
            Predicate = facts =>
                facts.TryGetValue(PgTargetFactKeys.CpuPercent, out var cpu)
                && cpu.Metadata.GetValueOrDefault(CpuCapacityMeasuredKey) >= 1.0
                && cpu.Value >= CpuCapacityWarningPercent,
        });
        return amplifiers;
    }
}
