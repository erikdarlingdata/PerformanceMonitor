/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The <c>ANOMALY_PG_*</c> arm (lane 9 fills the ramps and the co-fire table; the MEMBERSHIP predicates are
/// vocabulary and are declared here by the plumbing lane, because they follow from each detector's SHAPE
/// rather than from any bar).
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
/// unchanged. The ratio family (<see cref="PgTargetFactKeys.AnomalyDeadlockRate"/>) needs its own ramp
/// because the SQL Server one recognises <c>ANOMALY_BLOCKING_SPIKE</c> / <c>ANOMALY_DEADLOCK_SPIKE</c> by
/// literal prefix. <see cref="PgTargetFactKeys.AnomalyWaitProfile"/> is neither: lane 9 decides whether it
/// grades on <c>modified_z</c> like the SQL Server profile fact or on its own ratio, and registers it in
/// whichever predicate that is.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>The PostgreSQL z-score families — graded off <c>deviation_sigma</c> against
    /// <c>fire_threshold</c> by the shared ramp, and eligible for the extremity escape from the cap.</summary>
    public static bool IsDeviationScoredAnomalyKey(string? key) =>
        key is PgTargetFactKeys.AnomalyTps
            or PgTargetFactKeys.AnomalySessionSpike
            or PgTargetFactKeys.AnomalyCpuSpike;

    /// <summary>The PostgreSQL ratio-vs-own-baseline families — <see cref="ScoreRatioAnomaly"/> grades these.</summary>
    public static bool IsPgRatioAnomalyKey(string? key) =>
        key is PgTargetFactKeys.AnomalyDeadlockRate;

    /// <summary>
    /// Base severity for a ratio-family PostgreSQL anomaly from its <c>ratio</c> metadata.
    /// <para>/* filled by lane 9 — the ramp's floor and span are bars and carry their lineage marker; the
    /// SQL Server 3x → 0.5 / 10x → 1.0 shape is a METHOD to inherit, not a constant to reuse by value. */</para>
    /// </summary>
    public static double ScoreRatioAnomaly(Fact fact) => 0.0;

    /* filled by lane 9 — the PostgreSQL load-family co-fire arm: TPS / session / CPU siblings corroborate
       one another (+0.3 each, the SQL Server LoadAnomalyAmplifiers shape), confirmed by the MEASURED
       PG_CPU_PERCENT fact at a bar that carries its own lineage marker. On stock PostgreSQL there is no CPU
       series and the confirmer is absent — the advice must not imply CPU was checked (D6). The deadlock-rate
       anomaly folds into PG_DEADLOCK_RATE through the reconciler instead and has no arm. */
    private static partial List<AmplifierDefinition> AnomalyAmplifiers(string key) => [];
}
