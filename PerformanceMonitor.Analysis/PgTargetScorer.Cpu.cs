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
/// <c>pg_cpu</c> — instance CPU (lane 9 of #3542; Aurora / Performance Insights only). <c>PG_CPU_PERCENT</c> is the
/// measured confirmer the PostgreSQL load-family anomaly arm reads and the sibling lane 3's saturation arm asks
/// "did it fire"; on stock there is no CPU series, the fact is absent, and both arms are inert — the advice must not
/// imply CPU was checked (D6).
///
/// <para><b>What is graded (#3281).</b> Only a capacity reading: percent of the CONFIGURED ceiling
/// (<c>acu_utilization_percent</c>), which the collector puts in <see cref="Fact.Value"/> and marks with
/// <see cref="CpuCapacityMeasuredKey"/> = 1. A fact whose window carried no capacity sample holds the RAW
/// <c>cpu_percent</c> — percent of the capacity currently allocated, which on the serverless class reads 100
/// whenever one core is busy for a minute — and scores 0 here: "Unknown, never Healthy" is the fleet card's rule
/// for the same reading (#3271), and a grade on percent-of-allocated would be the exact defect #3281 removed from
/// the card. The number is still stated by the advice as "was a core pinned".</para>
///
/// <para><b>Self-gating below the warning line, the session family's shape.</b> <see cref="FactScorer.ApplyThresholdFormula"/>
/// grades any positive value below the concerning bar as a fraction of it, so a 40% instance would read 0.25 — and
/// lane 3's saturation amplifier and this lane's load confirmer both ask <c>BaseSeverity &gt; 0</c>. Zero below the
/// warning bar, 0.5 at it, 1.0 at the critical bar: a fired CPU fact always means the instance is genuinely near
/// its ceiling.</para>
///
/// <para><b>Lineage.</b> The two bars are the fleet card's own CPU ladder on the same quantity
/// (<c>ServerHealthThresholds.CpuWarningPercent</c> / <c>CpuCriticalPercent</c> — repeated here because this
/// assembly references nothing; <c>PgTargetAnomalyTests</c> pins each pair equal), so the card, the High CPU alert
/// and this pass cannot disagree about the colour of one minute. They are UNMEASURED all the same: that ladder is
/// stated against a quantity, not read off a fleet distribution, and every graded fact carries
/// <c>threshold_lineage = 0</c>; the calibrating read is the per-server p95 of <c>acu_utilization_percent</c> over
/// <c>pg_cpu_utilization</c>.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* ── the PG_CPU_PERCENT metadata vocabulary; the collector stamps, the scorer, amplifiers and advice read ── */

    /// <summary>1 when <see cref="Fact.Value"/> is percent of the configured ceiling; 0 when it is the raw reading.</summary>
    public const string CpuCapacityMeasuredKey = "capacity_measured";
    public const string CpuPeakPercentKey = "peak_cpu_percent";
    public const string CpuAvgPercentKey = "avg_cpu_percent";
    public const string CpuPeakCapacityPctKey = "peak_capacity_pct";
    public const string CpuAvgCapacityPctKey = "avg_capacity_pct";
    public const string CpuPeakCapacityAcuKey = "peak_capacity_acu";
    public const string CpuMaxConfiguredAcuKey = "max_configured_acu";
    public const string CpuSampleCountKey = "sample_count";
    public const string CpuCapacitySamplesKey = "capacity_samples";
    public const string CpuObservedMsKey = "observed_ms";
    public const string CpuPeakAgeSecondsKey = "peak_age_s";
    public const string CpuLatestAgeSecondsKey = "latest_age_s";

    /// <summary>
    /// Percent of the configured capacity ceiling at which the instance is CONCERNING (0.5) and CRITICAL (1.0), and
    /// — the warning bar — the line the load-family anomaly confirmer reads (<c>PgTargetScorer.Anomaly.cs</c>).
    /// unmeasured: the fleet card's CPU ladder repeated on the same quantity (see the class summary), chosen, not
    /// measured — calibrate against pg_cpu_utilization before the next release; the fact carries
    /// threshold_lineage = 0.
    /// </summary>
    public const double CpuCapacityWarningPercent = 80.0;
    public const double CpuCapacityCriticalPercent = 95.0;

    /// <summary>
    /// Layer-1 base severity for the <c>pg_cpu</c> source: the capacity percent graded between the two bars above
    /// and ZERO below the warning bar; a fact without a capacity reading is context (0) whatever its raw percent
    /// says. Stamps <c>threshold_lineage = 0</c> on every fact it sees, graded or not — the bar that decided is
    /// unmeasured either way.
    /// </summary>
    private static partial double ScoreCpuFact(Fact fact)
    {
        if (fact.Key != PgTargetFactKeys.CpuPercent) return 0.0;

        fact.Metadata["threshold_lineage"] = 0;
        if (fact.Metadata.GetValueOrDefault(CpuCapacityMeasuredKey) < 1 || fact.Value <= 0)
            return 0.0;

        /* unmeasured: CpuCapacityWarningPercent / CpuCapacityCriticalPercent (see the constants); below the warning
           bar the fact is context, never a fraction of the bar — see the class summary on self-gating. */
        if (fact.Value < CpuCapacityWarningPercent)
            return 0.0;

        return FactScorer.ApplyThresholdFormula(fact.Value, CpuCapacityWarningPercent, CpuCapacityCriticalPercent);
    }
}
