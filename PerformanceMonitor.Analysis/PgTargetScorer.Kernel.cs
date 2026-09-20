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
/// <c>pg_kernel</c> — the kernel-time family (filled by lane 28 of #3691): <c>PG_CPU_BURN_CORES</c>, user-plus-system-
/// plus-plan CPU seconds per WALL second from the reset-aware <c>pg_kernel_stats</c> (<c>pg_stat_kcache</c>)
/// differences summed across statements — cores busy, the self-hosted CPU PROXY that stands where the Aurora
/// capacity percent does not exist — and <c>PG_CPU_DECOMPOSITION</c>, kernel CPU time against the wait profile over
/// the window (burning versus waiting). The extension's availability is read from <c>pg_extension_availability</c>,
/// never inferred from rows.
///
/// <para><b><c>PG_CPU_BURN_CORES</c> is context, graded only through its anomaly.</b> No core count is collected
/// (<c>pg_settings</c> has none; <c>max_parallel_workers</c> is not cores), so "4.2 cores busy" has no percentage and
/// no absolute bar this file could honestly draw: the same 4 cores is idle on a 64-core host and saturation on a
/// 4-core one. The fact therefore scores 0 here ALWAYS — the <c>PG_WAL_VOLUME_SHIFT</c> rule — and carries
/// <c>threshold_lineage = 1</c> from the collector (no bar was chosen to have a lineage). The grade is
/// <c>ANOMALY_PG_CPU_BURN</c>'s: the window's peak AND mean cores busy against this server's own hour-of-week bucket
/// through the shared deviation ramp (<c>IsDeviationScoredAnomalyKey</c>), whose floor and fallback are unmeasured and
/// say so. A server whose routine is 2 cores busy and today burns 6 is the finding; "6 of how many" is unknowable here
/// and the advice says so. The <c>unavailable</c> shape (the extension not installed) scores 0 by the same rule.</para>
///
/// <para><b><c>PG_CPU_DECOMPOSITION</c> is context that becomes a finding on a co-fire.</b> A burning-versus-waiting
/// split is always worth reading and never, alone, a problem — so it roots at the informational base and nothing
/// walks to it; it crosses the incident line only when one side DOMINATES and that side's own instrument fired in the
/// same window: compute-bound (burn share at or past <see cref="CpuDecompositionDominantShare"/> AND the CPU-burn
/// anomaly fired) or wait-bound (wait share at or past the same line AND a <c>PG_WAIT_*</c> fact is itself a finding).
/// Both amplifiers read the sibling's verdict (<c>BaseSeverity &gt; 0</c>), never a bar of their own. Every number
/// below is unmeasured: Aurora does not ship <c>pg_stat_kcache</c>, so the 2026-09-19 fleet calibration (fifty
/// clusters) had NO population for this family, and the calibrating read — the per-server distribution of
/// <c>burn_share</c> over <c>pg_kernel_stats</c> × the wait source, four-hour windows, on a stock population — is
/// named here for the release that monitors one. Gates are shares and rates, never absolute-ms totals.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 28 of #3691 — the marker stays, as v1's did. */

    /* ── Metadata keys the kernel collector stamps and the advice / amplifiers / edges read (one spelling). ── */

    /// <summary>Cores busy over the window: Σ CPU ms ÷ the kernel source's own rated ms (the fact's Value).</summary>
    public const string KernelCoresBusyKey = "cores_busy";
    /// <summary>The largest per-collection cores-busy figure in the window — the anomaly's window statistic.</summary>
    public const string KernelCoresBusyPeakKey = "cores_busy_peak";
    /// <summary>The per-collection mean — the #3653 pair gate's second statistic.</summary>
    public const string KernelCoresBusyMeanKey = "cores_busy_mean";
    /// <summary>user ÷ (user + system) execution CPU.</summary>
    public const string KernelUserShareKey = "user_share";
    /// <summary>Plan CPU (<c>plan_user_time + plan_system_time</c>) as a share of all CPU counted.</summary>
    public const string KernelPlanCpuShareKey = "plan_cpu_share";
    /// <summary>The <c>query_id</c> that burned the most CPU in the window — the CPU bad actor, a different ordering
    /// from lane 7's exec-time one.</summary>
    public const string KernelTopQueryIdKey = "top_query_id";
    /// <summary>That statement's share of the window's CPU.</summary>
    public const string KernelTopQueryShareKey = "top_query_share";
    /// <summary>The kernel source's own rated time in ms — the cores-busy denominator.</summary>
    public const string KernelObservedMsKey = "kernel_observed_ms";
    /// <summary>Decomposition: cores busy ÷ (cores busy + backends waiting).</summary>
    public const string KernelBurnShareKey = "burn_share";
    /// <summary>Decomposition: backends waiting ÷ (cores busy + backends waiting).</summary>
    public const string KernelWaitShareKey = "wait_share";
    /// <summary>Decomposition: the IO wait type's backends waiting over the same denominator.</summary>
    public const string KernelIoWaitShareKey = "io_wait_share";
    /// <summary>Decomposition: all waiting (CPU excluded) over the wait source's own observed time — backend-
    /// equivalents parked.</summary>
    public const string KernelBackendsWaitingKey = "backends_waiting";
    /// <summary>Decomposition: 1 when the wait side is stock's sampled estimate (<c>pg_wait_sampling</c>), 0 when
    /// it is Aurora's measured deltas — the advice says which grade the split rests on.</summary>
    public const string KernelWaitIsSampledKey = "wait_is_sampled";

    /* unmeasured: chosen, not measured — calibrate against the per-server burn_share distribution over
       pg_kernel_stats × the wait source (four-hour windows) on a stock population before the next release that
       monitors one; the measured fleet is Aurora and has no pg_stat_kcache. 0.4 is the shared advisory base
       (D5 — the line a convention reading roots at), below the 0.5 incident line: a decomposition is context. */
    public const double CpuDecompositionInformational = 0.4;

    /* unmeasured: chosen, not measured — same calibrating read as above. Four fifths of backend time on one
       side is "the box is out of CPU" or "the backends are parked"; below it the two are mixed and neither
       instrument's advice is the whole story. */
    public const double CpuDecompositionDominantShare = 0.8;

    /* unmeasured: chosen, not measured — the boost that lifts the 0.4 informational base to 0.6 (0.4 × 1.5),
       past the 0.5 incident line and well short of the 1.5 notify line: a dominant side whose own instrument
       fired is a finding to read, not a page — the page, when one is due, is the instrument's. */
    public const double CpuDecompositionCoFireBoost = 0.5;

    /// <summary>
    /// <c>PG_CPU_BURN_CORES</c>: 0 always (context; graded by its anomaly — class summary).
    /// <c>PG_CPU_DECOMPOSITION</c>: the informational base whenever the collector emitted it (it is emitted only
    /// when both sides are known), with <c>threshold_lineage = 0</c> stamped — the share line its amplifiers read is
    /// unmeasured. Any other <c>pg_kernel</c> key is 0.
    /// </summary>
    private static partial double ScoreKernelFact(Fact fact)
    {
        if (fact.Key != PgTargetFactKeys.CpuDecomposition) return 0.0;

        /* unmeasured: CpuDecompositionInformational and the dominant-share line the amplifiers below read (see the
           constants); the fact says so for get_analysis_facts. */
        fact.Metadata["threshold_lineage"] = 0;
        return CpuDecompositionInformational;
    }

    /// <summary>
    /// The kernel family's Layer-2 amplifiers — <c>PG_CPU_DECOMPOSITION</c>'s two co-fires (class summary):
    /// compute-bound when its own <c>burn_share</c> is at or past the dominant line AND <c>ANOMALY_PG_CPU_BURN</c>
    /// fired (the <c>anomaly</c> source is in <c>FactScorer.ScoreAll</c>'s lookup, so its <c>BaseSeverity &gt; 0</c> is
    /// readable here exactly as a regular fact's — the write family's trigger amplifier is the precedent);
    /// wait-bound when its <c>wait_share</c> is at or past the same line AND a <c>PG_WAIT_*</c> fact is itself a
    /// finding (a PostgreSQL wait scores 0 below its concerning bar, so "fired" means "a finding"). The two are
    /// exclusive by arithmetic (the shares sum to one and the line is above a half), so the fact reaches 0.6 and no
    /// higher. <c>PG_CPU_BURN_CORES</c> has no amplifier: base 0, amplifiers never run on it.
    /// </summary>
    private static partial List<AmplifierDefinition> KernelAmplifiers(string key)
    {
        if (key != PgTargetFactKeys.CpuDecomposition) return [];

        return
        [
            new AmplifierDefinition
            {
                Description = "Compute-bound: at least four fifths of backend time was burning CPU, and ANOMALY_PG_CPU_BURN fired — cores busy ran above this server's own hour-of-week routine",
                /* unmeasured: CpuDecompositionCoFireBoost (see the constant) — 0.4 × 1.5 = 0.6. */
                Boost = CpuDecompositionCoFireBoost,
                Predicate = facts => IsDominant(facts, KernelBurnShareKey)
                    && facts.TryGetValue(PgTargetFactKeys.AnomalyCpuBurn, out var burn) && burn.BaseSeverity > 0,
            },
            new AmplifierDefinition
            {
                Description = "Wait-bound: at least four fifths of backend time was waiting, and a named PostgreSQL wait crossed its own threshold in the same window",
                /* unmeasured: CpuDecompositionCoFireBoost (see the constant). */
                Boost = CpuDecompositionCoFireBoost,
                Predicate = facts => IsDominant(facts, KernelWaitShareKey) && AnyWaitFired(facts),
            },
        ];
    }

    /// <summary>Whether the pass's <c>PG_CPU_DECOMPOSITION</c> carries <paramref name="shareKey"/> at or past
    /// <see cref="CpuDecompositionDominantShare"/> — read off the fact in the lookup, so the edge predicates in
    /// <c>PgTargetRelationshipGraph.Kernel.cs</c> and the amplifiers above apply one line.</summary>
    internal static bool IsDominant(IReadOnlyDictionary<string, Fact> facts, string shareKey) =>
        facts.TryGetValue(PgTargetFactKeys.CpuDecomposition, out var split)
        /* unmeasured: CpuDecompositionDominantShare (see the constant). */
        && split.Metadata.GetValueOrDefault(shareKey) >= CpuDecompositionDominantShare;

    /// <summary>Whether any <c>PG_WAIT_*</c> fact in the lookup is a finding (<c>BaseSeverity &gt; 0</c>) — the wait
    /// profile anomaly's named-standout arm, applied to the decomposition.</summary>
    internal static bool AnyWaitFired(IReadOnlyDictionary<string, Fact> facts)
    {
        foreach (var (factKey, fact) in facts)
        {
            if (factKey.StartsWith(PgTargetFactKeys.WaitKeyPrefix, StringComparison.Ordinal) && fact.BaseSeverity > 0)
                return true;
        }
        return false;
    }
}
