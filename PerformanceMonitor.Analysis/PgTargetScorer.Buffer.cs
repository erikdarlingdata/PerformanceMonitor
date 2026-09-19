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
/// <c>pg_buffer</c> — the buffer-cache pressure composite (lane 2): hit ratio + evictions (PG 16+, metadata
/// <c>evictions_tracked</c>) + bgwriter, with the hit-ratio arm suppressed on Aurora (<c>hit_ratio_suppressed</c>).
///
/// <para><b>Composition rule.</b> Three arms, each graded through the shared formula on its OWN dimensionless
/// ratio once it reaches its concerning line (below it an arm is 0 — see <c>GradeArm</c>), combined by MAX
/// rather than by sum: the arms measure one condition (a working set larger than the
/// cache) three ways, and adding them would count it three times — the D2 collapse the fact exists for. An
/// arm whose honesty flag says it is unavailable on this flavour or major, or whose denominator the config
/// snapshot did not supply, is skipped — not scored 0 and averaged in, which would dilute the arms that DO
/// have evidence. <c>arms_graded</c> is stamped so the advice can say how many legs the finding stands on.</para>
///
/// <para><b>Lineage.</b> Every bar here is unmeasured and the fact carries <c>threshold_lineage = 0</c>. The
/// denominators are engine-defined (block size, <c>shared_buffers</c>, <c>bgwriter_delay</c>) and the
/// collector computes them; the lines drawn over the resulting ratios are chosen and say so. No SQL Server
/// constant is reused: the SQL Server pass has no buffer-cache fact at all.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* unmeasured: chosen, not measured — calibrate against pg_database_stats before the next release. A miss
       share on a trickle of blocks is noise (one cold read of a ten-block table is a 100 % miss); fifty block
       requests a second over the observed window is the floor below which the hit-ratio arm is not graded. */
    public const double BufferHitArmMinimumBlocksPerSec = 50;

    /* unmeasured: chosen, not measured — calibrate against pg_database_stats before the next release. One block
       request in ten leaving shared_buffers is the concerning line; one in two is critical. Community lore
       says "99 % hit ratio"; that is a reporting convention, not a measurement, and is not borrowed. */
    public const double BufferMissShareConcerning = 0.10;
    public const double BufferMissShareCritical = 0.50;

    /* unmeasured: chosen, not measured — calibrate against pg_io_stats before the next release. Evictions are
       graded as CACHE TURNOVER — evictions × block size / shared_buffers per observed hour, how many times the
       whole cache was replaced. Replacing it once an hour is the concerning line; ten times an hour is critical
       (a 128 MB cache turning over every six minutes). */
    public const double BufferTurnoverPerHourConcerning = 1.0;
    public const double BufferTurnoverPerHourCritical = 10.0;

    /* unmeasured: chosen, not measured — calibrate against pg_write_stats before the next release. The bgwriter
       arm is the share of its wake-ups (observed seconds / bgwriter_delay) on which it hit bgwriter_lru_maxpages
       and stopped: one in ten is concerning, one in two critical — half its rounds it left dirty buffers for
       backends to write themselves. */
    public const double BufferBgwriterHaltShareConcerning = 0.10;
    public const double BufferBgwriterHaltShareCritical = 0.50;

    /// <summary>
    /// The composite: MAX over the arms that are graded. The hit-ratio arm is skipped when
    /// <c>hit_ratio_suppressed</c> (Aurora) or below the block-rate floor; the eviction arm when
    /// <c>evictions_tracked = 0</c> (below PG 16) or no <c>cache_turnovers_per_hour</c> was stamped
    /// (<c>shared_buffers</c> unknown); the bgwriter arm when <c>bgwriter_tracked = 0</c> (below PG 14) or no
    /// <c>bgwriter_halt_share</c> was stamped (<c>bgwriter_delay</c> unknown). Any other <c>pg_buffer</c> key
    /// is 0.
    /// </summary>
    private static partial double ScoreBufferFact(Fact fact)
    {
        if (fact.Key != PgTargetFactKeys.BufferCachePressure) return 0.0;

        var score = 0.0;
        var armsGraded = 0;

        /* unmeasured: the hit-ratio arm — BufferHitArmMinimumBlocksPerSec floor, BufferMissShareConcerning /
           Critical ramp (see the constants). */
        if (fact.Metadata.GetValueOrDefault("hit_ratio_suppressed") == 0
            && fact.Metadata.GetValueOrDefault("block_requests_per_sec") >= BufferHitArmMinimumBlocksPerSec)
        {
            score = Math.Max(score, GradeArm(fact.Metadata.GetValueOrDefault("miss_share"), BufferMissShareConcerning, BufferMissShareCritical));
            armsGraded++;
        }

        /* unmeasured: the eviction arm — BufferTurnoverPerHourConcerning / Critical ramp (see the constants). */
        if (fact.Metadata.GetValueOrDefault("evictions_tracked") > 0
            && fact.Metadata.TryGetValue("cache_turnovers_per_hour", out var turnovers))
        {
            score = Math.Max(score, GradeArm(turnovers, BufferTurnoverPerHourConcerning, BufferTurnoverPerHourCritical));
            armsGraded++;
        }

        /* unmeasured: the bgwriter arm — BufferBgwriterHaltShareConcerning / Critical ramp (see the constants). */
        if (fact.Metadata.GetValueOrDefault("bgwriter_tracked") > 0
            && fact.Metadata.TryGetValue("bgwriter_halt_share", out var haltShare))
        {
            score = Math.Max(score, GradeArm(haltShare, BufferBgwriterHaltShareConcerning, BufferBgwriterHaltShareCritical));
            armsGraded++;
        }

        fact.Metadata["arms_graded"] = armsGraded;
        fact.Metadata["threshold_lineage"] = 0;
        return score;
    }

    /// <summary>
    /// One arm through the shared formula, with the floor that makes the composite a CO-FIRE rather than a
    /// presence: BELOW its concerning line an arm contributes 0, not the formula's fraction. <c>BaseSeverity &gt; 0</c>
    /// is the predicate that lifts <c>CONFIG_PG_SHARED_BUFFERS</c> to the incident line (D5), and a healthy
    /// cache's 1 % miss share graded as 0.05 would arm the knob on every server that ships with the default —
    /// "fired" has to mean "at least one arm at its concerning line". The bars are the callers'; this is the
    /// shape.
    /// </summary>
    private static double GradeArm(double value, double concerning, double critical) =>
        /* unmeasured: the floor is the caller's concerning bar, whose lineage is on its constant. */
        value < concerning ? 0.0 : FactScorer.ApplyThresholdFormula(value, concerning, critical);

    /* unmeasured: chosen, not measured — calibrate against pg_database_stats before the next release. The
       composite's corroboration: the knob at its default names the cause (0.3). Alone it cannot lift a 0.5 base
       to the 1.5 notify line. */
    public const double BufferCauseBoost = 0.3;

    /// <summary>
    /// The memory chain's Layer-2 amplifiers. <c>CONFIG_PG_SHARED_BUFFERS</c> is lifted to the incident line
    /// by <c>PG_BUFFER_CACHE_PRESSURE</c> firing (<c>BaseSeverity &gt; 0</c> — the composite is emitted whenever
    /// blocks moved, and a healthy cache's fact must not arm the knob); the composite is corroborated by the
    /// knob at its default. Lane 5 adds the <c>IO:DataFileRead</c> confirmer here.
    /// </summary>
    private static partial List<AmplifierDefinition> BufferAmplifiers(string key)
    {
        switch (key)
        {
            case PgTargetFactKeys.ConfigSharedBuffers:
                return
                [
                    new AmplifierDefinition
                    {
                        Description = "PG_BUFFER_CACHE_PRESSURE co-fires — the cache is measurably short: misses, evictions or a halting bgwriter",
                        /* unmeasured: KnobCoFireBoost (see PgTargetScorer.Write.cs) — 0.4 × 1.25 = 0.5, the D5 line. */
                        Boost = KnobCoFireBoost,
                        Predicate = facts => facts.TryGetValue(PgTargetFactKeys.BufferCachePressure, out var pressure) && pressure.BaseSeverity > 0,
                    },
                ];

            case PgTargetFactKeys.BufferCachePressure:
                return
                [
                    new AmplifierDefinition
                    {
                        Description = "shared_buffers at the initdb default — the cache was never sized for this host",
                        /* unmeasured: BufferCauseBoost (see the constant). */
                        Boost = BufferCauseBoost,
                        Predicate = facts => facts.TryGetValue(PgTargetFactKeys.ConfigSharedBuffers, out var knob) && knob.BaseSeverity > 0,
                    },
                ];

            default:
                return [];
        }
    }
}
