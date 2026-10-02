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
/// <c>pg_write</c> — checkpoint / WAL pressure (lane 2). <c>PG_CHECKPOINT_PRESSURE</c> grades the
/// requested-vs-timed checkpoint ratio over the window; the bars are rates, never absolute totals (A7).
///
/// <para><b>Lineage.</b> The SIGNAL is engine-defined — a requested checkpoint is PostgreSQL's own report that
/// WAL reached <c>max_wal_size</c> before <c>checkpoint_timeout</c> elapsed — but the SHARE at which that
/// becomes a finding is not a line the engine draws, so the ramp below is unmeasured and the fact carries
/// <c>threshold_lineage = 0</c>. The one engine-defined line in the neighbourhood, <c>checkpoint_warning</c>
/// (30 s: the engine logs when requested checkpoints come closer together than this), is a spacing the
/// one-minute counters cannot resolve, so it is not borrowed as a bar; the expected-timed count the
/// collector stamps is the engine-defined sentence the advice states instead.</para>
///
/// <para><b>Aurora (lane 15 — #3691 calibration §A4).</b> A pressure fact stamped <c>not_applicable</c> by the
/// collector (Aurora storage owns checkpointing; the counters are synthetic — sixty timed an hour and a
/// requested share of 0 on every one of fifty measured clusters) scores 0 before any bar is consulted and
/// takes no lineage stamp: there is nothing to grade, so there is no bar whose lineage the fact should
/// disclose. Base 0 is what makes the rest follow without a second gate anywhere: the graph's edges read
/// <c>BaseSeverity &gt; 0</c> on this fact, the knob's co-fire amplifier reads the same, and
/// <c>InferenceEngine</c> roots nothing at 0 — so the checkpoint → <c>max_wal_size</c> story cannot form on
/// Aurora even from planted requested-dominant counters (pinned).</para>
///
/// <para><b><c>PG_WAL_VOLUME_SHIFT</c> is context, graded only through its anomaly (lane 15, design §3.11).</b>
/// WAL volume is a server-relative quantity — the same 9 MiB/s is routine on one server and a deployment gone
/// wrong on another — so this file draws NO bar for it: the fact scores 0 here always and carries
/// <c>threshold_lineage = 1</c> from the collector (no bar was chosen to have a lineage). The grading is
/// <c>ANOMALY_PG_WAL_VOLUME</c>'s — the window's peak against the server's own hour-of-week bucket through the
/// shared deviation ramp — and that anomaly is what the checkpoint fact's trigger amplifier below reads, since
/// the anomaly, not the context fact, is the thing that can have FIRED. The <c>unavailable</c> shape (Aurora,
/// PG &lt; 14) scores 0 by the same rule.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* unmeasured: chosen, not measured — calibrate against pg_write_stats before the next release. Three is
       the smallest count at which a share has any resolution (one requested of two is 50 %, one of three is
       33 %); below it a single manual CHECKPOINT or base backup would read as total pressure. */
    public const double CheckpointShareMinimumCheckpoints = 3;

    /* unmeasured: chosen, not measured — calibrate against pg_write_stats before the next release. 0.5 is the
       majority line (requested checkpoints outnumber timed ones: WAL volume, not the clock, is deciding);
       0.9 is near-total (the timer almost never gets to fire because WAL fills max_wal_size first). */
    public const double CheckpointRequestedShareConcerning = 0.5;
    public const double CheckpointRequestedShareCritical = 0.9;

    /// <summary>
    /// The requested share graded through the shared formula: 0 below <see cref="CheckpointShareMinimumCheckpoints"/>
    /// total checkpoints, 0 below the majority line (the fact has not FIRED — see the body), 0.5 at it, 1.0 at
    /// near-total. <c>PG_WAL_VOLUME_SHIFT</c> is a v2
    /// fact and scores 0 here until its lane defines it; any other <c>pg_write</c> key is 0.
    /// </summary>
    private static partial double ScoreWriteFact(Fact fact)
    {
        if (fact.Key != PgTargetFactKeys.CheckpointPressure) return 0.0;

        /* Lane 15 (#3691 §A4): not_applicable on Aurora — nothing to grade, no lineage to stamp (class summary). */
        if (fact.Metadata.GetValueOrDefault("not_applicable") > 0) return 0.0;

        /* unmeasured: the minimum-count gate, CheckpointShareMinimumCheckpoints (see the constant). */
        var total = fact.Metadata.GetValueOrDefault("checkpoints_total");
        if (total < CheckpointShareMinimumCheckpoints) return 0.0;

        /* unmeasured: the share ramp, CheckpointRequestedShareConcerning / Critical (see the constants); the
           fact says so for get_analysis_facts. BELOW the concerning line the fact scores 0, not the shared
           formula's fraction: BaseSeverity > 0 is the co-fire predicate that lifts CONFIG_PG_MAX_WAL_SIZE to the
           incident line (D5), and one nightly bulk load's 7 % requested share must not arm a knob on every
           server that ships with the default — "fired" has to mean "at least concerning". */
        fact.Metadata["threshold_lineage"] = 0;
        if (fact.Value < CheckpointRequestedShareConcerning) return 0.0;
        return FactScorer.ApplyThresholdFormula(fact.Value, CheckpointRequestedShareConcerning, CheckpointRequestedShareCritical);
    }

    /* unmeasured: chosen, not measured — the boost that lifts the 0.4 advisory to exactly the 0.5 incident line
       (0.4 × 1.25 = 0.5), so a knob at its default crosses into a finding precisely when, and only when, the
       engine's own exhaustion signal co-fires (D5). Shared by the buffer arm for the same reason. */
    public const double KnobCoFireBoost = 0.25;

    /* unmeasured: chosen, not measured — calibrate against pg_write_stats before the next release. The
       pressure fact's corroborations: the knob at its default names the cause (0.3), the WAL-volume anomaly
       (lane 15) names the trigger (0.2). Neither alone lifts a 0.5 base to the 1.5 notify line — corroboration,
       not a page. */
    public const double CheckpointCauseBoost = 0.3;
    public const double CheckpointTriggerBoost = 0.2;

    /// <summary>
    /// The write chain's Layer-2 amplifiers. <c>CONFIG_PG_MAX_WAL_SIZE</c> is lifted to the incident line by
    /// <c>PG_CHECKPOINT_PRESSURE</c> firing (<c>BaseSeverity &gt; 0</c>, never mere presence — the collector
    /// emits the fact whenever checkpoints ran, and a quiet server's 0 %-requested fact must not arm the
    /// knob). <c>PG_CHECKPOINT_PRESSURE</c> is corroborated by the knob at its default and by
    /// <c>ANOMALY_PG_WAL_VOLUME</c> having fired (lane 15): the anomaly is the graded instrument behind the
    /// context fact <c>PG_WAL_VOLUME_SHIFT</c>, and the <c>anomaly</c> source is in <c>FactScorer.ScoreAll</c>'s
    /// lookup, so its <c>BaseSeverity &gt; 0</c> is readable here exactly as a regular fact's. Never on Aurora: the
    /// pressure fact there is base 0 (<c>not_applicable</c>), so the amplifier pass skips it entirely, and the
    /// detector emits no WAL anomaly where WAL is not reported. <c>PG_WAL_VOLUME_SHIFT</c> itself has no
    /// amplifier here (base 0 — amplifiers never run on it).
    /// </summary>
    private static partial List<AmplifierDefinition> WriteAmplifiers(string key)
    {
        switch (key)
        {
            case PgTargetFactKeys.ConfigMaxWalSize:
                return
                [
                    new AmplifierDefinition
                    {
                        Description = "PG_CHECKPOINT_PRESSURE co-fires — requested checkpoints dominate: WAL is reaching max_wal_size before checkpoint_timeout",
                        /* unmeasured: KnobCoFireBoost (see the constant) — 0.4 × 1.25 = 0.5, the D5 line. */
                        Boost = KnobCoFireBoost,
                        Predicate = facts => facts.TryGetValue(PgTargetFactKeys.CheckpointPressure, out var pressure) && pressure.BaseSeverity > 0,
                    },
                ];

            case PgTargetFactKeys.CheckpointPressure:
                return
                [
                    new AmplifierDefinition
                    {
                        Description = "max_wal_size at the shipped default — the checkpoint ceiling was never sized for this WAL rate",
                        /* unmeasured: CheckpointCauseBoost (see the constant). */
                        Boost = CheckpointCauseBoost,
                        Predicate = facts => facts.TryGetValue(PgTargetFactKeys.ConfigMaxWalSize, out var knob) && knob.BaseSeverity > 0,
                    },
                    new AmplifierDefinition
                    {
                        Description = "ANOMALY_PG_WAL_VOLUME fired — WAL volume moved against this server's own hour-of-week baseline this window: the leading edge of the pressure",
                        /* unmeasured: CheckpointTriggerBoost (see the constant). */
                        Boost = CheckpointTriggerBoost,
                        Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AnomalyWalVolume, out var shift) && shift.BaseSeverity > 0,
                    },
                ];

            default:
                return [];
        }
    }
}
