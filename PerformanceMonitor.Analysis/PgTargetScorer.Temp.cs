/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_temp</c> — temp-file spill and its <c>work_mem</c> co-fire (lane 6 of #3542, design §3.9).
///
/// <para><b>What the spill fact is in v1.</b> A SPOT fact: spilled bytes per second of observed time, graded
/// against an ABSOLUTE floor. The design's trigger is baseline-relative — a z against the database's own
/// 168-bucket temp-bytes-rate baseline, with the absolute floor only so a genuinely tiny spill never fires —
/// and a temp-rate z is not among lane 9's v1 detectors, so this fact is the floor alone. Below the floor the fact
/// scores 0, not the formula's fraction (the <c>GradeArm</c> shape the buffer
/// composite uses): "fired" has to mean "at least the floor", because <c>BaseSeverity &gt; 0</c> is what arms
/// <c>work_mem</c>, and a trickle of temp files on every server that ever sorted would otherwise arm the knob
/// on the whole fleet.</para>
///
/// <para><b>Lineage.</b> Both bars are <b>measured</b> (#3691 calibration, 2026-09-19): the per-server
/// distribution of <c>temp_bytes</c> per second over <c>pg_database_stats</c>, 14 days × 50 Aurora PostgreSQL
/// clusters of the dogfood fleet, pre-bucketed to 5-minute rates. The bars were chosen before the read and
/// the read validated them where they stood — the constants below carry the percentile each sits at, and the
/// facts this arm grades carry <c>threshold_lineage = 1</c>. The quantity is engine-neutral, but the population
/// is Aurora; the stock-PostgreSQL population has not been measured. No SQL Server constant is reused: the SQL
/// Server pass grades tempdb by version store and file usage, which are different quantities.</para>
///
/// <para><b>D5, made structural.</b> <c>CONFIG_PG_WORK_MEM</c> is evidence-gated: it is NOT in
/// <see cref="PgTargetFactKeys.ConfigAdvisoryRoots"/>, its base is 0 unless the collector stamped spill
/// evidence onto it (<see cref="WorkMemSpillBytesPerSecKey"/> — see <c>PgTargetFactCollector.Database.cs</c>
/// for why the stamp is the only mechanism: the shared scorer grades a base one fact at a time and never
/// amplifies a base of 0), and with the stamp at or above the SAME floor the spill fact is graded on it
/// roots at <see cref="ConfigAdvisoryBase"/> and is lifted to exactly the 0.5 incident line by the spill
/// co-fire amplifier (<see cref="KnobCoFireBoost"/>, 0.4 × 1.25 — the shape <c>shared_buffers</c> and
/// <c>max_wal_size</c> take). No spill fact, or a spill below the floor: 0, and the knob roots nothing.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* ── metadata keys the collector stamps and the advice reads (this family) ── */

    public const string TempSpillBytesKey = "temp_bytes";
    public const string TempSpillFilesKey = "temp_files";
    public const string TempSpillBytesPerSecKey = "temp_bytes_per_sec";
    public const string TempSpillFilesPerSecKey = "temp_files_per_sec";
    public const string TempSpillTopDatabaseShareKey = "top_database_share";
    /// <summary><c>work_mem</c> in bytes, read off the <c>CONFIG_PG_WORK_MEM</c> fact at collection; absent
    /// when the config snapshot has not been collected.</summary>
    public const string TempSpillWorkMemBytesKey = "work_mem_bytes";
    /// <summary><c>max_connections</c>, read off its context fact at collection — the multiplier in the
    /// overcommit arithmetic the advice states; absent when not collected.</summary>
    public const string TempSpillMaxConnectionsKey = "max_connections";
    /// <summary>The spill evidence stamped ONTO the <c>CONFIG_PG_WORK_MEM</c> fact (bytes per observed second)
    /// — the one channel through which a spill reaches the knob's base severity.</summary>
    public const string WorkMemSpillBytesPerSecKey = "temp_spill_bytes_per_sec";

    /* ── metadata keys shared by every fact the one pg_database_stats read emits (Database.cs + this file) ── */

    public const string CounterDatabasesKey = "databases";
    public const string CounterSampleCountKey = "sample_count";
    /// <summary>How many consecutive-sample differences were taken across every series — what makes a zero a
    /// measurement rather than the arithmetic of an empty set.</summary>
    public const string CounterIntervalsKey = "intervals";
    /// <summary>Explicit resets seen (<c>stats_reset</c> moved between two samples).</summary>
    public const string CounterStatsResetCountKey = "stats_reset_count";
    /// <summary>Implicit resets seen (a counter below its predecessor) — the crash-restart case.</summary>
    public const string CounterRewindCountKey = "counter_rewind_count";
    public const string CounterObservedMsKey = "observed_ms";

    /* measured: ≈ p99.7 of 5-minute temp_bytes-per-second buckets over 14 days × 50 Aurora PostgreSQL clusters of
       the dogfood fleet, 2026-09-19 (pg_database_stats). Per-server p50 ranged 0 – 1.06 MB/s (median 46 KB/s);
       per-server p99 median 58 KB/s, fleet p90 313 KB/s, max 2.06 MB/s. The share of buckets at or above this
       floor was 0 on the median cluster and 0.27 % at the fleet p90 — and 61.8 % on one chronic spiller, where a
       permanent finding is the correct one. Measured on Aurora; the stock-PostgreSQL population is not yet
       measured. One mebibyte per second of observed time is ~3.5 GB of temp-file writes per observed hour: a
       sustained sort/hash working set exceeding work_mem, not one report's cold sort. Below it the fact is 0 and
       arms nothing. */
    public const double TempSpillConcerningBytesPerSec = 1024 * 1024;

    /* measured: inside the empty interval of the same 14 days × 50 Aurora PostgreSQL clusters of the dogfood
       fleet, 2026-09-19 — the share of 5-minute buckets at or above 10 MiB/s was 0 on 49 of 50 clusters and
       0.42 % on the worst (the chronic spiller above), so the line sits above the fleet's routine maximum.
       Measured on Aurora; the stock-PostgreSQL population is not yet measured. Ten mebibytes per second (~35 GB
       per observed hour) is the critical line: temp-file I/O at that rate competes with the data files for the
       same storage, and a temp_file_limit that would contain it is unusual. */
    public const double TempSpillCriticalBytesPerSec = 10 * 1024 * 1024;

    /// <summary>Whether a spill rate is at or above the floor — the ONE predicate both the spill fact's base and
    /// the knob's stamped base are graded on, so the two cannot disagree about whether the evidence exists.</summary>
    /* measured: the floor is TempSpillConcerningBytesPerSec (see the constant for the 2026-09-19 lineage). */
    private static bool SpillClearsFloor(double bytesPerSec) => bytesPerSec >= TempSpillConcerningBytesPerSec;

    /// <summary>
    /// The spill fact's base: 0 below the floor, the shared formula from the floor to the critical line
    /// above it. Stamps <c>threshold_lineage = 1</c> on every fact it grades — including the ones it grades to
    /// 0 — so <c>get_analysis_facts</c> shows both bars are fleet-measured (0 would mean at least one chosen
    /// bar decided; the stamp is on every fact this arm grades so the reader never infers it from absence). Any other
    /// <c>pg_temp</c> key is 0.
    /// </summary>
    private static partial double ScoreTempFact(Fact fact)
    {
        if (fact.Key != PgTargetFactKeys.TempSpill) return 0.0;

        fact.Metadata["threshold_lineage"] = 1;
        /* measured: TempSpillConcerningBytesPerSec / TempSpillCriticalBytesPerSec (see the constants, 2026-09-19). */
        return SpillClearsFloor(fact.Value)
            ? FactScorer.ApplyThresholdFormula(fact.Value, TempSpillConcerningBytesPerSec, TempSpillCriticalBytesPerSec)
            : 0.0;
    }

    /// <summary>
    /// <c>CONFIG_PG_WORK_MEM</c>'s base — the evidence-gated arm <c>ScoreConfigFact</c> delegates to (D5):
    /// <see cref="ConfigAdvisoryBase"/> when the collector stamped spill evidence at or above the floor onto the
    /// fact, 0 otherwise. The fact's <see cref="Fact.Value"/> (megabytes) is NOT graded: there is no right
    /// <c>work_mem</c> without the workload, and the paper's directional-wrongness example is exactly a memory
    /// knob raised on a convention. <c>threshold_lineage = 1</c> (the floor is the spill fact's measured one) is
    /// stamped only when the floor was consulted — an un-stamped knob is context and makes no claim.
    /// </summary>
    private static double ScoreConfigWorkMem(Fact fact)
    {
        if (!fact.Metadata.TryGetValue(WorkMemSpillBytesPerSecKey, out var spillBytesPerSec))
            return 0.0;

        fact.Metadata["threshold_lineage"] = 1;
        /* measured: the same TempSpillConcerningBytesPerSec floor the spill fact is graded on (see the constant). */
        return SpillClearsFloor(spillBytesPerSec) ? ConfigAdvisoryBase : 0.0;
    }

    /* engine-defined: work_mem = 4MB is the compiled boot_val (4096 kB) and initdb does not write it, so on an
       untouched install source = 'default'. At or below it means nobody sized the per-sort budget for this host;
       the advice says "the shipped default" off this line, in megabytes as the config fact carries it. */
    public const double WorkMemBootValMb = 4;

    /* unmeasured: chosen, not measured — calibrate against pg_database_stats and pg_statement_stats before the
       next release. The two corroborations of a spill: the knob co-fired (the budget these files exceeded is known
       for this host, and the story has its leaf); a top statement of the window wrote temp blocks (the story has
       its offender). Neither alone lifts a 0.5 base to the 1.5 notify line. */
    public const double TempCauseBoost = 0.3;

    /// <summary>
    /// The temp chain's Layer-2 amplifiers. <c>CONFIG_PG_WORK_MEM</c> is lifted from the advisory base to the
    /// incident line by <c>PG_TEMP_SPILL</c> firing (<c>BaseSeverity &gt; 0</c> — the floor, not presence); the
    /// spill is corroborated by the knob having co-fired and by a <c>PG_BAD_ACTOR_*</c> of the window that wrote
    /// temp blocks. Every predicate reads a sibling's <see cref="Fact.BaseSeverity"/> or metadata, never its
    /// amplified <see cref="Fact.Severity"/> — the shared scorer assigns amplified severities one fact at a
    /// time, so a predicate on <c>Severity</c> would depend on emission order (lane 4's finding).
    ///
    /// <para><b>Why the knob co-fire is an amplifier on the SPILL too, and not only the other way.</b> The
    /// inference engine roots stories in severity order and breaks ties by emission order, and the config
    /// family is emitted before this one. A spill exactly at the floor is 0.5 and the co-fired knob is exactly
    /// 0.5 (0.4 × 1.25); left equal, the knob roots first, the spill cannot reach it (consumed), and one
    /// condition becomes two cards. With this boost the spill is at least 0.65 whenever the knob fired, so the
    /// spill leads, the walk is spill → knob, and the knob is the leaf D5 says it is. Found by executing the
    /// pins, not by reading.</para>
    /// </summary>
    private static partial List<AmplifierDefinition> TempAmplifiers(string key)
    {
        switch (key)
        {
            case PgTargetFactKeys.ConfigWorkMem:
                return
                [
                    new AmplifierDefinition
                    {
                        Description = "PG_TEMP_SPILL co-fires — sorts and hashes are exceeding work_mem and writing temp files at or above the floor",
                        /* unmeasured: KnobCoFireBoost (see PgTargetScorer.Write.cs) — 0.4 × 1.25 = 0.5, the D5 line. */
                        Boost = KnobCoFireBoost,
                        Predicate = facts => facts.TryGetValue(PgTargetFactKeys.TempSpill, out var spill) && spill.BaseSeverity > 0,
                    },
                ];

            case PgTargetFactKeys.TempSpill:
                return
                [
                    new AmplifierDefinition
                    {
                        Description = "CONFIG_PG_WORK_MEM co-fired — the per-sort budget these temp files exceeded is known for this host, and the knob is this story's leaf",
                        /* unmeasured: TempCauseBoost (see the constant). */
                        Boost = TempCauseBoost,
                        Predicate = facts => facts.TryGetValue(PgTargetFactKeys.ConfigWorkMem, out var knob) && knob.BaseSeverity > 0,
                    },
                    new AmplifierDefinition
                    {
                        Description = "A top statement of the window (PG_BAD_ACTOR_*) wrote temp blocks — the spill has a named offender",
                        /* unmeasured: TempCauseBoost (see the constant). */
                        Boost = TempCauseBoost,
                        Predicate = facts => facts.Values.Any(f =>
                            f.Key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal)
                            && f.Metadata.GetValueOrDefault("temp_blks_written") > 0),
                    },
                ];

            default:
                return [];
        }
    }
}
