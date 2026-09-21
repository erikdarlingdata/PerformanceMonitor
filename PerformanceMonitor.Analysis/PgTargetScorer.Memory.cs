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
/// <c>pg_memory</c> — the memory-composition family (filled by lane 32 of #3691, design §4b): does the configuration
/// FIT the host? Two facts. <c>CONFIG_PG_MEMORY_OVERCOMMIT</c> is the configured worst case —
/// <c>shared_buffers + max_connections × work_mem × (1 + max_parallel_workers_per_gather) + autovacuum_max_workers ×
/// maintenance_work_mem + wal_buffers</c>, every term read from the latest <c>pg_server_config</c> snapshot through
/// <c>PgSettingValue</c> — as a RATIO of the host's <c>memory_total_bytes</c>, which rides <c>pg_cpu_utilization</c>'s
/// row since V136 (the window's MINIMUM total: on Aurora Serverless v2 the instance scales, and a scaled-down minute is
/// where an overcommitted configuration bites; on a provisioned class the minimum is the constant). <c>PG_HOST_MEMORY_PRESSURE</c>
/// is the OS's reclaimable share — <c>(memory_free_bytes + memory_cached_bytes) / memory_total_bytes</c> — at its worst
/// SUSTAINED point of the window, from the same rows.
///
/// <para><b>The composition check is a CONVENTION reading (D5).</b> <c>work_mem</c> is a per-sort / per-hash-node budget,
/// not a per-connection allocation, so the product is a CEILING the workload may never approach; the fact therefore roots an
/// ADVISORY card at <see cref="ConfigAdvisoryBase"/> (0.4) whenever the sum exceeds the box and reaches the 0.5 incident
/// line ONLY when a workload co-fire says the arithmetic is being felt: <c>PG_HOST_MEMORY_PRESSURE</c> fired (the host is
/// measurably short) or <c>PG_TEMP_SPILL</c> fired (<c>work_mem</c> pressure is real — sorts are already spilling). Both
/// are amplifiers here, predicated on the co-firing fact's <see cref="Fact.BaseSeverity"/>, never on a bar of their own.
/// The 2× band is a third amplifier predicated on the SAME co-fire, so the arithmetic alone never lifts the card
/// however large the ratio (pinned).</para>
///
/// <para><b>Lineage.</b> The 1.0 line is engine-defined — the arithmetic is PostgreSQL's own documented per-backend
/// allocation model, and "the sum exceeds the box" needs no fleet to be true; the <c>effective_cache_size</c>
/// plausibility line (a planner assumption larger than the machine is a lie to the planner) is engine-defined the same
/// way. Everything else here is UNMEASURED: the 2× critical band, the reclaimable-share bars, the sustain count and the
/// two boosts. The 2026-09-19 calibration ran before V136 landed the memory columns, so it read none; the coordinator's
/// next calibration batch reads <c>pg_cpu_utilization</c>'s six columns and flips these. <c>threshold_lineage</c> is
/// stamped accordingly: the pressure fact always 0; the overcommit fact 1 while only the engine's line decided it and
/// 0 once the chosen 2× band participated (the <c>work_mem</c> arm's "stamped only when consulted" shape).</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* ── the overcommit fact's metadata keys (the collector writes them, the advice reads them) ── */

    /// <summary>Each configured term in BYTES, as <c>PgSettingValue.ToBytes</c> normalised it from the snapshot.</summary>
    public const string MemorySharedBuffersBytesKey = "shared_buffers_bytes";
    public const string MemoryWorkMemBytesKey = "work_mem_bytes";
    public const string MemoryMaintWorkMemBytesKey = "maintenance_work_mem_bytes";
    /// <summary>Present only when <c>autovacuum_work_mem</c> is set (not the <c>-1</c> "inherit" sentinel); then it,
    /// not <c>maintenance_work_mem</c>, is the per-worker term — PostgreSQL's own rule for the autovacuum workers.</summary>
    public const string MemoryAutovacuumWorkMemBytesKey = "autovacuum_work_mem_bytes";
    public const string MemoryWalBuffersBytesKey = "wal_buffers_bytes";
    /// <summary>The unitless knobs the terms multiply by.</summary>
    public const string MemoryMaxConnectionsKey = "max_connections";
    public const string MemoryParallelWorkersPerGatherKey = "max_parallel_workers_per_gather";
    public const string MemoryAutovacuumMaxWorkersKey = "autovacuum_max_workers";
    /// <summary>The two composite terms in bytes: <c>max_connections × work_mem × (1 + max_parallel_workers_per_gather)</c>
    /// and <c>autovacuum_max_workers × (autovacuum_work_mem or maintenance_work_mem)</c>.</summary>
    public const string MemoryBackendTermBytesKey = "backend_term_bytes";
    public const string MemoryAutovacuumTermBytesKey = "autovacuum_term_bytes";
    /// <summary>The whole sum in bytes — the numerator of the ratio.</summary>
    public const string MemoryWorstCaseBytesKey = "configured_worst_case_bytes";
    /// <summary>The host's <c>memory_total_bytes</c> over the window's memory-carrying samples: the MINIMUM is the
    /// denominator (the tightest box the window saw), the maximum is stated so a Serverless reader sees the range.</summary>
    public const string MemoryTotalMinBytesKey = "memory_total_min_bytes";
    public const string MemoryTotalMaxBytesKey = "memory_total_max_bytes";
    /// <summary>1 when any sample in the window carried <c>configured_memory_bytes</c> (Serverless v2: capacity ACU × 2 GiB;
    /// NULL on a provisioned class), 0 otherwise; the minimum configured figure rides beside it when 1.</summary>
    public const string MemoryIsServerlessKey = "is_serverless";
    public const string MemoryConfiguredMinBytesKey = "configured_memory_min_bytes";
    /// <summary>The fact's value, repeated by name: the sum over the minimum total.</summary>
    public const string MemoryOvercommitRatioKey = "overcommit_ratio";
    /// <summary>1 when the ratio is at or past <see cref="OvercommitCriticalRatio"/> — stated, and read by the band amplifier.</summary>
    public const string MemoryOvercommitCriticalBandKey = "overcommit_critical_band";
    /// <summary>The planner's <c>effective_cache_size</c> in bytes and whether it exceeds the host's MINIMUM total — the
    /// plausibility line stated in the advice, never a fact of its own.</summary>
    public const string MemoryEffectiveCacheSizeBytesKey = "effective_cache_size_bytes";
    public const string MemoryEffectiveCacheExceedsTotalKey = "effective_cache_size_exceeds_total";
    /// <summary>How many <c>pg_cpu_utilization</c> rows the window had, and how many of them carried <c>memory_total_bytes</c>.</summary>
    public const string MemorySamplesKey = "samples";
    public const string MemorySamplesWithMemoryKey = "samples_with_memory";
    /// <summary>The config snapshot's age against the window's end (the config family's <c>snapshot_age_s</c>).</summary>
    public const string MemorySnapshotAgeSecondsKey = "snapshot_age_s";

    /* ── the pressure fact's metadata keys ── */

    /// <summary>The window's minimum, mean and SUSTAINED-minimum reclaimable share (<c>(free + cached) / total</c>); the
    /// sustained figure is the worst share held for <see cref="HostMemoryPressureSustainSamples"/> consecutive
    /// memory-carrying samples — the one the bars grade, so a single five-minute dip never pages.</summary>
    public const string HostMemoryMinReclaimableShareKey = "min_reclaimable_share";
    public const string HostMemoryMeanReclaimableShareKey = "mean_reclaimable_share";
    public const string HostMemorySustainedMinReclaimableShareKey = "sustained_min_reclaimable_share";
    /// <summary>The window's peak <c>memory_active_bytes / memory_total_bytes</c> — what the working set was using at its widest.</summary>
    public const string HostMemoryPeakActiveShareKey = "peak_active_share";
    /// <summary>The window's peak <c>memory_buffers_bytes / memory_total_bytes</c> — stated beside the reclaimable share, not
    /// folded into it (the design's share is free + cached; the calibration decides whether buffers join it).</summary>
    public const string HostMemoryPeakBuffersShareKey = "peak_buffers_share";
    /// <summary>The sustain count the collector applied (the scorer's constant, repeated onto the fact so the reader sees it).</summary>
    public const string HostMemorySustainSamplesKey = "sustain_samples";
    /// <summary>Metadata KEYS on the pressure fact's <c>unavailable</c> shape (the numeric-metadata idiom, one key per
    /// reason): the window had NO <c>pg_cpu_utilization</c> row at all — a stock PostgreSQL target, which has no OS memory
    /// source from inside the engine (V136's rule: absence is <c>unavailable</c>, never "no memory"); or the window had rows
    /// but fewer than half carried <c>memory_total_bytes</c> — rows written before V136, or a Performance Insights
    /// endpoint that does not publish <c>os.memory.*</c> (the ingestor's middle fallback step).</summary>
    public const string HostMemoryReasonNoSourceKey = "reason_no_host_memory_source";
    public const string HostMemoryReasonSparseKey = "reason_memory_columns_sparse";

    /* ── bars ── */

    /* engine-defined: 1.0 — the configured worst case equals the host's memory. The sum is PostgreSQL's own documented
       per-backend allocation model (shared_buffers once; work_mem per sort/hash node per backend, and per parallel
       worker; maintenance_work_mem or autovacuum_work_mem per autovacuum worker; wal_buffers once), and "the sum
       exceeds the box" is arithmetic, not a judgment. At or past it the configuration CAN exceed physical memory if
       every backend spills once — which is what the advisory says, no more. */
    public const double OvercommitRatioLine = 1.0;

    /* unmeasured: chosen, not measured — calibrate against pg_server_config × pg_cpu_utilization before the next
       release. Twice the box: the band the advice calls out as "more than double", and the third amplifier's line —
       predicated on the SAME workload co-fire as the others, so the arithmetic alone never lifts the card (D5). The
       2026-09-19 calibration ran before V136 landed the memory columns and read no ratio. */
    public const double OvercommitCriticalRatio = 2.0;

    /* unmeasured: chosen, not measured — calibrate against pg_cpu_utilization (memory_free_bytes + memory_cached_bytes
       over memory_total_bytes) before the next release. 10 % reclaimable is the warning line: below it the OS has
       little left to give a backend that spills, and the next work_mem allocation is the one that swaps or is refused.
       The 2026-09-19 calibration read no memory columns (V136 landed after it); the coordinator's next batch does. */
    public const double HostMemoryReclaimableWarningShare = 0.10;

    /* unmeasured: chosen, not measured — same read as the warning share. 3 % reclaimable is the critical line. */
    public const double HostMemoryReclaimableCriticalShare = 0.03;

    /* unmeasured: chosen, not measured — calibrate against pg_cpu_utilization before the next release. Three consecutive
       five-minute samples (a quarter of an hour) at or below the line: a single dip while a maintenance job ran is not
       pressure; a quarter-hour is. The collector reads this constant and computes the sustained minimum with it. */
    public const int HostMemoryPressureSustainSamples = 3;

    /* unmeasured: chosen, not measured — calibrate against pg_cpu_utilization before the next release. The 2× band's
       boost, added to a co-fire's KnobCoFireBoost: 0.4 × (1 + 0.25 + 0.25) = 0.6 — one notch past the line the co-fire
       alone reaches, because a sum at twice the box under measured pressure is a nearer fault than one at 1.1×. */
    public const double OvercommitCriticalBandBoost = 0.25;

    /* unmeasured: chosen, not measured — calibrate against pg_cpu_utilization before the next release. The pressure
       fact's corroboration: the arithmetic predicted the shortage (0.3, BufferCauseBoost's twin). Alone it cannot lift
       a 0.5 base to the 1.5 notify line. */
    public const double HostMemoryCauseBoost = 0.3;

    /// <summary>
    /// Layer-1 base for the two memory facts. <c>CONFIG_PG_MEMORY_OVERCOMMIT</c>: <see cref="ConfigAdvisoryBase"/> when the
    /// ratio is at or past <see cref="OvercommitRatioLine"/>, 0 otherwise — a convention check, never more than the
    /// advisory base on its own (D5). <c>PG_HOST_MEMORY_PRESSURE</c>: 0 below the warning line, the shared formula from
    /// the warning line to the critical line on the SHORTAGE (<c>1 − sustained reclaimable share</c>, so the formula's
    /// ascending scale reads the right way); the <c>unavailable</c> shape (no host-memory source, or the columns sparse)
    /// scores 0 and stamps its lineage like every other fact this arm touches. Any other <c>pg_memory</c> key is 0.
    /// </summary>
    /* filled by lane 32 of #3691 — the marker stays, as v1's did. */
    private static partial double ScoreMemoryFact(Fact fact)
    {
        switch (fact.Key)
        {
            case PgTargetFactKeys.ConfigMemoryOvercommit:
            {
                var ratio = fact.Metadata.GetValueOrDefault(MemoryOvercommitRatioKey, fact.Value);
                /* unmeasured: OvercommitCriticalRatio (see the constant) decides only the band flag; the base below is the
                   engine's line. The stamp says which decided: 1 while the arithmetic alone did, 0 once the chosen band did. */
                var critical = ratio >= OvercommitCriticalRatio;
                fact.Metadata[MemoryOvercommitCriticalBandKey] = critical ? 1 : 0;
                fact.Metadata["threshold_lineage"] = critical ? 0 : 1;
                /* engine-defined: OvercommitRatioLine (see the constant). */
                return ratio >= OvercommitRatioLine ? ConfigAdvisoryBase : 0.0;
            }

            case PgTargetFactKeys.HostMemoryPressure:
            {
                fact.Metadata["threshold_lineage"] = 0;
                if (fact.Metadata.GetValueOrDefault("unavailable") > 0) return 0.0;
                if (!fact.Metadata.TryGetValue(HostMemorySustainedMinReclaimableShareKey, out var sustained)) return 0.0;

                /* unmeasured: HostMemoryReclaimableWarningShare / HostMemoryReclaimableCriticalShare (see the constants),
                   graded as SHORTAGE so the shared ascending formula applies; 0 below the warning line — "fired" means
                   "at least at the warning line", the buffer composite's self-gating shape, so a healthy host's 40 %
                   reclaimable never arms the overcommit card. */
                var shortage = 1.0 - sustained;
                var concerning = 1.0 - HostMemoryReclaimableWarningShare;
                var critical = 1.0 - HostMemoryReclaimableCriticalShare;
                /* unmeasured: the floor is the warning share above, whose lineage is on its constant. */
                return shortage < concerning ? 0.0 : FactScorer.ApplyThresholdFormula(shortage, concerning, critical);
            }

            default:
                return 0.0;
        }
    }

    /// <summary>
    /// The host-memory chain's Layer-2 amplifiers. <c>CONFIG_PG_MEMORY_OVERCOMMIT</c> is lifted to the 0.5 line by either
    /// workload co-fire — <c>PG_HOST_MEMORY_PRESSURE</c> fired (<see cref="Fact.BaseSeverity"/> &gt; 0: the host is
    /// measurably short) or <c>PG_TEMP_SPILL</c> fired (sorts are spilling — <c>work_mem</c> pressure is real, lane 6's
    /// fact) — and one notch further by the 2× band WHEN a co-fire is present (the band alone lifts nothing: D5).
    /// <c>PG_HOST_MEMORY_PRESSURE</c> is corroborated by the arithmetic having predicted it. Every predicate reads a
    /// fact's verdict or its stated metadata, never a bar of its own.
    /// </summary>
    private static partial List<AmplifierDefinition> MemoryAmplifiers(string key)
    {
        switch (key)
        {
            case PgTargetFactKeys.ConfigMemoryOvercommit:
                return
                [
                    new AmplifierDefinition
                    {
                        Description = "PG_HOST_MEMORY_PRESSURE co-fires — the host's reclaimable memory is measurably short, so the configured worst case is being felt",
                        /* unmeasured: KnobCoFireBoost (see PgTargetScorer.Write.cs) — 0.4 × 1.25 = 0.5, the D5 line. */
                        Boost = KnobCoFireBoost,
                        Predicate = PressureFired,
                    },
                    new AmplifierDefinition
                    {
                        Description = "PG_TEMP_SPILL co-fires — sorts and hashes are already spilling past work_mem, so the per-backend term is real, not a ceiling",
                        /* unmeasured: KnobCoFireBoost (see PgTargetScorer.Write.cs) — 0.4 × 1.25 = 0.5, the D5 line. */
                        Boost = KnobCoFireBoost,
                        Predicate = SpillFired,
                    },
                    new AmplifierDefinition
                    {
                        Description = "The configured worst case is more than twice the host's memory, and a workload co-fire says it is being felt",
                        /* unmeasured: OvercommitCriticalBandBoost (see the constant); the band flag is the scorer's, off OvercommitCriticalRatio. */
                        Boost = OvercommitCriticalBandBoost,
                        Predicate = facts => (PressureFired(facts) || SpillFired(facts))
                            && facts.TryGetValue(PgTargetFactKeys.ConfigMemoryOvercommit, out var sum)
                            && sum.Metadata.GetValueOrDefault(MemoryOvercommitCriticalBandKey) > 0,
                    },
                ];

            case PgTargetFactKeys.HostMemoryPressure:
                return
                [
                    new AmplifierDefinition
                    {
                        Description = "CONFIG_PG_MEMORY_OVERCOMMIT fired — the configured worst case exceeds this host, and the arithmetic predicted the shortage",
                        /* unmeasured: HostMemoryCauseBoost (see the constant). */
                        Boost = HostMemoryCauseBoost,
                        Predicate = facts => facts.TryGetValue(PgTargetFactKeys.ConfigMemoryOvercommit, out var sum) && sum.BaseSeverity > 0,
                    },
                ];

            default:
                return [];
        }
    }

    private static bool PressureFired(Dictionary<string, Fact> facts)
    {
        return facts.TryGetValue(PgTargetFactKeys.HostMemoryPressure, out var pressure) && pressure.BaseSeverity > 0;
    }

    private static bool SpillFired(Dictionary<string, Fact> facts)
    {
        return facts.TryGetValue(PgTargetFactKeys.TempSpill, out var spill) && spill.BaseSeverity > 0;
    }
}
