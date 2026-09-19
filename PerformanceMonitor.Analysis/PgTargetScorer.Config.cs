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
/// <c>pg_config</c> — the <c>CONFIG_PG_*</c> setting checks. One SOURCE, four owning families, because the
/// config snapshot read emits every key and the base-severity seam receives one fact at a time: owner: knobs
/// (lane 2) — the two knobs and the convention / meta checks; owner: sessions (lane 3) — the connection-ceiling
/// context keys; owner: vacuum (lane 4) — <c>autovacuum</c> and <c>maintenance_work_mem</c>; owner: temp
/// (lane 6) — <c>work_mem</c>. Each family's arm is labelled with its owner below, and an arm whose grading is
/// more than a line delegates to the owner's partial (<c>ScoreConfigWorkMem</c> in <c>PgTargetScorer.Temp.cs</c>)
/// so the bar and its lineage live beside the evidence it reads. Pattern: <c>FactScorer.ScoreConfigFact</c>
/// — 0.4 ONLY when the setting is bad, so a convention check roots an advisory card and never an incident
/// (D5). <see cref="PgTargetFactKeys.ServerMajorVersion"/> is context and stays 0. Every bar carries its
/// lineage marker (see the class summary in <c>PgTargetScorer.cs</c>).
///
/// <para><b>Every bar in this file is engine-defined</b> — the value PostgreSQL itself ships, taken from
/// <c>postgresql.conf.sample</c> / <c>initdb</c> for the version line this product supports — and the check
/// is "at (or below) that shipped value", which is the one config statement that needs no fleet measurement
/// to be true: the number is the engine's, and "nobody sized this" is what being at it means. No bar here is
/// a judgment about the RIGHT value; the right value is host- and workload-specific and the advice says so
/// (the counter-objective rule). <c>threshold_lineage</c> is therefore NOT stamped 0 on these facts — that
/// stamp marks an unmeasured bar, and these are not chosen numbers.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>The advisory base every bad convention check roots at — <c>FactScorer.ScoreConfigFact</c>'s
    /// 0.4, by reference in spirit: below the 0.5 incident line so a standing default never pages, above zero so
    /// <c>InferenceEngine</c>'s advisory-root bypass shows it on a quiet server (D5).</summary>
    public const double ConfigAdvisoryBase = 0.4;

    /* engine-defined: shared_buffers = 128MB is the line initdb writes into every new cluster's postgresql.conf
       (src/bin/initdb/initdb.c test_config_settings, falling back to the 8 MB boot_val only where the platform
       refuses the shared segment). At or below it means nobody sized the cache for this host. */
    public const double SharedBuffersInitdbDefaultMb = 128;

    /* engine-defined: max_wal_size = 1GB is the shipped default (postgresql.conf.sample; boot_val 1024 MB since
       9.5, when checkpoint_segments was replaced). At or below it means the checkpoint ceiling was never sized
       against this server's WAL rate. */
    public const double MaxWalSizeDefaultMb = 1024;

    /* engine-defined: effective_cache_size = 4GB is the compiled boot_val (524288 blocks × 8 kB); initdb does
       not write it, so on an untouched install source = 'default'. A planner assumption about how much of the
       working set the OS and shared caches hold, which no host of any size ships tuned. */
    public const double EffectiveCacheSizeDefaultMb = 4096;

    /* engine-defined: random_page_cost = 4.0 is the compiled boot_val, the 1990s spinning-disk ratio against
       seq_page_cost = 1.0; the documentation itself names it as the value to lower on storage where a random
       read is not four times a sequential one. */
    public const double RandomPageCostDefault = 4.0;

    /* engine-defined / posture (owner: vacuum): autovacuum = off is the one server-wide setting whose value alone
       is a fault, not a convention — with the launcher stopped nothing reclaims dead tuples and nothing advances
       relfrozenxid, so the wraparound wall is reached by arithmetic. Design §3.1 places it in the 0.9 band: above
       the 0.5 incident line on its own evidence (the setting IS the evidence, the way pg_posture's fsync = off
       is), below 1.0 because the backlog co-fire (VacuumConfigCoFireAmplifiers, +0.5) is what says the damage is
       measured rather than pending. The fact's Value is 0/1 as the collector writes it. */
    public const double AutovacuumOffPostureSeverity = 0.9;

    /// <summary>
    /// The evidence stamp the vacuum collector writes onto the <c>CONFIG_PG_MAINT_WORK_MEM</c> fact when a
    /// backlog fact exists: the worst table's dead-or-insert tuples as a multiple of its OWN autovacuum trigger
    /// line (the backlog fact's <see cref="BacklogRatioKey"/>). The knob's base is graded from this, one fact at
    /// a time — the D5 shape <c>work_mem</c> uses with <see cref="WorkMemSpillBytesPerSecKey"/>. Absent when no
    /// backlog fact was emitted, and then the knob scores 0 and is context.
    /// </summary>
    public const string MaintWorkMemBacklogRatioKey = "autovacuum_backlog_ratio";

    /// <summary>
    /// 0.4 when the setting is at (or below) its shipped default and 0 otherwise — a convention check (D5).
    /// Context keys (<c>checkpoint_timeout</c>, <c>wal_compression</c>, <c>max_connections</c>,
    /// <c>superuser_reserved_connections</c>, <c>reserved_connections</c>, the registry major) score 0 here and
    /// are read by the advice and by the sessions family's saturation arm. <c>work_mem</c> is the temp family's
    /// arm — evidence-gated (D5), delegated to <c>ScoreConfigWorkMem</c> in <c>PgTargetScorer.Temp.cs</c>, which
    /// grades the spill evidence the collector stamped onto the fact and is 0 without it.
    /// <c>maintenance_work_mem</c> is the vacuum family's evidence-gated twin of it (<see cref="ScoreConfigMaintWorkMem"/>:
    /// 0 without the backlog stamp, the advisory base with it, lifted past the incident line only by the backlog
    /// co-fire); <c>autovacuum</c> is the vacuum family's posture arm (<see cref="AutovacuumOffPostureSeverity"/>).
    /// Both were inert until the between-waves pass: their source is <c>pg_config</c>, so this is the only
    /// switch that can give them a base, and <c>FactScorer.ScoreAll</c> skips amplifiers on a base of 0 — no
    /// co-fire could ever have lifted them.
    /// </summary>
    private static partial double ScoreConfigFact(Fact fact)
    {
        switch (fact.Key)
        {
            /* ── owner: knobs (lane 2) ── */

            /* engine-defined: the initdb line, SharedBuffersInitdbDefaultMb. Value is megabytes. */
            case PgTargetFactKeys.ConfigSharedBuffers:
                return fact.Value <= SharedBuffersInitdbDefaultMb ? ConfigAdvisoryBase : 0.0;

            /* engine-defined: the shipped default, MaxWalSizeDefaultMb. Value is megabytes. Lane 15 (#3691 §A4):
               0 when the collector stamped not_applicable — on aurora-postgres the engine does not consult
               max_wal_size (Aurora storage owns checkpointing), so a knob at the default is not "nobody sized
               this"; it is a value nothing reads. Base 0 means no advisory roots and the co-fire amplifier
               never runs (FactScorer skips amplifiers at base 0) — the suppression is one flag read, here. */
            case PgTargetFactKeys.ConfigMaxWalSize:
                if (fact.Metadata.GetValueOrDefault("not_applicable") > 0) return 0.0;
                return fact.Value <= MaxWalSizeDefaultMb ? ConfigAdvisoryBase : 0.0;

            /* engine-defined: the compiled boot_val, EffectiveCacheSizeDefaultMb. Exactly AT the default —
               an operator who set it lower on a small host made a decision; the check is "nobody set it". */
            case PgTargetFactKeys.ConfigEffectiveCacheSize:
                return fact.Value == EffectiveCacheSizeDefaultMb ? ConfigAdvisoryBase : 0.0;

            /* engine-defined: the compiled boot_val, RandomPageCostDefault. Exactly at it, same reasoning. */
            case PgTargetFactKeys.ConfigRandomPageCost:
                return fact.Value == RandomPageCostDefault ? ConfigAdvisoryBase : 0.0;

            /* engine-defined: track_io_timing = off is the boot_val, and off blinds blk_read_time /
               blk_write_time in pg_stat_statements and pg_stat_database — a monitoring-coverage advisory, the
               Query-Store-advisory analogue. Value is 0/1. */
            case PgTargetFactKeys.ConfigTrackIoTiming:
                return fact.Value == 0 ? ConfigAdvisoryBase : 0.0;

            /* ── owner: temp (lane 6) ── */

            /* D5, evidence-gated: the value is not graded; the stamped spill evidence is. The arm lives in
               PgTargetScorer.Temp.cs beside the floor it shares with PG_TEMP_SPILL. */
            case PgTargetFactKeys.ConfigWorkMem:
                return ScoreConfigWorkMem(fact);

            /* ── owner: vacuum (lane 4) ── */

            /* engine-defined / posture: AutovacuumOffPostureSeverity when autovacuum = off (Value 1), 0 when on. */
            case PgTargetFactKeys.ConfigAutovacuumOff:
                return fact.Value > 0 ? AutovacuumOffPostureSeverity : 0.0;

            /* D5, evidence-gated: the value is not graded; the stamped backlog evidence is. */
            case PgTargetFactKeys.ConfigMaintWorkMem:
                return ScoreConfigMaintWorkMem(fact);

            /* ── owner: sessions (lane 3): max_connections / superuser_reserved_connections / reserved_connections
               are context (base 0) and fall through; the ceiling they compose is graded on the saturation fact. ── */

            default:
                return 0.0;
        }
    }

    /// <summary>
    /// The D5 arm for <c>CONFIG_PG_MAINT_WORK_MEM</c>, the vacuum family's twin of <c>ScoreConfigWorkMem</c>: 0
    /// when the collector stamped no backlog evidence (no <c>PG_AUTOVACUUM_BACKLOG</c> fact this pass — the knob
    /// is context), the advisory base when the stamped backlog is AT OR PAST the table's own trigger line. That
    /// base is below the incident line and the key is not an advisory root, so alone it still roots nothing;
    /// <c>VacuumConfigCoFireAmplifiers</c>' +0.5 on the backlog having FIRED is what carries it to 0.6, and the
    /// vacuum chain's <c>PG_AUTOVACUUM_BACKLOG → CONFIG_PG_MAINT_WORK_MEM</c> edge then has a destination that
    /// fired. No <c>threshold_lineage = 0</c> stamp: the bar below is the engine's, not a chosen number.
    /// </summary>
    private static double ScoreConfigMaintWorkMem(Fact fact)
    {
        if (!fact.Metadata.TryGetValue(MaintWorkMemBacklogRatioKey, out var backlogRatio))
            return 0.0;

        /* engine-defined: 1.0 is the table's own autovacuum trigger line (autovacuum_vacuum_threshold +
           autovacuum_vacuum_scale_factor × reltuples, reloptions honoured), the same line the backlog fact's
           concerning bar sits on — the knob has evidence exactly when autovacuum itself would have acted. */
        return backlogRatio >= 1.0 ? ConfigAdvisoryBase : 0.0;
    }

    /// <summary>
    /// No amplifier for a PURE convention key (<c>effective_cache_size</c>, <c>random_page_cost</c>,
    /// <c>track_io_timing</c>, the context keys): D5 says a config value alone roots an advisory and is
    /// amplified only by a workload co-fire, and these have none by design — the paper's directional-wrongness
    /// example is exactly an I/O-cost-class rule. The two knobs never reach this arm: the dispatcher routes
    /// <c>CONFIG_PG_SHARED_BUFFERS</c> to <c>BufferAmplifiers</c> and <c>CONFIG_PG_MAX_WAL_SIZE</c> to
    /// <c>WriteAmplifiers</c>, where their co-fires live beside the facts that supply them.
    /// </summary>
    private static partial List<AmplifierDefinition> ConfigAmplifiers(string key) => [];
}
