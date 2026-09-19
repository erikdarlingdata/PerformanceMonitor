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
/// <c>pg_config</c> — the <c>CONFIG_PG_*</c> setting checks (lane 2 fills the two knobs and the convention /
/// meta checks; lanes 3, 4 and 6 fill the keys their families own). Pattern: <c>FactScorer.ScoreConfigFact</c>
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

    /// <summary>
    /// 0.4 when the setting is at (or below) its shipped default and 0 otherwise — a convention check (D5).
    /// Context keys (<c>checkpoint_timeout</c>, <c>wal_compression</c>, <c>max_connections</c>,
    /// <c>superuser_reserved_connections</c>, the registry major) score 0 here and are read by the advice
    /// and by lane 3's saturation arm. <c>work_mem</c> / <c>maintenance_work_mem</c> / <c>autovacuum</c> are
    /// lanes 6 and 4's arms (evidence-gated and the 0.9-band posture respectively) and fall to the default 0
    /// until those lanes fill them, so the fact exists as context and roots nothing.
    /// </summary>
    private static partial double ScoreConfigFact(Fact fact)
    {
        switch (fact.Key)
        {
            /* engine-defined: the initdb line, SharedBuffersInitdbDefaultMb. Value is megabytes. */
            case PgTargetFactKeys.ConfigSharedBuffers:
                return fact.Value <= SharedBuffersInitdbDefaultMb ? ConfigAdvisoryBase : 0.0;

            /* engine-defined: the shipped default, MaxWalSizeDefaultMb. Value is megabytes. */
            case PgTargetFactKeys.ConfigMaxWalSize:
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

            default:
                return 0.0;
        }
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
