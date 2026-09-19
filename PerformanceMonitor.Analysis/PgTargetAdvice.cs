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
/// Advice for the PostgreSQL-target vocabulary (<see cref="PgTargetFactKeys"/>) — the <c>pg_</c> arm
/// <see cref="FactAdvice"/> delegates to (#3542), in both of its shapes: <see cref="Compose"/> is the
/// value-stated block built from the FULL scored fact set at analysis time (the <c>ComposeConfigMaxdop</c>
/// pattern — state the current setting and the measured evidence, never generic folklore), and
/// <see cref="Static"/> is the read-time fallback for a finding persisted without frozen story text.
///
/// <para>One partial file per family, each filled by the lane that owns its facts, so the content lanes
/// never edit this file or <see cref="FactAdvice"/> again. The dispatch is by key: exact constants for the
/// fixed keys, prefix for the two dynamic families (<see cref="PgTargetFactKeys.WaitKeyPrefix"/>,
/// <see cref="PgTargetFactKeys.BadActorKeyPrefix"/>) and for the <c>CONFIG_PG_</c> / <c>ANOMALY_PG_</c>
/// classes.</para>
///
/// <para>Two house rules every block inherits. A sampled wait fact's prose says "estimated from sampling"
/// and never that the server "spent" time waiting (the per-backend-sample estimate is time summed over
/// tasks). A posture block (lane 8) contains none of "faster", "performance", "throughput", "speed" — a
/// durability setting is never argued as a speed trade (D6); <c>PgTargetPostureIsolationTests</c> pins the
/// words.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    /// <summary>Whether <paramref name="rootFactKey"/> belongs to the PostgreSQL-target vocabulary.</summary>
    public static bool IsPgKey(string? rootFactKey) => PgTargetFactKeys.IsPgKey(rootFactKey);

    /// <summary>
    /// Value-stated advice for a PostgreSQL-target root key from the full scored fact set, or <c>null</c>
    /// when no family claims the key (the shared caller then falls back to <see cref="Static"/>).
    /// </summary>
    public static AdviceBlock? Compose(string rootFactKey, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        ArgumentNullException.ThrowIfNull(rootFactKey);
        ArgumentNullException.ThrowIfNull(factsByKey);

        if (PgTargetFactKeys.IsPgAnomalyKey(rootFactKey))
            return ComposeAnomaly(rootFactKey, factsByKey);
        if (rootFactKey.StartsWith(PgTargetFactKeys.WaitKeyPrefix, StringComparison.Ordinal))
            return ComposeWait(rootFactKey, factsByKey);
        if (rootFactKey.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))
            return ComposeQueries(rootFactKey, factsByKey);

        return rootFactKey switch
        {
            PgTargetFactKeys.PostureFsync or PgTargetFactKeys.PostureFullPageWrites or PgTargetFactKeys.PostureSynchronousCommit
                => ComposePosture(rootFactKey, factsByKey),
            PgTargetFactKeys.CheckpointPressure or PgTargetFactKeys.WalVolumeShift or PgTargetFactKeys.ConfigMaxWalSize
                => ComposeWrite(rootFactKey, factsByKey),
            PgTargetFactKeys.BufferCachePressure or PgTargetFactKeys.ConfigSharedBuffers
                => ComposeBuffer(rootFactKey, factsByKey),
            PgTargetFactKeys.ConnectionSaturation or PgTargetFactKeys.MonitoringPermissions or PgTargetFactKeys.IdleInTransaction
                => ComposeSessions(rootFactKey, factsByKey),
            PgTargetFactKeys.AutovacuumBacklog or PgTargetFactKeys.WraparoundTrend or PgTargetFactKeys.XminHold
                or PgTargetFactKeys.ConfigAutovacuumOff or PgTargetFactKeys.ConfigMaintWorkMem
                => ComposeVacuum(rootFactKey, factsByKey),
            PgTargetFactKeys.TempSpill or PgTargetFactKeys.ConfigWorkMem
                => ComposeTemp(rootFactKey, factsByKey),
            PgTargetFactKeys.DeadlockRate or PgTargetFactKeys.Tps or PgTargetFactKeys.HitRatio
                => ComposeDatabase(rootFactKey, factsByKey),
            PgTargetFactKeys.CpuPercent
                => ComposeCpu(rootFactKey, factsByKey),
            /* v2 (#3691): the measured keys of the three new families, one arm each. Their anomalies
               (ANOMALY_PG_IO_LATENCY, ANOMALY_PG_REPLICATION_LAG, ANOMALY_PG_WAL_VOLUME) take the ANOMALY_PG_
               prefix arm above into ComposeAnomaly, as every PostgreSQL anomaly does. */
            PgTargetFactKeys.IoReadLatencyMs or PgTargetFactKeys.IoWriteLatencyMs
                => ComposeIo(rootFactKey, factsByKey),
            PgTargetFactKeys.ReplicationLag or PgTargetFactKeys.SlotRetention or PgTargetFactKeys.SlotXmin
                => ComposeReplication(rootFactKey, factsByKey),
            PgTargetFactKeys.BloatTrend or PgTargetFactKeys.IndexBloatTrend
                => ComposeBloat(rootFactKey, factsByKey),
            /* wave 3 (#3691, between waves): the blocking family's three measured keys; its anomaly takes the prefix
               arm above into ComposeAnomaly, whose ANOMALY_PG_BLOCKING case delegates to ComposeBlockingAnomaly. */
            PgTargetFactKeys.BlockingChain or PgTargetFactKeys.LockWaitEvents or PgTargetFactKeys.LongRunningQuery
                => ComposeBlocking(rootFactKey, factsByKey),
            _ when rootFactKey.StartsWith(PgTargetFactKeys.ConfigPrefix, StringComparison.Ordinal)
                => ComposeConfig(rootFactKey, factsByKey),
            _ => null,
        };
    }

    /// <summary>
    /// The static (value-free) block for a PostgreSQL-target key, or <c>null</c> — the read-time fallback for
    /// a finding row whose <c>StoryText</c> carries no frozen advice. Composed from the same family partials
    /// with an EMPTY fact set, so a family maintains one composer and the static block is what it says
    /// when it has no values to state.
    /// </summary>
    public static AdviceBlock? Static(string? key)
    {
        if (string.IsNullOrEmpty(key) || !IsPgKey(key))
            return null;
        return Compose(key, s_noFacts);
    }

    private static readonly IReadOnlyDictionary<string, Fact> s_noFacts = new Dictionary<string, Fact>(0, StringComparer.Ordinal);

    /* ── Family partials, one per file, each stubbed to null here and filled by the lane named in its file. ── */

    private static partial AdviceBlock? ComposeConfig(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposePosture(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeWrite(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeBuffer(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeSessions(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeVacuum(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeWait(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeTemp(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeQueries(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeDatabase(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeCpu(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeAnomaly(string key, IReadOnlyDictionary<string, Fact> factsByKey);

    /* v2 (#3691) families — stubbed to null in their own files, filled by lanes 11 / 12 / 13. */
    private static partial AdviceBlock? ComposeIo(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeReplication(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeBloat(string key, IReadOnlyDictionary<string, Fact> factsByKey);

    /* wave 3 (#3691) — stubbed to null in PgTargetAdvice.Blocking.cs, filled by lane 17. The anomaly composer is
       declared here too so ComposeAnomaly's arm exists before the family does (lanes 12 and 15 each had to add their
       own arm to the anomaly partial; lane 17 does not). */
    private static partial AdviceBlock? ComposeBlocking(string key, IReadOnlyDictionary<string, Fact> factsByKey);
    private static partial AdviceBlock? ComposeBlockingAnomaly(IReadOnlyDictionary<string, Fact> factsByKey);
}
