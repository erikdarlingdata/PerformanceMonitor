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
/// Layer-1 base severity and Layer-2 amplifiers for the PostgreSQL-target fact vocabulary
/// (<see cref="PgTargetFactKeys"/>) — the <c>pg_</c> arm <see cref="FactScorer"/> delegates to (#3542).
///
/// <para><b>Shape.</b> One dispatcher per concern here, routing by SOURCE to a partial method in its own
/// file (<c>PgTargetScorer.Config.cs</c>, <c>.Write.cs</c>, …), so that the content lanes that fill each
/// family never edit this file, each other's files, or <see cref="FactScorer"/> again. The formula is
/// shared — every arm grades through <see cref="FactScorer.ApplyThresholdFormula"/> onto the same 0–2
/// scale (D5: fleet views mix engines, so a PostgreSQL 1.0 must MEAN a SQL Server 1.0) — and the
/// amplifier boost / cap arithmetic runs unchanged in <see cref="FactScorer.ScoreAll"/>.</para>
///
/// <para><b>Threshold lineage (#3538 A5/A7 via #3605).</b> The SQL Server scorer's bars are inherited
/// constants; the PostgreSQL detectors inherit the METHOD, not the numbers. Every numeric bar a content
/// lane writes into a <c>PgTargetScorer*.cs</c> partial carries, within the six lines above it, exactly one
/// of three lineage markers — <c>measured</c> (pNN over N days × M servers of the dogfood PostgreSQL fleet,
/// dated), <c>engine-defined</c> (the bar IS the engine's own line, named), or <c>unmeasured</c> ("chosen,
/// not measured — calibrate against &lt;table&gt; before the next release", with the fact carrying
/// <c>threshold_lineage = 0</c>). <c>PgTargetThresholdLineageTests</c> scans for a bar without a marker.
/// No PostgreSQL bar may be a SQL Server constant reused by value, and no gate may be an absolute-ms total:
/// PostgreSQL gates are rates or fractions of OBSERVED time so they scale with <c>hours_back</c>.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>
    /// Layer-1 base severity for a fact whose <see cref="Fact.Source"/> is a PostgreSQL-target source.
    /// Routes by source to the family partial; an unknown <c>pg_</c> source scores 0 — the same silent
    /// floor the SQL Server switch's <c>_ =&gt; 0.0</c> arm gives an unrecognised source.
    /// </summary>
    public static double ScoreBase(Fact fact)
    {
        ArgumentNullException.ThrowIfNull(fact);
        return fact.Source switch
        {
            PgTargetSources.ConfigSource => ScoreConfigFact(fact),
            PgTargetSources.PostureSource => ScorePostureFact(fact),
            PgTargetSources.WriteSource => ScoreWriteFact(fact),
            PgTargetSources.BufferSource => ScoreBufferFact(fact),
            PgTargetSources.SessionsSource => ScoreSessionsFact(fact),
            PgTargetSources.VacuumSource => ScoreVacuumFact(fact),
            PgTargetSources.WaitsSource => ScoreWaitFact(fact),
            PgTargetSources.TempSource => ScoreTempFact(fact),
            PgTargetSources.QueriesSource => ScoreQueriesFact(fact),
            PgTargetSources.DatabaseSource => ScoreDatabaseFact(fact),
            PgTargetSources.CpuSource => ScoreCpuFact(fact),
            PgTargetSources.IoSource => ScoreIoFact(fact),
            PgTargetSources.ReplicationSource => ScoreReplicationFact(fact),
            PgTargetSources.BloatSource => ScoreBloatFact(fact),
            _ => 0.0,
        };
    }

    /// <summary>Whether <paramref name="key"/> is a PostgreSQL-target key (any of the three prefixes) — the
    /// predicate <see cref="FactScorer"/>'s amplifier switch routes on, ahead of its <c>ANOMALY_</c> arm so
    /// <c>ANOMALY_PG_*</c> lands here and not in the SQL Server anomaly table.</summary>
    public static bool IsPgKey(string? key) => PgTargetFactKeys.IsPgKey(key);

    /// <summary>
    /// Layer-2 amplifiers for a PostgreSQL-target key: <c>ANOMALY_PG_*</c> to the anomaly partial, every
    /// measured / config key to its family partial by prefix. Empty when no arm claims the key.
    /// </summary>
    internal static List<AmplifierDefinition> Amplifiers(string key)
    {
        if (PgTargetFactKeys.IsPgAnomalyKey(key))
            return AnomalyAmplifiers(key);

        return key switch
        {
            _ when key.StartsWith(PgTargetFactKeys.WaitKeyPrefix, StringComparison.Ordinal) => WaitAmplifiers(key),
            _ when key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal) => QueriesAmplifiers(key),
            PgTargetFactKeys.BufferCachePressure or PgTargetFactKeys.ConfigSharedBuffers => BufferAmplifiers(key),
            PgTargetFactKeys.CheckpointPressure or PgTargetFactKeys.ConfigMaxWalSize or PgTargetFactKeys.WalVolumeShift => WriteAmplifiers(key),
            PgTargetFactKeys.ConnectionSaturation or PgTargetFactKeys.IdleInTransaction => SessionsAmplifiers(key),
            PgTargetFactKeys.AutovacuumBacklog or PgTargetFactKeys.WraparoundTrend or PgTargetFactKeys.XminHold
                or PgTargetFactKeys.ConfigAutovacuumOff or PgTargetFactKeys.ConfigMaintWorkMem => VacuumAmplifiers(key),
            PgTargetFactKeys.TempSpill or PgTargetFactKeys.ConfigWorkMem => TempAmplifiers(key),
            /* v2 (#3691): one arm per new family, each to its own partial's stub. */
            PgTargetFactKeys.IoReadLatencyMs or PgTargetFactKeys.IoWriteLatencyMs => IoAmplifiers(key),
            PgTargetFactKeys.ReplicationLag or PgTargetFactKeys.SlotRetention or PgTargetFactKeys.SlotXmin => ReplicationAmplifiers(key),
            PgTargetFactKeys.BloatTrend or PgTargetFactKeys.IndexBloatTrend => BloatAmplifiers(key),
            _ when key.StartsWith(PgTargetFactKeys.ConfigPrefix, StringComparison.Ordinal) => ConfigAmplifiers(key),
            _ => [],
        };
    }

    /* ── Family partials, one per file. Each is stubbed to 0.0 / [] here and filled by the lane named
       in its file; the dispatchers above are the whole contract between the families and the shared
       scorer, so a lane's edit never leaves its own file. ── */

    private static partial double ScoreConfigFact(Fact fact);
    private static partial List<AmplifierDefinition> ConfigAmplifiers(string key);

    private static partial double ScorePostureFact(Fact fact);

    private static partial double ScoreWriteFact(Fact fact);
    private static partial List<AmplifierDefinition> WriteAmplifiers(string key);

    private static partial double ScoreBufferFact(Fact fact);
    private static partial List<AmplifierDefinition> BufferAmplifiers(string key);

    private static partial double ScoreSessionsFact(Fact fact);
    private static partial List<AmplifierDefinition> SessionsAmplifiers(string key);

    private static partial double ScoreVacuumFact(Fact fact);
    private static partial List<AmplifierDefinition> VacuumAmplifiers(string key);

    private static partial double ScoreWaitFact(Fact fact);
    private static partial List<AmplifierDefinition> WaitAmplifiers(string key);

    private static partial double ScoreTempFact(Fact fact);
    private static partial List<AmplifierDefinition> TempAmplifiers(string key);

    private static partial double ScoreQueriesFact(Fact fact);
    private static partial List<AmplifierDefinition> QueriesAmplifiers(string key);

    private static partial double ScoreDatabaseFact(Fact fact);

    private static partial double ScoreCpuFact(Fact fact);

    private static partial List<AmplifierDefinition> AnomalyAmplifiers(string key);

    /* v2 (#3691) families — stubbed in their own files, filled by lanes 11 / 12 / 13. */

    private static partial double ScoreIoFact(Fact fact);
    private static partial List<AmplifierDefinition> IoAmplifiers(string key);

    private static partial double ScoreReplicationFact(Fact fact);
    private static partial List<AmplifierDefinition> ReplicationAmplifiers(string key);

    private static partial double ScoreBloatFact(Fact fact);
    private static partial List<AmplifierDefinition> BloatAmplifiers(string key);
}
