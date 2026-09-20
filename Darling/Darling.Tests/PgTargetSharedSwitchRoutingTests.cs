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
using System.Reflection;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3542 plumbing: the PostgreSQL-target vocabulary (<see cref="PgTargetFactKeys"/> / <see cref="PgTargetSources"/>)
/// and the ONE delegating arm each shared switch grew for it — <see cref="FactScorer"/> (source arm, context
/// set, deviation-scored membership, ratio-anomaly first line, amplifier arm), <see cref="InferenceEngine"/>'s
/// advisory roots, <see cref="RelationshipGraph"/>'s protected seam, <see cref="FactAdvice"/>'s compose and
/// static arms, and <see cref="AnomalyIncidentReconciler"/>'s family lookup. After this PR the content lanes
/// never touch those files again, so these pins are the contract they build against: every assertion here is
/// about ROUTING — that a PostgreSQL key reaches the PostgreSQL arm and no SQL Server arm, and that the SQL
/// Server behaviour is byte-identical — not about any bar or any prose, which the lanes own.
///
/// <para>Behavioural where the surface allows it: a fact is constructed, scored through the real
/// <see cref="FactScorer.ScoreAll"/>, storied through the real <see cref="InferenceEngine"/>, reconciled
/// through the real reconciler. Source pins only for what reflection cannot see.</para>
/// </summary>
public sealed class PgTargetSharedSwitchRoutingTests
{
    /* ── the vocabulary ── */

    [Fact]
    public void EverySource_IsPgPrefixed_LowercaseSnakeCase_SortedOnce_AndNamedForTheRegistrySweep()
    {
        /* Eleven v1 sources (#3542) plus the three v2 families (#3691: pg_io, pg_replication, pg_bloat) plus the wave-3
           blocking family's source, declared with its stubs by the between-waves batch (pg_blocking), plus the three v3
           families' sources, declared with their stubs by the v3 plumbing (pg_plans, pg_kernel, pg_memory). */
        Assert.Equal(18, PgTargetSources.All.Count);
        Assert.Contains(PgTargetSources.PlansSource, PgTargetSources.All);
        Assert.Contains(PgTargetSources.KernelSource, PgTargetSources.All);
        Assert.Contains(PgTargetSources.MemorySource, PgTargetSources.All);
        Assert.Contains(PgTargetSources.IoSource, PgTargetSources.All);
        Assert.Contains(PgTargetSources.ReplicationSource, PgTargetSources.All);
        Assert.Contains(PgTargetSources.BloatSource, PgTargetSources.All);
        Assert.All(PgTargetSources.All, s => Assert.StartsWith(PgTargetSources.Prefix, s, StringComparison.Ordinal));
        Assert.All(PgTargetSources.All, s => Assert.Matches("^[a-z_]+$", s));
        Assert.Equal(PgTargetSources.All.Order(StringComparer.Ordinal).ToArray(), PgTargetSources.All.ToArray());
        Assert.Equal(PgTargetSources.All.Count, PgTargetSources.All.Distinct(StringComparer.Ordinal).Count());

        /* Every const's NAME ends in Source — the shape FactSourceRegistryTests' sweep reads a declared
           source constant by (WindowCoverage.FactSource is its precedent). A const renamed out of that shape
           would drop its source from the swept set and fail that pin against the registry; this says why. */
        var consts = typeof(PgTargetSources).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name != nameof(PgTargetSources.Prefix))
            .ToList();
        Assert.Equal(PgTargetSources.All.Count, consts.Count);
        Assert.All(consts, f => Assert.EndsWith("Source", f.Name, StringComparison.Ordinal));
        Assert.Equal(
            PgTargetSources.All.ToArray(),
            consts.Select(f => (string)f.GetRawConstantValue()!).Order(StringComparer.Ordinal).ToArray());

        /* And every one is registered — declared and registered on the same day, so a lane can never emit a
           source the get_analysis_facts filter refuses. */
        Assert.All(PgTargetSources.All, s => Assert.Contains(s, FactScorer.KnownSources));
        Assert.DoesNotContain("coverage", PgTargetSources.All);
        Assert.DoesNotContain("anomaly", PgTargetSources.All);
    }

    [Fact]
    public void EveryKey_WearsOneOfTheThreePrefixes_AndNoSqlServerKeyDoes()
    {
        var keys = typeof(PgTargetFactKeys).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (f.Name, Value: (string)f.GetRawConstantValue()!))
            .Where(k => k.Name is not (nameof(PgTargetFactKeys.MeasuredPrefix) or nameof(PgTargetFactKeys.ConfigPrefix) or nameof(PgTargetFactKeys.AnomalyPrefix)))
            .ToList();

        Assert.True(keys.Count >= 36, $"the v1 vocabulary should be declared in full (38 keys and family prefixes when this landed); found {keys.Count}");
        Assert.All(keys, k => Assert.True(PgTargetFactKeys.IsPgKey(k.Value), $"{k.Name} = {k.Value} wears none of PG_ / CONFIG_PG_ / ANOMALY_PG_"));
        Assert.All(keys, k => Assert.Matches("^[A-Z_]+$", k.Value));
        Assert.Equal(keys.Count, keys.Select(k => k.Value).Distinct(StringComparer.Ordinal).Count());

        /* The SQL Server vocabulary never matches: the ANOMALY_PG_ infix is what keeps ANOMALY_CPU_SPIKE and
           ANOMALY_PG_CPU_SPIKE apart in both directions. */
        foreach (var sqlServerKey in new[] { "CXPACKET", "CPU_SQL_PERCENT", "CONFIG_MAXDOP", "ANOMALY_CPU_SPIKE", "ANOMALY_WAIT_PROFILE", "BAD_ACTOR_123", "COLLECTION_GAP", "PGX", "" })
            Assert.False(PgTargetFactKeys.IsPgKey(sqlServerKey), sqlServerKey);
        Assert.False(PgTargetFactKeys.IsPgKey(null));
        Assert.False(PgTargetFactKeys.IsPgAnomalyKey("ANOMALY_CPU_SPIKE"));
        Assert.True(PgTargetFactKeys.IsPgAnomalyKey(PgTargetFactKeys.AnomalyCpuSpike));
    }

    [Theory]
    [InlineData("Lock", null, "PG_WAIT_LOCK")]
    [InlineData("Lock", "relation", "PG_WAIT_LOCK_RELATION")]
    [InlineData("LWLock", "WALWrite", "PG_WAIT_LWLOCK_WALWRITE")]
    [InlineData("IO", "DataFileRead", "PG_WAIT_IO_DATAFILEREAD")]
    [InlineData("io", "datafileread", "PG_WAIT_IO_DATAFILEREAD")]
    [InlineData("IO", "", "PG_WAIT_IO")]
    [InlineData("IPC", "  ", "PG_WAIT_IPC")]
    [InlineData("Lock", "transactionid", "PG_WAIT_LOCK_TRANSACTIONID")]
    [InlineData("Client", "ClientRead", "PG_WAIT_CLIENT_CLIENTREAD")]
    public void WaitKey_NormalisesCaseAndPunctuation_SoARenamedEventKeepsOneHistory(string type, string? waitEvent, string expected)
    {
        var key = PgTargetFactKeys.WaitKey(type, waitEvent);
        Assert.Equal(expected, key);
        Assert.True(PgTargetFactKeys.IsPgKey(key));
        Assert.StartsWith(PgTargetFactKeys.WaitKeyPrefix, key, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitKey_CollapsesPunctuationRuns_AndBadActorKey_IsInvariant()
    {
        /* Aurora spells some events with punctuation ("Lock:relation" already split; "IO:Aurora/foo.bar"):
           every non-alphanumeric run becomes ONE underscore and nothing leads or trails. */
        Assert.Equal("PG_WAIT_IO_AURORA_FOO_BAR", PgTargetFactKeys.WaitKey("IO", "Aurora/foo.bar"));
        Assert.Equal("PG_WAIT_IO_X", PgTargetFactKeys.WaitKey("IO", "--x--"));
        Assert.Equal("PG_WAIT_LW_LOCK", PgTargetFactKeys.WaitKey("LW Lock", null));

        Assert.Equal("PG_BAD_ACTOR_123", PgTargetFactKeys.BadActorKey(123));
        Assert.Equal("PG_BAD_ACTOR_-9223372036854775808", PgTargetFactKeys.BadActorKey(long.MinValue));
        Assert.True(PgTargetFactKeys.IsPgKey(PgTargetFactKeys.BadActorKey(1)));
    }

    [Fact]
    public void ConfigAdvisoryRoots_AreConventionAndPostureChecksOnly_NeverEvidenceGatedOrContext()
    {
        Assert.All(PgTargetFactKeys.ConfigAdvisoryRoots, k => Assert.True(PgTargetFactKeys.IsConfigAdvisoryRoot(k)));
        Assert.All(PgTargetFactKeys.ConfigAdvisoryRoots, k => Assert.True(
            k.StartsWith(PgTargetFactKeys.ConfigPrefix, StringComparison.Ordinal) || k.StartsWith("PG_POSTURE_", StringComparison.Ordinal),
            $"{k} is neither a CONFIG_PG_ check nor a posture key"));

        /* D5: an evidence-gated setting roots nothing on its own; its workload co-fire is the root. */
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigWorkMem));
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigMaintWorkMem));
        /* Context facts (base 0) are not roots either. */
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigMaxConnections));
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigSuperuserReserved));
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ServerMajorVersion));
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot("CONFIG_MAXDOP"));
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(null));

        /* The two knobs and the three posture keys are in. */
        Assert.Contains(PgTargetFactKeys.ConfigSharedBuffers, PgTargetFactKeys.ConfigAdvisoryRoots);
        Assert.Contains(PgTargetFactKeys.ConfigMaxWalSize, PgTargetFactKeys.ConfigAdvisoryRoots);
        Assert.Contains(PgTargetFactKeys.PostureFsync, PgTargetFactKeys.ConfigAdvisoryRoots);
        /* v3 (#3691 plumbing): the §4b composition check is a CONVENTION reading (five knobs against the host) and roots
           at the 0.4 advisory base on a quiet server; the co-fire lifts it (D5). Routed here so lane 32 never edits the
           shared file; the pressure fact beside it is measured, not config, and is NOT a root here. */
        Assert.Contains(PgTargetFactKeys.ConfigMemoryOvercommit, PgTargetFactKeys.ConfigAdvisoryRoots);
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.HostMemoryPressure));
    }

    [Fact]
    public void AnomalyToFamilies_MapsAnomalyKeysToMeasuredKeys_AndLeavesTpsSolo()
    {
        Assert.All(PgTargetFactKeys.AnomalyToFamilies.Keys, k => Assert.True(PgTargetFactKeys.IsPgAnomalyKey(k)));
        Assert.All(PgTargetFactKeys.AnomalyToFamilies.Values.SelectMany(v => v), k =>
            Assert.StartsWith(PgTargetFactKeys.MeasuredPrefix, k, StringComparison.Ordinal));
        Assert.Equal(new[] { PgTargetFactKeys.DeadlockRate }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyDeadlockRate]);
        Assert.False(PgTargetFactKeys.AnomalyToFamilies.ContainsKey(PgTargetFactKeys.AnomalyTps));

        /* v2 (#3691): the three new anomalies fold onto the family they deviate from — and the WAL-volume one onto
           CHECKPOINT pressure, not the WalVolumeShift fact it is the detector behind (§3.11: WAL volume is the
           leading edge of checkpoint pressure; the operator's incident is the checkpoint one). */
        Assert.Equal(new[] { PgTargetFactKeys.IoReadLatencyMs }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyIoLatency]);
        Assert.Equal(new[] { PgTargetFactKeys.ReplicationLag }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyReplicationLag]);
        Assert.Equal(new[] { PgTargetFactKeys.CheckpointPressure }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyWalVolume]);
        /* wave 3 (#3691 between waves): the blocking anomaly folds onto the chain fact, declared with the stubs. */
        Assert.Equal(new[] { PgTargetFactKeys.BlockingChain }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyBlocking]);
        /* v3 (#3691 plumbing): the plan-regression anomaly folds onto the regular fact that names the plan flip; the
           CPU-burn anomaly onto the cores-busy fact — both declared with the stubs. */
        Assert.Equal(new[] { PgTargetFactKeys.PlanRegression }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyPlanRegression]);
        Assert.Equal(new[] { PgTargetFactKeys.CpuBurnCores }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyCpuBurn]);
        /* The wait profile is resolved per story, never statically. */
        Assert.False(PgTargetFactKeys.AnomalyToFamilies.ContainsKey(PgTargetFactKeys.AnomalyWaitProfile));
    }

    /// <summary>
    /// #3691 (v1 residue): the PostgreSQL wait profile resolves its fold parent from its dominant contributor the
    /// way the SQL Server profile does — the standout key first, the type rollup second (the wait scorer grades one
    /// wait once, so exactly one of them can have fired), ordinal ties, empty without contributors.
    /// </summary>
    [Fact]
    public void TheWaitProfile_ResolvesItsFamilyFromTheDominantContributor_StandoutThenRollup()
    {
        var lockStorm = new Dictionary<string, double>(StringComparer.Ordinal)
        {
            ["contrib_Lock:relation"] = 900_000, ["contrib_IO:DataFileRead"] = 500_000, ["contrib_LWLock:WALWrite"] = 100_000, ["ratio"] = 6.0,
        };
        Assert.Equal(
            new[] { PgTargetFactKeys.WaitKey("Lock", "relation"), PgTargetFactKeys.WaitKey("Lock", null) },
            PgTargetFactKeys.WaitProfileFamilies(lockStorm));

        /* A contributor without an event names the rollup alone; a tie breaks on the ordinal name. */
        Assert.Equal(new[] { PgTargetFactKeys.WaitKey("Lock", null) }, PgTargetFactKeys.WaitProfileFamilies(new Dictionary<string, double> { ["contrib_Lock"] = 5 }));
        Assert.Equal(
            new[] { PgTargetFactKeys.WaitKey("IO", "DataFileRead"), PgTargetFactKeys.WaitKey("IO", null) },
            PgTargetFactKeys.WaitProfileFamilies(new Dictionary<string, double> { ["contrib_Lock:relation"] = 5, ["contrib_IO:DataFileRead"] = 5 }));
        Assert.Empty(PgTargetFactKeys.WaitProfileFamilies(new Dictionary<string, double> { ["ratio"] = 6.0 }));
        Assert.Empty(PgTargetFactKeys.WaitProfileFamilies(null));
    }

    /* ── FactScorer: the source arm, the context set, the anomaly membership, the amplifier arm ── */

    [Fact]
    public void ScoreAll_RoutesEveryPgSourceToThePgScorer_AndScoresTheStubsAtZeroWithoutThrowing()
    {
        var facts = PgTargetSources.All
            .Select(s => new Fact { Source = s, Key = "PG_PROBE", Value = 99, ServerId = 1 })
            .ToList();

        new FactScorer().ScoreAll(facts);

        /* Every family partial is a stub: 0.0 through PgTargetScorer.ScoreBase, never the SQL Server
           default arm — indistinguishable by value today, so the routing is pinned separately below. */
        Assert.All(facts, f => Assert.Equal(0.0, f.BaseSeverity));
        Assert.All(facts, f => Assert.Equal(0.0, f.Severity));
        Assert.All(PgTargetSources.All, s => Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = s, Key = "PG_PROBE" })));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = "pg_unknown", Key = "PG_PROBE" }));
    }

    /// <summary>
    /// The load-bearing half of correction #3 (#3584 twin): a PostgreSQL z-score anomaly grades through the
    /// SHARED deviation ramp off the same AnomalyGate metadata — 2x the fire anchor saturates at 1.0, under the
    /// anchor is 0 — because PgTargetScorer registers it as deviation-scored. Without the delegating arm this
    /// fact fell to the SQL Server ratio arms and scored 0 with a 4-sigma deviation.
    /// </summary>
    [Theory]
    [InlineData(PgTargetFactKeys.AnomalyTps)]
    [InlineData(PgTargetFactKeys.AnomalySessionSpike)]
    [InlineData(PgTargetFactKeys.AnomalyCpuSpike)]
    public void APgZScoreAnomaly_ScoresThroughTheSharedDeviationRamp(string key)
    {
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(key));
        Assert.False(PgTargetScorer.IsPgRatioAnomalyKey(key));

        var saturated = Anomaly(key, ("deviation_sigma", 4.0), ("fire_threshold", 2.0));
        var atAnchor = Anomaly(key, ("deviation_sigma", 2.0), ("fire_threshold", 2.0));
        var under = Anomaly(key, ("deviation_sigma", 1.9), ("fire_threshold", 2.0));
        var facts = new List<Fact> { saturated, atAnchor, under };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(1.0, saturated.BaseSeverity, precision: 9);
        Assert.Equal(0.5, atAnchor.BaseSeverity, precision: 9);
        Assert.Equal(0.0, under.BaseSeverity);
    }

    [Fact]
    public void ThePgRatioAnomaly_TakesThePgRatioRamp_NotTheSqlServerDeadlockSpikeArm()
    {
        Assert.True(PgTargetScorer.IsPgRatioAnomalyKey(PgTargetFactKeys.AnomalyDeadlockRate));
        Assert.False(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyDeadlockRate));

        /* A ratio the two ramps grade DIFFERENTLY: the SQL Server ANOMALY_DEADLOCK_SPIKE arm (3× → 0.5, 10× → 1.0)
           reads 6× as 0.5 + 0.5 × 3/7 = 0.714; the PostgreSQL ramp (lane 9: 3× → 0.5, 9× → 1.0) reads it as 0.75 —
           so 0.75 here is the ROUTING, not a coincidence of two ramps agreeing. */
        var fact = Anomaly(PgTargetFactKeys.AnomalyDeadlockRate, ("ratio", 6.0));
        new FactScorer().ScoreAll([fact]);
        Assert.Equal(PgTargetScorer.ScoreRatioAnomaly(fact), fact.BaseSeverity);
        Assert.Equal(0.75, fact.BaseSeverity, precision: 9);
        Assert.True(PgTargetScorer.IsPgRatioAnomalyKey(PgTargetFactKeys.AnomalyWaitProfile));
        Assert.False(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyWaitProfile));
    }

    [Fact]
    public void SqlServerAnomalies_AreUnaffected_ByThePgMembershipArm()
    {
        var cpu = Anomaly("ANOMALY_CPU_SPIKE", ("deviation_sigma", 4.0), ("fire_threshold", 2.0));
        var deadlock = Anomaly("ANOMALY_DEADLOCK_SPIKE", ("ratio", 10.0));
        new FactScorer().ScoreAll([cpu, deadlock]);
        Assert.Equal(1.0, cpu.BaseSeverity, precision: 9);
        Assert.Equal(1.0, deadlock.BaseSeverity, precision: 9);
        Assert.False(PgTargetScorer.IsDeviationScoredAnomalyKey("ANOMALY_CPU_SPIKE"));
        Assert.False(PgTargetScorer.IsPgRatioAnomalyKey("ANOMALY_DEADLOCK_SPIKE"));
    }

    [Fact]
    public void TheAmplifierArm_RoutesEveryPgKeyToThePgTable_AheadOfTheAnomalyArm()
    {
        var scorer = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "FactScorer.cs");
        var pgArm = scorer.IndexOf("_ when PgTargetScorer.IsPgKey(fact.Key) => PgTargetScorer.Amplifiers(fact.Key),", StringComparison.Ordinal);
        var anomalyArm = scorer.IndexOf("_ when fact.Key.StartsWith(\"ANOMALY_\", StringComparison.OrdinalIgnoreCase) => AnomalyAmplifiers(fact.Key),", StringComparison.Ordinal);
        Assert.True(pgArm > 0, "the PostgreSQL amplifier arm is missing from GetAmplifiers");
        Assert.True(anomalyArm > pgArm, "the PostgreSQL arm must precede the ANOMALY_ arm or ANOMALY_PG_* takes the SQL Server load-family confirmers");

        /* The pg_ sources join the amplifier context set: a base-0 PG_CPU_PERCENT must be visible to a confirmer. */
        Assert.Contains("contextSources.UnionWith(PgTargetSources.All);", scorer, StringComparison.Ordinal);

        /* The source switch has exactly one pg_ arm, by prefix, and IsDeviationScoredAnomalyKey / ScoreAnomalyFact
           each delegate once. */
        Assert.Equal(1, Count(scorer, "_ when PgTargetSources.IsPgSource(fact.Source) => PgTargetScorer.ScoreBase(fact),"));
        Assert.Equal(1, Count(scorer, "PgTargetScorer.IsDeviationScoredAnomalyKey(key)"));
        /* Two since #3691: the ratio-anomaly first line of ScoreAnomalyFact, and the wait profile's extremity-escape
           arm in IsExtremeAnomaly (the v1 residue closed by the v2 plumbing) — each delegating to PgTargetScorer. */
        Assert.Equal(2, Count(scorer, "if (PgTargetScorer.IsPgRatioAnomalyKey(fact.Key))"));
        Assert.Equal(1, Count(scorer, "return PgTargetScorer.IsExtremeWaitProfileAnomaly(fact, ExtremeAnomalyMultiple);"));

        /* And the amplifier dispatcher answers something for every declared key without throwing. */
        var keys = typeof(PgTargetFactKeys).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(PgTargetFactKeys.IsPgKey);
        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        foreach (var key in keys)
            Assert.NotNull(amplifiers.Invoke(null, [key]));
        Assert.NotNull(amplifiers.Invoke(null, [PgTargetFactKeys.WaitKey("Lock", "relation")]));
        Assert.NotNull(amplifiers.Invoke(null, [PgTargetFactKeys.BadActorKey(7)]));
    }

    /* ── InferenceEngine + RelationshipGraph ── */

    [Fact]
    public void APgConfigAdvisoryRoot_RootsAStandaloneCard_BelowTheIncidentThreshold_OnThePgGraph()
    {
        var engine = new InferenceEngine(new PgTargetRelationshipGraph());
        var advisory = new Fact { Source = PgTargetSources.ConfigSource, Key = PgTargetFactKeys.ConfigSharedBuffers, Value = 128, Severity = 0.4, BaseSeverity = 0.4 };
        var evidenceGated = new Fact { Source = PgTargetSources.ConfigSource, Key = PgTargetFactKeys.ConfigWorkMem, Value = 4, Severity = 0.4, BaseSeverity = 0.4 };

        var stories = engine.BuildStories([advisory, evidenceGated]);

        /* The advisory root roots at 0.4 (the delegating check); the evidence-gated key at the same severity
           does not — it needs its co-fire to carry it past the 0.5 incident threshold. */
        var story = Assert.Single(stories);
        Assert.Equal(PgTargetFactKeys.ConfigSharedBuffers, story.RootFactKey);
    }

    [Fact]
    public void ThePgGraph_StartsEmpty_AndTheParameterlessSqlServerGraph_IsUnchanged()
    {
        var pg = new PgTargetRelationshipGraph();
        var sqlServer = new RelationshipGraph();

        /* No SQL Server chain is reachable from the PostgreSQL graph — D2 made the keys disjoint, so an edge
           that could never fire would only be audit-trail noise. */
        foreach (var sqlServerRoot in new[] { "CXPACKET", "SOS_SCHEDULER_YIELD", "PAGEIOLATCH_SH", "BLOCKING_EVENTS", "THREADPOOL" })
        {
            Assert.NotEmpty(sqlServer.GetAllEdges(sqlServerRoot));
            Assert.Empty(pg.GetAllEdges(sqlServerRoot));
        }

        /* And the seam is the only way in: the parameterless ctor stays public, the flag ctor protected,
           AddEdge protected — seven Lite tests construct RelationshipGraph() directly. */
        var flagCtor = typeof(RelationshipGraph).GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, [typeof(bool)]);
        Assert.NotNull(flagCtor);
        Assert.True(flagCtor!.IsFamily);
        Assert.NotNull(typeof(RelationshipGraph).GetConstructor(BindingFlags.Instance | BindingFlags.Public, Type.EmptyTypes));
        var addEdge = typeof(RelationshipGraph).GetMethod("AddEdge", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(addEdge);
        Assert.True(addEdge!.IsFamily);
    }

    /* ── FactAdvice ── */

    [Fact]
    public void FactAdvice_DelegatesEveryPgKey_AndNeverReachesASqlServerComposer()
    {
        var lookup = new Dictionary<string, Fact>(StringComparer.Ordinal);

        /* DELEGATION, for every declared key and both dynamic families: what the shared entry points answer is
           exactly what the PostgreSQL composer answers — null while a family is a stub, its own block once its
           lane lands (the bad-actor family was the first) — and never a SQL Server composer's block. A block
           that came from the SQL Server side would differ from PgTargetAdvice's answer, which is what the
           equality catches; AdviceBlock is a record, so the comparison is by value. */
        var keys = typeof(PgTargetFactKeys).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(PgTargetFactKeys.IsPgKey)
            .Append(PgTargetFactKeys.WaitKey("Lock", "relation"))
            .Append(PgTargetFactKeys.BadActorKey(7))
            .ToList();
        foreach (var key in keys)
        {
            Assert.Equal(PgTargetAdvice.Compose(key, lookup), FactAdvice.Compose(key, lookup));
            Assert.Equal(PgTargetAdvice.Static(key), FactAdvice.GetForFactKey(key));
            Assert.Equal(PgTargetAdvice.Static(key), PgTargetAdvice.Compose(key, lookup));
        }
        /* And the filled families answer their own block — one representative each, so a lane that fills its
           family moves its own line here and says so. Lane 5 (the wait profile) moved the wait line: a v1 wait
           key composes; a wait key outside the v1 vocabulary still answers null. */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.WaitKey("Lock", "relation")));
        Assert.Null(PgTargetAdvice.Static(PgTargetFactKeys.WaitKey("Lock", "transactionid")));
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.BadActorKey(7)));
        /* v2 (#3691) stubs: null IS the delegation — the shared entry points answer exactly what the stub does (the
           equality above), and no SQL Server composer claims the key. Each lane moves its own line to NotNull.
           Lane 11 (I/O latency) filled: both measured keys AND the family's anomaly compose — the anomaly through
           the ANOMALY_PG_ prefix arm's one delegating case into PgTargetAdvice.Io.cs, never the SQL Server
           "Anomalous spike" composer. */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.IoReadLatencyMs));     /* lane 11 */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.IoWriteLatencyMs));    /* lane 11 */
        var pgIoAnomaly = FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyIoLatency);
        Assert.NotNull(pgIoAnomaly);
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.AnomalyIoLatency), pgIoAnomaly);
        Assert.DoesNotContain("Anomalous spike", pgIoAnomaly!.Headline, StringComparison.Ordinal);
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.ReplicationLag));      /* lane 12 filled the replication family */
        var pgLagAnomaly = FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyReplicationLag);   /* lane 12: the anomaly too, lane 11's shape */
        Assert.NotNull(pgLagAnomaly);
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.AnomalyReplicationLag), pgLagAnomaly);
        Assert.DoesNotContain("Anomalous spike", pgLagAnomaly!.Headline, StringComparison.Ordinal);
        /* Lane 13 filled the bloat family: both keys compose their own block (the static shape when no fact is
           in the lookup), and the delegation equality above proves the shared entry points answer that block. */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.BloatTrend));          /* lane 13 */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.IndexBloatTrend));     /* lane 13 */
        /* Wave 3 (#3691): lane 17 filled the blocking family — the three keys compose their own block (the static shape
           when no fact is in the lookup), the delegation equality above proves the shared entry points answer that
           block, and the anomaly's ComposeAnomaly arm delegates to the same file (never the SQL Server "Anomalous
           spike" composer, which the equality would expose). Moved from Null, as the stub comment said it would be. */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.BlockingChain));       /* lane 17 */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.LockWaitEvents));      /* lane 17 */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.LongRunningQuery));    /* lane 17 */
        var pgBlockingAnomaly = FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyBlocking);   /* lane 17: the anomaly too, lane 11's shape */
        Assert.NotNull(pgBlockingAnomaly);
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.AnomalyBlocking), pgBlockingAnomaly);
        Assert.DoesNotContain("Anomalous spike", pgBlockingAnomaly!.Headline, StringComparison.Ordinal);
        /* v3 (#3691 plumbing) stubs: null IS the delegation — the equality above proves the shared entry points answer
           what PgTargetAdvice.Plans.cs / .Kernel.cs / .Memory.cs answer, and each anomaly's ComposeAnomaly arm delegates
           to the same family file (never the SQL Server "Anomalous spike" composer, which the equality would expose).
           Each content lane moves ITS lines to NotNull, as lanes 11–13 and 17 did. */
        /* Lane 27 filled the plan family's regression and sensitivity arms and the anomaly composer, lane 30 the Seq-Scan
           arm in the same partial — each moved from Null, as the stub comment said they would be. */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.PlanRegression));       /* lane 27 */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.ParameterSensitivity)); /* lane 27 */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.SeqScanAdvisory));      /* lane 30 */
        var pgPlanAnomaly = FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyPlanRegression);   /* lane 27: the anomaly too, lane 17's shape */
        Assert.NotNull(pgPlanAnomaly);
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.AnomalyPlanRegression), pgPlanAnomaly);
        Assert.DoesNotContain("Anomalous spike", pgPlanAnomaly!.Headline, StringComparison.Ordinal);
        /* v3 (#3691): lane 28 filled the kernel family — both keys compose their own block (the static shape when no fact
           is in the lookup), and the anomaly's ComposeAnomaly arm delegates to the same file (never the SQL Server
           "Anomalous spike" composer). Moved from Null, as the stub comment said it would be. */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.CpuBurnCores));         /* lane 28 */
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.CpuDecomposition));     /* lane 28 */
        var pgCpuBurnAnomaly = FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyCpuBurn);   /* lane 28: the anomaly too, lane 17's shape */
        Assert.NotNull(pgCpuBurnAnomaly);
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.AnomalyCpuBurn), pgCpuBurnAnomaly);
        Assert.DoesNotContain("Anomalous spike", pgCpuBurnAnomaly!.Headline, StringComparison.Ordinal);
        Assert.Null(PgTargetAdvice.Static(PgTargetFactKeys.ConfigMemoryOvercommit));  /* lane 32 — routed by name to ComposeMemory, ahead of the config prefix arm */
        Assert.Null(PgTargetAdvice.Static(PgTargetFactKeys.HostMemoryPressure));      /* lane 32 */

        /* ANOMALY_PG_WAIT_PROFILE must not fall into the SQL Server ANOMALY_WAIT_ composer, which would render
           "Anomalous spike in PG_WAIT_PROFILE" for it. Lane 9 filled the anomaly family, so the line moved from
           "null" to "the PostgreSQL block, and not the SQL Server one". */
        var pgWaitProfile = FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyWaitProfile);
        Assert.NotNull(pgWaitProfile);
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.AnomalyWaitProfile), pgWaitProfile);
        Assert.DoesNotContain("Anomalous spike", pgWaitProfile!.Headline, StringComparison.Ordinal);
        Assert.NotEqual(FactAdvice.GetForFactKey("ANOMALY_WAIT_PROFILE"), pgWaitProfile);

        /* The SQL Server side is untouched. */
        Assert.NotNull(FactAdvice.GetForFactKey("CXPACKET"));
        Assert.NotNull(FactAdvice.GetForFactKey("ANOMALY_WAIT_PROFILE"));
        Assert.NotNull(FactAdvice.GetForFactKey("BAD_ACTOR_abc"));
        Assert.Null(PgTargetAdvice.Static("CXPACKET"));
        Assert.Null(PgTargetAdvice.Static(null));

        var advice = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "FactAdvice.cs");
        Assert.Equal(1, Count(advice, "return PgTargetAdvice.Compose(rootFactKey, factsByKey);"));
        Assert.Equal(1, Count(advice, "return PgTargetAdvice.Static(factKey);"));
    }

    /* ── AnomalyIncidentReconciler ── */

    [Fact]
    public void APgAnomalyStory_FoldsIntoItsRegisteredParent_AndAnUnmappedOneStaysSolo()
    {
        var parent = Story(PgTargetFactKeys.DeadlockRate, "inc-parent", severity: 0.9);
        var anomaly = Story(PgTargetFactKeys.AnomalyDeadlockRate, "inc-anomaly", severity: 0.6);
        var tps = Story(PgTargetFactKeys.AnomalyTps, "inc-tps", severity: 0.6);
        var otherDb = Story(PgTargetFactKeys.AnomalyDeadlockRate, "inc-other", severity: 0.6, database: "otherdb");

        AnomalyIncidentReconciler.Reconcile([parent, anomaly, tps, otherDb]);

        Assert.Equal("inc-parent", anomaly.IncidentId);
        Assert.Equal("inc-tps", tps.IncidentId);
        Assert.Equal("inc-other", otherDb.IncidentId);
        Assert.Equal("inc-parent", parent.IncidentId);

        /* #3691: the wait profile folds onto the fired wait card its dominant contributor names — the standout when
           it fired, the rollup when only the rollup did — and stays solo with no contributor metadata (v1's
           behaviour for every profile story). Same-database only, as every fold. */
        var profileMeta = new Dictionary<string, double>(StringComparer.Ordinal) { ["contrib_Lock:relation"] = 900_000, ["contrib_IO:DataFileRead"] = 100 };
        var standout = Story(PgTargetFactKeys.WaitKey("Lock", "relation"), "inc-standout", severity: 0.8);
        var profile = Story(PgTargetFactKeys.AnomalyWaitProfile, "inc-profile", severity: 0.6);
        profile.RootFactMetadata = new Dictionary<string, double>(profileMeta);
        AnomalyIncidentReconciler.Reconcile([standout, profile]);
        Assert.Equal("inc-standout", profile.IncidentId);

        var rollup = Story(PgTargetFactKeys.WaitKey("Lock", null), "inc-rollup", severity: 0.8);
        var profileOntoRollup = Story(PgTargetFactKeys.AnomalyWaitProfile, "inc-profile-2", severity: 0.6);
        profileOntoRollup.RootFactMetadata = new Dictionary<string, double>(profileMeta);
        AnomalyIncidentReconciler.Reconcile([rollup, profileOntoRollup]);
        Assert.Equal("inc-rollup", profileOntoRollup.IncidentId);

        var bareProfile = Story(PgTargetFactKeys.AnomalyWaitProfile, "inc-bare", severity: 0.6);
        AnomalyIncidentReconciler.Reconcile([Story(PgTargetFactKeys.WaitKey("Lock", "relation"), "inc-x", severity: 0.8), bareProfile]);
        Assert.Equal("inc-bare", bareProfile.IncidentId);

        /* And the SQL Server map still folds its own — the lookup is a second map, not a replacement. */
        var sqlParent = Story("DEADLOCKS", "sql-parent", severity: 0.9);
        var sqlAnomaly = Story("ANOMALY_DEADLOCK_SPIKE", "sql-anomaly", severity: 0.6);
        AnomalyIncidentReconciler.Reconcile([sqlParent, sqlAnomaly]);
        Assert.Equal("sql-parent", sqlAnomaly.IncidentId);
    }

    private static Fact Anomaly(string key, params (string Name, double Value)[] metadata)
    {
        var fact = new Fact { Source = "anomaly", Key = key, Value = 1, ServerId = 1 };
        foreach (var (name, value) in metadata)
            fact.Metadata[name] = value;
        return fact;
    }

    private static AnalysisStory Story(string rootKey, string incidentId, double severity, string? database = null) => new()
    {
        RootFactKey = rootKey,
        Path = [rootKey],
        StoryPath = rootKey,
        Severity = severity,
        IncidentId = incidentId,
        DatabaseName = database,
    };

    /* ── The routing census (#3691 between waves, item 7) ── */

    /// <summary>
    /// Every declared <see cref="PgTargetFactKeys"/> key is ROUTED by every root dispatcher that must know it by name
    /// — or is excluded here, by name, with the reason. Lane 14 of #3691 found <c>PG_IDLE_IN_TRANSACTION</c> declared
    /// in v1 and never named by <c>PgTargetScorer.Amplifiers</c> or <c>PgTargetAdvice.Compose</c>: the fact scored,
    /// rooted a card, and had no amplifier and no advice, because both switches fell to their defaults and nothing
    /// said so. Lanes 12 and 15 each found their anomaly's <c>ComposeAnomaly</c> arm missing the same way. This
    /// census makes the next such miss a compile-time-adjacent failure: declare a key, and the four dispatchers
    /// (the amplifier switch, the advice switch, the anomaly composer's switch, the source switch) must name it —
    /// or this file must say why not.
    ///
    /// <para>Read as SOURCE, not behaviour: a switch that falls to <c>_ =&gt; []</c> answers the same empty list a
    /// stub does, so only the text can tell "routed to a family" from "nobody wrote the arm". The prefix-routed
    /// families (<c>PG_WAIT_*</c>, <c>PG_BAD_ACTOR_*</c>, <c>CONFIG_PG_*</c>, <c>ANOMALY_PG_*</c>) are routed by their
    /// prefix arms, which are pinned by exact text below; the anomalies additionally need a per-key case in
    /// <c>ComposeAnomaly</c>, which is pinned per key.</para>
    /// </summary>
    [Fact]
    public void EveryDeclaredPgKey_IsRoutedByEveryRootDispatcher_OrExcludedHereByName()
    {
        var scorerRoot = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.cs"));
        var adviceRoot = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.cs"));
        var anomalyAdvice = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.Anomaly.cs"));

        static string Between(string code, string start, string end)
        {
            var a = code.IndexOf(start, StringComparison.Ordinal);
            Assert.True(a >= 0, start + " has moved");
            var b = code.IndexOf(end, a, StringComparison.Ordinal);
            Assert.True(b > a, end + " has moved");
            return code[a..b];
        }
        var amplifiers = Between(scorerRoot, "internal static List<AmplifierDefinition> Amplifiers(string key)", "private static partial double ScoreConfigFact");
        var scoreBase = Between(scorerRoot, "fact.Source switch", "};");
        var compose = Between(adviceRoot, "public static AdviceBlock? Compose(string rootFactKey", "public static AdviceBlock? Static(");
        var composeAnomaly = Between(anomalyAdvice, "private static partial AdviceBlock? ComposeAnomaly(string key", "private static AdviceBlock ComposeDeviation(");

        /* The prefix arms, by exact text — the four families that route without a per-key name. */
        Assert.Contains("_ when key.StartsWith(PgTargetFactKeys.WaitKeyPrefix, StringComparison.Ordinal) => WaitAmplifiers(key),", amplifiers, StringComparison.Ordinal);
        Assert.Contains("_ when key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal) => QueriesAmplifiers(key),", amplifiers, StringComparison.Ordinal);
        Assert.Contains("_ when key.StartsWith(PgTargetFactKeys.ConfigPrefix, StringComparison.Ordinal) => ConfigAmplifiers(key),", amplifiers, StringComparison.Ordinal);
        Assert.Contains("if (PgTargetFactKeys.IsPgAnomalyKey(key))", amplifiers, StringComparison.Ordinal);
        Assert.Contains("if (PgTargetFactKeys.IsPgAnomalyKey(rootFactKey))", compose, StringComparison.Ordinal);
        Assert.Contains("if (rootFactKey.StartsWith(PgTargetFactKeys.WaitKeyPrefix, StringComparison.Ordinal))", compose, StringComparison.Ordinal);
        Assert.Contains("if (rootFactKey.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))", compose, StringComparison.Ordinal);
        Assert.Contains("_ when rootFactKey.StartsWith(PgTargetFactKeys.ConfigPrefix, StringComparison.Ordinal)", compose, StringComparison.Ordinal);

        /* Every declared source has a ScoreBase arm — a source without one scores every fact 0 through the default. */
        foreach (var source in typeof(PgTargetSources).GetFields(BindingFlags.Public | BindingFlags.Static)
                     .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.EndsWith("Source", StringComparison.Ordinal)))
        {
            Assert.Contains($"PgTargetSources.{source.Name} => Score", scoreBase, StringComparison.Ordinal);
        }

        /* Keys that are NOT routed by name, each with its reason. Adding to this list is the deliberate act. */
        var noAmplifierArm = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [nameof(PgTargetFactKeys.ServerMajorVersion)] = "registry context fact (base 0, never roots); the D6 flavour disclosure hangs on it, nothing amplifies it",
            [nameof(PgTargetFactKeys.Tps)] = "database family: context (base 0) — throughput is a denominator, not a symptom; no amplifier family",
            [nameof(PgTargetFactKeys.HitRatio)] = "database family: context under the buffer family's pressure fact; no amplifier family",
            [nameof(PgTargetFactKeys.DeadlockRate)] = "database family: graded on its own bar, corroborated through the anomaly that folds INTO it; no amplifier arm of its own (PgTargetAnomalyTests pins the deadlock rate never escapes the cap)",
            [nameof(PgTargetFactKeys.MonitoringPermissions)] = "sessions family: a visibility fact about the monitoring role, never amplified by workload",
            [nameof(PgTargetFactKeys.PostureFsync)] = "posture: no amplifier may touch fsync / synchronous_commit / full_page_writes (PgTargetPostureIsolationTests)",
            [nameof(PgTargetFactKeys.PostureFullPageWrites)] = "posture (same rule)",
            [nameof(PgTargetFactKeys.PostureSynchronousCommit)] = "posture (same rule)",
            [nameof(PgTargetFactKeys.CpuPercent)] = "cpu family: graded on the capacity bar, corroborated through ANOMALY_PG_CPU_SPIKE folding into it; no amplifier arm",
            [nameof(PgTargetFactKeys.MaintenanceShapeShift)] = "wave 2, deprioritised to v3 by the calibration read (§B6): declared, no stub, no lane, nothing emits it",
        };
        var noComposeArm = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [nameof(PgTargetFactKeys.ServerMajorVersion)] = "registry context fact; never a story root, so no card to compose",
            [nameof(PgTargetFactKeys.MaintenanceShapeShift)] = "wave 2, deprioritised to v3 (§B6): nothing emits it",
        };
        var neverAKey = new HashSet<string>(StringComparer.Ordinal)
        {
            nameof(PgTargetFactKeys.BadActorFamily),   /* the graph alias — resolved to a PG_BAD_ACTOR_<id> before any path is built */
        };

        var measured = new List<string>();
        var config = new List<string>();
        var anomalies = new List<string>();
        foreach (var field in typeof(PgTargetFactKeys).GetFields(BindingFlags.Public | BindingFlags.Static)
                     .Where(f => f.IsLiteral && f.FieldType == typeof(string) && !f.Name.EndsWith("Prefix", StringComparison.Ordinal)))
        {
            var value = (string)field.GetRawConstantValue()!;
            if (!PgTargetFactKeys.IsPgKey(value) || neverAKey.Contains(field.Name)) continue;
            if (PgTargetFactKeys.IsPgAnomalyKey(value)) anomalies.Add(field.Name);
            else if (value.StartsWith(PgTargetFactKeys.ConfigPrefix, StringComparison.Ordinal)) config.Add(field.Name);
            else measured.Add(field.Name);
        }
        Assert.True(measured.Count >= 25 && config.Count >= 12 && anomalies.Count >= 9, $"the sweep found {measured.Count} / {config.Count} / {anomalies.Count} keys; the vocabulary has moved");

        /* Measured PG_ keys: named in the amplifier switch and the advice switch, or excluded above. */
        foreach (var name in measured)
        {
            var token = $"PgTargetFactKeys.{name}";
            if (!noAmplifierArm.ContainsKey(name))
                Assert.True(amplifiers.Contains(token, StringComparison.Ordinal), $"{name} is a declared measured key that PgTargetScorer.Amplifiers never names — route it to its family's partial, or exclude it here with the reason");
            if (!noComposeArm.ContainsKey(name))
                Assert.True(compose.Contains(token, StringComparison.Ordinal), $"{name} is a declared measured key that PgTargetAdvice.Compose never names — a story rooted on it would carry no advice");
        }
        /* An exclusion that IS routed is stale: the reason no longer holds, and the list must shrink. */
        foreach (var name in noAmplifierArm.Keys)
            Assert.False(amplifiers.Contains($"PgTargetFactKeys.{name}", StringComparison.Ordinal), $"{name} is excluded from the amplifier census but the switch names it — drop the exclusion");
        foreach (var name in noComposeArm.Keys)
            Assert.False(compose.Contains($"PgTargetFactKeys.{name}", StringComparison.Ordinal), $"{name} is excluded from the compose census but the switch names it — drop the exclusion");

        /* CONFIG_PG_ keys route by prefix (pinned above); the ones a family claims by name are a subset, never a requirement. */
        Assert.Contains(nameof(PgTargetFactKeys.ConfigSharedBuffers), config);

        /* ANOMALY_PG_ keys: the prefix arms carry them to AnomalyAmplifiers / ComposeAnomaly, and ComposeAnomaly's
           switch must then name EACH one — the v2 lanes' miss. A stub arm (wave 3's AnomalyBlocking) counts: it is named. */
        foreach (var name in anomalies)
            Assert.True(composeAnomaly.Contains($"case PgTargetFactKeys.{name}:", StringComparison.Ordinal), $"{name} has no case in PgTargetAdvice.ComposeAnomaly — its card would compose null (or, without the prefix arm, the SQL Server \"Anomalous spike\" block)");

        /* Every key has next reads: PgTargetMcpSurfaceTests.EveryPgRecommendation_NamesARegisteredTool_AndOnlyPgOrAnalysisReads
           is the full census (registered tool names, get_pg_ only); this is the routing half — a row exists. */
        foreach (var name in measured.Concat(config).Concat(anomalies))
        {
            var value = (string)typeof(PgTargetFactKeys).GetField(name)!.GetRawConstantValue()!;
            Assert.NotNull(PerformanceMonitor.Darling.Service.Mcp.PgTargetToolRecommendations.GetForKey(value));
        }
    }

    private static int Count(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }
}
