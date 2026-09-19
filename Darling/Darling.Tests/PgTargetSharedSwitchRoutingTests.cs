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
        Assert.Equal(11, PgTargetSources.All.Count);
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
    }

    [Fact]
    public void AnomalyToFamilies_MapsAnomalyKeysToMeasuredKeys_AndLeavesTpsSolo()
    {
        Assert.All(PgTargetFactKeys.AnomalyToFamilies.Keys, k => Assert.True(PgTargetFactKeys.IsPgAnomalyKey(k)));
        Assert.All(PgTargetFactKeys.AnomalyToFamilies.Values.SelectMany(v => v), k =>
            Assert.StartsWith(PgTargetFactKeys.MeasuredPrefix, k, StringComparison.Ordinal));
        Assert.Equal(new[] { PgTargetFactKeys.DeadlockRate }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyDeadlockRate]);
        Assert.False(PgTargetFactKeys.AnomalyToFamilies.ContainsKey(PgTargetFactKeys.AnomalyTps));
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

        /* A ratio the SQL Server ANOMALY_DEADLOCK_SPIKE arm would grade at 1.0. The PostgreSQL ramp is lane 9's
           and is stubbed at 0 — so 0 here is the ROUTING: had the key reached the SQL Server arm it would read 1.0. */
        var fact = Anomaly(PgTargetFactKeys.AnomalyDeadlockRate, ("ratio", 10.0));
        new FactScorer().ScoreAll([fact]);
        Assert.Equal(PgTargetScorer.ScoreRatioAnomaly(fact), fact.BaseSeverity);
        Assert.Equal(0.0, fact.BaseSeverity);
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
        Assert.Equal(1, Count(scorer, "if (PgTargetScorer.IsPgRatioAnomalyKey(fact.Key))"));

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

        /* ANOMALY_PG_WAIT_PROFILE must not fall into the SQL Server ANOMALY_WAIT_ composer, which would render
           "Anomalous spike in PG_WAIT_PROFILE" for it. */
        Assert.Null(FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyWaitProfile));

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

    private static int Count(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }
}
