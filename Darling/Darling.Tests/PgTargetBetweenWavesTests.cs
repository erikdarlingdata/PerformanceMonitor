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
using ModelContextProtocol.Server;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The between-waves shared-vocabulary pass of #3542 — the items six content lanes reported that could not be
/// made inside a file-disjoint lane: the bad-actor ALIAS and its story-build-time resolution, the two vacuum-side
/// config arms that were inert because their source is <c>pg_config</c> and no arm gave them a base, the
/// PostgreSQL 16+ <c>reserved_connections</c> carve-out in the connection ceiling, and the bad actor's first
/// next-read. Each pin here is the property the item's decision rests on; the families' own tests keep their
/// pins and the ones this pass deliberately moved say so inline.
/// </summary>
public sealed class PgTargetBetweenWavesTests
{
    /* ───────────────────────── item 1: the bad-actor alias ───────────────────────── */

    [Fact]
    public void TheAlias_IsNeverABadActorKey_AndNoArmClaimsItByPrefix()
    {
        Assert.Equal("PG_BAD_ACTOR", PgTargetFactKeys.BadActorFamily);
        /* The alias lacks the prefix's trailing underscore, so nothing routing on BadActorKeyPrefix can claim it —
           the queries scorer, advice, tool recommendations and ResolveBadActor itself all skip it. */
        Assert.False(PgTargetFactKeys.BadActorFamily.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal));
        Assert.True(PgTargetRelationshipGraph.IsBadActorAlias(PgTargetFactKeys.BadActorFamily));
        Assert.False(PgTargetRelationshipGraph.IsBadActorAlias(PgTargetFactKeys.BadActorKey(42)));
        Assert.Null(PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.BadActorFamily));
        Assert.Null(PgTargetRelationshipGraph.ResolveBadActor(new Dictionary<string, Fact>
        {
            [PgTargetFactKeys.BadActorFamily] = new() { Key = PgTargetFactKeys.BadActorFamily, Severity = 2.0 },
        }));
    }

    [Fact]
    public void ResolveBadActor_TakesTheHigherSeverity_TiesByOrdinalKey_AndIsNullWithNone()
    {
        var low = BadActor(11, severity: 0.55);
        var high = BadActor(7, severity: 1.0);
        Assert.Equal(high.Key, PgTargetRelationshipGraph.ResolveBadActor(Lookup(low, high)));
        Assert.Equal(high.Key, PgTargetRelationshipGraph.ResolveBadActor(Lookup(high, low)));

        /* Two at the same severity resolve the same way regardless of dictionary order. */
        var tieA = BadActor(200, severity: 0.7);
        var tieB = BadActor(100, severity: 0.7);
        Assert.Equal(tieB.Key, PgTargetRelationshipGraph.ResolveBadActor(Lookup(tieA, tieB)));
        Assert.Equal(tieB.Key, PgTargetRelationshipGraph.ResolveBadActor(Lookup(tieB, tieA)));

        Assert.Null(PgTargetRelationshipGraph.ResolveBadActor(Lookup(Spill(0.6))));
        Assert.Null(PgTargetRelationshipGraph.ResolveBadActor(new Dictionary<string, Fact>()));
    }

    [Fact]
    public void AnEdgeIntoTheAlias_ResolvesToTheHighestSeverityBadActor_ThroughTheRealTraversal()
    {
        var graph = GraphWithAliasEdge();
        var spill = Spill(0.6);
        /* Both bad actors are under the 0.5 story line so neither roots (and is consumed) before the spill walks —
           the same rule every leaf on the SQL Server graph lives under; a 1.0 bad actor roots its own card first,
           exactly as before this pass. The alias is what lets the walk reach the leaf when the leaf is a leaf. */
        var low = BadActor(11, severity: 0.3);
        var high = BadActor(7, severity: 0.45);

        var stories = new InferenceEngine(graph).BuildStories([spill, low, high]);

        var spillStory = Assert.Single(stories, s => s.RootFactKey == PgTargetFactKeys.TempSpill);
        Assert.Equal([PgTargetFactKeys.TempSpill, high.Key], spillStory.Path);
        /* The alias never appears in a path; the stored edge keeps it (no mutation of the singleton's edges). */
        Assert.DoesNotContain(stories, s => s.Path.Contains(PgTargetFactKeys.BadActorFamily));
        var stored = Assert.Single(graph.GetAllEdges(PgTargetFactKeys.TempSpill), e => PgTargetRelationshipGraph.IsBadActorAlias(e.Destination));
        Assert.Equal(PgTargetFactKeys.BadActorFamily, stored.Destination);

        /* And a second call over a different fact set resolves afresh — the copy, not the stored edge, carried
           the first answer. */
        var other = BadActor(99, severity: 0.9);
        var active = graph.GetActiveEdges(PgTargetFactKeys.TempSpill, Lookup(spill, other));
        Assert.Equal(other.Key, Assert.Single(active, e => e.Source == PgTargetFactKeys.TempSpill && e.Destination.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal)).Destination);
    }

    [Fact]
    public void AnEdgeIntoTheAlias_IsDroppedNotThrown_WhenNoBadActorIsPresent()
    {
        var graph = GraphWithAliasEdge();
        var spill = Spill(0.6);

        var active = graph.GetActiveEdges(PgTargetFactKeys.TempSpill, Lookup(spill));
        Assert.DoesNotContain(active, e => PgTargetRelationshipGraph.IsBadActorAlias(e.Destination));
        Assert.DoesNotContain(active, e => e.Destination.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal));

        var stories = new InferenceEngine(graph).BuildStories([spill]);
        var story = Assert.Single(stories, s => s.RootFactKey == PgTargetFactKeys.TempSpill);
        Assert.Equal([PgTargetFactKeys.TempSpill], story.Path);
    }

    [Fact]
    public void NonAliasEdges_PassThroughAsTheSameInstances_AndTheSqlServerGraphIsNotVirtualised()
    {
        var graph = new PgTargetRelationshipGraph();
        var knob = new Fact { Source = PgTargetSources.ConfigSource, Key = PgTargetFactKeys.ConfigWorkMem, Severity = 0.6, BaseSeverity = 0.4 };
        var spill = Spill(0.6);
        var stored = graph.GetAllEdges(PgTargetFactKeys.TempSpill);
        var active = graph.GetActiveEdges(PgTargetFactKeys.TempSpill, Lookup(spill, knob));
        Assert.All(active, e => Assert.Contains(e, stored));

        /* The seam: GetActiveEdges is virtual on the base and overridden only here; the SQL Server graph's own
           class does not override it, so that graph's behaviour is the base body's, byte for byte. */
        var baseMethod = typeof(RelationshipGraph).GetMethod("GetActiveEdges", BindingFlags.Instance | BindingFlags.Public)!;
        Assert.True(baseMethod.IsVirtual);
        Assert.Equal(typeof(PgTargetRelationshipGraph), typeof(PgTargetRelationshipGraph).GetMethod("GetActiveEdges", BindingFlags.Instance | BindingFlags.Public)!.DeclaringType);
        Assert.Equal(typeof(RelationshipGraph), typeof(RelationshipGraph).GetMethod("GetActiveEdges", BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)!.DeclaringType);
    }

    /* ───────────────────────── item 2: the compare_analysis identity prefix stays SQL Server's ───────────────────────── */

    [Fact]
    public void APgBadActor_TakesThePresencePath_InCompareAnalysis()
    {
        Assert.Equal("BAD_ACTOR_", ComparisonBanding.PlanCacheIdentityPrefix);
        Assert.False(ComparisonBanding.IsPlanCacheIdentityKey(PgTargetFactKeys.BadActorKey(42)));
        Assert.True(ComparisonBanding.IsPlanCacheIdentityKey("BAD_ACTOR_0x1234"));
    }

    /* ───────────────────────── item 3: autovacuum = off is a posture fact in the 0.9 band ───────────────────────── */

    [Fact]
    public void AutovacuumOff_ScoresThePostureBand_AndOnScoresZero()
    {
        Assert.Equal(0.9, PgTargetScorer.AutovacuumOffPostureSeverity);
        var off = Config(PgTargetFactKeys.ConfigAutovacuumOff, 1);
        Assert.Equal(0.9, PgTargetScorer.ScoreBase(off));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigAutovacuumOff, 0)));
        /* Engine-defined / posture, not a chosen number: no unmeasured stamp. */
        Assert.False(off.Metadata.ContainsKey("threshold_lineage"));
        Assert.True(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigAutovacuumOff));
    }

    [Fact]
    public void AutovacuumOff_RootsAloneAbove_TheIncidentLine_AndTheBacklogCoFireLiftsIt()
    {
        var off = Config(PgTargetFactKeys.ConfigAutovacuumOff, 1);
        new FactScorer().ScoreAll([off]);
        Assert.Equal(0.9, off.Severity, precision: 6);

        var fired = Config(PgTargetFactKeys.ConfigAutovacuumOff, 1);
        var backlog = Backlog(5.0, 4);
        new FactScorer().ScoreAll([fired, backlog]);
        Assert.True(backlog.BaseSeverity > 0, "the fixture backlog must fire for the co-fire to be tested");
        Assert.Equal(0.9 * 1.5, fired.Severity, precision: 6);
        Assert.Contains(fired.AmplifierResults, a => a.Matched && a.Description.StartsWith("PG_AUTOVACUUM_BACKLOG co-fired", StringComparison.Ordinal));
    }

    /* ───────────────────────── item 10: maintenance_work_mem is evidence-gated, the work_mem shape ───────────────────────── */

    [Fact]
    public void MaintWorkMem_IsZeroWithoutTheBacklogStamp_AndTheAdvisoryBaseAtOrPastTheTriggerLine()
    {
        Assert.Equal("autovacuum_backlog_ratio", PgTargetScorer.MaintWorkMemBacklogRatioKey);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigMaintWorkMem, 64)));

        var under = Config(PgTargetFactKeys.ConfigMaintWorkMem, 64);
        under.Metadata[PgTargetScorer.MaintWorkMemBacklogRatioKey] = 0.5;
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(under));

        var at = Config(PgTargetFactKeys.ConfigMaintWorkMem, 64);
        at.Metadata[PgTargetScorer.MaintWorkMemBacklogRatioKey] = 1.0;
        Assert.Equal(PgTargetScorer.ConfigAdvisoryBase, PgTargetScorer.ScoreBase(at));
        /* Engine-defined bar (the table's own trigger line): no unmeasured stamp. */
        Assert.False(at.Metadata.ContainsKey("threshold_lineage"));
        /* D5: never an advisory root — alone it roots nothing even when stamped. */
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigMaintWorkMem));
    }

    [Fact]
    public void MaintWorkMem_IsLiftedPastTheIncidentLine_OnlyBesideTheBacklogCoFire()
    {
        /* Stamped and the backlog fired: 0.4 × (1 + 0.5) = 0.6 — the vacuum chain's edge into the knob now has a
           destination that fired. */
        var lifted = Config(PgTargetFactKeys.ConfigMaintWorkMem, 64);
        lifted.Metadata[PgTargetScorer.MaintWorkMemBacklogRatioKey] = 5.0;
        var backlog = Backlog(5.0, 4);
        new FactScorer().ScoreAll([lifted, backlog]);
        Assert.Equal(0.6, lifted.Severity, precision: 6);

        /* Stamped but the backlog did NOT fire (not persistent enough): the base stands alone below the line. */
        var alone = Config(PgTargetFactKeys.ConfigMaintWorkMem, 64);
        alone.Metadata[PgTargetScorer.MaintWorkMemBacklogRatioKey] = 5.0;
        var flicker = Backlog(5.0, trailing: 1);
        new FactScorer().ScoreAll([alone, flicker]);
        Assert.Equal(0.0, flicker.BaseSeverity);
        Assert.Equal(0.4, alone.Severity, precision: 6);

        /* Unstamped beside a fired backlog: base 0, and FactScorer skips amplifiers on a base of 0 — the exact
           inertness this arm ends. */
        var inert = Config(PgTargetFactKeys.ConfigMaintWorkMem, 64);
        new FactScorer().ScoreAll([inert, Backlog(5.0, 4)]);
        Assert.Equal(0.0, inert.Severity);

        /* And the collector is where the stamp comes from — off the config fact, the seam Database.cs uses. */
        var collector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Vacuum.cs"));
        Assert.Contains("facts.Find(f => f.Key == PgTargetFactKeys.ConfigMaintWorkMem)", collector, StringComparison.Ordinal);
        Assert.Contains("Metadata[PgTargetScorer.MaintWorkMemBacklogRatioKey] = ratio", collector, StringComparison.Ordinal);
    }

    /* ───────────────────────── item 6: reserved_connections in the ceiling ───────────────────────── */

    [Fact]
    public void ReservedConnections_IsInTheSnapshotList_HasAKey_IsContext_AndTheCeilingSubtractsItWhenPresent()
    {
        Assert.Equal("CONFIG_PG_RESERVED_CONNECTIONS", PgTargetFactKeys.ConfigReservedConnections);
        Assert.Contains("'reserved_connections'", PgTargetFactCollector.PgTargetConfigSnapshotSql, StringComparison.Ordinal);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Config(PgTargetFactKeys.ConfigReservedConnections, 2)));
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigReservedConnections));

        var config = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Config.cs"));
        Assert.Contains("PgTargetFactKeys.ConfigReservedConnections", config, StringComparison.Ordinal);

        /* The ceiling line: the third fact is optional (pre-16: absent → 0) and subtracted alongside the two
           mandatory ones; the mandatory pair still gates emission on its own. */
        var sessions = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Sessions.cs"));
        Assert.Contains("facts.Find(f => f.Key == PgTargetFactKeys.ConfigReservedConnections)?.Value ?? 0", sessions, StringComparison.Ordinal);
        Assert.Contains("var usable = maxConnections.Value - reserved.Value - reservedForRole;", sessions, StringComparison.Ordinal);
        Assert.Contains("if (maxConnections is null || reserved is null)", sessions, StringComparison.Ordinal);
        Assert.Contains("pg_use_reserved_connections", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Sessions.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheSaturationAdvice_StatesTheThirdCarveOut_OnlyWhenItIsNonZero()
    {
        /* Without the setting (every pre-16 target, and the 16+ default): the sentence is byte-identical to lane 3's. */
        var without = Saturation(peak: 90, maxConnections: 100, superuserReserved: 3, reservedConnections: 0);
        var block = FactAdvice.Compose(PgTargetFactKeys.ConnectionSaturation, Lookup(without))!;
        Assert.Contains("90 sessions were connected against 97 usable connections — max_connections 100 minus superuser_reserved_connections 3 — that is 90 / (100 − 3) = 93%.", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("reserved_connections 0", block.Investigation, StringComparison.Ordinal);

        /* With it: the arithmetic the advice shows is the arithmetic the ceiling did. */
        var with = Saturation(peak: 90, maxConnections: 100, superuserReserved: 3, reservedConnections: 2);
        block = FactAdvice.Compose(PgTargetFactKeys.ConnectionSaturation, Lookup(with))!;
        Assert.Contains("90 sessions were connected against 95 usable connections — max_connections 100 minus superuser_reserved_connections 3 minus reserved_connections 2 — that is 90 / (100 − 3 − 2) = 95%.", block.Investigation, StringComparison.Ordinal);
    }

    /* ───────────────────────── item 8: the bad actor's first next-read ───────────────────────── */

    [Fact]
    public void TheBadActorNextReads_LeadWithTheDurationTrend_WhichIsARegisteredTool()
    {
        var recommendations = PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.BadActorKey(42))!;
        Assert.Equal("get_pg_query_duration_trend", recommendations[0].Tool);
        Assert.Contains("first question", recommendations[0].Reason, StringComparison.Ordinal);
        var registered = typeof(DarlingMcpTools).Assembly.GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
            .Where(n => n is not null)
            .ToHashSet(StringComparer.Ordinal);
        Assert.Contains("get_pg_query_duration_trend", registered);
        /* The advice names it as the first question — the recommendation and the prose agree. */
        Assert.Contains("get_pg_query_duration_trend", PgTargetAdvice.Static(PgTargetFactKeys.BadActorKey(42))!.Remediation, StringComparison.Ordinal);
    }

    /* ───────────────────────── item 11: the parked hook names the shipped trend key ───────────────────────── */

    [Fact]
    public void TheParkedOfferedVsDeliveredHook_NamesTpsTrendKey_NotALiteral()
    {
        var sessions = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Sessions.cs");
        Assert.Contains("tps.Metadata.GetValueOrDefault(TpsTrendKey) <= 0", sessions, StringComparison.Ordinal);
        Assert.DoesNotContain("GetValueOrDefault(\"trend\")", sessions, StringComparison.Ordinal);
        Assert.Equal("tps_trend", PgTargetScorer.TpsTrendKey);
    }

    /* ───────────────────────── fixtures ───────────────────────── */

    /// <summary>A <see cref="PgTargetRelationshipGraph"/> with one edge INTO the alias, registered through the
    /// protected <c>AddEdge</c> the content lanes use — the shape lane 6 / lane 9 will write, arranged here because
    /// no shipped chain names the alias yet.</summary>
    private static PgTargetRelationshipGraph GraphWithAliasEdge()
    {
        var graph = new PgTargetRelationshipGraph();
        var addEdge = typeof(RelationshipGraph).GetMethod("AddEdge", BindingFlags.Instance | BindingFlags.NonPublic)!;
        addEdge.Invoke(graph,
        [
            PgTargetFactKeys.TempSpill, PgTargetFactKeys.BadActorFamily, "temp_spill",
            "a top statement of the window wrote temp blocks",
            (Func<IReadOnlyDictionary<string, Fact>, bool>)(_ => true),
        ]);
        return graph;
    }

    private static IReadOnlyDictionary<string, Fact> Lookup(params Fact[] facts) => facts.ToFactLookup();

    private static Fact Config(string key, double value) =>
        new() { Source = PgTargetSources.ConfigSource, Key = key, Value = value, ServerId = 1 };

    private static Fact BadActor(long queryId, double severity) => new()
    {
        Source = PgTargetSources.QueriesSource,
        Key = PgTargetFactKeys.BadActorKey(queryId),
        Value = severity,
        ServerId = 1,
        BaseSeverity = severity,
        Severity = severity,
    };

    private static Fact Spill(double severity) => new()
    {
        Source = PgTargetSources.TempSource,
        Key = PgTargetFactKeys.TempSpill,
        Value = severity,
        ServerId = 1,
        BaseSeverity = severity,
        Severity = severity,
    };

    private static Fact Backlog(double ratio, int trailing) => new()
    {
        Source = PgTargetSources.VacuumSource,
        Key = PgTargetFactKeys.AutovacuumBacklog,
        Value = ratio,
        ServerId = 1,
        Metadata =
        {
            [PgTargetScorer.BacklogRatioKey] = ratio,
            [PgTargetScorer.BacklogTrailingSamplesKey] = trailing,
            [PgTargetScorer.BacklogSamplesInWindowKey] = 5,
            [PgTargetScorer.BacklogTablesKey] = 1,
        },
    };

    private static Fact Saturation(double peak, double maxConnections, double superuserReserved, double reservedConnections)
    {
        var usable = maxConnections - superuserReserved - reservedConnections;
        return new Fact
        {
            Source = PgTargetSources.SessionsSource,
            Key = PgTargetFactKeys.ConnectionSaturation,
            Value = peak / usable,
            ServerId = 1,
            Metadata =
            {
                ["saturation_ratio"] = peak / usable,
                ["peak_total_sessions"] = peak,
                ["peak_active_sessions"] = 60,
                ["peak_idle_in_transaction_sessions"] = 10,
                ["peak_other_sessions"] = peak - 70,
                ["peak_idle_in_transaction_share"] = 10 / peak,
                ["latest_total_sessions"] = peak,
                ["max_connections"] = maxConnections,
                ["superuser_reserved_connections"] = superuserReserved,
                ["reserved_connections"] = reservedConnections,
                ["usable_connections"] = usable,
                ["captures_with_rows"] = 240,
                ["rows_redacted_share"] = 0,
            },
        };
    }
}
