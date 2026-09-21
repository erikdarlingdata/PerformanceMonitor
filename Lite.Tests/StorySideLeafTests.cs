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
using System.Text.Json;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3691 (lane 42) — "the greedy walk must not orphan config leaves hanging off its path". The traversal in
/// <see cref="InferenceEngine.BuildStories"/> follows the SINGLE highest-severity active edge from each node, so a
/// mid-path node with two active edges reaches only one of them: the vacuum backlog walks to the wraparound trend
/// (and the hold chain behind it) and skips <c>CONFIG_PG_MAINT_WORK_MEM</c> — the lever that would relieve the
/// backlog. Un-visited meant un-consumed, so the lever then rooted its own one-node card at 0.6 beside the
/// incident it belongs to. These pins hold the four halves of the fix: the sweep attaches the lever as a SIDE LEAF
/// and consumes it, the traversal and therefore the incident identity are untouched, an incident-class side
/// destination still keeps its own root, and the payload/prose say so only when there is something to say.
/// </summary>
public class StorySideLeafTests
{
    /// <summary>A graph with exactly the edges the test names, none of the SQL Server chains underneath — the
    /// <c>StoryNamedHopsTests.ChainGraph</c> shape, because a side-leaf pin is about which edges EXIST from a node
    /// and the real chains would supply their own.</summary>
    private sealed class ChainGraph : RelationshipGraph
    {
        public ChainGraph(params (string From, string To)[] edges) : base(buildSqlServerEdges: false)
        {
            foreach (var (from, to) in edges)
                AddEdge(from, to, "test", "destination present", facts => facts.ContainsKey(to));
        }
    }

    /// <summary>A scored fact. <paramref name="source"/> is load-bearing here and not decoration: the sweep's
    /// advisory-class test reads the SOURCE (<c>InferenceEngine.IsConfigAdvisoryFact</c>), because the evidence-gated
    /// knob this lane exists to fix is NOT in the advisory-root set.</summary>
    private static Fact Scored(string key, double severity, string source = "waits") => new()
    {
        Source = source,
        Key = key,
        Value = 1,
        BaseSeverity = severity,
        Severity = severity,
    };

    /* ── the sweep: a config leaf off the MIDDLE of a three-node path ── */

    /// <summary>
    /// The reported shape, at engine level: ROOT → MID → TAIL is the walk (each hop the highest-severity edge from
    /// its node), and a config advisory hangs off MID by an equally-active edge the walk could not take. One story,
    /// the lever on it as a side leaf, the lever consumed so it roots nothing of its own.
    /// </summary>
    [Fact]
    public void AConfigLeafOffTheMiddleNode_IsAttachedAsASideLeaf_AndRootsNoSecondStory()
    {
        var graph = new ChainGraph(("ROOT", "MID"), ("MID", "TAIL"), ("MID", "CONFIG_MAXDOP"));
        var facts = new List<Fact>
        {
            Scored("ROOT", 1.2),
            Scored("MID", 1.0),
            Scored("TAIL", 0.9),
            /* 0.6 — below MID's other destination, so the walk takes TAIL and never visits this. */
            Scored("CONFIG_MAXDOP", 0.6, source: "config"),
        };

        var story = Assert.Single(new InferenceEngine(graph).BuildStories(facts));

        Assert.Equal(["ROOT", "MID", "TAIL"], story.Path);
        Assert.Equal(["CONFIG_MAXDOP"], story.SideLeafKeys);

        /* The path, the hash inputs, the leaf and the fact count are what they were: incident identity is stable,
           which is the property every existing story pin, mute row and occurrence count rests on. */
        Assert.Equal("ROOT → MID → TAIL", story.StoryPath);
        Assert.Equal("TAIL", story.LeafFactKey);
        Assert.Equal(3, story.FactCount);
        Assert.DoesNotContain("CONFIG_MAXDOP", story.Path);

        /* Context, not an amplifier: the side leaf does not lift the story. */
        Assert.Equal(1.2, story.Severity);
    }

    /// <summary>
    /// The same graph with the lever hanging off the TAIL instead: it is then simply the highest-severity
    /// destination from the tail, so the WALK takes it and it is ON the path — today's behaviour, unchanged. The
    /// sweep must not double-count a node the traversal already consumed.
    /// </summary>
    [Fact]
    public void AConfigLeafOffTheTailNode_IsStillWalkedOntoThePath_NotSweptBeside()
    {
        var graph = new ChainGraph(("ROOT", "MID"), ("MID", "TAIL"), ("TAIL", "CONFIG_MAXDOP"));
        var facts = new List<Fact>
        {
            Scored("ROOT", 1.2),
            Scored("MID", 1.0),
            Scored("TAIL", 0.9),
            Scored("CONFIG_MAXDOP", 0.6, source: "config"),
        };

        var story = Assert.Single(new InferenceEngine(graph).BuildStories(facts));

        Assert.Equal(["ROOT", "MID", "TAIL", "CONFIG_MAXDOP"], story.Path);
        Assert.Equal("CONFIG_MAXDOP", story.LeafFactKey);
        Assert.Empty(story.SideLeafKeys);
        Assert.Equal(4, story.FactCount);
    }

    /// <summary>
    /// An INCIDENT-class side destination is a different story and keeps its own root — the one line of this design
    /// that must not be widened. It has its own symptom, its own advice and its own occurrence history, and
    /// <see cref="InferenceEngine.ClusterIntoIncidents"/> is what re-merges it into one incident without taking its
    /// card away.
    /// </summary>
    [Fact]
    public void AnIncidentClassSideDestination_IsNotSwept_AndStillRootsItsOwnStory()
    {
        var graph = new ChainGraph(("ROOT", "MID"), ("MID", "TAIL"), ("MID", "SIDE_SYMPTOM"));
        var facts = new List<Fact>
        {
            Scored("ROOT", 1.2),
            Scored("MID", 1.0),
            Scored("TAIL", 0.9),
            /* A measured wait, above the incident line, off the middle node. */
            Scored("SIDE_SYMPTOM", 0.8),
        };

        var stories = new InferenceEngine(graph).BuildStories(facts);

        var chain = Assert.Single(stories, s => s.RootFactKey == "ROOT");
        Assert.Equal(["ROOT", "MID", "TAIL"], chain.Path);
        Assert.Empty(chain.SideLeafKeys);

        var side = Assert.Single(stories, s => s.RootFactKey == "SIDE_SYMPTOM");
        Assert.Equal(["SIDE_SYMPTOM"], side.Path);
        Assert.Equal(2, stories.Count);
    }

    /// <summary>
    /// A side destination at severity 0 is ignored. Two reasons, both structural: the working set the traversal
    /// walks holds only facts above zero, and a config fact at 0 is CONTEXT by the scorer's own D5 rule (the knob
    /// with no evidence stamped on it) — it had no card to lose.
    /// </summary>
    [Fact]
    public void AConfigSideLeafAtSeverityZero_IsIgnored()
    {
        var graph = new ChainGraph(("ROOT", "MID"), ("MID", "TAIL"), ("MID", "CONFIG_MAXDOP"));
        var facts = new List<Fact>
        {
            Scored("ROOT", 1.2),
            Scored("MID", 1.0),
            Scored("TAIL", 0.9),
            Scored("CONFIG_MAXDOP", 0.0, source: "config"),
        };

        var story = Assert.Single(new InferenceEngine(graph).BuildStories(facts));
        Assert.Equal(["ROOT", "MID", "TAIL"], story.Path);
        Assert.Empty(story.SideLeafKeys);
    }

    /// <summary>
    /// Two levers off two different nodes of one path: both attach, highest severity first (ties by ordinal key),
    /// so a pass is deterministic. Also the pin on the ordering being the ENGINE's and not the edge-declaration
    /// order — the sweep visits nodes in path order, which would have put the 0.4 first.
    /// </summary>
    [Fact]
    public void TwoLeversOffTwoNodes_BothAttach_HighestSeverityFirst()
    {
        var graph = new ChainGraph(("ROOT", "MID"), ("MID", "TAIL"),
            ("ROOT", "CONFIG_CTFP"), ("MID", "CONFIG_MAXDOP"));
        var facts = new List<Fact>
        {
            Scored("ROOT", 1.2),
            Scored("MID", 1.0),
            Scored("TAIL", 0.9),
            Scored("CONFIG_CTFP", 0.4, source: "config"),
            Scored("CONFIG_MAXDOP", 0.6, source: "config"),
        };

        var story = Assert.Single(new InferenceEngine(graph).BuildStories(facts));
        Assert.Equal(["ROOT", "MID", "TAIL"], story.Path);
        Assert.Equal(["CONFIG_MAXDOP", "CONFIG_CTFP"], story.SideLeafKeys);
    }

    /// <summary>
    /// A lever already consumed by an EARLIER (higher-severity) story is not swept onto a later one. The sweep's
    /// one <c>consumed</c> lookup is what makes this true, and it is the same set the traversal filters on — so
    /// "on this path", "consumed by a prior story" and "already swept from an earlier node of this same path" are
    /// one question with one answer.
    /// </summary>
    [Fact]
    public void ALeverConsumedByAnEarlierStory_IsNotSweptOntoALaterOne()
    {
        /* Two disjoint chains. The first (root 1.5) walks ROOT_A → CONFIG_MAXDOP, consuming the lever; the second
           (root 1.2) has an active edge to the same lever and must not claim it. */
        var graph = new ChainGraph(("ROOT_A", "CONFIG_MAXDOP"), ("ROOT_B", "MID_B"), ("MID_B", "CONFIG_MAXDOP"));
        var facts = new List<Fact>
        {
            Scored("ROOT_A", 1.5),
            Scored("ROOT_B", 1.2),
            Scored("MID_B", 1.0),
            Scored("CONFIG_MAXDOP", 0.6, source: "config"),
        };

        var stories = new InferenceEngine(graph).BuildStories(facts);
        Assert.Equal(2, stories.Count);

        var first = Assert.Single(stories, s => s.RootFactKey == "ROOT_A");
        Assert.Equal(["ROOT_A", "CONFIG_MAXDOP"], first.Path);
        Assert.Empty(first.SideLeafKeys);

        var second = Assert.Single(stories, s => s.RootFactKey == "ROOT_B");
        Assert.Equal(["ROOT_B", "MID_B"], second.Path);
        Assert.Empty(second.SideLeafKeys);
    }

    /* ── the class test is by SOURCE, and that is the whole reason the reported defect is caught ── */

    /// <summary>
    /// The PostgreSQL scenario from the exit check, at engine level with the REAL vacuum graph and the REAL
    /// keys: <c>PG_AUTOVACUUM_BACKLOG</c> → <c>PG_WRAPAROUND_TREND</c> → <c>PG_XMIN_HOLD</c> is the mesh walk,
    /// and <c>CONFIG_PG_MAINT_WORK_MEM</c> hangs off the BACKLOG by an active edge the walk cannot take. One
    /// card, the knob on it as a side leaf, no second card.
    ///
    /// <para><b>This is the case a key-list membership test would have MISSED</b>, which is why it is pinned with
    /// the real keys rather than the placeholders above. <c>CONFIG_PG_MAINT_WORK_MEM</c> is deliberately NOT in
    /// <c>PgTargetFactKeys.ConfigAdvisoryRoots</c> (D5: an evidence-gated knob may not root on a quiet server) —
    /// it takes the 0.4 advisory base only from the stamped backlog ratio and the backlog co-fire amplifier lifts
    /// it to 0.6, past the ORDINARY 0.5 incident threshold, so it roots a card without ever consulting the
    /// advisory-root set. The sweep therefore classifies by the fact's SOURCE (<c>pg_config</c>), which is the
    /// collector's own statement that it read a setting.</para>
    /// </summary>
    [Fact]
    public void ThePgVacuumChain_CarriesMaintWorkMemAsASideLeaf_TheKeyAnAdvisoryRootTestWouldMiss()
    {
        /* The knob is NOT an advisory root — the premise of this pin, asserted so a future change to that list
           cannot silently turn this test into a tautology. */
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.ConfigMaintWorkMem));

        var facts = new List<Fact>
        {
            Scored(PgTargetFactKeys.AutovacuumBacklog, 1.16, source: PgTargetSources.VacuumSource),
            Scored(PgTargetFactKeys.WraparoundTrend, 0.99, source: PgTargetSources.VacuumSource),
            Scored(PgTargetFactKeys.XminHold, 0.96, source: PgTargetSources.VacuumSource),
            /* 0.4 base × (1 + 0.5 backlog co-fire) = 0.6, the severity the face reported it rooting alone at. */
            Scored(PgTargetFactKeys.ConfigMaintWorkMem, 0.6, source: PgTargetSources.ConfigSource),
        };

        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);
        var story = Assert.Single(stories);

        Assert.Equal(
            [PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.WraparoundTrend, PgTargetFactKeys.XminHold],
            story.Path);
        Assert.Equal([PgTargetFactKeys.ConfigMaintWorkMem], story.SideLeafKeys);

        /* The story path — and so the hash, the mute key and the occurrence history — is byte-identical to what
           this chain produced before the sweep existed. */
        Assert.Equal(
            $"{PgTargetFactKeys.AutovacuumBacklog} → {PgTargetFactKeys.WraparoundTrend} → {PgTargetFactKeys.XminHold}",
            story.StoryPath);
        Assert.Equal(3, story.FactCount);
        Assert.Equal(PgTargetFactKeys.XminHold, story.LeafFactKey);
    }

    /* ── the SQL Server side: byte-identical, and provably so ── */

    /// <summary>
    /// <b>The SQL Server pass is unchanged, and this pin is the STRUCTURAL reason rather than a sample.</b>
    /// A side leaf requires a node with two active edges, one of them to a config advisory. In the SQL Server
    /// graph <c>DB_CONFIG</c> is the ONLY config-advisory destination any edge names, its only inbound edge is
    /// from <c>LCK_M_S</c>, and <c>LCK_M_S</c> declares exactly that one edge — so whenever the walk stands on
    /// <c>LCK_M_S</c> with <c>DB_CONFIG</c> fired, <c>DB_CONFIG</c> is the single candidate and goes ON the path,
    /// exactly as before. No SQL Server story can therefore carry a side leaf, and the sweep adds nothing to any
    /// SQL Server payload: the <c>analyze_server</c> bytes are identical, not merely equivalent.
    ///
    /// <para>Kept as a pin because it is the kind of claim that stops being true silently: the day someone adds a
    /// second edge out of <c>LCK_M_S</c>, or an edge into any <c>CONFIG_*</c> key, SQL Server gains side leaves and
    /// this test fails to say so — which is the review conversation that should happen, not a surprise in a
    /// payload diff. (The reverse direction, <c>DB_CONFIG → LCK_M_S</c>, is fine: a path rooted at
    /// <c>DB_CONFIG</c> reaches <c>LCK_M_S</c>, whose only edge points back at a node already on the path, which
    /// the sweep's one consumed-lookup rejects.)</para>
    /// </summary>
    [Fact]
    public void TheSqlServerGraph_CanProduceNoSideLeafAtAll_SoThatPassIsByteIdentical()
    {
        var graph = new RelationshipGraph();

        var fromLck = graph.GetAllEdges("LCK_M_S");
        Assert.Equal(["DB_CONFIG"], fromLck.Select(e => e.Destination));

        /* And with the chain actually fired, DB_CONFIG lands on the path — never beside it. */
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "LCK_M_S", Value = 0.10, BaseSeverity = 1.0, Severity = 1.0 },
            new() { Source = "database_config", Key = "DB_CONFIG", Value = 3, BaseSeverity = 0.3, Severity = 0.3,
                /* The edge predicate reads this seam, not mere presence — three databases without RCSI. */
                Metadata = new() { ["rcsi_off_count"] = 3 } },
        };
        var story = Assert.Single(new InferenceEngine(graph).BuildStories(facts));
        Assert.Equal(["LCK_M_S", "DB_CONFIG"], story.Path);
        Assert.Empty(story.SideLeafKeys);
    }

    /* ── the payload and the prose: only when there is something to say ── */

    [Fact]
    public void ToPayload_IsNullWithNoLevers_AndOneCardPerLeverOtherwise()
    {
        Assert.Null(StorySideLeaves.ToPayload(null));
        Assert.Null(StorySideLeaves.ToPayload([]));

        var json = JsonSerializer.Serialize(
            StorySideLeaves.ToPayload(["CONFIG_MAXDOP", "CONFIG_CTFP"]), McpHelpers.JsonOptions);
        using var doc = JsonDocument.Parse(json);
        var cards = doc.RootElement.EnumerateArray().ToList();
        Assert.Equal(2, cards.Count);
        Assert.Equal("CONFIG_MAXDOP", cards[0].GetProperty("key").GetString());
        Assert.Equal("CONFIG_CTFP", cards[1].GetProperty("key").GetString());

        /* Each card carries the key's own advice block, so a reader told a lever hangs off the story does not
           need a second tool call to learn what the lever says. */
        var advice = cards[0].GetProperty("advice");
        Assert.Equal(FactAdvice.GetForFactKey("CONFIG_MAXDOP")!.Headline, advice.GetProperty("headline").GetString());
        Assert.False(string.IsNullOrWhiteSpace(advice.GetProperty("remediation").GetString()));
    }

    /// <summary>
    /// <see cref="StorySideLeaves.Attach"/> returns the caller's payload object UNTOUCHED when no lever hangs off
    /// the finding — the <c>CollectionCaveats.Attach</c> discipline, and the reason the SQL Server pass stays
    /// byte-identical: the tools' serializer writes nulls (the payload's own <c>leaf_fact</c> proves it), so a
    /// <c>side_leaves: null</c> property would have been a byte change on every finding of every clean pass.
    /// </summary>
    [Fact]
    public void Attach_ReturnsTheSameObjectWithNoLevers_AndAppendsSideLeavesLastOtherwise()
    {
        var payload = new { severity = 1.2, story_path = "ROOT → MID" };

        Assert.Same(payload, StorySideLeaves.Attach(payload, null, McpHelpers.JsonOptions));
        Assert.Same(payload, StorySideLeaves.Attach(payload, [], McpHelpers.JsonOptions));

        var attached = StorySideLeaves.Attach(payload, ["CONFIG_MAXDOP"], McpHelpers.JsonOptions);
        Assert.NotSame(payload, attached);
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(attached, McpHelpers.JsonOptions));
        var names = doc.RootElement.EnumerateObject().Select(p => p.Name).ToList();
        Assert.Equal(["severity", "story_path", "side_leaves"], names);
        Assert.Equal("CONFIG_MAXDOP",
            doc.RootElement.GetProperty("side_leaves")[0].GetProperty("key").GetString());
    }

    [Fact]
    public void Sentence_IsNullWithNoLevers_AndNamesThemOnceOtherwise()
    {
        Assert.Null(StorySideLeaves.Sentence(null));
        Assert.Null(StorySideLeaves.Sentence([]));

        Assert.Equal(
            $" {StorySideLeaves.SentenceMarker} `CONFIG_PG_MAINT_WORK_MEM` — see its card.",
            StorySideLeaves.Sentence([PgTargetFactKeys.ConfigMaintWorkMem]));
        Assert.Equal(
            $" {StorySideLeaves.SentenceMarker} `CONFIG_MAXDOP`, `CONFIG_CTFP` — see their cards.",
            StorySideLeaves.Sentence(["CONFIG_MAXDOP", "CONFIG_CTFP"]));
    }

    /// <summary>
    /// The composer appends the sentence to the root's INVESTIGATION and leaves headline and remediation alone —
    /// the lever's fix is the lever's, in its own family's words, on its own card. And a story with no lever
    /// freezes exactly what <see cref="FactAdvice.Compose"/> returns: the byte-identity arm for every chain this
    /// does not concern, which is nearly all of them.
    /// </summary>
    [Fact]
    public void PopulateStoryText_AppendsTheOneSentence_AndIsByteIdenticalWithoutALever()
    {
        var facts = new List<Fact>
        {
            Scored("CXPACKET", 1.2),
            Scored("SOS_SCHEDULER_YIELD", 1.0),
            Scored("CONFIG_MAXDOP", 0.6, source: "config"),
        };
        var byKey = facts.ToFactLookup();
        var untouched = FactAdvice.Compose("CXPACKET", byKey)!;

        var withLever = new AnalysisStory
        {
            RootFactKey = "CXPACKET",
            Path = ["CXPACKET", "SOS_SCHEDULER_YIELD"],
            SideLeafKeys = ["CONFIG_MAXDOP"],
        };
        FactAdvice.PopulateStoryText([withLever], facts);
        var advice = FactAdvice.TryReadStoryText(withLever.StoryText)!;

        Assert.Equal(untouched.Headline, advice.Headline);
        Assert.Equal(untouched.Remediation, advice.Remediation);
        Assert.Equal(untouched.Investigation + StorySideLeaves.Sentence(["CONFIG_MAXDOP"]), advice.Investigation);

        /* No lever: byte-identical to the composed root block, no marker anywhere in it. */
        var without = new AnalysisStory { RootFactKey = "CXPACKET", Path = ["CXPACKET", "SOS_SCHEDULER_YIELD"] };
        FactAdvice.PopulateStoryText([without], facts);
        Assert.Equal(FactAdvice.SerializeForStoryText(untouched), without.StoryText);
        Assert.DoesNotContain(StorySideLeaves.SentenceMarker, without.StoryText, StringComparison.Ordinal);
    }

    /* ── clustering: the lever is consumed, so nothing keys on the orphan's presence ── */

    /// <summary>
    /// <see cref="InferenceEngine.ClusterIntoIncidents"/> keys on each story's <see cref="AnalysisStory.Path"/>, and
    /// a side leaf is deliberately not in it — so the lever's fact is owned by NO story for clustering purposes,
    /// and the incident it belongs to is the one the chain formed, unchanged. Before this change the lever was a
    /// second story that the union-find merged into the same incident by the very edge the walk skipped; the
    /// incident is therefore the same incident, with one card instead of two.
    /// </summary>
    [Fact]
    public void ClusterIntoIncidents_SeesOneIncident_AndNothingKeysOnTheOrphansPresence()
    {
        var graph = new ChainGraph(("ROOT", "MID"), ("MID", "TAIL"), ("MID", "CONFIG_MAXDOP"));
        var facts = new List<Fact>
        {
            Scored("ROOT", 1.2),
            Scored("MID", 1.0),
            Scored("TAIL", 0.9),
            Scored("CONFIG_MAXDOP", 0.6, source: "config"),
        };

        var engine = new InferenceEngine(graph);
        var stories = engine.BuildStories(facts);
        var incident = Assert.Single(engine.ClusterIntoIncidents(stories, facts));
        var only = Assert.Single(incident);
        Assert.Equal("ROOT", only.RootFactKey);
        Assert.Equal(["CONFIG_MAXDOP"], only.SideLeafKeys);
    }
}
