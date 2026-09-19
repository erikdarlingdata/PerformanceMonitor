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
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3691 — "a CRITICAL leaf consumed mid-chain is never NAMED by the root". The greedy traversal consumes a
/// hop and roots no card for it, so whatever that hop named (the lead-blocker session, the Agent job, the
/// application that parked a transaction) reached no surface. <see cref="InferenceEngine.BuildStories"/> now
/// records the identity-bearing hops on the story (<see cref="AnalysisStory.NamedHops"/>) and
/// <see cref="FactAdvice.PopulateStoryText"/> composes one "Behind it:" sentence per hop onto the root's
/// investigation. These pins hold the SQL Server side of the contract: the roster is explicit, the identity is
/// read off the seams the collectors already fill, the cap and order are the engine's, and a chain with no
/// identity-bearing hop is byte-identical to what it was.
/// </summary>
public class StoryNamedHopsTests
{
    /* ── the roster and the seams ── */

    [Fact]
    public void TheSqlServerRoster_IsExplicit_AndTheseAreItsMembers()
    {
        /* A key joins by a one-row edit in FactIdentity that says what it names; this pin is the reviewer's
           prompt to read that row. Exact set, exact prefix. */
        Assert.Equal(["BLOCKING_CHAIN", "RUNNING_JOBS", "ANOMALY_OBJECT_GROWTH", "ANOMALY_OBJECT_CONTENTION"], FactIdentity.SqlServerKeys);
        Assert.Equal("BAD_ACTOR_", FactIdentity.SqlServerBadActorPrefix);
        Assert.All(FactIdentity.SqlServerKeys, k => Assert.True(FactIdentity.IsIdentityBearingKey(k)));
        Assert.True(FactIdentity.IsIdentityBearingKey("BAD_ACTOR_0x1234"));
        Assert.False(FactIdentity.IsIdentityBearingKey("BAD_ACTOR_"));
        Assert.False(FactIdentity.IsIdentityBearingKey("LCK"));
        Assert.False(FactIdentity.IsIdentityBearingKey(null));
        Assert.Equal(3, FactIdentity.MaxNamedHops);
    }

    [Fact]
    public void Describe_ReadsEachSqlServerIdentityOffTheSeamItsCollectorFills()
    {
        Assert.Equal("lead-blocker session 57, sleeping",
            FactIdentity.Describe(new Fact { Key = "BLOCKING_CHAIN", Metadata = new() { ["worst_apex_spid"] = 57, ["worst_apex_sleeping"] = 1 } }));
        Assert.Equal("lead-blocker session 57",
            FactIdentity.Describe(new Fact { Key = "BLOCKING_CHAIN", Metadata = new() { ["worst_apex_spid"] = 57 } }));
        Assert.Equal("Agent job `Nightly Index Maintenance`",
            FactIdentity.Describe(new Fact { Key = "RUNNING_JOBS", ObjectName = "Nightly Index Maintenance" }));
        Assert.Equal("table `dbo.Orders` in `Sales`",
            FactIdentity.Describe(new Fact { Key = "ANOMALY_OBJECT_GROWTH", ObjectName = "dbo.Orders", DatabaseName = "Sales" }));
        Assert.Equal("table `dbo.Orders`",
            FactIdentity.Describe(new Fact { Key = "ANOMALY_OBJECT_CONTENTION", ObjectName = "dbo.Orders" }));
        Assert.Equal("query hash `0xABCD` in `Sales`",
            FactIdentity.Describe(new Fact { Key = "BAD_ACTOR_0xABCD", DatabaseName = "Sales" }));
    }

    [Fact]
    public void Describe_IsNullForAnEmptySeam_AndForAKeyOffTheRoster()
    {
        /* On the roster, nothing to name: a chain with no apex, a job fact with no name. */
        Assert.Null(FactIdentity.Describe(new Fact { Key = "BLOCKING_CHAIN" }));
        Assert.Null(FactIdentity.Describe(new Fact { Key = "BLOCKING_CHAIN", Metadata = new() { ["worst_apex_spid"] = 0 } }));
        Assert.Null(FactIdentity.Describe(new Fact { Key = "RUNNING_JOBS", ObjectName = "  " }));
        Assert.Null(FactIdentity.Describe(new Fact { Key = "BAD_ACTOR_" }));
        /* Off the roster, an ObjectName is not an identity: the config-change fact carries setting names there,
           a wait fact its display name. The roster is the contract, not the seam. */
        Assert.Null(FactIdentity.Describe(new Fact { Key = ConfigChangeAttribution.FactKey, ObjectName = "max degree of parallelism" }));
        Assert.Null(FactIdentity.Describe(new Fact { Key = "SOS_SCHEDULER_YIELD", ObjectName = "SOS_SCHEDULER_YIELD" }));
        Assert.Null(FactIdentity.Describe(null));
    }

    /* ── the engine records, ranks and caps ── */

    /// <summary>A graph with exactly the edges the test names, none of the SQL Server chains underneath.</summary>
    private sealed class ChainGraph : RelationshipGraph
    {
        public ChainGraph(params (string From, string To)[] edges) : base(buildSqlServerEdges: false)
        {
            foreach (var (from, to) in edges)
                AddEdge(from, to, "test", "destination present", facts => facts.ContainsKey(to));
        }
    }

    private static Fact Scored(string key, double severity, string? objectName = null, string? database = null, Dictionary<string, double>? metadata = null) => new()
    {
        Source = "test",
        Key = key,
        Value = 1,
        BaseSeverity = severity,
        Severity = severity,
        ObjectName = objectName,
        DatabaseName = database,
        Metadata = metadata ?? [],
    };

    [Fact]
    public void BuildStories_RecordsTheIdentityBearingHops_HighestSeverityFirst_CappedAtThree_RootExcluded()
    {
        /* ROOT (1.0, names nothing) → five hops in path order: four name something at 0.6 / 0.9 / 0.7 / 0.8 and one
           (a plain wait) does not. The engine keeps the three highest, in severity order, and never the root. */
        var graph = new ChainGraph(("CXPACKET", "RUNNING_JOBS"), ("RUNNING_JOBS", "SOS_SCHEDULER_YIELD"),
            ("SOS_SCHEDULER_YIELD", "BAD_ACTOR_0xF00D"), ("BAD_ACTOR_0xF00D", "ANOMALY_OBJECT_GROWTH"), ("ANOMALY_OBJECT_GROWTH", "BLOCKING_CHAIN"));
        var facts = new List<Fact>
        {
            Scored("CXPACKET", 1.0),
            Scored("RUNNING_JOBS", 0.6, objectName: "Nightly Index Maintenance"),
            Scored("SOS_SCHEDULER_YIELD", 0.95),
            Scored("BAD_ACTOR_0xF00D", 0.9, database: "Sales"),
            Scored("ANOMALY_OBJECT_GROWTH", 0.7, objectName: "dbo.Orders", database: "Sales"),
            Scored("BLOCKING_CHAIN", 0.8, metadata: new() { ["worst_apex_spid"] = 57 }),
        };

        var story = Assert.Single(new InferenceEngine(graph).BuildStories(facts));
        Assert.Equal(["CXPACKET", "RUNNING_JOBS", "SOS_SCHEDULER_YIELD", "BAD_ACTOR_0xF00D", "ANOMALY_OBJECT_GROWTH", "BLOCKING_CHAIN"], story.Path);

        Assert.Equal(3, story.NamedHops.Count);
        Assert.Equal(new NamedHop("BAD_ACTOR_0xF00D", "query hash `0xF00D` in `Sales`", 0.9), story.NamedHops[0]);
        Assert.Equal(new NamedHop("BLOCKING_CHAIN", "lead-blocker session 57", 0.8), story.NamedHops[1]);
        Assert.Equal(new NamedHop("ANOMALY_OBJECT_GROWTH", "table `dbo.Orders` in `Sales`", 0.7), story.NamedHops[2]);
        Assert.DoesNotContain(story.NamedHops, h => h.Key == "RUNNING_JOBS");          // fourth by severity: capped
        Assert.DoesNotContain(story.NamedHops, h => h.Key == "SOS_SCHEDULER_YIELD");   // names nothing
        Assert.DoesNotContain(story.NamedHops, h => h.Key == "CXPACKET");              // the root names itself

        /* The traversal itself is untouched by the recording: same path, same severity, same hash inputs. */
        Assert.Equal(1.0, story.Severity);
        Assert.Equal(string.Join(" → ", story.Path), story.StoryPath);
    }

    [Fact]
    public void BuildStories_AnIdentityBearingROOT_IsNotItsOwnNamedHop_AndAOneNodeStoryHasNone()
    {
        var facts = new List<Fact> { Scored("RUNNING_JOBS", 0.9, objectName: "Nightly Index Maintenance") };
        var story = Assert.Single(new InferenceEngine(new ChainGraph()).BuildStories(facts));
        Assert.Equal(["RUNNING_JOBS"], story.Path);
        Assert.Empty(story.NamedHops);

        /* Absolution carries none either. */
        var absolution = Assert.Single(new InferenceEngine(new ChainGraph()).BuildStories([Scored("CXPACKET", 0.1)]));
        Assert.True(absolution.IsAbsolution);
        Assert.Empty(absolution.NamedHops);
    }

    /* ── the composer says it, once per hop, and says nothing when there is nothing to say ── */

    private static string Investigation(AnalysisStory story) => FactAdvice.TryReadStoryText(story.StoryText)!.Investigation;

    [Fact]
    public void PopulateStoryText_AppendsOneBehindItSentencePerNamedHop_InTheEnginesOrder_ToTheRootsInvestigation()
    {
        var graph = new ChainGraph(("CXPACKET", "BLOCKING_CHAIN"), ("BLOCKING_CHAIN", "RUNNING_JOBS"));
        var facts = new List<Fact>
        {
            Scored("CXPACKET", 1.0),
            Scored("BLOCKING_CHAIN", 0.7, metadata: new() { ["worst_apex_spid"] = 57, ["worst_apex_sleeping"] = 1, ["worst_chain_depth"] = 4, ["worst_chain_victim_count"] = 6 }),
            Scored("RUNNING_JOBS", 0.8, objectName: "Nightly Index Maintenance", metadata: new() { ["running_count"] = 1, ["running_long_count"] = 1, ["max_percent_of_average"] = 400, ["max_duration_seconds"] = 6_960 }),
        };
        var story = Assert.Single(new InferenceEngine(graph).BuildStories(facts));
        var untouched = FactAdvice.Compose("CXPACKET", facts.ToFactLookup())!;

        FactAdvice.PopulateStoryText([story], facts);
        var advice = FactAdvice.TryReadStoryText(story.StoryText)!;

        /* Headline and remediation are the root's own; only the investigation grew, by exactly two sentences. */
        Assert.Equal(untouched.Headline, advice.Headline);
        Assert.Equal(untouched.Remediation, advice.Remediation);
        Assert.StartsWith(untouched.Investigation, advice.Investigation, StringComparison.Ordinal);
        var appended = advice.Investigation.Substring(untouched.Investigation.Length);
        Assert.Equal(2, appended.Split(FactIdentity.SentenceMarker).Length - 1);

        /* Higher severity first: the job (0.8) before the chain (0.7). One shape for both: the identity is the
           label, the hop's own composed headline follows it. */
        var byKey = facts.ToFactLookup();
        var jobHeadline = FactAdvice.Compose("RUNNING_JOBS", byKey)!.Headline;
        var chainHeadline = FactAdvice.Compose("BLOCKING_CHAIN", byKey)!.Headline;
        Assert.Equal(
            $" {FactIdentity.SentenceMarker} `RUNNING_JOBS` (severity 0.80) — Agent job `Nightly Index Maintenance`: {jobHeadline}." +
            $" {FactIdentity.SentenceMarker} `BLOCKING_CHAIN` (severity 0.70) — lead-blocker session 57, sleeping: {chainHeadline}.",
            appended);

        /* A hop with no composable advice gets the identity alone; a headline that already ends a sentence is not
           given a second full stop. */
        Assert.Equal(" Behind it: `RUNNING_JOBS` (severity 0.80) — Agent job `X`.", FactIdentity.Sentence(new NamedHop("RUNNING_JOBS", "Agent job `X`", 0.8), null));
        Assert.Equal(" Behind it: `RUNNING_JOBS` (severity 0.80) — Agent job `X`: Stuck?", FactIdentity.Sentence(new NamedHop("RUNNING_JOBS", "Agent job `X`", 0.8), " Stuck? "));
    }

    [Fact]
    public void PopulateStoryText_AChainWithNoIdentityBearingHop_IsByteIdenticalToTheComposedRootBlock()
    {
        /* The byte-identity pin for every story this does not concern: a root that walks to a hop naming nothing
           freezes exactly what Compose returns for the root — no marker, no trailing space, nothing. */
        var graph = new ChainGraph(("CXPACKET", "SOS_SCHEDULER_YIELD"));
        var facts = new List<Fact> { Scored("CXPACKET", 1.0), Scored("SOS_SCHEDULER_YIELD", 0.8) };
        var story = Assert.Single(new InferenceEngine(graph).BuildStories(facts));
        Assert.Equal(["CXPACKET", "SOS_SCHEDULER_YIELD"], story.Path);
        Assert.Empty(story.NamedHops);

        FactAdvice.PopulateStoryText([story], facts);
        Assert.Equal(FactAdvice.SerializeForStoryText(FactAdvice.Compose("CXPACKET", facts.ToFactLookup())), story.StoryText);
        Assert.DoesNotContain(FactIdentity.SentenceMarker, story.StoryText, StringComparison.Ordinal);

        /* And a hand-built story (no engine, no NamedHops) composes as it always did. */
        var handBuilt = new AnalysisStory { RootFactKey = "CXPACKET", Path = ["CXPACKET", "BLOCKING_CHAIN"] };
        FactAdvice.PopulateStoryText([handBuilt], facts);
        Assert.Equal(FactAdvice.SerializeForStoryText(FactAdvice.Compose("CXPACKET", facts.ToFactLookup())), handBuilt.StoryText);
    }

    /* ── the real SQL Server shape, through the real scorer and graph ── */

    /// <summary>
    /// The SQL Server twin of the exit check's PostgreSQL defect: LCK at 1.0 (a tenth of the window in lock waits)
    /// walks to BLOCKING_CHAIN at 0.7 (depth 3, sleeping apex ×1.4) and consumes it — before this change the only
    /// card said "lock waits" and the session at the head of the chain was named nowhere. Now the LCK card's
    /// investigation names the lead blocker, with the chain composer's headline behind it.
    /// </summary>
    [Fact]
    public void LckRootConsumingABlockingChain_NamesTheLeadBlockerOnTheLckCard()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "LCK", Value = 0.10, ServerId = 1, Metadata = new() { ["wait_time_ms"] = 360_000, ["waiting_tasks_count"] = 1_200, ["avg_ms_per_wait"] = 300, ["period_duration_ms"] = 3_600_000 } },
            new() { Source = "blocking", Key = "BLOCKING_CHAIN", Value = 3, ServerId = 1, Metadata = new()
                {
                    ["worst_chain_depth"] = 3, ["worst_chain_victim_count"] = 2, ["worst_apex_spid"] = 57, ["worst_apex_sleeping"] = 1,
                    ["worst_chain_max_wait_ms"] = 45_000, ["total_reconstructed_chains"] = 1, ["deepest_chain_overall"] = 3,
                    ["max_victim_count_overall"] = 2, ["depth_capped"] = 0, ["traversal_truncated"] = 0, ["cycle_detected"] = 0,
                } },
        };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(1.0, facts[0].Severity, 9);
        Assert.Equal(0.7, facts[1].Severity, 9);

        var engine = new InferenceEngine(new RelationshipGraph());
        var story = Assert.Single(engine.BuildStories(facts));
        Assert.Equal(["LCK", "BLOCKING_CHAIN"], story.Path);
        var hop = Assert.Single(story.NamedHops);
        Assert.Equal(new NamedHop("BLOCKING_CHAIN", "lead-blocker session 57, sleeping", 0.7), hop);

        FactAdvice.PopulateStoryText([story], facts);
        var investigation = Investigation(story);
        Assert.Contains($" {FactIdentity.SentenceMarker} `BLOCKING_CHAIN` (severity 0.70) — lead-blocker session 57, sleeping: ", investigation, StringComparison.Ordinal);
        Assert.Contains("57", investigation, StringComparison.Ordinal);
        Assert.Equal(1, investigation.Split(FactIdentity.SentenceMarker).Length - 1);
    }
}
