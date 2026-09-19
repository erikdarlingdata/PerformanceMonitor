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
/// #3538 A6 + A9 — the two engine changes that make a finding's <c>confidence</c> mean corroboration and
/// make the nightly maintenance window one incident.
///
/// <para><b>A6.</b> Confidence was <c>(n-1)/n</c> over the story path with a lone symptom at 1.0 — the
/// LEAST evidenced finding carried the HIGHEST confidence, exported under an evidence name. It is now an
/// evidence statistic (<see cref="StoryConfidence"/>): a floor for the fired symptom, the share of the root
/// fact's amplifier checks that matched, and the depth of the traversed chain. These pins hold the
/// properties the formula was chosen for — monotonic in corroboration, lone symptom below any corroborated
/// chain, never 1.0, the worked examples in the class remarks — and the one property the read-time basis
/// label DEPENDS on: the new formula never lands on a legacy value for the same path length, which is
/// what lets <c>confidence_basis</c> call a pre-change row "path-shape" without a schema marker.</para>
///
/// <para><b>A9.</b> Nothing in the relationship graph touched SCH_M or RUNNING_JOBS, so the rebuild's
/// schema-lock waits, its long-running job and its write-latency / log-flush pair were two or three
/// unlinked single-node cards every night. Three symptom → job edges, gated on the job having FIRED,
/// let <see cref="InferenceEngine.ClusterIntoIncidents"/> fold them into one incident; the same
/// predicate drives the advice sentence that names the job on each card.</para>
///
/// <para>Pure logic over hand-built facts — no store, no fixture — so every value here was also executed
/// on the development machine before CI (the brief's execute-your-pins rule), not just compiled.</para>
/// </summary>
public sealed class StoryConfidenceTests
{
    /* ── A6: the formula's properties ── */

    /// <summary>
    /// One more matched amplifier, or one more node on the path, never LOWERS confidence — the property
    /// that makes "more corroboration" and "higher confidence" the same direction, which the legacy
    /// formula inverted at the lone-symptom step. Enumerated over every catalogue size a real key has
    /// (the largest amplifier list is six entries; twelve is headroom) and every path length the traversal
    /// can build — the root plus MaxPathDepth hops, i.e. <see cref="StoryConfidence.MaxPathNodes"/> (11),
    /// not a round ten: the engine's depth constant counts HOPS, and the first review of this lane caught
    /// the off-by-one in the pins' upper bound.
    /// </summary>
    [Fact]
    public void Compute_IsMonotonic_InMatchedAmplifiers_AndInPathDepth()
    {
        for (var defined = 0; defined <= 12; defined++)
        for (var n = 1; n <= StoryConfidence.MaxPathNodes; n++)
        for (var matched = 0; matched <= defined; matched++)
        {
            var here = StoryConfidence.Compute(matched, defined, n);
            if (matched < defined)
                Assert.True(StoryConfidence.Compute(matched + 1, defined, n) >= here,
                    $"matching one more amplifier lowered confidence at matched={matched} defined={defined} n={n}");
            Assert.True(StoryConfidence.Compute(matched, defined, n + 1) >= here,
                $"one more path node lowered confidence at matched={matched} defined={defined} n={n}");
        }
    }

    /// <summary>
    /// The A6 defect, stated as the ordering it violated: a lone symptom with no corroboration sits at
    /// the floor (0.20) whatever its catalogue size, and BELOW every corroborated shape — a two-node
    /// chain with nothing else, a lone symptom with one match, a two-node chain on a catalogued key
    /// with nothing matched. Under the old formula the lone symptom was 1.0 and outranked all of them.
    /// </summary>
    [Fact]
    public void Compute_LoneUncorroboratedSymptom_SitsAtTheFloor_BelowEveryCorroboratedShape()
    {
        for (var defined = 0; defined <= 12; defined++)
            Assert.Equal(StoryConfidence.Floor, StoryConfidence.Compute(0, defined, 1), precision: 9);

        var lone = StoryConfidence.Compute(0, 3, 1);
        Assert.True(StoryConfidence.Compute(0, 0, 2) > lone, "an uncatalogued two-node chain must beat a lone symptom");
        Assert.True(StoryConfidence.Compute(1, 3, 1) > lone, "one matched amplifier must beat none");
        Assert.True(StoryConfidence.Compute(0, 3, 2) > lone, "a catalogued two-node chain with no matches must still beat a lone symptom");
    }

    /// <summary>
    /// Nothing the formula builds is 1.0: path depth (n-1)/n never reaches 1, so even a fully matched
    /// catalogue on the deepest path the engine allows stays under it. 1.0 is reserved for the two
    /// by-construction stories (absolution, the pileup detector) and for legacy rows — which is what
    /// lets a lone-symptom 1.0 be recognised as legacy at read time.
    /// </summary>
    [Fact]
    public void Compute_NeverReachesOne()
    {
        var max = 0.0;
        for (var defined = 0; defined <= 12; defined++)
        for (var n = 1; n <= StoryConfidence.MaxPathNodes; n++)
            max = Math.Max(max, StoryConfidence.Compute(defined, defined, n));
        Assert.True(max < 1.0, $"the formula reached {max}");
        /* The two ceilings at the deepest path the traversal builds (11 nodes = root + 10 hops):
           uncatalogued 0.20 + 0.80 × 10/11, catalogued 0.20 + 0.48 + 0.32 × 10/11. */
        Assert.Equal(11, StoryConfidence.MaxPathNodes);
        Assert.Equal(0.2 + 0.8 * 10.0 / 11.0, StoryConfidence.Compute(0, 0, StoryConfidence.MaxPathNodes), precision: 9);
        Assert.Equal(0.2 + 0.48 + 0.32 * 10.0 / 11.0, StoryConfidence.Compute(5, 5, StoryConfidence.MaxPathNodes), precision: 9);
    }

    /// <summary>
    /// The worked examples from the <see cref="StoryConfidence"/> remarks, as the class states them.
    /// A comment that quotes numbers is a claim; this is the claim executed.
    /// </summary>
    [Theory]
    [InlineData(0, 0, 1, 0.20)]     // lone SCH_M: no catalogue, one node — the floor
    [InlineData(0, 3, 1, 0.20)]     // lone PAGEIOLATCH_SH, 0 of 3 matched — the floor
    [InlineData(3, 3, 1, 0.68)]     // lone PAGEIOLATCH_SH, 3 of 3 matched: 0.20 + 0.48
    [InlineData(2, 3, 3, 0.7333333333)] // PAGEIOLATCH_SH → RESOURCE_SEMAPHORE → MEMORY_GRANT_PENDING, 2 of 3: 0.20 + 0.32 + 0.32 × 2/3
    [InlineData(0, 0, 2, 0.60)]     // SCH_M → RUNNING_JOBS: no catalogue, two nodes: 0.20 + 0.80 × 0.5
    [InlineData(0, 0, 10, 0.92)]    // ten-node uncatalogued chain: 0.20 + 0.80 × 0.9
    [InlineData(0, 5, 10, 0.488)]   // ten-node chain whose five checks all came back false: 0.20 + 0.32 × 0.9
    public void Compute_WorkedExamples(int matched, int defined, int pathLength, double expected)
    {
        Assert.Equal(expected, StoryConfidence.Compute(matched, defined, pathLength), precision: 9);
    }

    /// <summary>
    /// The property the read-time basis label rests on: for every path length the engine can build and
    /// every catalogue size a key can have, the new value is never the legacy path-shape value for that
    /// same length. Without this, <c>confidence_basis</c> could call a fresh corroboration score
    /// "path-shape (pre-#3538)" — or a legacy row corroborated — and a schema marker would be the only
    /// honest alternative (a rung this campaign is not permitted). The legacy shape is exactly 1.0 or
    /// (n-1)/n; the formula's 0.20 floor keeps every lone value away from 1.0, and no small-integer
    /// amplifier share puts a multi-node value on (n-1)/n. Enumerated to the engine's REAL maximum path
    /// (<see cref="StoryConfidence.MaxPathNodes"/> = root + MaxPathDepth hops = 11), not to ten.
    /// </summary>
    [Fact]
    public void Compute_NeverCollidesWithTheLegacyPathShape_SoTheBasisCanBeReDerivedAtReadTime()
    {
        for (var n = 1; n <= StoryConfidence.MaxPathNodes; n++)
        for (var defined = 0; defined <= 12; defined++)
        for (var matched = 0; matched <= defined; matched++)
        {
            var value = StoryConfidence.Compute(matched, defined, n);
            Assert.False(StoryConfidence.IsLegacyPathShape(value, n),
                $"Compute({matched},{defined},{n}) = {value} equals the legacy path-shape value for n={n}");
        }
    }

    /* ── A6: the basis string's arms ── */

    [Fact]
    public void DescribeBasis_LegacyRows_AreLabelledPathShape_AndNeverReadAsCorroborated()
    {
        var lone = StoryConfidence.DescribeBasis("SCH_M", 1.0, 1);
        Assert.Contains("path-shape (pre-#3538)", lone, StringComparison.Ordinal);
        Assert.Contains("UNCORROBORATED", lone, StringComparison.Ordinal);
        Assert.DoesNotContain("corroboration (#3538)", lone, StringComparison.Ordinal);

        /* The multi-node legacy values too: (n-1)/n for n = 2, 3, 4. */
        Assert.Contains("path-shape (pre-#3538)", StoryConfidence.DescribeBasis("CXPACKET", 0.5, 2), StringComparison.Ordinal);
        Assert.Contains("path-shape (pre-#3538)", StoryConfidence.DescribeBasis("CXPACKET", 2.0 / 3.0, 3), StringComparison.Ordinal);
        Assert.Contains("path-shape (pre-#3538)", StoryConfidence.DescribeBasis("CXPACKET", 0.75, 4), StringComparison.Ordinal);
    }

    [Fact]
    public void DescribeBasis_CorroborationRows_NameTheFormula_AndThePathLength()
    {
        var basis = StoryConfidence.DescribeBasis("PAGEIOLATCH_SH", StoryConfidence.Compute(2, 3, 3), 3);
        Assert.Contains("corroboration (#3538)", basis, StringComparison.Ordinal);
        Assert.Contains("3-node story path", basis, StringComparison.Ordinal);
        Assert.Contains("0.20 for the fired symptom", basis, StringComparison.Ordinal);
        Assert.DoesNotContain("pre-#3538", basis, StringComparison.Ordinal);

        /* A fresh lone symptom (0.20) is corroboration-derived, not legacy — the floor is not 1.0. */
        Assert.Contains("corroboration (#3538)", StoryConfidence.DescribeBasis("SCH_M", StoryConfidence.Compute(0, 0, 1), 1), StringComparison.Ordinal);
    }

    /// <summary>
    /// The two 1.0s that are NOT legacy: the absolution story and the same-statement pileup detector's
    /// story both carry 1.0 by construction and are named by root key, so a reader is never told a
    /// directly measured convoy is an uncorroborated pre-change row.
    /// </summary>
    [Fact]
    public void DescribeBasis_ByConstructionOnes_AreNamed_NotCalledLegacy()
    {
        var absolution = StoryConfidence.DescribeBasis(StoryConfidence.AbsolutionRootKey, 1.0, 1);
        Assert.StartsWith("absolution:", absolution, StringComparison.Ordinal);
        Assert.DoesNotContain("pre-#3538", absolution, StringComparison.Ordinal);

        var pileup = StoryConfidence.DescribeBasis(SameStatementPileupDetector.RootFactKey, 1.0, 1);
        Assert.StartsWith("detector-measured:", pileup, StringComparison.Ordinal);
        Assert.DoesNotContain("pre-#3538", pileup, StringComparison.Ordinal);
    }

    /* ── A6: through the engine ── */

    /// <summary>
    /// End to end through FactScorer + InferenceEngine: a lone SCH_M that cleared its threshold roots a
    /// story at the floor, and a PAGEIOLATCH_SH whose companions showed up — read latency over 20 ms and
    /// memory-grant waiters (2 of its 3 amplifiers) — roots a story that traverses its highest-severity
    /// active edge (executed here: PAGEIOLATCH_SH → IO_READ_LATENCY_MS, two nodes, 0.20 + 0.32 + 0.16 =
    /// 0.68) and whose confidence is exactly the formula over its own root fact and path. The story's
    /// confidence must equal <see cref="StoryConfidence.Compute(Fact, int)"/> over the facts the engine
    /// actually saw, so the pin cannot drift from the code it describes.
    /// </summary>
    [Fact]
    public void BuildStories_LoneSymptomScoresTheFloor_CorroboratedChainScoresHigher_AndBothMatchTheFormula()
    {
        var engine = new InferenceEngine(new RelationshipGraph());

        var lone = new List<Fact> { Wait("SCH_M", 0.05) };
        new FactScorer().ScoreAll(lone);
        var loneStory = Assert.Single(engine.BuildStories(lone));
        Assert.Equal("SCH_M", loneStory.StoryPath);
        Assert.Equal(StoryConfidence.Floor, loneStory.Confidence, precision: 9);
        Assert.Empty(lone[0].AmplifierResults); // SCH_M has no catalogue: the uncatalogued arm

        var chain = new List<Fact>
        {
            Wait("PAGEIOLATCH_SH", 0.30),
            Wait("RESOURCE_SEMAPHORE", 0.05),
            new() { Source = "io", Key = "IO_READ_LATENCY_MS", Value = 25 },
            new() { Source = "memory", Key = "MEMORY_GRANT_PENDING", Value = 3 },
        };
        new FactScorer().ScoreAll(chain);
        var stories = engine.BuildStories(chain);
        var root = stories.First(s => s.RootFactKey == "PAGEIOLATCH_SH");
        var rootFact = chain.Single(f => f.Key == "PAGEIOLATCH_SH");

        Assert.Equal(3, rootFact.AmplifierResults.Count);
        Assert.Equal(2, rootFact.AmplifierResults.Count(a => a.Matched)); // read latency ≥ 20 ms, grant waiters ≥ 1; SOS absent
        Assert.True(root.Path.Count >= 2, $"expected a traversed chain, got {root.StoryPath}");
        Assert.Equal(StoryConfidence.Compute(rootFact, root.Path.Count), root.Confidence, precision: 9);
        Assert.Equal(0.68, root.Confidence, precision: 9); // executed: PAGEIOLATCH_SH → IO_READ_LATENCY_MS
        Assert.True(root.Confidence > loneStory.Confidence,
            $"corroborated {root.StoryPath} ({root.Confidence:F3}) must outrank a lone symptom ({loneStory.Confidence:F3})");
        Assert.True(root.Confidence < 1.0);
    }

    /* ── A9: the maintenance edges ── */

    /// <summary>
    /// Each symptom → job edge fires on RUNNING_JOBS having FIRED (base severity above zero — at least one
    /// job running past its own history) and not on the fact merely being present: the collector emits
    /// RUNNING_JOBS whenever any job is running, and an edge on presence would pin every schema-lock or
    /// write-latency finding to whatever routine job happened to be executing.
    /// </summary>
    [Theory]
    [InlineData("SCH_M")]
    [InlineData("IO_WRITE_LATENCY_MS")]
    [InlineData("WRITELOG")]
    public void MaintenanceEdges_FireOnlyWhenRunningJobsFired_NotOnPresence(string symptom)
    {
        var graph = new RelationshipGraph();

        var fired = new Dictionary<string, Fact>
        {
            ["RUNNING_JOBS"] = new() { Key = "RUNNING_JOBS", Source = "jobs", Value = 1, BaseSeverity = 0.5, Severity = 0.5 }
        };
        Assert.Contains(graph.GetActiveEdges(symptom, fired), e => e.Destination == "RUNNING_JOBS");

        var presentButQuiet = new Dictionary<string, Fact>
        {
            ["RUNNING_JOBS"] = new() { Key = "RUNNING_JOBS", Source = "jobs", Value = 0, BaseSeverity = 0, Severity = 0 }
        };
        Assert.DoesNotContain(graph.GetActiveEdges(symptom, presentButQuiet), e => e.Destination == "RUNNING_JOBS");

        Assert.DoesNotContain(graph.GetActiveEdges(symptom, new Dictionary<string, Fact>()), e => e.Destination == "RUNNING_JOBS");
    }

    /// <summary>
    /// The nightly rebuild, as the engine now reads it: SCH_M, IO_WRITE_LATENCY_MS, WRITELOG and a
    /// RUNNING_JOBS that fired all score, and <see cref="InferenceEngine.ClusterIntoIncidents"/> returns
    /// ONE incident holding every story — the job is on the write-latency story's path (the highest root
    /// follows its highest-severity active edge, and the job at 1.0 outranks WRITELOG), and the schema-lock
    /// and log-flush stories union across their own edges onto it. Before the edges this was three
    /// incidents. The same facts with the job NOT running long stay apart: SCH_M has no active edge to
    /// anything present and is its own incident, while the write pair still unions on the pre-existing
    /// IO_WRITE_LATENCY_MS ↔ WRITELOG edges — so the fold is the job's doing, not a side effect.
    /// </summary>
    [Fact]
    public void ClusterIntoIncidents_FoldsTheRebuildIntoOneIncident_WhenTheJobFired_AndKeepsSchMApartWhenItDidNot()
    {
        var engine = new InferenceEngine(new RelationshipGraph());

        var withJob = RebuildFacts(runningLong: 3);
        new FactScorer().ScoreAll(withJob);
        var stories = engine.BuildStories(withJob);
        Assert.Equal(3, stories.Count);
        Assert.Contains(stories, s => s.StoryPath == "IO_WRITE_LATENCY_MS → RUNNING_JOBS");
        Assert.Contains(stories, s => s.StoryPath == "SCH_M");
        Assert.Contains(stories, s => s.StoryPath == "WRITELOG");

        var incidents = engine.ClusterIntoIncidents(stories, withJob);
        var incident = Assert.Single(incidents);
        Assert.Equal(3, incident.Count);

        var withoutJob = RebuildFacts(runningLong: 0);
        new FactScorer().ScoreAll(withoutJob);
        stories = engine.BuildStories(withoutJob);
        Assert.Equal(2, stories.Count);
        Assert.Contains(stories, s => s.StoryPath == "IO_WRITE_LATENCY_MS → WRITELOG");
        Assert.Contains(stories, s => s.StoryPath == "SCH_M");

        incidents = engine.ClusterIntoIncidents(stories, withoutJob);
        Assert.Equal(2, incidents.Count);
        var schM = Assert.Single(incidents, i => i.Any(s => s.RootFactKey == "SCH_M"));
        Assert.Single(schM);
    }

    /* ── A9: the prose ── */

    /// <summary>
    /// The SCH_M card names the job when RUNNING_JOBS fired — the collected counts and overrun, and the
    /// statement that the two cards are one incident — and says plainly that no job was running long
    /// when it was not, instead of the static "open the Running Jobs tab to see whether".
    /// </summary>
    [Fact]
    public void SchMAdvice_NamesTheLinkedJob_WhenRunningJobsFired_AndSaysNoJobOtherwise()
    {
        var withJob = RebuildFacts(runningLong: 3);
        new FactScorer().ScoreAll(withJob);
        var linked = FactAdvice.Compose("SCH_M", withJob.ToFactLookup());
        Assert.NotNull(linked);
        Assert.Contains("RUNNING_JOBS fired in the same window", linked!.Investigation, StringComparison.Ordinal);
        Assert.Contains("3 Agent jobs running well past normal duration", linked.Investigation, StringComparison.Ordinal);
        Assert.Contains("400% of its historical average", linked.Investigation, StringComparison.Ordinal);
        Assert.Contains("ONE incident, not two", linked.Investigation, StringComparison.Ordinal);
        Assert.Contains("Agent job runs long", linked.Headline, StringComparison.Ordinal);

        var withoutJob = RebuildFacts(runningLong: 0);
        new FactScorer().ScoreAll(withoutJob);
        var alone = FactAdvice.Compose("SCH_M", withoutJob.ToFactLookup());
        Assert.NotNull(alone);
        Assert.DoesNotContain("RUNNING_JOBS fired", alone!.Investigation, StringComparison.Ordinal);
        Assert.Contains("No Agent job was running past its normal duration this window", alone.Investigation, StringComparison.Ordinal);
    }

    /// <summary>
    /// The other three cards carry the same link: the write-latency and log-flush remediations name the
    /// job beside their storage advice, and the job card names the symptoms whose edges point at it.
    /// </summary>
    [Fact]
    public void WriteLatency_Writelog_AndRunningJobs_AdviceCarryTheLink_BothWays()
    {
        var facts = RebuildFacts(runningLong: 3);
        new FactScorer().ScoreAll(facts);
        var lookup = facts.ToFactLookup();

        var io = FactAdvice.Compose("IO_WRITE_LATENCY_MS", lookup);
        Assert.Contains("RUNNING_JOBS fired in the same window", io!.Remediation, StringComparison.Ordinal);

        var writelog = FactAdvice.Compose("WRITELOG", lookup);
        Assert.Contains("RUNNING_JOBS fired in the same window", writelog!.Remediation, StringComparison.Ordinal);

        var jobs = FactAdvice.Compose("RUNNING_JOBS", lookup);
        Assert.Contains("SCH_M (schema-modification lock waits", jobs!.Investigation, StringComparison.Ordinal);
        Assert.Contains("IO_WRITE_LATENCY_MS (", jobs.Investigation, StringComparison.Ordinal);
        Assert.Contains("WRITELOG (", jobs.Investigation, StringComparison.Ordinal);

        /* And none of it when the job did not fire. */
        var quiet = RebuildFacts(runningLong: 0);
        new FactScorer().ScoreAll(quiet);
        Assert.DoesNotContain("RUNNING_JOBS fired", FactAdvice.Compose("IO_WRITE_LATENCY_MS", quiet.ToFactLookup())!.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("RUNNING_JOBS fired", FactAdvice.Compose("WRITELOG", quiet.ToFactLookup())!.Remediation, StringComparison.Ordinal);
    }

    /* ── #3653: the job card names the job ── */

    /// <summary>
    /// #3653: when the collectors put the worst job's name on the fact's ObjectName, the RUNNING_JOBS card
    /// names it in the headline, the first sentence and the remediation — with "and N others" when more
    /// than one ran long, and as the single subject when exactly one did. The overrun figures stay
    /// attributed to "the worst", never to the named job (they are window maxima over every running row).
    /// </summary>
    [Fact]
    public void RunningJobsAdvice_NamesTheJob_WhenTheFactCarriesOne()
    {
        var several = RebuildFacts(runningLong: 3, worstJob: "Nightly Index Maintenance");
        new FactScorer().ScoreAll(several);
        var card = FactAdvice.Compose("RUNNING_JOBS", several.ToFactLookup());
        Assert.NotNull(card);
        Assert.Equal("Agent job `Nightly Index Maintenance` and 2 others running well past normal — likely stuck, not busy", card!.Headline);
        Assert.StartsWith("3 Agent jobs ran well past normal duration this window, `Nightly Index Maintenance` the furthest past its own history (the Running Jobs view lists the 2 others)", card.Investigation, StringComparison.Ordinal);
        Assert.Contains(", the worst at 400% of its historical average", card.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("Check `Nightly Index Maintenance` in Agent history first, then the others:", card.Remediation, StringComparison.Ordinal);
        /* The reverse link to the co-fired symptoms is unchanged by the name. */
        Assert.Contains("SCH_M (schema-modification lock waits", card.Investigation, StringComparison.Ordinal);

        var one = RebuildFacts(runningLong: 1, worstJob: "Weekly CHECKDB");
        new FactScorer().ScoreAll(one);
        var single = FactAdvice.Compose("RUNNING_JOBS", one.ToFactLookup());
        Assert.Equal("Agent job `Weekly CHECKDB` running well past normal — likely stuck, not busy", single!.Headline);
        Assert.StartsWith("Agent job `Weekly CHECKDB` ran well past normal duration this window, the worst at", single.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("other", single.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("Check `Weekly CHECKDB` in Agent history first: a job at several times", single.Remediation, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3653: without a name — a finding persisted before the collectors carried one, or a window where jobs
    /// ran and none ran long — every sentence is the pre-#3653 one, so old findings' frozen text and the
    /// nothing-long case read exactly as they did. And a name beside a ZERO running-long count is refused:
    /// the collectors cannot produce that pairing (the FILTERed aggregate is NULL when nothing ran long),
    /// so if it ever arrives it is a bug upstream, and naming a job that did not run long is the lie this
    /// card exists to stop.
    /// </summary>
    [Fact]
    public void RunningJobsAdvice_KeepsTheUnnamedSentence_WithoutAName_AndRefusesANameOnAZeroCount()
    {
        var unnamed = RebuildFacts(runningLong: 3);
        new FactScorer().ScoreAll(unnamed);
        var card = FactAdvice.Compose("RUNNING_JOBS", unnamed.ToFactLookup());
        Assert.Equal("3 Agent jobs running well past normal — likely stuck, not busy", card!.Headline);
        Assert.StartsWith("3 Agent jobs ran well past normal duration this window, the worst at 400% of its historical average", card.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("Check the long-running jobs in Agent history:", card.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("`", card.Headline, StringComparison.Ordinal);

        var contradictory = RebuildFacts(runningLong: 0, worstJob: "Should Not Appear");
        new FactScorer().ScoreAll(contradictory);
        var refused = FactAdvice.Compose("RUNNING_JOBS", contradictory.ToFactLookup());
        Assert.NotNull(refused);
        Assert.DoesNotContain("Should Not Appear", refused!.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("Should Not Appear", refused.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("Should Not Appear", refused.Remediation, StringComparison.Ordinal);
        Assert.Equal("0 Agent jobs running well past normal — likely stuck, not busy", refused.Headline);
    }

    /* ── Helpers ── */

    /// <summary>
    /// The rebuild window: SCH_M at 5% of period (0.72 on its (0.01, 0.10) ramp), write latency 25 ms
    /// (0.875 on (10, 30), then the WRITELOG amplifier), WRITELOG at 30% (0.60 on (0.25, 0.50)), and a
    /// RUNNING_JOBS fact whose value is the running-long count — 3 saturates its (1, 3) ramp at 1.0; 0
    /// scores nothing and drops out of the working set, which is the "job present but not long" case.
    /// Wait facts carry the metadata the advice composers read (wait_time_ms), the I/O fact the average
    /// its composer reads, and the job fact the counts its sentence states.
    /// </summary>
    private static List<Fact> RebuildFacts(int runningLong, string? worstJob = null) =>
    [
        Wait("SCH_M", 0.05),
        Wait("WRITELOG", 0.30),
        new()
        {
            Source = "io", Key = "IO_WRITE_LATENCY_MS", Value = 25,
            Metadata = new() { ["avg_write_latency_ms"] = 25, ["total_writes"] = 500_000, ["total_stall_write_ms"] = 12_500_000 }
        },
        new()
        {
            Source = "jobs", Key = "RUNNING_JOBS", Value = runningLong,
            /* #3653: the collectors' one job name, on the string slot (null = pre-#3653 row / nothing long). */
            ObjectName = worstJob,
            Metadata = new()
            {
                ["running_count"] = 5, ["running_long_count"] = runningLong,
                ["max_percent_of_average"] = runningLong > 0 ? 400 : 0,
                ["max_duration_seconds"] = runningLong > 0 ? 10_800 : 0
            }
        },
    ];

    private static Fact Wait(string key, double fraction) =>
        new()
        {
            Source = "waits", Key = key, Value = fraction,
            Metadata = new()
            {
                ["wait_time_ms"] = fraction * 4 * 3_600_000, ["waiting_tasks_count"] = 1_000,
                ["avg_ms_per_wait"] = fraction * 4 * 3_600, ["period_duration_ms"] = 4 * 3_600_000
            }
        };
}
