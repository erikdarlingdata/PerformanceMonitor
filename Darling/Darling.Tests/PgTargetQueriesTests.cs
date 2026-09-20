/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The <c>pg_queries</c> family (#3542 v1 step 7, lane 7; re-graded by #3691 lane 34 on the 2026-09-20 ruling):
/// <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> from <c>pg_statement_stats</c> — the query-shaped leaf every PostgreSQL story
/// needs — and <c>ANOMALY_PG_BAD_ACTOR_SHARE</c>, the statement's share against its OWN hour-of-week share baseline,
/// which is the bad actor's grade now.
///
/// <para><b>What is pinned, and why each.</b> The scorer admits a fact on the fleet-measured busy floor and lands it
/// in a CONTEXT band (half to all of <c>BadActorContextBand</c>, linear in share — under the story line, in share
/// order) stamping <c>share_band</c> and <c>threshold_lineage = 0</c>; the floor exit stamps 1 (the rule
/// <c>PgTargetThresholdLineageTests</c> / <c>PgTargetMeasuredLineageTests</c> enforce on the source, this pins the
/// behaviour). The FOUR grading cases of the ruling are exercised through the real gate arithmetic and the real
/// scorer: a 0.60 statement whose own normal is 0.58 ± 0.03 grades LOW (no anomaly; context); the same 0.60 against
/// 0.10 ± 0.02 grades HIGH through the anomaly (the shared deviation ramp) and lifts its card; an untrustworthy own
/// normal grades nothing (never the absolute bar); the busy floor still gates admission. The anomaly folds onto its
/// statement through the alias edge — one story when it outranks, one incident (graph connectivity) when the card
/// does — and the static <c>AnomalyToFamilies</c> entry is pinned NOT to fold by itself (an honest limitation, filed).
/// The collector's read is unchanged and its pins stay (stored deltas, the <c>SUM(…) OVER ()</c> denominator, the
/// three-state interval). The detector's read shares the bucket arm's sample rule. The advice states the share, the
/// statement's own routine and the sigma, names the absolute bars as routine context, and says which of three
/// things is true of the statement's own normal — or that the pass could not tell.</para>
///
/// <para><b>The exit criterion (gated on <c>DARLING_TEST_PG</c>).</b> Four weeks of hour-of-week history where the
/// heavy statement usually takes 10 %, the medium 30 % and the light 60 %; a window where the heavy takes 60 %, the
/// medium 30 %, the light 10 % and a newcomer appears. Through the REAL <c>analyze_server</c>: ONE finding, rooted on
/// <c>ANOMALY_PG_BAD_ACTOR_SHARE</c> naming the heavy statement, walking into its <c>PG_BAD_ACTOR</c> card (the
/// drill-down's text on the path), severity 1.0, advice naming the share and the routine; the steady medium and the
/// dropped light are context cards under the story line; the newcomer has no own-normal.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetQueriesTests
{
    private const string ServerName = "darling-pg-target-bad-actor-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>A signed 64-bit id spread over the int8 range, the way real <c>queryid</c>s are — exact in the
    /// key, NOT exact in a double, which is why the fact carries it in the key alone.</summary>
    private const long HeavyQueryId = -1234567890123456789L;
    private const long MediumQueryId = 42L;
    private const long LightQueryId = 7L;
    private const long NewcomerQueryId = 9_001L;
    private const string HeavyText = "SELECT o.id, o.total FROM orders AS o WHERE o.customer_id = $1 AND o.placed_at >= $2 ORDER BY o.placed_at DESC LIMIT $3";

    private static Fact BadActor(long queryId, double share, double busy = 0.2, double tempBlocks = 0) => new()
    {
        Source = PgTargetSources.QueriesSource,
        Key = PgTargetFactKeys.BadActorKey(queryId),
        Value = share,
        ServerId = 1,
        Metadata =
        {
            ["share_of_window_time"] = share,
            ["window_total_exec_ms"] = 1_000_000,
            ["window_busy_fraction"] = busy,
            ["calls"] = 2640,
            ["total_exec_ms"] = share * 1_000_000,
            ["mean_exec_ms"] = 227.36,
            ["max_exec_ms"] = 900.5,
            ["calls_per_sec"] = 0.1833,
            ["temp_blks_written"] = tempBlocks,
            ["database_count"] = 2,
        },
    };

    /// <summary>The anomaly fact as <c>PgTargetAnomalyDetector.Queries.cs</c> emits it for a statement whose own
    /// bucket is <paramref name="mean"/> ± <paramref name="stddev"/> (Full tier, 48 samples over 4 days) and whose
    /// window share is <paramref name="windowShare"/> — the gate arithmetic is the REAL <c>AnomalyGate</c>'s, so the
    /// sigma and the fire verdict are what the detector would compute, not a number typed here.</summary>
    private static (Fact? Anomaly, AnomalyGate.ZDecision Decision, BaselineBucket Bucket) Detect(long queryId, double windowShare, double mean, double stddev, params (long QueryId, int Verdict)[] others)
    {
        /* Median = mean and MAD = 0.6745 × stddev, so the robust frame the real bucket carries has the SAME sigma as the
           classical one and the cutoff in force is the robust 3.5 (ModifiedZThresholdFor's default — the detector passes
           it by name). */
        var bucket = new BaselineBucket { HourOfDay = 12, DayOfWeek = 2, Mean = mean, StdDev = stddev, Median = mean, Mad = 0.6745 * stddev, SampleCount = 48, DistinctDays = 4, Tier = BaselineTier.Full };
        Assert.True(bucket.IsTrustworthy);
        /* The detector's call, argument for argument (pinned on the source in TheDetector_… below). */
        var decision = AnomalyGate.EvaluateZScore(
            bucket, windowShare, windowShare,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgStatementShare),
            PgTargetScorer.BadActorShareConcerning, double.PositiveInfinity, AnomalyThresholds.SigmaDisplayCap);
        if (!decision.Fire)
            return (null, decision, bucket);

        var fact = new Fact
        {
            Source = "anomaly",
            Key = PgTargetFactKeys.AnomalyBadActorShare,
            Value = windowShare,
            ServerId = 1,
            ObjectName = queryId.ToString(CultureInfo.InvariantCulture),
            Metadata =
            {
                ["baseline_mean"] = bucket.Mean,
                ["baseline_stddev"] = bucket.EffectiveStdDev,
                ["baseline_median"] = bucket.Median,
                ["baseline_mad"] = bucket.Mad,
                ["deviation_sigma"] = decision.Sigma,
                ["mean_sigma"] = decision.MeanSigma ?? 0,
                ["fire_threshold"] = decision.ThresholdUsed,
                ["baseline_low_quality"] = 0,
                ["fallback_exceedance"] = 0,
                ["baseline_samples"] = bucket.SampleCount,
                ["window_samples"] = 48,
                ["confidence"] = bucket.Confidence,
                ["threshold_lineage"] = 0,
                ["window_share"] = windowShare,
                ["peak_share"] = windowShare,
                ["mean_share"] = windowShare,
                ["ratio"] = windowShare / mean,
                ["candidates_evaluated"] = 1 + others.Length,
                ["candidates_fired"] = 1 + others.Count(o => o.Verdict == 2),
                ["candidates_without_baseline"] = others.Count(o => o.Verdict == 0),
                [PgTargetAnomalyDetector.OwnNormalMetadataPrefix + queryId.ToString(CultureInfo.InvariantCulture)] = 2,
            },
        };
        foreach (var (otherId, verdict) in others)
            fact.Metadata[PgTargetAnomalyDetector.OwnNormalMetadataPrefix + otherId.ToString(CultureInfo.InvariantCulture)] = verdict;
        return (fact, decision, bucket);
    }

    private static Dictionary<string, Fact> Lookup(params Fact[] facts) => facts.ToFactLookup();

    /* ── the scorer ── */

    [Fact]
    public void ScoreQueriesFact_LandsAnAdmittedShareInTheContextBand_InShareOrder_UnderTheStoryLine_AndStampsTheChosenLineage()
    {
        var whole = BadActor(0, 1.0);
        var critical = BadActor(1, 0.60);
        var concerning = BadActor(2, 0.25);
        var under = BadActor(3, 0.10);

        /* 0.3 × (0.5 + 0.5 × share): the band runs from half of BadActorContextBand to all of it. */
        Assert.Equal(0.30, PgTargetScorer.ScoreBase(whole), precision: 9);
        Assert.Equal(0.24, PgTargetScorer.ScoreBase(critical), precision: 9);
        Assert.Equal(0.1875, PgTargetScorer.ScoreBase(concerning), precision: 9);
        Assert.Equal(0.165, PgTargetScorer.ScoreBase(under), precision: 9);

        /* Every admitted card is under the 0.5 story line — no statement roots on its share alone any more. */
        foreach (var fact in new[] { whole, critical, concerning, under })
        {
            Assert.InRange(fact.BaseSeverity = PgTargetScorer.ScoreBase(fact), 0.15, 0.30);
            /* … and says a CHOSEN number decided it. */
            Assert.Equal(0, fact.Metadata["threshold_lineage"]);
        }

        /* The share bars survive as context: the band the share sits in, by the two measured-routine lines. */
        Assert.Equal(2, whole.Metadata["share_band"]);
        Assert.Equal(2, critical.Metadata["share_band"]);
        Assert.Equal(1, concerning.Metadata["share_band"]);
        Assert.Equal(0, under.Metadata["share_band"]);

        Assert.Equal(0.25, PgTargetScorer.BadActorShareConcerning);
        Assert.Equal(0.60, PgTargetScorer.BadActorShareCritical);
        Assert.Equal(0.3, PgTargetScorer.BadActorContextBand);
        Assert.Equal(2.5, PgTargetScorer.BadActorOwnNormalBoost);

        /* Share order is preserved, so the alias still resolves to the largest statement when none is deviant. */
        var facts = new List<Fact> { under, concerning, critical, whole };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(whole.Key, PgTargetRelationshipGraph.ResolveBadActor(facts.ToFactLookup()));
    }

    [Fact]
    public void ScoreQueriesFact_IsZeroOnAnIdleWindow_WithoutAShare_AndForAnyOtherKeyUnderTheSource()
    {
        /* 60% of nothing: the busy floor gates the share, and the gate is a fleet-MEASURED bar, so the fact it zeroes
           says so (threshold_lineage = 1) and carries no share band — nothing was admitted to be banded. */
        var idle = BadActor(1, 0.60, busy: 0.04);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(idle));
        Assert.Equal(1, idle.Metadata["threshold_lineage"]);
        Assert.False(idle.Metadata.ContainsKey("share_band"));
        Assert.Equal(0.05, PgTargetScorer.BadActorBusyFloor);

        /* At the floor exactly, the gate opens onto the context band. */
        var atFloor = BadActor(1, 0.60, busy: 0.05);
        Assert.Equal(0.24, PgTargetScorer.ScoreBase(atFloor), precision: 9);
        Assert.Equal(0, atFloor.Metadata["threshold_lineage"]);

        /* No share, no grade — and no stamp, because nothing was graded. */
        var noShare = new Fact { Source = PgTargetSources.QueriesSource, Key = PgTargetFactKeys.BadActorKey(9), Value = 0.6 };
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(noShare));
        Assert.False(noShare.Metadata.ContainsKey("threshold_lineage"));

        /* The routing probe the shared-switch test uses: a non-bad-actor key under pg_queries scores 0. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.QueriesSource, Key = "PG_PROBE", Value = 99 }));
    }

    [Fact]
    public void QueriesAmplifiers_FireOnlyWhenTheSiblingFired_AndTheStatementItselfCorroborates_AndTheOwnNormalArmNamesTheStatement()
    {
        var key = PgTargetFactKeys.BadActorKey(HeavyQueryId);
        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        var definitions = ((System.Collections.IEnumerable)amplifiers.Invoke(null, [key])!).Cast<object>().ToList();
        Assert.Equal(3, definitions.Count);

        static (double Boost, Func<Dictionary<string, Fact>, bool> Predicate) Read(object definition)
        {
            var type = definition.GetType();
            return (
                (double)type.GetProperty("Boost")!.GetValue(definition)!,
                (Func<Dictionary<string, Fact>, bool>)type.GetProperty("Predicate")!.GetValue(definition)!);
        }

        var spill = Read(definitions[0]);
        var cpu = Read(definitions[1]);
        var ownNormal = Read(definitions[2]);
        Assert.Equal(PgTargetScorer.BadActorCoFireBoost, spill.Boost);
        Assert.Equal(PgTargetScorer.BadActorCoFireBoost, cpu.Boost);
        Assert.Equal(PgTargetScorer.BadActorOwnNormalBoost, ownNormal.Boost);

        var spiller = BadActor(HeavyQueryId, 0.6, tempBlocks: 1912);
        var nonSpiller = BadActor(HeavyQueryId, 0.6, tempBlocks: 0);
        var firedSpill = new Fact { Source = PgTargetSources.TempSource, Key = PgTargetFactKeys.TempSpill, BaseSeverity = 0.7 };
        var quietSpill = new Fact { Source = PgTargetSources.TempSource, Key = PgTargetFactKeys.TempSpill, BaseSeverity = 0.0 };
        var firedCpu = new Fact { Source = PgTargetSources.CpuSource, Key = PgTargetFactKeys.CpuPercent, BaseSeverity = 0.9 };

        Assert.True(spill.Predicate(Lookup(spiller, firedSpill)));
        /* The server spilled but THIS statement wrote no temp blocks: not corroborated. */
        Assert.False(spill.Predicate(Lookup(nonSpiller, firedSpill)));
        /* The statement spilled but the server-level fact did not fire (its own bar decided). */
        Assert.False(spill.Predicate(Lookup(spiller, quietSpill)));
        Assert.False(spill.Predicate(Lookup(spiller)));

        Assert.True(cpu.Predicate(Lookup(spiller, firedCpu)));
        Assert.False(cpu.Predicate(Lookup(spiller)));

        /* The own-normal arm: the anomaly FIRED (base > 0) and NAMES this statement — by the id's text. */
        var named = Detect(HeavyQueryId, 0.60, mean: 0.10, stddev: 0.02).Anomaly!;
        named.BaseSeverity = 1.0;
        Assert.True(ownNormal.Predicate(Lookup(spiller, named)));
        var namesAnother = Detect(MediumQueryId, 0.60, mean: 0.10, stddev: 0.02, (HeavyQueryId, 2)).Anomaly!;
        namesAnother.BaseSeverity = 1.0;
        /* Fired for this statement too (own_normal 2) but names another: the lift is the NAMED card's alone. */
        Assert.False(ownNormal.Predicate(Lookup(spiller, namesAnother)));
        var quietAnomaly = Detect(HeavyQueryId, 0.60, mean: 0.10, stddev: 0.02).Anomaly!;
        quietAnomaly.BaseSeverity = 0.0;
        Assert.False(ownNormal.Predicate(Lookup(spiller, quietAnomaly)));
        Assert.False(ownNormal.Predicate(Lookup(spiller)));
        Assert.True(PgTargetScorer.OwnNormalAnomalyNames(Lookup(spiller, named), key));
        Assert.False(PgTargetScorer.OwnNormalAnomalyNames(Lookup(spiller, named), PgTargetFactKeys.BadActorKey(MediumQueryId)));

        /* Through the real scorer with no anomaly: a bad actor scores its context band and no boost — the two workload
           arms and the own-normal arm are evaluated and none matched. */
        var facts = new List<Fact> { BadActor(HeavyQueryId, 0.6, tempBlocks: 1912), new Fact { Source = PgTargetSources.TempSource, Key = PgTargetFactKeys.TempSpill, Value = 5 } };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.24, facts[0].BaseSeverity, precision: 9);
        Assert.Equal(0.24, facts[0].Severity, precision: 9);
        Assert.Equal(3, facts[0].AmplifierResults.Count);
        Assert.All(facts[0].AmplifierResults, r => Assert.False(r.Matched));
    }

    /// <summary>The ruling's four grading cases, through the REAL gate arithmetic and the REAL scorer.</summary>
    [Fact]
    public void TheFourGradingCases_RoutineDominantIsLow_BeyondItsOwnNormalIsHigh_NoOwnNormalIsNothing_TheBusyFloorStillGates()
    {
        /* (1) A statement at 0.60 whose OWN normal for this hour is 0.58 ± 0.03: within its normal — the gate does not
               fire (0.67σ against the 3.5 robust cutoff), no anomaly exists, and the card is the LOW context band. */
        var routine = Detect(HeavyQueryId, 0.60, mean: 0.58, stddev: 0.03);
        Assert.Null(routine.Anomaly);
        Assert.False(routine.Decision.Fire);
        Assert.InRange(routine.Decision.Sigma, 0.6, 0.7);
        var routineCard = BadActor(HeavyQueryId, 0.60);
        var routineFacts = new List<Fact> { routineCard };
        new FactScorer().ScoreAll(routineFacts);
        Assert.Equal(0.24, routineCard.Severity, precision: 9);
        Assert.DoesNotContain(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(routineFacts), s => !s.IsAbsolution);

        /* (2) The SAME 0.60 against an own normal of 0.10 ± 0.02: 25σ (the display cap) on the peak AND the mean — the
               anomaly fires, the shared ramp grades it 1.0 (2× the anchor and beyond), and the card it names is lifted
               to 0.24 × 3.5 = 0.84 — the story roots on the anomaly and walks into the card. */
        var (anomaly, decision, bucket) = Detect(HeavyQueryId, 0.60, mean: 0.10, stddev: 0.02);
        Assert.NotNull(anomaly);
        Assert.True(decision.Fire);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, decision.Sigma);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, decision.MeanSigma!.Value);
        Assert.Equal(1.0, bucket.Confidence);
        var deviantCard = BadActor(HeavyQueryId, 0.60);
        var steadyCard = BadActor(MediumQueryId, 0.30);
        var deviantFacts = new List<Fact> { deviantCard, steadyCard, anomaly! };
        new FactScorer().ScoreAll(deviantFacts);
        Assert.Equal(1.0, anomaly!.BaseSeverity, precision: 9);
        Assert.Equal(1.0, anomaly.Severity, precision: 9);
        Assert.Equal(0.84, deviantCard.Severity, precision: 9);
        Assert.Contains(deviantCard.AmplifierResults, r => r.Matched && r.Description.Contains("ANOMALY_PG_BAD_ACTOR_SHARE", StringComparison.Ordinal));
        Assert.Equal(0.195, steadyCard.Severity, precision: 9);   /* 0.3 × (0.5 + 0.15): the un-named statement stays context */
        var graph = new PgTargetRelationshipGraph();
        var stories = new InferenceEngine(graph).BuildStories(deviantFacts);
        var story = Assert.Single(stories);
        Assert.Equal(new[] { PgTargetFactKeys.AnomalyBadActorShare, PgTargetFactKeys.BadActorKey(HeavyQueryId) }, story.Path);
        Assert.Equal(1.0, story.Severity, precision: 9);
        Assert.Equal("anomaly", story.Category);

        /* The anomaly's grade is graded: at a modest deviation it lands mid-ramp, not saturated. A 0.305 window share
           against 0.20 ± 0.02 is 5.25σ — over the 3.5 anchor by half of it → 0.75. */
        var modest = Detect(HeavyQueryId, 0.305, mean: 0.20, stddev: 0.02);
        Assert.NotNull(modest.Anomaly);
        Assert.Equal(5.25, modest.Decision.Sigma, precision: 9);
        Assert.Equal(3.5, modest.Decision.ThresholdUsed);
        var modestFacts = new List<Fact> { BadActor(HeavyQueryId, 0.305), modest.Anomaly! };
        new FactScorer().ScoreAll(modestFacts);
        Assert.Equal(0.75, modest.Anomaly!.BaseSeverity, precision: 9);
        /* … and the named card at 0.305 share reads 0.3 × 0.6525 × 3.5 = 0.685: the lift keeps a SMALL deviant statement's
           card over the story line and over every un-named card (ceiling 0.45), so the alias resolves to it. */
        Assert.Equal(0.3 * 0.6525 * 3.5, modestFacts[0].Severity, precision: 9);

        /* The magnitude floor on the peak: a 10× jump that never reaches the measured-routine line (0.205 against
           0.10 ± 0.02 is 5.25σ, but no collection's share reached 0.25) is not a bad actor — the gate stays shut. */
        var trivial = Detect(HeavyQueryId, 0.205, mean: 0.10, stddev: 0.02);
        Assert.Null(trivial.Anomaly);
        Assert.Equal(5.25, trivial.Decision.Sigma, precision: 9);

        /* (3) No trustworthy own normal (five samples, one day): the detector records own_normal = 0 and grades
               NOTHING — the ruling forbids the absolute bar standing in. The bucket's trust verdict is what the
               detector checks (pinned on the source below); the gate's fallback arm is unreachable there. */
        var thin = new BaselineBucket { HourOfDay = 12, DayOfWeek = 2, Mean = 0.10, StdDev = 0.02, SampleCount = 5, DistinctDays = 1, Tier = BaselineTier.Full };
        Assert.False(thin.IsTrustworthy);
        var steadyBucket = new BaselineBucket { HourOfDay = 12, DayOfWeek = 2, Mean = 0.0, StdDev = 0.0, SampleCount = 48, DistinctDays = 4, Tier = BaselineTier.Full };
        Assert.False(steadyBucket.IsTrustworthy);   /* a share that was always exactly zero has no dispersion to judge against */

        /* (4) The busy floor still gates admission, whatever the deviation: an idle window's 0.60 is zero, and a zero
               card is never a leaf — the anomaly, if one fired on the same statement, stands alone. */
        var idleCard = BadActor(HeavyQueryId, 0.60, busy: 0.01);
        var idleFacts = new List<Fact> { idleCard, Detect(HeavyQueryId, 0.60, mean: 0.10, stddev: 0.02).Anomaly! };
        new FactScorer().ScoreAll(idleFacts);
        Assert.Equal(0.0, idleCard.Severity);
        Assert.Equal(1, idleCard.Metadata["threshold_lineage"]);
        var idleStory = Assert.Single(new InferenceEngine(graph).BuildStories(idleFacts));
        Assert.Equal(new[] { PgTargetFactKeys.AnomalyBadActorShare }, idleStory.Path);
    }

    /* ── the fold ── */

    [Fact]
    public void TheAnomaly_WalksIntoTheStatementItNames_ThroughTheAliasEdge_AndIsOneIncidentWithItsCardWhicheverRoots()
    {
        var graph = new PgTargetRelationshipGraph();
        var declared = Assert.Single(graph.GetAllEdges(PgTargetFactKeys.AnomalyBadActorShare));
        Assert.Equal(PgTargetFactKeys.BadActorFamily, declared.Destination);
        Assert.Equal("queries", declared.Category);

        /* Active: resolved to the NAMED statement's key — the lift makes it the highest-severity bad actor. */
        var named = Detect(HeavyQueryId, 0.60, mean: 0.10, stddev: 0.02, (MediumQueryId, 1)).Anomaly!;
        var facts = new List<Fact> { BadActor(HeavyQueryId, 0.60), BadActor(MediumQueryId, 0.30), named };
        new FactScorer().ScoreAll(facts);
        var active = Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.AnomalyBadActorShare, facts.ToFactLookup()));
        Assert.Equal(PgTargetFactKeys.BadActorKey(HeavyQueryId), active.Destination);

        /* Shut when the alias would land on another statement: the anomaly names a SMALL statement whose card sits under
           a huge un-named one — impossible after the lift (0.525 floor vs 0.45 ceiling), so arrange it by hand. */
        var handMade = Lookup(
            new Fact { Source = PgTargetSources.QueriesSource, Key = PgTargetFactKeys.BadActorKey(HeavyQueryId), BaseSeverity = 0.2, Severity = 0.2 },
            new Fact { Source = PgTargetSources.QueriesSource, Key = PgTargetFactKeys.BadActorKey(MediumQueryId), BaseSeverity = 0.3, Severity = 0.3 },
            new Fact { Source = "anomaly", Key = PgTargetFactKeys.AnomalyBadActorShare, BaseSeverity = 1.0, Severity = 1.0, ObjectName = HeavyQueryId.ToString(CultureInfo.InvariantCulture) });
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.AnomalyBadActorShare, handMade));
        /* Shut with no bad actor at all — dropped, not thrown. */
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.AnomalyBadActorShare, Lookup(named)));

        /* The card outranks the anomaly (a large share at a modest sigma: card 0.84, anomaly 0.75): the card roots first
           and the anomaly is its own story — TWO stories, ONE incident, unioned across the active alias edge. */
        var modest = Detect(HeavyQueryId, 0.60, mean: 0.495, stddev: 0.02).Anomaly!;   /* 5.25σ → 0.75 */
        var outranked = new List<Fact> { BadActor(HeavyQueryId, 0.60), BadActor(MediumQueryId, 0.30), modest };
        new FactScorer().ScoreAll(outranked);
        Assert.Equal(0.75, modest.Severity, precision: 9);
        Assert.Equal(0.84, outranked[0].Severity, precision: 9);
        var engine = new InferenceEngine(graph);
        var stories = engine.BuildStories(outranked);
        Assert.Equal(2, stories.Count);
        Assert.Equal(new[] { PgTargetFactKeys.BadActorKey(HeavyQueryId) }, stories[0].Path);
        Assert.Equal(new[] { PgTargetFactKeys.AnomalyBadActorShare }, stories[1].Path);
        var incidents = engine.ClusterIntoIncidents(stories, outranked);
        Assert.Single(incidents);
        Assert.Equal(2, incidents[0].Count);

        /* Honest about the static map: AnomalyToFamilies names the alias, and the reconciler's exact-key fold cannot
           resolve it — a solo anomaly story keeps its own incident id there. The graph, not the map, is the fold. */
        Assert.Equal(new[] { PgTargetFactKeys.BadActorFamily }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyBadActorShare]);
        var cardStory = new AnalysisStory { RootFactKey = PgTargetFactKeys.BadActorKey(HeavyQueryId), Path = [PgTargetFactKeys.BadActorKey(HeavyQueryId)], Severity = 0.84, IncidentId = "inc-card" };
        var anomalyStory = new AnalysisStory { RootFactKey = PgTargetFactKeys.AnomalyBadActorShare, Path = [PgTargetFactKeys.AnomalyBadActorShare], Severity = 0.75, IncidentId = "inc-anomaly" };
        AnomalyIncidentReconciler.Reconcile([cardStory, anomalyStory]);
        Assert.Equal("inc-anomaly", anomalyStory.IncidentId);
    }

    /* ── the advice ── */

    [Fact]
    public void ComposeQueries_StatesTheNumbersItRead_TheStatementsOwnNormal_TheShareAsContext_BothLevers_AndTheQueryidCaveat()
    {
        var key = PgTargetFactKeys.BadActorKey(HeavyQueryId);
        var fact = BadActor(HeavyQueryId, 0.6002, tempBlocks: 1912);
        fact.Metadata["total_exec_ms"] = 600_240;

        /* No anomaly in the pass: the card cannot tell "within" from "none yet" and says exactly that. */
        var block = PgTargetAdvice.Compose(key, Lookup(fact));
        Assert.NotNull(block);
        Assert.Equal("One statement shape held 60% of the window's execution time", block!.Headline);

        /* Value-stated: the share, the totals at a readable scale, the calls, the rate, the mean, the max. */
        Assert.Contains("queryid -1234567890123456789", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("60% of the window's total statement execution time (10 min of 16.7 min)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("2,640 calls (0.18/s", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("averaging 227.4 ms per call", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("worst single execution of 900.5 ms", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("ran against 2 databases", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("wrote 1,912 temp blocks", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("No statement in this pass was beyond its own hour-of-week normal, so this card is context", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("or it has no trustworthy own-normal yet (first seen; a young store)", block.Investigation, StringComparison.Ordinal);
        /* The absolute share, named as routine context with the measured figures. */
        Assert.Contains("The absolute share (at or above 60%) is context: on the measured fleet a top statement's share of a busy hour is at or above 25% 85% of the time and at or above 60% 44% of the time — routine", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("re-keyed by a major upgrade", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("occurrence history restarts at one", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("threshold_lineage = 1", block.Investigation, StringComparison.Ordinal);

        /* Both levers with the arithmetic, each with its counter-objective; no DDL, and an index is a thing to test. */
        Assert.Contains("total = calls × mean: 2,640 × 227.4 ms", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("trades result freshness", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("trades write amplification", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("a new index can change other statements' plans, so it is a thing to test, not a promise", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_query_duration_trend", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_plans", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", block.Remediation, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CREATE INDEX", block.Investigation, StringComparison.OrdinalIgnoreCase);
        Assert.Null(block.RemediationTsql);

        /* Named by the anomaly: the routine and the sigma are the sentence that says what decided. */
        var named = Detect(HeavyQueryId, 0.6002, mean: 0.22, stddev: 0.04, (MediumQueryId, 1), (LightQueryId, 0)).Anomaly!;
        var beyond = PgTargetAdvice.Compose(key, Lookup(fact, named))!;
        Assert.Equal("One statement shape held 60% of the window's execution time — 9.5σ beyond its own normal for this hour", beyond.Headline);
        Assert.Contains("This is 9.5σ beyond the statement's OWN hour-of-week normal (its own routine of 22% ± 4% for this hour-of-week) — which is the grade (ANOMALY_PG_BAD_ACTOR_SHARE, walked into this card)", beyond.Investigation, StringComparison.Ordinal);

        /* Within its own normal (the anomaly names another statement, this one's verdict is 1): context, said so. */
        var within = PgTargetAdvice.Compose(PgTargetFactKeys.BadActorKey(MediumQueryId), Lookup(BadActor(MediumQueryId, 0.3), named))!;
        Assert.EndsWith(" — within its own normal for this hour", within.Headline, StringComparison.Ordinal);
        Assert.Contains("This is WITHIN the statement's own hour-of-week normal", within.Investigation, StringComparison.Ordinal);
        Assert.Contains("a routine dominant statement, not a bad actor by its own history", within.Investigation, StringComparison.Ordinal);
        Assert.Contains("The absolute share (between 25% and 60%) is context", within.Investigation, StringComparison.Ordinal);

        /* No own normal yet (verdict 0): first seen, and never the absolute share standing in. */
        var none = PgTargetAdvice.Compose(PgTargetFactKeys.BadActorKey(LightQueryId), Lookup(BadActor(LightQueryId, 0.1), named))!;
        Assert.EndsWith(" — no own-normal yet", none.Headline, StringComparison.Ordinal);
        Assert.Contains("The statement has no trustworthy own-normal yet — first seen, or on a store too young", none.Investigation, StringComparison.Ordinal);
        Assert.Contains("never the absolute share standing in for a baseline", none.Investigation, StringComparison.Ordinal);
        Assert.Contains("The absolute share (under 25%) is context", none.Investigation, StringComparison.Ordinal);

        /* Beyond its own normal but NOT the named one (verdict 2 on another statement's anomaly): context beside it. */
        var also = Detect(MediumQueryId, 0.55, mean: 0.10, stddev: 0.02, (HeavyQueryId, 2)).Anomaly!;
        var alsoBeyond = PgTargetAdvice.Compose(key, Lookup(fact, also))!;
        Assert.Contains("This statement is also beyond its OWN hour-of-week normal this window; the pass's own-normal anomaly (ANOMALY_PG_BAD_ACTOR_SHARE) names queryid 42", alsoBeyond.Investigation, StringComparison.Ordinal);
        Assert.EndsWith(" — 22.5σ beyond its own normal for this hour", alsoBeyond.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeQueries_OmitsWhatTheFactDoesNotCarry_AndTheStaticBlockClaimsNoFigure()
    {
        var key = PgTargetFactKeys.BadActorKey(MediumQueryId);
        var fact = BadActor(MediumQueryId, 0.3);
        fact.Metadata.Remove("mean_exec_ms");
        fact.Metadata.Remove("calls_per_sec");
        fact.Metadata.Remove("max_exec_ms");
        fact.Metadata["database_count"] = 1;
        var block = PgTargetAdvice.Compose(key, Lookup(fact))!;

        Assert.DoesNotContain("averaging", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("/s over", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("worst single execution", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("databases", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("temp blocks", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("the mean is unknown for this window", block.Remediation, StringComparison.Ordinal);

        /* The static block: the family's conclusion with no number in it, the same object from both entry points. */
        var statik = PgTargetAdvice.Static(key);
        Assert.NotNull(statik);
        Assert.Same(statik, PgTargetAdvice.Static(PgTargetFactKeys.BadActorKey(LightQueryId)));
        Assert.Equal(statik, FactAdvice.GetForFactKey(key));
        Assert.DoesNotContain("%", statik!.Headline, StringComparison.Ordinal);
        Assert.Contains("queryid", statik.Investigation, StringComparison.Ordinal);
        Assert.Contains("RE-KEYED by a major", statik.Investigation, StringComparison.Ordinal);
        Assert.Contains("this card is CONTEXT", statik.Investigation, StringComparison.Ordinal);
        Assert.Contains("The grade is the statement's deviation from its OWN hour-of-week share baseline (ANOMALY_PG_BAD_ACTOR_SHARE)", statik.Investigation, StringComparison.Ordinal);
        Assert.Contains("never graded on the absolute share in its place", statik.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", statik.Remediation, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("a new index can change other statements' plans", statik.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeBadActorShareAnomaly_StatesTheShareAgainstTheRoutine_BothSigmas_TheCardsFigures_AndTheCandidateCount()
    {
        var named = Detect(HeavyQueryId, 0.612, mean: 0.22, stddev: 0.04, (MediumQueryId, 1), (LightQueryId, 0)).Anomaly!;
        var card = BadActor(HeavyQueryId, 0.612);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyBadActorShare, Lookup(named, card))!;

        Assert.Equal("One statement held 61% of the window's execution time — 9.8σ beyond its own normal for this hour", block.Headline);
        Assert.Contains("Statement queryid -1234567890123456789 held 61.2% of the window's total statement execution time against its own routine of 22% ± 4% for this hour-of-week — 9.8σ on the window's peak per-collection share (61.2%) and 9.8σ on its mean (61.2%), over 48 baseline samples.", block.Investigation, StringComparison.Ordinal);
        /* The card's figures, borrowed so the anomaly's story states the same numbers. */
        Assert.Contains("The statement ran 2,640 calls averaging 227.4 ms per call this window; its PG_BAD_ACTOR card carries the rest", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("Of the window's 3 top statements, 1 was beyond its own normal and 1 has no trustworthy own-normal yet; this card names the one furthest beyond it.", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The absolute share (at or above 60%) is context", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("re-keyed by a major upgrade", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("deviation from this server's own normal", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_query_duration_trend for queryid -1234567890123456789", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("a thing to test, not a promise", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", block.Remediation, StringComparison.OrdinalIgnoreCase);

        /* Without the card in the fact set: the anomaly's own numbers only. */
        var alone = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyBadActorShare, Lookup(named))!;
        Assert.DoesNotContain("The statement ran", alone.Investigation, StringComparison.Ordinal);

        /* The static block through both entry points — the delegation-equality census's expectation. */
        var statik = PgTargetAdvice.Static(PgTargetFactKeys.AnomalyBadActorShare)!;
        Assert.Equal(statik, FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyBadActorShare));
        Assert.Equal("One statement held far more of the window's execution time than is normal for it at this hour", statik.Headline);
        Assert.Contains("THAT STATEMENT's own hour-of-week share", statik.Investigation, StringComparison.Ordinal);
        Assert.Contains("the absolute share is context", statik.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("%", statik.Headline, StringComparison.Ordinal);
    }

    /* ── the reads, as source ── */

    [Fact]
    public void TheDetectorRead_TakesTheCandidateSet_SharesTheBucketArmsSampleRule_AndTheDetectorNeverGradesWithoutAnOwnNormal()
    {
        var sql = PgTargetAnomalyDetector.StatementShareWindowSql;
        /* Lane 7's candidate set — the window's top $4 by time, bounding the keyed population (lane 33's note). */
        Assert.Contains("ORDER BY SUM(stmt_ms) DESC\r\n    LIMIT $4", sql.Replace("\n", "\r\n", StringComparison.Ordinal).Replace("\r\r\n", "\r\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains("SUM(SUM(stmt_ms)) OVER () AS window_ms", sql, StringComparison.Ordinal);
        /* Stored deltas; never a LAG over the stored rows. */
        Assert.Contains("SUM(delta_total_exec_time_ms)::DOUBLE PRECISION AS stmt_ms", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(", sql, StringComparison.Ordinal);
        /* The bucket arm's sample rule: a collection where nothing ran is no sample; one where this statement sat out
           while others ran is a ZERO sample — CROSS JOIN every busy collection, LEFT JOIN the statement, coalesce. */
        Assert.Contains("HAVING SUM(stmt_ms) > 0", sql, StringComparison.Ordinal);
        Assert.Contains("CROSS JOIN collections AS c", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN per_collection AS p", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(coalesce(p.stmt_ms, 0) / c.collection_ms)        AS peak_share", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(coalesce(p.stmt_ms, 0) / c.collection_ms)        AS mean_share", sql, StringComparison.Ordinal);
        Assert.Contains("w.stmt_ms / w.window_ms", sql, StringComparison.Ordinal);
        /* The same rule as the keyed arm it is compared against. */
        var arm = PgTargetBaselineProvider.GetPgTargetKeyedBaselineQuery(MetricNames.PgStatementShare)!;
        Assert.Contains("HAVING SUM(delta_total_exec_time_ms) > 0", arm, StringComparison.Ordinal);
        Assert.Contains("coalesce(stmt_ms, 0) / total_ms", arm, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Queries.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        /* One anomaly per pass — the candidate furthest beyond its normal — with the queryid on the string seam. */
        Assert.Contains("ObjectName = queryId.ToString(CultureInfo.InvariantCulture)", code, StringComparison.Ordinal);
        Assert.Contains("decision.Sigma > worst.Value.Decision.Sigma", code, StringComparison.Ordinal);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(code, "anomalies.Add\\("));
        /* Every candidate's verdict rides on the fact; the queryid is in the KEY's text, never a double. */
        Assert.Equal("own_normal_", PgTargetAnomalyDetector.OwnNormalMetadataPrefix);
        Assert.Contains("verdicts[OwnNormalMetadataPrefix + key] = 0;", code, StringComparison.Ordinal);
        Assert.Contains("verdicts[OwnNormalMetadataPrefix + key] = 1;", code, StringComparison.Ordinal);
        Assert.Contains("verdicts[OwnNormalMetadataPrefix + key] = 2;", code, StringComparison.Ordinal);
        Assert.DoesNotContain("[\"queryid\"]", code, StringComparison.Ordinal);
        /* The keyed seam, the trust check ahead of the gate, the unreachable fallback, the pair gate on the measured-routine floor. */
        Assert.Contains("_baselineProvider.GetBaselineAsync(\r\n                    context.ServerId, MetricNames.PgStatementShare, key, context.TimeRangeStart, context.CancellationToken)", code.Replace("\n", "\r\n", StringComparison.Ordinal).Replace("\r\r\n", "\r\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains("if (baseline.SampleCount == 0 || !baseline.IsTrustworthy || candidate.Samples == 0)", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.BadActorShareConcerning, double.PositiveInfinity, SigmaDisplayCap", code, StringComparison.Ordinal);
        Assert.Contains("ModifiedZThresholdFor(MetricNames.PgStatementShare)", code, StringComparison.Ordinal);
        Assert.Contains("catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))", code, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.UtcNow", code, StringComparison.Ordinal);
        /* The lineage stamp is the helper's default 0 (the cutoffs are unmeasured for statement share); the prose says so. */
        Assert.Contains("ZScoreMetadata(bucket, verdict, samples)", code, StringComparison.Ordinal);
        Assert.DoesNotContain("barsMeasured: true", code, StringComparison.Ordinal);
        Assert.Contains("unmeasured for statement share", source, StringComparison.Ordinal);
        /* Wired into the root's emission order, after the plan detector that reads the same table. */
        var root = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.cs"));
        Assert.True(root.IndexOf("await DetectPlanRegressionAnomalies(context, anomalies);", StringComparison.Ordinal) < root.IndexOf("await DetectBadActorShareAnomalies(context, anomalies);", StringComparison.Ordinal));
        Assert.Contains("private partial Task DetectBadActorShareAnomalies(AnalysisContext context, List<Fact> anomalies);", root, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRouting_ForTheNewKey_IsDeviationScored_ComposedBesideItsFamily_FoldedOnTheAlias_AndReadsTheStatementTools()
    {
        Assert.Equal("ANOMALY_PG_BAD_ACTOR_SHARE", PgTargetFactKeys.AnomalyBadActorShare);
        Assert.True(PgTargetFactKeys.IsPgAnomalyKey(PgTargetFactKeys.AnomalyBadActorShare));
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyBadActorShare));
        Assert.False(PgTargetScorer.IsPgRatioAnomalyKey(PgTargetFactKeys.AnomalyBadActorShare));
        Assert.Equal(new[] { PgTargetFactKeys.BadActorFamily }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyBadActorShare]);

        /* The anomaly composer's arm delegates to the family file (the routing census pins the case exists). */
        var anomalyAdvice = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.Anomaly.cs");
        Assert.Contains("case PgTargetFactKeys.AnomalyBadActorShare:\r\n                return ComposeBadActorShareAnomaly(factsByKey);", anomalyAdvice.Replace("\n", "\r\n", StringComparison.Ordinal).Replace("\r\r\n", "\r\n", StringComparison.Ordinal), StringComparison.Ordinal);

        /* The same three reads as the card it walks into, the trend question first. */
        var reads = PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.AnomalyBadActorShare)!.Select(r => r.Tool).ToArray();
        Assert.Equal(new[] { "get_pg_query_duration_trend", "get_pg_top_queries", "get_pg_plans" }, reads);
        Assert.Equal(reads, PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.BadActorKey(HeavyQueryId))!.Select(r => r.Tool).ToArray());

        /* No amplifier arm of its own: the anomaly IS the corroboration; its impact lives in the card it lifts. */
        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        Assert.Empty((System.Collections.IEnumerable)amplifiers.Invoke(null, [PgTargetFactKeys.AnomalyBadActorShare])!);

        /* Through the shared scorer: a deviation-scored PostgreSQL anomaly, graded off deviation_sigma against fire_threshold. */
        var fact = Detect(HeavyQueryId, 0.305, mean: 0.20, stddev: 0.02).Anomaly!;
        new FactScorer().ScoreAll([fact]);
        Assert.Equal(0.75, fact.BaseSeverity, precision: 9);
    }

    [Fact]
    public void TheCollectorRead_ReadsStoredDeltas_TakesTheWindowDenominator_AndUsesTheThreeStateInterval()
    {
        var sql = PgTargetFactCollector.PgTargetTopStatementsSql;
        Assert.Contains(sql, PgTargetFactCollector.AllSql);

        /* Stored deltas, never a re-derivation of the cumulative columns. */
        Assert.Contains("SUM(delta_calls)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_total_exec_time_ms)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(calls)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(total_exec_time_ms)", sql, StringComparison.Ordinal);
        /* The one unstored counter, differenced over the FULL identity and clamped — the reference reader's shape. */
        Assert.Contains("GREATEST(temp_blks_written - LAG(temp_blks_written) OVER identity, 0)", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY queryid, database_id, user_id, toplevel", sql, StringComparison.Ordinal);
        /* #3541 A7 / #3613: the denominator is the window's, evaluated over the grouped result before LIMIT. */
        Assert.Contains("SUM(SUM(p.total_exec_ms)) OVER ()", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(p.total_exec_ms) > 0", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        /* The three-state interval (V128): stored → NULLIF, NULL → LAG, and no fabricated zero. */
        Assert.Contains("CASE WHEN MAX(sample_interval_seconds) IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time) OVER (PARTITION BY queryid ORDER BY collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE NULLIF(MAX(sample_interval_seconds), 0)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);
        /* The rate is calls over the KNOWN span only. */
        Assert.Contains("FILTER (WHERE p.interval_seconds IS NOT NULL)", sql, StringComparison.Ordinal);
        /* Units: the ≥ 13 column name is the collector's; no pre-13 branch exists to write. */
        Assert.DoesNotContain("total_time", sql.Replace("total_exec_time", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        /* No text join here — the census confines this collector to collector tables; the text is the drill-down's. */
        Assert.DoesNotContain("pg_statement_text", sql, StringComparison.Ordinal);

        Assert.Equal(5, PgTargetFactCollector.TopStatementCount);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Queries.cs");
        Assert.Contains("Key = PgTargetFactKeys.BadActorKey(queryId)", source, StringComparison.Ordinal);
        Assert.Contains("Source = PgTargetSources.QueriesSource", source, StringComparison.Ordinal);
        /* The idle gate divides by OBSERVED time (#3538 A2/A7), and nothing is emitted without it. */
        Assert.Contains("[\"window_busy_fraction\"] = windowTotalExecMs / observedMs", source, StringComparison.Ordinal);
        Assert.Contains("var observedMs = context.ObservedDurationMs;", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PeriodDurationMs", source, StringComparison.Ordinal);
        /* The queryid lives in the key alone — a double would round most real ids. */
        Assert.DoesNotContain("[\"queryid\"]", source, StringComparison.Ordinal);
        /* Absent, not zero: the three conditional metadata writes. */
        Assert.Contains("if (calls > 0) fact.Metadata[\"mean_exec_ms\"]", source, StringComparison.Ordinal);
        Assert.Contains("if (callsPerSec is { } rate) fact.Metadata[\"calls_per_sec\"]", source, StringComparison.Ordinal);
        Assert.Contains("if (maxExecMs is { } max) fact.Metadata[\"max_exec_ms\"]", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDrillDown_JoinsTheTextForDisplayAndHash_ParsesTheQueryidFromTheKey_AndNeverDeSkews()
    {
        var sql = PgTargetDrillDownCollector.PgTargetBadActorDetailSql;
        Assert.Contains("FROM pg_statement_stats", sql, StringComparison.Ordinal);
        Assert.Contains("AND   queryid = $4", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN pg_statement_text AS t", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT(MAX(t.query_text), $5)", sql, StringComparison.Ordinal);
        Assert.Contains("hashtext(MAX(t.query_text))", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(t.first_seen)", sql, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY queryid, database_id, user_id, toplevel", sql, StringComparison.Ordinal);
        Assert.Contains("array_agg(pd.database_id ORDER BY pd.total_exec_ms DESC)", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetDrillDownCollector.Queries.cs");
        /* In CODE: the doc comment names DeSkew to say why it is absent, and prose is not a call. */
        Assert.DoesNotContain("DeSkew", CSharpSourceWalker.StripCommentsAndStrings(source), StringComparison.Ordinal);
        Assert.Contains("CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds", source, StringComparison.Ordinal);
        Assert.Contains("long.TryParse(key.AsSpan(PgTargetFactKeys.BadActorKeyPrefix.Length)", source, StringComparison.Ordinal);
        Assert.Contains("finding.DrillDown![\"pg_bad_actor_statements\"]", source, StringComparison.Ordinal);
        /* queryid as a STRING in the JSON, as get_pg_top_queries returns it. */
        Assert.Contains("queryid = queryId.ToString(CultureInfo.InvariantCulture)", source, StringComparison.Ordinal);
        /* Lane 6's seam is marked, not silently absent. */
        Assert.Contains("filled by lane 6", source, StringComparison.Ordinal);
        Assert.Equal(2000, PgTargetDrillDownCollector.StatementTextCap);
    }

    [Fact]
    public void TheNextReads_ForABadActor_AreTheStatementAndPlanTools()
    {
        /* The duration trend leads since the between-waves pass: the advice's "first question" (mean stepped vs
           calls changed) is the read that answers it. */
        var recommendations = PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.BadActorKey(HeavyQueryId));
        Assert.NotNull(recommendations);
        Assert.Equal(new[] { "get_pg_query_duration_trend", "get_pg_top_queries", "get_pg_plans" }, recommendations!.Select(r => r.Tool).ToArray());
    }

    /* ── gated: the exit criterion ── */

    /// <summary>
    /// Lane 7's exit criterion, re-shaped by lane 34's ruling: a window where one statement holds 60 % of
    /// <c>total_exec_time</c> yields its <c>PG_BAD_ACTOR</c> fact with the share, calls/sec, mean ms and its
    /// <c>queryid</c> in the key (unchanged), and — with FOUR WEEKS of hour-of-week history in which that statement
    /// usually takes 10 %, the medium 30 % and the light 60 % — the REAL <c>analyze_server</c> roots ONE finding on
    /// <c>ANOMALY_PG_BAD_ACTOR_SHARE</c> naming the heavy statement, walking into its card, severity 1.0, the
    /// drill-down's text on the path; the steady medium and the dropped light are context cards under the story
    /// line (visible through <c>get_analysis_facts</c>, not as findings), and a newcomer with no history has no
    /// own-normal.
    /// </summary>
    [Fact]
    public async Task APlantedWindowWhereOneStatementJumpsToSixtyPercent_YieldsTheAnomalyRootedStory_AndTheSteadyOnesStayContext()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the PG_BAD_ACTOR e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, "postgres", 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            /* The coverage witness and the span gate read pg_database_stats: 25 h of span, one row a minute. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* The statements' OWN normals: four weeks back, the six clock hours around the window's start on the same
               weekday (the tool's window starts a minute after the planted one, so its hour-of-week bucket is one of
               these), twelve five-minute collections each — 48 samples over 4 distinct days per (hour, weekday) bucket,
               a trustworthy Full bucket. Per collection: heavy 2,083 ms, medium 6,250 ms, light 12,500 ms — shares
               0.10 / 0.30 / 0.60. The newcomer has no history at all. Cumulative counters are per-series monotone;
               only the deltas are read. */
            long baseHeavyCalls = 0, baseHeavyMs = 0, baseMediumCalls = 0, baseMediumMs = 0, baseLightCalls = 0, baseLightMs = 0;
            for (var week = 1; week <= 4; week++)
            {
                for (var hour = -1; hour <= 4; hour++)
                {
                    var hourStart = windowStart.AddDays(-7 * week).AddHours(hour);
                    for (var slot = 0; slot < 12; slot++)
                    {
                        var at = hourStart.AddMinutes(slot * 5);
                        baseHeavyCalls += 5; baseHeavyMs += 2_083;
                        await PlantStatementAsync(connection, at, HeavyQueryId, 16384, baseHeavyCalls, baseHeavyMs, 5, 2_083, 300, 0, ct);
                        baseMediumCalls += 250; baseMediumMs += 6_250;
                        await PlantStatementAsync(connection, at, MediumQueryId, 16384, baseMediumCalls, baseMediumMs, 250, 6_250, 300, 0, ct);
                        baseLightCalls += 50; baseLightMs += 12_500;
                        await PlantStatementAsync(connection, at, LightQueryId, 16384, baseLightCalls, baseLightMs, 50, 12_500, 300, 0, ct);
                    }
                }
            }

            /* The window: statement snapshots every five minutes, 48 in the window, each carrying the interval it accrued
               over. Per snapshot: heavy 12,500 ms over 50 calls (+ a one-call, one-ms sibling series against a second
               database), medium 6,250 ms over 250 calls, light 2,083 ms over 5 calls with NO stored interval (the
               pre-V128 row shape, so the LAG fallback is the path it takes), and a newcomer at 1 ms over 1 call. Window
               total 1,000,080 ms → shares 0.600 / 0.300 / 0.100 / 0.00005; busy fraction 1,000,080 / 14,400,000 =
               0.069, above the 0.05 floor. The heavy statement JUMPED (0.10 → 0.60), the medium is steady (0.30), the
               light DROPPED (0.60 → 0.10 — below its normal, which the one-sided gate never fires on). The baseline
               series' counters continue where the history left them. */
            long heavyCalls = baseHeavyCalls, heavyMs = baseHeavyMs, heavyTemp = 0, mediumCalls = baseMediumCalls, mediumMs = baseMediumMs, lightCalls = baseLightCalls, lightMs = baseLightMs;
            for (var snapshot = 1; snapshot <= 48; snapshot++)
            {
                var at = windowStart.AddMinutes(snapshot * 5);
                heavyCalls += 50; heavyMs += 12_500; heavyTemp += 40;
                await PlantStatementAsync(connection, at, HeavyQueryId, 16384, heavyCalls, heavyMs, 50, 12_500, 300, heavyTemp, ct);
                await PlantStatementAsync(connection, at, HeavyQueryId, 16385, snapshot, snapshot, 1, 1, 300, 0, ct);
                mediumCalls += 250; mediumMs += 6_250;
                await PlantStatementAsync(connection, at, MediumQueryId, 16384, mediumCalls, mediumMs, 250, 6_250, 300, 0, ct);
                lightCalls += 5; lightMs += 2_083;
                await PlantStatementAsync(connection, at, LightQueryId, 16384, lightCalls, lightMs, 5, 2_083, null, 0, ct);
                await PlantStatementAsync(connection, at, NewcomerQueryId, 16384, snapshot, snapshot, 1, 1, 300, 0, ct);
            }
            await PlantTextAsync(connection, HeavyQueryId, HeavyText, ct);

            /* ── the collector alone, on the exact planted window (lane 7's read, unchanged). */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);
            Assert.Equal(14_400_000, context.ObservedDurationMs, precision: 3);

            var badActors = facts.Where(f => f.Source == PgTargetSources.QueriesSource).OrderByDescending(f => f.Value).ToList();
            Assert.Equal(4, badActors.Count);
            Assert.All(badActors, f => Assert.Null(f.DatabaseName));

            var heavy = badActors[0];
            Assert.Equal(PgTargetFactKeys.BadActorKey(HeavyQueryId), heavy.Key);
            /* 600,048 of 1,000,080 (600,000 + 48 + 300,000 + 48 × 2,083 + 48): the one-ms sibling series and the
               newcomer are in the denominator; the light statement's per-snapshot 2,083 ms is 99,984 over 48. */
            Assert.Equal(0.60, heavy.Value, precision: 3);
            Assert.Equal(heavy.Value, heavy.Metadata["share_of_window_time"]);
            Assert.Equal(1_000_080, heavy.Metadata["window_total_exec_ms"]);
            Assert.Equal(1_000_080 / 14_400_000.0, heavy.Metadata["window_busy_fraction"], precision: 9);
            Assert.Equal(2_448, heavy.Metadata["calls"]);
            Assert.Equal(600_048, heavy.Metadata["total_exec_ms"]);
            Assert.Equal(600_048 / 2_448.0, heavy.Metadata["mean_exec_ms"], precision: 9);
            Assert.Equal(900.5, heavy.Metadata["max_exec_ms"]);
            /* Stored interval, MAX per snapshot: 48 × 300 s of known span. */
            Assert.Equal(2_448 / 14_400.0, heavy.Metadata["calls_per_sec"], precision: 9);
            Assert.Equal(1_880, heavy.Metadata["temp_blks_written"]);
            Assert.Equal(2, heavy.Metadata["database_count"]);
            Assert.False(heavy.Metadata.ContainsKey("queryid"));

            var medium = badActors[1];
            Assert.Equal(PgTargetFactKeys.BadActorKey(MediumQueryId), medium.Key);
            Assert.Equal(0.30, medium.Value, precision: 3);
            Assert.Equal(1, medium.Metadata["database_count"]);

            /* The pre-V128 shape: no stored interval, so the span is the LAG over its 48 snapshots — 47 known
               five-minute gaps, and the first snapshot's calls are excluded from the rate with it. */
            var light = badActors[2];
            Assert.Equal(PgTargetFactKeys.BadActorKey(LightQueryId), light.Key);
            Assert.Equal(0.10, light.Value, precision: 3);
            Assert.Equal(240, light.Metadata["calls"]);
            Assert.Equal(235 / (47 * 300.0), light.Metadata["calls_per_sec"], precision: 9);

            var newcomer = badActors[3];
            Assert.Equal(PgTargetFactKeys.BadActorKey(NewcomerQueryId), newcomer.Key);

            /* ── the detector alone: the heavy statement is 25σ (the cap) beyond its own 0.10 normal on the peak AND
                  the mean; the medium is within its 0.30; the light is below its 0.60; the newcomer has no normal. */
            var provider = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, provider);
            var anomalies = await detector.DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyBadActorShare);
            Assert.Equal(HeavyQueryId.ToString(CultureInfo.InvariantCulture), anomaly.ObjectName);
            Assert.Equal(0.60, anomaly.Value, precision: 3);
            Assert.Equal(0.60, anomaly.Metadata["window_share"], precision: 3);
            Assert.Equal(0.60, anomaly.Metadata["peak_share"], precision: 3);
            Assert.Equal(0.60, anomaly.Metadata["mean_share"], precision: 3);
            Assert.Equal(AnomalyThresholds.SigmaDisplayCap, anomaly.Metadata["deviation_sigma"]);
            Assert.Equal(AnomalyThresholds.SigmaDisplayCap, anomaly.Metadata["mean_sigma"]);
            Assert.Equal(0.10, anomaly.Metadata["baseline_mean"], precision: 3);   /* 2,083 of 20,833 */
            Assert.Equal(48, anomaly.Metadata["baseline_samples"]);
            Assert.Equal((double)BaselineTier.Full, anomaly.Metadata["baseline_tier"]);
            Assert.Equal(1.0, anomaly.Metadata["confidence"]);
            Assert.Equal(6.0, anomaly.Metadata["ratio"], precision: 2);
            Assert.Equal(0, anomaly.Metadata["baseline_low_quality"]);
            Assert.Equal(0, anomaly.Metadata["threshold_lineage"]);
            Assert.Equal(4, anomaly.Metadata["candidates_evaluated"]);
            Assert.Equal(1, anomaly.Metadata["candidates_fired"]);
            Assert.Equal(1, anomaly.Metadata["candidates_without_baseline"]);
            Assert.Equal(2, anomaly.Metadata[PgTargetAnomalyDetector.OwnNormalMetadataPrefix + HeavyQueryId.ToString(CultureInfo.InvariantCulture)]);
            Assert.Equal(1, anomaly.Metadata[PgTargetAnomalyDetector.OwnNormalMetadataPrefix + MediumQueryId.ToString(CultureInfo.InvariantCulture)]);
            Assert.Equal(1, anomaly.Metadata[PgTargetAnomalyDetector.OwnNormalMetadataPrefix + LightQueryId.ToString(CultureInfo.InvariantCulture)]);
            Assert.Equal(0, anomaly.Metadata[PgTargetAnomalyDetector.OwnNormalMetadataPrefix + NewcomerQueryId.ToString(CultureInfo.InvariantCulture)]);

            /* ── scored through the real scorer: the anomaly grades 1.0 and lifts the heavy card to 0.24 × 3.5 = 0.84;
                  the steady, the dropped and the newcomer stay context under the story line, stamped 0. */
            facts.AddRange(anomalies);
            new FactScorer().ScoreAll(facts);
            Assert.Equal(1.0, anomaly.Severity, precision: 9);
            Assert.Equal(0.84, heavy.Severity, precision: 6);
            Assert.Equal(0.3 * (0.5 + 0.5 * medium.Value), medium.Severity, precision: 6);
            Assert.Equal(0.3 * (0.5 + 0.5 * light.Value), light.Severity, precision: 6);
            Assert.InRange(newcomer.Severity, 0.15, 0.1501);
            Assert.All(badActors, f => Assert.Equal(0, f.Metadata["threshold_lineage"]));
            Assert.Equal(2, heavy.Metadata["share_band"]);
            Assert.Equal(1, medium.Metadata["share_band"]);
            Assert.Equal(0, light.Metadata["share_band"]);

            /* ── THE EXIT CRITERION: the real analyze_server tool. */
            var service = new DarlingAnalysisService(postgres);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                /* ONE finding: the anomaly roots and walks into the heavy statement's card. No card roots on its share. */
                var card = Assert.Single(findings);
                Assert.Equal(PgTargetFactKeys.AnomalyBadActorShare, card.GetProperty("root_fact").GetProperty("key").GetString());
                Assert.Equal($"{PgTargetFactKeys.AnomalyBadActorShare} → {PgTargetFactKeys.BadActorKey(HeavyQueryId)}", card.GetProperty("story_path").GetString());
                Assert.Equal(1.0, card.GetProperty("severity").GetDouble());
                /* The tool's window is now − 4 h → now, a minute off the planted bounds; the ratio holds. */
                Assert.InRange(card.GetProperty("root_fact").GetProperty("value").GetDouble(), 0.58, 0.62);

                var advice = card.GetProperty("advice");
                Assert.Equal("One statement held 60% of the window's execution time — 25σ beyond its own normal for this hour", advice.GetProperty("headline").GetString());
                var investigation = advice.GetProperty("investigation").GetString()!;
                Assert.Contains("queryid -1234567890123456789", investigation, StringComparison.Ordinal);
                Assert.Contains("against its own routine of 10% ± 0.1% for this hour-of-week", investigation, StringComparison.Ordinal);
                Assert.Contains("The statement ran 2,4", investigation, StringComparison.Ordinal);
                Assert.Contains("Of the window's 4 top statements, 1 was beyond its own normal and 1 has no trustworthy own-normal yet", investigation, StringComparison.Ordinal);

                /* The drill-down rides on the path: the card is the leaf, and its text is here. */
                var detail = card.GetProperty("drill_down").GetProperty("pg_bad_actor_statements").GetProperty(PgTargetFactKeys.BadActorKey(HeavyQueryId));
                Assert.Equal("-1234567890123456789", detail.GetProperty("queryid").GetString());
                Assert.Equal(HeavyText, detail.GetProperty("query_text").GetString());
                Assert.Equal(JsonValueKind.Number, detail.GetProperty("text_hash").ValueKind);
                Assert.Equal(2, detail.GetProperty("databases").GetArrayLength());

                var tools = card.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()).ToList();
                Assert.Contains("get_pg_query_duration_trend", tools);
                Assert.Contains("get_pg_top_queries", tools);
                Assert.Contains("get_pg_plans", tools);
            }

            /* ── get_analysis_facts under the family's source: the four context cards with their stamps and bands — the
                  steady medium and the dropped light are LOW context cards, visible here and nowhere as a finding. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.QueriesSource);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var root = doc.RootElement;
                /* total_facts is the pass's unfiltered count (four bad actors + the registry major + the anomaly); shown
                   is the filtered page. */
                Assert.Equal(6, root.GetProperty("total_facts").GetInt32());
                Assert.Equal(4, root.GetProperty("shown").GetInt32());
                var shown = root.GetProperty("facts").EnumerateArray().ToDictionary(f => f.GetProperty("key").GetString()!, f => f);
                foreach (var fact in shown.Values)
                {
                    Assert.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, fact.GetProperty("key").GetString(), StringComparison.Ordinal);
                    Assert.Equal(0, fact.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
                }
                Assert.InRange(shown[PgTargetFactKeys.BadActorKey(HeavyQueryId)].GetProperty("severity").GetDouble(), 0.83, 0.85);
                Assert.InRange(shown[PgTargetFactKeys.BadActorKey(MediumQueryId)].GetProperty("severity").GetDouble(), 0.19, 0.20);
                Assert.InRange(shown[PgTargetFactKeys.BadActorKey(LightQueryId)].GetProperty("severity").GetDouble(), 0.16, 0.17);
                Assert.Equal(1, shown[PgTargetFactKeys.BadActorKey(MediumQueryId)].GetProperty("metadata").GetProperty("share_band").GetDouble());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One <c>pg_statement_stats</c> row as the collector writes it: cumulative counters beside the
    /// deltas it computed at the write, and the interval those deltas accrued over (NULL = a pre-V128 row).</summary>
    private static async Task PlantStatementAsync(
        NpgsqlConnection connection, DateTime at, long queryId, long databaseId, long calls, long totalMs,
        long deltaCalls, long deltaMs, int? intervalSeconds, long tempBlocksWritten, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read,
     temp_blks_read, temp_blks_written, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, 10, TRUE, $7, $8, 900.5, 100, 10, 5, 0, $9, 0, $10, $11, 10, $12)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(databaseId);
        command.Parameters.AddWithValue(calls);
        command.Parameters.AddWithValue((double)totalMs);
        command.Parameters.AddWithValue(tempBlocksWritten);
        command.Parameters.AddWithValue(deltaCalls);
        command.Parameters.AddWithValue(deltaMs);
        command.Parameters.Add(new NpgsqlParameter { Value = intervalSeconds.HasValue ? intervalSeconds.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantTextAsync(NpgsqlConnection connection, long queryId, string text, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
VALUES ($1, $2, $3, $4, $4)
ON CONFLICT (server_id, queryid) DO UPDATE SET query_text = EXCLUDED.query_text", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(text);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_statement_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_statement_text WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
