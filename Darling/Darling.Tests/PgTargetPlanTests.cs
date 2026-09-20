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
using System.Text.RegularExpressions;
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
/// The plan family of the PostgreSQL-target analysis engine (#3691 lane 27, design §6): <c>PG_PLAN_REGRESSION</c>,
/// <c>PG_PARAMETER_SENSITIVITY</c>, <c>ANOMALY_PG_PLAN_REGRESSION</c>. Lane 30's <c>PG_SEQ_SCAN_ADVISORY</c> shares the
/// partials and is pinned in <c>PgTargetSeqScanTests</c>; the shared-file counts here (lineage stamps, command sites,
/// edges) include its arms.
///
/// <para><b>Ungated:</b> every bar unmeasured by text and <c>threshold_lineage = 0</c> by stamp, and none a SQL Server
/// constant by value; the regression ramp (2× → 0.5, 5× → 1.0, capped), the self-gates (under 2×, under the 50 ms
/// delta, under 20 calls on either side, the <c>unavailable</c> shape); the collector's PURE picks (the worst cleared
/// flip over a larger-ratio flip that misses the delta bar; a comparable flip shown at 0 when none clears; the
/// sensitivity pick, its two <c>unavailable</c> reasons, and silence when predicates were read and none is skewed);
/// the amplifiers through the real <c>ScoreAll</c> (same-statement bad actor, the anomaly's verdict, the D5 lift to
/// 0.6 — and NOT for a different statement's bad actor); the edges — declared set, the alias edge opening only when
/// the pass's top bad actor IS the regressed statement, the same-id sensitivity edge, the anomaly's fold edge — and
/// the traversal; the advice by clause for every branch and both <c>unavailable</c> shapes, with the queryid caveat and
/// no <c>CREATE INDEX</c>; the three collector reads by text (stored deltas, the first-vs-last split, the 48-bit hash
/// prefix guard, the orphan exclusion, the scorer's bars passed as parameters, the state lookback, no clock, no
/// <c>EXTRACT … FROM</c>); the baseline arm and the window read sharing one grain and one zero rule; the anomaly's
/// heavy-tail cutoff by reference; the routing / fence / roster pins flipped from stub to filled.</para>
///
/// <para><b>Gated e2e</b> (<c>DARLING_TEST_PG</c>): 31 days of one-minute <c>pg_statement_stats</c> for five statements
/// (server-wide per-call mean ≈ 10 ms, jittered), then a 4-hour window in which the heaviest statement's captured plan
/// changes twice (three hashes) and its per-call mean steps 20 ms → 200 ms; <c>pg_plan_capture</c> rows for all three
/// hashes; a skewed predicate column (<c>pg_predicate_stats</c> × <c>pg_column_stats</c>); readiness and extension rows
/// saying both instruments are on. The REAL <c>analyze_server</c> anchored at the window's end returns
/// <c>PG_PLAN_REGRESSION</c> naming the id, lifted by the same-id bad actor and the anomaly to 1.6 and walking to the
/// bad actor; <c>PG_PARAMETER_SENSITIVITY</c> chained on the same id at 0.6; <c>ANOMALY_PG_PLAN_REGRESSION</c> folded
/// into the regression's incident.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetPlanTests
{
    private const string ServerName = "darling-pg-target-plan-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const long HotQueryId = 7_001_001_001L;
    private const string HashA = "A1B2C3D4E5F60718293A4B5C6D7E8F90";
    private const string HashB = "0F1E2D3C4B5A69788796A5B4C3D2E1F0";
    private const string HashC = "FEDCBA98765432100123456789ABCDEF";

    /* ───────────────────────── lineage ───────────────────────── */

    [Fact]
    public void EveryBar_IsUnmeasured_SaysSo_AndIsNoSqlServerConstantByValue()
    {
        /* Per constant, PgTargetMeasuredLineageTests.s_stillUnmeasured walks the block above each declaration; here the
           file as a whole: eleven declared bars (seven of lane 27's, four of lane 30's), every marker the unmeasured
           shape, no bare "measured:" anywhere; three graded arms, three stamps. */
        var scorer = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Plans.cs");
        Assert.True(Regex.Matches(scorer, @"\bunmeasured:").Count >= 11);
        Assert.DoesNotMatch(new Regex(@"(?<!un)measured:"), scorer);
        Assert.DoesNotContain("[\"threshold_lineage\"] = 1;", scorer, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(scorer, Regex.Escape("[\"threshold_lineage\"] = 0;")).Count);

        Assert.Equal(2.0, PgTargetScorer.PlanRegressionRatioConcerning);
        Assert.Equal(5.0, PgTargetScorer.PlanRegressionRatioCritical);
        Assert.Equal(50.0, PgTargetScorer.PlanRegressionMinDeltaMs);
        Assert.Equal(20, PgTargetScorer.PlanRegressionMinCallsPerSide);
        Assert.Equal(3, PgTargetScorer.ParameterSensitivityMinHashes);
        Assert.Equal(0.5, PgTargetScorer.ParameterSensitivitySkewFrequency);
        Assert.Equal(0.4, PgTargetScorer.ParameterSensitivityBase);
        /* D5: base × (1 + boost) reaches exactly the 0.6 the brief names, over the 0.5 story threshold. */
        Assert.Equal(0.6, PgTargetScorer.ParameterSensitivityBase * (1 + PgTargetScorer.ParameterSensitivityCoFireBoost), precision: 9);
        /* Two corroborators clear the 1.5 notify floor; one does not. */
        Assert.True(1.0 * (1 + 2 * PgTargetScorer.PlanCoFireBoost) > 1.5);
        Assert.True(1.0 * (1 + PgTargetScorer.PlanCoFireBoost) < 1.5);

        /* The anomaly's two bars: in the lane 27 block, unmeasured, not the SQL Server query-duration floor by value. */
        var thresholds = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "Baselines", "AnomalyThresholds.cs");
        Assert.Contains("// lane 27 (#3691, v3)", thresholds, StringComparison.Ordinal);
        Assert.Equal(10.0, AnomalyThresholds.PgStatementMeanMsFloor);
        Assert.Equal(250.0, AnomalyThresholds.PgStatementMeanMsFallback);
        Assert.NotEqual(AnomalyThresholds.QueryDurationFloorUs, AnomalyThresholds.PgStatementMeanMsFloor);
        Assert.NotEqual(AnomalyThresholds.QueryDurationFallbackUs, AnomalyThresholds.PgStatementMeanMsFallback);
        Assert.True(AnomalyThresholds.PgStatementMeanMsFallback > AnomalyThresholds.PgStatementMeanMsFloor);
    }

    /* ───────────────────────── the scorer ───────────────────────── */

    [Theory]
    [InlineData(50, 100, 0.5)]       /* 2×, +50 ms: concerning */
    [InlineData(40, 140, 0.75)]      /* 3.5×: midway */
    [InlineData(20, 100, 1.0)]       /* 5×: critical */
    [InlineData(20, 400, 1.0)]       /* 20×: capped */
    [InlineData(100, 199, 0.0)]      /* 1.99×: under the concerning ratio — not a regression */
    [InlineData(10, 30, 0.0)]        /* 3× but +20 ms: under the absolute delta */
    public void TheRegression_GradesTheRatio_SelfGatesUnderTheBars_AndStampsLineageZero(double before, double after, double expected)
    {
        var fact = Regression(before, after);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        Assert.Equal(0, fact.Metadata["threshold_lineage"]);
    }

    [Fact]
    public void TheRegression_NeedsTwentyCallsOnEachSide_AndAnUnavailableFactIsGradedNothing()
    {
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(Regression(20, 200, callsBefore: 20, callsAfter: 20)), precision: 9);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Regression(20, 200, callsBefore: 19, callsAfter: 2_000)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Regression(20, 200, callsBefore: 2_000, callsAfter: 19)));

        var unavailable = UnavailableRegression();
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(unavailable));
        /* No bar decided, so the stamp stays the collector's 1 — the write family's rule for the unavailable shape. */
        Assert.Equal(1, unavailable.Metadata["threshold_lineage"]);

        /* A fact with no means (a foreign shape under the source) scores 0 and is not stamped. */
        var bare = new Fact { Source = PgTargetSources.PlansSource, Key = PgTargetFactKeys.PlanRegression, Value = 9, ServerId = 1 };
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(bare));
        Assert.False(bare.Metadata.ContainsKey("threshold_lineage"));
    }

    [Fact]
    public void TheSensitivity_RootsAtTheAdvisoryBase_OnlyWithThreeHashesAndASkewedColumn()
    {
        Assert.Equal(0.4, PgTargetScorer.ScoreBase(Sensitivity(hashes: 3, frequency: 0.5)), precision: 9);
        Assert.Equal(0.4, PgTargetScorer.ScoreBase(Sensitivity(hashes: 7, frequency: 0.95)), precision: 9);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Sensitivity(hashes: 2, frequency: 0.9)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Sensitivity(hashes: 3, frequency: 0.49)));
        var graded = Sensitivity(hashes: 3, frequency: 0.49);
        PgTargetScorer.ScoreBase(graded);
        Assert.Equal(0, graded.Metadata["threshold_lineage"]);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(UnavailableSensitivity(qualstatsAbsent: true)));

        /* Lane 30's key self-gates on its own pairs: a bare fact under it grades 0 (its shapes are PgTargetSeqScanTests'). */
        var seqScan = new Fact { Source = PgTargetSources.PlansSource, Key = PgTargetFactKeys.SeqScanAdvisory, Value = 42, ServerId = 1 };
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(seqScan));
        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        Assert.Single((System.Collections.IEnumerable)amplifiers.Invoke(null, [PgTargetFactKeys.SeqScanAdvisory])!);
    }

    /* ───────────────────────── the collector's pure picks ───────────────────────── */

    [Fact]
    public void PickRegression_PrefersTheWorstFlipThatClearsEveryBar_OverALargerRatioThatMissesTheDelta()
    {
        /* 1 ms → 3 ms is 3× and +2 ms (misses the delta); 20 → 50 is 2.5× and +30 ms (misses the delta);
           40 → 100 is 2.5× and +60 ms (clears); 30 → 120 is 4× and +90 ms (clears, larger ratio). */
        var flips = new[]
        {
            Flip(1, callsBefore: 500, msBefore: 500, callsAfter: 500, msAfter: 1_500),
            Flip(2, callsBefore: 100, msBefore: 2_000, callsAfter: 100, msAfter: 5_000),
            Flip(3, callsBefore: 100, msBefore: 4_000, callsAfter: 100, msAfter: 10_000),
            Flip(4, callsBefore: 100, msBefore: 3_000, callsAfter: 100, msAfter: 12_000),
        };
        Assert.Equal(4, PgTargetFactCollector.PickRegression(flips)!.QueryId);

        /* Nothing clears: the largest-ratio COMPARABLE flip is shown (the scorer grades it 0). */
        var none = new[]
        {
            Flip(1, callsBefore: 500, msBefore: 500, callsAfter: 500, msAfter: 1_500),
            Flip(2, callsBefore: 100, msBefore: 2_000, callsAfter: 100, msAfter: 5_000),
        };
        Assert.Equal(1, PgTargetFactCollector.PickRegression(none)!.QueryId);

        /* Too few calls on a side is not comparable at all; a side with no calls has no mean. */
        Assert.Null(PgTargetFactCollector.PickRegression([Flip(1, callsBefore: 19, msBefore: 380, callsAfter: 1_000, msAfter: 200_000)]));
        Assert.Null(PgTargetFactCollector.PickRegression([Flip(1, callsBefore: 0, msBefore: 0, callsAfter: 1_000, msAfter: 200_000)]));
        Assert.Null(PgTargetFactCollector.PickRegression([]));
    }

    [Fact]
    public void PickSensitivity_TakesTheFirstSkewedRow_ElseOneUnavailableWithTheReason_ElseNothing()
    {
        var skewed = PgTargetFactCollector.PickSensitivity(
        [
            new(11, 3, PredicateColumns: 2, SkewedColumns: 1, "appdb", TopValueFrequency: 0.62, NDistinct: 1_200, WorstEstimateErrorRatio: 57.9),
            new(12, 5, PredicateColumns: 1, SkewedColumns: 0, "appdb", TopValueFrequency: 0.1, NDistinct: -1, WorstEstimateErrorRatio: 1.04),
        ], qualstatsAbsent: false, serverId: 7)!;
        Assert.Equal(PgTargetFactKeys.ParameterSensitivity, skewed.Key);
        Assert.Equal("11", skewed.ObjectName);
        Assert.Equal("appdb", skewed.DatabaseName);
        Assert.Equal(7, skewed.ServerId);
        Assert.Equal(0.62, skewed.Value);
        Assert.Equal(3, skewed.Metadata[PgTargetScorer.PlanHashCountKey]);
        Assert.Equal(2, skewed.Metadata["predicate_columns"]);
        Assert.Equal(1, skewed.Metadata["skewed_columns"]);
        Assert.Equal(2, skewed.Metadata["sensitive_statements"]);
        Assert.Equal(1_200, skewed.Metadata["n_distinct"]);
        Assert.Equal(57.9, skewed.Metadata["worst_estimate_error_ratio"]);
        Assert.False(skewed.Metadata.ContainsKey(PgTargetScorer.PlanUnavailableKey));

        /* Hashes but no predicate anywhere: the extension's state decides the reason, the most-flipped statement carries it. */
        var absent = PgTargetFactCollector.PickSensitivity(
        [
            new(11, 3, PredicateColumns: 0, SkewedColumns: 0, null, null, null, null),
            new(12, 5, PredicateColumns: 0, SkewedColumns: 0, null, null, null, null),
        ], qualstatsAbsent: true, serverId: 7)!;
        Assert.Equal("12", absent.ObjectName);
        Assert.Equal(0, absent.Value);
        Assert.Equal(1, absent.Metadata[PgTargetScorer.PlanUnavailableKey]);
        Assert.Equal(1, absent.Metadata[PgTargetScorer.PlanReasonQualstatsAbsentKey]);
        Assert.False(absent.Metadata.ContainsKey(PgTargetScorer.PlanReasonNoPredicateSampledKey));
        Assert.Equal(1, absent.Metadata["threshold_lineage"]);

        var unsampled = PgTargetFactCollector.PickSensitivity([new(11, 3, 0, 0, null, null, null, null)], qualstatsAbsent: false, serverId: 7)!;
        Assert.Equal(1, unsampled.Metadata[PgTargetScorer.PlanReasonNoPredicateSampledKey]);
        Assert.False(unsampled.Metadata.ContainsKey(PgTargetScorer.PlanReasonQualstatsAbsentKey));

        /* Predicates were read and none is skewed: variance without the mechanism's evidence is not a card. */
        Assert.Null(PgTargetFactCollector.PickSensitivity([new(11, 3, PredicateColumns: 2, SkewedColumns: 0, "appdb", 0.2, 500, 1.1)], qualstatsAbsent: false, serverId: 7));
        Assert.Null(PgTargetFactCollector.PickSensitivity([], qualstatsAbsent: true, serverId: 7));
    }

    /* ───────────────────────── the amplifiers ───────────────────────── */

    [Fact]
    public void TheRegressionsAmplifiers_AreTheSameStatementsBadActor_AndTheAnomaly_EachOnItsOwnVerdict()
    {
        /* Alone at 5×: 1.0, two amplifiers evaluated, none matched. */
        var alone = Regression(20, 200);
        new FactScorer().ScoreAll([alone]);
        Assert.Equal(1.0, alone.Severity, precision: 9);
        Assert.Equal(2, alone.AmplifierResults.Count);
        Assert.All(alone.AmplifierResults, r => Assert.False(r.Matched));

        /* The same statement's bad actor fired: 1.3. A DIFFERENT statement's bad actor, however hot: still 1.0. */
        var regression = Regression(20, 200);
        new FactScorer().ScoreAll([regression, BadActor(HotQueryId, share: 0.9)]);
        Assert.Equal(1.3, regression.Severity, precision: 9);
        var other = Regression(20, 200);
        new FactScorer().ScoreAll([other, BadActor(HotQueryId + 1, share: 0.9)]);
        Assert.Equal(1.0, other.Severity, precision: 9);

        /* Both: 1.0 × (1 + 0.3 + 0.3) = 1.6, over the notify floor. */
        var corroborated = Regression(20, 200);
        var anomaly = Anomaly(sigma: 8);
        new FactScorer().ScoreAll([corroborated, BadActor(HotQueryId, share: 0.9), anomaly]);
        Assert.Equal(1.6, corroborated.Severity, precision: 9);
        Assert.True(anomaly.BaseSeverity > 0);

        /* An unfired sibling amplifies nothing: a bad actor under the busy floor, a low-quality anomaly under its bar. */
        var quiet = Regression(20, 200);
        new FactScorer().ScoreAll([quiet, BadActor(HotQueryId, share: 0.9, busy: 0.01), Anomaly(sigma: 1)]);
        Assert.Equal(1.0, quiet.Severity, precision: 9);
    }

    [Fact]
    public void TheSensitivitysOnlyAmplifier_IsTheSameStatementsRegression_LiftingItTo0Point6()
    {
        var alone = Sensitivity(hashes: 3, frequency: 0.62);
        new FactScorer().ScoreAll([alone]);
        Assert.Equal(0.4, alone.Severity, precision: 9);
        Assert.Single(alone.AmplifierResults);

        var lifted = Sensitivity(hashes: 3, frequency: 0.62);
        new FactScorer().ScoreAll([lifted, Regression(20, 200)]);
        Assert.Equal(0.6, lifted.Severity, precision: 9);

        var otherStatement = Sensitivity(hashes: 3, frequency: 0.62, queryId: HotQueryId + 1);
        new FactScorer().ScoreAll([otherStatement, Regression(20, 200)]);
        Assert.Equal(0.4, otherStatement.Severity, precision: 9);

        var underBar = Sensitivity(hashes: 3, frequency: 0.62);
        new FactScorer().ScoreAll([underBar, Regression(100, 150)]);
        Assert.Equal(0.4, underBar.Severity, precision: 9);
    }

    /* ───────────────────────── the graph ───────────────────────── */

    [Fact]
    public void TheEdges_AreDeclared_AndTheAliasEdgeOpensOnlyWhenTheTopBadActorIsTheRegressedStatement()
    {
        var graph = new PgTargetRelationshipGraph();
        Assert.Equal(PgTargetFactKeys.BadActorFamily, Assert.Single(graph.GetAllEdges(PgTargetFactKeys.PlanRegression)).Destination);
        Assert.Equal(PgTargetFactKeys.PlanRegression, Assert.Single(graph.GetAllEdges(PgTargetFactKeys.ParameterSensitivity)).Destination);
        Assert.Equal(PgTargetFactKeys.PlanRegression, Assert.Single(graph.GetAllEdges(PgTargetFactKeys.AnomalyPlanRegression)).Destination);
        Assert.Equal(2, graph.GetAllEdges(PgTargetFactKeys.SeqScanAdvisory).Count);   /* lane 30's two, pinned in PgTargetSeqScanTests */
        Assert.All(graph.GetAllEdges(PgTargetFactKeys.PlanRegression).Concat(graph.GetAllEdges(PgTargetFactKeys.ParameterSensitivity)).Concat(graph.GetAllEdges(PgTargetFactKeys.AnomalyPlanRegression)),
            e => Assert.Equal("plans", e.Category));
        var file = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Plans.cs"));
        Assert.Equal(5, Regex.Matches(file, @"AddEdge\(").Count);
        Assert.DoesNotContain(".Severity", file, StringComparison.Ordinal);

        /* The regression and ITS bad actor, the only one: the alias resolves to it and the edge opens. */
        var regression = Regression(20, 200);
        var own = BadActor(HotQueryId, share: 0.9);
        var facts = new List<Fact> { regression, own };
        new FactScorer().ScoreAll(facts);
        var active = graph.GetActiveEdges(PgTargetFactKeys.PlanRegression, facts.ToFactLookup());
        Assert.Equal(PgTargetFactKeys.BadActorKey(HotQueryId), Assert.Single(active).Destination);

        /* A hotter bad actor of ANOTHER statement outranks: the alias would point at the wrong statement, so the edge
           stays shut (the amplifier still lifted the regression — it needs no edge). */
        var outranked = Regression(20, 200);
        var mine = BadActor(HotQueryId, share: 0.3);
        var hotter = BadActor(HotQueryId + 1, share: 0.9);
        var mixed = new List<Fact> { outranked, mine, hotter };
        new FactScorer().ScoreAll(mixed);
        Assert.Equal(1.3, outranked.Severity, precision: 9);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.PlanRegression, mixed.ToFactLookup()));

        /* No bad actor at all: the alias edge is dropped, not thrown. */
        var lonely = new List<Fact> { Regression(20, 200) };
        new FactScorer().ScoreAll(lonely);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.PlanRegression, lonely.ToFactLookup()));
    }

    [Fact]
    public void TheSensitivityAndTheAnomaly_WalkIntoAFiredRegressionOnTheSameStatement_AndNotOtherwise()
    {
        var graph = new PgTargetRelationshipGraph();

        var fired = new List<Fact> { Sensitivity(3, 0.62), Regression(20, 200), Anomaly(sigma: 8) };
        new FactScorer().ScoreAll(fired);
        Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.ParameterSensitivity, fired.ToFactLookup()));
        Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.AnomalyPlanRegression, fired.ToFactLookup()));

        var otherStatement = new List<Fact> { Sensitivity(3, 0.62, queryId: HotQueryId + 1), Regression(20, 200) };
        new FactScorer().ScoreAll(otherStatement);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.ParameterSensitivity, otherStatement.ToFactLookup()));

        var unfired = new List<Fact> { Sensitivity(3, 0.62), Regression(100, 150), Anomaly(sigma: 8) };
        new FactScorer().ScoreAll(unfired);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.ParameterSensitivity, unfired.ToFactLookup()));
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.AnomalyPlanRegression, unfired.ToFactLookup()));

        /* The traversal: regression 1.6 roots and walks to its bad actor; the sensitivity (0.6) is its own story into the regression. */
        var facts = new List<Fact> { Regression(20, 200), BadActor(HotQueryId, share: 0.9), Sensitivity(3, 0.62), Anomaly(sigma: 8) };
        new FactScorer().ScoreAll(facts);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);
        var regressionStory = Assert.Single(stories, s => s.Path[0] == PgTargetFactKeys.PlanRegression);
        Assert.Equal(new[] { PgTargetFactKeys.PlanRegression, PgTargetFactKeys.BadActorKey(HotQueryId) }, regressionStory.Path);
        Assert.Equal(PgTargetSources.PlansSource, regressionStory.Category);
        Assert.Contains(stories, s => s.Path[0] == PgTargetFactKeys.ParameterSensitivity);
    }

    /* ───────────────────────── the advice ───────────────────────── */

    [Fact]
    public void TheRegressionAdvice_StatesTheStep_TheHashes_TheCoFires_TheCaveats_AndNoDdl()
    {
        var regression = Regression(20, 200, callsBefore: 6_000, callsAfter: 3_000, hashes: 3);
        regression.Metadata["plan_hash_before_prefix"] = 0xA1B2C3D4E5F6;
        regression.Metadata["plan_hash_after_prefix"] = 0xFEDCBA987654;
        regression.Metadata["flip_age_s"] = 1_800;
        regression.Metadata["top_node_changed"] = 1;
        regression.Metadata["node_count_delta"] = 3;
        regression.Metadata["flipped_statements"] = 2;
        var facts = new List<Fact> { regression, BadActor(HotQueryId, 0.9), Anomaly(sigma: 8), Sensitivity(3, 0.62) };
        new FactScorer().ScoreAll(facts);

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.PlanRegression, facts.ToFactLookup())!;
        Assert.Equal($"Statement queryid {HotQueryId} got 10× slower per call when its plan changed (20 ms → 200 ms)", advice.Headline);
        Assert.Contains("stepped from 20 ms to 200 ms (10×, +180 ms per call) at the moment its captured plan changed (30 min before the window's end), over 6,000 calls before the flip and 3,000 after.", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("captured under 3 distinct plan hashes in the window (plan hash A1B2C3D4E5F6… before the flip, FEDCBA987654… after); the plan's top node changed and the node count moved by +3.", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("2 statements changed plan in the window", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_BAD_ACTOR fired for the same queryid", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("ANOMALY_PG_PLAN_REGRESSION co-fired: the server-wide per-call mean reached 55 ms", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_PARAMETER_SENSITIVITY names the likely mechanism: a predicate column of this statement has a top value covering 62% of its table.", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("chosen, not measured (threshold_lineage = 0)", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("'not slow enough to log', never 'no plan change'", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("re-keyed by a major upgrade", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains($"get_pg_plans for queryid {HotQueryId}", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("The step is 180 ms on every call, 3,000 calls since the flip", advice.Remediation, StringComparison.Ordinal);
        foreach (var lever in new[] { "plan_cache_mode = force_custom_plan", "planning time on every execution", "statistics target", "ANALYZE", "an application change" })
            Assert.Contains(lever, advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", advice.Investigation + advice.Remediation, StringComparison.OrdinalIgnoreCase);
        Assert.Null(advice.RemediationTsql);

        /* Alone: no co-fire sentence, no hash clause when the prefixes are absent, the same-top-node branch. */
        var plain = Regression(20, 200);
        var lookup = new List<Fact> { plain }.ToFactLookup();
        new FactScorer().ScoreAll([plain]);
        var solo = PgTargetAdvice.Compose(PgTargetFactKeys.PlanRegression, lookup)!;
        Assert.DoesNotContain("PG_BAD_ACTOR fired", solo.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("co-fired", solo.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("plan hash ", solo.Investigation, StringComparison.Ordinal);
        Assert.Contains("the plan's top node is the same in both.", solo.Investigation, StringComparison.Ordinal);

        /* The static block claims no figure and is the same object from both entry points. */
        var stat = PgTargetAdvice.Static(PgTargetFactKeys.PlanRegression)!;
        Assert.Equal("A statement's per-call time stepped up when its captured plan changed", stat.Headline);
        Assert.DoesNotContain("%", stat.Investigation, StringComparison.Ordinal);
        Assert.Same(stat, FactAdvice.GetForFactKey(PgTargetFactKeys.PlanRegression));
        Assert.Contains("not slow enough to log", stat.Investigation, StringComparison.Ordinal);
        Assert.Contains("re-keyed by a major upgrade", stat.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheUnavailableShapes_SayWhichPreconditionIsMissing()
    {
        var off = PgTargetAdvice.Compose(PgTargetFactKeys.PlanRegression, new List<Fact> { UnavailableRegression() }.ToFactLookup())!;
        Assert.Equal("Plan regressions cannot be seen on this server: auto_explain is not loaded", off.Headline);
        Assert.Contains("not in shared_preload_libraries", off.Investigation, StringComparison.Ordinal);
        Assert.Contains("Nothing is graded off this fact", off.Investigation, StringComparison.Ordinal);
        Assert.Contains("log volume", off.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_plan_capture_readiness", off.Remediation, StringComparison.Ordinal);

        var absent = PgTargetAdvice.Compose(PgTargetFactKeys.ParameterSensitivity, new List<Fact> { UnavailableSensitivity(qualstatsAbsent: true) }.ToFactLookup())!;
        Assert.Equal($"Statement queryid {HotQueryId} was captured under 4 plans, but its predicate columns cannot be read", absent.Headline);
        Assert.Contains("pg_qualstats is not installed in any database", absent.Investigation, StringComparison.Ordinal);
        Assert.Contains("pg_qualstats.sample_rate", absent.Remediation, StringComparison.Ordinal);

        var unsampled = PgTargetAdvice.Compose(PgTargetFactKeys.ParameterSensitivity, new List<Fact> { UnavailableSensitivity(qualstatsAbsent: false) }.ToFactLookup())!;
        Assert.Contains("recorded no predicate for this statement", unsampled.Investigation, StringComparison.Ordinal);
        Assert.Contains("1 / max_connections", unsampled.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", off.Remediation + absent.Remediation + unsampled.Remediation, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheSensitivityAdvice_NamesTheSkew_TheEstimateError_AndSwitchesOnTheRegressionCoFire()
    {
        var sensitivity = Sensitivity(3, 0.62);
        sensitivity.DatabaseName = "appdb";
        sensitivity.Metadata["n_distinct"] = 1_200;
        sensitivity.Metadata["worst_estimate_error_ratio"] = 57.9;
        sensitivity.Metadata["predicate_columns"] = 2;
        sensitivity.Metadata["skewed_columns"] = 1;
        var facts = new List<Fact> { sensitivity, Regression(20, 200) };
        new FactScorer().ScoreAll(facts);

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.ParameterSensitivity, facts.ToFactLookup())!;
        Assert.Equal($"Statement queryid {HotQueryId} was planned 3 different ways in the window, and a predicate column has one value covering 62% of its table", advice.Headline);
        Assert.Contains("pg_qualstats names 2 predicate columns for it, of which 1 is skewed: the most skewed has a top value covering 62% of its table (in appdb), with 1,200 distinct values.", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("worst row estimate on that predicate was off by 57.9×", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_PLAN_REGRESSION fired on this statement in the same window (20 ms → 200 ms per call)", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("No index is proposed here", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", advice.Investigation + advice.Remediation, StringComparison.OrdinalIgnoreCase);

        /* The ratio-shaped n_distinct, and the under-threshold sentence without a regression. */
        var alone = Sensitivity(3, 0.62);
        alone.Metadata["n_distinct"] = -0.25;
        var soloAdvice = PgTargetAdvice.Compose(PgTargetFactKeys.ParameterSensitivity, new List<Fact> { alone }.ToFactLookup())!;
        Assert.Contains("n_distinct -0.25 (a ratio of the row count: distinct values ≈ 25% of rows)", soloAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("No regression fired on this statement in the window", soloAdvice.Investigation, StringComparison.Ordinal);

        Assert.Same(PgTargetAdvice.Static(PgTargetFactKeys.ParameterSensitivity), FactAdvice.GetForFactKey(PgTargetFactKeys.ParameterSensitivity));
    }

    [Fact]
    public void TheAnomalyAdvice_StatesThePeakAndSigma_NamesTheServerWideGrain_OrFirstOccurrence()
    {
        var trusted = FactAdvice.Compose(PgTargetFactKeys.AnomalyPlanRegression, new List<Fact> { Anomaly(sigma: 8, peak: 55) }.ToFactLookup())!;
        Assert.StartsWith("The server-wide mean execution time per statement call spiked to 55 ms — ", trusted.Headline, StringComparison.Ordinal);
        Assert.Contains("55 ms this window, 8σ above its", trusted.Investigation, StringComparison.Ordinal);
        Assert.Contains("no per-statement dimension", trusted.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("Anomalous spike", trusted.Headline, StringComparison.Ordinal);

        var first = FactAdvice.Compose(PgTargetFactKeys.AnomalyPlanRegression, new List<Fact> { Anomaly(sigma: 0, peak: 300, lowQuality: true, exceedance: 1.2) }.ToFactLookup())!;
        Assert.Contains("first occurrence, no baseline yet", first.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("σ", first.Investigation, StringComparison.Ordinal);

        var stat = FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyPlanRegression)!;
        Assert.Equal("Statements ran slower per call than this server's normal for this time of week", stat.Headline);
        Assert.Contains("not which one", stat.Investigation, StringComparison.Ordinal);
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.AnomalyPlanRegression), stat);
    }

    /* ───────────────────────── the reads, by text ───────────────────────── */

    [Fact]
    public void TheCollectorsReads_UseStoredDeltas_SplitFirstAgainstLast_GuardTheHashCast_ExcludeOrphans_AndPassTheScorersBars()
    {
        var flip = PgTargetFactCollector.PgTargetPlanFlipSql;
        Assert.Contains("SUM(s.delta_calls)", flip, StringComparison.Ordinal);
        Assert.Contains("SUM(s.delta_total_exec_time_ms)", flip, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(", flip, StringComparison.Ordinal);
        Assert.DoesNotContain("total_exec_time_ms - ", flip, StringComparison.Ordinal);
        /* Before = the opening plan's period (before the SECOND hash appeared); after = the closing plan's (from the LAST). */
        Assert.Contains("AND n.rn = 2", flip, StringComparison.Ordinal);
        Assert.Contains("AND l.rn = f.hash_count", flip, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE s.collection_time <  fl.first_flip_time)", flip, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE s.collection_time >= fl.flip_time)", flip, StringComparison.Ordinal);
        Assert.Contains("f.hash_count >= 2", flip, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN plan_hash ~ '^[0-9A-Fa-f]{12}'", flip, StringComparison.Ordinal);
        Assert.Contains("('x' || left(plan_hash, 12))::bit(48)::bigint", flip, StringComparison.Ordinal);
        Assert.Contains("query_id <> 0", flip, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", flip, StringComparison.Ordinal);

        var sensitivity = PgTargetFactCollector.PgTargetPlanSensitivitySql;
        Assert.Contains("HAVING COUNT(DISTINCT plan_hash) >= $6", sensitivity, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE j.top_value_frequency >= $7)", sensitivity, StringComparison.Ordinal);
        Assert.Contains("make_interval(days => $4)", sensitivity, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (database_name, schema_name, table_name, column_name)", sensitivity, StringComparison.Ordinal);
        Assert.Contains("AND c.column_name   = pr.column_name", sensitivity, StringComparison.Ordinal);
        Assert.DoesNotContain("most_common_vals", sensitivity, StringComparison.Ordinal);

        var trackedness = PgTargetFactCollector.PgTargetPlanTrackednessSql;
        Assert.Contains("facet = 'library_loaded'", trackedness, StringComparison.Ordinal);
        Assert.Contains("extension_name = 'pg_qualstats'", trackedness, StringComparison.Ordinal);
        Assert.Contains("ORDER BY (state IN ('installed', 'outdated')) DESC", trackedness, StringComparison.Ordinal);

        foreach (var sql in new[] { flip, sensitivity, trackedness })
        {
            Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotMatch(new Regex(@"EXTRACT\s*\(\s*\w+\s+FROM\s+\w+"), sql);
            Assert.Contains(sql, PgTargetFactCollector.AllSql);
        }

        /* The scorer's bars reach the read as parameters, never re-declared; the page size and the lookback are the collector's. */
        var collector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Plans.cs"));
        Assert.Contains("cmd.Parameters.AddWithValue((long)PgTargetScorer.ParameterSensitivityMinHashes);", collector, StringComparison.Ordinal);
        Assert.Contains("cmd.Parameters.AddWithValue(PgTargetScorer.ParameterSensitivitySkewFrequency);", collector, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.PlanRegressionMinCallsPerSide", collector, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.PlanRegressionMinDeltaMs", collector, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.PlanRegressionRatioConcerning", collector, StringComparison.Ordinal);
        Assert.Equal(50, PgTargetFactCollector.PlanFlipCandidateCount);
        Assert.Equal(2, PgTargetFactCollector.PlanStateLookbackDays);
        /* Five command sites: lane 27's three and lane 30's two (the captures walk, the one witness read). */
        Assert.Equal(5, Regex.Matches(collector, @"CommandTimeout = FactCommandTimeoutSeconds").Count);
        /* Lane 27's quantities take no rate; the ONLY division of observed time in the partial is lane 30's observed
           hours (captures per hour is a rate over OBSERVED time, #3538 A7), and it is never the nominal window. */
        Assert.Equal(2, Regex.Matches(collector, Regex.Escape("context.ObservedDurationMs / 3_600_000.0")).Count);
        Assert.Equal(2, Regex.Matches(collector, @"ObservedDurationMs /").Count);
        Assert.DoesNotContain("PeriodDurationMs", collector, StringComparison.Ordinal);
        /* Lane 30's region is marked, at the end of the method, after both facts. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Plans.cs");
        Assert.Contains("/* lane 30: Seq-Scan read", source, StringComparison.Ordinal);
        Assert.True(source.IndexOf("/* lane 30: Seq-Scan read", StringComparison.Ordinal) > source.IndexOf("PickSensitivity(sensitivity", StringComparison.Ordinal));
        Assert.Contains("/* filled by lane 27", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBaselineArmAndTheWindowRead_ShareTheServerWideGrainAndTheNoCallsRule_AndTheArmEndsInTheOneScaffold()
    {
        var arm = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgStatementMeanMs)!;
        Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, arm, StringComparison.Ordinal);
        Assert.Contains("clean AS (", arm, StringComparison.Ordinal);
        Assert.Contains("FROM pg_statement_stats", arm, StringComparison.Ordinal);
        Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time < $3", arm, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_total_exec_time_ms)::DOUBLE PRECISION / SUM(delta_calls)", arm, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(delta_calls) > 0", arm, StringComparison.Ordinal);
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgStatementMeanMs));
        /* The arm's own text never keys the bucket — the scaffold does, through the root's clock parameters (Q6). */
        var own = arm[..^PgBaselineProvider.RobustTierScaffold.Length];
        Assert.DoesNotContain("EXTRACT", own, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("now(", own, StringComparison.OrdinalIgnoreCase);

        var window = PgTargetAnomalyDetector.StatementMeanWindowSql;
        Assert.Contains("SUM(delta_total_exec_time_ms)::DOUBLE PRECISION / SUM(delta_calls)", window, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(delta_calls) > 0", window, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2 AND collection_time <= $3", window, StringComparison.Ordinal);
        Assert.Contains("MAX(mean_ms) AS peak_mean_ms", window, StringComparison.Ordinal);
        Assert.Contains("AVG(mean_ms) AS avg_mean_ms", window, StringComparison.Ordinal);

        /* D10: the table joins the retention floor by collector name the day the arm reads it. */
        Assert.Contains("pg_statement_stats", DarlingRetentionHorizons.BaselineServingRawCollectors);

        /* The detector: the pair gate, the heavy-tail cutoff by reference, the fence, no clock. */
        var detector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Plans.cs"));
        Assert.Contains("baseline, peakMeanMs, avgMeanMs,", detector, StringComparison.Ordinal);
        Assert.Contains("DefaultDeviationThreshold, HeavyTailModifiedZThreshold, PgStatementMeanMsFloor, PgStatementMeanMsFallback, SigmaDisplayCap", detector, StringComparison.Ordinal);
        Assert.Contains("catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))", detector, StringComparison.Ordinal);
        Assert.Contains("_baselineProvider.GetBaselineAsync(", detector, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.UtcNow", detector, StringComparison.Ordinal);
        Assert.DoesNotContain("=> Task.CompletedTask;", detector, StringComparison.Ordinal);
        Assert.Contains("metadata[\"server_wide\"] = 1;", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Plans.cs"), StringComparison.Ordinal);
    }

    /* ───────────────────────── the gated e2e ───────────────────────── */

    [Fact]
    public async Task ThirtyOneDaysOfSteadyStatements_ThenAPlanFlipThatTenfoldsTheHotStatement_YieldsTheRegressionStory_TheSensitivity_AndTheAnomalyInOneIncident()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the plan-family e2e.");

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
            var historyStart = windowStart.AddDays(-31);
            var firstFlip = windowStart.AddHours(1);
            var lastFlip = windowStart.AddHours(3).AddMinutes(30);

            /* The coverage witness, the span gate and the baseline gate read pg_database_stats: 25 h of span, one row a minute in the window. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* 31 days + the window of one-minute pg_statement_stats for five statements, set-based. Steady: 400 calls and
               ≈ 4,100 ms a minute (server-wide mean ≈ 10.25 ms, jittered by the third statement's call count so the
               bucket has a spread). In the window the hot statement's per-call mean steps 20 ms → 200 ms from the first
               flip on: 400 calls, 22,100 ms a minute — a server-wide mean of 55 ms. */
            await PlantStatementHistoryAsync(connection, historyStart, windowEnd, firstFlip, ct);

            /* The captures: hash A from the window's start, B from the first flip, C (a brief re-plan at 3 h 30) to the end
               — three hashes, so the sensitivity's shape is this statement's too. Before = A's hour; after = C's half hour. */
            for (var minute = 10; minute < 60; minute += 10)
                await PlantCaptureAsync(connection, windowStart.AddMinutes(minute), HotQueryId, HashA, 18.5, 7, "Index Scan", ct);
            for (var minute = 0; minute < 150; minute += 15)
                await PlantCaptureAsync(connection, firstFlip.AddMinutes(minute), HotQueryId, HashB, 195.0, 9, "Hash Join", ct);
            for (var minute = 0; minute < 30; minute += 10)
                await PlantCaptureAsync(connection, lastFlip.AddMinutes(minute), HotQueryId, HashC, 205.0, 11, "Hash Join", ct);
            /* An orphan (query_id 0, no %Q in the prefix) and a one-plan statement: neither is a flip. */
            await PlantCaptureAsync(connection, windowStart.AddMinutes(30), 0, HashB, 50, 3, "Seq Scan", ct);
            await PlantCaptureAsync(connection, windowStart.AddMinutes(45), HotQueryId + 3, HashA, 5, 2, "Index Only Scan", ct);

            /* The mechanism: the hot statement filters orders.customer_id, whose top value covers 62 % of the table. */
            await PlantPredicateAsync(connection, windowStart.AddHours(-1), HotQueryId, "appdb", "public", "orders", "customer_id", 57.9, ct);
            await PlantPredicateAsync(connection, windowStart.AddHours(-1), HotQueryId, "appdb", "public", "orders", "created_at", 1.04, ct);
            await PlantColumnStatsAsync(connection, windowStart.AddHours(-1), "appdb", "public", "orders", "customer_id", 0.62, 1_200, ct);
            await PlantColumnStatsAsync(connection, windowStart.AddHours(-1), "appdb", "public", "orders", "created_at", 0.001, -1, ct);

            /* Both instruments are on. */
            await PlantReadinessAsync(connection, windowStart.AddMinutes(-30), "library_loaded", true, ct);
            await PlantExtensionAsync(connection, windowStart.AddHours(-1), "appdb", "pg_qualstats", "installed", ct);

            /* ── the collector alone, on the exact planted window. */
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

            var family = facts.Where(f => f.Source == PgTargetSources.PlansSource).ToList();
            Assert.Equal(new[] { PgTargetFactKeys.PlanRegression, PgTargetFactKeys.ParameterSensitivity }, family.Select(f => f.Key).ToArray());

            var regression = family[0];
            Assert.Equal(HotQueryId.ToString(CultureInfo.InvariantCulture), regression.ObjectName);
            Assert.Null(regression.DatabaseName);
            Assert.Equal(10.0, regression.Value, precision: 9);
            Assert.Equal(20.0, regression.Metadata[PgTargetScorer.PlanMeanMsBeforeKey], precision: 9);
            Assert.Equal(200.0, regression.Metadata[PgTargetScorer.PlanMeanMsAfterKey], precision: 9);
            /* Before: the 60 minutes of hash A (rows stamped :01 … :60 before the first flip at +1 h → 60 × 100). After:
               the 30 minutes from hash C's first capture to the window's end, inclusive. */
            Assert.Equal(6_000, regression.Metadata[PgTargetScorer.PlanCallsBeforeKey]);
            Assert.Equal(3_100, regression.Metadata[PgTargetScorer.PlanCallsAfterKey]);
            Assert.Equal(3, regression.Metadata[PgTargetScorer.PlanHashCountKey]);
            Assert.Equal(Convert.ToInt64(HashA[..12], 16), regression.Metadata["plan_hash_before_prefix"]);
            Assert.Equal(Convert.ToInt64(HashC[..12], 16), regression.Metadata["plan_hash_after_prefix"]);
            Assert.Equal(1, regression.Metadata["top_node_changed"]);
            Assert.Equal(7, regression.Metadata["nodes_before"]);
            Assert.Equal(11, regression.Metadata["nodes_after"]);
            Assert.Equal(4, regression.Metadata["node_count_delta"]);
            Assert.Equal(30 * 60, regression.Metadata["flip_age_s"]);
            Assert.Equal(3 * 3_600, regression.Metadata["first_flip_age_s"]);
            Assert.Equal(1, regression.Metadata["flipped_statements"]);
            Assert.False(regression.Metadata.ContainsKey(PgTargetScorer.PlanUnavailableKey));

            var sensitivity = family[1];
            Assert.Equal(HotQueryId.ToString(CultureInfo.InvariantCulture), sensitivity.ObjectName);
            Assert.Equal("appdb", sensitivity.DatabaseName);
            Assert.Equal(0.62, sensitivity.Value, precision: 9);
            Assert.Equal(3, sensitivity.Metadata[PgTargetScorer.PlanHashCountKey]);
            Assert.Equal(2, sensitivity.Metadata["predicate_columns"]);
            Assert.Equal(1, sensitivity.Metadata["skewed_columns"]);
            Assert.Equal(1_200, sensitivity.Metadata["n_distinct"]);
            Assert.Equal(57.9, sensitivity.Metadata["worst_estimate_error_ratio"], precision: 9);
            Assert.Equal(1, sensitivity.Metadata["sensitive_statements"]);

            /* Lane 7's bad actor for the hot statement is in the same list, ahead of the family (emission order). */
            var badActor = Assert.Single(facts, f => f.Key == PgTargetFactKeys.BadActorKey(HotQueryId));
            Assert.True(facts.IndexOf(badActor) < facts.IndexOf(regression));

            /* ── the baseline and the detector alone. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(ServerId, MetricNames.PgStatementMeanMs, windowStart, ct);
            Assert.True(bucket.SampleCount > 0);
            Assert.True(bucket.IsTrustworthy, $"the 30-day statement-mean bucket is not trustworthy (samples {bucket.SampleCount}, days {bucket.DistinctDays})");
            Assert.InRange(bucket.Median, 9.5, 11.0);

            var anomalies = await new PgTargetAnomalyDetector(postgres, baselines).DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyPlanRegression);
            /* (20,000 + 500 + 2c + 1,000 + 200) / (200 + c) for the jittered c ∈ [190, 210]: 53.9 … 56.6 ms; the peak is the
               c = 190 minute. The window's mean: an hour at ≈ 10 ms and three at ≈ 55 ms. */
            Assert.InRange(anomaly.Value, 56.5, 56.7);
            Assert.Equal(anomaly.Value, anomaly.Metadata["peak_mean_ms"]);
            Assert.InRange(anomaly.Metadata["avg_mean_ms"], 40, 50);
            Assert.Equal(1, anomaly.Metadata["server_wide"]);
            Assert.Equal(0, anomaly.Metadata["baseline_low_quality"]);
            Assert.Equal(0, anomaly.Metadata["threshold_lineage"]);
            Assert.True(anomaly.Metadata["deviation_sigma"] >= AnomalyThresholds.HeavyTailModifiedZThreshold);
            Assert.True(anomaly.Metadata["mean_sigma"] >= AnomalyThresholds.HeavyTailModifiedZThreshold);

            /* ── THE EXIT CRITERION: the real analyze_server tool, anchored at the planted window's end. */
            var service = new DarlingAnalysisService(postgres);
            var asOf = windowEnd.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                string RootKey(JsonElement f) => f.GetProperty("root_fact").GetProperty("key").GetString()!;

                /* The regression roots CRITICAL (10×), lifted by the same-id bad actor and the anomaly (1.6), and walks to
                   the bad actor — the pass's top bad actor IS the regressed statement. */
                var card = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.PlanRegression);
                Assert.Equal(1.6, card.GetProperty("severity").GetDouble(), precision: 9);
                Assert.Equal(PgTargetSources.PlansSource, card.GetProperty("category").GetString());
                Assert.Equal(PgTargetFactKeys.BadActorKey(HotQueryId), card.GetProperty("leaf_fact").GetProperty("key").GetString());
                Assert.Equal(2, card.GetProperty("fact_count").GetInt32());
                var advice = card.GetProperty("advice");
                Assert.Equal($"Statement queryid {HotQueryId} got 10× slower per call when its plan changed (20 ms → 200 ms)", advice.GetProperty("headline").GetString());
                var investigation = advice.GetProperty("investigation").GetString()!;
                Assert.Contains("over 6,000 calls before the flip and 3,100 after", investigation, StringComparison.Ordinal);
                Assert.Contains($"(plan hash {HashA[..12]}… before the flip, {HashC[..12]}… after)", investigation, StringComparison.Ordinal);
                Assert.Contains("PG_BAD_ACTOR fired for the same queryid", investigation, StringComparison.Ordinal);
                Assert.Contains("ANOMALY_PG_PLAN_REGRESSION co-fired: the server-wide per-call mean reached 56.6 ms", investigation, StringComparison.Ordinal);
                Assert.Contains("PG_PARAMETER_SENSITIVITY names the likely mechanism", investigation, StringComparison.Ordinal);
                Assert.DoesNotContain("CREATE INDEX", investigation + advice.GetProperty("remediation").GetString(), StringComparison.OrdinalIgnoreCase);
                var tools = card.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains("get_pg_plans", tools);
                Assert.Contains("get_pg_top_queries", tools);
                /* The bad actor is consumed into the regression's story: no card of its own. */
                Assert.DoesNotContain(findings, f => RootKey(f) == PgTargetFactKeys.BadActorKey(HotQueryId));

                /* The sensitivity: 0.4 × 1.5 = 0.6, chained on the same statement, its own card. */
                var sensitivityCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.ParameterSensitivity);
                Assert.Equal(0.6, sensitivityCard.GetProperty("severity").GetDouble(), precision: 9);
                Assert.Equal($"Statement queryid {HotQueryId} was planned 3 different ways in the window, and a predicate column has one value covering 62% of its table", sensitivityCard.GetProperty("advice").GetProperty("headline").GetString());
                Assert.Contains("PG_PLAN_REGRESSION fired on this statement in the same window (20 ms → 200 ms per call)", sensitivityCard.GetProperty("advice").GetProperty("investigation").GetString(), StringComparison.Ordinal);

                /* The anomaly shares the regression's incident (the fold), and its prose is the family's, never the SQL Server composer's. */
                var anomalyCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalyPlanRegression);
                Assert.Equal(card.GetProperty("incident_id").GetString(), anomalyCard.GetProperty("incident_id").GetString());
                var anomalyHeadline = anomalyCard.GetProperty("advice").GetProperty("headline").GetString()!;
                Assert.StartsWith("The server-wide mean execution time per statement call spiked to 56.6 ms — ", anomalyHeadline, StringComparison.Ordinal);
                Assert.DoesNotContain("Anomalous spike", anomalyHeadline, StringComparison.Ordinal);

                Assert.All(findings.SelectMany(f => f.GetProperty("next_tools").EnumerateArray()).Select(t => t.GetProperty("tool").GetString()!), t => Assert.StartsWith("get_pg_", t, StringComparison.Ordinal));
            }

            /* get_analysis_facts under the family's source: two facts, both stamped unmeasured. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.PlansSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var shown = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.Equal(2, shown.Count);
                Assert.All(shown, f => Assert.Equal(0, f.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble()));
                var regressionShown = Assert.Single(shown, f => f.GetProperty("key").GetString() == PgTargetFactKeys.PlanRegression);
                Assert.Equal(3, regressionShown.GetProperty("metadata").GetProperty(PgTargetScorer.PlanHashCountKey).GetDouble());
            }

            /* ── the unavailable shape, on the same store: a second server whose readiness says the library is off and
               whose window captured nothing. One regression fact, unavailable, the reason on it; no sensitivity fact. */
            var offId = ServerIdHelper.GetDeterministicHashCode(ServerName + "-off");
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, offId, ServerName + "-off", "postgres", 18, ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, offId, ServerName + "-off", windowStart.AddMinutes(minute - 1), ct);
            await PlantReadinessAsync(connection, windowStart.AddMinutes(-30), "library_loaded", false, ct, offId, ServerName + "-off");
            var offContext = new AnalysisContext { ServerId = offId, ServerName = ServerName + "-off", TimeRangeStart = windowStart, TimeRangeEnd = windowEnd, ServerUtcOffset = TimeSpan.Zero };
            var offFacts = await collector.CollectFactsAsync(offContext);
            /* Two keys, one missing instrument: the regression's unavailable fact and (lane 30) the Seq-Scan advisory's,
               each with the same reason — two questions, two occurrence histories. */
            var offFamily = offFacts.Where(f => f.Source == PgTargetSources.PlansSource).ToList();
            Assert.Equal(new[] { PgTargetFactKeys.PlanRegression, PgTargetFactKeys.SeqScanAdvisory }, offFamily.Select(f => f.Key).ToArray());
            foreach (var off in offFamily)
            {
                Assert.Equal(1, off.Metadata[PgTargetScorer.PlanUnavailableKey]);
                Assert.Equal(1, off.Metadata[PgTargetScorer.PlanReasonAutoExplainOffKey]);
                Assert.Equal(0, PgTargetScorer.ScoreBase(off));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────────── builders ───────────────────────── */

    private static Fact Regression(double before, double after, double callsBefore = 6_000, double callsAfter = 3_000, double hashes = 2, long queryId = HotQueryId) => new()
    {
        Source = PgTargetSources.PlansSource,
        Key = PgTargetFactKeys.PlanRegression,
        Value = before > 0 ? after / before : 0,
        ServerId = 1,
        ObjectName = queryId.ToString(CultureInfo.InvariantCulture),
        Metadata =
        {
            [PgTargetScorer.PlanMeanMsBeforeKey] = before,
            [PgTargetScorer.PlanMeanMsAfterKey] = after,
            ["mean_ms_delta"] = after - before,
            [PgTargetScorer.PlanCallsBeforeKey] = callsBefore,
            [PgTargetScorer.PlanCallsAfterKey] = callsAfter,
            [PgTargetScorer.PlanHashCountKey] = hashes,
            ["top_node_changed"] = 0,
            ["flipped_statements"] = 1,
        },
    };

    private static Fact UnavailableRegression() => new()
    {
        Source = PgTargetSources.PlansSource,
        Key = PgTargetFactKeys.PlanRegression,
        Value = 0,
        ServerId = 1,
        Metadata = { [PgTargetScorer.PlanUnavailableKey] = 1, [PgTargetScorer.PlanReasonAutoExplainOffKey] = 1, ["threshold_lineage"] = 1 },
    };

    private static Fact Sensitivity(double hashes, double frequency, long queryId = HotQueryId) => new()
    {
        Source = PgTargetSources.PlansSource,
        Key = PgTargetFactKeys.ParameterSensitivity,
        Value = frequency,
        ServerId = 1,
        ObjectName = queryId.ToString(CultureInfo.InvariantCulture),
        Metadata =
        {
            [PgTargetScorer.PlanHashCountKey] = hashes,
            [PgTargetScorer.PlanTopValueFrequencyKey] = frequency,
            ["predicate_columns"] = 1,
            ["skewed_columns"] = 1,
            ["sensitive_statements"] = 1,
        },
    };

    private static Fact UnavailableSensitivity(bool qualstatsAbsent) => new()
    {
        Source = PgTargetSources.PlansSource,
        Key = PgTargetFactKeys.ParameterSensitivity,
        Value = 0,
        ServerId = 1,
        ObjectName = HotQueryId.ToString(CultureInfo.InvariantCulture),
        Metadata =
        {
            [PgTargetScorer.PlanHashCountKey] = 4,
            [PgTargetScorer.PlanUnavailableKey] = 1,
            [qualstatsAbsent ? PgTargetScorer.PlanReasonQualstatsAbsentKey : PgTargetScorer.PlanReasonNoPredicateSampledKey] = 1,
            ["threshold_lineage"] = 1,
        },
    };

    /// <summary>Lane 7's bad actor, the shape its own tests build — over the measured busy floor unless told otherwise.</summary>
    private static Fact BadActor(long queryId, double share, double busy = 0.3) => new()
    {
        Source = PgTargetSources.QueriesSource,
        Key = PgTargetFactKeys.BadActorKey(queryId),
        Value = share,
        ServerId = 1,
        Metadata =
        {
            ["share_of_window_time"] = share,
            ["window_total_exec_ms"] = 4_000_000,
            ["window_busy_fraction"] = busy,
            ["calls"] = 9_000,
            ["total_exec_ms"] = share * 4_000_000,
            ["temp_blks_written"] = 0,
            ["database_count"] = 1,
        },
    };

    private static Fact Anomaly(double sigma, double peak = 55, bool lowQuality = false, double exceedance = 0) => new()
    {
        Source = "anomaly",
        Key = PgTargetFactKeys.AnomalyPlanRegression,
        Value = peak,
        ServerId = 1,
        Metadata =
        {
            ["baseline_mean"] = 10.25,
            ["baseline_stddev"] = 0.12,
            ["baseline_median"] = 10.2,
            ["baseline_mad"] = 0.08,
            ["deviation_sigma"] = sigma,
            ["fire_threshold"] = AnomalyThresholds.HeavyTailModifiedZThreshold,
            ["baseline_low_quality"] = lowQuality ? 1 : 0,
            ["fallback_exceedance"] = exceedance,
            ["baseline_samples"] = 44_000,
            ["window_samples"] = 240,
            ["peak_mean_ms"] = peak,
            ["avg_mean_ms"] = peak * 0.8,
            ["mean_sigma"] = sigma,
            ["server_wide"] = 1,
            ["confidence"] = 1.0,
            ["threshold_lineage"] = 0,
        },
    };

    private static PgTargetFactCollector.PlanFlipRow Flip(long queryId, long callsBefore, long msBefore, long callsAfter, long msAfter) =>
        new(queryId, 2, null, null, DateTime.UnixEpoch, DateTime.UnixEpoch, false, null, null, callsBefore, msBefore, callsAfter, msAfter, 1);

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /* ───────────────────────── planting ───────────────────────── */

    /// <summary>Five statements, one row a minute each from <paramref name="from"/> (exclusive) to <paramref name="to"/>
    /// (inclusive), set-based. The hot statement (100 calls, 20 ms) steps to 200 ms per call from <paramref name="flipAt"/>
    /// on; the third statement's call count is jittered by the minute so the baseline bucket has a spread.</summary>
    private static async Task PlantStatementHistoryAsync(NpgsqlConnection connection, DateTime from, DateTime to, DateTime flipAt, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read,
     temp_blks_read, temp_blks_written, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds)
SELECT $6 + (ROW_NUMBER() OVER ()) AS collection_id,
       m.minute,
       $1, $2,
       s.queryid, 16384, 10, TRUE,
       0, 0, 900.5, 100, 10, 5, 0, 0, 0,
       s.calls,
       CASE WHEN s.queryid = $5 AND m.minute >= $7 THEN s.calls * 200 ELSE s.calls * s.mean_ms END,
       s.calls,
       60
FROM generate_series($3::timestamp + INTERVAL '1 minute', $4::timestamp, INTERVAL '1 minute') AS m(minute)
CROSS JOIN LATERAL (VALUES
    ($5,                    100::bigint,                                                    20::bigint),
    ($5 + 1,                50::bigint,                                                     10::bigint),
    ($5 + 2,                (190 + (EXTRACT(MINUTE FROM m.minute)::int % 21))::bigint,      2::bigint),
    ($5 + 3,                10::bigint,                                                     100::bigint),
    ($5 + 4,                40::bigint,                                                     5::bigint)
) AS s(queryid, calls, mean_ms)", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(from);
        command.Parameters.AddWithValue(to);
        command.Parameters.AddWithValue(HotQueryId);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(flipAt);
        command.CommandTimeout = 300;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantCaptureAsync(NpgsqlConnection connection, DateTime at, long queryId, string planHash, double durationMs, int nodeCount, string topNode, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_plan_capture
    (collection_id, collection_time, server_id, server_name, query_id, plan_hash, duration_ms, node_count, top_node_type, plan_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '{""Plan"": {""Node Type"": ""redacted""}}')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planHash);
        command.Parameters.AddWithValue(durationMs);
        command.Parameters.AddWithValue(nodeCount);
        command.Parameters.AddWithValue(topNode);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantPredicateAsync(NpgsqlConnection connection, DateTime at, long queryId, string database, string schema, string table, string column, double errorRatio, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_predicate_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name, operator, query_id,
     sample_count, rows_evaluated, rows_filtered, worst_estimate_error_ratio, sample_rate)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '=', $9, 120, 480000, 300000, $10, 0.01)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(column);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(errorRatio);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantColumnStatsAsync(NpgsqlConnection connection, DateTime at, string database, string schema, string table, string column, double topValueFrequency, double nDistinct, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_column_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name,
     n_distinct, null_frac, avg_width, correlation, top_value_frequency, common_value_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0.0, 8, 0.1, $10, 10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(column);
        command.Parameters.AddWithValue(nDistinct);
        command.Parameters.AddWithValue(topValueFrequency);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantReadinessAsync(NpgsqlConnection connection, DateTime at, string facet, bool satisfied, CancellationToken ct, int? serverId = null, string? serverName = null)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_plan_capture_readiness
    (collection_id, collection_time, server_id, server_name, facet, is_satisfied, observed, detail)
VALUES ($1, $2, $3, $4, $5, $6, 'planted', 'planted')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId ?? ServerId);
        command.Parameters.AddWithValue(serverName ?? ServerName);
        command.Parameters.AddWithValue(facet);
        command.Parameters.AddWithValue(satisfied);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantExtensionAsync(NpgsqlConnection connection, DateTime at, string database, string extension, string state, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_extension_availability
    (collection_id, collection_time, server_id, server_name, database_name, extension_name, state, installed_version, default_version, is_monitoring_relevant, comment)
VALUES ($1, $2, $3, $4, $5, $6, $7, '2.1.1', '2.1.1', TRUE, 'planted')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(extension);
        command.Parameters.AddWithValue(state);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var offId = ServerIdHelper.GetDeterministicHashCode(ServerName + "-off");
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_statement_stats WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM pg_plan_capture WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM pg_predicate_stats WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM pg_column_stats WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM pg_plan_capture_readiness WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM pg_extension_availability WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM pg_database_stats WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM servers WHERE server_id IN ({ServerId}, {offId});", connection);
        cleanup.CommandTimeout = 300;
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
