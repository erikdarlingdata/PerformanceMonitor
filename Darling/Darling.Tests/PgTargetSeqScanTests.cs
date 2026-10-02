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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>PG_SEQ_SCAN_ADVISORY</c> (#3691 lane 30, design v3 §6): a large relation read sequentially, often, by a statement
/// whose predicate keeps few of the rows it reads — <c>plan_json</c> Seq Scan nodes beside <c>pg_predicate_stats</c>'
/// selectivity and <c>pg_table_bloat_stats</c>' size. Evidence first; an index named only where <c>pg_qualstats</c>
/// named the columns and every gate holds (the maintainer withdrew the "never DDL" reading of D8 on 2026-09-20).
///
/// <para><b>Ungated:</b> the four bars unmeasured by value and stamp; the JSON walker on arranged plans (nested
/// <c>Plans</c>, an array root, a node without a relation name or without a Filter, a non-Seq-Scan node, malformed
/// JSON — skipped, never guessed); the aggregation (one capture counts once per relation, analyzed captures summed
/// apart from unanalyzed ones); the ranking (rate × plan share, the qualstats selectivity standing in, ties); the
/// fact's shape (the relation on <c>ObjectName</c> with the columns when known, the id halves rejoining exactly for a
/// negative id, absent-never-zero, the qualstats-absent <c>unavailable</c> shape still carrying the plan-side
/// evidence); the three gates one at a time and a lower pair clearing; the one amplifier through the real
/// <c>ScoreAll</c> (bad actor or regression on the TOP pair's statement — 0.65, once, never for a lower pair's or a
/// different statement); the two edges and when each opens; the advice by clause for every branch — the index text
/// appears in exactly one branch, with its cost beside it, and nowhere else in the family's source; the two reads by
/// text and the census memberships; the tool row; and the maintainer's prose discipline (2026-09-20) — wherever a block
/// names an index it also says a new index can cause regressions elsewhere and must be tested, calls its impact an
/// ESTIMATE and its counts SAMPLED, and puts the statement LAST, after the evidence and the questions.</para>
///
/// <para><b>Gated e2e</b> (<c>DARLING_TEST_PG</c>): twenty captured plans over four hours (five per hour) with a Seq
/// Scan over <c>big</c> removing 99 % of its rows, a <c>pg_table_bloat_stats</c> heap of 1 GiB for <c>public.big</c>, a
/// selective <c>pg_qualstats</c> row on <c>customer_id</c>, readiness and extension rows saying both instruments are on.
/// The REAL <c>analyze_server</c> returns the advisory at exactly 0.5 naming <c>public.big</c>, its card carrying the
/// candidate index WITH its cost sentence — and no other card in the payload carries index text. Then a second server
/// with the library off yields the <c>unavailable</c> shape.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetSeqScanTests
{
    private const string ServerName = "darling-pg-target-seqscan-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const long ScanQueryId = -8_100_200_300_400L;     /* negative on purpose: the id halves must rejoin a sign bit */
    private const long OtherQueryId = 6_001_001_001L;
    private const long GiB = 1024L * 1024 * 1024;

    /* ───────────────────────── lineage ───────────────────────── */

    [Fact]
    public void EveryBar_IsUnmeasured_InTheAdvisoryBand_AndNotTheBloatFloorByValue()
    {
        Assert.Equal(0.9, PgTargetScorer.SeqScanSelectiveFraction);
        Assert.Equal(256L * 1024 * 1024, PgTargetScorer.SeqScanLargeRelationBytes);
        Assert.Equal(3.0, PgTargetScorer.SeqScanMinCapturesPerHour);
        Assert.Equal(0.5, PgTargetScorer.SeqScanAdvisoryBase);
        Assert.Equal(0.3, PgTargetScorer.SeqScanCoFireBoost);
        Assert.Equal(3, PgTargetScorer.SeqScanCarriedPairs);
        /* D5: the base sits AT the story threshold, one co-fire lifts it inside the band, never to critical. */
        Assert.Equal(0.65, PgTargetScorer.SeqScanAdvisoryBase * (1 + PgTargetScorer.SeqScanCoFireBoost), precision: 9);
        Assert.True(PgTargetScorer.SeqScanAdvisoryBase * (1 + PgTargetScorer.SeqScanCoFireBoost) < 1.0);
        /* The size gate is not the calibration's bloat floor reused: that floor is where the bloat ESTIMATE becomes
           trustworthy, a different question, and the scorer says so above the bar. */
        Assert.NotEqual(PgTargetScorer.BloatSizeFloorBytes, PgTargetScorer.SeqScanLargeRelationBytes);

        var scorer = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Plans.cs");
        foreach (var bar in new[] { "SeqScanSelectiveFraction", "SeqScanLargeRelationBytes", "SeqScanMinCapturesPerHour", "SeqScanAdvisoryBase" })
        {
            var declaration = scorer.IndexOf($"public const {(bar == "SeqScanLargeRelationBytes" ? "long" : "double")} {bar} =", StringComparison.Ordinal);
            Assert.True(declaration > 0, bar);
            var above = scorer[Math.Max(0, declaration - 1_400)..declaration];
            Assert.Contains("unmeasured: chosen, not measured", above, StringComparison.Ordinal);
            Assert.Contains("threshold_lineage = 0", above, StringComparison.Ordinal);
        }
    }

    /* ───────────────────────── the walker ───────────────────────── */

    [Fact]
    public void TheWalker_FindsSeqScansWithAFilterAtAnyDepth_AndSkipsWhatItCannotRead()
    {
        const string plan = """
            {"Plan": {"Node Type": "Hash Join", "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "orders", "Filter": "(status = '?')", "Plan Rows": 1200, "Actual Rows": 1000, "Rows Removed by Filter": 99000},
                {"Node Type": "Hash", "Plans": [
                    {"Node Type": "Seq Scan", "Relation Name": "customers", "Plan Rows": 5000},
                    {"Node Type": "Index Scan", "Relation Name": "orders", "Filter": "(x = ?)", "Index Name": "orders_pkey"},
                    {"Node Type": "Seq Scan", "Filter": "(y = ?)", "Plan Rows": 5},
                    {"Node Type": "Seq Scan", "Relation Name": "events", "Filter": "(kind = '?')", "Plan Rows": "not a number"}
                ]}
            ]}}
            """;
        var nodes = PgTargetFactCollector.WalkSeqScans(plan);
        Assert.Equal(2, nodes.Count);
        var orders = nodes[0];
        Assert.Equal("orders", orders.RelationName);
        Assert.Equal(1200, orders.PlanRows);
        Assert.Equal(1000, orders.ActualRows);
        Assert.Equal(99000, orders.RowsRemovedByFilter);
        /* customers: a Seq Scan with no Filter — reading the whole relation IS the intent; skipped. The unnamed scan and
           the Index Scan are skipped. events: the node counts, its unreadable Plan Rows does not. */
        var events = nodes[1];
        Assert.Equal("events", events.RelationName);
        Assert.Null(events.PlanRows);
        Assert.Null(events.ActualRows);
        Assert.Null(events.RowsRemovedByFilter);

        /* An EXPLAIN (FORMAT JSON) array root, and a bare node root, both walk. */
        Assert.Single(PgTargetFactCollector.WalkSeqScans("""[{"Plan": {"Node Type": "Seq Scan", "Relation Name": "t", "Filter": "(a = ?)"}}]"""));
        Assert.Single(PgTargetFactCollector.WalkSeqScans("""{"Node Type": "Seq Scan", "Relation Name": "t", "Filter": "(a = ?)"}"""));
        /* Nothing to read: lane 27's planted placeholder, malformed text, a non-object root, an empty relation name. */
        Assert.Empty(PgTargetFactCollector.WalkSeqScans("""{"Plan": {"Node Type": "redacted"}}"""));
        Assert.Empty(PgTargetFactCollector.WalkSeqScans("{\"Plan\": {\"Node Type\": \"Seq Scan\", \"Relation Name\": \"t\", \"Filter\": "));
        Assert.Empty(PgTargetFactCollector.WalkSeqScans("42"));
        Assert.Empty(PgTargetFactCollector.WalkSeqScans("""{"Plan": {"Node Type": "Seq Scan", "Relation Name": "", "Filter": "(a = ?)"}}"""));
    }

    [Fact]
    public void TheAggregation_CountsACaptureOncePerRelation_AndSumsRowsOnlyOverAnalyzedCaptures()
    {
        const string analyzed = """
            {"Plan": {"Node Type": "Append", "Plans": [
                {"Node Type": "Seq Scan", "Relation Name": "big", "Filter": "(a = ?)", "Plan Rows": 100, "Actual Rows": 10, "Rows Removed by Filter": 990},
                {"Node Type": "Seq Scan", "Relation Name": "big", "Filter": "(b = ?)", "Plan Rows": 300, "Actual Rows": 0, "Rows Removed by Filter": 1000}]}}
            """;
        const string estimateOnly = """{"Plan": {"Node Type": "Seq Scan", "Relation Name": "big", "Filter": "(a = ?)", "Plan Rows": 500}}""";
        const string other = """{"Plan": {"Node Type": "Seq Scan", "Relation Name": "small", "Filter": "(a = ?)", "Actual Rows": 50, "Rows Removed by Filter": 50}}""";

        var scans = PgTargetFactCollector.AggregateSeqScans(
        [
            new(ScanQueryId, analyzed),
            new(ScanQueryId, estimateOnly),
            new(ScanQueryId, other),
            new(OtherQueryId, other),
        ]);

        Assert.Equal(3, scans.Count);
        var big = Assert.Single(scans, s => s.QueryId == ScanQueryId && s.RelationName == "big");
        Assert.Equal(2, big.Captures);                 /* two nodes in one plan = one capture of the pair */
        Assert.Equal(1, big.AnalyzedCaptures);
        Assert.Equal(500, big.PlanRows);               /* the largest estimate seen */
        Assert.Equal(10, big.KeptRows);
        Assert.Equal(1990, big.RemovedRows);
        Assert.Equal(1990.0 / 2000, big.RowsRemovedShare!.Value, precision: 12);
        var small = Assert.Single(scans, s => s.QueryId == ScanQueryId && s.RelationName == "small");
        Assert.Equal(0.5, small.RowsRemovedShare);
        Assert.Null(Assert.Single(PgTargetFactCollector.AggregateSeqScans([new(ScanQueryId, estimateOnly)])).RowsRemovedShare);
    }

    [Fact]
    public void ThePick_RanksByRateTimesShare_LetsSelectivityStandInForAMissingShare_AndIsStable()
    {
        var scans = new List<PgTargetFactCollector.SeqScanAggregate>
        {
            new(ScanQueryId, "big", Captures: 20, PlanRows: 100, KeptRows: 1_000, RemovedRows: 99_000, AnalyzedCaptures: 20),    /* 5/h × 0.99 = 4.95 */
            new(OtherQueryId, "mid", Captures: 40, PlanRows: null, KeptRows: 0, RemovedRows: 0, AnalyzedCaptures: 0),           /* 10/h × selectivity 0.95 = 9.5 */
            new(OtherQueryId + 1, "blind", Captures: 8, PlanRows: null, KeptRows: 0, RemovedRows: 0, AnalyzedCaptures: 0),      /* 2/h × nothing = 0 */
            new(OtherQueryId + 2, "tie", Captures: 20, PlanRows: null, KeptRows: 1_000, RemovedRows: 99_000, AnalyzedCaptures: 20), /* 4.95, same as big: more captures? equal; smaller id? big is negative */
        };
        var witnesses = new List<PgTargetFactCollector.SeqScanWitnessRow>
        {
            Witness(OtherQueryId, "mid", selectivity: 0.95, heap: GiB),
        };

        var ranked = PgTargetFactCollector.PickSeqScans(scans, witnesses, observedHours: 4);
        Assert.Equal(new[] { "mid", "big", "tie", "blind" }, ranked.Select(p => p.Scan.RelationName).ToArray());
        Assert.Equal(10.0, ranked[0].CapturesPerHour);
        Assert.Equal(9.5, ranked[0].RankScore, precision: 9);
        Assert.Same(witnesses[0], ranked[0].Witness);
        Assert.Null(ranked[1].Witness);
        Assert.Equal(0.0, ranked[3].RankScore);
        /* No observed time, no rate — never a division by zero. */
        Assert.All(PgTargetFactCollector.PickSeqScans(scans, witnesses, observedHours: 0), p => Assert.Equal(0.0, p.CapturesPerHour));
    }

    /* ───────────────────────── the fact ───────────────────────── */

    [Fact]
    public void TheFact_CarriesTheRelationAndColumnsOnObjectName_TheIdAsTwoExactHalves_AndNothingAsZero()
    {
        var ranked = PgTargetFactCollector.PickSeqScans(
        [
            new(ScanQueryId, "big", Captures: 20, PlanRows: 100, KeptRows: 1_000, RemovedRows: 99_000, AnalyzedCaptures: 20),
            new(OtherQueryId, "mid", Captures: 4, PlanRows: null, KeptRows: 0, RemovedRows: 0, AnalyzedCaptures: 0),
            new(OtherQueryId + 1, "bare", Captures: 4, PlanRows: null, KeptRows: 0, RemovedRows: 0, AnalyzedCaptures: 0),
            new(OtherQueryId + 2, "fourth", Captures: 1, PlanRows: null, KeptRows: 0, RemovedRows: 0, AnalyzedCaptures: 0),
        ],
        [
            Witness(ScanQueryId, "big", selectivity: 0.99, heap: GiB, columns: "customer_id, status", schema: "public", database: "appdb", rowsEvaluated: 1_000_000, rowsFiltered: 990_000, sampleRate: 0.01, error: 12.5),
            Witness(OtherQueryId, "mid", selectivity: null, heap: 300L * 1024 * 1024, relationsNamed: 2),
        ], observedHours: 4);

        var fact = PgTargetFactCollector.SeqScanFact(Context(), ranked, qualstatsAbsent: false);
        Assert.Equal(PgTargetSources.PlansSource, fact.Source);
        Assert.Equal(PgTargetFactKeys.SeqScanAdvisory, fact.Key);
        Assert.Equal("public.big (customer_id, status)", fact.ObjectName);
        Assert.Equal("appdb", fact.DatabaseName);
        Assert.Equal(5 * 0.99, fact.Value, precision: 9);
        Assert.Equal(3, fact.Metadata[PgTargetScorer.SeqScanPairsKey]);
        Assert.Equal(4, fact.Metadata[PgTargetScorer.SeqScanCandidatePairsKey]);
        Assert.Equal(4, fact.Metadata["observed_hours"]);

        /* Pair 1: everything present; the negative id rejoins exactly from its halves. */
        Assert.Equal(ScanQueryId, PgTargetScorer.SeqScanQueryId(fact, 1));
        Assert.Equal(20, fact.Metadata["captures_1"]);
        Assert.Equal(5, fact.Metadata["captures_per_hour_1"]);
        Assert.Equal(0.99, fact.Metadata["rows_removed_share_1"], precision: 12);
        Assert.Equal(1_000, fact.Metadata["actual_rows_1"]);
        Assert.Equal(99_000, fact.Metadata["rows_removed_by_filter_1"]);
        Assert.Equal(100, fact.Metadata["plan_rows_1"]);
        Assert.Equal(GiB, fact.Metadata["heap_bytes_1"]);
        Assert.Equal(1, fact.Metadata["relations_named_1"]);
        Assert.Equal(0.99, fact.Metadata["selectivity_1"]);
        Assert.Equal(1_000_000, fact.Metadata["rows_evaluated_1"]);
        Assert.Equal(990_000, fact.Metadata["rows_filtered_1"]);
        Assert.Equal(0.01, fact.Metadata["sample_rate_1"]);
        Assert.Equal(12.5, fact.Metadata["estimate_error_ratio_1"]);
        /* Pair 2: a size but no predicate, no analyzed capture — the absent keys are ABSENT, not 0. */
        Assert.Equal(OtherQueryId, PgTargetScorer.SeqScanQueryId(fact, 2));
        Assert.Equal(300L * 1024 * 1024, fact.Metadata["heap_bytes_2"]);
        Assert.Equal(2, fact.Metadata["relations_named_2"]);
        Assert.False(fact.Metadata.ContainsKey("selectivity_2"));
        Assert.False(fact.Metadata.ContainsKey("rows_removed_share_2"));
        Assert.False(fact.Metadata.ContainsKey("plan_rows_2"));
        /* Pair 3: no witness at all. Pair 4: not carried. */
        Assert.Equal(OtherQueryId + 1, PgTargetScorer.SeqScanQueryId(fact, 3));
        Assert.False(fact.Metadata.ContainsKey("heap_bytes_3"));
        Assert.Null(PgTargetScorer.SeqScanQueryId(fact, 4));
        Assert.False(fact.Metadata.ContainsKey(PgTargetScorer.PlanUnavailableKey));

        /* The halves, on the boundary ids. */
        foreach (var id in new[] { long.MinValue, long.MaxValue, -1L, 0L, 1L, ScanQueryId, 1L << 53, (1L << 53) + 1 })
        {
            var (hi, lo) = PgTargetScorer.SeqScanSplitQueryId(id);
            var probe = new Fact { Metadata = { ["query_id_hi_1"] = hi, ["query_id_lo_1"] = lo } };
            Assert.Equal(id, PgTargetScorer.SeqScanQueryId(probe, 1));
        }

        /* Without a schema or columns the relation stands alone; with pg_qualstats KNOWN absent the fact is unavailable,
           the selectivity is withheld even if a stale row named one, and the plan-side evidence still rides. */
        var bare = PgTargetFactCollector.SeqScanFact(Context(), ranked.Skip(2).ToList(), qualstatsAbsent: false);
        Assert.Equal("bare", bare.ObjectName);
        Assert.Null(bare.DatabaseName);
        var absent = PgTargetFactCollector.SeqScanFact(Context(), ranked, qualstatsAbsent: true);
        Assert.Equal(0, absent.Value);
        Assert.Equal(1, absent.Metadata[PgTargetScorer.PlanUnavailableKey]);
        Assert.Equal(1, absent.Metadata[PgTargetScorer.PlanReasonQualstatsAbsentKey]);
        Assert.Equal(1, absent.Metadata["threshold_lineage"]);
        Assert.False(absent.Metadata.ContainsKey("selectivity_1"));
        Assert.Equal(0.99, absent.Metadata["rows_removed_share_1"], precision: 12);
        Assert.Equal(GiB, absent.Metadata["heap_bytes_1"]);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(absent));
    }

    /* ───────────────────────── the scorer ───────────────────────── */

    [Theory]
    [InlineData(0.99, 1L << 30, 5.0, 0.5)]        /* all three gates */
    [InlineData(0.9, 256L * 1024 * 1024, 3.0, 0.5)] /* exactly on every bar */
    [InlineData(0.89, 1L << 30, 5.0, 0.0)]        /* not selective */
    [InlineData(0.99, 255L * 1024 * 1024, 5.0, 0.0)] /* not large */
    [InlineData(0.99, 1L << 30, 2.9, 0.0)]        /* not recurring */
    public void TheGrade_IsTheAdvisoryBaseOnlyWhenEveryGateHolds_AndStampsLineageZero(double selectivity, long heap, double perHour, double expected)
    {
        var fact = Scan(selectivity, heap, perHour);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        Assert.Equal(0, fact.Metadata["threshold_lineage"]);
        Assert.Equal(expected > 0 ? 1 : 0, fact.Metadata["pairs_clearing"]);
    }

    [Fact]
    public void AnUnknownFailsItsGate_ALowerPairCanClear_AndTheUnavailableAndBareShapesGradeNothing()
    {
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Scan(selectivity: null, heap: GiB, perHour: 5)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Scan(selectivity: 0.99, heap: null, perHour: 5)));

        var lower = Scan(selectivity: 0.5, heap: GiB, perHour: 9);
        AddPair(lower, 2, OtherQueryId, selectivity: 0.95, heap: GiB, perHour: 4);
        lower.Metadata[PgTargetScorer.SeqScanPairsKey] = 2;
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(lower), precision: 9);
        Assert.Equal(1, lower.Metadata["pairs_clearing"]);
        Assert.False(PgTargetScorer.SeqScanPairClearsEveryGate(lower, 1));
        Assert.True(PgTargetScorer.SeqScanPairClearsEveryGate(lower, 2));

        var off = new Fact { Source = PgTargetSources.PlansSource, Key = PgTargetFactKeys.SeqScanAdvisory, ServerId = 1, Metadata = { [PgTargetScorer.PlanUnavailableKey] = 1, [PgTargetScorer.PlanReasonAutoExplainOffKey] = 1, ["threshold_lineage"] = 1 } };
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(off));
        Assert.Equal(1, off.Metadata["threshold_lineage"]);
        var bare = new Fact { Source = PgTargetSources.PlansSource, Key = PgTargetFactKeys.SeqScanAdvisory, Value = 42, ServerId = 1 };
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(bare));
        Assert.False(bare.Metadata.ContainsKey("threshold_lineage"));
    }

    [Fact]
    public void TheOneAmplifier_LiftsTo0Point65_ForTheTopPairsStatementOnly_ByEitherCorroborator_Once()
    {
        double Severity(params Fact[] facts)
        {
            var list = facts.ToList();
            new FactScorer().ScoreAll(list);
            return list[0].Severity;
        }

        Assert.Equal(0.5, Severity(Scan(0.99, GiB, 5)), precision: 9);
        Assert.Equal(0.65, Severity(Scan(0.99, GiB, 5), BadActor(ScanQueryId, 0.9)), precision: 9);
        Assert.Equal(0.65, Severity(Scan(0.99, GiB, 5), Regression(ScanQueryId, 20, 200)), precision: 9);
        /* Both corroborators: one amplifier, one lift — 0.65, not 0.8. */
        Assert.Equal(0.65, Severity(Scan(0.99, GiB, 5), BadActor(ScanQueryId, 0.9), Regression(ScanQueryId, 20, 200)), precision: 9);
        /* A different statement's bad actor, a regression on another id, an unfired regression, an unfired bad actor: nothing. */
        Assert.Equal(0.5, Severity(Scan(0.99, GiB, 5), BadActor(OtherQueryId, 0.9)), precision: 9);
        Assert.Equal(0.5, Severity(Scan(0.99, GiB, 5), Regression(OtherQueryId, 20, 200)), precision: 9);
        Assert.Equal(0.5, Severity(Scan(0.99, GiB, 5), Regression(ScanQueryId, 100, 150)), precision: 9);
        Assert.Equal(0.5, Severity(Scan(0.99, GiB, 5), BadActor(ScanQueryId, 0.01, busy: 0.001)), precision: 9);
        /* The second pair's statement being the bad actor lifts nothing: the headline names the first. */
        var twoPairs = Scan(0.99, GiB, 5);
        AddPair(twoPairs, 2, OtherQueryId, 0.99, GiB, 5);
        twoPairs.Metadata[PgTargetScorer.SeqScanPairsKey] = 2;
        Assert.Equal(0.5, Severity(twoPairs, BadActor(OtherQueryId, 0.9)), precision: 9);
        /* An ungraded shape is not lifted into a card by a co-fire. */
        Assert.Equal(0.0, Severity(Scan(0.5, GiB, 5), BadActor(ScanQueryId, 0.9)), precision: 9);
    }

    /* ───────────────────────── the graph ───────────────────────── */

    [Fact]
    public void TheTwoEdges_AreDeclared_AndOpenOnlyForTheTopPairsStatement()
    {
        var graph = new PgTargetRelationshipGraph();
        var edges = graph.GetAllEdges(PgTargetFactKeys.SeqScanAdvisory);
        Assert.Equal(new[] { PgTargetFactKeys.BadActorFamily, PgTargetFactKeys.PlanRegression }, edges.Select(e => e.Destination).ToArray());
        Assert.All(edges, e => Assert.Equal("plans", e.Category));

        /* The alias edge: only when the pass's TOP bad actor is the scanning statement (lane 27's rule for the same seam). */
        var top = new List<Fact> { Scan(0.99, GiB, 5), BadActor(ScanQueryId, 0.9) };
        new FactScorer().ScoreAll(top);
        /* Active edges arrive RESOLVED: the alias becomes the top bad actor's concrete key. */
        Assert.Equal(PgTargetFactKeys.BadActorKey(ScanQueryId), Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.SeqScanAdvisory, top.ToFactLookup())).Destination);

        var outranked = new List<Fact> { Scan(0.99, GiB, 5), BadActor(ScanQueryId, 0.3), BadActor(OtherQueryId, 0.9) };
        new FactScorer().ScoreAll(outranked);
        Assert.Equal(0.65, outranked[0].Severity, precision: 9);     /* the amplifier still lifts … */
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.SeqScanAdvisory, outranked.ToFactLookup()));   /* … the story ends at the scan */

        /* The regression edge: same statement, fired. */
        var regressed = new List<Fact> { Scan(0.99, GiB, 5), Regression(ScanQueryId, 20, 200) };
        new FactScorer().ScoreAll(regressed);
        Assert.Equal(PgTargetFactKeys.PlanRegression, Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.SeqScanAdvisory, regressed.ToFactLookup())).Destination);
        var otherRegressed = new List<Fact> { Scan(0.99, GiB, 5), Regression(OtherQueryId, 20, 200) };
        new FactScorer().ScoreAll(otherRegressed);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.SeqScanAdvisory, otherRegressed.ToFactLookup()));

        /* An unavailable scan fact opens nothing, whatever fired. */
        var off = new List<Fact> { new() { Source = PgTargetSources.PlansSource, Key = PgTargetFactKeys.SeqScanAdvisory, ServerId = 1, Metadata = { [PgTargetScorer.PlanUnavailableKey] = 1, [PgTargetScorer.PlanReasonQualstatsAbsentKey] = 1, ["query_id_hi_1"] = 0, ["query_id_lo_1"] = OtherQueryId & 0xFFFFFFFFL, [PgTargetScorer.SeqScanPairsKey] = 1 } }, BadActor(OtherQueryId, 0.9) };
        new FactScorer().ScoreAll(off);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.SeqScanAdvisory, off.ToFactLookup()));

        /* The traversal: the scan (0.65) roots and walks to its bad actor when the actor fired UNDER it (severity order
           roots the higher fact first and consumes it — the SQL Server engine's rule, unchanged). Since lane 34's ruling
           (2026-09-20) a bad actor is a CONTEXT band on its share alone — 0.3 × (0.5 + 0.5 × share), under the 0.5 story
           line — so a 0.9-share statement with no own-normal deviation no longer roots its own story: the scan walks to
           it too, and the story is the scan's. */
        var walked = new List<Fact> { Scan(0.99, GiB, 5), BadActor(ScanQueryId, 0.12) };
        new FactScorer().ScoreAll(walked);
        Assert.InRange(walked[1].Severity, 0.01, 0.64);
        var stories = new InferenceEngine(graph).BuildStories(walked);
        var story = Assert.Single(stories, s => s.Path[0] == PgTargetFactKeys.SeqScanAdvisory);
        Assert.Equal(new[] { PgTargetFactKeys.SeqScanAdvisory, PgTargetFactKeys.BadActorKey(ScanQueryId) }, story.Path);
        Assert.Equal(PgTargetSources.PlansSource, story.Category);
        var topStory = Assert.Single(new InferenceEngine(graph).BuildStories(top));
        Assert.Equal(new[] { PgTargetFactKeys.SeqScanAdvisory, PgTargetFactKeys.BadActorKey(ScanQueryId) }, topStory.Path);

        var file = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Plans.cs"));
        Assert.DoesNotContain(".Severity", file, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.SeqScanTopStatement(facts)", file, StringComparison.Ordinal);
    }

    /* ───────────────────────── the advice ───────────────────────── */

    [Fact]
    public void TheAdvice_StatesTheEvidenceFirst_NamesTheIndexWithItsCostOnlyWhenClearedAndTheColumnsAreKnown_AndCaveatsTheRate()
    {
        var facts = new List<Fact> { Scan(0.99, GiB, 5), BadActor(ScanQueryId, 0.9), Regression(ScanQueryId, 20, 200) };
        new FactScorer().ScoreAll(facts);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.SeqScanAdvisory, facts.ToFactLookup())!;

        Assert.Equal($"Statement queryid {ScanQueryId} scans public.orders (1 GB) sequentially at least 5× per hour, keeping 1% of the rows it reads", block.Headline);
        var inv = block.Investigation;
        Assert.StartsWith($"Statement queryid {ScanQueryId} was captured 20 times in 4 observed hours (5 per hour — a lower bound: only executions slower than auto_explain.log_min_duration are logged) with a Seq Scan over public.orders carrying a Filter; across those captures the filter removed 99% of the rows the scan read", inv, StringComparison.Ordinal);
        Assert.Contains("(columns: customer_id, status) filtered 99% of the rows it evaluated (990,000 of 1,000,000, sampled at 0.01 — counts are of the sample, not scaled); the planner's worst row estimate on it was off by 12.5×.", inv, StringComparison.Ordinal);
        Assert.Contains("The relation's heap is 1 GB (pg_table_bloat_stats, newest sample).", inv, StringComparison.Ordinal);
        Assert.Contains("All three gates hold — selective (≥ 90% filtered), large (≥ 256 MB), recurring (≥ 3 captures per observed hour)", inv, StringComparison.Ordinal);
        Assert.Contains("PG_BAD_ACTOR fired for the same queryid", inv, StringComparison.Ordinal);
        Assert.Contains("PG_PLAN_REGRESSION fired on the same queryid (20 ms → 200 ms per call)", inv, StringComparison.Ordinal);
        Assert.Contains("chosen, not measured (threshold_lineage = 0)", inv, StringComparison.Ordinal);
        Assert.Contains("queryid is re-keyed by a major upgrade", inv, StringComparison.Ordinal);

        /* The index, with its cost in the same breath, the columns pg_qualstats' own, and the observability caveat. */
        var rem = block.Remediation;
        var evidence = rem.IndexOf("get_pg_plans for queryid", StringComparison.Ordinal);
        var questions = rem.IndexOf("The questions before any access path", StringComparison.Ordinal);
        var ddl = rem.IndexOf("CREATE INDEX CONCURRENTLY ON public.orders (customer_id, status);", StringComparison.Ordinal);
        Assert.True(evidence >= 0 && questions > evidence && ddl > questions, "evidence, then the questions, then the suggestion");
        Assert.Contains("Its cost: every insert, update and delete on public.orders maintains it from then on; storage in proportion to the relation's row count and the key width", rem, StringComparison.Ordinal);
        Assert.Contains("CONCURRENTLY takes no write lock but runs longer, cannot run inside a transaction, and leaves an INVALID index behind if it fails", rem, StringComparison.Ordinal);
        Assert.Contains("off by 12.5×, so ANALYZE public.orders first", rem, StringComparison.Ordinal);
        Assert.Contains("the scan rate above is the floor of what the scan costs, not its size", rem, StringComparison.Ordinal);
        /* The maintainer's prose discipline (2026-09-20): the suggestion is CORROBORATION and comes last; the regression
           caveat rides with it; the impact is an ESTIMATE, the counts SAMPLED, the captures bounded by the log threshold. */
        Assert.Contains("A new index can cause regressions elsewhere — plan changes on other statements that read public.orders, write amplification on every insert, update and delete to it, and storage — so test it against the workload before it reaches production.", rem, StringComparison.Ordinal);
        Assert.Contains("What it might save is an ESTIMATE from the plan's own numbers, not a promise", rem, StringComparison.Ordinal);
        Assert.Contains("pg_qualstats' SAMPLE (sample_rate 0.01), not the full count", rem, StringComparison.Ordinal);
        Assert.DoesNotMatch(new Regex(@"\d+(\.\d+)?% (faster|improvement|saving)"), rem);
        Assert.True(rem.IndexOf("A new index can cause regressions elsewhere", StringComparison.Ordinal) > ddl);
        AssertRegressionCaveatRidesWithEverySuggestion(block);
        Assert.Contains("get_pg_top_queries", rem, StringComparison.Ordinal);
        Assert.Contains("get_pg_table_bloat has public.orders's size", rem, StringComparison.Ordinal);

        /* Cleared but the columns unknown (no qualstats row → not cleared either, so: cleared by a lower pair while the top
           pair has no columns): no index text, the reason said. */
        var noColumns = Scan(0.99, GiB, 5, columns: null);
        var lonely = new List<Fact> { noColumns };
        new FactScorer().ScoreAll(lonely);
        var noColumnsBlock = PgTargetAdvice.Compose(PgTargetFactKeys.SeqScanAdvisory, lonely.ToFactLookup())!;
        Assert.DoesNotContain("CREATE INDEX", noColumnsBlock.Remediation, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("No index is named: the predicate columns are not known here", noColumnsBlock.Remediation, StringComparison.Ordinal);
        Assert.Contains("pg_qualstats' most selective predicate of this statement on the relation filtered 99%", noColumnsBlock.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("PG_BAD_ACTOR fired", noColumnsBlock.Investigation, StringComparison.Ordinal);

        /* Under the bars: shown, not graded; no index. */
        var under = new List<Fact> { Scan(0.5, GiB, 5) };
        new FactScorer().ScoreAll(under);
        var underBlock = PgTargetAdvice.Compose(PgTargetFactKeys.SeqScanAdvisory, under.ToFactLookup())!;
        Assert.Equal($"Statement queryid {ScanQueryId} scans public.orders sequentially in captured plans; the shape is under this card's bars", underBlock.Headline);
        Assert.Contains("filtered 50% of the rows it evaluated", underBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("The shape is under this card's bars (selective ≥ 90% filtered, large ≥ 256 MB, recurring ≥ 3 captures per observed hour); it is shown, not graded.", underBlock.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", underBlock.Remediation, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("No index is named: the shape is under this card's bars", underBlock.Remediation, StringComparison.Ordinal);

        /* No predicate sampled: the unknown is said, the gate fails on it. */
        var unsampled = Scan(selectivity: null, heap: GiB, perHour: 5, columns: null);
        Assert.Contains("pg_qualstats recorded no predicate for this statement on the relation — its default sample_rate is 1 / max_connections", PgTargetAdvice.Compose(PgTargetFactKeys.SeqScanAdvisory, new[] { unsampled }.ToFactLookup())!.Investigation, StringComparison.Ordinal);
        /* No size sampled, no Actual Rows: both unknowns said. */
        var blind = Scan(selectivity: 0.99, heap: null, perHour: 5);
        blind.Metadata.Remove("rows_removed_share_1");
        var blindBlock = PgTargetAdvice.Compose(PgTargetFactKeys.SeqScanAdvisory, new[] { blind }.ToFactLookup())!;
        Assert.Contains("the captured plans carry no Actual Rows (auto_explain.log_analyze is off)", blindBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("The relation's size is not known: pg_table_bloat_stats has no sample of it inside the lookback", blindBlock.Investigation, StringComparison.Ordinal);

        /* Three pairs: the lower two by id and figures, their names deferred to get_pg_plans; several relations sharing a name. */
        var three = Scan(0.99, GiB, 5);
        AddPair(three, 2, OtherQueryId, 0.95, 2 * GiB, 4);
        AddPair(three, 3, OtherQueryId + 1, null, null, 3.5);
        three.Metadata[PgTargetScorer.SeqScanPairsKey] = 3;
        three.Metadata[PgTargetScorer.SeqScanCandidatePairsKey] = 7;
        three.Metadata["relations_named_1"] = 2;
        var threeBlock = PgTargetAdvice.Compose(PgTargetFactKeys.SeqScanAdvisory, new[] { three }.ToFactLookup())!;
        Assert.Contains("; 2 relations share the name across databases or schemas and the largest is stated", threeBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains($" 7 (relation, statement) pairs showed the shape in the window; the next by scan rate × rows removed: queryid {OtherQueryId} at 4 per hour, 95% filtered, heap 2 GB (clears every gate); queryid {OtherQueryId + 1} at 3.5 per hour (under the bars). Their relation names are on get_pg_plans", threeBlock.Investigation, StringComparison.Ordinal);
    }

    /// <summary>The pin the maintainer asked for: wherever the advice names an index, the same block says a new index
    /// can cause regressions elsewhere and must be tested — and a block naming no index needs no caveat. Applied to
    /// every composed shape this class builds.</summary>
    private static void AssertRegressionCaveatRidesWithEverySuggestion(AdviceBlock block)
    {
        var prose = block.Headline + "\n" + block.Investigation + "\n" + block.Remediation;
        if (prose.Contains("CREATE INDEX", StringComparison.OrdinalIgnoreCase))
        {
            Assert.Contains("A new index can cause regressions elsewhere", prose, StringComparison.Ordinal);
            Assert.Contains("test it", prose, StringComparison.Ordinal);
            Assert.Contains("ESTIMATE", prose, StringComparison.Ordinal);
            Assert.Contains("SAMPLE", prose, StringComparison.Ordinal);
            Assert.Contains("auto_explain.log_min_duration", prose, StringComparison.Ordinal);
            /* The statement comes LAST: the evidence and the questions precede it. */
            Assert.True(block.Remediation.IndexOf("The questions before any access path", StringComparison.Ordinal) < block.Remediation.IndexOf("CREATE INDEX", StringComparison.Ordinal));
        }
    }

    [Fact]
    public void EveryComposedShape_CarriesTheRegressionCaveatWheneverItNamesAnIndex()
    {
        var shapes = new List<Fact>
        {
            Scan(0.99, GiB, 5),
            Scan(0.99, GiB, 5, columns: null),
            Scan(0.5, GiB, 5),
            Scan(null, GiB, 5),
            Scan(0.99, null, 5),
            Scan(0.99, GiB, 2),
        };
        var named = 0;
        foreach (var shape in shapes)
        {
            var block = PgTargetAdvice.Compose(PgTargetFactKeys.SeqScanAdvisory, new[] { shape }.ToFactLookup())!;
            AssertRegressionCaveatRidesWithEverySuggestion(block);
            if (block.Remediation.Contains("CREATE INDEX", StringComparison.Ordinal)) named++;
        }
        /* Exactly one of the six shapes names an index: every gate held AND the columns are known. */
        Assert.Equal(1, named);
    }

    [Fact]
    public void TheUnavailableShapes_SayWhichInstrumentIsMissing_AndTheStaticBlockNamesNoIndex()
    {
        var off = new Fact { Source = PgTargetSources.PlansSource, Key = PgTargetFactKeys.SeqScanAdvisory, ServerId = 1, Metadata = { [PgTargetScorer.PlanUnavailableKey] = 1, [PgTargetScorer.PlanReasonAutoExplainOffKey] = 1, ["threshold_lineage"] = 1 } };
        var offBlock = PgTargetAdvice.Compose(PgTargetFactKeys.SeqScanAdvisory, new[] { off }.ToFactLookup())!;
        Assert.Equal("Sequential-scan shapes cannot be seen on this server: auto_explain is not loaded", offBlock.Headline);
        Assert.Contains("Nothing is graded off this fact.", offBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("auto_explain.log_analyze adds Actual Rows and Rows Removed by Filter", offBlock.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", offBlock.Remediation, StringComparison.OrdinalIgnoreCase);

        var absent = Scan(0.99, GiB, 5, columns: null);
        absent.Metadata.Remove("selectivity_1");
        absent.Metadata[PgTargetScorer.PlanUnavailableKey] = 1;
        absent.Metadata[PgTargetScorer.PlanReasonQualstatsAbsentKey] = 1;
        absent.Metadata["threshold_lineage"] = 1;
        var absentBlock = PgTargetAdvice.Compose(PgTargetFactKeys.SeqScanAdvisory, new[] { absent }.ToFactLookup())!;
        Assert.Equal($"Statement queryid {ScanQueryId} scans public.orders sequentially in captured plans, but the predicate's selectivity cannot be read: pg_qualstats is not installed", absentBlock.Headline);
        Assert.Contains("across those captures the filter removed 99% of the rows the scan read", absentBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("pg_extension_availability reports pg_qualstats is not installed in any database", absentBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("Installing pg_qualstats (it must be in shared_preload_libraries", absentBlock.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", absentBlock.Remediation, StringComparison.OrdinalIgnoreCase);

        var statics = PgTargetAdvice.Static(PgTargetFactKeys.SeqScanAdvisory)!;
        Assert.Equal("A large relation is read sequentially, often, by a statement whose predicate keeps few of the rows it reads", statics.Headline);
        Assert.Contains("LOWER BOUND", statics.Investigation, StringComparison.Ordinal);
        Assert.Contains("when they are not known, it does not guess one from the plan", statics.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", statics.Investigation + statics.Remediation, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(statics, FactAdvice.GetForFactKey(PgTargetFactKeys.SeqScanAdvisory));

        /* Source-level: the family's ONE index literal lives in the guarded branch of the Seq-Scan composer — after the
           gates and the columns are checked — and nowhere else in the plan family's four files. The "never DDL" census
           that stood in PgTargetV3PlumbingTests was withdrawn by the maintainer on 2026-09-20; this is the rule that
           replaced it: evidence first, a suggestion with its cost where the predicate is known. */
        var advice = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetAdvice.Plans.cs");
        var literals = CSharpSourceWalker.StringLiteralBodies(advice).Where(l => l.Text.Contains("CREATE INDEX", StringComparison.OrdinalIgnoreCase)).ToList();
        Assert.Single(literals);
        var guarded = advice.IndexOf("else if (cleared && columns is not null)", StringComparison.Ordinal);
        var ddl = advice.IndexOf("CREATE INDEX CONCURRENTLY ON {relation} ({columns});", StringComparison.Ordinal);
        Assert.True(guarded > 0 && ddl > guarded);
        Assert.Contains("Its cost:", advice[ddl..], StringComparison.Ordinal);
        foreach (var file in new[] { "PgTargetScorer.Plans.cs", "PgTargetRelationshipGraph.Plans.cs" })
            Assert.DoesNotContain("CREATE INDEX", RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", file), StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Plans.cs"), StringComparison.Ordinal);
    }

    /* ───────────────────────── the reads and the censuses ───────────────────────── */

    [Fact]
    public void TheReads_PrefilterByText_WalkInCsharp_JoinBothWitnessesInOneTrip_AndSitInEveryCensus()
    {
        var captures = PgTargetFactCollector.PgTargetSeqScanCapturesSql;
        Assert.Contains("FROM pg_plan_capture", captures, StringComparison.Ordinal);
        Assert.Contains("AND   query_id <> 0", captures, StringComparison.Ordinal);
        Assert.Contains("AND   plan_json LIKE '%Seq Scan%'", captures, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC\nLIMIT $4", captures.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.DoesNotContain("::jsonb", captures, StringComparison.Ordinal);
        Assert.Equal(2_000, PgTargetFactCollector.SeqScanCaptureRowCap);

        var witness = PgTargetFactCollector.PgTargetSeqScanWitnessSql;
        Assert.Contains("SELECT unnest($5::bigint[]) AS query_id, unnest($6::text[]) AS table_name", witness, StringComparison.Ordinal);
        Assert.Contains("FROM pg_predicate_stats AS p", witness, StringComparison.Ordinal);
        Assert.Contains("FROM pg_table_bloat_stats AS b", witness, StringComparison.Ordinal);
        Assert.Contains("p.rows_filtered::double precision / p.rows_evaluated AS selectivity", witness, StringComparison.Ordinal);
        Assert.Contains("AND   p.rows_evaluated > 0", witness, StringComparison.Ordinal);
        Assert.Contains("string_agg(column_name, ', ' ORDER BY selectivity DESC, column_name)    AS predicate_columns", witness, StringComparison.Ordinal);
        Assert.Contains("make_interval(days => $4)", witness, StringComparison.Ordinal);
        Assert.Contains("COALESCE(h.heap_bytes, z.heap_bytes) AS heap_bytes", witness, StringComparison.Ordinal);
        Assert.Contains("MAX(heap_bytes) AS heap_bytes", witness, StringComparison.Ordinal);
        Assert.Contains("COUNT(*)        AS relations_named", witness, StringComparison.Ordinal);

        foreach (var sql in new[] { captures, witness })
        {
            Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotMatch(new Regex(@"EXTRACT\s*\(\s*\w+\s+FROM\s+\w+"), sql);
            Assert.Contains(sql, PgTargetFactCollector.AllSql);
        }

        var collector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Plans.cs"));
        Assert.Contains("using var document = JsonDocument.Parse(planJson);", collector, StringComparison.Ordinal);
        Assert.Contains("catch (JsonException)", collector, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.SeqScanCarriedPairs", collector, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.SeqScanSplitQueryId(pair.Scan.QueryId)", collector, StringComparison.Ordinal);
        Assert.Contains("cmd.Parameters.AddWithValue(SeqScanCaptureRowCap);", collector, StringComparison.Ordinal);
        Assert.Contains("facts.Add(SeqScanUnavailableFact(context));", collector, StringComparison.Ordinal);
        /* The rate is over OBSERVED hours; the nominal window is never named. */
        Assert.Contains("context.ObservedDurationMs / 3_600_000.0", collector, StringComparison.Ordinal);
        Assert.DoesNotContain("TimeRangeEnd - context.TimeRangeStart", collector, StringComparison.Ordinal);

        /* The tool row names the brief's three reads and the size. */
        var tools = PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.SeqScanAdvisory)!.Select(t => t.Tool).ToList();
        foreach (var tool in new[] { "get_pg_plans", "get_pg_predicate_stats", "get_pg_top_queries", "get_pg_table_bloat" })
            Assert.Contains(tool, tools);
    }

    /* ───────────────────────── the gated e2e ───────────────────────── */

    [Fact]
    public async Task TwentyCapturedSeqScansOverAGibibyteRelation_WithASelectivePredicate_YieldTheAdvisoryAt0Point5_NamingTheRelationAndTheCandidateIndexWithItsCost()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the Seq-Scan e2e.");

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

            /* The coverage witness reads pg_database_stats: 25 h of span, one row a minute in the window. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* Twenty captures — five an hour — of one plan hash: a Seq Scan over big keeping 1 % of 1,000,000 rows. Plus a
               plan of another statement scanning small (a 40 MiB relation, 20 % removed): the second pair, under the bars.
               Plus an orphan and a plan whose only Seq Scan has no Filter: neither is a pair. */
            const string bigPlan = """{"Plan": {"Node Type": "Aggregate", "Plans": [{"Node Type": "Seq Scan", "Relation Name": "big", "Alias": "b", "Filter": "(customer_id = ?)", "Plan Rows": 12000, "Actual Rows": 10000, "Rows Removed by Filter": 990000}]}}""";
            const string smallPlan = """{"Plan": {"Node Type": "Seq Scan", "Relation Name": "small", "Filter": "(kind = '?')", "Plan Rows": 800, "Actual Rows": 800, "Rows Removed by Filter": 200}}""";
            const string wholePlan = """{"Plan": {"Node Type": "Seq Scan", "Relation Name": "big", "Plan Rows": 1000000, "Actual Rows": 1000000}}""";
            for (var minute = 6; minute <= 4 * 60; minute += 12)
                await PlantCaptureAsync(connection, windowStart.AddMinutes(minute), ScanQueryId, "1111111111111111AAAAAAAAAAAAAAAA", 840.5, 2, "Aggregate", bigPlan, ct);
            for (var minute = 30; minute <= 4 * 60; minute += 60)
                await PlantCaptureAsync(connection, windowStart.AddMinutes(minute), OtherQueryId, "2222222222222222BBBBBBBBBBBBBBBB", 120.0, 1, "Seq Scan", smallPlan, ct);
            await PlantCaptureAsync(connection, windowStart.AddMinutes(90), 0, "3333333333333333CCCCCCCCCCCCCCCC", 50, 1, "Seq Scan", smallPlan, ct);
            await PlantCaptureAsync(connection, windowStart.AddMinutes(100), OtherQueryId + 1, "4444444444444444DDDDDDDDDDDDDDDD", 900, 1, "Seq Scan", wholePlan, ct);

            /* The witnesses: big is 1 GiB in appdb.public; small is 40 MiB. pg_qualstats saw the scanning statement's
               predicate on big.customer_id (99 % filtered) and on big.region (30 %); nothing on small. */
            await PlantBloatAsync(connection, windowEnd.AddMinutes(-20), "appdb", "public", "big", GiB, ct);
            await PlantBloatAsync(connection, windowEnd.AddMinutes(-20), "appdb", "public", "small", 40L * 1024 * 1024, ct);
            await PlantPredicateAsync(connection, windowEnd.AddMinutes(-40), ScanQueryId, "appdb", "public", "big", "customer_id", 1_000_000, 990_000, 8.4, ct);
            await PlantPredicateAsync(connection, windowEnd.AddMinutes(-40), ScanQueryId, "appdb", "public", "big", "region", 1_000_000, 300_000, 1.1, ct);
            await PlantReadinessAsync(connection, windowStart.AddMinutes(-30), "library_loaded", true, ct);
            await PlantExtensionAsync(connection, windowStart.AddHours(-1), "appdb", "pg_qualstats", "installed", ct);

            /* ── the collector alone. */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext { ServerId = ServerId, ServerName = ServerName, TimeRangeStart = windowStart, TimeRangeEnd = windowEnd, ServerUtcOffset = TimeSpan.Zero };
            var facts = await collector.CollectFactsAsync(context);
            Assert.Equal(14_400_000, context.ObservedDurationMs, precision: 3);

            var scan = Assert.Single(facts, f => f.Source == PgTargetSources.PlansSource);
            Assert.Equal(PgTargetFactKeys.SeqScanAdvisory, scan.Key);
            Assert.Equal("public.big (customer_id, region)", scan.ObjectName);
            Assert.Equal("appdb", scan.DatabaseName);
            Assert.Equal(2, scan.Metadata[PgTargetScorer.SeqScanPairsKey]);
            Assert.Equal(2, scan.Metadata[PgTargetScorer.SeqScanCandidatePairsKey]);
            Assert.Equal(ScanQueryId, PgTargetScorer.SeqScanQueryId(scan, 1));
            Assert.Equal(20, scan.Metadata["captures_1"]);
            Assert.Equal(5, scan.Metadata["captures_per_hour_1"]);
            Assert.Equal(0.99, scan.Metadata["rows_removed_share_1"], precision: 9);
            Assert.Equal(GiB, scan.Metadata["heap_bytes_1"]);
            Assert.Equal(1, scan.Metadata["relations_named_1"]);
            Assert.Equal(0.99, scan.Metadata["selectivity_1"], precision: 9);
            Assert.Equal(990_000, scan.Metadata["rows_filtered_1"]);
            Assert.Equal(8.4, scan.Metadata["estimate_error_ratio_1"], precision: 9);
            Assert.Equal(5 * 0.99, scan.Value, precision: 9);
            Assert.Equal(OtherQueryId, PgTargetScorer.SeqScanQueryId(scan, 2));
            Assert.Equal(1, scan.Metadata["captures_per_hour_2"]);
            Assert.Equal(0.2, scan.Metadata["rows_removed_share_2"], precision: 9);
            Assert.Equal(40L * 1024 * 1024, scan.Metadata["heap_bytes_2"]);
            Assert.False(scan.Metadata.ContainsKey("selectivity_2"));
            Assert.False(scan.Metadata.ContainsKey(PgTargetScorer.PlanUnavailableKey));
            /* No pg_statement_stats were planted: no bad actor, no regression — the card must sit at exactly the base. */
            Assert.DoesNotContain(facts, f => f.Key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal));

            /* ── THE EXIT CRITERION: the real analyze_server tool, anchored at the planted window's end. */
            var service = new DarlingAnalysisService(postgres);
            var asOf = windowEnd.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.SeqScanAdvisory);
                Assert.Equal(0.5, card.GetProperty("severity").GetDouble(), precision: 9);
                Assert.Equal(PgTargetSources.PlansSource, card.GetProperty("category").GetString());
                Assert.Equal(1, card.GetProperty("fact_count").GetInt32());
                var advice = card.GetProperty("advice");
                Assert.Equal($"Statement queryid {ScanQueryId} scans public.big (1 GB) sequentially at least 5× per hour, keeping 1% of the rows it reads", advice.GetProperty("headline").GetString());
                var investigation = advice.GetProperty("investigation").GetString()!;
                Assert.Contains("was captured 20 times in 4 observed hours (5 per hour — a lower bound", investigation, StringComparison.Ordinal);
                Assert.Contains("(columns: customer_id, region) filtered 99% of the rows it evaluated (990,000 of 1,000,000, sampled at 0.01", investigation, StringComparison.Ordinal);
                Assert.Contains("All three gates hold", investigation, StringComparison.Ordinal);
                Assert.Contains($"queryid {OtherQueryId} at 1 per hour, heap 40 MB (under the bars)", investigation, StringComparison.Ordinal);
                var remediation = advice.GetProperty("remediation").GetString()!;
                Assert.Contains("CREATE INDEX CONCURRENTLY ON public.big (customer_id, region);", remediation, StringComparison.Ordinal);
                Assert.Contains("Its cost: every insert, update and delete on public.big maintains it from then on", remediation, StringComparison.Ordinal);
                Assert.Contains("off by 8.4×, so ANALYZE public.big first", remediation, StringComparison.Ordinal);
                Assert.Contains("A new index can cause regressions elsewhere — plan changes on other statements that read public.big", remediation, StringComparison.Ordinal);
                Assert.Contains("ESTIMATE from the plan's own numbers, not a promise", remediation, StringComparison.Ordinal);
                Assert.True(remediation.IndexOf("The questions before any access path", StringComparison.Ordinal) < remediation.IndexOf("CREATE INDEX", StringComparison.Ordinal));
                var tools = card.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains("get_pg_plans", tools);
                Assert.Contains("get_pg_predicate_stats", tools);
                Assert.Contains("get_pg_top_queries", tools);
                /* The index text is this card's alone: nowhere else in the payload. */
                var others = findings.Where(f => f.GetProperty("root_fact").GetProperty("key").GetString() != PgTargetFactKeys.SeqScanAdvisory).Select(f => f.GetRawText()).ToList();
                Assert.All(others, text => Assert.DoesNotContain("CREATE INDEX", text, StringComparison.OrdinalIgnoreCase));
                Assert.Single(Regex.Matches(json, "CREATE INDEX"));
            }

            /* get_analysis_facts under the family's source: the one fact, stamped unmeasured. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.PlansSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var shown = Assert.Single(doc.RootElement.GetProperty("facts").EnumerateArray());
                Assert.Equal(PgTargetFactKeys.SeqScanAdvisory, shown.GetProperty("key").GetString());
                Assert.Equal(0, shown.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
                Assert.Equal(1, shown.GetProperty("metadata").GetProperty("pairs_clearing").GetDouble());
            }

            /* ── the unavailable shape: a second server, library off, nothing captured. */
            var offId = ServerIdHelper.GetDeterministicHashCode(ServerName + "-off");
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, offId, ServerName + "-off", "postgres", 18, ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, offId, ServerName + "-off", windowStart.AddMinutes(minute - 1), ct);
            await PlantReadinessAsync(connection, windowStart.AddMinutes(-30), "library_loaded", false, ct, offId, ServerName + "-off");
            var offFacts = await collector.CollectFactsAsync(new AnalysisContext { ServerId = offId, ServerName = ServerName + "-off", TimeRangeStart = windowStart, TimeRangeEnd = windowEnd, ServerUtcOffset = TimeSpan.Zero });
            var off = Assert.Single(offFacts, f => f.Key == PgTargetFactKeys.SeqScanAdvisory);
            Assert.Equal(1, off.Metadata[PgTargetScorer.PlanUnavailableKey]);
            Assert.Equal(1, off.Metadata[PgTargetScorer.PlanReasonAutoExplainOffKey]);
            Assert.Equal(0, PgTargetScorer.ScoreBase(off));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────────── builders ───────────────────────── */

    private static AnalysisContext Context() => new() { ServerId = 1, ServerName = "s", TimeRangeStart = DateTime.UnixEpoch, TimeRangeEnd = DateTime.UnixEpoch.AddHours(4), ServerUtcOffset = TimeSpan.Zero, Coverage = new WindowCoverage { NominalMs = 14_400_000, ObservedMs = 14_400_000, SampleCount = 240 } };

    private static PgTargetFactCollector.SeqScanWitnessRow Witness(long queryId, string table, double? selectivity, long? heap, string? columns = null, string? schema = null, string? database = null,
        long? rowsEvaluated = null, long? rowsFiltered = null, double? sampleRate = null, double? error = null, long? relationsNamed = 1) =>
        new(queryId, table, database, schema, selectivity, rowsEvaluated, rowsFiltered, sampleRate, error, columns, heap, heap is null ? null : relationsNamed);

    /// <summary>A one-pair fact over <c>public.orders</c> for the statement, 20 captures in 4 h unless told otherwise.</summary>
    private static Fact Scan(double? selectivity, long? heap, double perHour, string? columns = "customer_id, status", long queryId = ScanQueryId)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.PlansSource,
            Key = PgTargetFactKeys.SeqScanAdvisory,
            Value = perHour * 0.99,
            ServerId = 1,
            ObjectName = columns is null ? "public.orders" : $"public.orders ({columns})",
            DatabaseName = "appdb",
            Metadata = { [PgTargetScorer.SeqScanPairsKey] = 1, [PgTargetScorer.SeqScanCandidatePairsKey] = 1, ["observed_hours"] = 4 },
        };
        AddPair(fact, 1, queryId, selectivity, heap, perHour);
        fact.Metadata["rows_removed_share_1"] = 0.99;
        fact.Metadata["actual_rows_1"] = 1_000;
        fact.Metadata["rows_removed_by_filter_1"] = 99_000;
        return fact;
    }

    private static void AddPair(Fact fact, int rank, long queryId, double? selectivity, long? heap, double perHour)
    {
        var (hi, lo) = PgTargetScorer.SeqScanSplitQueryId(queryId);
        fact.Metadata[$"query_id_hi_{rank}"] = hi;
        fact.Metadata[$"query_id_lo_{rank}"] = lo;
        fact.Metadata[$"captures_{rank}"] = perHour * 4;
        fact.Metadata[$"captures_per_hour_{rank}"] = perHour;
        if (heap is { } h) { fact.Metadata[$"heap_bytes_{rank}"] = h; fact.Metadata[$"relations_named_{rank}"] = 1; }
        if (selectivity is { } s)
        {
            fact.Metadata[$"selectivity_{rank}"] = s;
            fact.Metadata[$"rows_evaluated_{rank}"] = 1_000_000;
            fact.Metadata[$"rows_filtered_{rank}"] = s * 1_000_000;
            fact.Metadata[$"sample_rate_{rank}"] = 0.01;
            fact.Metadata[$"estimate_error_ratio_{rank}"] = 12.5;
        }
    }

    /// <summary>Lane 7's bad actor, its own tests' shape — over the measured busy floor unless told otherwise.</summary>
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

    /// <summary>Lane 27's regression, its own tests' shape.</summary>
    private static Fact Regression(long queryId, double before, double after) => new()
    {
        Source = PgTargetSources.PlansSource,
        Key = PgTargetFactKeys.PlanRegression,
        Value = after / before,
        ServerId = 1,
        ObjectName = queryId.ToString(CultureInfo.InvariantCulture),
        Metadata =
        {
            [PgTargetScorer.PlanMeanMsBeforeKey] = before,
            [PgTargetScorer.PlanMeanMsAfterKey] = after,
            ["mean_ms_delta"] = after - before,
            [PgTargetScorer.PlanCallsBeforeKey] = 6_000,
            [PgTargetScorer.PlanCallsAfterKey] = 3_000,
            [PgTargetScorer.PlanHashCountKey] = 2,
            ["top_node_changed"] = 0,
            ["flipped_statements"] = 1,
        },
    };

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /* ───────────────────────── planting ───────────────────────── */

    private static async Task PlantCaptureAsync(NpgsqlConnection connection, DateTime at, long queryId, string planHash, double durationMs, int nodeCount, string topNode, string planJson, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_plan_capture
    (collection_id, collection_time, server_id, server_name, query_id, plan_hash, duration_ms, node_count, top_node_type, plan_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planHash);
        command.Parameters.AddWithValue(durationMs);
        command.Parameters.AddWithValue(nodeCount);
        command.Parameters.AddWithValue(topNode);
        command.Parameters.AddWithValue(planJson);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantBloatAsync(NpgsqlConnection connection, DateTime at, string database, string schema, string table, long heapBytes, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_table_bloat_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name,
     heap_bytes, heap_pages, toast_bytes, index_bytes, live_tuples, dead_tuples, mods_since_analyze, last_analyzed,
     estimated_tuple_bytes, estimated_heap_pages, fillfactor, bloat_bytes_estimate, bloat_pct_estimate, estimate_unavailable,
     alignment_bytes, pgstattuple_available)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8 / 8192, 0, 0, 1000000, 1000, 0, NULL, 120.0, $8 / 8192, 100, 0, 0.0, FALSE, 8, FALSE)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(heapBytes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantPredicateAsync(NpgsqlConnection connection, DateTime at, long queryId, string database, string schema, string table, string column, long evaluated, long filtered, double errorRatio, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_predicate_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name, operator, query_id,
     sample_count, rows_evaluated, rows_filtered, worst_estimate_error_ratio, sample_rate)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '=', $9, 120, $10, $11, $12, 0.01)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(column);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(evaluated);
        command.Parameters.AddWithValue(filtered);
        command.Parameters.AddWithValue(errorRatio);
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
            $"DELETE FROM pg_plan_capture WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM pg_predicate_stats WHERE server_id IN ({ServerId}, {offId}); " +
            $"DELETE FROM pg_table_bloat_stats WHERE server_id IN ({ServerId}, {offId}); " +
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
