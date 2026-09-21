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
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The bloat family (#3691 lane 13, design §3.4): <c>PG_BLOAT_TREND</c> and <c>PG_INDEX_BLOAT_TREND</c> as
/// TRENDS with size floors, never a spot percentage. Pins: the measured line (both arms — bytes AND fraction
/// of the earlier estimate — with the earlier-zero case passing the fraction arm), the size floor re-applied
/// by the scorer, the index's chosen sample minimum and its <c>threshold_lineage = 0</c>, the withheld shapes
/// and their reasons, the top-3 named metadata, the same-table co-fire (amplifier and edge through one
/// predicate), the story shapes, the advice's value-stated sentences and D8, and the collector SQL's honesty
/// filters by text. The live e2e (<see cref="PgTargetBloatLiveTests"/>) drives the real reads and the real
/// <c>analyze_server</c>; everything here is pure logic, executed locally before CI.
/// </summary>
public sealed class PgTargetBloatTests
{
    private const long MiB = 1024L * 1024;
    private const long GiB = 1024L * 1024 * 1024;

    /* ── the constants, by value, so a retype fails here with the number in the message ── */

    [Fact]
    public void TheMeasuredBars_AreTheCalibrationPassesFigures()
    {
        Assert.Equal(14, PgTargetScorer.BloatLookbackDays);
        Assert.Equal(256 * MiB, PgTargetScorer.BloatGrowthConcerningBytes);
        Assert.Equal(0.25, PgTargetScorer.BloatGrowthConcerningFraction);
        Assert.Equal(GiB, PgTargetScorer.BloatGrowthCriticalBytes);
        Assert.Equal(64 * MiB, PgTargetScorer.BloatSizeFloorBytes);
        Assert.Equal(3, PgTargetScorer.IndexBloatMinimumSamples);
        Assert.Equal(3, PgTargetScorer.BloatTopObjects);
    }

    /* ── PG_BLOAT_TREND: the line, both arms, and the floor ── */

    [Theory]
    [InlineData(1_000 * MiB, 0L, 300 * MiB, 0.5286458333)]       /* earlier zero: fraction arm passes trivially, bytes arm decides */
    [InlineData(1_000 * MiB, 512 * MiB, 256 * MiB, 0.5)]        /* exactly at both arms: 256 MiB is 50 % of 512 MiB */
    [InlineData(1_000 * MiB, 512 * MiB, 255 * MiB, 0.0)]        /* one MiB under the bytes line */
    [InlineData(4 * GiB, 2 * GiB, 300 * MiB, 0.0)]              /* over the bytes line, under a quarter of the earlier estimate */
    [InlineData(4 * GiB, 2 * GiB, 512 * MiB, 0.6666666667)]     /* exactly a quarter: 0.5 + 0.5 × 256/768 */
    [InlineData(2 * GiB, 100 * MiB, 640 * MiB, 0.75)]           /* halfway up the ramp */
    [InlineData(8 * GiB, 100 * MiB, GiB, 1.0)]                  /* the critical line */
    [InlineData(8 * GiB, 100 * MiB, 7 * GiB, 1.0)]              /* past it: capped */
    [InlineData(63 * MiB, 0L, 2 * GiB, 0.0)]                    /* under the heap floor: never graded, however large the growth */
    [InlineData(1_000 * MiB, 500 * MiB, -100 * MiB, 0.0)]       /* shrank (a VACUUM FULL inside the lookback): nothing */
    public void TheTableTrendBase_NeedsBothArmsOfTheLine_AndTheHeapFloor(long heapBytes, long earlier, long growth, double expected)
    {
        var fact = Table("public.orders", heapBytes, earlier, growth);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        /* Every bar here is measured; a fact that reached the line (either way) says so. Under the floor no bar
           was consulted, so no flag. */
        if (heapBytes >= PgTargetScorer.BloatSizeFloorBytes)
            Assert.Equal(1, fact.Metadata["threshold_lineage"]);
        else
            Assert.False(fact.Metadata.ContainsKey("threshold_lineage"));
    }

    [Fact]
    public void AWithheldTableFact_ScoresZero_CarriesItsReason_AndNoLineageFlag()
    {
        var unavailable = Withheld(PgTargetFactKeys.BloatTrend, PgTargetScorer.BloatReasonEstimateUnavailable, seen: 40, estimable: 0);
        var belowFloor = Withheld(PgTargetFactKeys.BloatTrend, PgTargetScorer.BloatReasonBelowSizeFloor, seen: 40, estimable: 40);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(unavailable));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(belowFloor));
        Assert.False(unavailable.Metadata.ContainsKey("threshold_lineage"));
        Assert.False(belowFloor.Metadata.ContainsKey("threshold_lineage"));
        Assert.Equal(PgTargetScorer.BloatReasonEstimateUnavailable, unavailable.Metadata[PgTargetScorer.BloatUnavailableReasonKey]);
    }

    /* ── PG_INDEX_BLOAT_TREND: the same line, the index floor, and the chosen sample minimum ── */

    [Theory]
    [InlineData(3, 0.75)]
    [InlineData(14, 0.75)]
    [InlineData(2, 0.0)]
    [InlineData(1, 0.0)]
    public void TheIndexTrendBase_IsHeldBackUnderThreeSamples_AndTheFlagSaysTheChosenGateDecided(long samples, double expected)
    {
        var fact = Index("public.orders.orders_pkey", 512 * MiB, 100 * MiB, 640 * MiB, samples);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        Assert.Equal(expected > 0 ? 1 : 0, fact.Metadata["threshold_lineage"]);

        var insufficient = Withheld(PgTargetFactKeys.IndexBloatTrend, PgTargetScorer.BloatReasonInsufficientSamples, seen: 10, estimable: 4);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(insufficient));
        Assert.Equal(0, insufficient.Metadata["threshold_lineage"]);

        var allSkipped = Withheld(PgTargetFactKeys.IndexBloatTrend, PgTargetScorer.BloatReasonPgstattupleUnavailable, seen: 10, estimable: 0);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(allSkipped));
        Assert.False(allSkipped.Metadata.ContainsKey("threshold_lineage"));
    }

    [Fact]
    public void TheIndexFloor_IsTheSameSixtyFourMiB_OnIndexBytes()
    {
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Index("public.t.i", 64 * MiB - 1, 0, 2 * GiB, 14)));
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(Index("public.t.i", 64 * MiB, 0, 2 * GiB, 14)));
    }

    /* ── the named objects and the same-table predicate ── */

    [Fact]
    public void NamedObjects_ReadTheWorstAndEveryGrowthKey_AndParentTablesDropTheIndexPart()
    {
        var table = Table("public.orders", GiB, 100 * MiB, 500 * MiB,
            (PgTargetScorer.BloatNamedGrowthBytesPrefix + "public.events", 300 * MiB),
            (PgTargetScorer.BloatNamedGrowthBytesPrefix + "sales.ledger", 10 * MiB));
        Assert.Equal(["public.events", "public.orders", "sales.ledger"], PgTargetScorer.BloatNamedObjects(table).Order(StringComparer.Ordinal));

        var index = Index("public.orders.orders_pkey", 512 * MiB, 0, 300 * MiB, 5,
            (PgTargetScorer.BloatNamedGrowthBytesPrefix + "sales.ledger.ledger_idx", 20 * MiB),
            (PgTargetScorer.BloatNamedGrowthBytesPrefix + "bare_index", MiB));
        Assert.Equal(["bare_index", "public.orders", "sales.ledger"], PgTargetScorer.BloatNamedObjects(index, parentTables: true).Order(StringComparer.Ordinal));
        Assert.True(PgTargetScorer.BloatNamedObjects(index, parentTables: true).Overlaps(PgTargetScorer.BloatNamedObjects(table)));

        var backlogSame = Backlog(5.0, "public.events");
        var backlogOther = Backlog(5.0, "public.customers");
        var backlogNameless = Backlog(5.0, null);
        Assert.True(PgTargetScorer.BloatAndBacklogShareATable(table, backlogSame));
        Assert.False(PgTargetScorer.BloatAndBacklogShareATable(table, backlogOther));
        /* No name to intersect on → co-presence, by design (the description says so). */
        Assert.True(PgTargetScorer.BloatAndBacklogShareATable(table, backlogNameless));
    }

    /* ── amplifiers ── */

    [Fact]
    public void TheBacklogAmplifier_FiresOnTheSameTable_NotOnADifferentOne_AndTheHoldAmplifierOnCoPresence()
    {
        var same = Table("public.orders", GiB, 100 * MiB, 500 * MiB);
        var backlog = Backlog(5.0, "public.orders");
        new FactScorer().ScoreAll([same, backlog]);
        Assert.Contains(same.AmplifierResults, a => a.Matched && a.Description.Contains("PG_AUTOVACUUM_BACKLOG co-fired", StringComparison.Ordinal));
        Assert.True(same.Severity > same.BaseSeverity);

        var other = Table("public.orders", GiB, 100 * MiB, 500 * MiB);
        var otherBacklog = Backlog(5.0, "public.customers");
        new FactScorer().ScoreAll([other, otherBacklog]);
        Assert.DoesNotContain(other.AmplifierResults, a => a.Matched && a.Description.Contains("PG_AUTOVACUUM_BACKLOG co-fired", StringComparison.Ordinal));
        Assert.Equal(other.BaseSeverity, other.Severity);

        var held = Table("public.orders", GiB, 100 * MiB, 500 * MiB);
        var hold = Xmin(60_000_000, 31, 41);
        new FactScorer().ScoreAll([held, hold]);
        Assert.Contains(held.AmplifierResults, a => a.Matched && a.Description.Contains("PG_XMIN_HOLD co-fired", StringComparison.Ordinal));

        /* Under the line, amplifiers never lift it: FactScorer skips amplifiers at base 0. */
        var quiet = Table("public.orders", GiB, 2 * GiB, 300 * MiB);
        var quietBacklog = Backlog(5.0, "public.orders");
        new FactScorer().ScoreAll([quiet, quietBacklog]);
        Assert.Equal(0.0, quiet.Severity);
    }

    [Fact]
    public void TheIndexAmplifier_FiresWhenTheTableTrendNamesItsParent()
    {
        var index = Index("public.orders.orders_pkey", 512 * MiB, 0, 300 * MiB, 5);
        var table = Table("public.orders", GiB, 100 * MiB, 500 * MiB);
        new FactScorer().ScoreAll([index, table]);
        Assert.Contains(index.AmplifierResults, a => a.Matched && a.Description.Contains("PG_BLOAT_TREND co-fired", StringComparison.Ordinal));

        var elsewhere = Index("public.orders.orders_pkey", 512 * MiB, 0, 300 * MiB, 5);
        var otherTable = Table("public.events", GiB, 100 * MiB, 500 * MiB);
        new FactScorer().ScoreAll([elsewhere, otherTable]);
        Assert.DoesNotContain(elsewhere.AmplifierResults, a => a.Matched && a.Description.Contains("PG_BLOAT_TREND co-fired", StringComparison.Ordinal));
    }

    /* ── the chain ── */

    [Fact]
    public void TrendAndBacklogOnTheSameTable_WalkOneStory_EitherOneLeading()
    {
        /* Trend leads: 500 MiB growth → 0.66 base × 1.3 = 0.86; backlog exactly at its line 0.5. */
        var trend = Table("public.orders", GiB, 100 * MiB, 500 * MiB);
        var backlog = Backlog(1.0, "public.orders");
        var facts = new List<Fact> { trend, backlog };
        new FactScorer().ScoreAll(facts);
        Assert.True(trend.Severity > backlog.Severity);
        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(new[] { PgTargetFactKeys.BloatTrend, PgTargetFactKeys.AutovacuumBacklog }, story.Path);

        /* Backlog leads: 40× its line → 1.0; trend just over the line 0.5 × 1.3 = 0.65. Still one story. */
        var trend2 = Table("public.orders", GiB, 512 * MiB, 256 * MiB);
        var backlog2 = Backlog(40.0, "public.orders");
        var facts2 = new List<Fact> { trend2, backlog2 };
        new FactScorer().ScoreAll(facts2);
        Assert.True(backlog2.Severity > trend2.Severity);
        var story2 = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts2));
        Assert.Equal(PgTargetFactKeys.AutovacuumBacklog, story2.RootFactKey);
        Assert.Contains(PgTargetFactKeys.BloatTrend, story2.Path);
    }

    [Fact]
    public void TrendAndBacklogOnDifferentTables_AreTwoStories()
    {
        var trend = Table("public.orders", GiB, 100 * MiB, 500 * MiB);
        var backlog = Backlog(5.0, "public.customers");
        var facts = new List<Fact> { trend, backlog };
        new FactScorer().ScoreAll(facts);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);
        Assert.Equal(2, stories.Count);
        Assert.All(stories, s => Assert.Single(s.Path));
    }

    [Fact]
    public void AnIndexTrend_ReachesItsTablesTrend_AndAloneRootsItsOwnStory()
    {
        var index = Index("public.orders.orders_pkey", 512 * MiB, 0, 900 * MiB, 5);   /* 0.92 × 1.3 = 1.19 leads */
        var table = Table("public.orders", GiB, 100 * MiB, 500 * MiB);                  /* 0.66 */
        var facts = new List<Fact> { index, table };
        new FactScorer().ScoreAll(facts);
        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(new[] { PgTargetFactKeys.IndexBloatTrend, PgTargetFactKeys.BloatTrend }, story.Path);

        var alone = new List<Fact> { Index("public.orders.orders_pkey", 512 * MiB, 0, 900 * MiB, 5) };
        new FactScorer().ScoreAll(alone);
        var solo = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(alone));
        Assert.Equal(new[] { PgTargetFactKeys.IndexBloatTrend }, solo.Path);
    }

    /* ── advice ── */

    [Fact]
    public void TheStaticBlocks_Exist_SayTrendNotSpot_AndCarryNoDdl()
    {
        foreach (var key in new[] { PgTargetFactKeys.BloatTrend, PgTargetFactKeys.IndexBloatTrend })
        {
            var block = PgTargetAdvice.Static(key);
            Assert.NotNull(block);
            Assert.Equal(block, FactAdvice.GetForFactKey(key));
            var text = block!.Headline + "\n" + block.Investigation + "\n" + block.Remediation;
            Assert.Contains("TREND", text, StringComparison.Ordinal);
            Assert.Contains("percentage", text, StringComparison.Ordinal);
            Assert.DoesNotContain("CREATE INDEX", text, StringComparison.OrdinalIgnoreCase);
            Assert.Null(block.RemediationTsql);
        }
    }

    [Fact]
    public void TheTableCard_StatesTheValues_NamesTheOthers_OrdersTheLevers_AndNeverArguesFromThePercentage()
    {
        var trend = Table("public.orders", GiB, 100 * MiB, 500 * MiB,
            (PgTargetScorer.BloatSpotPctKey, 86.0),
            (PgTargetScorer.BloatDeadTuplesKey, 1_250_000),
            (PgTargetScorer.BloatLiveTuplesKey, 5_000_000),
            (PgTargetScorer.BloatSpanHoursKey, 13 * 24 + 6),
            (PgTargetScorer.BloatObjectsSeenKey, 40),
            (PgTargetScorer.BloatObjectsEstimableKey, 38),
            (PgTargetScorer.BloatObjectsConsideredKey, 6),
            (PgTargetScorer.BloatObjectsOverLineKey, 1),
            (PgTargetScorer.BloatNamedGrowthBytesPrefix + "public.events", 300 * MiB),
            (PgTargetScorer.BloatNamedGrowthPctPrefix + "public.events", 40),
            (PgTargetScorer.BloatNamedDeadTuplesPrefix + "public.events", 20_000),
            (PgTargetScorer.BloatNamedGrowthBytesPrefix + "sales.ledger", 10 * MiB));
        var backlog = Backlog(5.0, "public.orders");
        var facts = new List<Fact> { trend, backlog };
        new FactScorer().ScoreAll(facts);
        var lookup = facts.ToDictionary(f => f.Key, StringComparer.Ordinal);

        var block = PgTargetAdvice.Compose(PgTargetFactKeys.BloatTrend, lookup)!;
        Assert.Equal("public.orders in appdb grew 500 MB of estimated bloat (500% of the earlier estimate) across 14 samples spanning 13.3 days", block.Headline);
        Assert.Contains("moved from 100 MB to 600 MB", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The heap measures 1 GB", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("1,250,000 dead tuples against 5,000,000 live", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The spot estimate reads 86% and was NOT graded", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("at least 256 MB AND at least 25%", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("Next by growth: public.events (300 MB, 40%, 20,000 dead tuples now); sales.ledger (10 MB)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("40 tables seen in the lookback, 38 with a usable estimate, 6 at or over the 64 MB heap floor, 1 over the line", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_AUTOVACUUM_BACKLOG co-fired on public.orders", block.Investigation, StringComparison.Ordinal);

        /* Lever order: vacuum side first, pg_repack second, VACUUM (FULL) last — each with its counter-objective. */
        var rem = block.Remediation;
        Assert.StartsWith("The trend is the finding; the percentage is not", rem, StringComparison.Ordinal);
        var vacuumSide = rem.IndexOf("PG_AUTOVACUUM_BACKLOG card", StringComparison.Ordinal);
        var repack = rem.IndexOf("pg_repack", StringComparison.Ordinal);
        var full = rem.IndexOf("VACUUM (FULL) is the last lever", StringComparison.Ordinal);
        Assert.True(vacuumSide >= 0 && repack > vacuumSide && full > repack, rem);
        Assert.Contains("ACCESS EXCLUSIVE lock", rem, StringComparison.Ordinal);
        Assert.Contains("table's size again", rem, StringComparison.Ordinal);
        Assert.Contains("Counter-objective", rem, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", block.Investigation + rem, StringComparison.OrdinalIgnoreCase);
        Assert.Null(block.RemediationTsql);

        /* A different-table backlog is named as such, and the vacuum-side lever becomes the reloption sentence. */
        var otherLookup = new Dictionary<string, Fact>(StringComparer.Ordinal)
        {
            [PgTargetFactKeys.BloatTrend] = trend,
            [PgTargetFactKeys.AutovacuumBacklog] = Scored(Backlog(5.0, "public.customers")),
        };
        var other = PgTargetAdvice.Compose(PgTargetFactKeys.BloatTrend, otherLookup)!;
        Assert.Contains("PG_AUTOVACUUM_BACKLOG fired on public.customers — a different table", other.Investigation, StringComparison.Ordinal);
        Assert.Contains("autovacuum_vacuum_scale_factor", other.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheEarlierZeroCase_StatesNoPercentage_AndTheUnderLineCase_SaysContext()
    {
        var fromZero = Scored(Table("public.orders", GiB, 0, 300 * MiB));
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.BloatTrend, Lookup(fromZero))!;
        Assert.Contains("from an earlier estimate of zero — no percentage to state", block.Headline, StringComparison.Ordinal);
        Assert.Contains("This table crossed it.", block.Investigation, StringComparison.Ordinal);

        var under = Scored(Table("public.orders", GiB, 2 * GiB, 300 * MiB));
        var context = PgTargetAdvice.Compose(PgTargetFactKeys.BloatTrend, Lookup(under))!;
        Assert.Contains("under the measured line", context.Headline, StringComparison.Ordinal);
        Assert.Contains("the fact is context, not a finding", context.Investigation, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(PgTargetFactKeys.BloatTrend, PgTargetScorer.BloatReasonEstimateUnavailable, "estimate_unavailable on every row", "pg_read_all_data")]
    [InlineData(PgTargetFactKeys.BloatTrend, PgTargetScorer.BloatReasonBelowSizeFloor, "No table at or over the 64 MB size floor", "small objects are not graded")]
    [InlineData(PgTargetFactKeys.IndexBloatTrend, PgTargetScorer.BloatReasonPgstattupleUnavailable, "pgstattuple is not installed", "install the pgstattuple extension")]
    [InlineData(PgTargetFactKeys.IndexBloatTrend, PgTargetScorer.BloatReasonAllSkipped, "every index was skipped with a stated reason", "pgstatindex measures it exactly")]
    [InlineData(PgTargetFactKeys.IndexBloatTrend, PgTargetScorer.BloatReasonInsufficientSamples, "none has 3 daily samples", "wait for the collector")]
    [InlineData(PgTargetFactKeys.IndexBloatTrend, PgTargetScorer.BloatReasonBelowSizeFloor, "No index at or over the 64 MB size floor", "small objects are not graded")]
    public void TheWithheldCards_NameTheGate_AndNeverReadAsNoBloat(string key, int reason, string headlineFragment, string remediationFragment)
    {
        var fact = Withheld(key, reason, seen: 12, estimable: reason is PgTargetScorer.BloatReasonEstimateUnavailable or PgTargetScorer.BloatReasonPgstattupleUnavailable or PgTargetScorer.BloatReasonAllSkipped ? 0 : 12);
        if (key == PgTargetFactKeys.IndexBloatTrend)
        {
            fact.Metadata[PgTargetScorer.IndexBloatSkippedShareKey] = 0.75;
            fact.Metadata[PgTargetScorer.IndexBloatSkipReasonsKey] = 3;
            fact.Metadata[PgTargetScorer.IndexBloatPgstattupleAvailableKey] = reason == PgTargetScorer.BloatReasonAllSkipped ? 1 : 0;
        }
        new FactScorer().ScoreAll([fact]);
        var block = PgTargetAdvice.Compose(key, Lookup(fact))!;
        Assert.Contains(headlineFragment, block.Headline, StringComparison.Ordinal);
        Assert.Contains("a withheld estimate never reads as zero bloat", block.Investigation, StringComparison.Ordinal);
        Assert.Contains(remediationFragment, block.Remediation, StringComparison.Ordinal);
        Assert.EndsWith("a spot percentage — when one becomes available — is not.", block.Remediation, StringComparison.Ordinal);
        if (reason is PgTargetScorer.BloatReasonPgstattupleUnavailable or PgTargetScorer.BloatReasonAllSkipped)
            Assert.Contains("75% of the indexes seen were skipped at their latest sample — a majority, across 3 distinct reasons", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", block.Investigation + block.Remediation, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheIndexCard_StatesTheValues_PointsAtUsageFirst_AndNamesTheConcurrentRebuildsCost()
    {
        var index = Index("public.orders.orders_pkey", 512 * MiB, 100 * MiB, 640 * MiB, 9,
            (PgTargetScorer.BloatSpotPctKey, 61.5),
            (PgTargetScorer.BloatSpanHoursKey, 8 * 24),
            (PgTargetScorer.BloatObjectsSeenKey, 120),
            (PgTargetScorer.BloatObjectsEstimableKey, 30),
            (PgTargetScorer.BloatObjectsConsideredKey, 4),
            (PgTargetScorer.BloatObjectsOverLineKey, 1),
            (PgTargetScorer.IndexBloatSkippedShareKey, 0.75),
            (PgTargetScorer.IndexBloatSkipReasonsKey, 4),
            (PgTargetScorer.IndexBloatPgstattupleAvailableKey, 1));
        var table = Table("public.orders", GiB, 100 * MiB, 500 * MiB);
        var facts = new List<Fact> { index, table };
        new FactScorer().ScoreAll(facts);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.IndexBloatTrend, facts.ToDictionary(f => f.Key, StringComparer.Ordinal))!;

        Assert.Equal("public.orders.orders_pkey in appdb grew 640 MB of estimated reclaimable space (640% of the earlier estimate) across 9 daily samples spanning 8 days", block.Headline);
        Assert.Contains("The index measures 512 MB", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The spot estimate reads 61.5% and was NOT graded", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("needs 3 daily samples", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("75% of the indexes seen were skipped at their latest sample — a majority, across 4 distinct reasons", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_BLOAT_TREND co-fired on this index's table", block.Investigation, StringComparison.Ordinal);

        var rem = block.Remediation;
        var usage = rem.IndexOf("get_pg_index_usage", StringComparison.Ordinal);
        var tableFirst = rem.IndexOf("PG_BLOAT_TREND card", StringComparison.Ordinal);
        var rebuild = rem.IndexOf("REINDEX CONCURRENTLY", StringComparison.Ordinal);
        Assert.True(usage >= 0 && tableFirst > usage && rebuild > tableFirst, rem);
        Assert.Contains("second copy of the index on disk", rem, StringComparison.Ordinal);
        Assert.Contains("holds the xmin horizon", rem, StringComparison.Ordinal);
        /* D8: the lever is NAMED; no statement against any object is emitted. */
        Assert.DoesNotContain("REINDEX INDEX", rem, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", block.Investigation + rem, StringComparison.OrdinalIgnoreCase);
        Assert.Null(block.RemediationTsql);
    }

    /* ── the collector SQL, by its own text ── */

    [Fact]
    public void TheTableRead_ExcludesUnavailableEstimates_FloorsOnHeapBytes_AndBindsTheScorersLine()
    {
        var sql = PgTargetFactCollector.PgTargetTableBloatTrendSql;
        Assert.Contains("FROM pg_table_bloat_stats", sql, StringComparison.Ordinal);
        /* NULL reads as unavailable, never as available. */
        Assert.Contains("COALESCE(estimate_unavailable, TRUE) AS unavailable", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE NOT r.unavailable", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE l.heap_bytes >= $4", sql, StringComparison.Ordinal);
        /* The same two-arm line the scorer grades, bound not retyped: bytes AND fraction of the earlier estimate. */
        Assert.Contains("l.bloat_bytes_estimate - e.first_bloat_bytes >= $5", sql, StringComparison.Ordinal);
        Assert.Contains(">= e.first_bloat_bytes * $6", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $7", sql, StringComparison.Ordinal);
        /* Never ranked or graded on the percentage. */
        Assert.DoesNotContain("ORDER BY bloat_pct", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("bloat_pct_estimate >=", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("bloat_pct_estimate >", sql, StringComparison.Ordinal);
        /* The population row survives an empty top set. */
        Assert.Contains("LEFT JOIN ranked AS t ON TRUE", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIndexRead_ExcludesSkippedRows_FloorsOnIndexBytes_RanksOnReclaimableBytes_AndBindsTheSampleMinimum()
    {
        var sql = PgTargetFactCollector.PgTargetIndexBloatTrendSql;
        Assert.Contains("FROM pg_index_bloat", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE r.skipped_reason IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE l.index_bytes >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("l.est_reclaimable_bytes - e.first_reclaimable_bytes AS growth_bytes", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE o.samples >= $8", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT skipped_reason)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("est_bloat_pct >=", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY est_bloat_pct", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN ranked AS t ON TRUE", sql, StringComparison.Ordinal);
    }

    /* ── builders ── */

    private static Fact Table(string objectName, long heapBytes, long earlier, long growth, params (string Key, double Value)[] extra)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.BloatSource,
            Key = PgTargetFactKeys.BloatTrend,
            Value = growth,
            ServerId = 1,
            DatabaseName = "appdb",
            ObjectName = objectName,
            Metadata =
            {
                [PgTargetScorer.BloatEstimateAvailableKey] = 1,
                [PgTargetScorer.BloatGrowthBytesKey] = growth,
                [PgTargetScorer.BloatEarlierBytesKey] = earlier,
                [PgTargetScorer.BloatLatestBytesKey] = earlier + growth,
                [PgTargetScorer.BloatObjectBytesKey] = heapBytes,
                [PgTargetScorer.BloatSamplesKey] = 14,
                [PgTargetScorer.BloatLookbackDaysKey] = PgTargetScorer.BloatLookbackDays,
                [PgTargetScorer.BloatNamedGrowthBytesPrefix + objectName] = growth,
            },
        };
        if (earlier > 0)
        {
            fact.Metadata[PgTargetScorer.BloatGrowthPctKey] = 100.0 * growth / earlier;
            fact.Metadata[PgTargetScorer.BloatGrowthPctComputableKey] = 1;
            fact.Metadata[PgTargetScorer.BloatNamedGrowthPctPrefix + objectName] = 100.0 * growth / earlier;
        }
        else
        {
            fact.Metadata[PgTargetScorer.BloatGrowthPctComputableKey] = 0;
        }
        foreach (var (k, v) in extra) fact.Metadata[k] = v;
        return fact;
    }

    private static Fact Index(string objectName, long indexBytes, long earlier, long growth, long samples, params (string Key, double Value)[] extra)
    {
        var fact = Table(objectName, indexBytes, earlier, growth, extra);
        fact.Key = PgTargetFactKeys.IndexBloatTrend;
        fact.Metadata[PgTargetScorer.BloatSamplesKey] = samples;
        return fact;
    }

    private static Fact Withheld(string key, int reason, int seen, int estimable) => new()
    {
        Source = PgTargetSources.BloatSource,
        Key = key,
        Value = 0,
        ServerId = 1,
        Metadata =
        {
            [PgTargetScorer.BloatEstimateAvailableKey] = 0,
            [PgTargetScorer.BloatUnavailableReasonKey] = reason,
            [PgTargetScorer.BloatObjectsSeenKey] = seen,
            [PgTargetScorer.BloatObjectsEstimableKey] = estimable,
            [PgTargetScorer.BloatLookbackDaysKey] = PgTargetScorer.BloatLookbackDays,
            [PgTargetScorer.BloatSamplesInLookbackKey] = 2,
        },
    };

    private static Fact Backlog(double ratio, string? objectName) => new()
    {
        Source = PgTargetSources.VacuumSource,
        Key = PgTargetFactKeys.AutovacuumBacklog,
        Value = ratio,
        ServerId = 1,
        DatabaseName = "appdb",
        ObjectName = objectName,
        Metadata =
        {
            [PgTargetScorer.BacklogRatioKey] = ratio,
            [PgTargetScorer.BacklogTrailingSamplesKey] = 4,
            [PgTargetScorer.BacklogSamplesInWindowKey] = 5,
            [PgTargetScorer.BacklogTablesKey] = 1,
        },
    };

    private static Fact Xmin(long age, int held, int total) => new()
    {
        Source = PgTargetSources.VacuumSource,
        Key = PgTargetFactKeys.XminHold,
        Value = age,
        ServerId = 1,
        ObjectName = "session:4242",
        Metadata =
        {
            [PgTargetScorer.XminAgeKey] = age,
            [PgTargetScorer.XminHolderSourceKey] = 1,
            [PgTargetScorer.XminObservationsTotalKey] = total,
            [PgTargetScorer.XminObservationsHeldKey] = held,
            [PgTargetScorer.XminObservationsAboveThresholdKey] = total,
            [PgTargetScorer.XminMinutesSinceLastHolderKey] = 0,
        },
    };

    private static Fact Scored(Fact fact)
    {
        new FactScorer().ScoreAll([fact]);
        return fact;
    }

    private static Dictionary<string, Fact> Lookup(params Fact[] facts) =>
        facts.ToDictionary(f => f.Key, StringComparer.Ordinal);
}
