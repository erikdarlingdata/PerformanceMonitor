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
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The object-growth family of the PostgreSQL-target analysis pass (#3691 lane 38): <c>PG_DATABASE_GROWTH</c> over
/// <c>pg_database_size_stats</c> (V136) and <c>ANOMALY_PG_DATABASE_GROWTH</c> over the instance total's per-day growth
/// rate. Pure-logic pins here (the constants by value and lineage, the two-arm grade, the NULL-exclusion contract the
/// collector's SQL carries, the withheld shapes, the amplifiers, the bloat edge on a shared database, the advice's
/// value-stated headline and its stated limits, the routing and census rows this lane moved); the planted fortnight
/// through the real collector and <c>analyze_server</c> is <see cref="PgTargetGrowthLiveTests"/>.
/// </summary>
public sealed class PgTargetGrowthTests
{
    private const long MiB = 1024L * 1024;
    private const long GiB = 1024L * MiB;

    /* ── the constants ── */

    [Fact]
    public void TheBars_AreTheBriefsValues_AllUnmeasured_AndNoneIsASqlServerConstant()
    {
        Assert.Equal(14, PgTargetScorer.GrowthLookbackDays);
        Assert.Equal(GiB, PgTargetScorer.GrowthConcerningBytes);
        Assert.Equal(0.10, PgTargetScorer.GrowthConcerningFraction);
        Assert.Equal(10 * GiB, PgTargetScorer.GrowthCriticalBytes);
        Assert.Equal(0.25, PgTargetScorer.GrowthCriticalFraction);
        Assert.Equal(3, PgTargetScorer.GrowthMinimumSamples);
        Assert.Equal(3, PgTargetScorer.GrowthTopDatabases);
        Assert.Equal(256.0 * MiB, AnomalyThresholds.PgDatabaseGrowthFloorBytesPerDay);
        Assert.Equal(2.0 * GiB, AnomalyThresholds.PgDatabaseGrowthFallbackBytesPerDay);
        Assert.True(AnomalyThresholds.PgDatabaseGrowthFallbackBytesPerDay > AnomalyThresholds.PgDatabaseGrowthFloorBytesPerDay);

        /* Every bar says unmeasured in the block above it, and names the table to calibrate against. */
        var scorer = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Growth.cs").Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        foreach (var name in new[] { "GrowthLookbackDays", "GrowthConcerningBytes", "GrowthConcerningFraction", "GrowthCriticalBytes", "GrowthCriticalFraction", "GrowthMinimumSamples", "GrowthBloatCoFireBoost", "GrowthAnomalyCoFireBoost" })
        {
            var at = Array.FindIndex(scorer, l => l.Contains($" {name} = ", StringComparison.Ordinal));
            Assert.True(at > 0, name);
            var block = string.Join('\n', scorer[Math.Max(0, at - 12)..at]);
            Assert.Contains("unmeasured", block, StringComparison.Ordinal);
            Assert.DoesNotContain("<b>measured</b>", block, StringComparison.Ordinal);
        }
        Assert.DoesNotContain("[\"threshold_lineage\"] = 1", string.Join('\n', scorer), StringComparison.Ordinal);

        var thresholds = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "Baselines", "AnomalyThresholds.cs");
        foreach (var name in new[] { "PgDatabaseGrowthFloorBytesPerDay", "PgDatabaseGrowthFallbackBytesPerDay" })
        {
            var at = thresholds.IndexOf("public const double " + name, StringComparison.Ordinal);
            Assert.True(at > 0);
            Assert.Contains("unmeasured: chosen, not measured", thresholds[Math.Max(0, at - 1600)..at], StringComparison.Ordinal);
            Assert.Contains("pg_database_size_stats", thresholds[Math.Max(0, at - 1600)..at], StringComparison.Ordinal);
        }
    }

    /* ── the grade ── */

    [Theory]
    [InlineData(1.0, 10.0, 0.5)]              /* both arms exactly at the line: 0.5 */
    [InlineData(10.0, 25.0, 1.0)]             /* both at critical: 1.0 */
    [InlineData(10.0, 12.0, 0.5 + 0.5 * 2.0 / 15.0)]   /* bytes critical, fraction barely over: the WEAKER arm decides */
    [InlineData(1.5, 25.0, 0.5 + 0.5 * 0.5 / 9.0)]     /* fraction critical, bytes barely over: the bytes ramp decides */
    [InlineData(5.5, 17.5, 0.75)]             /* both mid-ramp, equal: 0.75 */
    public void TheGrade_IsTheWeakerOfTwoRamps_BytesAndFractionOfTheEarliestSize(double growthGiB, double pct, double expected)
    {
        Assert.Equal(expected, PgTargetScorer.GrowthGrade(growthGiB * GiB, pct), precision: 9);
    }

    [Fact]
    public void TheGrade_IsZeroUnderEitherLine_AndAnEarliestZeroPassesTheFractionArm()
    {
        Assert.Equal(0.0, PgTargetScorer.GrowthGrade(GiB - 1, 50.0));       /* 999.99 MiB: under the bytes line however large the fraction */
        Assert.Equal(0.0, PgTargetScorer.GrowthGrade(50 * GiB, 9.99));      /* a warehouse's routine week: under the fraction line however many bytes */
        Assert.Equal(0.0, PgTargetScorer.GrowthGrade(0, 100.0));
        Assert.Equal(0.0, PgTargetScorer.GrowthGrade(-GiB, 100.0));         /* a shrink is never a growth finding */
        /* Created inside the lookback (earliest size zero → no percentage): the bytes ramp decides alone. */
        Assert.Equal(0.5, PgTargetScorer.GrowthGrade(GiB, null), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.GrowthGrade(10 * GiB, null), precision: 9);
    }

    [Fact]
    public void TheFact_GradesTheBestNamedDatabaseOrTheInstanceTotal_StampsTheSubject_AndAlwaysLineageZero()
    {
        /* appdb 2 GiB / 20 %: bytes 0.5556, pct 0.8333 → 0.5556. reporting 1.2 GiB / 5 %: under the fraction line.
           The total 3.2 GiB / 4 %: under the line. The database leads. */
        var fact = Growth(
            ("appdb", 10 * GiB, 12 * GiB), ("reporting", 24 * GiB, 25200 * MiB), ("tiny", 100 * MiB, 120 * MiB),
            total: (80 * GiB, 83200 * MiB));
        var expectedBytes = 0.5 + 0.5 * (2.0 * GiB - GiB) / (9.0 * GiB);
        Assert.Equal(expectedBytes, PgTargetScorer.ScoreBase(fact), precision: 9);
        Assert.Equal(PgTargetScorer.GrowthSubjectDatabase, fact.Metadata[PgTargetScorer.GrowthGradedSubjectKey]);
        Assert.Equal(0, fact.Metadata["threshold_lineage"]);

        /* The instance leads: three databases each 900 MiB / 15 % (under the bytes line alone), the total 2.7 GiB / 15 %. */
        var instance = Growth(
            ("a", 6000 * MiB, 6900 * MiB), ("b", 6000 * MiB, 6900 * MiB), ("c", 6000 * MiB, 6900 * MiB),
            total: (18000 * MiB, 20700 * MiB));
        var totalBytes = 0.5 + 0.5 * (2700.0 * MiB - GiB) / (9.0 * GiB);
        var totalPct = 0.5 + 0.5 * (15.0 - 10.0) / 15.0;
        Assert.Equal(Math.Min(totalBytes, totalPct), PgTargetScorer.ScoreBase(instance), precision: 9);
        Assert.Equal(PgTargetScorer.GrowthSubjectInstance, instance.Metadata[PgTargetScorer.GrowthGradedSubjectKey]);

        /* Nothing over the line: context, subject 0, lineage still stamped. */
        var quiet = Growth(("appdb", 10 * GiB, 10 * GiB + 500 * MiB), total: (20 * GiB, 20 * GiB + 500 * MiB));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(quiet));
        Assert.Equal(0, quiet.Metadata[PgTargetScorer.GrowthGradedSubjectKey]);
        Assert.Equal(0, quiet.Metadata["threshold_lineage"]);

        /* No total (an unsized database NULLs it): the databases alone decide, and nothing is summed to stand in. */
        var noTotal = Growth(("appdb", 10 * GiB, 12 * GiB), total: null);
        Assert.Equal(expectedBytes, PgTargetScorer.ScoreBase(noTotal), precision: 9);
        Assert.Equal(0, noTotal.Metadata[PgTargetScorer.GrowthTotalAvailableKey]);
    }

    [Fact]
    public void TheWithheldShapes_ScoreZero_WithTheirReason_AndLineageZero()
    {
        var insufficient = Withheld(PgTargetScorer.GrowthReasonInsufficientSamples, seen: 3, unsized: 0);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(insufficient));
        Assert.Equal(0, insufficient.Metadata["threshold_lineage"]);
        var allUnsized = Withheld(PgTargetScorer.GrowthReasonAllUnsized, seen: 3, unsized: 3);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(allUnsized));
        /* Amplifiers never lift a withheld fact: FactScorer skips them at base 0. */
        var bloat = Bloat("appdb", "public.orders");
        new FactScorer().ScoreAll([insufficient, bloat]);
        Assert.Equal(0.0, insufficient.Severity);
    }

    /* ── the named set and the same-database predicate ── */

    [Fact]
    public void TheNamedDatabases_AreTheWorstPlusEveryNamedKey_AndTheBloatPredicateIntersectsOnDatabaseName()
    {
        var growth = Growth(("appdb", 10 * GiB, 12 * GiB), ("reporting", 24 * GiB, 26 * GiB), total: null);
        Assert.Equal(["appdb", "reporting"], PgTargetScorer.GrowthNamedDatabases(growth).Order(StringComparer.Ordinal).ToList());

        Assert.True(PgTargetScorer.GrowthAndBloatShareADatabase(growth, Bloat("appdb", "public.orders")));
        Assert.True(PgTargetScorer.GrowthAndBloatShareADatabase(growth, Bloat("reporting", "public.events")));
        Assert.False(PgTargetScorer.GrowthAndBloatShareADatabase(growth, Bloat("sandbox", "public.scratch")));
        /* No database name to intersect on → co-presence, by design (the description says so). */
        Assert.True(PgTargetScorer.GrowthAndBloatShareADatabase(growth, Bloat(null, "public.orders")));
    }

    /* ── amplifiers ── */

    [Fact]
    public void TheBloatAmplifier_FiresOnTheSameDatabase_NotOnAnother_AndTheAnomalyAmplifierOnAFiredDeviation()
    {
        var same = Growth(("appdb", 10 * GiB, 12 * GiB), total: null);
        var bloat = Bloat("appdb", "public.orders");
        new FactScorer().ScoreAll([same, bloat]);
        Assert.True(bloat.BaseSeverity > 0, "the bloat fixture must grade for the co-fire to be tested");
        var matched = Assert.Single(same.AmplifierResults, a => a.Description.Contains("PG_BLOAT_TREND co-fired", StringComparison.Ordinal));
        Assert.True(matched.Matched);
        Assert.Equal(same.BaseSeverity * (1 + PgTargetScorer.GrowthBloatCoFireBoost), same.Severity, precision: 9);

        var other = Growth(("appdb", 10 * GiB, 12 * GiB), total: null);
        var otherBloat = Bloat("sandbox", "public.scratch");
        new FactScorer().ScoreAll([other, otherBloat]);
        Assert.DoesNotContain(other.AmplifierResults, a => a.Matched && a.Description.Contains("PG_BLOAT_TREND co-fired", StringComparison.Ordinal));
        Assert.Equal(other.BaseSeverity, other.Severity);

        var accelerating = Growth(("appdb", 10 * GiB, 12 * GiB), total: null);
        var anomaly = Anomaly(3 * GiB, sigma: 6.0, lowQuality: false);
        new FactScorer().ScoreAll([accelerating, anomaly]);
        Assert.True(anomaly.BaseSeverity > 0);
        Assert.Contains(accelerating.AmplifierResults, a => a.Matched && a.Description.Contains("ANOMALY_PG_DATABASE_GROWTH co-fired", StringComparison.Ordinal));

        /* Under the line, amplifiers never lift it. */
        var quiet = Growth(("appdb", 10 * GiB, 10 * GiB + 500 * MiB), total: null);
        var quietBloat = Bloat("appdb", "public.orders");
        new FactScorer().ScoreAll([quiet, quietBloat]);
        Assert.Equal(0.0, quiet.Severity);

        /* Every predicate reads BaseSeverity, never Severity (the vacuum family's emission-order lesson). */
        var source = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Growth.cs"));
        Assert.DoesNotContain(".Severity > 0", source, StringComparison.Ordinal);
        Assert.Contains(".BaseSeverity > 0", source, StringComparison.Ordinal);
    }

    /* ── the chain ── */

    [Fact]
    public void GrowthAndBloatOnTheSameDatabase_WalkOneStory_GrowthLeading_AndOnAnotherDatabaseTwo()
    {
        /* Growth leads: 5 GiB / 50 % → bytes 0.72, pct 1.0 → 0.72 × 1.3; the bloat fixture sits at 0.59 × 1.0. */
        var growth = Growth(("appdb", 10 * GiB, 15 * GiB), total: null);
        var bloat = Bloat("appdb", "public.orders");
        var facts = new List<Fact> { growth, bloat };
        new FactScorer().ScoreAll(facts);
        Assert.True(growth.Severity > bloat.Severity);
        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(new[] { PgTargetFactKeys.DatabaseGrowth, PgTargetFactKeys.BloatTrend }, story.Path);

        /* Another database: two stories, no shared cause. */
        var elsewhere = Growth(("appdb", 10 * GiB, 15 * GiB), total: null);
        var otherBloat = Bloat("sandbox", "public.scratch");
        var facts2 = new List<Fact> { elsewhere, otherBloat };
        new FactScorer().ScoreAll(facts2);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts2);
        Assert.Equal(2, stories.Count);
        Assert.All(stories, s => Assert.Single(s.Path));

        /* The edges, by shape: growth → bloat and anomaly → growth; nothing else leaves the family, and no edge
           comes back from bloat to growth (the class summary says why). */
        var graph = new PgTargetRelationshipGraph();
        var fromGrowth = graph.GetAllEdges(PgTargetFactKeys.DatabaseGrowth);
        Assert.Equal([PgTargetFactKeys.BloatTrend], fromGrowth.Select(e => e.Destination).ToList());
        Assert.All(fromGrowth, e => Assert.Equal("object_growth", e.Category));
        var fromAnomaly = graph.GetAllEdges(PgTargetFactKeys.AnomalyDatabaseGrowth);
        Assert.Equal([PgTargetFactKeys.DatabaseGrowth], fromAnomaly.Select(e => e.Destination).ToList());
        Assert.DoesNotContain(graph.GetAllEdges(PgTargetFactKeys.BloatTrend), e => e.Destination == PgTargetFactKeys.DatabaseGrowth);
    }

    [Fact]
    public void TheAnomaly_FoldsOntoTheTrend_AndWalksIntoItsStoryWhenItOutranks()
    {
        Assert.Equal([PgTargetFactKeys.DatabaseGrowth], PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyDatabaseGrowth]);
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyDatabaseGrowth));
        Assert.False(PgTargetScorer.IsPgRatioAnomalyKey(PgTargetFactKeys.AnomalyDatabaseGrowth));

        var growth = Growth(("appdb", 10 * GiB, 11 * GiB + 100 * MiB), total: null);   /* just over the line: 0.5-ish */
        var anomaly = Anomaly(3 * GiB, sigma: 8.0, lowQuality: false);
        var facts = new List<Fact> { growth, anomaly };
        new FactScorer().ScoreAll(facts);
        Assert.True(anomaly.Severity > growth.Severity, $"{anomaly.Severity} vs {growth.Severity}");
        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(new[] { PgTargetFactKeys.AnomalyDatabaseGrowth, PgTargetFactKeys.DatabaseGrowth }, story.Path);
    }

    /* ── advice ── */

    [Fact]
    public void TheStaticBlocks_ExistThroughEverySharedEntryPoint_AndNeverTheSqlServerComposer()
    {
        Assert.NotNull(PgTargetAdvice.Static(PgTargetFactKeys.DatabaseGrowth));
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.DatabaseGrowth), FactAdvice.GetForFactKey(PgTargetFactKeys.DatabaseGrowth));
        var anomaly = FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyDatabaseGrowth);
        Assert.NotNull(anomaly);
        Assert.Equal(PgTargetAdvice.Static(PgTargetFactKeys.AnomalyDatabaseGrowth), anomaly);
        Assert.DoesNotContain("Anomalous spike", anomaly!.Headline, StringComparison.Ordinal);
        Assert.Contains("hour-only (or flat) tier for months", anomaly.Investigation, StringComparison.Ordinal);
        /* D8 / the index-evidence discipline: no DDL anywhere in the family's prose. */
        foreach (var block in new[] { PgTargetAdvice.Static(PgTargetFactKeys.DatabaseGrowth)!, anomaly })
        {
            Assert.Null(block.RemediationTsql);
            Assert.DoesNotContain("CREATE INDEX", block.Remediation, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void TheComposedCard_IsValueStated_NamesTheSlopeAndTheStraightLineDoubling_TheBloatQuestionFirst_AndDiskFreeAsNotCollected()
    {
        /* appdb 100 → 138 GB over 14 days (335 h at the ends): 38 GB, 38 %, 2.72 GB/day, doubles in ~50.8 days. */
        var growth = Growth(("appdb", 100 * GiB, 138 * GiB), ("reporting", 24 * GiB, 25 * GiB), total: (200 * GiB, 240 * GiB), spanHours: 335);
        new FactScorer().ScoreAll([growth]);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.DatabaseGrowth, Lookup(growth))!;

        Assert.Equal("appdb grew 38 GB (38%) in 14 days — 2.7 GB/day; at that slope it doubles in ~51 days", block.Headline);
        Assert.Contains("appdb measured 100 GB at its earliest sample", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("and 138 GB at its latest (336 hourly samples, 14 days apart at the ends)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("a straight-line extrapolation of the lookback's slope, not a forecast", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The instance total moved from 200 GB to 240 GB", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The line is chosen, not measured", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("threshold_lineage = 0 on this fact", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("This database crossed it.", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("Next by growth: reporting (1 GB, 4.2%)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("Population: 2 databases seen in the lookback, 0 unsized, 2 with at least 3 samples, 1 over the line.", block.Investigation, StringComparison.Ordinal);

        Assert.StartsWith("Ask the bloat question first. ", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_table_bloat for appdb shows whether the bytes are live rows or dead ones", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("retention or archival of the oldest rows", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("Partitioning the largest time-keyed tables by range", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("Disk free space is NOT collected for a PostgreSQL target, so this card cannot say when the volume fills: bring the volume's headroom to the 2.7 GB/day figure yourself.", block.Remediation, StringComparison.Ordinal);
        /* Every lever names its counter-objective. */
        Assert.Equal(2, Regex.Matches(block.Remediation, "counter-objective").Count);
        Assert.Null(block.RemediationTsql);
    }

    [Fact]
    public void TheComposedCard_StatesTheUnsizedDatabases_WhenTheTotalIsMissing_AndTheBloatCoFireByName()
    {
        var growth = Growth(("appdb", 10 * GiB, 12 * GiB), total: null, unsized: 1);
        var bloat = Bloat("appdb", "public.orders");
        new FactScorer().ScoreAll([growth, bloat]);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.DatabaseGrowth, Lookup(growth, bloat))!;
        Assert.Contains("The instance total could not be trended: 1 database could not be sized by the monitoring role", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("a sum over the databases this role can see is not the instance total. Their growth is unknown, not zero.", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_BLOAT_TREND co-fired on public.orders in appdb: part of this growth is dead space", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("Work the PG_BLOAT_TREND card for public.orders before buying storage for this", block.Remediation, StringComparison.Ordinal);

        /* Under the line: the headline says so and the card is context. */
        var quiet = Growth(("appdb", 10 * GiB, 10 * GiB + 500 * MiB), total: null);
        new FactScorer().ScoreAll([quiet]);
        var quietBlock = PgTargetAdvice.Compose(PgTargetFactKeys.DatabaseGrowth, Lookup(quiet))!;
        Assert.Equal("appdb is the fastest-growing database at 500 MB (4.9%) over 14 days — under the line", quietBlock.Headline);
        Assert.Contains("Nothing crossed it; the fact is context, not a finding.", quietBlock.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWithheldCards_SayWhichGateWithheldIt_AndNeverReadAsNoGrowth()
    {
        var insufficient = Withheld(PgTargetScorer.GrowthReasonInsufficientSamples, seen: 3, unsized: 0);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.DatabaseGrowth, Lookup(insufficient))!;
        Assert.Equal("Database growth has sized databases, but none has 3 hourly samples in the lookback yet", block.Headline);
        Assert.Contains("a withheld trend never reads as zero growth", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("threshold_lineage = 0 on this fact", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("Nothing to do but wait for the collector", block.Remediation, StringComparison.Ordinal);

        var unsized = Withheld(PgTargetScorer.GrowthReasonAllUnsized, seen: 2, unsized: 2);
        var unsizedBlock = PgTargetAdvice.Compose(PgTargetFactKeys.DatabaseGrowth, Lookup(unsized))!;
        Assert.Equal("No database's size could be read in the lookback — the monitoring role may size none of them", unsizedBlock.Headline);
        Assert.Contains("pg_read_all_stats", unsizedBlock.Remediation, StringComparison.Ordinal);
        Assert.EndsWith("The trend is the finding; a spot size — which every screen already shows — is not.", unsizedBlock.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAnomalyCard_StatesTheRate_TheThinBaseline_AndTheTrendBesideIt()
    {
        var anomaly = Anomaly(3 * GiB, sigma: 6.0, lowQuality: false);
        anomaly.Metadata["mean_bytes_per_day"] = 2.5 * GiB;
        anomaly.Metadata["baseline_tier"] = 1;
        var growth = Growth(("appdb", 10 * GiB, 15 * GiB), total: null);
        new FactScorer().ScoreAll([growth, anomaly]);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyDatabaseGrowth, Lookup(anomaly, growth))!;
        Assert.Equal("Instance growth spiked to 3 GB a day — 6σ above its baseline for this time of week", block.Headline);
        Assert.Contains("The window's mean rate was 2.5 GB a day, over 3 hourly samples.", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("this baseline is thin by construction (about four samples per hour-of-week bucket over 30 days) and will lean on its magnitude floor of 256 MB a day for months — it graded at tier 1 (0 = full hour-of-week, 1 = hour only, 2 = flat).", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_DATABASE_GROWTH names appdb at 5 GB over the fortnight — over the line, so this rate is a trend accelerating.", block.Investigation, StringComparison.Ordinal);

        /* Low-quality bucket: the first-occurrence rendering, no sigma. */
        var young = Anomaly(3 * GiB, sigma: 0, lowQuality: true);
        var youngBlock = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyDatabaseGrowth, Lookup(young))!;
        Assert.Equal("Instance growth reached 3 GB a day — first occurrence, no baseline yet for this time of week", youngBlock.Headline);
        Assert.DoesNotContain("σ", youngBlock.Headline, StringComparison.Ordinal);
    }

    /* ── the SQL, by text ── */

    [Fact]
    public void TheTrendRead_ExcludesNullSizes_CountsThem_TrendsTheTotalNeverASum_BindsTheLine_AndReadsOnlyTheSizeTable()
    {
        var sql = PgTargetFactCollector.PgTargetDatabaseGrowthSql;
        Assert.Contains("FROM pg_database_size_stats", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, @"\bFROM\s+pg_\w+"));
        Assert.DoesNotContain("SUM(", sql, StringComparison.Ordinal);                 /* never a sum standing in for the total */
        Assert.Contains("WHERE r.size_bytes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE size_bytes IS NOT NULL) AS databases_unsized", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE total_bytes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (collection_time) collection_time, total_bytes", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE l.samples >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE l.size_bytes - e.first_bytes >= $5", sql, StringComparison.Ordinal);
        Assert.Contains(">= e.first_bytes * $6", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $7", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN ranked AS g ON TRUE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("aurora_stat_", sql, StringComparison.Ordinal);
        /* The collector binds the scorer's OWN constants, so the count and the grade cannot drift. */
        var collector = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Growth.cs");
        Assert.Contains("cmd.Parameters.AddWithValue(PgTargetScorer.GrowthMinimumSamples);", collector, StringComparison.Ordinal);
        Assert.Contains("cmd.Parameters.AddWithValue(PgTargetScorer.GrowthConcerningBytes);", collector, StringComparison.Ordinal);
        Assert.Contains("cmd.Parameters.AddWithValue(PgTargetScorer.GrowthConcerningFraction);", collector, StringComparison.Ordinal);
        Assert.Contains("CommandTimeout = FactCommandTimeoutSeconds", collector, StringComparison.Ordinal);
        Assert.DoesNotContain("ObservedDurationMs", CSharpSourceWalker.StripCommentsAndStrings(collector), StringComparison.Ordinal);
    }

    [Fact]
    public void TheBaselineArm_AndTheWindowRead_ShareTheSeries_AndTheWindowReadReachesOneSampleBack()
    {
        var arm = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgDatabaseGrowthBytesPerDay)!;
        Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, arm, StringComparison.Ordinal);
        Assert.Contains("total_bytes - LAG(total_bytes) OVER series AS raw_growth_bytes", arm, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_growth_bytes, 0)::DOUBLE PRECISION * 86400.0 / interval_sec AS v", arm, StringComparison.Ordinal);
        Assert.Contains("AND   total_bytes IS NOT NULL", arm, StringComparison.Ordinal);
        Assert.DoesNotContain("SUM(", arm, StringComparison.Ordinal);
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgDatabaseGrowthBytesPerDay));
        Assert.Null(PgTargetBaselineProvider.GetPgTargetKeyedBaselineQuery(MetricNames.PgDatabaseGrowthBytesPerDay));

        var window = PgTargetAnomalyDetector.DatabaseGrowthWindowSql;
        Assert.Contains("total_bytes - LAG(total_bytes) OVER series AS raw_growth_bytes", window, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_growth_bytes, 0)::DOUBLE PRECISION * 86400.0 / interval_sec AS bytes_per_day", window, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2 - INTERVAL '2 hours'", window, StringComparison.Ordinal);
        /* The in-window filter on the rated CTE — matched newline-insensitively, because the literal is CRLF on a Windows
           checkout and LF on this one. Two occurrences: the reach-back bound and the in-window filter. */
        Assert.Matches(new Regex(@"AND   collection_time >= \$2\r?\n"), window);
        Assert.Equal(2, Regex.Matches(window, @"collection_time >= \$2\b").Count);
        Assert.Contains("AVG(bytes_per_day) AS mean_bytes_per_day", window, StringComparison.Ordinal);

        /* The pair gate, by text: peak AND mean handed to the gate. */
        var detector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Growth.cs"));
        Assert.Contains("baseline, peakPerDay, meanPerDay,", detector, StringComparison.Ordinal);
        Assert.Contains("PgDatabaseGrowthFloorBytesPerDay, PgDatabaseGrowthFallbackBytesPerDay, SigmaDisplayCap", detector, StringComparison.Ordinal);
        Assert.DoesNotContain("barsMeasured: true", detector, StringComparison.Ordinal);
    }

    /* ── routing, rosters, the R5 pin ── */

    [Fact]
    public void TheFamily_IsRoutedByEveryRootDispatcher_Floored_Recommended_AndCountedInTheCaveatDenominator()
    {
        Assert.Equal("pg_growth", PgTargetSources.GrowthSource);
        Assert.Contains(PgTargetSources.GrowthSource, PgTargetSources.All);
        Assert.Contains(PgTargetSources.GrowthSource, FactScorer.KnownSources);
        Assert.Equal("PG_DATABASE_GROWTH", PgTargetFactKeys.DatabaseGrowth);
        Assert.Equal("ANOMALY_PG_DATABASE_GROWTH", PgTargetFactKeys.AnomalyDatabaseGrowth);
        Assert.Equal("pg_database_growth_bytes_per_day", MetricNames.PgDatabaseGrowthBytesPerDay);
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.DatabaseGrowth));

        /* A hand-built fact under the source scores through the root arm; an unknown key under it is 0. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.GrowthSource, Key = "PG_NOT_A_KEY", Value = 1, ServerId = 1 }));

        /* Both keys carry tool rows, every tool registered (PgTargetMcpSurfaceTests sweeps the registry); the trend's first
           read is the fact itself — there is no get_pg_database_size tool yet. */
        foreach (var key in new[] { PgTargetFactKeys.DatabaseGrowth, PgTargetFactKeys.AnomalyDatabaseGrowth })
        {
            var rows = PgTargetToolRecommendations.GetForKey(key)!;
            Assert.Equal(["get_analysis_facts", "get_pg_table_bloat", "get_pg_autovacuum_health"], rows.Select(r => r.Tool).ToList());
            Assert.Contains("source=pg_growth", rows[0].Reason, StringComparison.Ordinal);
        }

        /* The retention floor, by collector name; the collector emits LAST; the family is the twentieth read. */
        Assert.Contains("pg_database_size_stats", DarlingRetentionHorizons.BaselineServingRawCollectors);
        var collector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.cs"));
        var memory = collector.IndexOf("await CollectMemoryFactsAsync(context, facts);", StringComparison.Ordinal);
        var growth = collector.IndexOf("await CollectGrowthFactsAsync(context, facts);", StringComparison.Ordinal);
        Assert.True(memory > 0 && growth > memory, "the growth family emits after the memory family");
        Assert.Equal(20, CollectionCaveats.CountFamilies(typeof(PgTargetFactCollector)));

        /* No file in the family mentions an Aurora-only surface, and the family's partials name their lane. */
        foreach (var (project, file) in new[]
        {
            (new[] { "PerformanceMonitor.Analysis" }, "PgTargetScorer.Growth.cs"),
            (new[] { "PerformanceMonitor.Analysis" }, "PgTargetAdvice.Growth.cs"),
            (new[] { "PerformanceMonitor.Analysis" }, "PgTargetRelationshipGraph.Growth.cs"),
            (new[] { "Darling", "PerformanceMonitor.Darling.Analysis" }, "PgTargetFactCollector.Growth.cs"),
            (new[] { "Darling", "PerformanceMonitor.Darling.Analysis" }, "PgTargetAnomalyDetector.Growth.cs"),
            (new[] { "Darling", "PerformanceMonitor.Darling.Analysis" }, "PgTargetBaselineProvider.Growth.cs"),
        })
        {
            var text = RepoFile.ReadRepoFile([.. project, file]);
            Assert.DoesNotContain("aurora_stat_", text, StringComparison.Ordinal);
            Assert.Contains("filled by lane 38", text, StringComparison.Ordinal);
        }
    }

    /* ── fixtures ── */

    /// <summary>A trend fact the way the collector builds it: the first named database is the worst (the subject), the
    /// rest ride by name; the total is trended when given and marked unavailable when null; 336 hourly samples over
    /// <paramref name="spanHours"/> unless told otherwise.</summary>
    private static Fact Growth((string Name, long First, long Latest) db, (long First, long Latest)? total, double spanHours = 335, int unsized = 0) =>
        Growth([db], total, spanHours, unsized);

    private static Fact Growth((string Name, long First, long Latest) a, (string Name, long First, long Latest) b, (long First, long Latest)? total, double spanHours = 335) =>
        Growth([a, b], total, spanHours);

    private static Fact Growth((string Name, long First, long Latest) a, (string Name, long First, long Latest) b, (string Name, long First, long Latest) c, (long First, long Latest)? total, double spanHours = 335) =>
        Growth([a, b, c], total, spanHours);

    private static Fact Growth((string Name, long First, long Latest)[] databases, (long First, long Latest)? total, double spanHours = 335, int unsized = 0)
    {
        var worst = databases[0];
        var growth = worst.Latest - worst.First;
        var spanDays = spanHours / 24.0;
        var fact = new Fact
        {
            Source = PgTargetSources.GrowthSource,
            Key = PgTargetFactKeys.DatabaseGrowth,
            Value = growth,
            ServerId = 1,
            DatabaseName = worst.Name,
            ObjectName = worst.Name,
            Metadata =
            {
                [PgTargetScorer.GrowthLookbackDaysKey] = PgTargetScorer.GrowthLookbackDays,
                [PgTargetScorer.GrowthSamplesInLookbackKey] = 336,
                [PgTargetScorer.GrowthDatabasesSeenKey] = databases.Length + unsized,
                [PgTargetScorer.GrowthDatabasesUnsizedKey] = unsized,
                [PgTargetScorer.GrowthDatabasesConsideredKey] = databases.Length,
                [PgTargetScorer.GrowthDatabasesOverLineKey] = databases.Count(d => PgTargetScorer.GrowthGrade(d.Latest - d.First, d.First > 0 ? 100.0 * (d.Latest - d.First) / d.First : null) > 0),
                [PgTargetScorer.GrowthAvailableKey] = 1,
                [PgTargetScorer.GrowthBytesKey] = growth,
                [PgTargetScorer.GrowthFirstBytesKey] = worst.First,
                [PgTargetScorer.GrowthLatestBytesKey] = worst.Latest,
                [PgTargetScorer.GrowthSamplesKey] = 336,
                [PgTargetScorer.GrowthSpanHoursKey] = spanHours,
                [PgTargetScorer.GrowthBytesPerDayKey] = growth / spanDays,
            },
        };
        if (worst.First > 0)
        {
            fact.Metadata[PgTargetScorer.GrowthPctKey] = 100.0 * growth / worst.First;
            fact.Metadata[PgTargetScorer.GrowthPctComputableKey] = 1;
            fact.Metadata[PgTargetScorer.GrowthPctPerDayKey] = 100.0 * growth / worst.First / spanDays;
        }
        else
        {
            fact.Metadata[PgTargetScorer.GrowthPctComputableKey] = 0;
        }
        if (growth > 0)
            fact.Metadata[PgTargetScorer.GrowthDaysToDoubleKey] = worst.Latest / (growth / spanDays);

        foreach (var (name, first, latest) in databases)
        {
            fact.Metadata[PgTargetScorer.GrowthNamedBytesPrefix + name] = latest - first;
            if (first > 0)
                fact.Metadata[PgTargetScorer.GrowthNamedPctPrefix + name] = 100.0 * (latest - first) / first;
        }

        if (total is { } t)
        {
            var tGrowth = t.Latest - t.First;
            fact.Metadata[PgTargetScorer.GrowthTotalAvailableKey] = 1;
            fact.Metadata[PgTargetScorer.GrowthTotalSamplesKey] = 336;
            fact.Metadata[PgTargetScorer.GrowthTotalBytesKey] = tGrowth;
            fact.Metadata[PgTargetScorer.GrowthTotalFirstBytesKey] = t.First;
            fact.Metadata[PgTargetScorer.GrowthTotalLatestBytesKey] = t.Latest;
            fact.Metadata[PgTargetScorer.GrowthTotalSpanHoursKey] = spanHours;
            fact.Metadata[PgTargetScorer.GrowthTotalBytesPerDayKey] = tGrowth / spanDays;
            if (t.First > 0) fact.Metadata[PgTargetScorer.GrowthTotalPctKey] = 100.0 * tGrowth / t.First;
            if (tGrowth > 0) fact.Metadata[PgTargetScorer.GrowthTotalDaysToDoubleKey] = t.Latest / (tGrowth / spanDays);
        }
        else
        {
            fact.Metadata[PgTargetScorer.GrowthTotalAvailableKey] = 0;
            fact.Metadata[PgTargetScorer.GrowthTotalSamplesKey] = 0;
        }
        return fact;
    }

    private static Fact Withheld(int reason, int seen, int unsized) => new()
    {
        Source = PgTargetSources.GrowthSource,
        Key = PgTargetFactKeys.DatabaseGrowth,
        Value = 0,
        ServerId = 1,
        Metadata =
        {
            [PgTargetScorer.GrowthAvailableKey] = 0,
            [PgTargetScorer.GrowthUnavailableReasonKey] = reason,
            [PgTargetScorer.GrowthLookbackDaysKey] = PgTargetScorer.GrowthLookbackDays,
            [PgTargetScorer.GrowthSamplesInLookbackKey] = 2,
            [PgTargetScorer.GrowthDatabasesSeenKey] = seen,
            [PgTargetScorer.GrowthDatabasesUnsizedKey] = unsized,
            [PgTargetScorer.GrowthTotalAvailableKey] = 0,
        },
    };

    /// <summary>A graded bloat trend (lane 13's shape) on one table: 1 GiB heap, 100 → 500 MiB estimate → 0.59375.
    /// The table rides on <c>Fact.Ranked</c> as its own subject since #3691 lane 43 — it was a
    /// <c>growth_bytes_&lt;table&gt;</c> metadata key, and <c>BloatNamedObjects</c> (which the growth family's
    /// same-database predicate reaches) reads the typed list now.</summary>
    private static Fact Bloat(string? database, string table)
    {
        var fact = BloatFact(database, table);
        fact.Ranked.Add(new RankedObject(
            table,
            database,
            400 * MiB,
            new Dictionary<string, double>(StringComparer.Ordinal)
            {
                [PgTargetScorer.BloatGrowthBytesKey] = 400 * MiB,
                [PgTargetScorer.BloatGrowthPctKey] = 400.0,
            }));
        return fact;
    }

    private static Fact BloatFact(string? database, string table) => new()
    {
        Source = PgTargetSources.BloatSource,
        Key = PgTargetFactKeys.BloatTrend,
        Value = 400 * MiB,
        ServerId = 1,
        DatabaseName = database,
        ObjectName = table,
        Metadata =
        {
            [PgTargetScorer.BloatEstimateAvailableKey] = 1,
            [PgTargetScorer.BloatGrowthBytesKey] = 400 * MiB,
            [PgTargetScorer.BloatEarlierBytesKey] = 100 * MiB,
            [PgTargetScorer.BloatLatestBytesKey] = 500 * MiB,
            [PgTargetScorer.BloatObjectBytesKey] = GiB,
            [PgTargetScorer.BloatSamplesKey] = 336,
            [PgTargetScorer.BloatSpanHoursKey] = 335,
            [PgTargetScorer.BloatLookbackDaysKey] = PgTargetScorer.BloatLookbackDays,
        },
    };

    private static Fact Anomaly(double peakBytesPerDay, double sigma, bool lowQuality)
    {
        var fact = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = PgTargetFactKeys.AnomalyDatabaseGrowth, Value = peakBytesPerDay, ServerId = 1 };
        fact.Metadata["baseline_mean"] = 200.0 * MiB;
        fact.Metadata["baseline_stddev"] = 40.0 * MiB;
        fact.Metadata["deviation_sigma"] = sigma;
        fact.Metadata["fire_threshold"] = AnomalyThresholds.ModifiedZThreshold;
        fact.Metadata["baseline_low_quality"] = lowQuality ? 1 : 0;
        fact.Metadata["fallback_exceedance"] = lowQuality ? peakBytesPerDay / AnomalyThresholds.PgDatabaseGrowthFallbackBytesPerDay : 0;
        fact.Metadata["baseline_samples"] = lowQuality ? 3 : 30;
        fact.Metadata["window_samples"] = 3;
        fact.Metadata["threshold_lineage"] = 0;
        fact.Metadata["peak_bytes_per_day"] = peakBytesPerDay;
        return fact;
    }

    private static Dictionary<string, Fact> Lookup(params Fact[] facts) =>
        facts.ToDictionary(f => f.Key, StringComparer.Ordinal);
}
