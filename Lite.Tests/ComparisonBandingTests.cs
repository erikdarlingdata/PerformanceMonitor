/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3538 A3: the arithmetic behind <c>compare_analysis</c>'s verdicts, pinned on the shared
/// <see cref="ComparisonBanding"/> both SKUs serialize. Every fact here is scored by the REAL
/// <see cref="FactScorer"/> before it is compared, so the ladder positions the absolute rule reads are the
/// scorer's own and a threshold change upstream moves these pins with it rather than past them. The
/// scenarios are the review's: the flat ±0.1 severity dead-band read a saturated doubling as "stable" and a
/// trace-to-trace wobble as "worse"; the same value delta banded by a tight and a loose baseline must
/// land on different sides; one I/O stall must be one family row; a plan-cache hash swap must be churn.
/// </summary>
public sealed class ComparisonBandingTests
{
    private static readonly IReadOnlyDictionary<string, BaselineBucket> NoDispersion = new Dictionary<string, BaselineBucket>();

    /* ── the distortion the dead-band produced ── */

    /// <summary>
    /// PAGEIOLATCH_SH's ladder saturates at 25% of observed time (<c>(0.25, null)</c>), so 30% and 60% both
    /// score 1.0 and the old band called a DOUBLING of I/O latch time "stable" (severity delta 0.0). The
    /// value moved by half of the larger side and the key sits at the top of its ladder: worse.
    /// </summary>
    [Fact]
    public void ASaturatedLadderDoubling_IsWorse_NotStable()
    {
        var (baseline, comparison) = Scored(
            [Wait("PAGEIOLATCH_SH", 0.30)],
            [Wait("PAGEIOLATCH_SH", 0.60)]);
        Assert.Equal(1.0, baseline[0].Severity, precision: 6);
        Assert.Equal(1.0, comparison[0].Severity, precision: 6);

        var row = Assert.Single(ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false).Rows);

        Assert.Equal(0.0, row.SeverityDelta, precision: 6);      // what the old band read
        Assert.Equal(ComparisonBanding.StatusWorse, row.Status); // what the value says
        Assert.Equal(ComparisonBanding.BandSourceAbsolute, row.BandSource);
        Assert.Equal(0.5, row.RelativeMove!.Value, precision: 6);
        Assert.Equal(1.0, row.LadderPosition, precision: 6);
    }

    /// <summary>
    /// The RESOURCE_SEMAPHORE-vs-CPU distortion. RESOURCE_SEMAPHORE's ramp is <c>(0.01, 0.10)</c>, so a trace
    /// doubling from 0.2% to 0.45% of observed time (7 s/hr → 16 s/hr of grant queueing) moved severity
    /// +0.125 — over the old dead-band, "worse". A 24-point CPU rise from 50% to 74% moved severity +0.16
    /// on the <c>(75, 95)</c> ramp — nearly the same number for an incomparably larger physical change. The
    /// absolute rule reads each on its own ladder: the trace never climbed a quarter of the way up its
    /// ladder (0.225), so it is stable; CPU did (0.493), and moved a third of the larger side, so it is
    /// worse. Ordering follows the value's relative move, not the formula's slope.
    /// </summary>
    [Fact]
    public void ATraceDoubling_IsStable_WhileARealCpuRise_IsWorse_WhateverTheSeverityDeltasSaid()
    {
        var (baseline, comparison) = Scored(
            [Wait("RESOURCE_SEMAPHORE", 0.002), Cpu(50)],
            [Wait("RESOURCE_SEMAPHORE", 0.0045), Cpu(74)]);

        var result = ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false);
        var rs = result.Rows.Single(r => r.Key == "RESOURCE_SEMAPHORE");
        var cpu = result.Rows.Single(r => r.Key == "CPU_SQL_PERCENT");

        /* The formula's slopes — the numbers the old band decided on. */
        Assert.Equal(0.125, rs.SeverityDelta, precision: 3);
        Assert.Equal(0.16, cpu.SeverityDelta, precision: 2);

        Assert.Equal(ComparisonBanding.StatusStable, rs.Status);
        Assert.InRange(rs.LadderPosition, 0.22, 0.23);
        Assert.InRange(rs.RelativeMove!.Value, 0.55, 0.56); // a big RELATIVE move alone is not a verdict

        Assert.Equal(ComparisonBanding.StatusWorse, cpu.Status);
        Assert.InRange(cpu.LadderPosition, 0.49, 0.50);
        Assert.InRange(cpu.RelativeMove!.Value, 0.32, 0.33);

        Assert.Equal("CPU_SQL_PERCENT", result.Rows[0].Key); // changed rows first
        Assert.Equal(1, result.Worse);
        Assert.Equal(1, result.Stable);
    }

    /// <summary>The absolute rule refuses a 1% wobble on a key that is otherwise high on its ladder.</summary>
    [Fact]
    public void AOnePercentWobble_IsStable_HoweverHighTheKeySits()
    {
        var (baseline, comparison) = Scored([Cpu(80)], [Cpu(80.8)]);
        var row = Assert.Single(ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false).Rows);

        Assert.Equal(ComparisonBanding.StatusStable, row.Status);
        Assert.InRange(row.LadderPosition, 0.6, 0.7);      // well up the ladder …
        Assert.InRange(row.RelativeMove!.Value, 0.0, 0.011); // … but it did not move
    }

    /// <summary>
    /// Both arms are required. Read latency doubling from 4 ms to 8 ms is a 50% relative move between two
    /// values the scorer grades as healthy (base 0.2 at 8 ms on the <c>(20, 50)</c> ramp): stable. Doubling
    /// from 12 ms to 24 ms crosses the concerning bar: worse.
    /// </summary>
    [Fact]
    public void ADoublingBetweenTwoHealthyValues_IsStable_ADoublingIntoConcerning_IsWorse()
    {
        var (b1, c1) = Scored([Io(4)], [Io(8)]);
        var healthy = Assert.Single(ComparisonBanding.Compare(b1, c1, NoDispersion, coverageCaveat: false).Rows);
        Assert.Equal(ComparisonBanding.StatusStable, healthy.Status);
        Assert.Equal(0.2, healthy.LadderPosition, precision: 6);

        var (b2, c2) = Scored([Io(12)], [Io(24)]);
        var concerning = Assert.Single(ComparisonBanding.Compare(b2, c2, NoDispersion, coverageCaveat: false).Rows);
        Assert.Equal(ComparisonBanding.StatusWorse, concerning.Status);
        Assert.InRange(concerning.LadderPosition, 0.56, 0.57);
    }

    /// <summary>A fall is "better" by the same two arms, and the direction comes from the scorer's ladder.</summary>
    [Fact]
    public void AFallByBothArms_IsBetter()
    {
        var (baseline, comparison) = Scored([Cpu(90)], [Cpu(45)]);
        var row = Assert.Single(ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false).Rows);
        Assert.Equal(ComparisonBanding.StatusBetter, row.Status);
        Assert.Equal(0.5, row.RelativeMove!.Value, precision: 6);
    }

    /// <summary>
    /// DISK_SPACE is the free fraction and the scorer inverts it. With both sides saturated (under 5% free
    /// scores 1.0 on either side) the value has to decide, and less free space must still read worse.
    /// </summary>
    [Fact]
    public void FreeSpaceFalling_OnASaturatedLadder_IsWorse()
    {
        var (baseline, comparison) = Scored(
            [new Fact { Source = "disk", Key = "DISK_SPACE", Value = 0.04 }],
            [new Fact { Source = "disk", Key = "DISK_SPACE", Value = 0.02 }]);
        Assert.Equal(1.0, baseline[0].BaseSeverity, precision: 6);
        Assert.Equal(1.0, comparison[0].BaseSeverity, precision: 6);

        var row = Assert.Single(ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false).Rows);
        Assert.Equal(ComparisonBanding.StatusWorse, row.Status);
    }

    /* ── sigma banding ── */

    /// <summary>
    /// The same +10-point CPU delta, banded by two servers' own dispersion. A tight bucket (MAD 2 → robust
    /// sigma 2.97, floored to the CPU model's 5-point absolute floor) reads it as 2σ: worse. A loose bucket
    /// (MAD 10 → robust sigma 14.8) reads it as 0.67σ: stable. Neither verdict used the severity ladder,
    /// and the baseline's confidence and tier travel with the row.
    /// </summary>
    [Fact]
    public void TheSameDelta_BandsDifferently_ByTheServersOwnDispersion()
    {
        var (baseline, comparison) = Scored([Cpu(50)], [Cpu(60)]);

        var tight = Compare(baseline, comparison, CpuBucket(median: 50, mad: 2));
        Assert.Equal(ComparisonBanding.BandSourceBaseline, tight.BandSource);
        Assert.Equal(ComparisonBanding.StatusWorse, tight.Status);
        Assert.Equal(5.0, tight.BaselineSigma!.Value, precision: 4);   // the 5-point CPU floor, not 2.97
        Assert.Equal(2.0, tight.DeltaSigma!.Value, precision: 2);
        Assert.Equal(MetricNames.Cpu, tight.BaselineMetric);
        Assert.Equal(nameof(BaselineTier.Full), tight.BaselineTier);
        Assert.Equal(1.0, tight.BaselineConfidence!.Value, precision: 2); // 20 samples = 2x the Full floor
        Assert.False(tight.BeyondAnomalyCutoff!.Value);                 // 2σ is under the 3.5σ robust cutoff

        var loose = Compare(baseline, comparison, CpuBucket(median: 50, mad: 10));
        Assert.Equal(ComparisonBanding.BandSourceBaseline, loose.BandSource);
        Assert.Equal(ComparisonBanding.StatusStable, loose.Status);
        Assert.InRange(loose.BaselineSigma!.Value, 14.82, 14.83);
        Assert.InRange(loose.DeltaSigma!.Value, 0.67, 0.68);
    }

    /// <summary>A move past the metric's own anomaly cutoff says so — and the display sigma is capped.</summary>
    [Fact]
    public void ABigSigmaMove_FlagsTheAnomalyCutoff_AndCapsTheDisplay()
    {
        var (baseline, comparison) = Scored([Cpu(20)], [Cpu(95)]);
        var row = Compare(baseline, comparison, CpuBucket(median: 20, mad: 0.5));

        Assert.Equal(5.0, row.BaselineSigma!.Value, precision: 4); // floored
        Assert.Equal(15.0, row.DeltaSigma!.Value, precision: 2);
        Assert.True(row.BeyondAnomalyCutoff!.Value);
        Assert.Equal(ComparisonBanding.StatusWorse, row.Status);

        /* A session bucket has no absolute floor: MAD 0 floors at 1% of the median = 1.0 connection, so a
           +75 move is 75σ raw — decided on the raw value, displayed at the detectors' 25σ cap. */
        var (sb, sc) = Scored([Sessions(100)], [Sessions(175)]);
        var sessions = Compare(sb, sc, SessionsBucket(median: 100, mad: 0.0));
        Assert.Equal(1.0, sessions.BaselineSigma!.Value, precision: 4);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, sessions.DeltaSigma!.Value, precision: 2);
        Assert.True(sessions.BeyondAnomalyCutoff!.Value);
        Assert.Equal(ComparisonBanding.StatusWorse, sessions.Status);
    }

    /// <summary>
    /// The never-blind rule: an untrustworthy bucket (too few distinct days) routes the key to the
    /// absolute rule with <c>band_source: "absolute"</c> rather than to a sigma nobody should trust — and
    /// a key with no baseline metric at all never looks one up.
    /// </summary>
    [Fact]
    public void AnUntrustworthyBaseline_FallsBackToTheAbsoluteRule()
    {
        var (baseline, comparison) = Scored([Cpu(50)], [Cpu(60)]);
        var thin = new BaselineBucket
        {
            HourOfDay = 9, DayOfWeek = 2, Tier = BaselineTier.Full,
            Mean = 50, StdDev = 5, Median = 50, Mad = 2, SampleCount = 20, DistinctDays = 1,
            AbsStdDevFloor = BaselineMath.AbsStdDevFloorFor(MetricNames.Cpu)
        };
        Assert.False(thin.IsTrustworthy);

        var row = Compare(baseline, comparison, thin);
        Assert.Equal(ComparisonBanding.BandSourceAbsolute, row.BandSource);
        Assert.Null(row.DeltaSigma);
        Assert.Equal(MetricNames.Cpu, row.BaselineMetric); // the row still says which baseline WOULD apply
        Assert.Equal(ComparisonBanding.StatusStable, row.Status); // +10 on 60 is a 17% move: under the quarter

        Assert.Null(ComparisonBanding.BaselinedMetricFor("PAGEIOLATCH_SH"));
        Assert.Null(ComparisonBanding.BaselinedMetricFor("BLOCKING_EVENTS"));
        Assert.Null(ComparisonBanding.BaselinedMetricFor("IO_WRITE_LATENCY_MS"));
    }

    [Fact]
    public void DispersionMetrics_AreOnlyTheOnesSomePresentKeyIsMeasuredIn()
    {
        var baseline = new List<Fact> { Wait("CXPACKET", 0.1), Io(5) };
        var comparison = new List<Fact> { Cpu(40), Wait("CXPACKET", 0.2) };
        Assert.Equal(new[] { MetricNames.Cpu, MetricNames.IoLatency }, ComparisonBanding.DispersionMetricsFor(baseline, comparison));
        Assert.Empty(ComparisonBanding.DispersionMetricsFor([Wait("CXPACKET", 0.1)], [Wait("LCK", 0.1)]));
    }

    /* ── families ── */

    /// <summary>
    /// One I/O stall: PAGEIOLATCH_SH and PAGEIOLATCH_EX up, read latency up. Three worse rows, ONE worse
    /// family, whose worst member is the one that moved most and whose members are all three named.
    /// </summary>
    [Fact]
    public void OneIoStall_IsOneFamilyRow_WithThreeMembers()
    {
        var (baseline, comparison) = Scored(
            [Wait("PAGEIOLATCH_SH", 0.10), Wait("PAGEIOLATCH_EX", 0.05), Io(12), Wait("CXPACKET", 0.10)],
            [Wait("PAGEIOLATCH_SH", 0.30), Wait("PAGEIOLATCH_EX", 0.20), Io(30), Wait("CXPACKET", 0.10)]);

        var result = ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false);

        Assert.Equal(3, result.Worse);
        Assert.Equal(1, result.FamiliesWorse);
        Assert.Equal(1, result.FamiliesStable);

        var io = Assert.Single(result.Families, f => f.Family == "io_pressure");
        Assert.Equal(ComparisonBanding.StatusWorse, io.Status);
        Assert.Equal("PAGEIOLATCH_EX", io.WorstKey); // 0.05 → 0.20 is the largest relative move (0.75)
        Assert.Equal(new[] { "PAGEIOLATCH_EX", "PAGEIOLATCH_SH", "IO_READ_LATENCY_MS" }, io.Members);
        Assert.Equal(3, io.Worse);
        Assert.Equal(0, io.Stable);

        Assert.Equal("io_pressure", result.Families[0].Family); // changed families first
        Assert.Equal("parallelism", result.Families[1].Family);
        Assert.All(result.Rows.Where(r => r.Family == "io_pressure"), r => Assert.Equal(ComparisonBanding.StatusWorse, r.Status));
    }

    /// <summary>
    /// A family with members moving in BOTH directions: BLOCKING_EVENTS falls 60% (better, the larger
    /// move) while LCK_M_S rises to a scored level (worse, the smaller move). The regression is the
    /// family's worst member and the family counts in families_worse — direction outranks magnitude, so
    /// a large improvement in a sibling symptom cannot hide a real degradation in the same cause. The
    /// review's catch on the first head; ordered rows read worse, then better, then stable.
    /// </summary>
    [Fact]
    public void AMixedDirectionFamily_IsWorse_WhenAnyMemberIs_WhateverTheLargerMoveDid()
    {
        var (baseline, comparison) = Scored(
            [Blocking(50), Wait("LCK_M_S", 0.02), Wait("CXPACKET", 0.10)],
            [Blocking(20), Wait("LCK_M_S", 0.03), Wait("CXPACKET", 0.10)]);

        var result = ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false);

        var blocking = result.Rows.Single(r => r.Key == "BLOCKING_EVENTS");
        var lck = result.Rows.Single(r => r.Key == "LCK_M_S");
        Assert.Equal(ComparisonBanding.StatusBetter, blocking.Status);
        Assert.Equal(0.6, blocking.RelativeMove!.Value, precision: 6);
        Assert.Equal(ComparisonBanding.StatusWorse, lck.Status);
        Assert.InRange(lck.RelativeMove!.Value, 0.33, 0.34);

        var family = Assert.Single(result.Families, f => f.Family == "lock_contention");
        Assert.Equal(ComparisonBanding.StatusWorse, family.Status);
        Assert.Equal("LCK_M_S", family.WorstKey);
        Assert.Equal(new[] { "LCK_M_S", "BLOCKING_EVENTS" }, family.Members);
        Assert.Equal(1, family.Worse);
        Assert.Equal(1, family.Better);
        Assert.Equal(1, result.FamiliesWorse);
        Assert.Equal(0, result.FamiliesBetter);

        Assert.Equal(new[] { "LCK_M_S", "BLOCKING_EVENTS", "CXPACKET" }, result.Rows.Select(r => r.Key));
        Assert.Equal("lock_contention", result.Families[0].Family);
    }

    /// <summary>
    /// The family map mirrors the collector's wait grouping and the reconciler's symptom families: every
    /// regular key the reconciler folds an anomaly into shares a family with its siblings; a raw CX* or
    /// general lock mode lands where the collector would have grouped it; a key with no family is its own.
    /// </summary>
    [Fact]
    public void Families_FollowTheCollectorGrouping_AndTheReconcilersSymptomFamilies()
    {
        /* AnomalyIncidentReconciler.AnomalyToFamilies: CPU_SPIKE → {CPU_SQL_PERCENT, CPU_SPIKE}. */
        Assert.Equal(ComparisonBanding.FamilyFor("CPU_SQL_PERCENT"), ComparisonBanding.FamilyFor("CPU_SPIKE"));
        Assert.Equal("cpu_pressure", ComparisonBanding.FamilyFor("SOS_SCHEDULER_YIELD"));
        Assert.Equal("io_pressure", ComparisonBanding.FamilyFor("IO_READ_LATENCY_MS"));
        Assert.Equal("log_io", ComparisonBanding.FamilyFor("IO_WRITE_LATENCY_MS"));
        Assert.Equal("log_io", ComparisonBanding.FamilyFor("WRITELOG"));
        Assert.Equal("log_io", ComparisonBanding.FamilyFor("HADR_SYNC_COMMIT"));
        Assert.Equal("memory_grants", ComparisonBanding.FamilyFor("RESOURCE_SEMAPHORE"));
        Assert.Equal("memory_grants", ComparisonBanding.FamilyFor("MEMORY_GRANT_PENDING"));
        Assert.Equal("lock_contention", ComparisonBanding.FamilyFor("BLOCKING_EVENTS"));
        Assert.Equal("lock_contention", ComparisonBanding.FamilyFor("LCK_M_S"));
        Assert.Equal("lock_contention", ComparisonBanding.FamilyFor("LCK_M_RS_U"));
        Assert.Equal("deadlocking", ComparisonBanding.FamilyFor("DEADLOCKS")); // kept apart, as the reconciler keeps it

        /* FactCollectorHelpers.WaitFamilyKey applied first. */
        Assert.Equal("parallelism", ComparisonBanding.FamilyFor("CXCONSUMER"));
        Assert.Equal("lock_contention", ComparisonBanding.FamilyFor("LCK_M_IX"));
        Assert.Equal("latch_contention", ComparisonBanding.FamilyFor("PAGELATCH_UP"));

        /* Its own family. */
        Assert.Equal("THREADPOOL", ComparisonBanding.FamilyFor("THREADPOOL"));
        Assert.Equal("TEMPDB_USAGE", ComparisonBanding.FamilyFor("TEMPDB_USAGE"));
        Assert.Equal("CONFIG_MAXDOP", ComparisonBanding.FamilyFor("CONFIG_MAXDOP"));
        Assert.Equal("bad_actor", ComparisonBanding.FamilyFor("BAD_ACTOR_0x1234"));
    }

    /* ── plan-cache churn and presence ── */

    /// <summary>
    /// A hash swap: the same statement under a recompiled plan is BAD_ACTOR_0xA yesterday and
    /// BAD_ACTOR_0xB today. Key-set arithmetic called that one new issue and one resolved issue; it is
    /// churn, counted nowhere in the issue counters. A hash present on both sides compares normally.
    /// A non-bad-actor key that appears at a scored level IS a new issue; one that appears at a trace is not.
    /// </summary>
    [Fact]
    public void ABadActorHashSwap_IsChurn_NotANewAndAResolvedIssue()
    {
        var (baseline, comparison) = Scored(
            [BadActor("0xA", avgCpuMs: 500), BadActor("0xC", avgCpuMs: 100), Wait("CXPACKET", 0.10)],
            [BadActor("0xB", avgCpuMs: 500), BadActor("0xC", avgCpuMs: 110), Wait("CXPACKET", 0.10), Wait("PAGEIOLATCH_SH", 0.20), Wait("LCK_M_IS", 0.001)]);
        Assert.True(baseline[0].Severity > 0 && comparison[0].Severity > 0);

        var result = ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false);

        Assert.Equal(new[] { "BAD_ACTOR_0xB" }, result.Churn.Appeared.Select(e => e.Key));
        Assert.Equal(new[] { "BAD_ACTOR_0xA" }, result.Churn.Disappeared.Select(e => e.Key));
        Assert.Equal(1, result.Churn.PresentInBoth);
        Assert.Equal(500, result.Churn.Appeared[0].Value);
        Assert.DoesNotContain(result.Rows, r => r.Key is "BAD_ACTOR_0xA" or "BAD_ACTOR_0xB");

        var shared = Assert.Single(result.Rows, r => r.Key == "BAD_ACTOR_0xC");
        Assert.Equal(ComparisonBanding.PresenceBoth, shared.Presence);
        Assert.Equal(ComparisonBanding.StatusStable, shared.Status); // 100 → 110 is under the quarter

        /* PAGEIOLATCH_SH appeared at 0.20 (base 0.8): a new issue. LCK_M_IS appeared at a trace (base 0.02): stable, not counted. */
        var pageio = Assert.Single(result.Rows, r => r.Key == "PAGEIOLATCH_SH");
        Assert.Equal(ComparisonBanding.PresenceComparisonOnly, pageio.Presence);
        Assert.Equal(ComparisonBanding.BandSourcePresence, pageio.BandSource);
        Assert.Equal(ComparisonBanding.StatusWorse, pageio.Status);
        Assert.Null(pageio.BaselineValue);
        Assert.Null(pageio.ValueDelta);

        var trace = Assert.Single(result.Rows, r => r.Key == "LCK_M_IS");
        Assert.Equal(ComparisonBanding.StatusStable, trace.Status);

        Assert.Equal(1, result.NewIssues);
        Assert.Equal(0, result.ResolvedIssues);
        Assert.Equal(1, result.Worse);
    }

    /// <summary>A disappearance at a scored level is a resolved issue; the baseline-only row reads better.</summary>
    [Fact]
    public void AScoredKeyThatDisappeared_IsAResolvedIssue()
    {
        var (baseline, comparison) = Scored([Wait("WRITELOG", 0.30), Cpu(40)], [Cpu(40)]);
        var result = ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false);

        var writelog = Assert.Single(result.Rows, r => r.Key == "WRITELOG");
        Assert.Equal(ComparisonBanding.PresenceBaselineOnly, writelog.Presence);
        Assert.Equal(ComparisonBanding.StatusBetter, writelog.Status);
        Assert.Equal(1, result.ResolvedIssues);
        Assert.Equal(0, result.NewIssues);
    }

    /* ── coverage and emptiness ── */

    [Fact]
    public void TheCoverageCaveat_RidesOnEveryVerdictRow_AndFamily_AndTheSummary()
    {
        var (baseline, comparison) = Scored([Cpu(50), Wait("CXPACKET", 0.1)], [Cpu(74), Wait("CXPACKET", 0.1)]);

        var caveated = ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: true);
        Assert.All(caveated.Rows, r => Assert.True(r.CoverageCaveat));
        Assert.All(caveated.Families, f => Assert.True(f.CoverageCaveat));
        Assert.True(caveated.CoverageCaveat);

        var clean = ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false);
        Assert.All(clean.Rows, r => Assert.False(r.CoverageCaveat));
    }

    [Fact]
    public void NothingOnEitherSide_IsEmpty_ButChurnAloneIsNot()
    {
        Assert.True(ComparisonBanding.Compare([], [], NoDispersion, coverageCaveat: false).IsEmpty);

        var (baseline, comparison) = Scored([], [BadActor("0xB", avgCpuMs: 500)]);
        var churnOnly = ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false);
        Assert.False(churnOnly.IsEmpty);
        Assert.Empty(churnOnly.Rows);
        Assert.Single(churnOnly.Churn.Appeared);
    }

    /// <summary>The rules the payload states name the constants they rest on, so a change to one moves the other.</summary>
    [Fact]
    public void TheStatedRules_NameTheConstants()
    {
        /* Parsed, not raw: System.Text.Json escapes ± and σ as \uXXXX in the serialized text. */
        using var doc = System.Text.Json.JsonDocument.Parse(System.Text.Json.JsonSerializer.Serialize(ComparisonBanding.BandRulesPayload));
        var baselineRule = doc.RootElement.GetProperty("baseline").GetString()!;
        var absoluteRule = doc.RootElement.GetProperty("absolute").GetString()!;
        var presenceRule = doc.RootElement.GetProperty("presence").GetString()!;
        Assert.Contains("±1σ", baselineRule, StringComparison.Ordinal);
        Assert.Contains("beyond_anomaly_cutoff", baselineRule, StringComparison.Ordinal);
        Assert.Contains("25%", absoluteRule, StringComparison.Ordinal);
        Assert.Contains("0.25", absoluteRule, StringComparison.Ordinal);
        Assert.Contains("0.25", presenceRule, StringComparison.Ordinal);
        Assert.Contains("plan_cache_churn", presenceRule, StringComparison.Ordinal);
        Assert.Equal(1.0, ComparisonBanding.StableWithinRobustSigmas);
        Assert.Equal(0.25, ComparisonBanding.MinimumRelativeMove);
        Assert.Equal(0.25, ComparisonBanding.MinimumLadderPosition);
    }

    /* ── the PostgreSQL-target keys (#3691 W1 / W2) ── */

    /// <summary>
    /// The v1 exit check's W1 scenario. <c>PG_TPS</c> has base severity 0 by design (throughput is context), so
    /// under the absolute rule a 10 → 60 tps move (relative_move 0.83, ladder_position 0) could only ever read
    /// "stable" — the same window whose anomaly detector called it 25σ. Mapped to <c>pg_tps</c>, the row is
    /// banded on this server's own same-hour dispersion: a planted bucket of median 10 and sigma 1 reads +50σ
    /// raw, displayed at the detectors' cap, past the metric's own anomaly cutoff: worse, with
    /// <c>band_source: baseline</c> and <c>baseline_metric: pg_tps</c>. The BEFORE picture is asserted too — no
    /// dispersion → absolute → stable — so the defect this closes stays written down.
    /// </summary>
    [Fact]
    public void APostgresTpsSurge_IsBandedByTheServersOwnSigma_NotStableByALadderThatCannotMove()
    {
        var (baseline, comparison) = Scored([PgTps(10)], [PgTps(60)]);
        Assert.Equal(0.0, baseline[0].BaseSeverity);
        Assert.Equal(0.0, comparison[0].BaseSeverity);

        var before = Assert.Single(ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false).Rows);
        Assert.Equal(ComparisonBanding.BandSourceAbsolute, before.BandSource);
        Assert.Equal(ComparisonBanding.StatusStable, before.Status);
        Assert.Equal(0.8333, before.RelativeMove!.Value, precision: 4);
        Assert.Equal(0.0, before.LadderPosition);
        Assert.Equal(MetricNames.PgTps, before.BaselineMetric); // the row already says which baseline WOULD apply

        var after = Compare(baseline, comparison, PgBucket(MetricNames.PgTps, median: 10, mad: 0.6745));
        Assert.Equal(ComparisonBanding.BandSourceBaseline, after.BandSource);
        Assert.Equal(ComparisonBanding.StatusWorse, after.Status);
        Assert.Equal(MetricNames.PgTps, after.BaselineMetric);
        Assert.Equal(1.0, after.BaselineSigma!.Value, precision: 4);      // MAD 0.6745 → exactly one sigma
        Assert.Equal(10.0, after.BaselineMedian!.Value, precision: 4);
        Assert.Equal(50.0, after.ValueDelta!.Value, precision: 6);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, after.DeltaSigma!.Value, precision: 2); // 50σ raw, capped
        Assert.True(after.BeyondAnomalyCutoff!.Value);
        Assert.Equal(PgTargetFactKeys.Tps, after.Family); // its own family: one cause row

        /* Inside one sigma is stable; a fall reads better (the detector's one-sided limit, stated in code). */
        var (qb, qc) = Scored([PgTps(10)], [PgTps(10.8)]);
        Assert.Equal(ComparisonBanding.StatusStable, Compare(qb, qc, PgBucket(MetricNames.PgTps, 10, 0.6745)).Status);
        var (fb, fc) = Scored([PgTps(10)], [PgTps(4)]);
        Assert.Equal(ComparisonBanding.StatusBetter, Compare(fb, fc, PgBucket(MetricNames.PgTps, 10, 0.6745)).Status);
    }

    /// <summary>
    /// <c>PG_CONNECTION_SATURATION</c>'s value is peak ÷ usable connections, a fraction; <c>pg_session_count</c> is
    /// a COUNT. The row is banded on the fact's <c>peak_total_sessions</c> — the reading the bucket is built from —
    /// never on the fraction in count-sigma: 20 → 60 sessions against a median of 20 with MAD 0 (sigma floors at
    /// 1% of the median = 0.2 sessions) is +200σ raw, worse; the fraction's own delta stays in value_delta. A
    /// saturation fact WITHOUT the metadata has no reading in the bucket's unit and takes the absolute rule, the
    /// row still naming the bucket that would apply.
    /// </summary>
    [Fact]
    public void APostgresSaturationFraction_IsBandedOnItsPeakSessionCount_NeverOnTheFractionInCountSigma()
    {
        var (baseline, comparison) = Scored([PgSaturation(peak: 20, usable: 100)], [PgSaturation(peak: 60, usable: 100)]);
        Assert.Equal(20.0, ComparisonBanding.BaselinedValueFor(baseline[0]));
        Assert.Equal(60.0, ComparisonBanding.BaselinedValueFor(comparison[0]));

        var row = Compare(baseline, comparison, PgBucket(MetricNames.PgSessionCount, median: 20, mad: 0));
        Assert.Equal(ComparisonBanding.BandSourceBaseline, row.BandSource);
        Assert.Equal(MetricNames.PgSessionCount, row.BaselineMetric);
        Assert.Equal(0.2, row.BaselineSigma!.Value, precision: 4);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, row.DeltaSigma!.Value, precision: 2); // 40 / 0.2 = 200σ raw
        Assert.Equal(ComparisonBanding.StatusWorse, row.Status);
        Assert.Equal(0.2, row.BaselineValue!.Value, precision: 6);   // the fraction, the fact's own unit
        Assert.Equal(0.6, row.ComparisonValue!.Value, precision: 6);
        Assert.Equal(0.4, row.ValueDelta!.Value, precision: 6);

        /* A fraction that moved by one sigma's worth of SESSIONS is stable: 20 → 20.2 of 100. */
        var (sb, sc) = Scored([PgSaturation(20, 100)], [PgSaturation(20.2, 100)]);
        Assert.Equal(ComparisonBanding.StatusStable, Compare(sb, sc, PgBucket(MetricNames.PgSessionCount, 20, 0)).Status);

        var bare = new Fact { Source = PgTargetSources.SessionsSource, Key = PgTargetFactKeys.ConnectionSaturation, Value = 0.6 };
        Assert.Null(ComparisonBanding.BaselinedValueFor(bare));
        var (bb, bc) = Scored([PgSaturation(20, 100)], [bare]);
        var fallback = Compare(bb, bc, PgBucket(MetricNames.PgSessionCount, 20, 0));
        Assert.Equal(ComparisonBanding.BandSourceAbsolute, fallback.BandSource);
        Assert.Null(fallback.DeltaSigma);
        Assert.Equal(MetricNames.PgSessionCount, fallback.BaselineMetric);
    }

    /// <summary>
    /// <c>pg_cpu</c> is percent of the CONFIGURED capacity ceiling; a <c>PG_CPU_PERCENT</c> fact holding the raw
    /// <c>cpu_percent</c> (<c>capacity_measured = 0</c>) is percent of the capacity currently allocated (#3281) —
    /// another unit — and is not sigma-banded. <c>PG_DEADLOCK_RATE</c> IS in its bucket's unit (deadlocks per
    /// hour on both sides), but a healthy server's bucket has median and MAD at 0, sigma 0, and the key takes
    /// the absolute rule exactly as the never-blind gate requires; a non-zero bucket bands it.
    /// </summary>
    [Fact]
    public void PostgresCpu_IsSigmaBandedOnlyInTheCapacityUnit_AndDeadlocks_OnlyAgainstANonZeroBucket()
    {
        var (mb, mc) = Scored([PgCpu(30, measured: true)], [PgCpu(90, measured: true)]);
        var measured = Compare(mb, mc, PgBucket(MetricNames.PgCpu, median: 30, mad: 1));
        Assert.Equal(ComparisonBanding.BandSourceBaseline, measured.BandSource);
        Assert.Equal(MetricNames.PgCpu, measured.BaselineMetric);
        Assert.Equal(ComparisonBanding.StatusWorse, measured.Status);

        var (rb, rc) = Scored([PgCpu(30, measured: false)], [PgCpu(90, measured: false)]);
        Assert.Null(ComparisonBanding.BaselinedValueFor(rb[0]));
        var raw = Compare(rb, rc, PgBucket(MetricNames.PgCpu, median: 30, mad: 1));
        Assert.Equal(ComparisonBanding.BandSourceAbsolute, raw.BandSource);
        Assert.Equal(MetricNames.PgCpu, raw.BaselineMetric);

        var (db, dc) = Scored([PgDeadlocks(0)], [PgDeadlocks(6)]);
        var zero = PgBucket(MetricNames.PgDeadlockRate, median: 0, mad: 0);
        Assert.Equal(0.0, zero.EffectiveRobustSigma);
        var quiet = Compare(db, dc, zero);
        Assert.Equal(ComparisonBanding.BandSourceAbsolute, quiet.BandSource);
        Assert.Equal(MetricNames.PgDeadlockRate, quiet.BaselineMetric);

        var (nb, nc) = Scored([PgDeadlocks(1)], [PgDeadlocks(6)]);
        var busy = Compare(nb, nc, PgBucket(MetricNames.PgDeadlockRate, median: 1, mad: 0.6745));
        Assert.Equal(ComparisonBanding.BandSourceBaseline, busy.BandSource);
        Assert.Equal(5.0, busy.DeltaSigma!.Value, precision: 2);
        Assert.Equal(ComparisonBanding.StatusWorse, busy.Status);

        Assert.Equal(
            new[] { MetricNames.PgCpu, MetricNames.PgDeadlockRate, MetricNames.PgSessionCount, MetricNames.PgTps },
            ComparisonBanding.DispersionMetricsFor([PgTps(1), PgSaturation(1, 10)], [PgCpu(1, true), PgDeadlocks(0)]));
    }

    /// <summary>
    /// The v2 baselined keys (#3691 exit check): the exit check's planted store had <c>PG_IO_READ_LATENCY_MS</c>
    /// 1.55 → 25 ms, <c>PG_REPLICATION_LAG</c> 1 → 200 MB and <c>PG_WAL_VOLUME_SHIFT</c> 1 → 9.7 MB/s all come back
    /// <c>band_source: absolute</c>, <c>baseline_metric: null</c> while their detectors said 25σ. Each is now mapped to
    /// the bucket its detector judges against and, given that bucket, reads <c>worse</c> by sigma. The I/O fact bands
    /// on its value (same ms-per-read quotient as the quarter-hour bucket); the lag fact on its value (peak bytes
    /// behind, the bucket's own <c>MAX</c>); the WAL fact on its PEAK metadata, never its mean value, because the bucket
    /// is per-collection and the detector judges the peak — <c>value_delta</c> stays the mean. The withheld shapes:
    /// an I/O fact that makes no latency claim or states an ungraded quotient, and the WAL <c>unavailable</c> shape
    /// (no peak stamped) — two unavailable WAL sides are <c>stable</c> by the absolute rule with no sigma, the row
    /// still naming the bucket.
    /// </summary>
    [Fact]
    public void TheV2PostgresBaselinedKeys_IoLatency_ReplayLag_AndWalVolume_BandBySigma_AndTheirUnavailableShapesDoNot()
    {
        /* PG_IO_READ_LATENCY_MS: 1.5 → 25 ms against a bucket at 1.5 with MAD 0.06745 (sigma 0.1). */
        var (ib, ic) = Scored([PgIo(1.5)], [PgIo(25)]);
        Assert.Equal(1.5, ComparisonBanding.BaselinedValueFor(ib[0]));
        var before = Assert.Single(ComparisonBanding.Compare(ib, ic, NoDispersion, coverageCaveat: false).Rows);
        Assert.Equal(ComparisonBanding.BandSourceAbsolute, before.BandSource);
        Assert.Equal(MetricNames.PgIoReadLatency, before.BaselineMetric);
        var io = Compare(ib, ic, PgBucket(MetricNames.PgIoReadLatency, median: 1.5, mad: 0.06745));
        Assert.Equal(ComparisonBanding.BandSourceBaseline, io.BandSource);
        Assert.Equal(ComparisonBanding.StatusWorse, io.Status);
        Assert.Equal(MetricNames.PgIoReadLatency, io.BaselineMetric);
        Assert.Equal(0.1, io.BaselineSigma!.Value, precision: 4);
        Assert.Equal(23.5, io.ValueDelta!.Value, precision: 6);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, io.DeltaSigma!.Value, precision: 2); // 235σ raw, capped
        Assert.True(io.BeyondAnomalyCutoff!.Value);
        /* Inside one sigma is stable. */
        var (sb, sc) = Scored([PgIo(1.5)], [PgIo(1.58)]);
        Assert.Equal(ComparisonBanding.StatusStable, Compare(sb, sc, PgBucket(MetricNames.PgIoReadLatency, 1.5, 0.06745)).Status);
        /* No latency claim (timing off) or an ungraded quotient: withheld, absolute, the bucket still named. */
        Assert.Null(ComparisonBanding.BaselinedValueFor(PgIo(0, measured: false)));
        Assert.Null(ComparisonBanding.BaselinedValueFor(PgIo(25, insufficientOps: true)));
        var (ub, uc) = Scored([PgIo(0, measured: false)], [PgIo(25)]);
        var untimed = Compare(ub, uc, PgBucket(MetricNames.PgIoReadLatency, 1.5, 0.06745));
        Assert.Equal(ComparisonBanding.BandSourceAbsolute, untimed.BandSource);
        Assert.Null(untimed.DeltaSigma);
        Assert.Equal(MetricNames.PgIoReadLatency, untimed.BaselineMetric);

        /* PG_REPLICATION_LAG: 1 MB → 200 MB against a bucket at 1 MB with MAD 67,450 bytes (sigma 100,000). */
        var (lb, lc) = Scored([PgLag(1_000_000)], [PgLag(200_000_000)]);
        Assert.Equal(1_000_000.0, ComparisonBanding.BaselinedValueFor(lb[0]));
        var lag = Compare(lb, lc, PgBucket(MetricNames.PgReplayLagBytes, median: 1_000_000, mad: 67_450));
        Assert.Equal(ComparisonBanding.BandSourceBaseline, lag.BandSource);
        Assert.Equal(ComparisonBanding.StatusWorse, lag.Status);
        Assert.Equal(MetricNames.PgReplayLagBytes, lag.BaselineMetric);
        Assert.Equal(100_000.0, lag.BaselineSigma!.Value, precision: 2);
        Assert.Equal(199_000_000.0, lag.ValueDelta!.Value, precision: 2);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, lag.DeltaSigma!.Value, precision: 2);
        var (qb, qc) = Scored([PgLag(1_000_000)], [PgLag(1_050_000)]);
        Assert.Equal(ComparisonBanding.StatusStable, Compare(qb, qc, PgBucket(MetricNames.PgReplayLagBytes, 1_000_000, 67_450)).Status);

        /* PG_WAL_VOLUME_SHIFT: mean 1 → 9.7 MB/s, peak 1.2 → 12 MB/s; the bucket (per-collection bytes/s) at 1 MB/s,
           MAD 67,450 (sigma 100,000). The band is on the PEAK delta; value_delta is the mean's. */
        var (wb, wc) = Scored([PgWal(mean: 1_000_000, peak: 1_200_000)], [PgWal(mean: 9_700_000, peak: 12_000_000)]);
        Assert.Equal(1_200_000.0, ComparisonBanding.BaselinedValueFor(wb[0]));
        Assert.Equal(12_000_000.0, ComparisonBanding.BaselinedValueFor(wc[0]));
        var wal = Compare(wb, wc, PgBucket(MetricNames.PgWalBytesPerSec, median: 1_000_000, mad: 67_450));
        Assert.Equal(ComparisonBanding.BandSourceBaseline, wal.BandSource);
        Assert.Equal(ComparisonBanding.StatusWorse, wal.Status);
        Assert.Equal(MetricNames.PgWalBytesPerSec, wal.BaselineMetric);
        Assert.Equal(8_700_000.0, wal.ValueDelta!.Value, precision: 2);          // the mean, the fact's own unit
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, wal.DeltaSigma!.Value, precision: 2); // (12M − 1.2M) / 100k = 108σ raw
        Assert.True(wal.BeyondAnomalyCutoff!.Value);
        /* A peak inside one sigma is stable however the mean wobbled. */
        var (pb, pc) = Scored([PgWal(1_000_000, 1_200_000)], [PgWal(1_100_000, 1_250_000)]);
        Assert.Equal(ComparisonBanding.StatusStable, Compare(pb, pc, PgBucket(MetricNames.PgWalBytesPerSec, 1_000_000, 67_450)).Status);
        /* Aurora: unavailable on both sides — no peak, no reading, stable by the absolute rule, no sigma. */
        Assert.Null(ComparisonBanding.BaselinedValueFor(PgWalUnavailable()));
        var (ab, ac) = Scored([PgWalUnavailable()], [PgWalUnavailable()]);
        var aurora = Compare(ab, ac, PgBucket(MetricNames.PgWalBytesPerSec, 1_000_000, 67_450));
        Assert.Equal(ComparisonBanding.BandSourceAbsolute, aurora.BandSource);
        Assert.Equal(ComparisonBanding.StatusStable, aurora.Status);
        Assert.Null(aurora.DeltaSigma);
        Assert.Null(aurora.BaselineSigma);
        Assert.Equal(MetricNames.PgWalBytesPerSec, aurora.BaselineMetric);

        /* The seven pg_ metrics a full PostgreSQL set asks for — and only those. */
        Assert.Equal(
            new[] { MetricNames.PgCpu, MetricNames.PgDeadlockRate, MetricNames.PgIoReadLatency, MetricNames.PgReplayLagBytes, MetricNames.PgSessionCount, MetricNames.PgTps, MetricNames.PgWalBytesPerSec },
            ComparisonBanding.DispersionMetricsFor([PgTps(1), PgSaturation(1, 10), PgIo(1), PgLag(1)], [PgCpu(1, true), PgDeadlocks(0), PgWal(1, 1)]));
    }

    /// <summary>
    /// W2: the churn note speaks the engine of the facts. A PostgreSQL fact set (any <c>pg_</c> source) gets the
    /// statement-identity sentence; its <c>PG_BAD_ACTOR_*</c> keys take the presence path and its churn lists
    /// are empty by construction. A SQL Server fact set's note is the sentence it always was, verbatim.
    /// </summary>
    [Fact]
    public void TheChurnNote_SpeaksTheEngineOfTheFacts()
    {
        var (pb, pc) = Scored([PgTps(10), PgBadActor(42, 500)], [PgTps(10), PgBadActor(43, 500)]);
        var pg = ComparisonBanding.Compare(pb, pc, NoDispersion, coverageCaveat: false);
        Assert.True(pg.Churn.PostgresTarget);
        Assert.Empty(pg.Churn.Appeared);
        Assert.Empty(pg.Churn.Disappeared);
        Assert.Equal(0, pg.Churn.PresentInBoth);
        Assert.Contains(pg.Rows, r => r.Key == PgTargetFactKeys.BadActorKey(43) && r.BandSource == ComparisonBanding.BandSourcePresence);
        using (var doc = System.Text.Json.JsonDocument.Parse(System.Text.Json.JsonSerializer.Serialize(pg.Churn.ToPayload())))
        {
            var note = doc.RootElement.GetProperty("note").GetString()!;
            Assert.Equal(PlanCacheChurn.PostgresNote, note);
            Assert.Contains("pg_stat_statements", note, StringComparison.Ordinal);
            Assert.DoesNotContain("BAD_ACTOR_<hash> keys are plan-cache identities", note, StringComparison.Ordinal);
            Assert.DoesNotContain("the cache held a different plan", note, StringComparison.Ordinal);
        }

        var (sb, sc) = Scored([Cpu(50), BadActor("0xA", 500)], [Cpu(50), BadActor("0xB", 500)]);
        var sql = ComparisonBanding.Compare(sb, sc, NoDispersion, coverageCaveat: false);
        Assert.False(sql.Churn.PostgresTarget);
        Assert.Equal(
            "BAD_ACTOR_<hash> keys are plan-cache identities: a hash present in one window only means the cache held a different plan for the top-5 cut, not that a problem began or ended. Not counted in new_issues / resolved_issues.",
            PlanCacheChurn.SqlServerNote);
        using var sqlDoc = System.Text.Json.JsonDocument.Parse(System.Text.Json.JsonSerializer.Serialize(sql.Churn.ToPayload()));
        Assert.Equal(PlanCacheChurn.SqlServerNote, sqlDoc.RootElement.GetProperty("note").GetString());
    }

    /// <summary>
    /// The routing pin: a SQL Server fact set reaches the unit-reconciliation seam and comes out untouched —
    /// every SQL Server key's reading is its value, the four SQL Server metric mappings are what they were, no
    /// <c>pg_</c> bucket is ever requested for a SQL Server set, and the churn note is the SQL Server one. With
    /// the sigma and ladder arithmetic unchanged for an unchanged reading, the fifteen SQL Server scenarios above
    /// are the before/after equality; this test pins the seam they rest on.
    /// </summary>
    [Fact]
    public void ASqlServerFactSet_NeverReachesThePostgresArms()
    {
        var (baseline, comparison) = Scored(
            [Cpu(50), Sessions(100), Io(5), Wait("PAGEIOLATCH_SH", 0.3), Blocking(10), BadActor("0xA", 500)],
            [Cpu(60), Sessions(175), Io(9), Wait("PAGEIOLATCH_SH", 0.6), Blocking(40), BadActor("0xB", 500)]);

        foreach (var fact in baseline.Concat(comparison))
            Assert.Equal(fact.Value, ComparisonBanding.BaselinedValueFor(fact));

        Assert.Equal(MetricNames.Cpu, ComparisonBanding.BaselinedMetricFor("CPU_SQL_PERCENT"));
        Assert.Equal(MetricNames.Cpu, ComparisonBanding.BaselinedMetricFor("CPU_SPIKE"));
        Assert.Equal(MetricNames.IoLatency, ComparisonBanding.BaselinedMetricFor("IO_READ_LATENCY_MS"));
        Assert.Equal(MetricNames.SessionCount, ComparisonBanding.BaselinedMetricFor("SESSION_STATS"));

        var metrics = ComparisonBanding.DispersionMetricsFor(baseline, comparison);
        Assert.Equal(new[] { MetricNames.Cpu, MetricNames.IoLatency, MetricNames.SessionCount }, metrics);
        Assert.DoesNotContain(metrics, m => m.StartsWith(PgTargetSources.Prefix, StringComparison.Ordinal));

        var result = ComparisonBanding.Compare(baseline, comparison, NoDispersion, coverageCaveat: false);
        Assert.False(result.Churn.PostgresTarget);
        Assert.All(result.Rows, r => Assert.Equal(ComparisonBanding.BandSourceAbsolute, r.BandSource));
    }

    /* ── helpers ── */

    private static Fact Wait(string type, double fraction) => new()
    {
        Source = "waits", Key = type, Value = fraction,
        Metadata = new Dictionary<string, double> { ["wait_time_ms"] = fraction * 14_400_000, ["period_duration_ms"] = 14_400_000 }
    };

    private static Fact Cpu(double avgPercent) => new() { Source = "cpu", Key = "CPU_SQL_PERCENT", Value = avgPercent };

    private static Fact Blocking(double eventsPerHour) => new()
    {
        Source = "blocking", Key = "BLOCKING_EVENTS", Value = eventsPerHour,
        Metadata = new Dictionary<string, double> { ["event_count"] = eventsPerHour * 4, ["period_hours"] = 4, ["observed_hours"] = 4 }
    };

    private static Fact Io(double avgReadMs) => new() { Source = "io", Key = "IO_READ_LATENCY_MS", Value = avgReadMs };

    private static Fact Sessions(double total) => new() { Source = "sessions", Key = "SESSION_STATS", Value = total };

    private static Fact BadActor(string hash, double avgCpuMs) => new()
    {
        Source = "bad_actor", Key = $"BAD_ACTOR_{hash}", Value = avgCpuMs, DatabaseName = "db",
        Metadata = new Dictionary<string, double> { ["execution_count"] = 5_000, ["avg_cpu_ms"] = avgCpuMs, ["avg_reads"] = 100 }
    };

    /// <summary>Scores both lists with the real scorer — the ladder positions are the scorer's, never hand-set.</summary>
    private static (List<Fact>, List<Fact>) Scored(List<Fact> baseline, List<Fact> comparison)
    {
        var scorer = new FactScorer();
        scorer.ScoreAll(baseline);
        scorer.ScoreAll(comparison);
        return (baseline, comparison);
    }

    private static ComparisonRow Compare(List<Fact> baseline, List<Fact> comparison, BaselineBucket bucket)
    {
        var metric = ComparisonBanding.BaselinedMetricFor(comparison[0].Key)!;
        var dispersion = new Dictionary<string, BaselineBucket> { [metric] = bucket };
        return Assert.Single(ComparisonBanding.Compare(baseline, comparison, dispersion, coverageCaveat: false).Rows);
    }

    /// <summary>A trustworthy Full-tier CPU bucket: 20 samples over 5 distinct days (2x the tier's sample floor).</summary>
    private static BaselineBucket CpuBucket(double median, double mad) => new()
    {
        HourOfDay = 9, DayOfWeek = 2, Tier = BaselineTier.Full,
        Mean = median, StdDev = Math.Max(mad, 1), Median = median, Mad = mad, SampleCount = 20, DistinctDays = 5,
        AbsStdDevFloor = BaselineMath.AbsStdDevFloorFor(MetricNames.Cpu)
    };

    private static BaselineBucket SessionsBucket(double median, double mad) => new()
    {
        HourOfDay = 9, DayOfWeek = 2, Tier = BaselineTier.Full,
        Mean = median, StdDev = Math.Max(mad, 1), Median = median, Mad = mad, SampleCount = 20, DistinctDays = 5,
        AbsStdDevFloor = BaselineMath.AbsStdDevFloorFor(MetricNames.SessionCount)
    };

    /* ── PostgreSQL-target facts, keyed and sourced from the shared vocabulary (never literals) ── */

    private static Fact PgTps(double tps) => new() { Source = PgTargetSources.DatabaseSource, Key = PgTargetFactKeys.Tps, Value = tps };

    private static Fact PgDeadlocks(double perHour) => new() { Source = PgTargetSources.DatabaseSource, Key = PgTargetFactKeys.DeadlockRate, Value = perHour };

    /// <summary>The collector's shape: value = peak ÷ usable (a fraction), the peak COUNT in <c>peak_total_sessions</c>.</summary>
    private static Fact PgSaturation(double peak, double usable) => new()
    {
        Source = PgTargetSources.SessionsSource, Key = PgTargetFactKeys.ConnectionSaturation, Value = peak / usable,
        Metadata = new Dictionary<string, double> { ["peak_total_sessions"] = peak, ["usable_connections"] = usable, ["max_connections"] = usable + 3 }
    };

    private static Fact PgCpu(double percent, bool measured) => new()
    {
        Source = PgTargetSources.CpuSource, Key = PgTargetFactKeys.CpuPercent, Value = percent,
        Metadata = new Dictionary<string, double> { [PgTargetScorer.CpuCapacityMeasuredKey] = measured ? 1 : 0 }
    };

    /// <summary>The I/O collector's shape (<c>PgTargetFactCollector.Io.cs</c>): value = window read ms ÷ reads;
    /// <c>latency_measured</c> 0 when timing is off or <c>pg_stat_io</c> is absent; <c>insufficient_ops</c> 1 when the
    /// quotient is stated but not graded.</summary>
    private static Fact PgIo(double msPerRead, bool measured = true, bool insufficientOps = false) => new()
    {
        Source = PgTargetSources.IoSource, Key = PgTargetFactKeys.IoReadLatencyMs, Value = msPerRead,
        Metadata = new Dictionary<string, double>
        {
            [PgTargetScorer.IoLatencyMeasuredKey] = measured ? 1 : 0,
            [PgTargetScorer.IoUnavailableKey] = measured ? 0 : 1,
            [PgTargetScorer.IoInsufficientOpsKey] = insufficientOps ? 1 : 0,
            [PgTargetScorer.IoOpsKey] = insufficientOps ? 100 : 26_400,
        }
    };

    /// <summary>The replication collector's shape: value = the window's peak <c>replay_bytes_behind</c>, the same figure
    /// in <c>replay_bytes_behind_peak</c>.</summary>
    private static Fact PgLag(double peakBytes) => new()
    {
        Source = PgTargetSources.ReplicationSource, Key = PgTargetFactKeys.ReplicationLag, Value = peakBytes,
        Metadata = new Dictionary<string, double> { [PgTargetScorer.LagPeakBytesKey] = peakBytes, [PgTargetScorer.LagLatestBytesKey] = peakBytes }
    };

    /// <summary>The write collector's tracked WAL shape: value = the window's MEAN bytes/s, the peak beside it.</summary>
    private static Fact PgWal(double mean, double peak) => new()
    {
        Source = PgTargetSources.WriteSource, Key = PgTargetFactKeys.WalVolumeShift, Value = mean,
        Metadata = new Dictionary<string, double>
        {
            ["wal_tracked"] = 1, ["avg_wal_bytes_per_sec"] = mean, ["peak_wal_bytes_per_sec"] = peak, ["threshold_lineage"] = 1
        }
    };

    /// <summary>The write collector's <c>unavailable</c> WAL shape (Aurora): value 0, no peak stamped.</summary>
    private static Fact PgWalUnavailable() => new()
    {
        Source = PgTargetSources.WriteSource, Key = PgTargetFactKeys.WalVolumeShift, Value = 0,
        Metadata = new Dictionary<string, double> { ["wal_tracked"] = 0, ["unavailable"] = 1, ["reason_wal_stats_not_reported"] = 1, ["threshold_lineage"] = 1 }
    };

    private static Fact PgBadActor(long queryId, double totalMs) => new()
    {
        Source = PgTargetSources.QueriesSource, Key = PgTargetFactKeys.BadActorKey(queryId), Value = totalMs, DatabaseName = "appdb"
    };

    /// <summary>A trustworthy Full-tier PostgreSQL bucket for <paramref name="metric"/>; the PG metrics have no absolute floor, so sigma is MAD / 0.6745 floored at 1% of the median.</summary>
    private static BaselineBucket PgBucket(string metric, double median, double mad) => new()
    {
        HourOfDay = 9, DayOfWeek = 2, Tier = BaselineTier.Full,
        Mean = median, StdDev = Math.Max(mad, 1), Median = median, Mad = mad, SampleCount = 20, DistinctDays = 5,
        AbsStdDevFloor = BaselineMath.AbsStdDevFloorFor(metric)
    };
}
