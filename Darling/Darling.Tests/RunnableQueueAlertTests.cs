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
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Runnable Queue alert (#5641) in the shared engine: runnable tasks per visible online scheduler, the average of
/// the stored collections (3 or more in the last 5 minutes, else the newest 3 if all are inside 10 minutes), Warning at
/// 8 and Critical at 16 by default, and no verdict when neither rule holds. The read adapters' own SQL is pinned in
/// <c>DarlingAlertReadAdapterTests</c> and <c>DarlingRunnableQueueReadLiveTests</c>; the 2026-10-09 incident replay is
/// <c>RunnableQueueIncidentReplayTests</c>.
/// </summary>
public sealed class RunnableQueueAlertTests
{
    /// <summary>The harness clock's default evaluation time.</summary>
    private static readonly DateTime T0 = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// <paramref name="count"/> collections one minute apart, the newest at <paramref name="newest"/>, each at
    /// <paramref name="average"/> per scheduler. Newest first, like the read.
    /// </summary>
    private static List<RunnableQueueSample> Samples(double average, DateTime newest, int count = 5, int schedulers = 4) =>
        Enumerable.Range(0, count)
            .Select(i => new RunnableQueueSample(
                newest.AddMinutes(-i), average, (long)Math.Round(average * schedulers), schedulers))
            .ToList();

    /// <summary>Collections at the exact times given (newest first), all at one average.</summary>
    private static List<RunnableQueueSample> SamplesAt(double average, params DateTime[] times) =>
        times.Select(t => new RunnableQueueSample(t, average, (long)Math.Round(average * 4), 4)).ToList();

    private static RunnableQueueReading? ReadAt(double average, int count) =>
        RunnableQueueEvaluator.Read(Samples(average, T0.AddSeconds(-10), count), T0);

    private static AlertEngineTests.Harness Armed()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.RunnableQueueEnabled = true;
        return h;
    }

    /* ---------------- the grading, without an engine ---------------- */

    [Theory]
    [InlineData(0.0, null)]
    [InlineData(3.99, null)]
    [InlineData(7.99, null)]
    [InlineData(8.0, AlertSeverityLevel.Warning)]
    [InlineData(12.5, AlertSeverityLevel.Warning)]
    [InlineData(15.99, AlertSeverityLevel.Warning)]
    [InlineData(16.0, AlertSeverityLevel.Critical)]
    [InlineData(57.0, AlertSeverityLevel.Critical)]
    public void Evaluate_GradesTheAverageAgainstTheDefaultBars(double average, AlertSeverityLevel? expected)
    {
        var finding = RunnableQueueEvaluator.Evaluate(ReadAt(average, 5), 8, 16);

        Assert.Equal(expected, finding?.Severity);
    }

    [Fact]
    public void Evaluate_AFloatingPointAverageSittingOnABar_StillCountsAsAtTheBar()
    {
        /* AVG over two-decimal samples can land a hair under the bar; the compare is at the collector's precision. */
        Assert.Equal(AlertSeverityLevel.Warning, RunnableQueueEvaluator.Evaluate(ReadAt(7.999999999, 5), 8, 16)!.Severity);
        Assert.Equal(AlertSeverityLevel.Critical, RunnableQueueEvaluator.Evaluate(ReadAt(15.9999999, 5), 8, 16)!.Severity);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void Read_FewerThanThreeCollections_IsNotAnObservation_EvenOnAHugeAverage(int count)
    {
        var reading = ReadAt(228.0, count);

        Assert.Null(reading);
        Assert.Null(RunnableQueueEvaluator.Evaluate(reading, 8, 16));
    }

    [Fact]
    public void Read_NullSamples_IsNotAnObservation()
    {
        Assert.Null(RunnableQueueEvaluator.Read(null, T0));
    }

    [Fact]
    public void Read_ExactlyThreeCollections_Evaluates()
    {
        Assert.NotNull(ReadAt(1.0, 3));
        Assert.NotNull(RunnableQueueEvaluator.Evaluate(ReadAt(20.0, 3), 8, 16));
    }

    [Fact]
    public void Evaluate_UsesTheBarsItIsGiven_AndNeverAWarningAboveTheCritical()
    {
        Assert.Equal(AlertSeverityLevel.Warning, RunnableQueueEvaluator.Evaluate(ReadAt(5.0, 5), 4, 6)!.Severity);
        Assert.Equal(AlertSeverityLevel.Critical, RunnableQueueEvaluator.Evaluate(ReadAt(6.0, 5), 4, 6)!.Severity);

        /* Critical below Warning is lifted to Warning; a Warning of zero is lifted to 1. */
        Assert.Equal((10, 10), RunnableQueueEvaluator.ClampBars(10, 3));
        Assert.Equal((1, 16), RunnableQueueEvaluator.ClampBars(0, 16));
        Assert.Equal((1, 1), RunnableQueueEvaluator.ClampBars(-5, -5));
        Assert.Equal((8, 16), RunnableQueueEvaluator.ClampBars(
            RunnableQueueEvaluator.DefaultWarningPerScheduler, RunnableQueueEvaluator.DefaultCriticalPerScheduler));
    }

    /* ---------------- the window rule ---------------- */

    [Fact]
    public void Read_ThreeOrMoreCollectionsInFiveMinutes_AveragesAllOfThem()
    {
        /* Four inside the 5 minutes (1, 2, 3, 4 per scheduler) and one older, ignored. */
        var samples = SamplesAt(0, T0.AddSeconds(-30), T0.AddSeconds(-90), T0.AddSeconds(-150), T0.AddSeconds(-210), T0.AddSeconds(-400));
        samples = new List<RunnableQueueSample>
        {
            samples[0] with { PerScheduler = 1 }, samples[1] with { PerScheduler = 2 },
            samples[2] with { PerScheduler = 3 }, samples[3] with { PerScheduler = 4 },
            samples[4] with { PerScheduler = 100 },
        };

        var reading = RunnableQueueEvaluator.Read(samples, T0)!;

        Assert.Equal(2.5, reading.AveragePerScheduler, 6);
        Assert.Equal(4, reading.SampleCount);
        Assert.Equal(T0.AddSeconds(-30), reading.NewestCollectionTime);
    }

    [Fact]
    public void Read_TheFiveMinuteFloorIsInclusive()
    {
        var onTheFloor = SamplesAt(10, T0.AddSeconds(-30), T0.AddSeconds(-90), T0.AddMinutes(-5));
        var justUnder = SamplesAt(10, T0.AddSeconds(-30), T0.AddSeconds(-90), T0.AddMinutes(-5).AddTicks(-1));

        Assert.Equal(3, RunnableQueueEvaluator.Read(onTheFloor, T0)!.SampleCount);

        /* One tick older falls out of the 5-minute window; the fallback still takes the newest 3, still 3. */
        Assert.Equal(3, RunnableQueueEvaluator.Read(justUnder, T0)!.SampleCount);
    }

    [Fact]
    public void Read_TwoCollectionsInFiveMinutes_FallsBackToTheNewestThreeInsideTenMinutes()
    {
        /* 10 per scheduler on the two fresh ones, 40 on the third at 8 minutes: all three are inside 10 minutes. */
        var samples = new List<RunnableQueueSample>
        {
            new(T0.AddSeconds(-20), 10, 40, 4),
            new(T0.AddMinutes(-2), 10, 40, 4),
            new(T0.AddMinutes(-8), 40, 160, 4),
            new(T0.AddMinutes(-9), 1000, 4000, 4),
        };

        var reading = RunnableQueueEvaluator.Read(samples, T0)!;

        Assert.Equal(20.0, reading.AveragePerScheduler, 6);
        Assert.Equal(3, reading.SampleCount);
        Assert.Equal(8, reading.SpanMinutes);
        Assert.Equal(T0.AddSeconds(-20), reading.NewestCollectionTime);
    }

    [Fact]
    public void Read_TheNewestThreeNotAllInsideTenMinutes_IsNotAnObservation()
    {
        var samples = SamplesAt(57, T0.AddSeconds(-20), T0.AddMinutes(-2), T0.AddMinutes(-10).AddTicks(-1), T0.AddMinutes(-11));

        Assert.Null(RunnableQueueEvaluator.Read(samples, T0));
    }

    [Fact]
    public void Read_TheTenMinuteFloorIsInclusive()
    {
        var samples = SamplesAt(57, T0.AddSeconds(-20), T0.AddMinutes(-2), T0.AddMinutes(-10));

        Assert.NotNull(RunnableQueueEvaluator.Read(samples, T0));
    }

    [Fact]
    public void Read_SamplesInAnyOrder_ReadTheSame()
    {
        var newestFirst = SamplesAt(12, T0.AddSeconds(-20), T0.AddMinutes(-2), T0.AddMinutes(-4));
        var shuffled = new List<RunnableQueueSample> { newestFirst[1], newestFirst[2], newestFirst[0] };

        Assert.Equal(RunnableQueueEvaluator.Read(newestFirst, T0), RunnableQueueEvaluator.Read(shuffled, T0));
    }

    [Fact]
    public void Read_CollectionsThreeToFourMinutesApart_StillEvaluateThroughTheFallback()
    {
        /* A late collector from the start of an incident: no 3 collections ever land inside 5 minutes. */
        var samples = SamplesAt(30, T0.AddSeconds(-10), T0.AddSeconds(-10).AddMinutes(-3.5), T0.AddSeconds(-10).AddMinutes(-7));

        var finding = RunnableQueueEvaluator.Evaluate(RunnableQueueEvaluator.Read(samples, T0), 8, 16);

        Assert.Equal(AlertSeverityLevel.Critical, finding!.Severity);
    }

    [Fact]
    public void TheSummary_IsPlainAndConcrete()
    {
        Assert.Equal(
            "Runnable tasks averaged 23.6 per scheduler over the last 3 collections (5 minutes, 4 schedulers).",
            RunnableQueueEvaluator.Summary(23.58, 3, 5, 4));
        Assert.Equal(
            "Runnable tasks averaged 9.0 per scheduler over the last 5 collections (1 minute, 1 scheduler).",
            RunnableQueueEvaluator.Summary(9.0, 5, 1, 1));
        Assert.Equal(
            "Runnable tasks averaged 9.0 per scheduler over the last 4 collections (4 minutes).",
            RunnableQueueEvaluator.Summary(9.0, 4, 4, null));
    }

    [Fact]
    public void TheDefaults_AreTheMeasuredBars()
    {
        Assert.Equal(8, RunnableQueueEvaluator.DefaultWarningPerScheduler);
        Assert.Equal(16, RunnableQueueEvaluator.DefaultCriticalPerScheduler);
        Assert.Equal(5, RunnableQueueEvaluator.WindowMinutes);
        Assert.Equal(10, RunnableQueueEvaluator.FallbackMinutes);
        Assert.Equal(3, RunnableQueueEvaluator.MinimumSamples);
        Assert.Equal("Runnable Queue", RunnableQueueEvaluator.MetricName);
    }

    /* ---------------- the engine ---------------- */

    [Fact]
    public async Task Disabled_ReadsNothingAndFiresNothing()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.RunnableQueueEnabled = false;
        h.Adapter.RunnableQueue = Samples(57.0, h.Now.AddSeconds(-10));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(0, h.Adapter.RunnableQueueReads);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task BelowTheWarningBar_Stays_Silent_AndAsksForTheTenMinuteLookback()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Samples(7.9, h.Now.AddSeconds(-10));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(RunnableQueueEvaluator.FallbackMinutes, h.Adapter.RunnableQueueLookbackMinutesAsked);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ExactlyAtTheWarningBar_FiresWarning_WithThePlainSummary()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Samples(8.0, h.Now.AddSeconds(-10));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Runnable Queue", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal(8.0, fired.NumericCurrentValue);
        Assert.Equal(8.0, fired.NumericThresholdValue);
        Assert.Equal("8.0 per scheduler", fired.CurrentValue);
        Assert.Equal("8 per scheduler", fired.ThresholdValue);
        Assert.Equal("Runnable tasks averaged 8.0 per scheduler over the last 5 collections (4 minutes, 4 schedulers).", fired.ShortMessage);
        Assert.NotNull(fired.Context);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Context!.SeverityOverride);
        Assert.False(fired.Muted);
    }

    [Fact]
    public async Task ExactlyAtTheCriticalBar_FiresCritical_AgainstTheCriticalBar()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Samples(16.0, h.Now.AddSeconds(-10));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Context!.SeverityOverride);
        Assert.Equal(16.0, fired.NumericThresholdValue);
        Assert.Equal("16 per scheduler", fired.ThresholdValue);
    }

    [Fact]
    public async Task TheIncident_57PerScheduler_OnFourSchedulers_FiresCriticalWithTheNumbers()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Samples(57.0, h.Now.AddSeconds(-10));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal("Runnable tasks averaged 57.0 per scheduler over the last 5 collections (4 minutes, 4 schedulers).", fired.ShortMessage);
    }

    [Fact]
    public async Task ASparseCollector_ThreeToFourMinutesApart_StillFiresThroughTheFallback()
    {
        var h = Armed();
        var newest = h.Now.AddSeconds(-10);
        h.Adapter.RunnableQueue = SamplesAt(30.0, newest, newest.AddMinutes(-3.5), newest.AddMinutes(-7));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal("Runnable tasks averaged 30.0 per scheduler over the last 3 collections (7 minutes, 4 schedulers).", fired.ShortMessage);
    }

    [Fact]
    public async Task TheCardsCpuItem_CoversTheSpanTheFallbackJudged_NotJustFiveMinutes()
    {
        var h = Armed();
        var newest = h.Now.AddSeconds(-10);
        h.Adapter.RunnableQueue = SamplesAt(30.0, newest, newest.AddMinutes(-3.5), newest.AddMinutes(-7));
        /* A CPU sample six minutes old: outside a fixed 5-minute window, inside the 7 minutes the queue average covers. */
        var snapshot = AlertEngineTests.Harness.Snapshot(sqlCpu: 50, totalCpu: 55) with
        {
            RecentCpuSamples = new List<AlertCpuSample> { new(h.Now.AddMinutes(-6), 90, 95) },
        };

        await h.Build().EvaluateServerAsync(snapshot);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        var cpu = Assert.Single(fired.Context!.Details, d => d.Heading.StartsWith("CPU over the last", StringComparison.Ordinal));
        Assert.Equal("CPU over the last 7 minutes", cpu.Heading);
    }

    [Fact]
    public async Task ConfiguredBars_GovernTheGrade()
    {
        var h = Armed();
        h.Settings.RunnableQueueWarningPerScheduler = 3;
        h.Settings.RunnableQueueCriticalPerScheduler = 5;
        h.Adapter.RunnableQueue = Samples(4.0, h.Now.AddSeconds(-10));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal(3.0, fired.NumericThresholdValue);
    }

    [Fact]
    public async Task FewerThanThreeCollections_FiresNothing()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Samples(57.0, h.Now.AddSeconds(-10), count: 2);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task NoObservation_HoldsAStandingAlert_AndOnlyAnObservedQuietReadingClearsIt()
    {
        var h = Armed();
        var engine = h.Build();

        h.Adapter.RunnableQueue = Samples(20.0, h.Now.AddSeconds(-10));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* A collection gap: two collections, even at zero, is not an observation. Nothing cleared. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Samples(0.0, h.Now.AddSeconds(-10), count: 2);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Resolutions);

        /* Three collections, but the oldest of them is outside even the 10-minute fallback: still no observation. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = SamplesAt(0.0, h.Now.AddSeconds(-10), h.Now.AddMinutes(-5), h.Now.AddMinutes(-11));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Resolutions);

        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = new List<RunnableQueueSample>();
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Resolutions);

        /* Five collections averaging 2: observed and quiet. Cleared, once. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Samples(2.0, h.Now.AddSeconds(-10));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("Runnable Queue Cleared", resolution.Title);
        Assert.Equal(
            "SRV-A: Runnable tasks per scheduler averaged over the last 5 collections are back below threshold",
            resolution.Message);
        Assert.Single(h.Deliverer.Outcomes);

        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Resolutions);
    }

    [Fact]
    public async Task ADropToAnUnfiredAlertLevel_NeverAnnouncesACleared()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Samples(2.0, h.Now.AddSeconds(-10));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task Cooldown_HoldsARepeat_AndAFreshCollectionAfterTheCooldownFiresAgain()
    {
        var h = Armed();
        var engine = h.Build();
        var first = h.Now.AddSeconds(-10);

        h.Adapter.RunnableQueue = Samples(20.0, first);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Inside the cooldown, even with a newer collection. */
        h.Now = h.Now.AddMinutes(1);
        h.Adapter.RunnableQueue = Samples(20.0, h.Now.AddSeconds(-10));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Cooldown elapsed but the store has handed back the SAME newest collection (the fallback still sees three
           inside 10 minutes): not a fresh observation. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Samples(20.0, first);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Cooldown elapsed and a newer collection: fires again. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Samples(20.0, h.Now.AddSeconds(-10));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AMutedAlert_IsStillRecorded_FlaggedMuted_AndTheMuteAsksAboutThisMetric()
    {
        var h = Armed();
        AlertMuteContext? asked = null;
        h.IsMuted = ctx => { asked = ctx; return true; };
        h.Adapter.RunnableQueue = Samples(20.0, h.Now.AddSeconds(-10));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(fired.Muted);
        Assert.NotNull(asked);
        Assert.Equal("Runnable Queue", asked!.MetricName);
        Assert.Equal("SRV-A", asked.ServerName);
        Assert.Null(asked.WaitType);
    }

    [Fact]
    public async Task ASuppressedSweep_FiresNothing_AndClearsNothing()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.RunnableQueue = Samples(20.0, h.Now.AddSeconds(-10));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Samples(1.0, h.Now.AddSeconds(-10));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot(suppressed: true));

        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public void TheHistoryGrid_RendersTheValueWithItsUnit_AndTheMetricIsAPerformanceAlert()
    {
        Assert.Equal("23.4 per scheduler", AlertMetricClassifier.FormatHistoryValue("Runnable Queue", 23.4));
        Assert.Equal(AlertFamily.Performance, AlertFamily.MetricFamilies["Runnable Queue"]);
        Assert.False(AlertMetricClassifier.IsResolution("Runnable Queue"));
        Assert.True(AlertMetricClassifier.IsResolution("Runnable Queue Cleared"));
        Assert.False(AlertMetricClassifier.IsCritical("Runnable Queue"));
        Assert.True(AlertMetricClassifier.IsWarning("Runnable Queue"));
    }
}
