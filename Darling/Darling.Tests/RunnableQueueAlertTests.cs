/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Runnable Queue alert (#5641) in the shared engine: runnable tasks per visible online scheduler, the average of
/// the stored samples over five minutes, Warning at 8 and Critical at 16 by default, and no verdict on fewer than three
/// samples. The read adapters' own SQL is pinned in <c>RunnableQueueReadAdapterTests</c> and
/// <c>RunnableQueueReadLiveTests</c>.
/// </summary>
public sealed class RunnableQueueAlertTests
{
    private static readonly DateTime Collected = new(2026, 10, 9, 14, 0, 0, DateTimeKind.Unspecified);

    private static RunnableQueueWindow Window(double average, long samples = 5, int schedulers = 4, DateTime? collected = null) =>
        new(samples, average, schedulers, (long)Math.Round(average * schedulers), collected ?? Collected);

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
    public void Evaluate_GradesTheWindowAverageAgainstTheDefaultBars(double average, AlertSeverityLevel? expected)
    {
        var finding = RunnableQueueEvaluator.Evaluate(Window(average), 8, 16);

        Assert.Equal(expected, finding?.Severity);
    }

    [Fact]
    public void Evaluate_AFloatingPointAverageSittingOnABar_StillCountsAsAtTheBar()
    {
        /* AVG over two-decimal samples can land a hair under the bar; the compare is at the collector's precision. */
        Assert.Equal(AlertSeverityLevel.Warning, RunnableQueueEvaluator.Evaluate(Window(7.999999999), 8, 16)!.Severity);
        Assert.Equal(AlertSeverityLevel.Critical, RunnableQueueEvaluator.Evaluate(Window(15.9999999), 8, 16)!.Severity);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void Evaluate_FewerThanThreeSamples_DoesNotEvaluate_EvenOnAHugeAverage(long samples)
    {
        var window = Window(228.0, samples);

        Assert.False(RunnableQueueEvaluator.IsEvaluable(window));
        Assert.Null(RunnableQueueEvaluator.Evaluate(window, 8, 16));
    }

    [Fact]
    public void Evaluate_ExactlyThreeSamples_Evaluates()
    {
        Assert.True(RunnableQueueEvaluator.IsEvaluable(Window(1.0, 3)));
        Assert.NotNull(RunnableQueueEvaluator.Evaluate(Window(20.0, 3), 8, 16));
    }

    [Fact]
    public void Evaluate_UsesTheBarsItIsGiven_AndNeverAWarningAboveTheCritical()
    {
        Assert.Equal(AlertSeverityLevel.Warning, RunnableQueueEvaluator.Evaluate(Window(5.0), 4, 6)!.Severity);
        Assert.Equal(AlertSeverityLevel.Critical, RunnableQueueEvaluator.Evaluate(Window(6.0), 4, 6)!.Severity);

        /* Critical below Warning is lifted to Warning; a Warning of zero is lifted to 1. */
        Assert.Equal((10, 10), RunnableQueueEvaluator.ClampBars(10, 3));
        Assert.Equal((1, 16), RunnableQueueEvaluator.ClampBars(0, 16));
        Assert.Equal((1, 1), RunnableQueueEvaluator.ClampBars(-5, -5));
        Assert.Equal((8, 16), RunnableQueueEvaluator.ClampBars(
            RunnableQueueEvaluator.DefaultWarningPerScheduler, RunnableQueueEvaluator.DefaultCriticalPerScheduler));
    }

    [Fact]
    public void TheSummary_IsPlainAndConcrete()
    {
        Assert.Equal(
            "Runnable tasks averaged 23.4 per scheduler over 5 minutes (4 schedulers).",
            RunnableQueueEvaluator.Summary(23.4, 4));
        Assert.Equal(
            "Runnable tasks averaged 9.0 per scheduler over 5 minutes (1 scheduler).",
            RunnableQueueEvaluator.Summary(9.0, 1));
        Assert.Equal(
            "Runnable tasks averaged 9.0 per scheduler over 5 minutes.",
            RunnableQueueEvaluator.Summary(9.0, null));
    }

    [Fact]
    public void TheDefaults_AreTheMeasuredBars()
    {
        Assert.Equal(8, RunnableQueueEvaluator.DefaultWarningPerScheduler);
        Assert.Equal(16, RunnableQueueEvaluator.DefaultCriticalPerScheduler);
        Assert.Equal(5, RunnableQueueEvaluator.WindowMinutes);
        Assert.Equal(3, RunnableQueueEvaluator.MinimumSamples);
        Assert.Equal("Runnable Queue", RunnableQueueEvaluator.MetricName);
    }

    /* ---------------- the engine ---------------- */

    [Fact]
    public async Task Disabled_ReadsNothingAndFiresNothing()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.RunnableQueueEnabled = false;
        h.Adapter.RunnableQueue = Window(57.0);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(0, h.Adapter.RunnableQueueReads);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task BelowTheWarningBar_Stays_Silent_AndAsksForTheFiveMinuteWindow()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Window(7.9);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(RunnableQueueEvaluator.WindowMinutes, h.Adapter.RunnableQueueWindowMinutesAsked);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ExactlyAtTheWarningBar_FiresWarning_WithThePlainSummary()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Window(8.0);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Runnable Queue", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal(8.0, fired.NumericCurrentValue);
        Assert.Equal(8.0, fired.NumericThresholdValue);
        Assert.Equal("8.0 per scheduler", fired.CurrentValue);
        Assert.Equal("8 per scheduler", fired.ThresholdValue);
        Assert.Equal("Runnable tasks averaged 8.0 per scheduler over 5 minutes (4 schedulers).", fired.ShortMessage);
        Assert.NotNull(fired.Context);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Context!.SeverityOverride);
        Assert.False(fired.Muted);
    }

    [Fact]
    public async Task ExactlyAtTheCriticalBar_FiresCritical_AgainstTheCriticalBar()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Window(16.0);

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
        h.Adapter.RunnableQueue = new RunnableQueueWindow(5, 57.0, 4, 228, Collected);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal("Runnable tasks averaged 57.0 per scheduler over 5 minutes (4 schedulers).", fired.ShortMessage);
    }

    [Fact]
    public async Task ConfiguredBars_GovernTheGrade()
    {
        var h = Armed();
        h.Settings.RunnableQueueWarningPerScheduler = 3;
        h.Settings.RunnableQueueCriticalPerScheduler = 5;
        h.Adapter.RunnableQueue = Window(4.0);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal(3.0, fired.NumericThresholdValue);
    }

    [Fact]
    public async Task FewerThanThreeSamples_FiresNothing()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Window(57.0, samples: 2);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task FewerThanThreeSamples_HoldsAStandingAlert_AndOnlyAnObservedQuietWindowClearsIt()
    {
        var h = Armed();
        var engine = h.Build();

        h.Adapter.RunnableQueue = Window(20.0);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* A collection gap: two samples, even at zero, is not an observation. Nothing cleared. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Window(0.0, samples: 2, collected: Collected.AddMinutes(6));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Resolutions);

        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = RunnableQueueWindow.Empty;
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Resolutions);

        /* Five samples averaging 2: observed and quiet. Cleared, once. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Window(2.0, collected: Collected.AddMinutes(18));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("Runnable Queue Cleared", resolution.Title);
        Assert.Equal(
            "SRV-A: Runnable tasks per scheduler averaged over the last 5 minutes are back below threshold",
            resolution.Message);
        Assert.Single(h.Deliverer.Outcomes);

        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Resolutions);
    }

    [Fact]
    public async Task ADropToAnUnfiredAlertLevel_NeverAnnouncesACleared()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Window(2.0);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task Cooldown_HoldsARepeat_AndAFreshSampleAfterTheCooldownFiresAgain()
    {
        var h = Armed();
        var engine = h.Build();

        h.Adapter.RunnableQueue = Window(20.0);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Inside the cooldown, even with a newer sample. */
        h.Now = h.Now.AddMinutes(1);
        h.Adapter.RunnableQueue = Window(20.0, collected: Collected.AddMinutes(1));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Cooldown elapsed but the store has handed back the SAME newest sample: not a fresh observation. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Window(20.0, collected: Collected);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* Cooldown elapsed and a newer sample: fires again. */
        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Window(20.0, collected: Collected.AddMinutes(14));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task AMutedAlert_IsStillRecorded_FlaggedMuted_AndTheMuteAsksAboutThisMetric()
    {
        var h = Armed();
        AlertMuteContext? asked = null;
        h.IsMuted = ctx => { asked = ctx; return true; };
        h.Adapter.RunnableQueue = Window(20.0);

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
        h.Adapter.RunnableQueue = Window(20.0);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddMinutes(6);
        h.Adapter.RunnableQueue = Window(1.0, collected: Collected.AddMinutes(6));
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
