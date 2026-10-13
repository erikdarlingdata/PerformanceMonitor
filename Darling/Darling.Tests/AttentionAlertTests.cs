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
/// The Client Timeouts alert (#5640), lane C: the pure evaluator (count model, covered span, not-observed rule, proportional
/// mute, bars) and the engine's check through the shared harness the Runnable Queue tests use.
/// </summary>
public sealed class AttentionAlertTests
{
    private static readonly DateTime T0 = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>Successful runs one minute apart, newest first, the newest at <paramref name="newest"/>.</summary>
    private static List<DateTime> Runs(DateTime newest, int count, int gapMinutes = 1) =>
        Enumerable.Range(0, count).Select(i => newest.AddMinutes(-i * gapMinutes)).ToList();

    private static AttentionWindow Window(
        IEnumerable<DateTime> runs, IEnumerable<AttentionCountRow>? counts = null, IEnumerable<AttentionSampleGroup>? samples = null) =>
        new(runs.ToList(), (counts ?? Array.Empty<AttentionCountRow>()).ToList(), (samples ?? Array.Empty<AttentionSampleGroup>()).ToList(),
            Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>(), Array.Empty<AttentionTopItem>());

    /// <summary>A count row stamped a couple of seconds after the run, like the runner stamps its rows.</summary>
    private static AttentionCountRow Count(DateTime run, long count) => new(run.AddSeconds(2), count);

    private static AttentionSampleGroup Sample(DateTime run, string? db, string? app, long n) => new(run.AddSeconds(2), db, app, n);

    private static AttentionWindow Steady(long perRun, int runCount = 10) =>
        Window(Runs(T0.AddSeconds(-5), runCount), Runs(T0.AddSeconds(-5), runCount).Select(r => Count(r, perRun)));

    /* ---------------- pure evaluator ---------------- */

    [Fact]
    public void QuietServer_RunsButNoCountRows_IsObservedAsZero_AndGradesNothing()
    {
        var reading = AttentionEvaluator.Read(Window(Runs(T0.AddSeconds(-5), 10)), T0);

        Assert.NotNull(reading);
        Assert.Equal(0, reading!.TotalCount);
        Assert.Null(AttentionEvaluator.Evaluate(reading, 10, 60));
    }

    [Fact]
    public void NoRunsAtAll_IsNotObserved_EvenWithCountRows()
    {
        var run = T0.AddMinutes(-1);
        Assert.Null(AttentionEvaluator.Read(Window(Array.Empty<DateTime>(), new[] { Count(run, 500) }), T0));
        Assert.Null(AttentionEvaluator.Read(AttentionWindow.Empty, T0));
    }

    [Fact]
    public void AStormThreeTimesTheBar_FiresOnTheFirstRunAfterItStarts()
    {
        /* Nine quiet runs, then one run with 90 attentions (three times the 10-per-minute bar, over three minutes: 30 per minute). */
        var runs = Runs(T0.AddSeconds(-5), 10);
        var window = Window(runs, new[] { Count(runs[0], 90) });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60);

        Assert.NotNull(finding);
        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);
        Assert.Equal(30.0, finding.RatePerMinute);
        Assert.Equal(10, finding.BarCrossed);
        Assert.False(finding.Muted);
    }

    [Fact]
    public void TheCoveredSpan_RunsFromTheRunBeforeTheOldestTakenRun_ToTheNewest()
    {
        var runs = Runs(T0.AddSeconds(-5), 10);
        var reading = AttentionEvaluator.Read(Steady(10), T0)!;

        /* Three one-minute gaps: taking 3 runs reaches 3 minutes from the run before the oldest taken (the 4th run). */
        Assert.Equal(3, reading.RunCount);
        Assert.Equal(3.0, reading.CoveredMinutes);
        Assert.Equal(30, reading.TotalCount);
        Assert.Equal(runs[0], reading.NewestRunTime);
    }

    [Fact]
    public void AFailedRun_WidensTheSpan_AndItsCountRowsStayExact()
    {
        /* Runs at -1, -2 and -4 minutes succeeded; the run at -3 failed (no log row, no count row). The counter delta of the
           run at -2 spans both minutes, so the exact sum stays right while the span reaches 3 minutes from the run at -5. */
        var newest = T0.AddSeconds(-5);
        var runs = new List<DateTime> { newest, newest.AddMinutes(-1), newest.AddMinutes(-3), newest.AddMinutes(-4), newest.AddMinutes(-5) };
        var counts = new[] { Count(runs[0], 10), Count(runs[1], 40), Count(runs[2], 10) };

        var reading = AttentionEvaluator.Read(Window(runs, counts), T0)!;

        Assert.Equal(2, reading.RunCount);
        Assert.Equal(3.0, reading.CoveredMinutes);
        Assert.Equal(50, reading.TotalCount);
    }

    [Fact]
    public void TheBaseRunsOwnRow_IsNotCountedInTheSpan()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var counts = new[] { Count(runs[0], 3), Count(runs[1], 3), Count(runs[2], 3), Count(runs[3], 9999) };

        var reading = AttentionEvaluator.Read(Window(runs, counts), T0)!;

        Assert.Equal(9, reading.TotalCount);
    }

    [Fact]
    public void FewerThanTwoRuns_IsNotAnObservation_BecauseThereIsNoSpan()
    {
        Assert.Null(AttentionEvaluator.Read(Window(Runs(T0.AddSeconds(-5), 1), new[] { Count(T0.AddSeconds(-5), 500) }), T0));
    }

    [Fact]
    public void AllTheHistoryUnderThreeMinutes_StillJudgesThatSpan()
    {
        var runs = Runs(T0.AddSeconds(-5), 2);
        var reading = AttentionEvaluator.Read(Window(runs, new[] { Count(runs[0], 20) }), T0)!;

        Assert.Equal(1.0, reading.CoveredMinutes);
        Assert.Equal(20, reading.TotalCount);
    }

    [Fact]
    public void NotObserved_WhenTheNewestSuccessfulRunIsOlderThanTenMinutes()
    {
        Assert.Null(AttentionEvaluator.Read(Window(Runs(T0.AddMinutes(-11), 10)), T0));
        Assert.NotNull(AttentionEvaluator.Read(Window(Runs(T0.AddMinutes(-9), 10)), T0));
    }

    [Fact]
    public void NotObserved_ScalesWithTheCadence_TheSameRuleRunnableQueueUses()
    {
        /* Five-minute cadence: the window is 4 x 5 = 20 minutes, so a newest run 15 minutes old is still an observation. */
        var runs = Runs(T0.AddMinutes(-15), 8, gapMinutes: 5);
        Assert.NotNull(AttentionEvaluator.Read(Window(runs), T0));
        Assert.Equal(
            RunnableQueueEvaluator.FallbackWindowOfTimes(runs),
            RunnableQueueEvaluator.FallbackWindowMinutes(runs.Select(r => new RunnableQueueSample(r, 1, null, null)).ToList()));
        Assert.Equal(RunnableQueueEvaluator.ReadLookbackMinutes, AttentionEvaluator.ReadLookbackMinutes);
    }

    [Fact]
    public void Mute_AllMuted_JudgesZero_AndRecordsTheFireMuted()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 300) }, new[] { Sample(runs[0], "db1", "etl", 20) });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60, (_, _) => true);

        Assert.NotNull(finding);
        Assert.True(finding!.Muted);
        Assert.Equal(AlertSeverityLevel.Critical, finding.Severity);
        Assert.Equal(100.0, finding.RatePerMinute);
    }

    [Fact]
    public void Mute_PartlyMuted_JudgesTheUnmutedShareOfTheExactCount()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var samples = new[] { Sample(runs[0], "db1", "etl", 15), Sample(runs[0], "db2", "web", 5) };
        var window = Window(runs, new[] { Count(runs[0], 300) }, samples);

        /* 300 over 3 minutes = 100 per minute exact; the etl group is 15 of 20 samples and muted, so 300 x 5/20 = 75 judged = 25 per minute. */
        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60, (_, app) => app == "etl");

        Assert.NotNull(finding);
        Assert.False(finding!.Muted);
        Assert.True(finding.Estimated);
        Assert.Equal(AlertSeverityLevel.Warning, finding.Severity);
        Assert.Equal(25.0, finding.RatePerMinute);
        Assert.Contains("Estimated from 20 sampled events", finding.Summary, StringComparison.Ordinal);
    }

    [Fact]
    public void Mute_WithNoSamplesInTheSpan_JudgesTheFullCount()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 300) });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60, (_, _) => true);

        Assert.False(finding!.Muted);
        Assert.False(finding.Estimated);
        Assert.Equal(100.0, finding.RatePerMinute);
    }

    [Fact]
    public void Mute_WhenNothingIsMuted_IsNotAnEstimate()
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], 300) }, new[] { Sample(runs[0], "db1", "etl", 3) });

        var finding = AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60, (_, _) => false);

        Assert.False(finding!.Estimated);
        Assert.DoesNotContain("Estimated", finding.Summary, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(29, null)]
    [InlineData(30, AlertSeverityLevel.Warning)]
    [InlineData(179, AlertSeverityLevel.Warning)]
    [InlineData(180, AlertSeverityLevel.Critical)]
    public void Bars_WarningAtTenPerMinute_CriticalAtSixty(long countInThreeMinutes, AlertSeverityLevel? expected)
    {
        var runs = Runs(T0.AddSeconds(-5), 6);
        var window = Window(runs, new[] { Count(runs[0], countInThreeMinutes) });

        Assert.Equal(expected, AttentionEvaluator.Evaluate(AttentionEvaluator.Read(window, T0), 10, 60)?.Severity);
    }

    [Fact]
    public void Defaults_AreTenAndSixty_AndTheMetricNameIsOneConstant()
    {
        Assert.Equal(10, AttentionEvaluator.DefaultWarningPerMinute);
        Assert.Equal(60, AttentionEvaluator.DefaultCriticalPerMinute);
        Assert.Equal("Client Timeouts", AttentionEvaluator.MetricName);
        Assert.Equal(AlertFamily.ClientTimeoutsMetric, AttentionEvaluator.MetricName);
        Assert.Equal((5, 5), AttentionEvaluator.ClampBars(5, 3));
        Assert.Equal("1.5 per minute", AlertMetricClassifier.FormatHistoryValue(AttentionEvaluator.MetricName, 1.5));
    }

    /* ---------------- through the engine ---------------- */

    private static AlertEngineTests.Harness Armed()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.ClientTimeoutsEnabled = true;
        return h;
    }

    private static AttentionWindow StormWindow(DateTime now, long count)
    {
        var runs = Runs(now.AddSeconds(-5), 8);
        return Window(runs, new[] { Count(runs[0], count) });
    }

    [Fact]
    public async Task Engine_OffByDefault_ReadsNothingAndFiresNothing()
    {
        var h = new AlertEngineTests.Harness();
        h.Adapter.Attention = StormWindow(h.Now, 600);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.False(h.Settings.ClientTimeoutsEnabled);
        Assert.Equal(0, h.Adapter.AttentionReads);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Engine_Storm_FiresOnce_WithTheRateAndTheBar_AndAsksForTheSixtyMinuteLookback()
    {
        var h = Armed();
        h.Adapter.Attention = StormWindow(h.Now, 90);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AttentionEvaluator.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, outcome.Severity);
        Assert.Equal("30.0 per minute", outcome.CurrentValue);
        Assert.Equal("10 per minute", outcome.ThresholdValue);
        Assert.False(outcome.Muted);
        Assert.Equal(AttentionEvaluator.ReadLookbackMinutes, h.Adapter.AttentionLookbackMinutesAsked);
    }

    [Fact]
    public async Task Engine_QuietServer_FiresNothing_AndAMissingCollectorIsNotACleared()
    {
        var h = Armed();
        h.Adapter.Attention = Window(Runs(h.Now.AddSeconds(-5), 8));
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task Engine_WarningThatWorsensToCritical_IsDeliveredPastTheCooldown()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.Attention = StormWindow(h.Now, 90);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(1);
        h.Adapter.Attention = StormWindow(h.Now, 900);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fires = h.Deliverer.Outcomes.Where(o => o.MetricName == AttentionEvaluator.MetricName).ToList();
        Assert.Equal(2, fires.Count);
        Assert.Equal(AlertSeverityLevel.Critical, fires[1].Severity);
        Assert.True(fires[1].Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task Engine_AllGroupsMuted_RecordsTheFireMuted_AndAsksTheMuteCheckPerGroupWithTheApp()
    {
        var h = Armed();
        var asked = new List<AlertMuteContext>();
        h.IsMuted = ctx => { asked.Add(ctx); return true; };
        var runs = Runs(h.Now.AddSeconds(-5), 8);
        h.Adapter.Attention = Window(runs, new[] { Count(runs[0], 300) },
            new[] { Sample(runs[0], "db1", "etl", 4), Sample(runs[0], "db2", "web", 2) });

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AttentionEvaluator.MetricName);
        Assert.True(outcome.Muted);
        Assert.Contains(asked, c => c.MetricName == AttentionEvaluator.MetricName && c.ClientAppName == "etl" && c.DatabaseName == "db1");
        Assert.Contains(asked, c => c.ClientAppName == "web" && c.DatabaseName == "db2");
    }

    [Fact]
    public async Task Engine_BackUnderTheBar_ResolvesOnce_AndStaysSilentAfterwards()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.Attention = StormWindow(h.Now, 90);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(1);
        h.Adapter.Attention = Window(Runs(h.Now.AddSeconds(-5), 8));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var resolution = Assert.Single(h.Resolutions, r => r.MetricName == AttentionEvaluator.MetricName);
        Assert.Equal("Client Timeouts Cleared", resolution.Title);
    }

    [Fact]
    public async Task Engine_NoRecentRuns_NeitherFiresNorClears_ButAStandingAlertResolvesAfterThirtyMinutes()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.Attention = StormWindow(h.Now, 90);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Now = h.Now.AddMinutes(20);
        h.Adapter.Attention = AttentionWindow.Empty;
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Resolutions);

        h.Now = h.Now.AddMinutes(11);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        var resolution = Assert.Single(h.Resolutions, r => r.MetricName == AttentionEvaluator.MetricName);
        Assert.Contains("no fresh attention collector runs", resolution.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OptedIntoEscalation_AndRegisteredInTheMetricMaps()
    {
        Assert.True(TierEscalation.IsOptedIn(AttentionEvaluator.MetricName));
        Assert.Equal(AlertFamily.Performance, AlertFamily.MetricFamilies[AttentionEvaluator.MetricName]);
    }
}
