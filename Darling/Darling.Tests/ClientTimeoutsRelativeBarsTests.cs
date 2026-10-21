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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The relative Client Timeouts bars (#5736) without a store: the baseline rule on the per-minute buckets, the bar arithmetic, and how
/// often the engine asks for the baseline (about once a day per server, and not at all while the rate is under the floor).
/// </summary>
public sealed class ClientTimeoutsRelativeBarsTests
{
    private static readonly DateTime T0 = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    private static List<AttentionMinute> Minutes(DateTime start, int count, Func<int, long> perMinute) =>
        Enumerable.Range(0, count).Select(i => new AttentionMinute(start.AddMinutes(i), perMinute(i))).ToList();

    [Fact]
    public void Baseline_IsTheP999OfTheThreeMinuteAverage_NotOfTheMinutes()
    {
        /* 10,000 minutes at 100 a minute with one 3,000 spike minute. The spike lifts three of the 9,998 windows (0.03%, under the top 0.1%),
           so the p99.9 of the windows is 100. */
        var minutes = Minutes(T0, 10_000, i => i == 5_000 ? 3_000 : 100);
        var baseline = AttentionBaselineMath.Compute(minutes);

        Assert.NotNull(baseline);
        Assert.Equal(100d, baseline!.P999ThreeMinuteAverage, 6);
        Assert.Equal(10_000, baseline.MinutesObserved);
    }

    [Fact]
    public void Baseline_ARunOfBusyWindows_LiftsTheP999()
    {
        /* 30 busy minutes of 10,000 is 0.3%, more than the top 0.1%, so the p99.9 of the windows sits inside the busy stretch. */
        var minutes = Minutes(T0, 10_000, i => i is >= 4_000 and < 4_030 ? 900 : 100);
        var baseline = AttentionBaselineMath.Compute(minutes);

        Assert.Equal(900d, baseline!.P999ThreeMinuteAverage, 6);
    }

    [Fact]
    public void Baseline_AHoleInTheRuns_BreaksTheWindow_SoAMissingMinuteIsNotAQuietOne()
    {
        /* Every other minute is missing (no run), so no three consecutive minutes exist and there is no window at all. */
        var minutes = Minutes(T0, 3_000, i => 500).Where((_, i) => i % 2 == 0).ToList();

        Assert.Null(AttentionBaselineMath.Compute(minutes));
    }

    [Fact]
    public void Baseline_UnderADayOfCapture_IsNull_AndADayOrMoreIsNot()
    {
        Assert.Null(AttentionBaselineMath.Compute(Minutes(T0, 1_000, _ => 150)));
        Assert.Null(AttentionBaselineMath.Compute(Minutes(T0, 1_440, _ => 150)));
        Assert.NotNull(AttentionBaselineMath.Compute(Minutes(T0, 1_441, _ => 150)));
    }

    [Theory]
    [InlineData(10, 3, 150d, 450)]
    [InlineData(10, 3, 1d, 10)]
    [InlineData(10, 3, 3.4d, 11)]
    [InlineData(60, 10, 150d, 1500)]
    [InlineData(60, 10, 2d, 60)]
    [InlineData(10, 3, 0d, 10)]
    public void TheBar_IsTheHigherOfTheFloorAndTheMultipleOfTheBaseline(int floor, int multiple, double baseline, int expected)
    {
        Assert.Equal(expected, AttentionEvaluator.EffectiveBar(floor, multiple, baseline));
    }

    [Fact]
    public void TheBar_WithNoBaseline_IsTheFloor()
    {
        Assert.Equal(10, AttentionEvaluator.EffectiveBar(10, 3, null));
    }

    [Fact]
    public void Multiples_AreAtLeastOne_AndCriticalIsNeverBelowWarning()
    {
        Assert.Equal((1, 1), AttentionBaselineMath.ClampMultiples(0, -4));
        Assert.Equal((5, 5), AttentionBaselineMath.ClampMultiples(5, 2));
        Assert.Equal((3, 10), AttentionBaselineMath.ClampMultiples(3, 10));
    }

    /* ---------------- how often the engine asks for the baseline ---------------- */

    private static AlertEngineTests.Harness Armed(AttentionBaseline? baseline)
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.ClientTimeoutsEnabled = true;
        h.Adapter.AttentionBaseline = baseline;
        return h;
    }

    private static AttentionBaseline Normal(double perMinute) => new(perMinute, 10_000, T0.AddDays(-7), T0);

    [Fact]
    public async Task TheBaseline_IsReadOncePerDay_NotOncePerEvaluation()
    {
        var h = Armed(Normal(1));
        var engine = h.Build();

        for (var i = 0; i < 6; i++)
        {
            h.Adapter.Attention = AttentionAlertTests.StormWindow(h.Now, 600);
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
            h.Now = h.Now.AddMinutes(1);
        }

        Assert.Equal(6, h.Adapter.AttentionReads);
        Assert.Equal(1, h.Adapter.AttentionBaselineReads);

        /* A day and an hour later it is read again, once. */
        h.Now = h.Now.AddHours(25);
        for (var i = 0; i < 3; i++)
        {
            h.Adapter.Attention = AttentionAlertTests.StormWindow(h.Now, 600);
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
            h.Now = h.Now.AddMinutes(1);
        }

        Assert.Equal(2, h.Adapter.AttentionBaselineReads);
    }

    [Fact]
    public async Task AServerWhoseRateIsUnderTheFloor_NeverAsksForTheBaseline()
    {
        var h = Armed(Normal(1));
        var engine = h.Build();

        for (var i = 0; i < 4; i++)
        {
            h.Adapter.Attention = AttentionAlertTests.StormWindow(h.Now, 12);   // 4 a minute over the span, under the floor of 10
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
            h.Now = h.Now.AddMinutes(1);
        }

        Assert.Equal(4, h.Adapter.AttentionReads);
        Assert.Equal(0, h.Adapter.AttentionBaselineReads);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AServerWithNoBaseline_IsAnsweredFromTheFloors_AndIsNotAskedAgainTheSameDay()
    {
        var h = Armed(null);
        var engine = h.Build();

        h.Adapter.Attention = AttentionAlertTests.StormWindow(h.Now, 90);   // 30 a minute
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        h.Now = h.Now.AddMinutes(1);
        h.Adapter.Attention = AttentionAlertTests.StormWindow(h.Now, 90);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AttentionEvaluator.MetricName);
        Assert.Equal("10 per minute", outcome.ThresholdValue);
        Assert.Contains("under 1 day of capture", outcome.ShortMessage, StringComparison.Ordinal);
        Assert.Equal(1, h.Adapter.AttentionBaselineReads);
    }

    [Fact]
    public async Task AFailedBaselineRead_FiresNothingForThatServer_AndIsNotRetriedAtOnce()
    {
        var h = Armed(Normal(1));
        h.Adapter.AttentionBaselineThrows = true;
        var engine = h.Build();

        for (var i = 0; i < 3; i++)
        {
            h.Adapter.Attention = AttentionAlertTests.StormWindow(h.Now, 600);
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
            h.Now = h.Now.AddMinutes(1);
        }

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Equal(1, h.Adapter.AttentionBaselineReads);

        /* After the retry wait the read is tried again, and now it answers. */
        h.Adapter.AttentionBaselineThrows = false;
        h.Now = h.Now.AddMinutes(AttentionBaselineMath.RetryAfterFailureMinutes);
        h.Adapter.Attention = AttentionAlertTests.StormWindow(h.Now, 600);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(2, h.Adapter.AttentionBaselineReads);
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AttentionEvaluator.MetricName);
    }
}
