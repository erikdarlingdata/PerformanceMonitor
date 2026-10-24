/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Review round 1 of PR #5709 (issue #5641, the Runnable Queue alert): the cadence-scaled fallback window and the
/// Low-Impact replay (M1), the 30-minute stale-incident resolution (M1), the top requests read by CPU and the
/// "at least N more" trailer (L1), and the Viewer's refusal of a bad bar pair whatever the checkbox says (L2).
/// </summary>
public sealed class RunnableQueueRound1Tests
{
    private static readonly DateTime T0 = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    private static RunnableQueueSample S(DateTime time, double perScheduler) =>
        new(time, perScheduler, (long)Math.Round(perScheduler * 4), 4);

    /// <summary>Collections <paramref name="gapMinutes"/> apart, the newest <paramref name="newestAgeMinutes"/> old, newest first.</summary>
    private static List<RunnableQueueSample> Cadence(double perScheduler, double gapMinutes, int count, double newestAgeMinutes = 1) =>
        Enumerable.Range(0, count).Select(i => S(T0.AddMinutes(-newestAgeMinutes - (i * gapMinutes)), perScheduler)).ToList();

    private static AlertEngineTests.Harness Armed()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.RunnableQueueEnabled = true;
        return h;
    }

    /* ---------------- M1: the window follows the cadence ---------------- */

    [Theory]
    [InlineData(1.0, 10.0)]
    [InlineData(2.0, 10.0)]
    [InlineData(2.5, 10.0)]
    [InlineData(3.0, 12.0)]
    [InlineData(5.0, 20.0)]
    [InlineData(15.0, 60.0)]
    [InlineData(30.0, 60.0)]
    public void TheFallbackWindow_IsTheLargerOfTenMinutesAndFourTimesTheCadence_CappedAtTheReadLookback(double gapMinutes, double expected)
    {
        Assert.Equal(expected, RunnableQueueEvaluator.FallbackWindowMinutes(Cadence(5, gapMinutes, 8)), 6);
    }

    [Fact]
    public void FewerThanThreeSamples_GiveNoCadence_AndTheTenMinuteFloor()
    {
        Assert.Null(RunnableQueueEvaluator.MedianGapMinutes(Cadence(5, 5, 2)));
        Assert.Equal(10.0, RunnableQueueEvaluator.FallbackWindowMinutes(Cadence(5, 5, 2)));
        Assert.Equal(10.0, RunnableQueueEvaluator.FallbackWindowMinutes(null));
    }

    [Fact]
    public void TheCadence_IsTheMedianGap_SoOneOutageOrAFewLateCollectionsDoNotWidenTheWindow()
    {
        /* 20 one-minute collections with a 12-minute hole and two late ones: the median gap is still one minute. */
        var times = new List<double> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 22, 23, 24, 26.3, 27.3, 28.3, 29.3, 30.3, 31.3, 32.3 };
        var samples = times.Select(t => S(T0.AddMinutes(-t), 1)).ToList();

        Assert.Equal(1.0, RunnableQueueEvaluator.MedianGapMinutes(samples)!.Value, 1);
        Assert.Equal(10.0, RunnableQueueEvaluator.FallbackWindowMinutes(samples));

        /* Two gaps only: the lower one, so a single outage gap cannot double the cadence. */
        var three = new List<RunnableQueueSample> { S(T0.AddSeconds(-10), 1), S(T0.AddMinutes(-2), 1), S(T0.AddMinutes(-11), 1) };
        Assert.Equal(10.0, RunnableQueueEvaluator.FallbackWindowMinutes(three), 1);
        Assert.Null(RunnableQueueEvaluator.Read(three, T0));
    }

    [Fact]
    public void FiveMinuteCadence_EvaluatesWhereTheFixedTenMinuteWindowWentBlind()
    {
        /* Newest 1 minute old, then 6, 11, 16. The old rule's newest 3 spanned 11 minutes: not inside 10, no evaluation. */
        var samples = Cadence(20, 5, 4);
        Assert.True(samples[2].CollectionTime < T0.AddMinutes(-RunnableQueueEvaluator.FallbackMinutes));

        var reading = RunnableQueueEvaluator.Read(samples, T0);

        Assert.NotNull(reading);
        Assert.Equal(3, reading!.SampleCount);
        Assert.Equal(11, reading.SpanMinutes);
        Assert.Equal(AlertSeverityLevel.Critical, RunnableQueueEvaluator.Evaluate(reading, 8, 16)!.Severity);
    }

    [Fact]
    public void FiveMinuteCadence_StillNeedsTheNewestThreeInsideTheScaledWindow()
    {
        /* Collections every 5 minutes, the newest 11 minutes old: the third is 21 minutes old, outside 20. */
        Assert.Null(RunnableQueueEvaluator.Read(Cadence(20, 5, 4, newestAgeMinutes: 11), T0));
        Assert.NotNull(RunnableQueueEvaluator.Read(Cadence(20, 5, 4, newestAgeMinutes: 4.9), T0));
    }

    [Theory]
    [InlineData(1.0)]
    [InlineData(1.5)]
    [InlineData(2.4)]
    public void OneMinuteDefault_BehavesExactlyAsTheFixedTenMinuteRule(double gapMinutes)
    {
        /* Two collections in the last 5 minutes, the third 8 minutes old: fallback inside 10 (Evaluates); at 11 it does not. */
        var inside = new List<RunnableQueueSample>
        {
            S(T0.AddSeconds(-10), 10), S(T0.AddMinutes(-2), 10), S(T0.AddMinutes(-8), 40),
        };
        inside.AddRange(Enumerable.Range(0, 20).Select(i => S(T0.AddMinutes(-9 - (i * gapMinutes)), 1)));
        Assert.Equal(10.0, RunnableQueueEvaluator.FallbackWindowMinutes(Cadence(1, gapMinutes, 20)));
        Assert.Equal(20.0, RunnableQueueEvaluator.Read(inside, T0)!.AveragePerScheduler, 6);

        var outside = new List<RunnableQueueSample>
        {
            S(T0.AddSeconds(-10), 10), S(T0.AddMinutes(-2), 10), S(T0.AddMinutes(-10).AddTicks(-1), 40),
        };
        Assert.Null(RunnableQueueEvaluator.Read(outside, T0));
    }

    [Fact]
    public async Task LowImpactReplay_The2026_10_09Incident_AtFiveMinuteCollection_Fires_WhereTheOldRuleStayedBlind()
    {
        /* The incident's per-collection values, thinned to the Low-Impact cadence of about 5 minutes. */
        var day = new DateTime(2026, 10, 9, 0, 0, 0, DateTimeKind.Utc);
        var collections = new[]
        {
            ("15:30:28", 0.0), ("15:35:28", 0.75), ("15:40:29", 1.0), ("15:45:00", 32.75), ("15:49:01", 52.5), ("15:54:00", 8.75),
        }.Select(c => S(day + TimeSpan.Parse(c.Item1), c.Item2)).ToList();

        var h = Armed();
        h.Now = day.AddHours(15).AddMinutes(52);
        h.Adapter.RunnableQueue = collections.Where(c => c.CollectionTime <= h.Now).OrderByDescending(c => c.CollectionTime).ToList();

        /* Under the old fixed rule: newest 3 are 15:49:01, 15:45:00, 15:40:29, the oldest 11.5 minutes old. */
        Assert.True(h.Adapter.RunnableQueue[2].CollectionTime < h.Now.AddMinutes(-RunnableQueueEvaluator.FallbackMinutes));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal("28.8 per scheduler", fired.CurrentValue);
        Assert.Equal(RunnableQueueEvaluator.ReadLookbackMinutes, h.Adapter.RunnableQueueLookbackMinutesAsked);
    }

    /* ---------------- M1: a stale incident resolves, with the reason ---------------- */

    [Fact]
    public async Task AStandingAlert_WithNoEvaluationFor30Minutes_Resolves_StatingTheReason_Once()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.RunnableQueue = Enumerable.Range(0, 5).Select(i => S(h.Now.AddSeconds(-10).AddMinutes(-i), 20)).ToList();
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Deliverer.Outcomes);

        /* No fresh scheduler data from here on: the collector stopped. 29 minutes of silence holds the alert. */
        h.Adapter.RunnableQueue = new List<RunnableQueueSample>();
        h.Now = h.Now.AddMinutes(29);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Resolutions);

        h.Now = h.Now.AddMinutes(1);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var resolution = Assert.Single(h.Resolutions);
        Assert.Equal("Runnable Queue Cleared", resolution.Title);
        Assert.Contains("no fresh scheduler data for 30 minutes", resolution.Message, StringComparison.Ordinal);
        Assert.Contains("SRV-A", resolution.Message, StringComparison.Ordinal);

        /* Resolved once: later silent sweeps add nothing. */
        h.Now = h.Now.AddMinutes(10);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Resolutions);
    }

    [Fact]
    public async Task AnEvaluatedReading_RestartsTheThirtyMinuteClock()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.RunnableQueue = Enumerable.Range(0, 5).Select(i => S(h.Now.AddSeconds(-10).AddMinutes(-i), 20)).ToList();
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        /* 20 minutes on, an observed reading still over the bar (held, inside the cooldown). */
        h.Now = h.Now.AddMinutes(20);
        h.Adapter.RunnableQueue = Enumerable.Range(0, 5).Select(i => S(h.Now.AddSeconds(-10).AddMinutes(-i), 20)).ToList();
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        /* 29 minutes after THAT reading: still held; at 30 it resolves. */
        h.Adapter.RunnableQueue = new List<RunnableQueueSample>();
        h.Now = h.Now.AddMinutes(29);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Empty(h.Resolutions);

        h.Now = h.Now.AddMinutes(1);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Single(h.Resolutions);
    }

    [Fact]
    public async Task AFiveMinuteCollector_NeverLooksStale_AndNeverResolvesWhileItKeepsEvaluating()
    {
        var h = Armed();
        var engine = h.Build();
        for (var sweep = 0; sweep < 12; sweep++)
        {
            h.Adapter.RunnableQueue = Enumerable.Range(0, 4).Select(i => S(h.Now.AddMinutes(-1 - (i * 5)), 20)).ToList();
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
            h.Now = h.Now.AddMinutes(5);
        }

        Assert.NotEmpty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task NoAlertStanding_NothingToResolve_AfterLongSilence()
    {
        var h = Armed();
        var engine = h.Build();
        h.Adapter.RunnableQueue = Enumerable.Range(0, 5).Select(i => S(h.Now.AddSeconds(-10).AddMinutes(-i), 2)).ToList();
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        h.Adapter.RunnableQueue = new List<RunnableQueueSample>();
        h.Now = h.Now.AddMinutes(90);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Resolutions);
    }

    /* ---------------- L1: top requests by CPU, and "at least N more" ---------------- */

    private static LongRunningQueryInfo Request(int spid, long cpuMs, long elapsedSeconds) => new()
    {
        SessionId = spid,
        DatabaseName = "StackOverflow",
        ProgramName = "App",
        QueryText = "SELECT 1",
        ElapsedSeconds = elapsedSeconds,
        CpuTimeMs = cpuMs,
        WaitType = "SOS_SCHEDULER_YIELD",
    };

    private static string MoreText(AlertOutcome fired) =>
        Assert.Single(Assert.Single(fired.Context!.Details, d => d.Heading == "More requests").Fields, f => f.Label == "Not shown").Value;

    [Fact]
    public async Task TheCardAsksTheReadForCpuOrder_SoBlockedLongRunnersCannotFillTheCapAheadOfTheBusyOnes()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Enumerable.Range(0, 5).Select(i => S(h.Now.AddSeconds(-10).AddMinutes(-i), 20)).ToList();
        /* 60 blocked requests (long elapsed, 1 ms CPU) and 5 short CPU-heavy ones. */
        for (var i = 0; i < 60; i++)
        {
            h.Adapter.LongRunning.Add(Request(100 + i, cpuMs: 1, elapsedSeconds: 600 + i));
        }

        for (var i = 0; i < 5; i++)
        {
            h.Adapter.LongRunning.Add(Request(200 + i, cpuMs: 90_000 - (i * 1_000), elapsedSeconds: 2));
        }

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(h.Adapter.LastLrqOrderedByCpu);
        var requests = fired.Context!.Details.Where(d => d.Heading.StartsWith("Top request:", StringComparison.Ordinal)).ToList();
        Assert.Contains("session #200", requests[0].Heading, StringComparison.Ordinal);
        Assert.Contains("session #201", requests[1].Heading, StringComparison.Ordinal);
        Assert.Contains("session #202", requests[2].Heading, StringComparison.Ordinal);
        /* The read hit its 50-row cap, so the 47 left are a floor. */
        Assert.Equal("at least 47 more requests in the newest query snapshot", MoreText(fired));
    }

    [Fact]
    public async Task UnderTheCap_TheTrailerIsAnExactCount()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Enumerable.Range(0, 5).Select(i => S(h.Now.AddSeconds(-10).AddMinutes(-i), 20)).ToList();
        for (var i = 0; i < 6; i++)
        {
            h.Adapter.LongRunning.Add(Request(100 + i, cpuMs: 1_000 * (i + 1), elapsedSeconds: 30));
        }

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal("3 more requests in the newest query snapshot", MoreText(Assert.Single(h.Deliverer.Outcomes)));
    }

    [Fact]
    public void TheBuilder_SaysAtLeast_OnlyWhenTheReadHitItsCap()
    {
        var sessions = Enumerable.Range(0, 5).Select(i => Request(i + 1, 1_000 * (i + 1), 10)).ToList();

        var exact = AlertContextBuilders.BuildRunnableQueueRequestItems(sessions);
        var floor = AlertContextBuilders.BuildRunnableQueueRequestItems(sessions, readHitCap: true);

        Assert.Equal("2 more requests in the newest query snapshot", exact[^1].Fields.Single().Value);
        Assert.Equal("at least 2 more requests in the newest query snapshot", floor[^1].Fields.Single().Value);

        /* #6703: one request past the display cap reads in the singular. */
        var one = AlertContextBuilders.BuildRunnableQueueRequestItems(
            sessions.Take(AlertContextBuilders.RunnableQueueRequestDisplayCap + 1).ToList());
        Assert.Equal("1 more request in the newest query snapshot", one[^1].Fields.Single().Value);
    }

    [Fact]
    public void TheDarlingRead_SwapsInACpuFirstOrderWithNullsLast_AndKeepsTheLongRunningOrderByDefault()
    {
        var source = File.ReadAllText(RepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertReadAdapter.cs"));

        Assert.Contains("ORDER BY r.cpu_time_ms DESC NULLS LAST, r.total_elapsed_time_ms DESC, r.session_id", source, StringComparison.Ordinal);
        Assert.Contains("if (orderByCpu)", source, StringComparison.Ordinal);
        Assert.Contains("ORDER BY r.total_elapsed_time_ms DESC", DarlingAlertReadAdapterTemplate(), StringComparison.Ordinal);
    }

    private static string DarlingAlertReadAdapterTemplate() =>
        PerformanceMonitor.Darling.Service.DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate;

    /* ---------------- L2: the Viewer refuses a bad pair whatever the checkbox says ---------------- */

    [Fact]
    public void TheViewer_RefusesABadBarPair_EvenWithTheAlertUnchecked()
    {
        var code = File.ReadAllText(RepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs"));
        var start = code.IndexOf("if (warningOk && criticalOk && rqCritical >= rqWarning)", StringComparison.Ordinal);
        Assert.True(start > 0);
        var end = code.IndexOf("Runnable queue Critical must be a whole number no lower than the Warning bar.", start, StringComparison.Ordinal);
        Assert.True(end > start);
        var arm = code[start..end];

        Assert.DoesNotContain("AlertRunnableQueueCheckBox.IsChecked", arm, StringComparison.Ordinal);
        Assert.DoesNotContain("else if", arm, StringComparison.Ordinal);
        Assert.Contains("errors.Add(!warningOk", arm, StringComparison.Ordinal);
    }

    private static string RepoFile(params string[] parts)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.Alerting", "AlertEngine.cs")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return Path.Combine(new[] { dir! }.Concat(parts).ToArray());
    }
}
