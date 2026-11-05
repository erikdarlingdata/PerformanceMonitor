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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Review round 2 of #5709 (the Runnable Queue alert, #5641): a failed escalation send keeps the escalation (M1), the
/// cadence follows the newest collections so a switch to the Low-Impact preset registers within three collections (M2),
/// the restart gap is a documented limit (L1), and the card's CPU window never claims more than the CPU samples cover
/// (L2).
/// </summary>
public sealed class RunnableQueueRound2Tests
{
    private static readonly DateTime T0 = new(2026, 10, 9, 15, 40, 0, DateTimeKind.Utc);

    private static RunnableQueueSample S(DateTime time, double perScheduler) =>
        new(time, perScheduler, (long)Math.Round(perScheduler * 4), 4);

    private static AlertEngineTests.Harness Armed()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.RunnableQueueEnabled = true;
        return h;
    }

    private static List<RunnableQueueSample> Samples(DateTime now, params double[] perMinuteAverages)
    {
        var list = new List<RunnableQueueSample>();
        for (var i = 0; i < perMinuteAverages.Length; i++)
        {
            var at = now.AddMinutes(-(perMinuteAverages.Length - 1 - i)).AddSeconds(-5);
            list.Add(new RunnableQueueSample(at, perMinuteAverages[i], (long)(perMinuteAverages[i] * 4), 4));
        }

        return list;
    }

    private static async Task<int> EvaluateAsync(AlertEngineTests.Harness h, AlertEngine engine, DateTime at, params double[] averages)
    {
        h.Now = at;
        h.Adapter.RunnableQueue = Samples(at, averages);
        var before = h.Deliverer.Outcomes.Count(o => o.Hold is null);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        return h.Deliverer.Outcomes.Count(o => o.Hold is null) - before;
    }

    /// <summary>A webhook-only fan-out whose every post failed (an HTTP 429, say).</summary>
    private static AlertDelivery FailedByWebhook() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Teams: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    /* ---------------- M1: a failed escalation send ---------------- */

    [Fact]
    public void TierEscalation_Restore_PutsTheTierBack_OrRemovesTheEntryWhenThereWasNone()
    {
        var tiers = new TierEscalation();
        Assert.Null(tiers.Highest("k"));

        tiers.Record("k", AlertSeverityLevel.Warning);
        Assert.Equal(AlertSeverityLevel.Warning, tiers.Highest("k"));

        var prior = tiers.Highest("k");
        tiers.Record("k", AlertSeverityLevel.Critical);
        Assert.False(tiers.IsEscalation("k", AlertSeverityLevel.Critical));
        tiers.Restore("k", prior);
        Assert.True(tiers.IsEscalation("k", AlertSeverityLevel.Critical));

        tiers.Restore("k", null);
        Assert.Null(tiers.Highest("k"));
        Assert.False(tiers.IsEscalation("k", AlertSeverityLevel.Critical));
    }

    [Fact]
    public async Task AnEscalationWhoseEveryChannelFailed_IsRetriedAsAnEscalation_AtOnce_AndALaterSuccessRecordsCritical()
    {
        var h = Armed();
        h.Now = T0;
        /* #7086: a Warning no longer pages, so the tier this test needs (a Warning the operator was told about) can only come
           from the restart seed: an install upgraded in the middle of an incident the old version paged at Warning. */
        h.EscalationSeedReader = (q, _) => Task.FromResult<AlertSeverityLevel?>(
            q.MetricName == RunnableQueueEvaluator.MetricName ? AlertSeverityLevel.Warning : null);
        var engine = h.Build();

        /* The Warning at T0 is held (the seed already holds the page the old version sent). */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0, 9, 9, 9));

        /* The Critical is an escalation, and the Teams post fails on every channel. */
        h.Deliverer.Report = _ => FailedByWebhook();
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(1), 9, 9, 9, 60));
        Assert.True(h.Deliverer.Outcomes[^1].Context!.EscalationBypassesCooldown);

        /* The retry a minute later (the failure backoff) is STILL the escalation and carries the bypass flag, so the
           webhook cooldown the Warning stamped lets it through. Before round 2 the tier stayed at Critical, the retry
           was an ordinary Critical with no flag, and the cooldown held it. */
        h.Deliverer.Report = null;
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(2).AddSeconds(1), 9, 9, 9, 60, 60));
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[^1].Severity);
        Assert.True(h.Deliverer.Outcomes[^1].Context!.EscalationBypassesCooldown);

        /* The success recorded Critical: a Critical after the cooldown is an ordinary repeat, not a second escalation. */
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(4), 60, 60, 60, 60, 60));
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(9), 60, 60, 60, 60, 60));
        Assert.False(h.Deliverer.Outcomes[^1].Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task AFirstFireWhoseEveryChannelFailed_IsRetriedAsAnOrdinaryFirstFire_NotAnEscalation()
    {
        var h = Armed();
        var engine = h.Build();

        h.Deliverer.Report = _ => FailedByWebhook();
        Assert.Equal(1, await EvaluateAsync(h, engine, T0, 60, 60, 60));
        Assert.False(h.Deliverer.Outcomes[^1].Context!.EscalationBypassesCooldown);

        /* Nothing was delivered, so the tier was removed rather than left at Critical: the retry is an ordinary first
           fire, and a Warning-then-Critical that follows it escalates only once something was delivered. */
        h.Deliverer.Report = null;
        Assert.Equal(1, await EvaluateAsync(h, engine, T0.AddMinutes(1).AddSeconds(1), 60, 60, 60, 60));
        Assert.False(h.Deliverer.Outcomes[^1].Context!.EscalationBypassesCooldown);
        Assert.Equal(0, await EvaluateAsync(h, engine, T0.AddMinutes(2), 60, 60, 60, 60, 60));
    }

    /* ---------------- M2: the cadence follows the newest collections ---------------- */

    [Fact]
    public void TheCadence_IsTheMedianOfTheNewestFiveGaps_SoASwitchRegistersAfterThreeSlowerOnes()
    {
        /* 50 one-minute collections, then five-minute ones. Newest first: gaps 5,5,1,1,1,... */
        List<RunnableQueueSample> Build(int fiveMinuteGaps)
        {
            var list = new List<RunnableQueueSample>();
            var t = T0;
            for (var i = 0; i < 50; i++)
            {
                list.Add(S(t.AddMinutes(-i), 1));
            }

            for (var i = 1; i <= fiveMinuteGaps; i++)
            {
                list.Add(S(T0.AddMinutes(5 * i), 1));
            }

            return list;
        }

        Assert.Equal(1.0, RunnableQueueEvaluator.MedianGapMinutes(Build(0))!.Value);
        Assert.Equal(1.0, RunnableQueueEvaluator.MedianGapMinutes(Build(1))!.Value);
        Assert.Equal(1.0, RunnableQueueEvaluator.MedianGapMinutes(Build(2))!.Value);
        Assert.Equal(5.0, RunnableQueueEvaluator.MedianGapMinutes(Build(3))!.Value);
        Assert.Equal(5.0, RunnableQueueEvaluator.MedianGapMinutes(Build(8))!.Value);

        /* One outage gap among the newest five still cannot widen the window. */
        var outage = new List<RunnableQueueSample>();
        foreach (var minute in new[] { 0, 1, 2, 3, 4, 5, 6, 20, 21, 22 })
        {
            outage.Add(S(T0.AddMinutes(minute), 1));
        }

        Assert.Equal(1.0, RunnableQueueEvaluator.MedianGapMinutes(outage)!.Value);
        Assert.Equal(10.0, RunnableQueueEvaluator.FallbackWindowMinutes(outage));
    }

    /// <summary>The collector's schedule: every minute up to <paramref name="switchAt"/>, every 5 minutes after it.
    /// The newest-first list of what a 60-minute read holds at <paramref name="now"/>.</summary>
    private static List<RunnableQueueSample> CollectedAt(DateTime now, DateTime switchAt, Func<DateTime, double> value)
    {
        var all = new List<DateTime>();
        for (var t = switchAt.AddMinutes(-90); t <= switchAt; t = t.AddMinutes(1))
        {
            all.Add(t);
        }

        for (var t = switchAt.AddMinutes(5); t <= switchAt.AddMinutes(200); t = t.AddMinutes(5))
        {
            all.Add(t);
        }

        return all.Where(t => t <= now && t >= now.AddMinutes(-RunnableQueueEvaluator.ReadLookbackMinutes))
            .OrderByDescending(t => t)
            .Select(t => S(t, value(t)))
            .ToList();
    }

    [Fact]
    public async Task ASwitchFromOneToFiveMinuteCollection_IsEvaluatedWithinThreeCollections()
    {
        var h = Armed();
        var engine = h.Build();
        var switchAt = T0;

        /* Warning (10 per scheduler) held in the 1-minute era; the queue goes Critical from the third
           five-minute collection on. */
        double Value(DateTime t) => t >= switchAt.AddMinutes(15) ? 60 : 10;

        DateTime? criticalAt = null;
        for (var minute = -30; minute <= 40; minute++)
        {
            h.Now = switchAt.AddMinutes(minute).AddSeconds(30);
            h.Adapter.RunnableQueue = CollectedAt(h.Now, switchAt, Value);
            var before = h.Deliverer.Outcomes.Count;
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
            if (h.Deliverer.Outcomes.Count > before
                && h.Deliverer.Outcomes[^1].Severity == AlertSeverityLevel.Critical
                && criticalAt is null)
            {
                criticalAt = h.Now;
            }
        }

        /* The third five-minute collection lands at +15; the escalation is delivered on the sweep that sees it. With the
           median over the whole hour the alert was blind until about +50. */
        Assert.NotNull(criticalAt);
        Assert.True(criticalAt <= switchAt.AddMinutes(16).AddSeconds(30), $"first Critical at {criticalAt:HH:mm:ss}");
        /* #7086: the Warnings before it were held (digest rows), so the first Critical is the first page. */
        Assert.Null(h.Deliverer.Outcomes.First(o => o.Severity == AlertSeverityLevel.Critical).Hold);
        Assert.All(h.Deliverer.Outcomes.Where(o => o.Severity == AlertSeverityLevel.Warning), o => Assert.NotNull(o.Hold));
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task AnOpenIncident_DoesNotStaleResolve_WhileFiveMinuteDataKeepsArriving_AfterASwitchFromOneMinute()
    {
        var h = Armed();
        var engine = h.Build();
        var switchAt = T0;

        /* A standing Warning the whole time: the queue never clears, so the only possible resolution is the stale one. */
        for (var minute = -30; minute <= 120; minute++)
        {
            h.Now = switchAt.AddMinutes(minute).AddSeconds(30);
            h.Adapter.RunnableQueue = CollectedAt(h.Now, switchAt, _ => 10);
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        }

        Assert.Empty(h.Resolutions);
        Assert.True(h.Deliverer.Outcomes.Count >= 10, $"only {h.Deliverer.Outcomes.Count} fires");
        Assert.All(h.Deliverer.Outcomes, o => Assert.Equal(AlertSeverityLevel.Warning, o.Severity));

        /* Healthy data at last: it resolves as an ordinary clear, with its own wording, not the stale reason. */
        for (var minute = 121; minute <= 140; minute++)
        {
            h.Now = switchAt.AddMinutes(minute).AddSeconds(30);
            h.Adapter.RunnableQueue = CollectedAt(h.Now, switchAt, t => t >= switchAt.AddMinutes(115) ? 0 : 10);
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        }

        var resolution = Assert.Single(h.Resolutions);
        Assert.Contains("back below threshold", resolution.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOneMinuteDefault_IsUnchanged_TheWindowStaysTenMinutes()
    {
        var steady = Enumerable.Range(0, 60).Select(i => S(T0.AddMinutes(-i), 1)).ToList();

        Assert.Equal(1.0, RunnableQueueEvaluator.MedianGapMinutes(steady)!.Value);
        Assert.Equal(10.0, RunnableQueueEvaluator.FallbackWindowMinutes(steady));
        Assert.Equal(5.0, RunnableQueueEvaluator.MedianGapMinutes(Enumerable.Range(0, 12).Select(i => S(T0.AddMinutes(-5 * i), 1)).ToList())!.Value);
    }

    /* ---------------- L2: the CPU window never claims more than the samples cover ---------------- */

    [Fact]
    public void TheCpuItem_NamesTheWindowTheSamplesCover_NeverALongerOne()
    {
        var now = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);
        var samples = new List<AlertCpuSample>
        {
            new(now.AddMinutes(-14), 40, 50),
            new(now.AddMinutes(-2), 80, 90),
        };

        foreach (var span in new[] { 5, 15, 20, 30, 60 })
        {
            var item = AlertContextBuilders.BuildRunnableQueueCpuItem(samples, 1, 2, now, span);
            Assert.NotNull(item);
            Assert.Equal(
                $"CPU over the last {Math.Min(span, AlertContextBuilders.RunnableQueueCpuSampleLookbackMinutes)} minutes",
                item!.Heading);
        }

        Assert.Equal("CPU over the last 15 minutes", AlertContextBuilders.BuildRunnableQueueCpuItem(samples, 1, 2, now, 45)!.Heading);
        Assert.Equal("CPU over the last 5 minutes", AlertContextBuilders.BuildRunnableQueueCpuItem(samples, 1, 2, now, 5)!.Heading);
    }

    [Fact]
    public void TheCpuWindowCap_IsTheLookbackTheDarlingSamplesAreReadWith()
    {
        Assert.Equal(AlertContextBuilders.RunnableQueueCpuSampleLookbackMinutes, DarlingWorker.RecentCpuLookback.TotalMinutes);
    }
}
