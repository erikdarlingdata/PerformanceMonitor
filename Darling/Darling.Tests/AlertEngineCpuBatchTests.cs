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
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5642: the shared engine feeds every NEW sample of the snapshot's recent CPU list through the persistence
/// gate, oldest first, so "three readings over the line" is three SAMPLES however the collector batched them.
/// The clock is pinned at 2026-07-01 12:00 UTC (the <see cref="AlertEngineTests.Harness"/> default).
///
/// <para><b>RED at base.</b> These tests build snapshots through <c>AlertServerSnapshot.RecentCpuSamples</c>
/// and <c>WatchingSinceUtc</c>, members this change adds, so they cannot compile at the base commit; the
/// base-RED is carried by <see cref="TheEngineFeedsTheRecentSampleList_SourceShape"/>, which reads the engine
/// source. Each guard below was RED-planted once at head: the guard reverted, the named test seen to fail,
/// the guard restored (see the PR body).</para>
/// </summary>
public sealed class AlertEngineCpuBatchTests
{
    private const string Key = "101";
    private static readonly DateTime Noon = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    private static AlertCpuSample Hot(DateTime at) => new(at, 70, 95);

    private static AlertCpuSample Cool(DateTime at) => new(at, 10, 20);

    /// <summary>A snapshot whose latest-sample fields are the NEWEST of <paramref name="samples"/> (as the worker
    /// builds it) and whose recent list is <paramref name="samples"/>.</summary>
    private static AlertServerSnapshot Batch(DateTime? watchingSince, params AlertCpuSample[] samples)
    {
        var newest = samples.OrderBy(s => s.SampleTimeUtc).Last();
        return AlertEngineTests.Harness.Snapshot(
                sqlCpu: newest.SqlCpuPercent, totalCpu: newest.TotalCpuPercent, cpuSampleTime: newest.SampleTimeUtc)
            with
            {
                CpuSampleUtcOnly = newest.SampleTimeUtc,
                RecentCpuSamples = samples,
                WatchingSinceUtc = watchingSince
            };
    }

    private static AlertEngineTests.Harness NewHarness()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.CpuEnabled = true;
        return h;
    }

    [Fact]
    public async Task ThreeOverTheLineSamplesInOneBatch_FireOnce_OnTheThird()
    {
        var h = NewHarness();
        var engine = h.Build();
        var t = Noon.AddMinutes(-10);

        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-30), Hot(t), Hot(t.AddMinutes(1)), Hot(t.AddMinutes(2))));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("High CPU", fired.MetricName);
        Assert.Equal("95% (Total CPU)", fired.CurrentValue);

        /* The same list again (the next 30 s sweep) finds nothing new: still one alert. */
        h.Now = Noon.AddSeconds(30);
        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-30), Hot(t), Hot(t.AddMinutes(1)), Hot(t.AddMinutes(2))));
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task TwoSamplesThenOneMore_FiresOnTheThird_WhateverTheBatching()
    {
        var h = NewHarness();
        var engine = h.Build();
        var t = Noon.AddMinutes(-10);

        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-30), Hot(t), Hot(t.AddMinutes(1))));
        Assert.Empty(h.Deliverer.Outcomes);

        /* The list is the last 15 minutes, so the next sweep still holds the first two: only the third is new. */
        h.Now = Noon.AddMinutes(1);
        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-30), Hot(t), Hot(t.AddMinutes(1)), Hot(t.AddMinutes(2))));
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ABacklogOlderThanFifteenMinutes_DoesNotFire()
    {
        var h = NewHarness();
        var engine = h.Build();
        var old = Noon.AddMinutes(-40);

        await engine.EvaluateServerAsync(Batch(Noon.AddHours(-2), Hot(old), Hot(old.AddMinutes(1)), Hot(old.AddMinutes(2))));

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AFutureDatedSample_IsIgnored()
    {
        var h = NewHarness();
        var engine = h.Build();

        /* Two honest readings and one dated five minutes ahead of the engine's clock: two countable readings. */
        await engine.EvaluateServerAsync(Batch(
            Noon.AddMinutes(-30), Hot(Noon.AddMinutes(-2)), Hot(Noon.AddMinutes(-1)), Hot(Noon.AddMinutes(5))));

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AListLongerThanTheBatchLimit_FeedsOnlyTheNewestSamples()
    {
        var h = NewHarness();
        var engine = h.Build();

        /* 40 samples inside the lookback: the oldest 8 are hot, the newest 32 are cool. Only the newest
           CpuSampleBatchLimit are fed, so the hot ones are never judged: no fire and nothing to resolve. */
        var first = Noon.AddMinutes(-14);
        var samples = Enumerable.Range(0, 40)
            .Select(i => i < 8 ? Hot(first.AddSeconds(i * 20)) : Cool(first.AddSeconds(i * 20)))
            .ToArray();
        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-30), samples));

        Assert.Equal(40, samples.Length);
        Assert.True(40 > AlertEngine.CpuSampleBatchLimit);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task AnEmptyList_KeepsToday_OneRowPerSweep()
    {
        var h = NewHarness();
        var engine = h.Build();

        for (var i = 1; i <= AlertEngine.CpuBreachSamples; i++)
        {
            Assert.Empty(h.Deliverer.Outcomes);
            var at = Noon.AddMinutes(-10 + i);
            var snapshot = AlertEngineTests.Harness.Snapshot(sqlCpu: 70, totalCpu: 95, cpuSampleTime: at)
                with
                { RecentCpuSamples = Array.Empty<AlertCpuSample>(), WatchingSinceUtc = Noon.AddMinutes(-30) };
            await engine.EvaluateServerAsync(snapshot);
        }

        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ARestartMidEpisode_WithTheFiringBitAndWatermarkReloaded_SendsNoDuplicate()
    {
        var h = NewHarness();
        var t = Noon.AddMinutes(-10);

        /* Before the restart the incident fired at the third sample (t+2) and the gate persisted that. */
        h.StateStore.Persistence[(Key, AlertEngine.CpuPersistenceMetric)] = new AlertPersistenceRecord(
            new PersistenceState(AlertEngine.CpuBreachSamples, 0, true), t.AddMinutes(2));

        /* The restarted engine reads the whole list again (it is the store's last 15 minutes) plus one new sample. */
        var engine = h.Build();
        await engine.EvaluateServerAsync(Batch(
            Noon.AddMinutes(-30), Hot(t), Hot(t.AddMinutes(1)), Hot(t.AddMinutes(2)), Hot(t.AddMinutes(3))));

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task ARestartMidStreak_DoesNotRecountSamplesAtOrBelowTheWatermark()
    {
        var h = NewHarness();
        var t = Noon.AddMinutes(-10);

        /* One breach counted, up to the sample at t+2. The list still holds three hot samples at or below it:
           re-judging them would reach the bar and fire a streak that never was. */
        h.StateStore.Persistence[(Key, AlertEngine.CpuPersistenceMetric)] = new AlertPersistenceRecord(
            new PersistenceState(1, 0, false), t.AddMinutes(2));

        var engine = h.Build();
        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-30), Hot(t), Hot(t.AddMinutes(1)), Hot(t.AddMinutes(2))));

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Equal(1, h.StateStore.Persistence[(Key, AlertEngine.CpuPersistenceMetric)].State.ConsecutiveBreaches);
    }

    [Fact]
    public async Task ASpikeThatEndedBeforeARestart_BackfilledAfterIt_SendsNothing()
    {
        var h = NewHarness();
        var engine = h.Build();

        /* The service began watching at 11:58. The spike (11:50-11:52) was collected, and ended, before that:
           it is inside the 15-minute lookback and above the persisted watermark (there is none), so only the
           watching-since floor keeps it from being judged as a fresh incident. */
        var spike = Noon.AddMinutes(-10);
        await engine.EvaluateServerAsync(Batch(
            Noon.AddMinutes(-2), Hot(spike), Hot(spike.AddMinutes(1)), Hot(spike.AddMinutes(2)), Cool(Noon.AddMinutes(-1))));

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);
    }

    [Fact]
    public async Task AReconnect_ANewWatchingSince_SendsNothingForTheBacklog_AndCountsLiveSamplesAfterIt()
    {
        var h = NewHarness();
        var engine = h.Build();

        /* The connection has been up and saw a quiet sample at 11:40. */
        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-60), Cool(Noon.AddMinutes(-20))));

        /* It drops and returns at 11:56: a new watching-since. The collector backfilled a spike (11:50-11:52) and
           a quiet 11:55 sample. The spike is inside the 15-minute lookback and above the watermark, so only the
           new watching-since keeps it from being judged: nothing fires, and the quiet sample counts once. */
        h.Now = Noon.AddMinutes(-4);
        var spike = Noon.AddMinutes(-10);
        await engine.EvaluateServerAsync(Batch(
            Noon.AddMinutes(-4), Hot(spike), Hot(spike.AddMinutes(1)), Hot(spike.AddMinutes(2)), Cool(Noon.AddMinutes(-5))));
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Resolutions);

        /* Samples collected after the reconnect count one by one: three hot ones fire. */
        h.Now = Noon;
        var live = Noon.AddMinutes(-3);
        await engine.EvaluateServerAsync(Batch(
            Noon.AddMinutes(-4), Cool(Noon.AddMinutes(-5)), Hot(live), Hot(live.AddMinutes(1)), Hot(live.AddMinutes(2))));
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AWatermarkLaterThanTheNewestSample_ReAnchorsOnce_AndCountsTheNewestOnce()
    {
        var h = NewHarness();
        var t = Noon.AddMinutes(-10);

        /* A frame or clock skew left the watermark ahead of every sample the list holds. */
        h.StateStore.Persistence[(Key, AlertEngine.CpuPersistenceMetric)] = new AlertPersistenceRecord(
            PersistenceState.Initial, Noon.AddMinutes(30));

        var engine = h.Build();
        var list = new[] { Hot(t), Hot(t.AddMinutes(1)), Hot(t.AddMinutes(2)) };
        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-30), list));

        var record = h.StateStore.Persistence[(Key, AlertEngine.CpuPersistenceMetric)];
        Assert.Equal(t.AddMinutes(2), record.LastObservedSampleUtc);
        Assert.Equal(1, record.State.ConsecutiveBreaches);
        Assert.Empty(h.Deliverer.Outcomes);

        /* Re-anchored: the next sweep over the same list sees nothing new. */
        h.Now = Noon.AddSeconds(30);
        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-30), list));
        Assert.Equal(1, h.StateStore.Persistence[(Key, AlertEngine.CpuPersistenceMetric)].State.ConsecutiveBreaches);
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AClearBatchAfterAFiredIncident_ResolvesOnce()
    {
        var h = NewHarness();
        var engine = h.Build();
        var t = Noon.AddMinutes(-10);

        await engine.EvaluateServerAsync(Batch(Noon.AddMinutes(-30), Hot(t), Hot(t.AddMinutes(1)), Hot(t.AddMinutes(2))));
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = Noon.AddMinutes(1);
        await engine.EvaluateServerAsync(Batch(
            Noon.AddMinutes(-30), Hot(t), Hot(t.AddMinutes(1)), Hot(t.AddMinutes(2)), Cool(t.AddMinutes(3)), Cool(t.AddMinutes(4))));

        var resolved = Assert.Single(h.Resolutions);
        Assert.Equal("High CPU", resolved.MetricName);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task TheFiredCpuAlert_CarriesTheSampleAge_SetAtFireTime_AndTheLinePrintsOnce()
    {
        var h = NewHarness();
        var engine = h.Build();
        var newest = Noon.AddMinutes(-8);

        await engine.EvaluateServerAsync(Batch(
            Noon.AddMinutes(-30), Hot(newest.AddMinutes(-2)), Hot(newest.AddMinutes(-1)), Hot(newest)));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        var sample = fired.Context!.NewestSample!;
        Assert.Equal("CPU", sample.Source);
        Assert.Equal(newest, sample.InstantUtc);
        Assert.Equal(TimeSpan.FromMinutes(8), sample.Age);

        /* CPU's context holds no Details, so the hand-built text carries the line, built by SampleAgeLine. */
        var line = AlertDetailText.SampleAgeLine(sample)!;
        Assert.Equal("Newest CPU sample: 11:52:00 UTC, 8m 0s old", line);
        Assert.Equal(1, CountOf(fired.DetailText!, line));
        Assert.Equal(1, CountOf(AlertDetailText.WithSampleAge(fired.DetailText, fired.Context)!, line));
        Assert.Null(AlertDetailText.SampleAgeLineFor(fired.Context, fired.DetailText));
    }

    [Fact]
    public async Task ACpuAlertWithNoUsableInstant_CarriesNoAgeAndNoLine()
    {
        var h = NewHarness();
        var engine = h.Build();

        /* The one-row path with no sample instant at all (a host that supplies none): no age, no line. */
        for (var i = 0; i < AlertEngine.CpuBreachSamples; i++)
        {
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot(sqlCpu: 70, totalCpu: 95, noCpuSampleTime: true));
        }

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Null(fired.Context!.NewestSample);
        Assert.DoesNotContain("Newest", fired.DetailText!, StringComparison.Ordinal);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// The engine source still feeds the recent list through the gate: the base-RED half of this file (ruling 8).
    /// Pins the call sites and the four guards by their source text, and that the old under-fire reasoning is gone.
    /// </summary>
    [Fact]
    public void TheEngineFeedsTheRecentSampleList_SourceShape()
    {
        var source = ReadRepoFile("PerformanceMonitor.Alerting", "AlertEngine.cs");
        var select = source.IndexOf("private IReadOnlyList<CpuObservation> SelectCpuObservations(", StringComparison.Ordinal);
        Assert.True(select > 0, "SelectCpuObservations is gone");
        var end = source.IndexOf("private async Task FireCpuAsync(", select, StringComparison.Ordinal);
        var body = source[select..end];

        Assert.Contains("snapshot.RecentCpuSamples", body, StringComparison.Ordinal);
        Assert.Contains("s.SampleTimeUtc <= now", body, StringComparison.Ordinal);
        Assert.Contains("CpuSampleLookback", body, StringComparison.Ordinal);
        Assert.Contains("snapshot.WatchingSinceUtc", body, StringComparison.Ordinal);
        Assert.Contains("s.SampleTimeUtc > watermark.Value", body, StringComparison.Ordinal);
        Assert.Contains("TakeLast(CpuSampleBatchLimit)", body, StringComparison.Ordinal);
        Assert.Contains("OrderBy(s => s.SampleTimeUtc)", body, StringComparison.Ordinal);

        var check = source[source.IndexOf("private async Task CheckCpuAsync(", StringComparison.Ordinal)..select];
        Assert.Contains("SelectCpuObservations(snapshot, key, now)", check, StringComparison.Ordinal);
        /* The comment that chose to skip the older sample of a batch (and so under-fire) is replaced. */
        Assert.DoesNotContain("the older one is skipped", source, StringComparison.Ordinal);
    }
}
