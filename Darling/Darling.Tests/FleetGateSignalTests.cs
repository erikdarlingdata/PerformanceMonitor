/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4732: the skipped-slot count and the backward-clock clamp, both pure and both on <see cref="CollectorCadence"/>.
/// </summary>
public sealed class CollectorCadenceSkippedSlotsTests
{
    private static readonly DateTime T0 = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    [Theory]
    [InlineData(0)]
    [InlineData(30)]
    [InlineData(59)]
    public void ARunOnTime_SkipsNothing(int secondsLate)
    {
        Assert.Equal(0, CollectorCadence.SkippedSlots(T0, T0.AddSeconds(secondsLate), Minute));
    }

    [Fact]
    public void ASlotThatIsNotDueYet_SkipsNothing()
    {
        Assert.Equal(0, CollectorCadence.SkippedSlots(T0, T0.AddSeconds(-10), Minute));
    }

    [Theory]
    [InlineData(60, 1)]
    [InlineData(119, 1)]
    [InlineData(330, 5)]
    [InlineData(600, 10)]
    public void ALateRun_CountsTheSlotsItSteppedOver(int secondsLate, long expected)
    {
        Assert.Equal(expected, CollectorCadence.SkippedSlots(T0, T0.AddSeconds(secondsLate), Minute));
    }

    [Fact]
    public void TheCount_AgreesWithWhereNextDueLands()
    {
        for (var seconds = -30; seconds <= 900; seconds += 7)
        {
            var now = T0.AddSeconds(seconds);
            var skipped = CollectorCadence.SkippedSlots(T0, now, Minute);
            Assert.Equal(T0 + TimeSpan.FromTicks(Minute.Ticks * (skipped + 1)), CollectorCadence.NextDue(T0, now, Minute));
        }
    }

    [Fact]
    public void ANonPositiveInterval_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => CollectorCadence.SkippedSlots(T0, T0, TimeSpan.Zero));
    }

    [Fact]
    public void ADueTimeTwoIntervalsAhead_RunsNow()
    {
        Assert.Equal(T0, CollectorCadence.ClampDue(T0.AddMinutes(2), T0, Minute));
        Assert.True(DarlingWorker.StampIsDue(T0.AddMinutes(2), Minute, T0));
    }

    [Fact]
    public void ADueTimeHalfAnIntervalAhead_Waits()
    {
        var due = T0.AddSeconds(30);
        Assert.Equal(due, CollectorCadence.ClampDue(due, T0, Minute));
        Assert.False(DarlingWorker.StampIsDue(due, Minute, T0));
    }

    [Fact]
    public void ADueTimeExactlyOneIntervalAhead_StillWaits_AndAPastOneIsDue()
    {
        Assert.False(DarlingWorker.StampIsDue(T0.AddMinutes(1), Minute, T0));
        Assert.True(DarlingWorker.StampIsDue(T0, Minute, T0));
        Assert.True(DarlingWorker.StampIsDue(T0.AddMinutes(-5), Minute, T0));
        Assert.True(DarlingWorker.StampIsDue(DateTime.MinValue, Minute, T0));
    }

    [Fact]
    public void IntervalElapsed_DecidesAsTheElapsedTimeDid_ExceptThatALastRunAheadOfTheClockCountsAsElapsed()
    {
        Assert.False(CollectorCadence.IntervalElapsed(T0, T0, Minute));
        Assert.False(CollectorCadence.IntervalElapsed(T0.AddSeconds(-30), T0, Minute));
        Assert.True(CollectorCadence.IntervalElapsed(T0.AddMinutes(-1), T0, Minute));
        Assert.True(CollectorCadence.IntervalElapsed(DateTime.MinValue, T0, Minute));
        Assert.True(CollectorCadence.IntervalElapsed(T0.AddSeconds(1), T0, Minute));
        Assert.True(CollectorCadence.IntervalElapsed(T0.AddMinutes(10), T0, Minute));

        for (var seconds = 0; seconds <= 180; seconds += 5)
        {
            var last = T0.AddSeconds(-seconds);
            Assert.Equal(T0 - last >= Minute, CollectorCadence.IntervalElapsed(last, T0, Minute));
        }
    }

    [Fact]
    public void TheServiceRetryAndRecheckThrottles_DecideThroughIntervalElapsed_NotFromTheRawElapsedTime()
    {
        foreach (var host in new[] { "DarlingMcpHostService.cs", "DarlingWebHostService.cs" })
        {
            var code = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", host);
            Assert.DoesNotContain("DateTime.UtcNow - lastFailedStartUtc", code, StringComparison.Ordinal);
            Assert.Equal(2, Regex.Matches(code, Regex.Escape("CollectorCadence.IntervalElapsed(lastFailedStartUtc, DateTime.UtcNow, FailedStartBackoff)")).Count);
        }

        var runner = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        Assert.DoesNotContain("DateTime.UtcNow - deniedAt <", runner, StringComparison.Ordinal);
        Assert.Contains("!CollectorCadence.IntervalElapsed(deniedAt, DateTime.UtcNow, AzureMasterRecheckInterval)", runner, StringComparison.Ordinal);

        var compose = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Compose", "ComposeStoreAvailability.cs");
        Assert.DoesNotContain("DateTime.UtcNow - entry.ProbedAtUtc <", compose, StringComparison.Ordinal);
        Assert.Contains("!CollectorCadence.IntervalElapsed(entry.ProbedAtUtc, DateTime.UtcNow, ReprobeInterval)", compose, StringComparison.Ordinal);
    }
}

/// <summary>#4732: the per-minute buckets behind the fleet-gate signal.</summary>
public sealed class FleetGateStatsTests
{
    private static readonly DateTime T0 = new(2026, 9, 29, 12, 0, 30, DateTimeKind.Utc);

    [Fact]
    public void ASaturatedMinute_RecordsItsSkips()
    {
        var now = T0;
        var stats = new FleetGateStats(() => now);

        for (var i = 0; i < 3; i++)
        {
            stats.RecordSlot(0);
        }

        stats.RecordSlot(4);
        stats.RecordSlot(4);

        var snapshot = stats.Snapshot();
        Assert.Equal(5, snapshot.Run);
        Assert.Equal(8, snapshot.Skipped);
        Assert.Equal(13, snapshot.Due);
        Assert.Equal(100.0 * 8 / 13, snapshot.SkippedPercent, 6);
    }

    [Fact]
    public void TheWindow_KeepsSixtyMinutes_AndDropsTheOldBuckets()
    {
        var now = T0;
        var stats = new FleetGateStats(() => now);
        stats.RecordSlot(2);

        now = T0.AddMinutes(59);
        Assert.Equal(2, stats.Snapshot().Skipped);

        now = T0.AddMinutes(60);
        Assert.Equal(0, stats.Snapshot().Skipped);
        Assert.Equal(0, stats.Snapshot().Run);
    }

    [Fact]
    public void ABucketSlotReusedAnHourLater_StartsEmpty()
    {
        var now = T0;
        var stats = new FleetGateStats(() => now);
        stats.RecordSlot(7);

        now = T0.AddMinutes(60);
        stats.RecordSlot(1);

        var snapshot = stats.Snapshot();
        Assert.Equal(1, snapshot.Run);
        Assert.Equal(1, snapshot.Skipped);
    }

    [Fact]
    public void BucketsAcrossTheHour_AreSummed()
    {
        var now = T0;
        var stats = new FleetGateStats(() => now);
        for (var minute = 0; minute < 90; minute++)
        {
            now = T0.AddMinutes(minute);
            stats.RecordSlot(1);
        }

        var snapshot = stats.Snapshot();
        Assert.Equal(60, snapshot.Run);
        Assert.Equal(60, snapshot.Skipped);
    }

    [Fact]
    public void AClockStepBack_FoldsTheBucketsItLeftAheadIntoTheCurrentMinute_AndCountsThemOnce()
    {
        var now = T0;
        var stats = new FleetGateStats(() => now);
        stats.RecordSlot(2);

        now = T0.AddMinutes(30);
        stats.RecordSlot(3);
        stats.RecordQueueWait(TimeSpan.FromMilliseconds(400));

        /* The clock steps back 20 minutes, so the minute-30 bucket is 20 minutes ahead of it. Ignoring that bucket until
           the clock caught up would read 1 run and 2 skipped slots here, though 2 and 5 happened in the last hour. */
        now = T0.AddMinutes(10);
        var stepped = stats.Snapshot();
        Assert.Equal(2, stepped.Run);
        Assert.Equal(5, stepped.Skipped);
        Assert.Equal(1, stepped.QueueWaits);
        Assert.Equal(TimeSpan.FromMilliseconds(400), stepped.QueueWaitMax);

        /* Recording goes on in the current minute, and the folded counts are not counted again when the clock
           reaches the minute they were first stamped in. */
        stats.RecordSlot(1);
        now = T0.AddMinutes(30);
        var caughtUp = stats.Snapshot();
        Assert.Equal(3, caughtUp.Run);
        Assert.Equal(6, caughtUp.Skipped);
        Assert.Equal(1, caughtUp.QueueWaits);

        /* They age out an hour after the step like anything recorded then: the current minute is the last one in. */
        now = T0.AddMinutes(69);
        Assert.Equal(2, stats.Snapshot().Run);
        now = T0.AddMinutes(70);
        Assert.Equal(0, stats.Snapshot().Run);
    }

    [Fact]
    public void AClockStepBackOfWholeHours_FoldsIntoTheSlotThatTheAheadBucketSharedWithTheCurrentMinute()
    {
        var now = T0.AddMinutes(30);
        var stats = new FleetGateStats(() => now);
        stats.RecordSlot(5);

        /* Two hours back: the minute the bucket was stamped in and the current minute use the same slot of the ring. */
        now = T0.AddMinutes(30).AddHours(-2);
        stats.RecordSlot(1);

        var snapshot = stats.Snapshot();
        Assert.Equal(2, snapshot.Run);
        Assert.Equal(6, snapshot.Skipped);
    }

    [Fact]
    public void ABucketThatHadAgedOutBeforeTheStep_IsNotFoldedBackIn()
    {
        var now = T0;
        var stats = new FleetGateStats(() => now);
        stats.RecordSlot(9);

        /* Two hours later, in another slot, so the old bucket is still in the ring, out of the window. */
        now = T0.AddMinutes(121);
        stats.RecordSlot(1);

        /* The clock steps back behind the old bucket. It was out of the window before the step, and stays out. */
        now = T0.AddMinutes(-5);
        var snapshot = stats.Snapshot();
        Assert.Equal(1, snapshot.Run);
        Assert.Equal(1, snapshot.Skipped);
    }

    [Fact]
    public void QueueWaits_CountTotalAndTrackTheLongest()
    {
        var now = T0;
        var stats = new FleetGateStats(() => now);
        stats.RecordQueueWait(TimeSpan.FromMilliseconds(100));
        now = T0.AddMinutes(1);
        stats.RecordQueueWait(TimeSpan.FromMilliseconds(300));
        stats.RecordQueueWait(TimeSpan.FromMilliseconds(200));

        var snapshot = stats.Snapshot();
        Assert.Equal(3, snapshot.QueueWaits);
        Assert.Equal(TimeSpan.FromMilliseconds(600), snapshot.QueueWaitTotal);
        Assert.Equal(TimeSpan.FromMilliseconds(300), snapshot.QueueWaitMax);
        Assert.Equal(TimeSpan.FromMilliseconds(200), snapshot.QueueWaitAverage);
    }

    [Fact]
    public void ConcurrentRecording_LosesNothing()
    {
        var stats = new FleetGateStats(() => T0);
        Parallel.For(0, 2000, _ =>
        {
            stats.RecordSlot(1);
            stats.RecordQueueWait(TimeSpan.FromMilliseconds(1));
        });

        var snapshot = stats.Snapshot();
        Assert.Equal(2000, snapshot.Run);
        Assert.Equal(2000, snapshot.Skipped);
        Assert.Equal(2000, snapshot.QueueWaits);
    }

    [Fact]
    public void TheLogLine_IsDueOnceAnHour_AndAtOnceWhenTheStateChanges()
    {
        var cadence = new FleetGateLogCadence();
        Assert.False(cadence.ShouldLog(false, T0));
        Assert.False(cadence.ShouldLog(false, T0.AddMinutes(59)));
        Assert.True(cadence.ShouldLog(false, T0.AddMinutes(60)));
        Assert.False(cadence.ShouldLog(false, T0.AddMinutes(61)));
        Assert.True(cadence.ShouldLog(true, T0.AddMinutes(62)));
        Assert.False(cadence.ShouldLog(true, T0.AddMinutes(63)));
        Assert.True(cadence.ShouldLog(false, T0.AddMinutes(64)));
    }

    [Fact]
    public void TheLogLine_IsDueAfterTheClockStepsBack()
    {
        var cadence = new FleetGateLogCadence();
        Assert.False(cadence.ShouldLog(false, T0));
        Assert.True(cadence.ShouldLog(true, T0.AddMinutes(1)));

        /* The next hourly line is stamped an hour ahead; a clock that steps back two hours leaves it more than
           an hour away, which counts as due. */
        Assert.True(cadence.ShouldLog(true, T0.AddMinutes(1).AddHours(-2)));
    }

    [Fact]
    public void TheLogLine_NamesTheCountsTheWaitsAndTheGateWidth()
    {
        var line = FleetGateLine.Describe(
            new FleetGateSnapshot(1900, 100, 40, TimeSpan.FromSeconds(8), TimeSpan.FromSeconds(3)), 4);

        Assert.Contains("1,900 collector slots ran", line, StringComparison.Ordinal);
        Assert.Contains("100 skipped (5%)", line, StringComparison.Ordinal);
        Assert.Contains("40 bodies queued for a gate slot", line, StringComparison.Ordinal);
        Assert.Contains("average 200 ms, longest 3,000 ms", line, StringComparison.Ordinal);
        Assert.Contains("gate width 4", line, StringComparison.Ordinal);
        Assert.Contains("last 60 minutes", line, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWorker_CountsEveryCollectorSlotItAdvances_AndTimesTheFleetGateWait()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();

        Assert.Contains("_fleetGateStats?.RecordSlot(_skipCreditFloor.Skipped(due, now, intervalSpan));", source, StringComparison.Ordinal);
        Assert.Contains("_fleetGateStats?.RecordQueueWait(Stopwatch.GetElapsedTime(gateWaitStarted));", source, StringComparison.Ordinal);

        /* Every place that advances a COLLECTOR's due time on the grid is preceded by the count. The store
           maintenance loops advance their own stamps through NextGridStamp and are not slots. */
        const string advance = "server.NextDue[name] = CollectorCadence.NextDue(";
        var advances = 0;
        for (var at = source.IndexOf(advance, StringComparison.Ordinal); at >= 0; at = source.IndexOf(advance, at + 1, StringComparison.Ordinal))
        {
            advances++;
            var counted = source.LastIndexOf("_fleetGateStats?.RecordSlot(", at, StringComparison.Ordinal);
            Assert.True(counted >= 0 && at - counted < 200, "a collector due time advances without counting its slot");
        }

        Assert.True(advances >= 1);
    }
}

/// <summary>#4732: the "Collection Falling Behind" self-alert.</summary>
public sealed class FleetGateSelfAlertTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DarlingSelfAlertEvaluator.FleetGateReport Report(long run, long skipped, DateTime end) =>
        new(run, skipped, QueueWaits: 12, QueueWaitTotal: TimeSpan.FromSeconds(6), QueueWaitMax: TimeSpan.FromSeconds(2), GateWidth: 4, WindowEndUtc: end);

    [Fact]
    public async Task FiresAtFivePercentAndTwentySlots_NamingTheCountsTheGateWidthAndTheHour()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        /* 20 of 400 due slots is exactly 5%. */
        await e.ApplyFleetGateAsync(Report(run: 380, skipped: 20, h.Now), Ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.FleetGateMetric, fired.MetricName);
        Assert.Equal("Collection Falling Behind", fired.MetricName);
        Assert.Equal("fleetgate", fired.ServerKey);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal(5.0, fired.NumericCurrentValue);
        Assert.Contains("skipped 20 of 400", fired.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("380 collector slots", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("width 4", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("hour to 2026-07-01 12:00 UTC", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("12 collection bodies waited", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DoesNotFire_AtFourPercent_OrAtNineteenSkippedSlots()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        /* 24 of 600 is 4%: over the floor of 20, under the share. */
        await e.ApplyFleetGateAsync(Report(run: 576, skipped: 24, h.Now), Ct);
        /* 19 of 20 is 95%: over the share, one under the floor. */
        await e.ApplyFleetGateAsync(Report(run: 1, skipped: 19, h.Now), Ct);
        /* And a quiet hour. */
        await e.ApplyFleetGateAsync(Report(run: 2400, skipped: 0, h.Now), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);
    }

    [Fact]
    public async Task StaysSilent_WhenTheMasterAlertsSwitchIsOff()
    {
        var h = new DarlingSelfAlertTests.Harness();
        h.Settings.AlertsEnabled = false;

        await h.Build().ApplyFleetGateAsync(Report(run: 100, skipped: 100, h.Now), Ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Resolves_OnlyAfterAFullHourUnderOnePercent()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var start = h.Now;

        await e.ApplyFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* 0.5% starts the clock at +10 minutes. */
        h.Now = start.AddMinutes(10);
        await e.ApplyFleetGateAsync(Report(run: 2000, skipped: 10, h.Now), Ct);
        h.Now = start.AddMinutes(69);
        await e.ApplyFleetGateAsync(Report(run: 2000, skipped: 10, h.Now), Ct);
        Assert.Empty(h.History.Records);

        h.Now = start.AddMinutes(70);
        await e.ApplyFleetGateAsync(Report(run: 2000, skipped: 10, h.Now), Ct);

        var resolved = Assert.Single(h.History.Records);
        Assert.Equal(DarlingSelfAlertEvaluator.FleetGateClearedMetric, resolved.MetricName);
        Assert.True(AlertMetricClassifier.IsResolution(resolved.MetricName));

        /* Resolved means armed again: the next bad hour fires afresh. */
        h.Now = start.AddMinutes(200);
        await e.ApplyFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task ABusyReadingInsideTheHour_RestartsTheQuietClock()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var start = h.Now;

        await e.ApplyFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct);

        h.Now = start.AddMinutes(10);
        await e.ApplyFleetGateAsync(Report(run: 2000, skipped: 10, h.Now), Ct);
        /* 3%: between the two thresholds. It is not quiet, and it does not fire again. */
        h.Now = start.AddMinutes(30);
        await e.ApplyFleetGateAsync(Report(run: 970, skipped: 30, h.Now), Ct);
        h.Now = start.AddMinutes(40);
        await e.ApplyFleetGateAsync(Report(run: 2000, skipped: 10, h.Now), Ct);
        h.Now = start.AddMinutes(99);
        await e.ApplyFleetGateAsync(Report(run: 2000, skipped: 10, h.Now), Ct);
        Assert.Empty(h.History.Records);

        h.Now = start.AddMinutes(100);
        await e.ApplyFleetGateAsync(Report(run: 2000, skipped: 10, h.Now), Ct);
        Assert.Single(h.History.Records);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AStandingCondition_RestatesOnlyAfterTheSharedCooldown()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        var start = h.Now;

        await e.ApplyFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct);
        h.Now = start.AddMinutes(1);
        await e.ApplyFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = start.AddHours(3);
        await e.ApplyFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task TheWrapper_ReportsWhetherTheAlertIsStanding()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        Assert.False(await e.EvaluateFleetGateAsync(Report(run: 2400, skipped: 0, h.Now), Ct));
        Assert.True(await e.EvaluateFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct));
    }

    [Fact]
    public void TheReport_IsBuiltFromTheGateCounts()
    {
        var end = new DateTime(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);
        var report = DarlingWorker.BuildFleetGateReport(
            new FleetGateSnapshot(10, 3, 5, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(4)), 8, end);

        Assert.Equal(10, report.Run);
        Assert.Equal(3, report.Skipped);
        Assert.Equal(5, report.QueueWaits);
        Assert.Equal(TimeSpan.FromSeconds(10), report.QueueWaitTotal);
        Assert.Equal(TimeSpan.FromSeconds(4), report.QueueWaitMax);
        Assert.Equal(8, report.GateWidth);
        Assert.Equal(end, report.WindowEndUtc);
    }

    [Fact]
    public void TheMetric_BelongsToTheSelfMonitorFamily_AndItsResolutionIsRecognised()
    {
        Assert.Equal(AlertFamily.SelfMonitor, AlertFamily.Of(DarlingSelfAlertEvaluator.FleetGateMetric));
        Assert.True(AlertMetricClassifier.IsResolution(DarlingSelfAlertEvaluator.FleetGateClearedMetric));
        Assert.False(AlertMetricClassifier.IsStateOnly(DarlingSelfAlertEvaluator.FleetGateMetric));
    }
}

/// <summary>#4732: a connect that finishes after the server's definition was edited is discarded.</summary>
public sealed class ConnectAfterDefinitionEditTests
{
    private static ServerRuntime RuntimeFor(MonitoredServer config) => new()
    {
        Config = config,
        ConnectionString = "Server=s.invalid;Integrated Security=true",
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
        StorageName = "s",
        ServerId = 7,
    };

    private static MonitoredServer Server(string host, decimal cost = 0m) =>
        new() { Name = "s", Host = host, StoredServerId = 7, MonthlyCostUsd = cost };

    [Fact]
    public void AnUneditedServer_KeepsItsRuntime()
    {
        var def = Server("a.invalid");
        var runtime = RuntimeFor(def);

        Assert.True(DarlingWorker.ConnectedRuntimeIsCurrent(def, def, runtime, runtime));
    }

    [Fact]
    public void AConnectionEdit_DiscardsTheRuntime()
    {
        var runtime = RuntimeFor(Server("old.invalid"));

        Assert.False(DarlingWorker.ConnectedRuntimeIsCurrent(Server("new.invalid"), Server("old.invalid"), runtime, runtime));
    }

    [Fact]
    public void ACostOnlyEdit_KeepsTheRuntime()
    {
        var runtime = RuntimeFor(Server("a.invalid", 100m));

        Assert.True(DarlingWorker.ConnectedRuntimeIsCurrent(Server("a.invalid", 900m), Server("a.invalid", 100m), runtime, runtime));
    }

    [Fact]
    public void ARuntimeTheReloadCleared_IsDiscarded()
    {
        var def = Server("a.invalid");
        var runtime = RuntimeFor(def);

        /* ReconcileServers sets Runtime to null when it replaces the definition. */
        Assert.False(DarlingWorker.ConnectedRuntimeIsCurrent(def, def, installed: null, connected: runtime));
        Assert.False(DarlingWorker.ConnectedRuntimeIsCurrent(def, def, RuntimeFor(def), runtime));
    }

    [Fact]
    public void TheConnectBody_AsksAgainAfterTheInstall_BeforeTheStoreWrites_AndBeforeTheFirstPass()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        var connect = source.IndexOf("private async Task TryConnectAsync(", StringComparison.Ordinal);
        Assert.True(connect > 0);

        var install = source.IndexOf("server.Runtime = runtime;", connect, StringComparison.Ordinal);
        var upsert = source.IndexOf("DarlingObservability.UpsertServerAsync(_postgres!, runtime", connect, StringComparison.Ordinal);
        var onLoad = source.IndexOf("var watermarks = await ReadCollectorWatermarksAsync(_postgres!, serverId", connect, StringComparison.Ordinal);
        Assert.True(install > connect && upsert > install && onLoad > upsert);

        var checks = new System.Collections.Generic.List<int>();
        for (var at = source.IndexOf("if (ConnectionIsStale())", connect, StringComparison.Ordinal);
             at >= 0;
             at = source.IndexOf("if (ConnectionIsStale())", at + 1, StringComparison.Ordinal))
        {
            checks.Add(at);
        }

        Assert.Equal(3, checks.Count);
        Assert.InRange(checks[0], install, upsert);
        Assert.InRange(checks[1], install, upsert);
        Assert.True(checks[2] > onLoad);

        /* Each check is followed, within a few lines, by its own discard. IndexOf answers -1 for a call that is gone,
           and -1 minus the check's offset is below 120, so the distance alone would pass a check with no discard. */
        foreach (var check in checks)
        {
            var discard = source.IndexOf("DiscardStaleConnection(server, runtime);", check, StringComparison.Ordinal);
            Assert.True(discard > check && discard - check < 120, $"the stale-connection check at offset {check} is not followed by DiscardStaleConnection");
        }
    }
}

/// <summary>
/// #4732: the worker loop's own timers (disk pressure, mute and TLS checks, fleet gate, compression, sweeps, the store
/// self-metrics tick, the daily purge) decide due through <see cref="DarlingWorker.StampIsDue"/>, so a wall clock that
/// steps backwards no longer pauses each of them for as long as the step. The span each one passes must be the longest
/// its writer can put the stamp ahead of now: a shorter span would wake the timer early.
/// </summary>
public sealed class WorkerLoopTimerClockStepTests
{
    private static readonly DateTime T0 = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);

    private static string WorkerCode() => CSharpMemberMap.Of(ServerConnectBackoffTests.ReadWorkerSource()).Code;

    [Fact]
    public void NoTimerStampIsComparedAgainstTheRawClock_AndEveryStampFieldIsReadThroughStampIsDue()
    {
        var code = WorkerCode();

        Assert.DoesNotContain("DateTime.UtcNow >= _next", code, StringComparison.Ordinal);

        /* A stored due stamp is a DateTime that starts at MinValue ("due at once"): the worker's _next... fields and
           the per-server Next... and FirstSweepDueUtc properties. The names come from the source, so a stamp added
           later is covered without editing this test. None is compared with a relational operator, on either side of
           it: the raw compare holds the work for the size of a backward step, StampIsDue does not (#4732). The first
           launch (FirstSweepDueUtc) sat outside this scan while it only looked at the _next fields. */
        var stamps = Regex.Matches(code, @"\bDateTime\s+(\w+)\s*(?:\{\s*get;\s*set;\s*\}\s*)?=\s*DateTime\.MinValue;")
            .Select(m => m.Groups[1].Value)
            .Where(name => name.StartsWith("_next", StringComparison.Ordinal)
                || name.StartsWith("Next", StringComparison.Ordinal)
                || name.Contains("Due", StringComparison.Ordinal))
            .Distinct()
            .ToList();
        foreach (var known in new[] { "FirstSweepDueUtc", "NextConnectAttempt", "NextAlertSweep", "NextSelfAlertSweep", "NextCustomAlertSweep", "NextPileupSweep", "NextAnalysisDue" })
        {
            Assert.Contains(known, stamps);
        }

        var names = "_next\\w*Utc|" + string.Join("|", stamps.Select(Regex.Escape));
        var rawCompare = new Regex(@"(?<![=\-])[<>]=?\s*(?:[\w.]+\.)?(?:" + names + @")\b|\b(?:" + names + @")\s*[<>]=?");
        Assert.Empty(rawCompare.Matches(code).Select(m => m.Value));

        var fields = Regex.Matches(code, @"private DateTime (_next\w+Utc) = DateTime\.MinValue;")
            .Select(m => m.Groups[1].Value)
            .ToList();
        Assert.True(fields.Count >= 12, "the worker's timer stamp fields were not found");
        foreach (var field in fields)
        {
            Assert.True(
                Regex.Matches(code, @"StampIsDue\(\s*" + field + @"\s*,").Count == 1,
                field + " must be read through exactly one StampIsDue call");
        }
    }

    [Theory]
    [InlineData("_nextDiskCheckUtc", "s_diskCheckInterval")]
    [InlineData("_nextCustomAlertHealthCheckUtc", "s_customAlertHealthInterval")]
    [InlineData("_nextStaleMuteCheckUtc", "s_staleMuteCheckInterval")]
    [InlineData("_nextWebTlsCheckUtc", "s_webTlsCheckInterval")]
    [InlineData("_nextFleetGateCheckUtc", "s_fleetGateCheckInterval")]
    [InlineData("_nextStoreSettingsCheckUtc", "s_storeSettingsCheckInterval")]
    public void ANowPlusIntervalTimer_SpansTheSameIntervalItsStampIsWrittenWith(string field, string interval)
    {
        var code = WorkerCode();

        Assert.Contains(field + " = DateTime.UtcNow.Add(" + interval + ");", code, StringComparison.Ordinal);
        Assert.Contains("StampIsDue(" + field + ", " + interval + ", DateTime.UtcNow)", code, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("_nextCheckpointSyncSampleUtc", "s_checkpointSyncSampleInterval")]
    [InlineData("_nextStoreMetricsUtc", "s_storeMetricsInterval")]
    public void AGridTimer_SpansTheSameIntervalItsStampIsAdvancedWith(string field, string interval)
    {
        var code = WorkerCode();

        Assert.Contains(field + " = NextGridStamp(" + field + ", DateTime.UtcNow, " + interval + ");", code, StringComparison.Ordinal);
        Assert.Contains("StampIsDue(" + field + ", " + interval + ", DateTime.UtcNow)", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOversizedPlanSweep_SpansTheLongestDelayTheSweepCanChoose()
    {
        var longest = OversizedPlanBacklogSweep.LongestSweepDelay;
        Assert.True(longest >= OversizedPlanBacklogSweep.SweepInterval);
        Assert.True(longest >= OversizedPlanBacklogSweep.ConnectWaitDelay);

        var largest = TimeSpan.Zero;
        foreach (var sweepable in new[] { 0, 1, 5 })
        {
            foreach (var connected in new[] { 0, 1, 5 })
            {
                for (var attempts = 0; attempts <= OversizedPlanBacklogSweep.MaxConnectWaitAttempts + 3; attempts++)
                {
                    var (delay, _) = OversizedPlanBacklogSweep.NextSweepDelay(sweepable, connected, attempts);
                    Assert.True(delay <= longest, $"delay {delay} for ({sweepable}, {connected}, {attempts}) is past the span {longest}");
                    largest = delay > largest ? delay : largest;
                }
            }
        }

        Assert.Equal(longest, largest);
        Assert.Contains(
            "StampIsDue(_nextOversizedPlanSweepUtc, OversizedPlanBacklogSweep.LongestSweepDelay, DateTime.UtcNow)",
            WorkerCode(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheCompressionCheck_SpansItsIntervalPlusTheSnapPhase_AndNoStampIsWrittenPastIt()
    {
        var phase = TimeSpan.FromSeconds(TimescaleSupport.CompressionCheckPhaseSeconds);
        var interval = DarlingWorker.CompressionCheckSpan - phase;
        Assert.Equal(TimeSpan.FromHours(1), interval);

        var largest = TimeSpan.Zero;
        for (var seconds = 0; seconds <= 7200; seconds++)
        {
            var now = T0.AddSeconds(seconds).AddMilliseconds(seconds % 3 * 333);
            var ahead = TimescaleSupport.NextCompressionCheckUtc(now, interval) - now;
            Assert.True(ahead <= DarlingWorker.CompressionCheckSpan, $"{now:O} wrote a stamp {ahead} ahead");
            largest = ahead > largest ? ahead : largest;
        }

        /* A due time that lands exactly on a minute boundary gets the whole phase added, and that is the largest offset. */
        Assert.Equal(DarlingWorker.CompressionCheckSpan, largest);
    }

    [Theory]
    [InlineData(int.MinValue)]
    [InlineData(0)]
    [InlineData(15)]
    [InlineData(60)]
    [InlineData(1439)]
    [InlineData(1440)]
    [InlineData(100000)]
    [InlineData(int.MaxValue)]
    public void TheFleetSweep_SpansTheLargestIntervalTheClampAllows(int configured)
    {
        var minutes = FleetSweepEngine.ClampIntervalMinutes(configured);
        var written = DarlingWorker.NextGridStamp(DateTime.MinValue, T0, TimeSpan.FromMinutes(minutes));

        Assert.True(written - T0 <= DarlingWorker.FleetSweepStampSpan);
        Assert.Equal(TimeSpan.FromMinutes(FleetSweepCadence.IntervalMinutesCeiling), DarlingWorker.FleetSweepStampSpan);

        /* Written at the longest interval and then the operator lowers it: the stamp still waits its full time. */
        var longWait = DarlingWorker.NextGridStamp(DateTime.MinValue, T0, TimeSpan.FromMinutes(FleetSweepCadence.IntervalMinutesCeiling));
        Assert.False(DarlingWorker.StampIsDue(longWait, DarlingWorker.FleetSweepStampSpan, T0.AddMinutes(1)));
    }

    [Fact]
    public void NextGridStamp_ReAnchorsAtNow_WhenTheStampIsMoreThanOneIntervalAhead()
    {
        var minute = TimeSpan.FromMinutes(1);

        /* On time, late, and up to one interval ahead: exactly what the grid gave before. */
        Assert.Equal(T0 + minute, DarlingWorker.NextGridStamp(DateTime.MinValue, T0, minute));
        Assert.Equal(T0.AddMinutes(2), DarlingWorker.NextGridStamp(T0.AddMinutes(1), T0.AddMinutes(1).AddSeconds(15), minute));
        Assert.Equal(T0.AddMinutes(5), DarlingWorker.NextGridStamp(T0, T0.AddMinutes(4).AddSeconds(20), minute));
        Assert.Equal(T0.AddSeconds(90), DarlingWorker.NextGridStamp(T0.AddSeconds(30), T0, minute));

        /* Ahead by more than an interval (the clock stepped back): re-anchored, so it is one interval ahead and not more. */
        Assert.Equal(T0 + minute, DarlingWorker.NextGridStamp(T0.AddMinutes(10), T0, minute));
        Assert.Equal(T0 + TimeSpan.FromHours(1), DarlingWorker.NextGridStamp(T0.AddHours(3), T0, TimeSpan.FromHours(1)));
    }

    [Theory]
    [InlineData(1, 10)]
    [InlineData(1, 90)]
    [InlineData(60, 600)]
    public void AGridTimer_AfterABackwardStep_RunsOnceAndThenWaitsItsIntervalAgain(int intervalMinutes, int stepBackMinutes)
    {
        var interval = TimeSpan.FromMinutes(intervalMinutes);
        var stamp = DarlingWorker.NextGridStamp(DateTime.MinValue, T0, interval);
        var now = T0.AddSeconds(15).AddMinutes(-stepBackMinutes);

        Assert.True(DarlingWorker.StampIsDue(stamp, interval, now), "the stamp must not wait out the step");

        stamp = DarlingWorker.NextGridStamp(stamp, now, interval);
        Assert.Equal(now + interval, stamp);
        for (var pass = 1; pass <= 3; pass++)
        {
            Assert.False(DarlingWorker.StampIsDue(stamp, interval, now.AddSeconds(15 * pass)), "the timer fired again on the next pass");
        }

        Assert.True(DarlingWorker.StampIsDue(stamp, interval, now + interval));
    }

    /// <summary>The per-server timers (the connect retry, the four alert sweeps, the analysis and a server's first launch)
    /// are each read through <see cref="DarlingWorker.StampIsDue"/> exactly once, with the span their stamp is written
    /// with. A read put back to a plain compare, or with a shorter span, turns this red.</summary>
    [Theory]
    [InlineData("NextConnectAttempt", "s_connectAttemptStampSpan", null)]
    [InlineData("NextSelfAlertSweep", "s_alertSweepInterval", "DateTime.UtcNow.Add(s_alertSweepInterval)")]
    [InlineData("NextCustomAlertSweep", "s_customAlertSweepInterval", "DateTime.UtcNow.Add(s_customAlertSweepInterval)")]
    [InlineData("NextAlertSweep", "s_alertSweepInterval", "DateTime.UtcNow.Add(s_alertSweepInterval)")]
    [InlineData("NextPileupSweep", "s_alertSweepInterval", "DateTime.UtcNow.Add(s_alertSweepInterval)")]
    [InlineData("NextAnalysisDue", "TimeSpan.FromMinutes(analysisIntervalMinutes)", "DateTime.UtcNow.AddMinutes(analysisIntervalMinutes)")]
    [InlineData("FirstSweepDueUtc", "TimeSpan.FromSeconds(ColdStartSpreadSeconds)", null)]
    public void APerServerTimer_IsReadThroughStampIsDue_WithTheSpanItsStampIsWrittenWith(string stamp, string span, string? writer)
    {
        var code = WorkerCode();

        var read = "StampIsDue(server." + stamp + ", " + span + ", DateTime.UtcNow)";
        Assert.True(
            Regex.Matches(code, Regex.Escape(read)).Count == 1,
            stamp + " must be read through exactly one StampIsDue call, with the span " + span);
        if (writer is not null)
        {
            Assert.Contains("server." + stamp + " = " + writer + ";", code, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheConnectRetry_SpansTheLongestBackoffTheConnectPolicyCanChoose()
    {
        var span = TimeSpan.FromSeconds(ServerConnectBackoff.CapSeconds * (1.0 + ServerConnectBackoff.JitterFraction));
        Assert.Equal(TimeSpan.FromSeconds(288), span);

        var largest = TimeSpan.Zero;
        for (var failures = 1; failures <= 60; failures++)
        {
            var longest = ServerConnectBackoff.NextDelay(failures, 1.0);
            Assert.True(longest <= span, $"after {failures} failures a retry stamp {longest} ahead is past the span {span}");
            largest = longest > largest ? longest : largest;
        }

        Assert.Equal(span, largest);
        Assert.Matches(
            @"s_connectAttemptStampSpan\s*=\s*TimeSpan\.FromSeconds\(ServerConnectBackoff\.CapSeconds \* \(1\.0 \+ ServerConnectBackoff\.JitterFraction\)\);",
            WorkerCode());
    }

    [Fact]
    public void TheFirstLaunch_SpansTheLargestColdStartOffset_AndAStepBackInsideTheWindowDoesNotHoldIt()
    {
        var span = TimeSpan.FromSeconds(DarlingWorker.ColdStartSpreadSeconds);

        for (var serverId = -500; serverId <= 5000; serverId++)
        {
            var due = DarlingWorker.ColdStartFirstSweepDue(T0, serverId);
            Assert.True(due - T0 < span, $"server {serverId} is held {due - T0} at start, past the span {span}");

            /* The clock unstepped: the launch waits out its own offset and no longer. */
            Assert.Equal(due <= T0, DarlingWorker.StampIsDue(due, span, T0));
            Assert.True(DarlingWorker.StampIsDue(due, span, due));

            /* The clock corrected backwards at boot, inside the window: the stamp is more than one span ahead of it, so the
               server launches at once and does not wait for the clock to come back to the stamp. */
            Assert.True(DarlingWorker.StampIsDue(due, span, T0.AddMinutes(-10)), $"server {serverId} waited out the step");
        }

        /* A server added by a reload has no stamp and launches at once. */
        Assert.True(DarlingWorker.StampIsDue(DateTime.MinValue, span, T0));
    }

    [Fact]
    public void TheCollectorLoop_ClampsADueTimeAheadOfTheClock_BeforeItComparesIt()
    {
        var code = WorkerCode();

        const string clamp = "due = CollectorCadence.ClampDue(due, now, intervalSpan);";
        var at = code.IndexOf(clamp, StringComparison.Ordinal);
        Assert.True(at > 0, "the collector loop no longer clamps a due time that a backward clock step left ahead of it");
        Assert.Equal(at, code.LastIndexOf(clamp, StringComparison.Ordinal));

        var compare = code.IndexOf("if (now < due)", at, StringComparison.Ordinal);
        Assert.True(compare > at && compare - at < 120, "the clamped due time is not the one compared with the clock");

        /* The grid advances from the clamped time too, so the stamp written is never more than one interval ahead. */
        Assert.Contains("server.NextDue[name] = CollectorCadence.NextDue(due, now, intervalSpan);", code, StringComparison.Ordinal);
    }
}
