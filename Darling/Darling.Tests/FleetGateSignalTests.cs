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
            Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(code, System.Text.RegularExpressions.Regex.Escape("CollectorCadence.IntervalElapsed(lastFailedStartUtc, DateTime.UtcNow, FailedStartBackoff)")).Count);
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

        Assert.Contains("_fleetGateStats?.RecordSlot(CollectorCadence.SkippedSlots(due, now, intervalSpan));", source, StringComparison.Ordinal);
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
        Assert.True(checks.All(c => source.IndexOf("DiscardStaleConnection(server, runtime);", c, StringComparison.Ordinal) - c < 120));
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
        Assert.Empty(Regex.Matches(code, @"[<>]=?\s*_next\w*Utc\b|\b_next\w*Utc\s*[<>]=?").Select(m => m.Value));

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
}
