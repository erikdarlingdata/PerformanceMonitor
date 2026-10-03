/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4938: the worker runs a daily collector that has a run time at its slot. The slot is the run time on the server's
/// clock plus the server's spread, one a day, and a run starts from the slot until an hour after it. These cases pin the
/// seed (<see cref="DarlingWorker.ComputeSeededNextDue"/>), the pass's decision (<see cref="DarlingWorker.StepRunTimeCollector"/>)
/// across daylight-saving changes, a fixed offset and an unknown clock, the clamp's room for a 25-hour stamp, the skipped-slot
/// count, and a schedule reload, with no store. A collector with no run time keeps every rule it had, which the existing
/// schedule tests (unchanged) pin.
/// </summary>
public sealed class DarlingRunTimeSchedulingTests
{
    private const int TwoAm = 120;
    private const int Daily = 1440;
    private const string Collector = "index_object_stats";
    private static readonly TimeSpan Jitter = TimeSpan.FromSeconds(90);

    private static DateTime Utc(int year, int month, int day, int hour = 0, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Utc);

    private static DarlingWorker.RunTimeRule UtcRule(int runAt = TwoAm, int serverId = 0) =>
        new(runAt, serverId, CollectorRunTime.LocalIsUtc);

    private static DarlingWorker.RunTimeRule EasternRule(int serverId = 0) =>
        new(TwoAm, serverId, ServerClock.Resolve("America/New_York", -300).ToUtc);

    /* ---- the seed: with a run time every row is the slot, and now + jitter only inside the hour after it ---- */

    [Fact]
    public void Seed_NeverRun_OutsideTheGrace_WaitsForTheNextSlot()
    {
        var now = Utc(2026, 10, 2, 12);
        Assert.Equal(Utc(2026, 10, 3, 2), DarlingWorker.ComputeSeededNextDue(null, Daily, now, Jitter, UtcRule()));
        Assert.Equal(now + Jitter, DarlingWorker.ComputeSeededNextDue(null, Daily, now, Jitter));
    }

    [Fact]
    public void Seed_Overdue_OutsideTheGrace_WaitsForTheNextSlot()
    {
        var now = Utc(2026, 10, 2, 12);
        var lastRun = Utc(2026, 9, 30, 2, 5);
        Assert.Equal(Utc(2026, 10, 3, 2), DarlingWorker.ComputeSeededNextDue(lastRun, Daily, now, Jitter, UtcRule()));
        Assert.Equal(now + Jitter, DarlingWorker.ComputeSeededNextDue(lastRun, Daily, now, Jitter));
    }

    [Fact]
    public void Seed_Overdue_InsideTheGrace_RunsNowPlusTheSeedJitter()
    {
        var now = Utc(2026, 10, 2, 2, 30);
        var lastRun = Utc(2026, 10, 1, 2, 5);
        Assert.Equal(now + Jitter, DarlingWorker.ComputeSeededNextDue(lastRun, Daily, now, Jitter, UtcRule()));
        Assert.Equal(now + Jitter, DarlingWorker.ComputeSeededNextDue(null, Daily, now, Jitter, UtcRule()));
    }

    [Fact]
    public void Seed_RanToday_WaitsForTheNextSlot_NotLastRunPlusTheInterval()
    {
        var now = Utc(2026, 10, 2, 12);
        var lastRun = Utc(2026, 10, 2, 2, 10);
        Assert.Equal(Utc(2026, 10, 3, 2), DarlingWorker.ComputeSeededNextDue(lastRun, Daily, now, Jitter, UtcRule()));
        Assert.Equal(lastRun.AddMinutes(Daily), DarlingWorker.ComputeSeededNextDue(lastRun, Daily, now, Jitter));
    }

    [Fact]
    public void Seed_AddsTheServersSpreadToTheSlot()
    {
        /* Server id 1800 spreads by 30 minutes. */
        var now = Utc(2026, 10, 2, 12);
        Assert.Equal(Utc(2026, 10, 3, 2, 30), DarlingWorker.ComputeSeededNextDue(null, Daily, now, Jitter, UtcRule(TwoAm, 1800)));
    }

    /* ---- the advance: the next stamp is the next day's slot on the server's clock, not the stamp plus 1440 ---- */

    [Fact]
    public void Advance_OnTheSpringForwardDay_Is0200LocalTheNextDay_NotThePlus1440Hour()
    {
        /* 02:00 on 2026-03-08 is inside the US Eastern gap, so it reads 03:00 EDT (07:00Z); the 9th is 02:00 EDT (06:00Z).
           The stamp plus 1440 minutes would be 07:00Z, an hour late. */
        var due = Utc(2026, 3, 8, 7);
        var step = DarlingWorker.StepRunTimeCollector(due, due.AddMinutes(10), DateTime.MinValue, Daily, EasternRule(), Jitter);
        Assert.Equal(DarlingWorker.RunTimeAction.HandOff, step.Action);
        Assert.Equal(Utc(2026, 3, 9, 6), step.NextDue);
        Assert.NotEqual(due.AddMinutes(Daily), step.NextDue);
    }

    [Fact]
    public void Advance_OnTheFallBackDay_Is0200LocalTheNextDay_NotThePlus1440Hour()
    {
        /* 02:00 EDT on 10-31 is 06:00Z; 02:00 EST on 11-01 is 07:00Z, 25 hours later. The stamp plus 1440 minutes would be
           06:00Z, which is 01:00 EST. */
        var due = Utc(2026, 10, 31, 6);
        var step = DarlingWorker.StepRunTimeCollector(due, due.AddMinutes(10), DateTime.MinValue, Daily, EasternRule(), Jitter);
        Assert.Equal(DarlingWorker.RunTimeAction.HandOff, step.Action);
        Assert.Equal(Utc(2026, 11, 1, 7), step.NextDue);
        Assert.NotEqual(due.AddMinutes(Daily), step.NextDue);
    }

    [Fact]
    public void Advance_OnAFixedOffsetClockAndAnUnknownClock_KeepsTheLocalTime()
    {
        var india = new DarlingWorker.RunTimeRule(TwoAm, 0, ServerClock.FixedOffset(330).ToUtc);
        var indiaDue = Utc(2026, 10, 2, 20, 30);
        var indiaStep = DarlingWorker.StepRunTimeCollector(indiaDue, indiaDue.AddMinutes(10), DateTime.MinValue, Daily, india, Jitter);
        Assert.Equal(Utc(2026, 10, 3, 20, 30), indiaStep.NextDue);

        var unknownDue = Utc(2026, 10, 2, 2);
        var unknownStep = DarlingWorker.StepRunTimeCollector(unknownDue, unknownDue.AddMinutes(10), DateTime.MinValue, Daily, UtcRule(), Jitter);
        Assert.Equal(Utc(2026, 10, 3, 2), unknownStep.NextDue);
    }

    /* ---- the clamp: a run-time stamp can sit a day and two hours ahead ---- */

    [Fact]
    public void ClampDue_A25HourStamp_IsAWait_NotAClockStep()
    {
        var now = Utc(2026, 11, 1, 12);
        var stamp = now.AddHours(25);
        var step = DarlingWorker.StepRunTimeCollector(stamp, now, DateTime.MinValue, Daily, UtcRule(), Jitter);
        Assert.Equal(DarlingWorker.RunTimeAction.NotDue, step.Action);
        Assert.Equal(stamp, step.NextDue);

        /* The plain interval would read the same stamp as a clock that stepped back, and run it at once. */
        Assert.Equal(now, CollectorCadence.ClampDue(stamp, now, TimeSpan.FromMinutes(Daily)));

        /* A stamp past the room (a day, an hour of spread and an hour of autumn) is still a clock step. */
        var past = DarlingWorker.StepRunTimeCollector(now.AddHours(27), now, DateTime.MinValue, Daily, UtcRule(), Jitter);
        Assert.Equal(DarlingWorker.RunTimeAction.HandOff, past.Action);
    }

    /* ---- skipped slots: a day lost while the loop ran counts 1, a day lost to a sleep or a stop counts 0 ---- */

    [Fact]
    public void Skip_PastTheGrace_NeverHandsOff_AndCountsOneWhenTheLoopWasRunning()
    {
        var due = Utc(2026, 10, 2, 2);
        var now = due.AddMinutes(61);
        var step = DarlingWorker.StepRunTimeCollector(due, now, DateTime.MinValue, Daily, UtcRule(), Jitter);
        Assert.Equal(DarlingWorker.RunTimeAction.SkipDay, step.Action);
        Assert.Equal(1, step.Skipped);
        Assert.Equal(Utc(2026, 10, 3, 2), step.NextDue);

        /* Exactly an hour after the slot is still inside the grace. */
        Assert.Equal(DarlingWorker.RunTimeAction.HandOff,
            DarlingWorker.StepRunTimeCollector(due, due.AddMinutes(60), DateTime.MinValue, Daily, UtcRule(), Jitter).Action);
    }

    [Fact]
    public void Skip_AfterASleepOrAStoppedService_CountsZero()
    {
        var due = Utc(2026, 10, 2, 2);
        var now = due.AddHours(6);
        var step = DarlingWorker.StepRunTimeCollector(due, now, now, Daily, UtcRule(), Jitter);
        Assert.Equal(DarlingWorker.RunTimeAction.SkipDay, step.Action);
        Assert.Equal(0, step.Skipped);
    }

    /* ---- the pass and the reload, on a worker with no store ---- */

    private static DarlingWorker MakeWorker(params ScheduleOverride[] overrides) =>
        new(
            NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            ScheduleOverridesForTest = overrides,
        };

    private static DarlingWorker.ServerLoopState MakeServer(int serverId)
    {
        var host = $"run-time-test-{serverId}";
        var config = new MonitoredServer { Name = host, Host = host };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{host},1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo(),
            StorageName = host,
            ServerId = serverId,
        };
        return new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime };
    }

    /* A run time about twelve hours from now: far from both edges of the grace, so a test that takes a second still lands
       on the same slot. */
    private static int RunTimeTwelveHoursAway() => (int)((DateTime.UtcNow.TimeOfDay.TotalMinutes + 12 * 60) % Daily);

    private static ScheduleOverride FleetRunTime(string collector, int? runAt, int? frequency = null) =>
        new(null, collector, frequency, null, true, null, runAt);

    [Fact]
    public async Task Pass_AHandOffPastTheGrace_RunsNothing_CountsOneSkippedSlot_AndMovesToTheNextSlot()
    {
        var runAt = RunTimeTwelveHoursAway();
        var worker = MakeWorker(FleetRunTime(Collector, runAt));
        var started = 0;
        worker.RunOneBodyOverride = (_, _, _) =>
        {
            Interlocked.Increment(ref started);
            return Task.FromResult(1);
        };
        var server = MakeServer(0);
        var now = DateTime.UtcNow;
        server.NextDue[Collector] = now.AddMinutes(-90);
        server.RunTimeSlots[Collector] = new DarlingWorker.RunTimeSlot(runAt, Daily, server.Clock.Id, null);

        await worker.RunDueCollectorsAsync(server, null!, TestContext.Current.CancellationToken);

        Assert.Equal(0, Volatile.Read(ref started));
        Assert.True(server.NextDue[Collector] > now.AddHours(10), "the stamp moves to the next slot, hours away");
        var snapshot = worker.FleetGateStatsForTest!.Snapshot();
        Assert.Equal(1, snapshot.Skipped);
        Assert.Equal(0, snapshot.Run);
    }

    [Fact]
    public async Task Pass_ADayLostWhileTheServiceWasDown_CountsZero()
    {
        var runAt = RunTimeTwelveHoursAway();
        var worker = MakeWorker(FleetRunTime(Collector, runAt));
        worker.SkipCreditFloorForTest.Resume(DateTime.UtcNow);
        var server = MakeServer(0);
        server.NextDue[Collector] = DateTime.UtcNow.AddMinutes(-90);
        server.RunTimeSlots[Collector] = new DarlingWorker.RunTimeSlot(runAt, Daily, server.Clock.Id, null);

        await worker.RunDueCollectorsAsync(server, null!, TestContext.Current.CancellationToken);

        Assert.Equal(0, worker.FleetGateStatsForTest!.Snapshot().Skipped);
    }

    [Fact]
    public async Task Pass_InsideTheGrace_HandsTheRunOff_AndAdvancesToTheNextDaysSlot()
    {
        var runAt = RunTimeTwelveHoursAway();
        var worker = MakeWorker(FleetRunTime(Collector, runAt));
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.RunOneBodyOverride = (_, _, _) =>
        {
            started.TrySetResult();
            return Task.FromResult(1);
        };
        var server = MakeServer(0);
        var now = DateTime.UtcNow;
        server.NextDue[Collector] = now.AddMinutes(-5);
        server.RunTimeSlots[Collector] = new DarlingWorker.RunTimeSlot(runAt, Daily, server.Clock.Id, null);

        await worker.RunDueCollectorsAsync(server, null!, TestContext.Current.CancellationToken);

        Assert.True(await Task.WhenAny(started.Task, Task.Delay(TimeSpan.FromSeconds(5))) == started.Task, "the run was handed off");
        Assert.True(server.NextDue[Collector] > now.AddHours(10), "the next stamp is a slot, not the stamp plus the interval");
        Assert.NotNull(server.RunTimeSlots[Collector].LastRunUtc);
    }

    [Fact]
    public async Task Pass_ANewClock_ComputesTheSlotAgainFromTheLastRun()
    {
        var runAt = RunTimeTwelveHoursAway();
        var worker = MakeWorker(FleetRunTime(Collector, runAt));
        var server = MakeServer(0);
        var lastRun = DateTime.UtcNow.AddHours(-1);
        server.NextDue[Collector] = DateTime.UtcNow.AddHours(5);
        server.RunTimeSlots[Collector] = new DarlingWorker.RunTimeSlot(runAt, Daily, "a clock that is no longer the server's", lastRun);

        await worker.RunDueCollectorsAsync(server, null!, TestContext.Current.CancellationToken);

        var expected = CollectorRunTime.NextDue(DateTime.UtcNow, lastRun, runAt, Daily, 0, CollectorRunTime.LocalIsUtc);
        Assert.Equal(expected, server.NextDue[Collector]);
        Assert.Equal(server.Clock.Id, server.RunTimeSlots[Collector].ClockId);
    }

    [Fact]
    public async Task Reload_AnEditToAnotherRow_KeepsARunTimeStamp()
    {
        var runAt = RunTimeTwelveHoursAway();
        var worker = MakeWorker(FleetRunTime(Collector, runAt), FleetRunTime("wait_stats", null, frequency: 2));
        var server = MakeServer(0);
        var stamp = DateTime.UtcNow.AddMinutes(25 * 60);
        server.NextDue[Collector] = stamp;
        server.RunTimeSlots[Collector] = new DarlingWorker.RunTimeSlot(runAt, Daily, server.Clock.Id, null);

        await worker.RecomputeNextDueAsync([server], TestContext.Current.CancellationToken);

        Assert.Equal(stamp, server.NextDue[Collector]);
    }

    [Fact]
    public async Task Reload_SettingARunTime_ComputesTheSlotAgain_AndClearingItReturnsToTheOldRule()
    {
        var runAt = RunTimeTwelveHoursAway();
        var worker = MakeWorker(FleetRunTime(Collector, runAt));
        var server = MakeServer(0);
        server.NextDue[Collector] = DateTime.UtcNow.AddMinutes(30);

        await worker.RecomputeNextDueAsync([server], TestContext.Current.CancellationToken);

        var slot = CollectorRunTime.NextDue(DateTime.UtcNow, null, runAt, Daily, 0, CollectorRunTime.LocalIsUtc);
        Assert.Equal(slot, server.NextDue[Collector]);
        Assert.Equal(runAt, server.RunTimeSlots[Collector].RunAtMinute);

        /* Changing it computes it again. */
        var otherRunAt = (runAt + 120) % Daily;
        worker.ScheduleOverridesForTest = [FleetRunTime(Collector, otherRunAt)];
        await worker.RecomputeNextDueAsync([server], TestContext.Current.CancellationToken);
        Assert.Equal(CollectorRunTime.NextDue(DateTime.UtcNow, null, otherRunAt, Daily, 0, CollectorRunTime.LocalIsUtc), server.NextDue[Collector]);

        /* Clearing it (a -1 on the fleet row) returns to the rule it had without one: the last run plus the interval. */
        var lastRun = DateTime.UtcNow.AddHours(-3);
        server.RunTimeSlots[Collector] = server.RunTimeSlots[Collector] with { LastRunUtc = lastRun };
        worker.ScheduleOverridesForTest = [FleetRunTime(Collector, -1)];
        await worker.RecomputeNextDueAsync([server], TestContext.Current.CancellationToken);
        Assert.Equal(lastRun.AddMinutes(Daily), server.NextDue[Collector]);
        Assert.False(server.RunTimeSlots.ContainsKey(Collector));
    }
}
