/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4732: "Collection Falling Behind" counts only the collector slots that came due while the sweep loop was
/// running. A host that slept for an hour, a wall clock that stepped forward an hour and a service paused for an hour
/// all leave every due stamp an hour old, and the first run after any of them used to count about sixty skipped slots
/// per one-minute collector per server, none of them a gate that was too narrow. The rule is <see cref="SkipCreditFloor"/>;
/// these tests drive it with a fixed clock.
/// </summary>
public sealed class SkipCreditFloorTests
{
    private static readonly DateTime T0 = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan LoopTick = TimeSpan.FromSeconds(15);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>The sweep loop ticking every 15 seconds, from <paramref name="from"/> to <paramref name="to"/> inclusive.</summary>
    private static void TickEveryFifteenSeconds(SkipCreditFloor floor, DateTime from, DateTime to)
    {
        for (var at = from; at <= to; at += LoopTick)
        {
            floor.Tick(at);
        }
    }

    [Fact]
    public void AnHourBetweenTwoTicks_RecordsNoSkippedSlotsForTheFirstSlotAfterIt()
    {
        /* A host that slept for an hour and a wall clock that stepped forward an hour look the same to a loop that
           reads the wall clock: the next tick lands an hour after the last one. The collector's due time was stamped
           before the gap, so it is an hour old when the first body after the gap runs. */
        var floor = new SkipCreditFloor();
        var now = T0;
        var stats = new FleetGateStats(() => now);

        TickEveryFifteenSeconds(floor, T0, T0.AddMinutes(2));
        var due = T0.AddMinutes(3);

        var wake = T0.AddMinutes(2).AddHours(1);
        floor.Tick(wake);
        now = wake.AddSeconds(1);

        Assert.True(CollectorCadence.SkippedSlots(due, now, Minute) >= 59, "the unfloored count is what used to be recorded");
        stats.RecordSlot(floor.Skipped(due, now, Minute));

        var snapshot = stats.Snapshot();
        Assert.Equal(1, snapshot.Run);
        Assert.Equal(0, snapshot.Skipped);
    }

    [Fact]
    public void AnHourPausedThenResumed_RecordsNoSkippedSlotsForTheFirstSlotAfterResume()
    {
        var floor = new SkipCreditFloor();
        var now = T0;
        var stats = new FleetGateStats(() => now);

        /* The loop keeps ticking every 15 seconds through the pause and starts nothing, so no tick sees a gap. */
        var due = T0.AddMinutes(1);
        TickEveryFifteenSeconds(floor, T0, T0.AddHours(1));
        Assert.Equal(DateTime.MinValue, floor.Floor);

        var resumed = T0.AddHours(1).Add(LoopTick);
        floor.Tick(resumed);
        floor.Resume(resumed);
        now = resumed.AddSeconds(1);

        Assert.True(CollectorCadence.SkippedSlots(due, now, Minute) >= 59, "the unfloored count is what used to be recorded");
        stats.RecordSlot(floor.Skipped(due, now, Minute));

        var snapshot = stats.Snapshot();
        Assert.Equal(1, snapshot.Run);
        Assert.Equal(0, snapshot.Skipped);
    }

    [Fact]
    public void APauseOfUnderTwoMinutes_IsStillNotCounted()
    {
        var floor = new SkipCreditFloor();

        /* The ticks kept coming through the pause, so the gap rule never fires; the resume is what lifts the floor. */
        var due = T0.AddSeconds(30);
        TickEveryFifteenSeconds(floor, T0, T0.AddSeconds(105));
        var resumed = T0.AddSeconds(120);
        floor.Tick(resumed);
        floor.Resume(resumed);

        /* The stamp is 91 seconds old, one slot behind, and nothing ran in between. */
        var now = resumed.AddSeconds(1);
        Assert.Equal(1, CollectorCadence.SkippedSlots(due, now, Minute));
        Assert.Equal(0, floor.Skipped(due, now, Minute));
    }

    [Fact]
    public void ABodyFiveMinutesAfterItsPreviousRun_WhileTheLoopKeepsTicking_StillRecordsFourSkippedSlots()
    {
        /* The collector ran at T0 and was next due at T0 + 1 minute. Every gate slot is taken, so its body only starts
           at T0 + 5 minutes: it is four minutes past its due time, and the slots at +2, +3, +4 and +5 minutes came due and
           will never run. The loop ticked every 15 seconds the whole time, so nothing lifts the floor. */
        var floor = new SkipCreditFloor();
        var now = T0;
        var stats = new FleetGateStats(() => now);

        TickEveryFifteenSeconds(floor, T0, T0.AddMinutes(5));
        Assert.Equal(DateTime.MinValue, floor.Floor);

        var due = T0.AddMinutes(1);
        now = T0.AddMinutes(5);
        stats.RecordSlot(floor.Skipped(due, now, Minute));

        var snapshot = stats.Snapshot();
        Assert.Equal(1, snapshot.Run);
        Assert.Equal(4, snapshot.Skipped);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(59)]
    [InlineData(60)]
    [InlineData(119)]
    [InlineData(300)]
    [InlineData(600)]
    public void WhileTheLoopKeepsTicking_TheFloorChangesNoCount(int secondsLate)
    {
        var floor = new SkipCreditFloor();
        TickEveryFifteenSeconds(floor, T0, T0.AddSeconds(secondsLate));

        var now = T0.AddSeconds(secondsLate);
        Assert.Equal(CollectorCadence.SkippedSlots(T0, now, Minute), floor.Skipped(T0, now, Minute));
    }

    [Fact]
    public void ABackwardStep_DoesNotRaiseTheCount()
    {
        var floor = new SkipCreditFloor();

        /* One body ran four slots late before the step. */
        TickEveryFifteenSeconds(floor, T0, T0.AddMinutes(5));
        var now = T0.AddMinutes(5);
        var skipped = floor.Skipped(T0.AddMinutes(1), now, Minute);
        Assert.Equal(4, skipped);

        /* The clock steps back two hours. The next collector stamps were written on the old clock, so they are two hours
           ahead: the worker treats a stamp more than one interval ahead as due now (ClampDue), which skips nothing. */
        var stepped = now.AddHours(-2);
        floor.Tick(stepped);
        var stamp = CollectorCadence.ClampDue(T0.AddMinutes(6), stepped, Minute);
        Assert.Equal(stepped, stamp);
        skipped += floor.Skipped(stamp, stepped, Minute);

        /* A stamp less than an interval ahead is a normal wait: the worker does not run it, so it records nothing. */
        skipped += floor.Skipped(stepped.AddSeconds(30), stepped.AddSeconds(1), Minute);

        Assert.Equal(4, skipped);
    }

    [Fact]
    public void ABackwardStep_MovesTheFloorDownWithTheClock_SoALaterRealBacklogStillCounts()
    {
        var floor = new SkipCreditFloor();

        /* A sleep raised the floor, and then the clock stepped back three hours. A floor left ahead of the clock would
           hide every skip until the clock caught up to it. */
        floor.Tick(T0);
        floor.Tick(T0.AddHours(1));
        Assert.Equal(T0.AddHours(1), floor.Floor);

        var stepped = T0.AddHours(-2);
        floor.Tick(stepped);
        Assert.Equal(stepped, floor.Floor);

        TickEveryFifteenSeconds(floor, stepped.Add(LoopTick), stepped.AddMinutes(5));
        Assert.Equal(stepped, floor.Floor);
        Assert.Equal(4, floor.Skipped(stepped.AddMinutes(1), stepped.AddMinutes(5), Minute));
    }

    [Fact]
    public void AGapOfExactlyTwoMinutes_IsTheLoopRunning_AndOneSecondMoreIsNot()
    {
        var floor = new SkipCreditFloor();

        floor.Tick(T0);
        floor.Tick(T0 + SkipCreditFloor.MaxTickGap);
        Assert.Equal(DateTime.MinValue, floor.Floor);

        var late = T0 + SkipCreditFloor.MaxTickGap + SkipCreditFloor.MaxTickGap + TimeSpan.FromSeconds(1);
        floor.Tick(late);
        Assert.Equal(late, floor.Floor);
    }

    [Fact]
    public void TheFirstTick_HasNoGapToMeasure()
    {
        var floor = new SkipCreditFloor();
        floor.Tick(T0);
        Assert.Equal(DateTime.MinValue, floor.Floor);
    }

    [Fact]
    public void AFloorAtOrAfterNow_CreditsNothing_AndNeverGoesNegative()
    {
        var floor = new SkipCreditFloor();
        floor.Resume(T0.AddMinutes(10));

        Assert.Equal(0, floor.Skipped(T0, T0.AddMinutes(5), Minute));
        Assert.Equal(0, floor.Skipped(T0, T0.AddMinutes(10), Minute));
    }

    [Fact]
    public void AFleetAfterAnHourWithoutTicks_IsNotBehind_WhereTheOldCountPutItNearNinetyEightPercent()
    {
        /* 44 servers with 20 one-minute collectors each, the first slot of each after a one-hour gap. */
        var floor = new SkipCreditFloor();
        var now = T0;
        var counted = new FleetGateStats(() => now);
        var unfloored = new FleetGateStats(() => now);

        TickEveryFifteenSeconds(floor, T0, T0.AddMinutes(2));
        var due = T0.AddMinutes(3);
        var wake = T0.AddMinutes(2).AddHours(1);
        floor.Tick(wake);
        now = wake.AddSeconds(1);

        for (var slot = 0; slot < 44 * 20; slot++)
        {
            counted.RecordSlot(floor.Skipped(due, now, Minute));
            unfloored.RecordSlot(CollectorCadence.SkippedSlots(due, now, Minute));
        }

        var report = DarlingWorker.BuildFleetGateReport(counted.Snapshot(), 4, now);
        Assert.Equal(880, report.Run);
        Assert.Equal(0, report.Skipped);
        Assert.False(report.IsBehind);

        var old = DarlingWorker.BuildFleetGateReport(unfloored.Snapshot(), 4, now);
        Assert.True(old.IsBehind);
        Assert.True(old.SkippedPercent > 97, $"the unfloored share was {old.SkippedPercent:0.#}%");
    }

    [Fact]
    public async Task ReadersOnOtherThreads_OnlySeeFloorsTheLoopWrote_AndNeverOneThatMovesBackwards()
    {
        var floor = new SkipCreditFloor();
        using var stop = new CancellationTokenSource();
        using var started = new CountdownEvent(4);

        var readers = new Task[4];
        for (var i = 0; i < readers.Length; i++)
        {
            readers[i] = Task.Run(() =>
            {
                long last = 0;
                started.Signal();
                while (!stop.IsCancellationRequested)
                {
                    var ticks = floor.Floor.Ticks;
                    Assert.True(ticks >= last, "a floor read went backwards");
                    if (ticks != 0)
                    {
                        Assert.Equal(0, (ticks - T0.Ticks) % TimeSpan.TicksPerSecond);
                    }

                    _ = floor.Skipped(T0, T0.AddHours(1), Minute);
                    last = ticks;
                }
            });
        }

        Assert.True(started.Wait(TimeSpan.FromSeconds(30), Ct), "the readers did not start");
        for (var second = 1; second <= 20_000; second++)
        {
            floor.Resume(T0.AddSeconds(second));
        }

        await stop.CancelAsync();
        await Task.WhenAll(readers);
        Assert.Equal(T0.AddSeconds(20_000), floor.Floor);
    }

    [Fact]
    public void TheWorker_TicksTheFloorFirstInThePass_ResumesItAfterAPause_AndRecordsEverySlotThroughIt()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource().Replace("\r\n", "\n", StringComparison.Ordinal);

        /* The slot record goes through the floor, and no slot is recorded from the raw count. */
        Assert.Contains("_fleetGateStats?.RecordSlot(_skipCreditFloor.Skipped(due, now, intervalSpan));", source, StringComparison.Ordinal);
        Assert.DoesNotContain("RecordSlot(CollectorCadence.SkippedSlots(", source, StringComparison.Ordinal);

        /* The tick is the first thing in the sweep loop's pass, ahead of the reload and the pause gate. */
        const string tick = "_skipCreditFloor.Tick(DateTime.UtcNow);";
        Assert.Equal(1, CountOf(source, tick));
        var loop = source.IndexOf("_collectorState.PublishCollecting();", StringComparison.Ordinal);
        Assert.True(loop >= 0);
        var loopTop = source.IndexOf("while (!stoppingToken.IsCancellationRequested)", loop, StringComparison.Ordinal);
        var tickAt = source.IndexOf(tick, StringComparison.Ordinal);
        var reload = source.IndexOf("await configProvider.ReadConfigVersionAsync(stoppingToken);", loopTop, StringComparison.Ordinal);
        Assert.True(loopTop > loop && tickAt > loopTop && reload > tickAt, "the tick must come before the reload, at the top of the loop");
        var beforeTick = Regex.Replace(source[loopTop..tickAt], @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        Assert.Equal("while (!stoppingToken.IsCancellationRequested)\n        {", beforeTick.Trim());

        /* A pause marks the loop, and the first pass that runs collection again lifts the floor, before any body is launched. */
        const string resume = "_skipCreditFloor.Resume(DateTime.UtcNow);";
        Assert.Equal(1, CountOf(source, resume));
        var gate = source.IndexOf("if (!ShouldRunCollection(_paused))", reload, StringComparison.Ordinal);
        var paused = source.IndexOf("pausedSinceLastRun = true;", gate, StringComparison.Ordinal);
        var resumeAt = source.IndexOf(resume, StringComparison.Ordinal);
        var launch = source.IndexOf("sweepTargets = servers.ToArray();", gate, StringComparison.Ordinal);
        Assert.True(gate > reload && paused > gate && resumeAt > paused && launch > resumeAt, "the resume must sit between the pause gate and the launch of the bodies");
    }

    private static int CountOf(string text, string value)
    {
        var count = 0;
        for (var at = text.IndexOf(value, StringComparison.Ordinal); at >= 0; at = text.IndexOf(value, at + 1, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}

/// <summary>#4732: the "Collection Falling Behind" alert's standing state and its history values.</summary>
public sealed class FleetGateStandingAndValueTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DarlingSelfAlertEvaluator.FleetGateReport Report(long run, long skipped, DateTime end) =>
        new(run, skipped, QueueWaits: 12, QueueWaitTotal: TimeSpan.FromSeconds(6), QueueWaitMax: TimeSpan.FromSeconds(2), GateWidth: 4, WindowEndUtc: end);

    [Fact]
    public async Task WithTheMasterAlertsSwitchOff_TheAlertIsNoLongerReportedStanding()
    {
        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();

        Assert.True(await e.EvaluateFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct));

        /* Switched off after it fired: ApplyFleetGateAsync returns before it can resolve, so the flag stays set, and the
           worker's hourly line kept saying "falling behind" at 0 skipped. */
        h.Settings.AlertsEnabled = false;
        Assert.False(await e.EvaluateFleetGateAsync(Report(run: 2400, skipped: 0, h.Now), Ct));
        Assert.False(await e.EvaluateFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct));

        /* Switched back on, the condition is where it was left. */
        h.Settings.AlertsEnabled = true;
        Assert.True(await e.EvaluateFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct));
    }

    [Fact]
    public async Task WithTheMasterAlertsSwitchOff_AFleetThatWasNeverBehindIsNotStanding()
    {
        var h = new DarlingSelfAlertTests.Harness();
        h.Settings.AlertsEnabled = false;

        Assert.False(await h.Build().EvaluateFleetGateAsync(Report(run: 300, skipped: 100, h.Now), Ct));
        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task TheHistoryGrid_ShowsTheFleetGateValuesAsPercentages()
    {
        var h = new DarlingSelfAlertTests.Harness();

        /* 20 of 400 due slots is exactly the 5% that fires it. */
        await h.Build().ApplyFleetGateAsync(Report(run: 380, skipped: 20, h.Now), Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.FleetGateMetric, fired.MetricName);

        Assert.Equal("5.0%", AlertMetricClassifier.FormatHistoryValue(fired.MetricName, Assert.NotNull(fired.NumericCurrentValue)));
        Assert.Equal("5.0%", AlertMetricClassifier.FormatHistoryValue(fired.MetricName, DarlingSelfAlertEvaluator.FleetGateBehindPercent));
        Assert.Equal("97.3%", AlertMetricClassifier.FormatHistoryValue("Collection Falling Behind", 97.3));
    }
}
