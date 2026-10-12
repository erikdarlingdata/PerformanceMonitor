using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4728: after the computer sleeps and resumes, the collection loop's delay returns long after the slot it waited
/// for. Handing the collectors that stale slot ran every collector that was due then, and ran it again at the next
/// slot a minute later. The wait returns the latest grid slot at or before now instead, so each due collector runs
/// once. The simulation drives the production wait (<see cref="CollectionBackgroundService.WaitForNextCycleAsync"/>)
/// with a fake clock and a fake delay, and <see cref="ScheduleManager"/>'s mark-and-due for the collectors, the way
/// <c>RemoteCollectorService</c> marks a run with the cycle's slot. #4732: the same wait, driven across a wall clock
/// that steps backwards, does not pause the loop for as long as the step, and does not run a cycle twice in a row when the
/// step lands inside the delay or the delay returns a hair before its slot.
/// </summary>
public sealed class CollectionCycleResumeTests : IDisposable
{
    private static readonly DateTime T0 = new(2026, 9, 29, 10, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(1);
    private const string ServerId = "resume-server";
    private const string EveryMinute = "every_minute";
    private const string EveryFive = "every_five";
    private const string EveryFifteen = "every_fifteen";

    private readonly string _configDir;

    public CollectionCycleResumeTests()
    {
        _configDir = Path.Combine(Path.GetTempPath(), "CollectionCycleResumeTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_configDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_configDir)) Directory.Delete(_configDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private sealed record Run(string Collector, DateTime Slot, DateTime WallClock);

    /// <summary>
    /// Runs <paramref name="iterations"/> loop passes from slot T0. The delay after pass <paramref name="sleepAfterPass"/>
    /// comes back <paramref name="sleep"/> late, the way a resume from sleep does (-1 for no sleep). #4732: a negative
    /// <paramref name="sleep"/> is a wall clock that steps backwards while the loop is inside that wait. The wall clock
    /// also moves by <paramref name="step"/> while pass <paramref name="stepDuringPass"/> is finishing, before the loop waits
    /// (-1 for no step; a negative step is a clock that went backwards), and every delay the loop asks for is added to
    /// <paramref name="delays"/>.
    /// </summary>
    private async Task<List<Run>> SimulateAsync(int iterations, int sleepAfterPass, TimeSpan sleep,
        int stepDuringPass = -1, TimeSpan step = default, List<TimeSpan>? delays = null)
    {
        var schedule = new ScheduleManager(_configDir);
        schedule.SetScheduleForServer(ServerId, new List<CollectorSchedule>
        {
            new() { Name = EveryMinute, Enabled = true, FrequencyMinutes = 1 },
            new() { Name = EveryFive, Enabled = true, FrequencyMinutes = 5 },
            new() { Name = EveryFifteen, Enabled = true, FrequencyMinutes = 15 },
        });

        var now = T0;
        var cycleStart = T0;
        var runs = new List<Run>();
        for (var pass = 0; pass < iterations; pass++)
        {
            foreach (var due in schedule.GetDueCollectorsForServer(ServerId, cycleStart))
            {
                runs.Add(new Run(due.Name, cycleStart, now));
                schedule.MarkCollectorRunForServer(ServerId, due.Name, cycleStart);
            }

            if (pass == stepDuringPass)
                now += step;

            var thisPass = pass;
            cycleStart = await CollectionBackgroundService.WaitForNextCycleAsync(
                cycleStart,
                Interval,
                () => now,
                (wait, _) =>
                {
                    delays?.Add(wait);
                    now += wait;
                    if (thisPass == sleepAfterPass)
                        now += sleep;
                    return Task.CompletedTask;
                },
                CancellationToken.None);
        }

        return runs;
    }

    /// <summary>
    /// A 47-minute sleep begins while the loop waits for the slot at which the 5-minute collector becomes due. Each
    /// due collector runs once after the resume: the 5-minute one is not run again a minute later (the old shape ran
    /// it at the stale slot and again at the next one, 60 s apart), and the resume slot is on the grid.
    /// </summary>
    [Fact]
    public async Task AfterASleepAndResume_EachDueCollectorRunsOnce_NotTwiceAMinuteApart()
    {
        var sleep = TimeSpan.FromMinutes(47);
        var runs = await SimulateAsync(iterations: 12, sleepAfterPass: 4, sleep);

        var resumeWall = T0 + TimeSpan.FromMinutes(5) + sleep;
        var afterResume = runs.Where(r => r.WallClock >= resumeWall).ToList();

        /* The slot handed to the collectors is the latest grid slot at or before now, not the one from before the sleep. */
        Assert.All(afterResume, r => Assert.Equal(r.WallClock, r.Slot));

        var five = afterResume.Where(r => r.Collector == EveryFive).ToList();
        Assert.Equal(new[] { resumeWall, resumeWall + TimeSpan.FromMinutes(5) }, five.Select(r => r.WallClock).ToArray());

        var fifteen = afterResume.Where(r => r.Collector == EveryFifteen).ToList();
        Assert.Equal(new[] { resumeWall }, fifteen.Select(r => r.WallClock).ToArray());

        /* A 1-minute collector still runs at the resume slot and at the next one. */
        var minute = afterResume.Where(r => r.Collector == EveryMinute).Select(r => r.Slot).ToList();
        Assert.Contains(resumeWall, minute);
        Assert.Contains(resumeWall + Interval, minute);
    }

    /// <summary>Without a sleep the grid is unchanged: each collector runs at its own cadence, at exact slots.</summary>
    [Fact]
    public async Task WithoutASleep_EachCollectorRunsAtItsOwnCadenceOnTheGrid()
    {
        var runs = await SimulateAsync(iterations: 16, sleepAfterPass: -1, TimeSpan.Zero);

        Assert.Equal(16, runs.Count(r => r.Collector == EveryMinute));
        Assert.Equal(new[] { 0, 5, 10, 15 }, runs.Where(r => r.Collector == EveryFive).Select(r => (int)(r.Slot - T0).TotalMinutes).ToArray());
        Assert.Equal(new[] { 0, 15 }, runs.Where(r => r.Collector == EveryFifteen).Select(r => (int)(r.Slot - T0).TotalMinutes).ToArray());
        Assert.All(runs, r => Assert.Equal(r.WallClock, r.Slot));
    }

    /// <summary>
    /// #4732: the slot is 10 minutes ahead of a clock that stepped backwards. Waiting for it would pause the loop for
    /// the whole step, so no delay is asked for and the cycle starts at the clock's reading.
    /// </summary>
    [Fact]
    public async Task WaitForNextCycle_ClockStepsBackTenMinutes_AsksForNoDelayAndStartsTheCycleAtTheClock()
    {
        var clock = T0 - TimeSpan.FromMinutes(10);
        var delays = new List<TimeSpan>();

        var start = await CollectionBackgroundService.WaitForNextCycleAsync(
            T0,
            Interval,
            () => clock,
            (wait, _) => { delays.Add(wait); return Task.CompletedTask; },
            CancellationToken.None);

        Assert.All(delays, d => Assert.True(d <= TimeSpan.Zero, $"the loop asked to wait {d}"));
        Assert.Equal(clock, start);
    }

    /// <summary>
    /// #4732: the cycle after a backward step starts at the clock, so the grid is re-anchored there and the next wait
    /// is one interval. A cycle that kept the old slot would be ahead of the clock again and skip its wait too, one
    /// cycle after another, until the clock caught up with a grid that moves a minute per cycle.
    /// </summary>
    [Fact]
    public async Task WaitForNextCycle_AfterAClockStepBack_TheNextWaitIsOneInterval()
    {
        var clock = T0 - TimeSpan.FromMinutes(10);
        var delays = new List<TimeSpan>();
        Task Delay(TimeSpan wait, CancellationToken _) { delays.Add(wait); clock += wait; return Task.CompletedTask; }

        var first = await CollectionBackgroundService.WaitForNextCycleAsync(T0, Interval, () => clock, Delay, CancellationToken.None);
        var second = await CollectionBackgroundService.WaitForNextCycleAsync(first, Interval, () => clock, Delay, CancellationToken.None);

        Assert.Equal(new[] { Interval }, delays);
        Assert.Equal(T0 - TimeSpan.FromMinutes(10) + Interval, second);
    }

    /// <summary>
    /// #4732: a slot exactly one interval ahead is the ordinary wait, not a stepped clock: a loop that has just run its
    /// cycle at the clock's own reading waits the whole interval.
    /// </summary>
    [Fact]
    public async Task WaitForNextCycle_SlotOneIntervalAhead_WaitsTheWholeInterval()
    {
        var delays = new List<TimeSpan>();

        var start = await CollectionBackgroundService.WaitForNextCycleAsync(
            T0,
            Interval,
            () => T0,
            (wait, _) => { delays.Add(wait); return Task.CompletedTask; },
            CancellationToken.None);

        Assert.Equal(new[] { Interval }, delays);
        Assert.Equal(T0 + Interval, start);
    }

    /// <summary>
    /// #4732: the wall clock goes back 10 minutes while a cycle finishes. The old loop then waited 11 minutes for the next
    /// slot; now no wait runs longer than one interval. The cycle after the step runs at the clock's reading with every
    /// collector due (each one's last run is ahead of it), and the cadence goes on from the new clock a minute at a time.
    /// </summary>
    [Fact]
    public async Task AfterAClockStepBack_NoWaitIsLongerThanAnInterval_AndEveryCollectorRunsAtOnce()
    {
        var delays = new List<TimeSpan>();
        var step = TimeSpan.FromMinutes(-10);
        var runs = await SimulateAsync(iterations: 12, sleepAfterPass: -1, TimeSpan.Zero, stepDuringPass: 4, step, delays);

        Assert.NotEmpty(delays);
        Assert.All(delays, d => Assert.True(d <= Interval, $"the loop asked to wait {d}"));

        var afterStep = T0 + TimeSpan.FromMinutes(4) + step;
        var atStep = runs.Where(r => r.WallClock == afterStep).ToList();
        Assert.Equal(new[] { EveryFifteen, EveryFive, EveryMinute }, atStep.Select(r => r.Collector).OrderBy(c => c, StringComparer.Ordinal).ToArray());
        Assert.All(atStep, r => Assert.Equal(afterStep, r.Slot));

        /* Passes 0-4 ran before the step; passes 5-11 are the seven after it. */
        var minute = runs.Where(r => r.Collector == EveryMinute).Skip(5).Select(r => r.Slot).ToArray();
        Assert.Equal(Enumerable.Range(0, 7).Select(i => afterStep + TimeSpan.FromMinutes(i)).ToArray(), minute);
    }

    /// <summary>
    /// #4732: the wall clock goes back 10 minutes while the loop is inside a wait (the step lands in the delay, not between
    /// two passes). The wait was one interval, so the step does not lengthen it, and no wait after it runs longer than an
    /// interval either. The delay returns with the clock at the slot minus 10 minutes, and exactly one cycle starts there, at
    /// the clock's reading, with every collector due (each one's last run is ahead of it). The old loop stamped that cycle with
    /// the slot it waited for, 10 minutes ahead of the clock, and ran a second cycle straight after it. From the one cycle
    /// the cadence goes on a minute at a time from the new clock.
    /// </summary>
    [Fact]
    public async Task AfterAClockStepBackDuringTheWait_ExactlyOneCycleRuns_AndTheCadenceGoesOnFromTheClock()
    {
        var delays = new List<TimeSpan>();
        var sleep = TimeSpan.FromMinutes(-10);
        var runs = await SimulateAsync(iterations: 12, sleepAfterPass: 4, sleep, delays: delays);

        Assert.NotEmpty(delays);
        Assert.All(delays, d => Assert.True(d > TimeSpan.Zero && d <= Interval, $"the loop asked to wait {d}"));

        /* The wait after pass 4 returns with the clock at slot 5 minus 10 minutes: every collector runs there once. */
        var afterStep = T0 + TimeSpan.FromMinutes(5) + sleep;
        var atClock = runs.Where(r => r.WallClock == afterStep).ToList();
        Assert.Equal(new[] { EveryFifteen, EveryFive, EveryMinute }, atClock.Select(r => r.Collector).OrderBy(c => c, StringComparer.Ordinal).ToArray());
        Assert.All(atClock, r => Assert.Equal(afterStep, r.Slot));

        /* Passes 0-4 ran before the step; passes 5-11 are the seven after it, one per minute of the new clock, and
           each cycle's slot is the clock's own reading. */
        var afterTheStep = runs.Where(r => r.Collector == EveryMinute).Skip(5).ToList();
        Assert.All(afterTheStep, r => Assert.Equal(r.WallClock, r.Slot));
        Assert.Equal(Enumerable.Range(0, 7).Select(i => afterStep + TimeSpan.FromMinutes(i)).ToArray(), afterTheStep.Select(r => r.Slot).ToArray());
    }

    /// <summary>
    /// #4732: the clock goes back 10 minutes while the delay runs. The wait returns one cycle, stamped with the clock's reading
    /// and not with the slot it waited for, and the wait after that cycle is a whole interval. A cycle stamped with the slot
    /// (10 minutes ahead of the clock) made the next wait count the cycle after it as due now: a second cycle straight after
    /// the first.
    /// </summary>
    [Fact]
    public async Task WaitForNextCycle_ClockStepsBackTenMinutesDuringTheDelay_OneCycleRunsAndTheNextStartsOneIntervalLater()
    {
        var clock = T0;
        var delays = new List<TimeSpan>();
        var step = TimeSpan.FromMinutes(-10);
        Task Delay(TimeSpan wait, CancellationToken _) { delays.Add(wait); clock += wait + step; step = TimeSpan.Zero; return Task.CompletedTask; }

        var first = await CollectionBackgroundService.WaitForNextCycleAsync(T0, Interval, () => clock, Delay, CancellationToken.None);
        var clockWhenTheDelayReturned = clock;
        var second = await CollectionBackgroundService.WaitForNextCycleAsync(first, Interval, () => clock, Delay, CancellationToken.None);

        Assert.Equal(T0 + Interval - TimeSpan.FromMinutes(10), clockWhenTheDelayReturned);
        Assert.Equal(clockWhenTheDelayReturned, first);
        Assert.Equal(new[] { Interval, Interval }, delays);
        Assert.Equal(clockWhenTheDelayReturned + Interval, second);
    }

    /// <summary>
    /// #4732: the delay returns 50 ms before its slot (a timer can fire a hair early) and the cycle takes no time. The cycle keeps
    /// its grid slot and the next wait is a whole interval; the old loop saw a stamp ahead of the clock, counted the next cycle
    /// as due now and ran it straight after the first. The slots that follow stay exactly on the original grid, because a
    /// slot even a hair before the grid would skip the collector that is due on it.
    /// </summary>
    [Fact]
    public async Task WaitForNextCycle_DelayReturns50MsEarlyAndTheCycleTakesNoTime_NoSecondCycleAtOnceAndTheGridHolds()
    {
        var clock = T0;
        var delays = new List<TimeSpan>();
        var early = TimeSpan.FromMilliseconds(50);
        Task Delay(TimeSpan wait, CancellationToken _) { delays.Add(wait); clock += wait - early; early = TimeSpan.Zero; return Task.CompletedTask; }

        var first = await CollectionBackgroundService.WaitForNextCycleAsync(T0, Interval, () => clock, Delay, CancellationToken.None);
        var second = await CollectionBackgroundService.WaitForNextCycleAsync(first, Interval, () => clock, Delay, CancellationToken.None);
        var third = await CollectionBackgroundService.WaitForNextCycleAsync(second, Interval, () => clock, Delay, CancellationToken.None);

        Assert.Equal(new[] { Interval, Interval, Interval }, delays);
        Assert.Equal(new[] { T0 + Interval, T0 + Interval * 2, T0 + Interval * 3 }, new[] { first, second, third });
    }

    /// <summary>
    /// #4732: one delay returns before its slot, by a timer's hair (50 ms) or by a step of half an interval (30 s), and every
    /// cycle takes no time. No cycle runs straight after another: the wall clock of consecutive cycles is a whole interval
    /// apart (less the early return, once), every wait is one interval, and each collector runs on its own grid slots
    /// without skipping a beat.
    /// </summary>
    [Theory]
    [InlineData(50)]
    [InlineData(30_000)]
    public async Task AfterADelayReturnsBeforeItsSlot_NoCycleRunsStraightAfterAnother_AndEveryCollectorStaysOnItsGrid(int earlyMilliseconds)
    {
        var early = TimeSpan.FromMilliseconds(earlyMilliseconds);
        var delays = new List<TimeSpan>();
        var runs = await SimulateAsync(iterations: 16, sleepAfterPass: 2, -early, delays: delays);

        Assert.Equal(16, delays.Count);
        Assert.All(delays, d => Assert.Equal(Interval, d));

        var minute = runs.Where(r => r.Collector == EveryMinute).ToList();
        Assert.Equal(Enumerable.Range(0, 16).Select(i => T0 + TimeSpan.FromMinutes(i)).ToArray(), minute.Select(r => r.Slot).ToArray());
        Assert.Equal(new[] { 0, 5, 10, 15 }, runs.Where(r => r.Collector == EveryFive).Select(r => (int)(r.Slot - T0).TotalMinutes).ToArray());
        Assert.Equal(new[] { 0, 15 }, runs.Where(r => r.Collector == EveryFifteen).Select(r => (int)(r.Slot - T0).TotalMinutes).ToArray());

        var walls = minute.Select(r => r.WallClock).ToArray();
        Assert.All(walls.Zip(walls.Skip(1), (a, b) => b - a), gap => Assert.True(gap >= Interval - early, $"a cycle started {gap} after the last"));
    }
}
