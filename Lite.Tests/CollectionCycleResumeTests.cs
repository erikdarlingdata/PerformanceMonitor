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
/// that steps backwards, does not pause the loop for as long as the step.
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
    /// comes back <paramref name="sleep"/> late, the way a resume from sleep does (-1 for no sleep). #4732: the wall clock
    /// moves by <paramref name="step"/> while pass <paramref name="stepDuringPass"/> is finishing, before the loop waits
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
}
