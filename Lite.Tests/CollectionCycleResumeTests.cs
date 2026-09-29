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
/// <c>RemoteCollectorService</c> marks a run with the cycle's slot.
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
    /// comes back <paramref name="sleep"/> late, the way a resume from sleep does (-1 for no sleep).
    /// </summary>
    private async Task<List<Run>> SimulateAsync(int iterations, int sleepAfterPass, TimeSpan sleep)
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

            var thisPass = pass;
            cycleStart = await CollectionBackgroundService.WaitForNextCycleAsync(
                cycleStart,
                Interval,
                () => now,
                (wait, _) =>
                {
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
}
