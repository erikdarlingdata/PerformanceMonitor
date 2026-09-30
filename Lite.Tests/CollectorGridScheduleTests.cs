/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4640: a run is recorded at its logical cycle time, and the due check compares logical cycle times, so a
/// 1-minute collector is due on every 60-second grid cycle.
/// </summary>
public sealed class CollectorGridScheduleTests : IDisposable
{
    private readonly string _configDir = Directory.CreateTempSubdirectory("pm-lite-grid-schedule-tests-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public void GetDueCollectorsForServer_OneMinuteCollector_IsDueExactlyOneGridCycleLater()
    {
        var manager = new ScheduleManager(_configDir);
        var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        manager.MarkCollectorRunForServer("srv1", "wait_stats", t0);

        var atCycle = manager.GetDueCollectorsForServer("srv1", t0.AddSeconds(60)).Select(s => s.Name);
        var justBefore = manager.GetDueCollectorsForServer("srv1", t0.AddSeconds(59.9)).Select(s => s.Name);

        Assert.Contains("wait_stats", atCycle);
        Assert.DoesNotContain("wait_stats", justBefore);
    }

    /// <summary>
    /// #4732: the due check, for a 1-minute collector, against where its last run sits relative to the cycle time.
    /// A run recorded AFTER the cycle time is what a wall clock that stepped backwards leaves behind, and it is due
    /// now instead of waiting out the step; a run one interval before is due, and one under an interval before is not.
    /// </summary>
    [Theory]
    [InlineData(600, true)]
    [InlineData(1, true)]
    [InlineData(0, false)]
    [InlineData(-30, false)]
    [InlineData(-59, false)]
    [InlineData(-60, true)]
    [InlineData(-61, true)]
    public void GetDueCollectorsForServer_OneMinuteCollector_IsDueByWhereItsLastRunSitsAgainstTheCycleTime(
        int lastRunSecondsFromCycleTime, bool expectedDue)
    {
        var manager = new ScheduleManager(_configDir);
        var atUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        manager.MarkCollectorRunForServer("srv1", "wait_stats", atUtc.AddSeconds(lastRunSecondsFromCycleTime));

        var due = manager.GetDueCollectorsForServer("srv1", atUtc).Select(s => s.Name);

        Assert.Equal(expectedDue, due.Contains("wait_stats"));
    }

    /// <summary>
    /// #4732: the same table for a collector that runs every 5 minutes: a run ahead of the cycle time is due, even one
    /// that is ahead by less than the interval, a run four minutes before is not, and one five minutes before is.
    /// </summary>
    [Theory]
    [InlineData(600, true)]
    [InlineData(120, true)]
    [InlineData(-240, false)]
    [InlineData(-300, true)]
    public void GetDueCollectorsForServer_FiveMinuteCollector_IsDueByWhereItsLastRunSitsAgainstTheCycleTime(
        int lastRunSecondsFromCycleTime, bool expectedDue)
    {
        var manager = new ScheduleManager(_configDir);
        var schedules = manager.GetSchedulesForServer("srv1").ToList();
        schedules.First(s => s.Name == "wait_stats").FrequencyMinutes = 5;
        manager.SetScheduleForServer("srv1", schedules);
        var atUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        manager.MarkCollectorRunForServer("srv1", "wait_stats", atUtc.AddSeconds(lastRunSecondsFromCycleTime));

        var due = manager.GetDueCollectorsForServer("srv1", atUtc).Select(s => s.Name);

        Assert.Equal(expectedDue, due.Contains("wait_stats"));
    }
}
