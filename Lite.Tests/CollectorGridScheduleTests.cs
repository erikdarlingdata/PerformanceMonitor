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
}
