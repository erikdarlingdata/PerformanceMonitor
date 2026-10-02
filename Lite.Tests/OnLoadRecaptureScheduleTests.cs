/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3929/#3930: an on-load collector (trace_flags, server_config, database_config, database_scoped_config,
/// server_properties - FrequencyMinutes 0) used to be excluded from <see cref="ScheduleManager.GetDueCollectorsForServer"/>
/// forever - <c>!s.IsScheduled</c> was a permanent skip, so nothing ever re-ran it after the tab-open capture.
/// It now becomes due on <see cref="CollectorScheduleDefaults.OnLoadRecaptureMinutes"/> too, mirroring Darling's
/// worker, so a server tab left open for weeks still re-captures its config snapshot (#3930) and a trace flag
/// turned off since the last connect eventually clears (#3929) instead of only on the next reconnect. The
/// tab-open path itself (<see cref="PerformanceMonitorLite.Services.RemoteCollectorService.RunAllCollectorsForServerAsync"/>)
/// is untouched - it already runs every enabled collector unconditionally, on-load included.
/// </summary>
public sealed class OnLoadRecaptureScheduleTests : IDisposable
{
    private readonly string _configDir = Directory.CreateTempSubdirectory("pm-lite-onload-recapture-tests-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public void GetDueCollectorsForServer_OnLoadCollectorNeverRun_IsDueImmediately()
    {
        var manager = new ScheduleManager(_configDir);

        var due = manager.GetDueCollectorsForServer("srv1").Select(s => s.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

        Assert.Contains("trace_flags", due);
        Assert.Contains("server_config", due);
        Assert.Contains("database_config", due);
        Assert.Contains("database_scoped_config", due);
        Assert.Contains("server_properties", due);
    }

    [Fact]
    public void GetDueCollectorsForServer_OnLoadCollectorRunRecently_IsNotYetDue()
    {
        var manager = new ScheduleManager(_configDir);
        manager.MarkCollectorRunForServer("srv1", "trace_flags", DateTime.UtcNow.AddHours(-1));

        var due = manager.GetDueCollectorsForServer("srv1").Select(s => s.Name);

        Assert.DoesNotContain("trace_flags", due);
    }

    /// <summary>Pin 1 (Lite's half): the on-load set has a daily cadence, not "never again".</summary>
    [Fact]
    public void GetDueCollectorsForServer_OnLoadCollectorRunOverADayAgo_IsDueAgain()
    {
        var manager = new ScheduleManager(_configDir);
        manager.MarkCollectorRunForServer("srv1", "trace_flags",
            DateTime.UtcNow.AddMinutes(-(CollectorScheduleDefaults.OnLoadRecaptureMinutes + 1)));

        var due = manager.GetDueCollectorsForServer("srv1").Select(s => s.Name);

        Assert.Contains("trace_flags", due);
    }

    /// <summary>
    /// Pin 3 (Lite's half): a server whose tab has been open for 45 days without a reconnect still gets a
    /// fresh on-load capture roughly once a day - the newest snapshot never gets anywhere close to aging out
    /// of the 30-day retention the #3930 field defect relied on.
    /// </summary>
    [Fact]
    public void GetDueCollectorsForServer_SimulatedOverFortyFiveDays_NeverGapsPastRetention()
    {
        var manager = new ScheduleManager(_configDir);
        var retentionDays = CollectorScheduleDefaults.All["trace_flags"].RetentionDays;

        var now = DateTime.UtcNow.AddDays(-45);
        manager.MarkCollectorRunForServer("srv1", "trace_flags", now); // the initial on-connect capture

        var maxGapMinutes = 0.0;
        var lastRun = now;
        for (var day = 0; day < 45; day++)
        {
            now = now.AddMinutes(CollectorScheduleDefaults.OnLoadRecaptureMinutes);

            var due = manager.GetDueCollectorsForServer("srv1").Select(s => s.Name);
            Assert.Contains("trace_flags", due); // due at exactly one interval out, every time

            var gap = (now - lastRun).TotalMinutes;
            if (gap > maxGapMinutes) maxGapMinutes = gap;

            manager.MarkCollectorRunForServer("srv1", "trace_flags", now);
            lastRun = now;
        }

        Assert.Equal(CollectorScheduleDefaults.OnLoadRecaptureMinutes, maxGapMinutes);
        Assert.True(maxGapMinutes < retentionDays * 24 * 60,
            $"a {maxGapMinutes}-minute gap is far inside the {retentionDays}-day retention window");
    }

    /// <summary>An operator's explicit override still wins (the "ruling"): a non-zero override is scheduled by
    /// ITS OWN cadence, untouched by the on-load daily substitution, because that substitution only fires when
    /// the effective frequency is 0.</summary>
    [Fact]
    public void GetDueCollectorsForServer_OperatorOverride_StillWinsOverTheDailySubstitution()
    {
        var manager = new ScheduleManager(_configDir);
        var schedules = manager.GetSchedulesForServer("srv1").ToList();
        schedules.First(s => s.Name == "trace_flags").FrequencyMinutes = 10;
        manager.SetScheduleForServer("srv1", schedules);

        manager.MarkCollectorRunForServer("srv1", "trace_flags", DateTime.UtcNow.AddMinutes(-5));
        Assert.DoesNotContain("trace_flags", manager.GetDueCollectorsForServer("srv1").Select(s => s.Name));

        manager.MarkCollectorRunForServer("srv1", "trace_flags", DateTime.UtcNow.AddMinutes(-11));
        Assert.Contains("trace_flags", manager.GetDueCollectorsForServer("srv1").Select(s => s.Name));
    }

    /// <summary>A disabled on-load collector stays excluded - the substitution never makes a disabled
    /// collector due.</summary>
    [Fact]
    public void GetDueCollectorsForServer_DisabledOnLoadCollector_StaysExcluded()
    {
        var manager = new ScheduleManager(_configDir);
        var schedules = manager.GetSchedulesForServer("srv1").ToList();
        schedules.First(s => s.Name == "trace_flags").Enabled = false;
        manager.SetScheduleForServer("srv1", schedules);

        Assert.DoesNotContain("trace_flags", manager.GetDueCollectorsForServer("srv1").Select(s => s.Name));
    }
}
