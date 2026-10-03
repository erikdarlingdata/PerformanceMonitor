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
using System.Runtime.CompilerServices;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4999 (part of #4938): get_collection_health judges a collector against the interval it is SCHEDULED at on the
/// server, not the one it shipped with. A collector moved to every 720 minutes in Lite's schedule store was banded
/// against its shipped five, and the sweep-pressure roll-up amortised its run cost by those five too.
///
/// <para>The schedule store's answer is <c>ScheduleManager.GetFrequencyForStorageServer</c> (the per-server
/// override, else the global schedule), run through <see cref="CollectorScheduleDefaults.ResolveFrequencyMinutes"/>
/// as the analysis lookback does. The tests hand
/// <see cref="LocalDataService.ApplyScheduledFrequencies"/>, the step the health read runs on every row it builds,
/// a resolver of their own, so no database is needed.</para>
/// </summary>
public sealed class CollectionHealthEffectiveCadenceTests
{
    private const string FiveMinute = "memory_clerks";
    private const int ServerId = 7;
    private const int OtherServerId = 8;

    private static CollectorHealthRow Row(string name, double avgMs = 0, double p95Ms = 0) =>
        new() { CollectorName = name, AvgDurationMs = avgMs, P95DurationMs = p95Ms };

    private static void Apply(CollectorHealthRow row, Func<int, string, int?>? resolver) =>
        LocalDataService.ApplyScheduledFrequencies(new[] { row }, ServerId, resolver);

    /// <summary>The schedule store with one collector on one server scheduled at <paramref name="minutes"/>.</summary>
    private static Func<int, string, int?> Schedule(int server, string collector, int minutes) =>
        (id, name) => id == server && name == collector ? minutes : null;

    private static CollectorHealthRow ProductiveRow(string name, double hoursSinceLastSuccess)
    {
        var last = DateTime.UtcNow.AddHours(-hoursSinceLastSuccess);
        return new CollectorHealthRow
        {
            CollectorName = name,
            TotalRuns = 10,
            SuccessCount = 10,
            RowsStored = 100,
            RunsWithRows = 10,
            LastSuccessTime = last,
            LastRunTime = last,
        };
    }

    [Fact]
    public void AnOverrideOf720OnAFiveMinuteCollector_IsJudgedAgainst720()
    {
        Assert.Equal(5, CollectorScheduleDefaults.All[FiveMinute].FrequencyMinutes);

        var row = Row(FiveMinute, avgMs: 30_000, p95Ms: 30_000);
        Assert.Equal(5, row.FrequencyMinutes);

        Apply(row, Schedule(ServerId, FiveMinute, 720));

        Assert.Equal(720, row.FrequencyMinutes);

        /* The tool's own expression: the roll-up amortises the run cost by the interval the row carries. */
        var rows = new[] { row };
        var pressure = SweepPressureClassifier.Compute(
            rows.Select(r => (r.CollectorName, r.AvgDurationMs, r.P95DurationMs, r.FrequencyMinutes)));
        Assert.Equal(30_000.0 / 720, pressure.BusyMsPerMinute, 3);
        Assert.Equal(720, pressure.PeakCollectorFrequencyMinutes);
    }

    [Fact]
    public void TheBandJudgesAgainstTheScheduledInterval_NotTheShippedOne()
    {
        /* Six hours since the newest success: past the four-hour floor a five-minute collector goes STALE at, well
           inside the 18 hours (one and a half intervals) a 720-minute collector gets. */
        var shipped = ProductiveRow(FiveMinute, hoursSinceLastSuccess: 6);
        Assert.Equal(CollectorHealthClassifier.Stale, shipped.HealthStatus);

        var scheduled = ProductiveRow(FiveMinute, hoursSinceLastSuccess: 6);
        Apply(scheduled, Schedule(ServerId, FiveMinute, 720));
        Assert.Equal(CollectorHealthClassifier.Healthy, scheduled.HealthStatus);
    }

    [Fact]
    public void AnotherServersScheduleIsIgnored_AndAnUnwiredStoreLeavesTheShippedCadence()
    {
        var elsewhere = Row(FiveMinute);
        Apply(elsewhere, Schedule(OtherServerId, FiveMinute, 720));
        Assert.Equal(5, elsewhere.FrequencyMinutes);

        var unwired = Row(FiveMinute);
        Apply(unwired, null);
        Assert.Null(unwired.EffectiveFrequencyMinutes);
        Assert.Equal(5, unwired.FrequencyMinutes);
    }

    [Fact]
    public void AnOnLoadCollectorReadsAsItsDailyRecapture_AndAnUnknownNameIsLeftAlone()
    {
        var onLoad = Row("server_config");
        Apply(onLoad, (_, _) => null);
        Assert.Equal(CollectorScheduleDefaults.OnLoadRecaptureMinutes, onLoad.FrequencyMinutes);

        var unknown = Row("not_a_collector");
        Apply(unknown, (_, _) => 720);
        Assert.Null(unknown.EffectiveFrequencyMinutes);
        Assert.Equal(0, unknown.FrequencyMinutes);
    }

    /// <summary>
    /// The host hands the health read the same schedule answer it hands the analysis tools, through the one
    /// storage-server resolution, and the read stamps its rows before returning them.
    /// </summary>
    [Fact]
    public void TheHostWiresTheScheduleStore_AndTheReadStampsItsRows()
    {
        var host = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHostService.cs"));
        Assert.Contains(
            "dataService.CollectorFrequencyMinutes = scheduleManager is null", host, StringComparison.Ordinal);
        Assert.Contains(
            "scheduleManager.GetFrequencyForStorageServer(serverManager, serverId, collector)", host, StringComparison.Ordinal);

        var reader = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Services", "LocalDataService.CollectionHealth.cs"));
        Assert.Contains("ApplyScheduledFrequencies(items, serverId, CollectorFrequencyMinutes);", reader, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
