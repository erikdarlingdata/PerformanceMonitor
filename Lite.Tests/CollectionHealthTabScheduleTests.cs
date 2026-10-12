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
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4999 (part of #4938): Lite's own Collection Health tab judges a collector against the interval it is scheduled at
/// on the server, as get_collection_health does. The tab builds its own <see cref="LocalDataService"/> (<c>ServerTab</c>
/// does <c>new LocalDataService(duckDb)</c>), and only the MCP host used to give its instance the schedule store's
/// answer, so the tab banded a collector moved to every 720 minutes against its shipped five: a collector last
/// successful six hours ago read STALE on the tab and HEALTHY in the MCP tool.
///
/// <para>Every instance reads the app-wide <see cref="LocalDataService.DefaultCollectorFrequencyMinutes"/> where it uses
/// it, so a service built the way the tab builds it, with no resolver of its own, bands by the schedule. These run
/// the tab's own read (<c>GetCollectionHealthAsync</c> on such a service) against a real DuckDB.</para>
///
/// <para>The default is process-wide, so the class that sets it is in a serial collection and puts it back.</para>
/// </summary>
[Collection(CollectorFrequencyDefaultCollection.Name)]
public sealed class CollectionHealthTabScheduleTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4999;
    private const int OverrideMinutes = 720;

    /// <summary>
    /// Six hours since the newest success: past the four-hour floor a five-minute collector goes STALE at, and well inside
    /// the 18 hours (one and a half intervals) a 720-minute collector gets.
    /// </summary>
    private const int MinutesSinceLastSuccess = 360;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionHealthTabScheduleTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        LocalDataService.DefaultCollectorFrequencyMinutes = null;
    }

    public void Dispose()
    {
        LocalDataService.DefaultCollectorFrequencyMinutes = null;
        _seedConn?.Dispose();
    }

    /// <summary>Five collectors that ship at five minutes, whose cadence an override of 720 minutes is honoured for.</summary>
    private static string[] FiveMinuteCollectors()
    {
        var names = CollectorScheduleDefaults.All
            .Where(c => c.Value.FrequencyMinutes == 5 && !CollectorDeltaCalculator.IsDeltaFamily(c.Key))
            .Select(c => c.Key)
            .OrderBy(n => n, StringComparer.Ordinal)
            .Take(5)
            .ToArray();
        Assert.Equal(5, names.Length);
        return names;
    }

    /// <summary>The schedule store as <c>ScheduleManager.GetFrequencyForStorageServer</c> answers it: the named collectors on this server, moved to 720 minutes.</summary>
    private static Func<int, string, int?> MovedTo720(params string[] collectors) =>
        (id, name) => id == ServerId && collectors.Contains(name) ? OverrideMinutes : null;

    [Fact]
    public async Task TheTabsOwnService_BandsAnOverrideOf720Against720_AndTheRestAgainstTheirShippedFive()
    {
        var collectors = FiveMinuteCollectors();
        var moved = collectors.Take(2).ToArray();
        foreach (var name in collectors)
        {
            await SeedProductiveRunsAsync(name);
        }

        LocalDataService.DefaultCollectorFrequencyMinutes = MovedTo720(moved);

        /* Exactly the tab's read: a service built with no resolver of its own, then GetCollectionHealthAsync. */
        var rows = await new LocalDataService(_duckDb).GetCollectionHealthAsync(ServerId);

        var healthy = rows.Where(r => r.HealthStatus == CollectorHealthClassifier.Healthy)
            .Select(r => r.CollectorName).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(moved.OrderBy(n => n, StringComparer.Ordinal).ToArray(), healthy);
        Assert.Equal(2, healthy.Length);

        foreach (var name in moved)
        {
            Assert.Equal(OverrideMinutes, rows.Single(r => r.CollectorName == name).FrequencyMinutes);
        }

        foreach (var name in collectors.Except(moved))
        {
            var row = rows.Single(r => r.CollectorName == name);
            Assert.Equal(5, row.FrequencyMinutes);
            Assert.Equal(CollectorHealthClassifier.Stale, row.HealthStatus);
        }
    }

    [Fact]
    public async Task WithNoSchedulesWired_EveryCollectorStaysOnItsShippedCadence()
    {
        var collectors = FiveMinuteCollectors();
        foreach (var name in collectors)
        {
            await SeedProductiveRunsAsync(name);
        }

        var rows = await new LocalDataService(_duckDb).GetCollectionHealthAsync(ServerId);

        Assert.All(collectors, name =>
        {
            var row = rows.Single(r => r.CollectorName == name);
            Assert.Equal(5, row.FrequencyMinutes);
            Assert.Equal(CollectorHealthClassifier.Stale, row.HealthStatus);
        });
    }

    /// <summary>
    /// The default is read where it is used, not copied when an instance is built, so a service that exists before the
    /// default is set sees it after, and an instance's own resolver (the MCP host sets one) still wins over it.
    /// </summary>
    [Fact]
    public void AnInstanceReadsTheAppWideDefaultWhenAsked_AndItsOwnResolverWins()
    {
        var service = new LocalDataService(_duckDb);
        Assert.Null(service.CollectorFrequencyMinutes);

        Func<int, string, int?> app = (_, _) => 720;
        LocalDataService.DefaultCollectorFrequencyMinutes = app;
        Assert.Same(app, service.CollectorFrequencyMinutes);
        Assert.Same(app, new LocalDataService(_duckDb).CollectorFrequencyMinutes);

        Func<int, string, int?> own = (_, _) => 60;
        service.CollectorFrequencyMinutes = own;
        Assert.Same(own, service.CollectorFrequencyMinutes);

        /* Setting an instance's own resolver to null (a host with no schedule manager) falls back to the default. */
        service.CollectorFrequencyMinutes = null;
        Assert.Same(app, service.CollectorFrequencyMinutes);
    }

    /// <summary>
    /// The main window wires the default once, from the schedule manager and server manager it builds, in the same answer the
    /// MCP host hands its own instance; and the tab builds its service with no resolver of its own, so the default is
    /// what it reads.
    /// </summary>
    [Fact]
    public void TheMainWindowWiresTheDefault_AndTheTabBuildsItsServiceWithNoResolverOfItsOwn()
    {
        var window = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "MainWindow.xaml.cs"));
        Assert.Contains("LocalDataService.DefaultCollectorFrequencyMinutes = (serverId, collector) =>", window, StringComparison.Ordinal);
        Assert.Contains("_scheduleManager.GetFrequencyForStorageServer(_serverManager, serverId, collector)", window, StringComparison.Ordinal);

        var tab = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Controls", "ServerTab.xaml.cs"));
        Assert.Contains("_dataService = new LocalDataService(duckDb);", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("CollectorFrequencyMinutes", tab, StringComparison.Ordinal);
    }

    /// <summary>Every test class that sets the process-wide default shares the one serial collection.</summary>
    [Fact]
    public void EveryClassSettingTheDefault_IsInTheSerialCollection()
    {
        var checkedFiles = 0;
        foreach (var file in Directory.GetFiles(Path.Combine(RepoRoot(), "Lite.Tests"), "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                || file.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            {
                continue;
            }

            var text = File.ReadAllText(file);
            if (!text.Contains("LocalDataService.DefaultCollectorFrequencyMinutes =", StringComparison.Ordinal))
            {
                continue;
            }

            checkedFiles++;
            Assert.True(
                text.Contains("[Collection(CollectorFrequencyDefaultCollection.Name)]", StringComparison.Ordinal),
                Path.GetFileName(file) + " sets the default outside the serial collection");
        }

        Assert.True(checkedFiles >= 1, "this file sets the default, so at least one file is checked");
    }

    /* ── helpers ── */

    private static DateTime MinutesAgo(int minutes)
    {
        var t = DateTime.UtcNow.AddMinutes(-minutes);
        return new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);
    }

    /// <summary>Ten successful runs that each stored rows, the newest <see cref="MinutesSinceLastSuccess"/> minutes ago.</summary>
    private async Task SeedProductiveRunsAsync(string collector)
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        for (var i = 0; i < 10; i++)
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var cmd = _seedConn.CreateCommand();
            cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
            cmd.Parameters.Add(new DuckDBParameter { Value = collector });
            cmd.Parameters.Add(new DuckDBParameter { Value = MinutesAgo(MinutesSinceLastSuccess + i * 5) });
            cmd.Parameters.Add(new DuckDBParameter { Value = 100 });
            cmd.Parameters.Add(new DuckDBParameter { Value = "SUCCESS" });
            cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
            cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
            cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
            await cmd.ExecuteNonQueryAsync();
        }
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

/// <summary>Every test class that sets <see cref="LocalDataService.DefaultCollectorFrequencyMinutes"/> runs alone, one after another.</summary>
[CollectionDefinition(CollectorFrequencyDefaultCollection.Name, DisableParallelization = true)]
public sealed class CollectorFrequencyDefaultCollection
{
    public const string Name = "CollectorFrequencyDefault";
}
