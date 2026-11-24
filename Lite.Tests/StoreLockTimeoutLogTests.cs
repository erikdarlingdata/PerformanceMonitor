/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9196: what the log says when a background write gives up waiting for the store lock. A burst of reads holding the lock is a
/// routine cause, so the daily purges write the one Warning line the neighbouring passes write, with no stack and no Error, and
/// the database-state maintenance skip names the server by its display name instead of its storage id.
///
/// <para>Collection <c>app-logger-statics</c>: the maintenance assertions drain the process-wide log buffer.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class StoreLockTimeoutLogTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 91960;

    private readonly DuckDbInitializer _duckDb;

    public StoreLockTimeoutLogTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static StoreLockTimeoutException Timeout(string requester) =>
        new(
            TimeSpan.FromSeconds(5),
            new[] { new StoreLockHolder("LocalDataService.QueryStore.GetQueryStoreTopQueriesAsync", Write: false, Held: TimeSpan.FromSeconds(2.6)) },
            requester);

    [Fact]
    public async Task BothDailyPurgesThatTimeOutOnTheStoreLock_LogOneWarningEach_WithNoStackAndNoError()
    {
        var log = new CapturingLogger();

        await CollectionBackgroundService.RunDismissedAlertsPurgesAsync(
            () => throw Timeout("LocalDataService.PurgeOldDismissedArchiveAlertsAsync"),
            () => throw Timeout("LocalDataService.QueryTags.PurgeOldQueryTagSightingsAsync"),
            () => { },
            log);

        Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Error);
        Assert.Equal(2, log.Entries.Count);
        Assert.All(log.Entries, e =>
        {
            Assert.Equal(LogLevel.Warning, e.Level);
            Assert.Null(e.Exception);
            Assert.Contains("LocalDataService.QueryStore.GetQueryStoreTopQueriesAsync (read lock, held 2.6 s)", e.Message, StringComparison.Ordinal);
        });
        Assert.Contains(log.Entries, e => e.Message.StartsWith("Query-tag sightings cleanup", StringComparison.Ordinal));
        Assert.Contains(log.Entries, e => e.Message.StartsWith("Dismissed-archive-alert cleanup", StringComparison.Ordinal));
    }

    [Fact]
    public async Task APurgeThatFailsForAnyOtherReason_StillLogsAnErrorWithItsException()
    {
        var log = new CapturingLogger();

        await CollectionBackgroundService.RunDismissedAlertsPurgesAsync(
            () => Task.CompletedTask,
            () => throw new InvalidOperationException("boom"),
            () => { },
            log);

        var entry = Assert.Single(log.Entries);
        Assert.Equal(LogLevel.Error, entry.Level);
        Assert.IsType<InvalidOperationException>(entry.Exception);
        Assert.Contains("Query-tag sightings cleanup", entry.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheSkippedMaintenanceLine_NamesTheServerByItsDisplayName()
    {
        var line = await SkipMaintenanceAndReadTheLineAsync(displayName: "Display One");

        Assert.Contains($"server 'Display One': skipped this cycle's database-state maintenance", line, StringComparison.Ordinal);
        Assert.DoesNotContain(ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture), line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheSkippedMaintenanceLine_KeepsTheStorageId_ForAServerNobodyCanName()
    {
        var line = await SkipMaintenanceAndReadTheLineAsync(displayName: null);

        Assert.Contains($"server {ServerId}: skipped this cycle's database-state maintenance", line, StringComparison.Ordinal);
    }

    private async Task<string> SkipMaintenanceAndReadTheLineAsync(string? displayName)
    {
        var names = displayName is null ? null : new Dictionary<int, string> { [ServerId] = displayName };
        return Assert.Single(await SkipMaintenanceForAsync(names, ServerId));
    }

    /// <summary>#9233: three servers' sweeps meet the same held write lock, one after another as the alert engine runs them. Their one line names all three.</summary>
    [Fact]
    public async Task ASkippedCycle_WritesOneLine_NamingEveryServerItSkipped()
    {
        var names = new Dictionary<int, string> { [ServerId] = "Display One", [ServerId - 1] = "Display Two", [ServerId - 2] = "Display Three" };

        var lines = await SkipMaintenanceForAsync(names, ServerId, ServerId - 1, ServerId - 2);

        var line = Assert.Single(lines);
        Assert.Contains("'Display One'", line, StringComparison.Ordinal);
        Assert.Contains("'Display Two'", line, StringComparison.Ordinal);
        Assert.Contains("'Display Three'", line, StringComparison.Ordinal);
    }

    /// <summary>The skips of a cycle that has been written do not leak into the next cycle's line.</summary>
    [Fact]
    public async Task TheNextCycleOfSkips_WritesItsOwnLine_WithoutTheEarlierServers()
    {
        var service = new LocalDataService(_duckDb, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100))
        {
            MaintenanceSkipLogQuietPeriodOverride = TimeSpan.FromHours(1),
            DisplayNames = () => new Dictionary<int, string> { [ServerId] = "Display One", [ServerId - 1] = "Display Two" },
        };

        var first = await SkipUnderAHeldLockAsync(service, ServerId);
        var second = await SkipUnderAHeldLockAsync(service, ServerId - 1);

        Assert.Contains("'Display One'", Assert.Single(first), StringComparison.Ordinal);
        var later = Assert.Single(second);
        Assert.Contains("'Display Two'", later, StringComparison.Ordinal);
        Assert.DoesNotContain("'Display One'", later, StringComparison.Ordinal);
    }

    /// <summary>
    /// #9233: a stuck lock keeps skipping servers, each one less than a quiet period after the last. The window opens at the first
    /// skip and is not pushed back by the later ones, so the line still logs and each window's line names its own servers.
    /// </summary>
    [Fact]
    public async Task SkipsThatKeepArriving_StillLogWithinOneWindowOfTheFirst_EachLineNamingItsOwnServers()
    {
        var quiet = TimeSpan.FromSeconds(1);
        var serverIds = Enumerable.Range(0, 9).Select(i => ServerId - i).ToArray();
        var names = serverIds.ToDictionary(id => id, id => $"Display {ServerId - id}");
        var service = new LocalDataService(_duckDb, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100))
        {
            MaintenanceSkipLogQuietPeriodOverride = quiet,
            DisplayNames = () => names,
        };

        var held = new ManualResetEventSlim(false);
        var release = new ManualResetEventSlim(false);
        var holder = new Thread(() =>
        {
            using (_duckDb.AcquireReadLock())
            {
                held.Set();
                release.Wait(TimeSpan.FromSeconds(30));
            }
        })
        { IsBackground = true };
        holder.Start();

        var lines = new List<string>();
        int? firstLineAtSkip = null;
        try
        {
            Assert.True(held.Wait(TimeSpan.FromSeconds(10)), "the read never took the lock");
            AppLogger.DrainBufferedLines();
            var skipNumber = 0;
            foreach (var serverId in serverIds)
            {
                /* Each skip waits the 100 ms budget, then the pause puts the skips half a quiet period apart: four and a
                   half quiet periods in all, and no gap between skips as long as one quiet period. */
                await service.GetDatabaseStateDeviationsAsync(serverId);
                var seen = AppLogger.DrainBufferedLines().Where(l => l.Contains("skipped this cycle's database-state maintenance", StringComparison.Ordinal)).ToList();
                skipNumber++;
                if (seen.Count > 0 && firstLineAtSkip is null)
                {
                    firstLineAtSkip = skipNumber;
                }

                lines.AddRange(seen);
                await Task.Delay(quiet / 2 - TimeSpan.FromMilliseconds(100));
            }

            service.FlushMaintenanceSkipLog();
            lines.AddRange(AppLogger.DrainBufferedLines().Where(l => l.Contains("skipped this cycle's database-state maintenance", StringComparison.Ordinal)));
        }
        finally
        {
            release.Set();
            holder.Join(TimeSpan.FromSeconds(10));
        }

        /* With a window that slid, nothing logged until the final flush and the one line held all nine servers. */
        Assert.NotNull(firstLineAtSkip);
        Assert.True(firstLineAtSkip <= 5, $"the first line came after skip {firstLineAtSkip}, skips being half a window apart");
        Assert.True(lines.Count >= 3, $"{lines.Count} lines for a burst that ran several windows");
        foreach (var id in serverIds)
        {
            Assert.Single(lines, l => l.Contains($"'{names[id]}'", StringComparison.Ordinal));
        }
    }

    private async Task<List<string>> SkipMaintenanceForAsync(Dictionary<int, string>? displayNames, params int[] serverIds)
    {
        var service = new LocalDataService(_duckDb, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100))
        {
            MaintenanceSkipLogQuietPeriodOverride = TimeSpan.FromHours(1),
        };
        if (displayNames is not null)
        {
            service.DisplayNames = () => displayNames;
        }

        return await SkipUnderAHeldLockAsync(service, serverIds);
    }

    /// <summary>Sweeps each server under a read held on another thread, then writes the burst's line, and returns the maintenance-skip lines the log buffered.</summary>
    private async Task<List<string>> SkipUnderAHeldLockAsync(LocalDataService service, params int[] serverIds)
    {
        /* A read held on another thread: the maintenance wants the write lock, waits its 100 ms and skips the cycle. */
        var held = new ManualResetEventSlim(false);
        var release = new ManualResetEventSlim(false);
        var holder = new Thread(() =>
        {
            using (_duckDb.AcquireReadLock())
            {
                held.Set();
                release.Wait(TimeSpan.FromSeconds(10));
            }
        })
        { IsBackground = true };
        holder.Start();

        try
        {
            Assert.True(held.Wait(TimeSpan.FromSeconds(10)), "the read never took the lock");
            AppLogger.DrainBufferedLines();
            foreach (var serverId in serverIds)
            {
                await service.GetDatabaseStateDeviationsAsync(serverId);
            }

            service.FlushMaintenanceSkipLog();
        }
        finally
        {
            release.Set();
            holder.Join(TimeSpan.FromSeconds(10));
        }

        return AppLogger.DrainBufferedLines().Where(l => l.Contains("skipped this cycle's database-state maintenance", StringComparison.Ordinal)).ToList();
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception), exception));
    }
}
