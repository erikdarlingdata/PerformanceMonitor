/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
        var service = new LocalDataService(_duckDb, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
        if (displayName is not null)
        {
            service.DisplayNames = () => new Dictionary<int, string> { [ServerId] = displayName };
        }

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
            await service.GetDatabaseStateDeviationsAsync(ServerId);
        }
        finally
        {
            release.Set();
            holder.Join(TimeSpan.FromSeconds(10));
        }

        return Assert.Single(AppLogger.DrainBufferedLines(), l => l.Contains("skipped this cycle's database-state maintenance", StringComparison.Ordinal));
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
