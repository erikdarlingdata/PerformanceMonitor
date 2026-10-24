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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6458: a first start on an empty data root must not log a warning about the <c>notification_routes</c> table. The cache
/// primed its first load when the loader was installed, before the store had created the table, so the first load failed and
/// logged. The loader is now installed after the store is initialized. A table that is missing after that is still logged.
/// In the <c>app-logger-statics</c> collection because the log buffer it drains is process-wide, and every class that drains it is in that collection.
/// </summary>
[Collection("app-logger-statics")]
public sealed class NotificationRouteFirstStartTests : IDisposable
{
    /* Only an upper bound on one in-process read of a freshly built file; the store is initialized before the wait, so it ends in
       milliseconds. Kept short because WriteLockBudgetTests sums the largest duration in every file that takes the write lock, and a
       30 s bound here was counted as a 30 s hold of it (#6517). */
    private static readonly TimeSpan FirstLoadWait = TimeSpan.FromSeconds(1);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;
    private readonly DuckDbNotificationRouteStore _store;

    public NotificationRouteFirstStartTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NotificationRouteFirstStartTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);

        /* An empty data root: the database file does not exist and nothing has initialized it. */
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _store = new DuckDbNotificationRouteStore(_initializer, () => new[] { 7101 }, new FakeRouteSecrets().Access);
    }

    public void Dispose()
    {
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static string[] RouteTableLines() =>
        AppLogger.DrainBufferedLines()
            .Where(l => l.Contains("notification_routes", StringComparison.OrdinalIgnoreCase))
            .ToArray();

    [Fact]
    public async Task AFirstStartOnAnEmptyDataRoot_LogsNothingAboutTheRoutesTable()
    {
        _ = AppLogger.DrainBufferedLines();
        var cache = new NotificationRouteCacheCore(() => false);

        await cache.InitializeStoreThenConfigureAsync(
            _initializer.InitializeAsync,
            () => Task.Run(() => _store.ListAsync()).GetAwaiter().GetResult());
        await cache.WaitForFirstLoadAsync(FirstLoadWait);

        Assert.Empty(cache.Current);
        Assert.Empty(RouteTableLines());
    }

    [Fact]
    public async Task ARoutesTableThatIsMissingAfterTheStart_IsStillLogged()
    {
        var cache = new NotificationRouteCacheCore(() => false);
        await cache.InitializeStoreThenConfigureAsync(
            _initializer.InitializeAsync,
            () => Task.Run(() => _store.ListAsync()).GetAwaiter().GetResult());
        await cache.WaitForFirstLoadAsync(FirstLoadWait);
        _ = AppLogger.DrainBufferedLines();

        using (_initializer.AcquireWriteLock())
        using (var connection = _initializer.CreateConnection())
        {
            await connection.OpenAsync();
            using var drop = connection.CreateCommand();
            drop.CommandText = "DROP TABLE notification_routes";
            await drop.ExecuteNonQueryAsync();
        }

        cache.Invalidate();
        while (cache.BackgroundReloadRunning)
        {
            await Task.Delay(20);
        }

        _ = cache.Current;

        Assert.Contains(RouteTableLines(), l => l.Contains("[WARN ]", StringComparison.Ordinal) && l.Contains("[NotificationRoutes]", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheLoader_IsNotCalled_UntilTheStoreInitializationHasFinished()
    {
        var initFinished = false;
        var loaderRanBeforeInit = false;
        var cache = new NotificationRouteCacheCore(() => false);

        await cache.InitializeStoreThenConfigureAsync(
            async () => { await Task.Delay(300); initFinished = true; },
            () => { loaderRanBeforeInit |= !initFinished; return Array.Empty<NotificationRoute>(); });
        await cache.WaitForFirstLoadAsync(FirstLoadWait);

        Assert.True(initFinished);
        Assert.False(loaderRanBeforeInit);
    }

    [Fact]
    public async Task AStoreThatFailsToInitialize_StillInstallsTheLoader_SoTheFaultIsLogged()
    {
        var loaderCalls = 0;
        var cache = new NotificationRouteCacheCore(() => false);

        await Assert.ThrowsAsync<InvalidOperationException>(() => cache.InitializeStoreThenConfigureAsync(
            () => throw new InvalidOperationException("init failed"),
            () => { Interlocked.Increment(ref loaderCalls); return Array.Empty<NotificationRoute>(); }));
        await cache.WaitForFirstLoadAsync(FirstLoadWait);

        Assert.Equal(1, Volatile.Read(ref loaderCalls));
    }
}
