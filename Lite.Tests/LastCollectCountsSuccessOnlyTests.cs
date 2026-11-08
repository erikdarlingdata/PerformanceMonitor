/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8304: the Overview card's Last Collect is the newest collection that reached the server. Every collector attempt writes a
/// collection_log row, a failed one included, so a server Lite cannot reach keeps writing ERROR rows every cycle; a Last Collect
/// read over every row moved forward on each failed attempt and its freshness band followed it, and an offline server read
/// "(stale)" at the time of a failed attempt while its last good collection was hours older. Only SUCCESS rows count, the same
/// rows <see cref="LocalDataService.GetLastSuccessfulCollectionAsync"/> counts for the server list.
/// </summary>
public sealed class LastCollectCountsSuccessOnlyTests : IDisposable
{
    private const int ServerId = 8304001;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private long _nextId = 1;

    public LastCollectCountsSuccessOnlyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteLastCollect_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Fact]
    public async Task AFailedAttemptAfterAnOlderSuccess_LeavesLastCollectAtTheSuccess_AndBandsFromIt()
    {
        var ct = TestContext.Current.CancellationToken;
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();

        /* Whole seconds: the store keeps microseconds, and a stamp with finer ticks would read back rounded. */
        var clock = DateTime.UtcNow;
        var now = new DateTime(clock.Ticks - clock.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc);
        /* The last good collection is past the stopped threshold; the failed attempts are from the last minute. */
        var lastSuccess = now.Add(-ServerHealthThresholds.OfflineThreshold).AddMinutes(-15);
        var failedAttempt = now.AddSeconds(-40);

        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(ct);
            await LogAsync(connection, "memory_stats", lastSuccess.AddMinutes(-1), "SUCCESS");
            await LogAsync(connection, "wait_stats", lastSuccess, "SUCCESS");
            await LogAsync(connection, "memory_stats", failedAttempt.AddSeconds(-5), "ERROR");
            await LogAsync(connection, "wait_stats", failedAttempt, "ERROR");
            await LogAsync(connection, "query_store", failedAttempt, "PERMISSIONS");
            await LogAsync(connection, "deadlocks", failedAttempt, "SKIPPED");
            await LogAsync(connection, "blocking", failedAttempt, "CANCELLED");
        }

        await initializer.CreateArchiveViewsAsync();
        var data = new LocalDataService(initializer);

        var summary = await data.GetServerSummaryAsync(ServerId, "dark", registeredAtUtc: null);
        Assert.NotNull(summary);
        Assert.Equal(lastSuccess, summary!.LastCollectionTime);
        Assert.Equal(ServerFreshness.Offline, summary.CollectionFreshness);
        Assert.EndsWith("(stopped)", summary.LastCollectionDisplay, StringComparison.Ordinal);

        /* The server list's own read of the same fact agrees with the card. */
        Assert.Equal(lastSuccess, await data.GetLastSuccessfulCollectionAsync(ServerId));
    }

    [Fact]
    public async Task AServerWhoseEveryAttemptFailed_HasNoLastCollect_AndReadsNever()
    {
        var ct = TestContext.Current.CancellationToken;
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(ct);
            await LogAsync(connection, "wait_stats", DateTime.UtcNow.AddSeconds(-30), "ERROR");
            await LogAsync(connection, "memory_stats", DateTime.UtcNow.AddSeconds(-20), "ERROR");
        }

        await initializer.CreateArchiveViewsAsync();
        var data = new LocalDataService(initializer);

        var summary = await data.GetServerSummaryAsync(ServerId, "dark", registeredAtUtc: null);
        Assert.NotNull(summary);
        Assert.Null(summary!.LastCollectionTime);
        Assert.Equal(ServerFreshness.NeverCollected, summary.CollectionFreshness);
        Assert.Equal("Never", summary.LastCollectionDisplay);
    }

    private async Task LogAsync(DuckDBConnection connection, string collector, DateTime at, string status)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, 'dark', $3, $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        await cmd.ExecuteNonQueryAsync();
    }
}
