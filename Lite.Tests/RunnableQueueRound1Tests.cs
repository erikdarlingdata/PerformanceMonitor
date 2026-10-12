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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Review round 1 of PR #5709 (issue #5641), Lite side, through the real DuckDB reads: the Runnable Queue alert at the
/// Low-Impact preset's 5-minute cadence (M1), the top requests read by CPU ahead of its cap (L1), and the summary text
/// the alert PR must not have changed (L3).
/// </summary>
[Trait("Reads", "Darling")]
public sealed class RunnableQueueRound1Tests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 5641_1;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public RunnableQueueRound1Tests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private async Task ExecAsync(string sql, params object?[] args)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var arg in args)
        {
            command.Parameters.Add(new DuckDBParameter { Value = arg ?? DBNull.Value });
        }

        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private Task InsertSchedulerRowAsync(DateTime collectionTime, decimal average) =>
        ExecAsync(@"
INSERT INTO cpu_scheduler_stats
    (collection_id, collection_time, server_id, server_name, scheduler_count, total_runnable_tasks_count, avg_runnable_tasks_count)
VALUES ($1, $2, $3, 'test-server', 4, $4, $5)",
            _nextId++, collectionTime, ServerId, (int)Math.Round(average * 4), average);

    /// <summary>
    /// The Low-Impact preset collects cpu_scheduler_stats every 5 minutes. The newest collection is 1 minute old, then 6,
    /// 11 and 16. The old 10-minute read held two of them (so the alert never evaluated); the 60-minute read holds all
    /// four, the cadence reads as 5 minutes, and the rule fires on the average of the newest three.
    /// </summary>
    [Fact]
    public async Task LowImpactCadence_FiresThroughTheRealRead_WhereTheTenMinuteReadSawTwoCollections()
    {
        var now = DateTime.UtcNow;
        foreach (var minutesAgo in new[] { 1, 6, 11, 16 })
        {
            await InsertSchedulerRowAsync(now.AddMinutes(-minutesAgo), 20.0m);
        }

        var service = new LocalDataService(_duckDb);

        var tenMinuteRead = await service.GetRunnableQueueSamplesAsync(ServerId, RunnableQueueEvaluator.FallbackMinutes);
        Assert.Equal(2, tenMinuteRead.Count);
        Assert.Null(RunnableQueueEvaluator.Read(tenMinuteRead, now));

        var samples = await service.GetRunnableQueueSamplesAsync(ServerId, RunnableQueueEvaluator.ReadLookbackMinutes);
        Assert.Equal(4, samples.Count);
        var reading = RunnableQueueEvaluator.Read(samples, now);

        Assert.NotNull(reading);
        Assert.Equal(3, reading!.SampleCount);
        var finding = RunnableQueueEvaluator.Evaluate(reading, 8, 16);
        Assert.Equal(PerformanceMonitor.Notifications.AlertSeverityLevel.Critical, finding!.Severity);
    }

    /// <summary>One-minute collection is unchanged: the same rows read through the 60-minute lookback give the same
    /// reading the 10-minute one did.</summary>
    [Fact]
    public async Task OneMinuteCadence_ReadsTheSameThroughTheLongerLookback()
    {
        var now = DateTime.UtcNow;
        for (var minutesAgo = 0; minutesAgo < 30; minutesAgo++)
        {
            await InsertSchedulerRowAsync(now.AddMinutes(-minutesAgo).AddSeconds(-5), minutesAgo < 5 ? 12.0m : 1.0m);
        }

        var service = new LocalDataService(_duckDb);
        var shortRead = RunnableQueueEvaluator.Read(await service.GetRunnableQueueSamplesAsync(ServerId, RunnableQueueEvaluator.FallbackMinutes), now);
        var longRead = RunnableQueueEvaluator.Read(await service.GetRunnableQueueSamplesAsync(ServerId, RunnableQueueEvaluator.ReadLookbackMinutes), now);

        Assert.NotNull(longRead);
        Assert.Equal(shortRead!.AveragePerScheduler, longRead!.AveragePerScheduler, 9);
        Assert.Equal(shortRead.SampleCount, longRead.SampleCount);
        Assert.Equal(10.0, RunnableQueueEvaluator.FallbackWindowMinutes(
            await service.GetRunnableQueueSamplesAsync(ServerId, RunnableQueueEvaluator.ReadLookbackMinutes)));
    }

    private Task InsertRequestAsync(DateTime collected, int session, long elapsedMs, long cpuMs) =>
        ExecAsync(@"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, total_elapsed_time_ms, cpu_time_ms, reads, writes, wait_type)
VALUES ($1, $2, $3, 'test-server', $4, 'Db', 'SELECT 1', 'running', $5, $6, 1, 0, 'LCK_M_X')",
            _nextId++, collected, ServerId, session, elapsedMs, cpuMs);

    /// <summary>
    /// L1: 60 blocked requests (long elapsed, almost no CPU) and 5 short CPU-heavy ones. The default read takes the 50
    /// longest-elapsed, all blocked; the CPU-ordered read takes the heavy five first, ahead of the cap.
    /// </summary>
    [Fact]
    public async Task TheCpuOrderedRead_TakesTheHeaviestRequestsAheadOfTheCap()
    {
        var collected = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-1), DateTimeKind.Unspecified);
        for (var i = 0; i < 60; i++)
        {
            await InsertRequestAsync(collected, 100 + i, elapsedMs: 600_000 + i, cpuMs: 1);
        }

        for (var i = 0; i < 5; i++)
        {
            await InsertRequestAsync(collected, 200 + i, elapsedMs: 2_000, cpuMs: 90_000 - (i * 1_000));
        }

        var service = new LocalDataService(_duckDb);
        var byElapsed = await service.GetLongRunningQueriesAsync(ServerId, 0, 50, false, false, false, false, false);
        var byCpu = await service.GetLongRunningQueriesAsync(ServerId, 0, 50, false, false, false, false, false, orderByCpu: true);

        Assert.Equal(50, byElapsed.Sessions.Count);
        Assert.DoesNotContain(byElapsed.Sessions, q => q.SessionId >= 200);
        Assert.Equal(50, byCpu.Sessions.Count);
        Assert.Equal(new[] { 200, 201, 202, 203, 204 }, byCpu.Sessions.Take(5).Select(q => q.SessionId).ToArray());
    }

    /// <summary>L1: a NULL CPU sorts last and ties fall to elapsed, then session id.</summary>
    [Fact]
    public async Task TheCpuOrderedRead_PutsNullCpuLast_AndBreaksTiesByElapsedThenSession()
    {
        var collected = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-1), DateTimeKind.Unspecified);
        await InsertRequestAsync(collected, 301, elapsedMs: 1_000, cpuMs: 500);
        await InsertRequestAsync(collected, 302, elapsedMs: 9_000, cpuMs: 500);
        await InsertRequestAsync(collected, 303, elapsedMs: 9_000, cpuMs: 500);
        await ExecAsync(@"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, total_elapsed_time_ms, cpu_time_ms)
VALUES ($1, $2, $3, 'test-server', 304, 'Db', 'SELECT 1', 'running', 99000, NULL)", _nextId++, collected, ServerId);

        var read = await new LocalDataService(_duckDb).GetLongRunningQueriesAsync(ServerId, 0, 10, false, false, false, false, false, orderByCpu: true);

        Assert.Equal(new[] { 302, 303, 301, 304 }, read.Sessions.Select(q => q.SessionId).ToArray());
    }

    /// <summary>L3: the Lite summary line keeps the space after the greater-than sign, as the Viewer's does.</summary>
    [Fact]
    public void TheLiteSummary_KeepsTheSpaceAfterTheGreaterThanSign()
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, "Lite", "Windows", "SettingsWindow.xaml.cs")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        var lite = File.ReadAllText(Path.Combine(dir!, "Lite", "Windows", "SettingsWindow.xaml.cs"));
        var viewer = File.ReadAllText(Path.Combine(dir!, "Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs"));

        Assert.Contains("parts.Add($\"queries > {AlertLongRunningQueryThresholdBox.Text}min\");", lite, StringComparison.Ordinal);
        Assert.Contains("parts.Add($\"queries > {AlertLongRunningQueryThresholdBox.Text}min\");", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("queries >{", lite, StringComparison.Ordinal);
    }
}
