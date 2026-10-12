/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5641: the Runnable Queue alert's DuckDB window read, <see cref="LocalDataService.GetRunnableQueueWindowAsync"/>
/// over <c>v_cpu_scheduler_stats</c>, run against a real DuckDB file. The twin of Darling's
/// <c>DarlingRunnableQueueReadLiveTests</c>: the same cases, so the two SKUs cannot drift.
///
/// <para>The read binds its floor as <c>DateTime.UtcNow.AddMinutes(-n)</c> (Kind = Utc), exactly as the poison-wait read
/// does, so a DuckDB that shifted that bound by the machine's local time zone would move the window by the offset. Rows
/// are therefore seeded with UTC times relative to the real clock, and every in-window row sits at least a minute
/// inside the 5-minute window and every out-of-window row at least two minutes outside it: a window shifted by hours
/// fails, and a slow test host cannot move a row across the bar.</para>
/// </summary>
public sealed class RunnableQueueWindowReadTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 5641;
    private const int OtherServerId = 5642;
    private const int WindowMinutes = RunnableQueueEvaluator.WindowMinutes;

    private readonly DuckDbInitializer _duckDb;

    public RunnableQueueWindowReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public void RunnableQueueWindowSql_IsTheNewestSampleCarryingTheWindowCountAndAverage()
    {
        var sql = LocalDataService.RunnableQueueWindowSql;

        Assert.Contains("FROM v_cpu_scheduler_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("avg_runnable_tasks_count IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) OVER ()", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(avg_runnable_tasks_count) OVER ()", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC, collection_id DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("node", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("offline", sql, StringComparison.OrdinalIgnoreCase);
        /* The floor is bound from the caller's clock; no engine-side now() to disagree with the evaluator. */
        Assert.DoesNotContain("now()", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("current_timestamp", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task EmptyWindow_ReturnsEmpty()
    {
        var window = await new LocalDataService(_duckDb).GetRunnableQueueWindowAsync(ServerId, WindowMinutes);

        Assert.Equal(RunnableQueueWindow.Empty, window);
    }

    [Fact]
    public async Task RowsOnlyBeforeTheFloor_ReturnEmpty()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-WindowMinutes - 2), 40.0m, 4, 160);
        await InsertAsync(2, now.AddMinutes(-WindowMinutes - 9), 50.0m, 4, 200);

        var window = await new LocalDataService(_duckDb).GetRunnableQueueWindowAsync(ServerId, WindowMinutes);

        Assert.Equal(RunnableQueueWindow.Empty, window);
    }

    /// <summary>
    /// The local-time-zone case. Under a 5-minute window a row 3 minutes old counts and a row 7 minutes old does not. If
    /// DuckDB shifted the Kind = Utc floor by this machine's local offset (this laptop is not on UTC), then on any
    /// machine west of UTC the floor would land hours in the future and the 3-minute row would vanish, and east of UTC it
    /// would land hours in the past and the 7-minute row would join.
    /// </summary>
    [Fact]
    public async Task AThreeMinuteOldRowCounts_AndASevenMinuteOldRowDoesNot_WhateverTheMachinesTimeZone()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-3), 6.0m, 4, 24);
        await InsertAsync(2, now.AddMinutes(-7), 600.0m, 4, 2400);

        var window = await new LocalDataService(_duckDb).GetRunnableQueueWindowAsync(ServerId, WindowMinutes);

        Assert.Equal(1L, window.SampleCount);
        Assert.Equal(6.0, window.AverageRunnableTasksPerScheduler, 6);
        Assert.Equal(24L, window.LatestTotalRunnableTasks);
        /* The newest sample's own time comes back as the UTC wall-clock it was stored with. */
        Assert.True(Math.Abs((window.NewestCollectionTime - now.AddMinutes(-3)).TotalSeconds) < 1,
            $"newest collection time {window.NewestCollectionTime:O} should be the stored UTC time {now.AddMinutes(-3):O}");
    }

    [Fact]
    public async Task AMixInsideAndOutsideTheWindow_CountsAndAveragesOnlyTheInWindowRows()
    {
        var now = DateTime.UtcNow;
        var newest = now.AddMinutes(-1);
        await InsertAsync(1, newest.AddMinutes(-2), 9.0m, 4, 36);
        await InsertAsync(2, newest.AddMinutes(-1), 4.0m, 4, 16);
        await InsertAsync(3, newest, 2.0m, 8, 16);
        await InsertAsync(4, now.AddMinutes(-WindowMinutes - 2), 500.0m, 4, 2000);
        await InsertAsync(5, now.AddMinutes(-WindowMinutes - 20), 700.0m, 4, 2800);

        var window = await new LocalDataService(_duckDb).GetRunnableQueueWindowAsync(ServerId, WindowMinutes);

        Assert.Equal(3L, window.SampleCount);
        Assert.Equal(5.0, window.AverageRunnableTasksPerScheduler, 6);
        Assert.Equal(8, window.LatestSchedulerCount);
        Assert.Equal(16L, window.LatestTotalRunnableTasks);
        Assert.Equal(newest.Ticks / 10, window.NewestCollectionTime.Ticks / 10);
    }

    [Fact]
    public async Task NullAverageRows_AreExcludedFromTheCountAndTheAverage_AndNeverCarryTheNewestSample()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-3), 6.0m, 4, 24);
        await InsertAsync(2, now.AddMinutes(-2), 12.0m, 4, 48);
        /* The NEWEST row has no average: not a sample, so not counted, not averaged, and not the row that rides along. */
        await InsertAsync(3, now.AddMinutes(-1), null, 16, 999);

        var window = await new LocalDataService(_duckDb).GetRunnableQueueWindowAsync(ServerId, WindowMinutes);

        Assert.Equal(2L, window.SampleCount);
        Assert.Equal(9.0, window.AverageRunnableTasksPerScheduler, 6);
        Assert.Equal(4, window.LatestSchedulerCount);
        Assert.Equal(48L, window.LatestTotalRunnableTasks);
        Assert.Equal(now.AddMinutes(-2).Ticks / 10, window.NewestCollectionTime.Ticks / 10);
    }

    [Fact]
    public async Task ASameInstantTie_IsBrokenByTheNewestCollectionId()
    {
        var now = DateTime.UtcNow;
        var instant = now.AddMinutes(-1);
        /* The higher collection_id goes in FIRST: DuckDB returns the first-inserted row of a tie when no tiebreak
           orders it (see CpuSchedulerSnapshotCollisionTests), so this ordering is the one that catches a missing one. */
        await InsertAsync(20, instant, 10.0m, 6, 60);
        await InsertAsync(10, instant, 2.0m, 2, 4);
        await InsertAsync(15, now.AddMinutes(-2), 3.0m, 3, 9);

        var window = await new LocalDataService(_duckDb).GetRunnableQueueWindowAsync(ServerId, WindowMinutes);

        Assert.Equal(3L, window.SampleCount);
        Assert.Equal(5.0, window.AverageRunnableTasksPerScheduler, 6);
        Assert.Equal(6, window.LatestSchedulerCount);
        Assert.Equal(60L, window.LatestTotalRunnableTasks);
        Assert.Equal(instant.Ticks / 10, window.NewestCollectionTime.Ticks / 10);
    }

    [Fact]
    public async Task AnotherServersRows_AreIgnored()
    {
        var now = DateTime.UtcNow;
        await InsertAsync(1, now.AddMinutes(-2), 4.0m, 4, 16);
        await InsertAsync(2, now.AddMinutes(-1), 400.0m, 32, 12800, OtherServerId);

        var service = new LocalDataService(_duckDb);
        var window = await service.GetRunnableQueueWindowAsync(ServerId, WindowMinutes);

        Assert.Equal(1L, window.SampleCount);
        Assert.Equal(4.0, window.AverageRunnableTasksPerScheduler, 6);
        Assert.Equal(4, window.LatestSchedulerCount);

        var other = await service.GetRunnableQueueWindowAsync(OtherServerId, WindowMinutes);
        Assert.Equal(1L, other.SampleCount);
        Assert.Equal(400.0, other.AverageRunnableTasksPerScheduler, 6);
    }

    [Fact]
    public async Task SubMillisecondTimesAtTheFloor_KeepMicrosecondPrecision()
    {
        /* The SQL text run directly with a floor the test chooses (the only way to put a row exactly on the floor):
           DuckDB stores microseconds, and a floor or a row truncated to milliseconds would move a row across the bar. */
        var floor = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc).AddTicks(1234560); // .123456 s
        var oneMicrosecond = TimeSpan.FromTicks(10);
        await InsertAsync(1, floor - oneMicrosecond, 100.0m, 4, 400);
        await InsertAsync(2, floor, 4.0m, 4, 16);
        await InsertAsync(3, floor + oneMicrosecond, 8.0m, 4, 32);

        var window = await ReadWithFloorAsync(floor);

        Assert.Equal(2L, window.SampleCount);
        Assert.Equal(6.0, window.AverageRunnableTasksPerScheduler, 6);
        Assert.Equal(32L, window.LatestTotalRunnableTasks);
        Assert.Equal((floor + oneMicrosecond).Ticks, window.NewestCollectionTime.Ticks);

        var later = await ReadWithFloorAsync(floor + oneMicrosecond);
        Assert.Equal(1L, later.SampleCount);
        Assert.Equal(8.0, later.AverageRunnableTasksPerScheduler, 6);
    }

    private async Task InsertAsync(
        long collectionId, DateTime collectionTime, decimal? average, int schedulerCount, int totalRunnable,
        int serverId = ServerId)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO cpu_scheduler_stats
    (collection_id, collection_time, server_id, server_name, scheduler_count, total_runnable_tasks_count, avg_runnable_tasks_count)
VALUES ($1, $2, $3, 'test-server', $4, $5, $6)";
        command.Parameters.Add(new DuckDBParameter { Value = collectionId });
        command.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = schedulerCount });
        command.Parameters.Add(new DuckDBParameter { Value = totalRunnable });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)average ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>Runs the read's SQL text with a floor the test chooses, mapped exactly as the read maps it.</summary>
    private async Task<RunnableQueueWindow> ReadWithFloorAsync(DateTime floor)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = LocalDataService.RunnableQueueWindowSql;
        command.Parameters.Add(new DuckDBParameter { Value = ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = floor });

        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        if (!await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            return RunnableQueueWindow.Empty;
        }

        return new RunnableQueueWindow(
            reader.GetInt64(0),
            reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
            reader.IsDBNull(2) ? null : reader.GetInt32(2),
            reader.IsDBNull(3) ? null : reader.GetInt64(3),
            reader.GetDateTime(4));
    }
}
