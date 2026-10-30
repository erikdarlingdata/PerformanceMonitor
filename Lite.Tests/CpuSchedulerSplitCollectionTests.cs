/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5710: a collection written before the collector fix can be split across several cpu_scheduler_stats rows that
/// share one collection_time and each cover some of the schedulers (4 schedulers stored as 3 + 1, 1 + 2 + 1, even
/// 1 + 1 + 1 + 1 at idle; every row still says scheduler_count = 4). Lite's readers combine them: the runnable total
/// is the SUM, the per-scheduler average is that total over scheduler_count (a one-row collection keeps its stored
/// average), the two sum-based warning flags are recomputed from the combined counts, and the single-row DMV
/// columns take their MAX. Run against a real DuckDB file: the snapshot (behind the CPU Scheduler tab and the
/// get_cpu_scheduler_pressure MCP tool), the trend chart, and the analysis RUNNABLE_TASKS read. This replaces the
/// #3936 same-instant tiebreak test: two rows under one collection_time are now ONE collection, not two competing
/// snapshots (the stored collection_id is per row, so a split collection cannot be told from a collision).
/// </summary>
public sealed class CpuSchedulerSplitCollectionTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 42;
    private const string ServerName = "test-server";
    private readonly DuckDbInitializer _duckDb;

    public CpuSchedulerSplitCollectionTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private const string InsertSql = @"
INSERT INTO cpu_scheduler_stats
    (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count,
     total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count,
     total_active_request_count, total_queued_request_count, total_blocked_task_count,
     total_active_parallel_thread_count, runnable_request_count, total_request_count, runnable_percent,
     worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning, queued_requests_warning,
     total_physical_memory_kb, available_physical_memory_kb, system_memory_state_desc,
     physical_memory_pressure_warning, total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)";

    /// <summary>One piece of a collection: 4 schedulers, 4 CPUs, 100 workers ceiling; only the numbers a test varies.</summary>
    private async Task InsertPieceAsync(
        long collectionId, DateTime collectionTime, int runnable, decimal avg, int currentWorkers,
        int activeRequests, decimal runnablePercent, long availableMemoryKb,
        int schedulerCount = 4, int offlineCpus = 0, bool memoryWarning = false,
        int runnableRequests = 1, int? totalRequests = null, long totalMemoryKb = 65536000L,
        string memoryState = "Available physical memory is high")
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = InsertSql;
        object?[] values =
        {
            collectionId, collectionTime, ServerId, ServerName, 100, schedulerCount, 4,
            runnable, 0L, currentWorkers, avg,
            activeRequests, 0, 0,
            3L, runnableRequests, totalRequests ?? activeRequests, runnablePercent,
            false, false, false, false,
            totalMemoryKb, availableMemoryKb, memoryState,
            memoryWarning, 1, 1, offlineCpus, false
        };
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Snapshot_OneRowCollection_KeepsItsStoredAverage()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);
        await InsertPieceAsync(9001L, time, runnable: 21, avg: 5.25m, currentWorkers: 60, activeRequests: 7, runnablePercent: 12.5m, availableMemoryKb: 32768000L);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(21, snapshot!.TotalRunnableTasksCount);
        Assert.Equal(5.25, snapshot.AvgRunnableTasksCount, 2);
        Assert.Equal(60, snapshot.TotalCurrentWorkersCount);
    }

    [Fact]
    public async Task Snapshot_SplitCollection_SumsTheRunnableTasks_AndDividesByTheSchedulerCount()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);

        /* A 3 + 1 split of 4 schedulers: 83 + 18 runnable tasks, 50 + 45 workers. The higher collection_id is
           inserted FIRST, and each piece alone looks like a smaller, different server (27.67 and 18.00 per
           scheduler). The pieces' request and memory columns differ, as the old collector's re-read per scheduler
           row made them; the read takes the request triple of the piece with the most requests and the memory group of the piece with the least available memory. The stored flags on every piece are FALSE (test data), so a TRUE flag
           proves the read recomputed it from the sums: 101 runnable >= 4 CPUs and 95 workers >= 90% of 100. */
        await InsertPieceAsync(9102L, time, runnable: 18, avg: 18.00m, currentWorkers: 45, activeRequests: 9, runnablePercent: 20.0m, availableMemoryKb: 30000000L);
        await InsertPieceAsync(9101L, time, runnable: 83, avg: 27.67m, currentWorkers: 50, activeRequests: 8, runnablePercent: 10.0m, availableMemoryKb: 31000000L);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(4, snapshot!.SchedulerCount);
        Assert.Equal(101, snapshot.TotalRunnableTasksCount);
        Assert.Equal(25.25, snapshot.AvgRunnableTasksCount, 2);
        Assert.Equal(95, snapshot.TotalCurrentWorkersCount);
        Assert.Equal(100, snapshot.MaxWorkersCount);
        Assert.Equal(9, snapshot.TotalActiveRequestCount);
        Assert.Equal(20.0, snapshot.RunnablePercent!.Value, 2);
        Assert.Equal(30000000L, snapshot.AvailablePhysicalMemoryKb);
        Assert.True(snapshot.RunnableTasksWarning);
        Assert.True(snapshot.WorkerThreadExhaustionWarning);
    }

    /// <summary>
    /// #5712 review: the memory group is ONE stored row, the one with the LEAST available memory, and the pressure flag is
    /// recomputed from that row's pair by the collector's own rule (available below 10 % of the 65,536,000 KB total =
    /// 6,553,600 KB).
    /// </summary>
    [Fact]
    public async Task Snapshot_SplitCollection_MemoryPressureFlag_FollowsTheLeastAvailableRow()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);

        /* One piece stored TRUE at 6,000,000 KB, the other FALSE at 20,000,000 KB: the read shows the 6,000,000 KB row (9.2 %),
           so the flag is TRUE. */
        await InsertPieceAsync(9301L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 6000000L, memoryWarning: true, memoryState: "Available physical memory is low");
        await InsertPieceAsync(9302L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 20000000L);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(6000000L, snapshot!.AvailablePhysicalMemoryKb);
        Assert.True(snapshot.PhysicalMemoryPressureWarning);
    }

    /// <summary>
    /// The 10 % rule does not hold while the engine says available memory is high (a buffer pool that uses the RAM): a split
    /// collection whose shown row is under 10 % but reports the high state is not under pressure, and one that reports any other
    /// state still is.
    /// </summary>
    [Fact]
    public async Task Snapshot_SplitCollection_MemoryPressureFlag_IsFalseWhileTheEngineSaysAvailableMemoryIsHigh()
    {
        var high = DateTime.UtcNow.AddMinutes(-2);
        await InsertPieceAsync(9321L, high, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 4000000L, memoryWarning: true, memoryState: "Available physical memory is high");
        await InsertPieceAsync(9322L, high, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 20000000L);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(4000000L, snapshot!.AvailablePhysicalMemoryKb);
        Assert.False(snapshot.PhysicalMemoryPressureWarning);
    }

    [Fact]
    public async Task Snapshot_SplitCollection_MemoryPressureFlag_IsFalseWhenTheLeastAvailableIsNotUnderTenPercent()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);

        /* Both pieces stored TRUE (test data), the least available value is 7,000,000 KB >= 6,553,600 KB: FALSE. */
        await InsertPieceAsync(9311L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 7000000L, memoryWarning: true);
        await InsertPieceAsync(9312L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 9000000L, memoryWarning: true);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(7000000L, snapshot!.AvailablePhysicalMemoryKb);
        Assert.False(snapshot.PhysicalMemoryPressureWarning);
    }

    /// <summary>
    /// #5712 review: the request triple is one reading of the collector's dm_exec_requests piece, so a split collection takes
    /// all three columns from ONE row: the one with the most requests, ties to the most runnable requests, then the newest
    /// collection_id. Independent MAX() would print a percent that contradicts the two counts beside it.
    /// </summary>
    [Fact]
    public async Task Snapshot_SplitCollection_RequestTriple_ComesFromTheRowWithTheMostRequests()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);

        /* Piece A: 4 runnable of 8 requests, 50 %. Piece B: 3 runnable of 9, 33.33 %. MAX() would print 4 of 9 at 50 %. One row
           is B's triple. The higher collection_id (A) goes in first so insertion order cannot decide it. */
        await InsertPieceAsync(9341L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 8, runnablePercent: 50.00m, availableMemoryKb: 30000000L, runnableRequests: 4);
        await InsertPieceAsync(9340L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 9, runnablePercent: 33.33m, availableMemoryKb: 30000000L, runnableRequests: 3);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(3, snapshot!.RunnableRequestCount);
        Assert.Equal(9, snapshot.TotalRequestCount);
        Assert.Equal(33.33, snapshot.RunnablePercent!.Value, 2);
    }

    [Fact]
    public async Task Snapshot_SplitCollection_RequestTriple_TiesGoToTheMostRunnableRequests()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);

        /* Equal totals (10). Piece A: 5 runnable, 70 %. Piece B: 6 runnable, 60 %. One row is B's (6, 10, 60.00); MAX() would
           print (6, 10, 70.00). */
        await InsertPieceAsync(9351L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 10, runnablePercent: 70.00m, availableMemoryKb: 30000000L, runnableRequests: 5);
        await InsertPieceAsync(9350L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 10, runnablePercent: 60.00m, availableMemoryKb: 30000000L, runnableRequests: 6);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(6, snapshot!.RunnableRequestCount);
        Assert.Equal(10, snapshot.TotalRequestCount);
        Assert.Equal(60.00, snapshot.RunnablePercent!.Value, 2);
    }

    /// <summary>
    /// #5712 review: the memory group (total, available, state text) is one row. Piece A has the larger total and the higher
    /// available value, piece B the smaller total and the lower one: MAX() would pair A's total with A's available and could
    /// never print B's state text; one row is B's whole group.
    /// </summary>
    [Fact]
    public async Task Snapshot_SplitCollection_MemoryGroup_ComesFromTheLeastAvailableRow()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);
        await InsertPieceAsync(9361L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 32768000L, totalMemoryKb: 65536000L, memoryState: "Available physical memory is high");
        await InsertPieceAsync(9360L, time, runnable: 1, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 4000000L, totalMemoryKb: 60000000L, memoryState: "Available physical memory is low");

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(60000000L, snapshot!.TotalPhysicalMemoryKb);
        Assert.Equal(4000000L, snapshot.AvailablePhysicalMemoryKb);
        Assert.Equal("Available physical memory is low", snapshot.SystemMemoryStateDesc);
        Assert.True(snapshot.PhysicalMemoryPressureWarning);
    }

    /// <summary>
    /// #5712 round 1 (L5): 1 runnable task over 8 schedulers is exactly 0.125. The average is rounded to 2 places in the
    /// SQL, so DuckDB reads 0.13 (half away from zero), the same as PostgreSQL's numeric.
    /// </summary>
    [Fact]
    public async Task Snapshot_SplitCollection_AverageAtAHalfCent_RoundsHalfAwayFromZero()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);
        await InsertPieceAsync(9321L, time, runnable: 1, avg: 0.50m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 30000000L, schedulerCount: 8);
        await InsertPieceAsync(9322L, time, runnable: 0, avg: 0.00m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 30000000L, schedulerCount: 8);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(0.13m, (decimal)snapshot!.AvgRunnableTasksCount);
    }

    /// <summary>
    /// The split average divides by <c>scheduler_count</c>, which IS the online count (measured on SQL Server 2019 with
    /// AFFINITY CPU = 0 on 4 CPUs: scheduler_count 1, VISIBLE OFFLINE 3). Nothing is subtracted for offline_cpu_count:
    /// 6 runnable tasks over 8 is 0.75 even with 4 offline CPUs stored.
    /// </summary>
    [Fact]
    public async Task Snapshot_SplitCollection_AverageDividesBySchedulerCount_NotLessTheOfflineCount()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);
        await InsertPieceAsync(9331L, time, runnable: 4, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 30000000L, schedulerCount: 8, offlineCpus: 4);
        await InsertPieceAsync(9332L, time, runnable: 2, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 30000000L, schedulerCount: 8, offlineCpus: 4);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(0.75m, (decimal)snapshot!.AvgRunnableTasksCount);
    }

    /// <summary>
    /// The measured affinity case: scheduler_count 1 and offline_cpu_count 3 (4 CPUs, AFFINITY CPU = 0). The earlier divisor
    /// (scheduler_count less offline_cpu_count, floored at 0) was NULL here. 5 runnable tasks over 1 scheduler is 5.00.
    /// </summary>
    [Fact]
    public async Task Snapshot_SplitCollection_OneSchedulerWithThreeOfflineCpus_HasANonNullAverage()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);
        await InsertPieceAsync(9337L, time, runnable: 3, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 30000000L, schedulerCount: 1, offlineCpus: 3);
        await InsertPieceAsync(9338L, time, runnable: 2, avg: 1m, currentWorkers: 10, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 30000000L, schedulerCount: 1, offlineCpus: 3);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(5.00m, (decimal)snapshot!.AvgRunnableTasksCount);
    }

    [Fact]
    public async Task Snapshot_AllZeroSplit_IsZeroNotNull()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);
        for (var i = 0; i < 4; i++)
        {
            await InsertPieceAsync(9200L + i, time, runnable: 0, avg: 0.00m, currentWorkers: 10, activeRequests: 0, runnablePercent: 0m, availableMemoryKb: 60000000L);
        }

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(0, snapshot!.TotalRunnableTasksCount);
        Assert.Equal(0.0, snapshot.AvgRunnableTasksCount, 2);
        Assert.Equal(40, snapshot.TotalCurrentWorkersCount);
        Assert.False(snapshot.RunnableTasksWarning);
    }

    [Fact]
    public async Task Snapshot_ReadsTheNewestCollection_WholeNotItsLatestPiece()
    {
        var older = DateTime.UtcNow.AddMinutes(-20);
        var newer = DateTime.UtcNow.AddMinutes(-2);
        await InsertPieceAsync(9301L, older, runnable: 500, avg: 125.00m, currentWorkers: 90, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 30000000L);
        await InsertPieceAsync(9303L, newer, runnable: 2, avg: 2.00m, currentWorkers: 20, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 30000000L);
        await InsertPieceAsync(9302L, newer, runnable: 3, avg: 3.00m, currentWorkers: 20, activeRequests: 1, runnablePercent: 1m, availableMemoryKb: 30000000L);

        var snapshot = await new LocalDataService(_duckDb).GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(5, snapshot!.TotalRunnableTasksCount);
        Assert.Equal(1.25, snapshot.AvgRunnableTasksCount, 2);
    }

    [Fact]
    public async Task Trend_SplitCollection_PlotsOnePointWithTheSummedRunnableTasks()
    {
        var start = DateTime.UtcNow.AddMinutes(-30);
        var oneRow = start.AddMinutes(5);
        var split = start.AddMinutes(15);
        await InsertPieceAsync(9401L, oneRow, runnable: 2, avg: 2.00m, currentWorkers: 20, activeRequests: 0, runnablePercent: 0m, availableMemoryKb: 30000000L);
        await InsertPieceAsync(9403L, split, runnable: 2, avg: 2.00m, currentWorkers: 20, activeRequests: 0, runnablePercent: 0m, availableMemoryKb: 30000000L);
        await InsertPieceAsync(9402L, split, runnable: 3, avg: 3.00m, currentWorkers: 20, activeRequests: 0, runnablePercent: 0m, availableMemoryKb: 30000000L);

        var trend = await new LocalDataService(_duckDb).GetCpuSchedulerTrendAsync(
            ServerId, fromDate: start, toDate: DateTime.UtcNow);

        Assert.Equal(2, trend.Count);
        Assert.Equal(new[] { 2, 5 }, trend.Select(p => p.RunnableTasks));
    }

    [Fact]
    public async Task RunnableTaskFact_ReadsTheCombinedNewestCollection()
    {
        var time = DateTime.UtcNow.AddMinutes(-2);
        await InsertPieceAsync(9502L, time, runnable: 2, avg: 2.00m, currentWorkers: 20, activeRequests: 0, runnablePercent: 0m, availableMemoryKb: 30000000L);
        await InsertPieceAsync(9501L, time, runnable: 3, avg: 3.00m, currentWorkers: 20, activeRequests: 0, runnablePercent: 0m, availableMemoryKb: 30000000L);

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = DuckDbFactCollector.RunnableTaskStatsSql;
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });

        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Equal(5.0, Convert.ToDouble(reader.GetValue(0)));
        /* 2 and 3 are each below the 4 CPUs; the whole server's 5 is not. */
        Assert.True(reader.GetBoolean(1));
        Assert.False(await reader.ReadAsync(TestContext.Current.CancellationToken));
    }
}
