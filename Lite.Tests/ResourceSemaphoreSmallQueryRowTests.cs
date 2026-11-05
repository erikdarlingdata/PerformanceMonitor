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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7676: the memory-grant collector now keeps the small-query semaphore (resource_semaphore_id 1) beside the regular one (id 0).
/// The per-semaphore reads show both rows; the per-pool reads still sum a pool's semaphores into one row, so the pool panels add up
/// to the two semaphore rows.
/// </summary>
[Collection("server-time-helper")]
public sealed class ResourceSemaphoreSmallQueryRowTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -7676301;
    private const string ServerName = "small-query-semaphore-lite-e2e";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _savedUtcOffsetMinutes;

    public ResourceSemaphoreSmallQueryRowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        _savedUtcOffsetMinutes = ServerTimeHelper.UtcOffsetMinutes;
        ServerTimeHelper.UtcOffsetMinutes = 0;
    }

    public void Dispose() => ServerTimeHelper.UtcOffsetMinutes = _savedUtcOffsetMinutes;

    [Fact]
    public async Task ASmallQuerySemaphoreRow_ShowsInThePerSemaphoreReads_AndThePerPoolReadsSumBoth()
    {
        var at = new DateTime(2026, 3, 10, 11, 5, 0);

        /* One pool, one collection: the regular semaphore (3 waiters, 100 MB granted) and the small-query semaphore (5 waiters,
           7 MB granted, no max target: the DMV returns NULL and the collector stores that NULL). */
        await SeedAsync(at, semaphoreId: 0, grantedMb: 100, availableMb: 50, waiters: 3, grantees: 2);
        await SeedAsync(at, semaphoreId: 1, grantedMb: 7, availableMb: 20, waiters: 5, grantees: 1);

        var from = at.AddMinutes(-5);
        var to = at.AddMinutes(5);

        var perSemaphore = await _dataService.GetResourceSemaphoreSnapshotAsync(ServerId, fromDate: from, toDate: to);
        Assert.Equal(new[] { 0, 1 }, perSemaphore.Select(r => r.ResourceSemaphoreId).ToArray());
        var perSemaphoreWindow = await _dataService.GetResourceSemaphoreWindowAsync(ServerId, fromDate: from, toDate: to);
        Assert.Equal(new int?[] { 0, 1 }, perSemaphoreWindow.Select(r => r.ResourceSemaphoreId).ToArray());

        var chart = Assert.Single(await _dataService.GetMemoryGrantChartDataAsync(ServerId, fromDate: from, toDate: to));
        Assert.Equal(107.0, chart.GrantedMemoryMb, precision: 3);
        Assert.Equal(70.0, chart.AvailableMemoryMb, precision: 3);
        Assert.Equal(8, chart.WaiterCount);
        Assert.Equal(3, chart.GranteeCount);

        var poolWindow = Assert.Single(await _dataService.GetMemoryGrantsWindowAsync(ServerId, fromDate: from, toDate: to));
        Assert.Equal(8L, poolWindow.PeakWaiterCount);
        Assert.Equal(107.0, poolWindow.PeakGrantedMemoryMb, precision: 3);
        Assert.Equal(70.0, poolWindow.MinAvailableMemoryMb, precision: 3);
    }

    /// <summary>#7676: the FinOps memory-grant efficiency is the regular semaphore's own figure. With the small-query row (7 MB granted,
    /// 5 waiters) beside it, the day still reports id 0 alone: 100 MB granted, 3 waiters. Deleting the read's id filter breaks this.</summary>
    [Fact]
    public async Task TheMemoryGrantEfficiencyRead_IgnoresTheSmallQuerySemaphoreRow()
    {
        var at = new DateTime(2026, 3, 10, 11, 5, 0);
        await SeedAsync(at, semaphoreId: 0, grantedMb: 100, availableMb: 50, waiters: 3, grantees: 2);
        await SeedAsync(at, semaphoreId: 1, grantedMb: 7, availableMb: 20, waiters: 5, grantees: 1);

        var day = Assert.Single(await _dataService.GetMemoryGrantEfficiencyAsync(ServerId, hoursBack: 24 * 800));

        Assert.Equal(100m, day.AvgGrantedMb);
        Assert.Equal(100m, day.PeakGrantedMb);
        Assert.Equal(3L, day.TotalWaiters);
        Assert.Equal(2L, day.TotalGrantees);
    }

    private async Task SeedAsync(DateTime at, int semaphoreId, double grantedMb, double availableMb, int waiters, int grantees)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO memory_grant_stats
            (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id,
             available_memory_mb, granted_memory_mb, used_memory_mb,
             grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, 1, $6, $7, 5, $8, $9, 0, 0, 60)";
        foreach (var v in new object[] { -7676000L - semaphoreId, at, ServerId, ServerName, semaphoreId, availableMb, grantedMb, grantees, waiters })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}
