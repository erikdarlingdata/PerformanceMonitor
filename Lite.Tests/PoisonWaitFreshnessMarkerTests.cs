/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9338: the Poison Wait read's <c>newest_collection_time</c> is the newest row that added wait. The collector keeps writing (0, 0) rows
/// after a spike, and the engine's same-evidence guard compares this time, so a marker that followed those rows re-fired one spike at every
/// cooldown. A spike then zero rows leaves the marker on the spike while the interval count still grows, and a later row with a new delta
/// moves it.
/// </summary>
public sealed class PoisonWaitFreshnessMarkerTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -938001;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public PoisonWaitFreshnessMarkerTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task NewestCollectionTime_IgnoresZeroRows_AndFollowsANewDelta()
    {
        var now = DateTime.UtcNow;
        var spike = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified).AddMinutes(-4);

        await InsertAsync(spike, tasks: 12, waitMs: 8_131_000);
        await InsertAsync(spike.AddMinutes(1), tasks: 0, waitMs: 0);
        await InsertAsync(spike.AddMinutes(2), tasks: 0, waitMs: 0);

        var afterZeroRows = Assert.Single(await _dataService.GetPoisonWaitAccumulationAsync(ServerId, 10));
        Assert.Equal(8_131_000L, afterZeroRows.AccumulatedWaitMs);
        Assert.Equal(3L, afterZeroRows.ObservedIntervals);
        Assert.Equal(spike, afterZeroRows.NewestCollectionTime);

        var newDelta = spike.AddMinutes(3);
        await InsertAsync(newDelta, tasks: 0, waitMs: 250);

        var afterNewDelta = Assert.Single(await _dataService.GetPoisonWaitAccumulationAsync(ServerId, 10));
        Assert.Equal(4L, afterNewDelta.ObservedIntervals);
        Assert.Equal(newDelta, afterNewDelta.NewestCollectionTime);
    }

    private async Task InsertAsync(DateTime at, long tasks, long waitMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
            VALUES ($1, $2, $3, 'TestServer', 'THREADPOOL', 0, 0, 0, $4, $5, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = tasks });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitMs });
        await cmd.ExecuteNonQueryAsync();
    }
}
