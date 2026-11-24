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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A row that stored no sample interval takes it from the row before it, so a read that rates such rows reads the row stamped
/// on its window start as that predecessor and returns only the rows collected in (start, end]. Every test stores three
/// collections an hour apart: one on the window start that stored its interval and a delta rated at 2 per second, and one
/// inside the window and one on its end that stored no interval and add 3600 each, 1 per second over the hour before them.
/// The whole window rates both hours at 1 per second, each half rates its own hour, and the start row's 2 per second is in none.
/// </summary>
public sealed class ReadTimeDifferencedBaselineTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4410;
    private const string ServerName = "BaselineSrv";
    private const string LockWait = "LCK_M_X";

    private static readonly DateTime Start = new(2026, 3, 2, 10, 0, 0);
    private static readonly DateTime Middle = Start.AddHours(1);
    private static readonly DateTime End = Start.AddHours(2);

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private DuckDBConnection? _conn;
    private long _id = 1;

    public ReadTimeDifferencedBaselineTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose() => _conn?.Dispose();

    [Fact]
    public async Task TheWaitTypeTrend_RatesTheFirstRowAfterTheStart_AgainstTheRowOnIt()
    {
        await SeedWaitsAsync();

        AssertEachHourIsRatedOnce(
            (await _service.GetWaitStatsTrendAsync(ServerId, LockWait, fromDate: Start, toDate: End)).Select(p => p.WaitTimeMsPerSecond),
            (await _service.GetWaitStatsTrendAsync(ServerId, LockWait, fromDate: Start, toDate: Middle)).Select(p => p.WaitTimeMsPerSecond),
            (await _service.GetWaitStatsTrendAsync(ServerId, LockWait, fromDate: Middle, toDate: End)).Select(p => p.WaitTimeMsPerSecond));
    }

    [Fact]
    public async Task TheWaitTrendsByType_RateTheFirstRowAfterTheStart_AgainstTheRowOnIt()
    {
        await SeedWaitsAsync();

        async Task<IEnumerable<double>> ReadAsync(DateTime from, DateTime to) =>
            (await _service.GetWaitStatsTrendsByTypesAsync(ServerId, [LockWait], fromDate: from, toDate: to))
            .SelectMany(series => series.Value).Select(p => p.WaitTimeMsPerSecond);

        AssertEachHourIsRatedOnce(await ReadAsync(Start, End), await ReadAsync(Start, Middle), await ReadAsync(Middle, End));
    }

    [Fact]
    public async Task TheTotalWaitTrend_RatesTheFirstCollectionAfterTheStart_AgainstTheCollectionOnIt()
    {
        await SeedWaitsAsync();

        AssertEachHourIsRatedOnce(
            (await _service.GetTotalWaitTrendAsync(ServerId, fromDate: Start, toDate: End)).Select(p => p.WaitTimeMsPerSecond),
            (await _service.GetTotalWaitTrendAsync(ServerId, fromDate: Start, toDate: Middle)).Select(p => p.WaitTimeMsPerSecond),
            (await _service.GetTotalWaitTrendAsync(ServerId, fromDate: Middle, toDate: End)).Select(p => p.WaitTimeMsPerSecond));
    }

    [Fact]
    public async Task TheLockWaitTrend_RatesTheFirstRowAfterTheStart_AgainstTheRowOnIt()
    {
        await SeedWaitsAsync();

        AssertEachHourIsRatedOnce(
            (await _service.GetLockWaitTrendAsync(ServerId, fromDate: Start, toDate: End)).Select(p => p.WaitTimeMsPerSecond),
            (await _service.GetLockWaitTrendAsync(ServerId, fromDate: Start, toDate: Middle)).Select(p => p.WaitTimeMsPerSecond),
            (await _service.GetLockWaitTrendAsync(ServerId, fromDate: Middle, toDate: End)).Select(p => p.WaitTimeMsPerSecond));
    }

    /// <summary>The bucketed wait read at a five minute width: each rated hour is one bucket, so the whole window has two and each half one.</summary>
    [Fact]
    public async Task TheWaitBuckets_RateTheFirstRowAfterTheStart_AgainstTheRowOnIt()
    {
        await SeedWaitsAsync();

        Assert.Equal(2, (await _service.GetWaitBucketsAsync(ServerId, LockWait, hoursBack: 2, asOfUtc: End, bucketMinutes: 5)).Count);
        Assert.Single(await _service.GetWaitBucketsAsync(ServerId, LockWait, hoursBack: 1, asOfUtc: Middle, bucketMinutes: 5));
        Assert.Single(await _service.GetWaitBucketsAsync(ServerId, LockWait, hoursBack: 1, asOfUtc: End, bucketMinutes: 5));
    }

    /// <summary>The lock wait types a window rated: the hour that ends inside the window is rated against the row on the start, so the
    /// first half lists the type as the whole window and the second half do.</summary>
    [Fact]
    public async Task TheLockWaitTypes_RateTheFirstRowAfterTheStart_AgainstTheRowOnIt()
    {
        await SeedWaitsAsync();

        Assert.Single(await _service.GetLockWaitTypesAsync(ServerId, hoursBack: 2, asOfUtc: End));
        Assert.Single(await _service.GetLockWaitTypesAsync(ServerId, hoursBack: 1, asOfUtc: Middle));
        Assert.Single(await _service.GetLockWaitTypesAsync(ServerId, hoursBack: 1, asOfUtc: End));
    }

    [Fact]
    public async Task TheFileThroughputTrend_RatesTheFirstRowAfterTheStart_AgainstTheRowOnIt()
    {
        const long megabyte = 1048576;
        foreach (var (at, megabytes, interval) in Collections())
        {
            await ExecuteAsync(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'AppDb', 'AppDb.mdf', 'ROWS', '', 100, 1, 0, $5, 0, 0, 0, $6)",
                _id++, at, ServerId, ServerName, megabytes * megabyte, interval);
        }

        AssertEachHourIsRatedOnce(
            (await _service.GetFileIoThroughputTrendAsync(ServerId, fromDate: Start, toDate: End)).Select(p => p.ReadMbPerSec),
            (await _service.GetFileIoThroughputTrendAsync(ServerId, fromDate: Start, toDate: Middle)).Select(p => p.ReadMbPerSec),
            (await _service.GetFileIoThroughputTrendAsync(ServerId, fromDate: Middle, toDate: End)).Select(p => p.ReadMbPerSec));
    }

    [Fact]
    public async Task TheLatchTrend_RatesTheFirstRowAfterTheStart_AgainstTheRowOnIt()
    {
        foreach (var (at, delta, interval) in Collections())
        {
            await ExecuteAsync(@"
INSERT INTO latch_stats
    (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms,
     delta_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'BUFFER', 1, 1, 1, $5, $6)",
                _id++, at, ServerId, ServerName, delta, interval);
        }

        AssertEachHourIsRatedOnce(
            (await _service.GetLatchStatsTrendAsync(ServerId, fromDate: Start, toDate: End)).Select(p => p.WaitTimeMsPerSecond),
            (await _service.GetLatchStatsTrendAsync(ServerId, fromDate: Start, toDate: Middle)).Select(p => p.WaitTimeMsPerSecond),
            (await _service.GetLatchStatsTrendAsync(ServerId, fromDate: Middle, toDate: End)).Select(p => p.WaitTimeMsPerSecond));
    }

    [Fact]
    public async Task TheSpinlockTrend_RatesTheFirstRowAfterTheStart_AgainstTheRowOnIt()
    {
        foreach (var (at, delta, interval) in Collections())
        {
            await ExecuteAsync(@"
INSERT INTO spinlock_stats
    (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs,
     delta_collisions, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'LOCK_HASH', 1, 1, 1, 0, 0, $5, $6)",
                _id++, at, ServerId, ServerName, delta, interval);
        }

        AssertEachHourIsRatedOnce(
            (await _service.GetSpinlockStatsTrendAsync(ServerId, fromDate: Start, toDate: End)).Select(p => p.CollisionsPerSecond),
            (await _service.GetSpinlockStatsTrendAsync(ServerId, fromDate: Start, toDate: Middle)).Select(p => p.CollisionsPerSecond),
            (await _service.GetSpinlockStatsTrendAsync(ServerId, fromDate: Middle, toDate: End)).Select(p => p.CollisionsPerSecond));
    }

    /// <summary>The procedure history lists each collection with its interval: the two rows that stored none take the hour before them,
    /// the first of them from the row on the window start, and that row is not listed.</summary>
    [Fact]
    public async Task TheProcedureHistory_GivesTheFirstRowAfterTheStartItsInterval_FromTheRowOnIt()
    {
        foreach (var (at, delta, interval) in Collections())
        {
            await ExecuteAsync(@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
     execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_physical_reads, total_logical_writes,
     delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'AppDb', 'dbo', 'usp_Steady', 'PROCEDURE', 0, 0, 0, 0, 0, 0, $5, 0, 0, $6)",
                _id++, at, ServerId, ServerName, delta, interval);
        }

        async Task<List<(DateTime At, int? Seconds)>> ReadAsync(DateTime from, DateTime to) =>
            (await _service.GetProcedureStatsHistoryAsync(ServerId, "AppDb", "dbo", "usp_Steady", fromDate: from, toDate: to))
            .Select(r => (r.CollectionTime, r.SampleIntervalSeconds)).ToList();

        Assert.Equal([(Middle, (int?)3600), (End, (int?)3600)], await ReadAsync(Start, End));
        Assert.Equal([(Middle, (int?)3600)], await ReadAsync(Start, Middle));
        Assert.Equal([(End, (int?)3600)], await ReadAsync(Middle, End));
    }

    /// <summary>The three collections: when, the delta, and the stored interval (none on the two inside the window).</summary>
    private static (DateTime At, long Delta, object Interval)[] Collections() =>
    [
        (Start, 7200, 3600),
        (Middle, 3600, DBNull.Value),
        (End, 3600, DBNull.Value),
    ];

    private async Task SeedWaitsAsync()
    {
        foreach (var (at, delta, interval) in Collections())
        {
            await ExecuteAsync(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 1, $6, 0, $7)",
                _id++, at, ServerId, ServerName, LockWait, delta, interval);
        }
    }

    /// <summary>Both hours are rated at 1 per second in the whole window, each half holds its own hour, and the halves add up to the whole.</summary>
    private static void AssertEachHourIsRatedOnce(IEnumerable<double> whole, IEnumerable<double> firstHalf, IEnumerable<double> secondHalf)
    {
        var all = whole.ToList();
        Assert.Equal(2, all.Count);
        Assert.All(all, rate => Assert.Equal(1.0, rate, 6));
        var first = Assert.Single(firstHalf);
        var second = Assert.Single(secondHalf);
        Assert.Equal(1.0, first, 6);
        Assert.Equal(1.0, second, 6);
        Assert.Equal(all.Sum(), first + second, 6);
    }

    private async Task ExecuteAsync(string sql, params object[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}
