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
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Deadlocks, Top Queries, Top Procedures and Query Store strips over a short window. Each read buckets a one-hour window by the
/// minute, the width <c>SlicerBuckets</c> picks for it, so the strip's line reaches the window's end instead of stopping at the last
/// whole hour while the grid below holds rows up to the end. Over a day the same reads keep the hour.
/// </summary>
public sealed class StripMinuteBucketsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public StripMinuteBucketsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose() => _seedConn?.Dispose();

    private static DateTime MinuteFloor(DateTime at) => new(at.Ticks - at.Ticks % TimeSpan.TicksPerMinute, at.Kind);

    /* Two instants, six and three minutes ago, each a few seconds past the minute so the bucket stamp is the minute and not the row's time. */
    private static (DateTime Older, DateTime Newer) TwoMinutes()
    {
        var floor = MinuteFloor(DateTime.UtcNow);
        var newer = floor.AddMinutes(-3).AddSeconds(7);
        var older = floor.AddMinutes(-6).AddSeconds(7);
        return (DateTime.SpecifyKind(older, DateTimeKind.Unspecified), DateTime.SpecifyKind(newer, DateTimeKind.Unspecified));
    }

    [Fact]
    public async Task TheDeadlocksStrip_BucketsAOneHourWindowByTheMinute()
    {
        var (older, newer) = TwoMinutes();
        await SeedDeadlockAsync(older);
        await SeedDeadlockAsync(newer);
        await SeedDeadlockAsync(newer.AddSeconds(1));

        var buckets = await _dataService.GetDeadlockSlicerDataAsync(_serverId, hoursBack: 1);

        Assert.Equal(new[] { MinuteFloor(older), MinuteFloor(newer) }, buckets.Select(b => b.BucketTime).ToArray());
        Assert.Equal(new long[] { 1, 2 }, buckets.Select(b => b.SessionCount).ToArray());
    }

    [Fact]
    public async Task TheTopQueriesStrip_BucketsAOneHourWindowByTheMinute()
    {
        var (older, newer) = TwoMinutes();
        await SeedQueryStatsAsync(older, "0x01", 100);
        await SeedQueryStatsAsync(newer, "0x02", 200);
        await SeedQueryStatsAsync(newer, "0x03", 300);

        var buckets = await _dataService.GetQueryStatsSlicerDataAsync(_serverId, hoursBack: 1);

        Assert.Equal(new[] { MinuteFloor(older), MinuteFloor(newer) }, buckets.Select(b => b.BucketTime).ToArray());
        Assert.Equal(new long[] { 1, 2 }, buckets.Select(b => b.SessionCount).ToArray());
    }

    [Fact]
    public async Task TheTopProceduresStrip_BucketsAOneHourWindowByTheMinute()
    {
        var (older, newer) = TwoMinutes();
        await SeedProcedureStatsAsync(older, "usp_One");
        await SeedProcedureStatsAsync(newer, "usp_Two");
        await SeedProcedureStatsAsync(newer, "usp_Three");

        var buckets = await _dataService.GetProcStatsSlicerDataAsync(_serverId, hoursBack: 1);

        Assert.Equal(new[] { MinuteFloor(older), MinuteFloor(newer) }, buckets.Select(b => b.BucketTime).ToArray());
        Assert.Equal(new long[] { 1, 2 }, buckets.Select(b => b.SessionCount).ToArray());
    }

    /* The Query Store strip keys its bars on the interval's start, so two one-minute intervals three minutes apart are two bars a minute wide each. */
    [Fact]
    public async Task TheQueryStoreStrip_BucketsAOneHourWindowByTheMinute()
    {
        var (older, newer) = TwoMinutes();
        await SeedQueryStoreAsync(older, queryId: 1);
        await SeedQueryStoreAsync(newer, queryId: 2);
        await SeedQueryStoreAsync(newer, queryId: 3);

        var buckets = await _dataService.GetQueryStoreSlicerDataAsync(_serverId, hoursBack: 1);

        Assert.Equal(new[] { MinuteFloor(older), MinuteFloor(newer) }, buckets.Select(b => b.BucketTime).ToArray());
        Assert.Equal(new long[] { 1, 2 }, buckets.Select(b => b.SessionCount).ToArray());
    }

    /* Over a day the strip keeps the hour: a Past 24 hours window with two deadlocks in one hour is one bucket stamped on the hour. */
    [Fact]
    public async Task TheDeadlocksStrip_KeepsTheHourOverADay()
    {
        var now = DateTime.UtcNow;
        var hour = new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerHour, DateTimeKind.Unspecified).AddHours(-3);
        await SeedDeadlockAsync(hour.AddMinutes(5));
        await SeedDeadlockAsync(hour.AddMinutes(25));

        var buckets = await _dataService.GetDeadlockSlicerDataAsync(_serverId, hoursBack: 24);

        var bucket = Assert.Single(buckets);
        Assert.Equal(hour, bucket.BucketTime);
        Assert.Equal(2, bucket.SessionCount);
    }

    private Task SeedDeadlockAsync(DateTime at) => ExecAsync(
        "INSERT INTO deadlocks (deadlock_id, collection_time, deadlock_time, server_id, server_name) VALUES ($1, $2, $2, $3, $4)",
        _nextId--, at, _serverId, ServerName);

    private Task SeedQueryStatsAsync(DateTime at, string queryHash, long workerTime) => ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count, delta_worker_time, delta_elapsed_time)
VALUES ($1, $2, $3, $4, 'TestDb', $5, 1, $6, $6)",
        _nextId--, at, _serverId, ServerName, queryHash, workerTime);

    private Task SeedProcedureStatsAsync(DateTime at, string objectName) => ExecAsync(@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, delta_execution_count, delta_worker_time, delta_elapsed_time)
VALUES ($1, $2, $3, $4, 'TestDb', 'dbo', $5, 1, 100, 100)",
        _nextId--, at, _serverId, ServerName, objectName);

    private Task SeedQueryStoreAsync(DateTime intervalStart, long queryId) => ExecAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     execution_count, avg_cpu_time_us, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc, replica_role)
VALUES ($1, $2, $3, $4, 'TestDb', $5, $5, 'Regular', $2, 1, 1000, 2000, $5, $6, 'PRIMARY')",
        _nextId--, intervalStart.AddSeconds(20), _serverId, ServerName, queryId, MinuteFloor(intervalStart));

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
