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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4234: the Performance Trends chart's three duration/execution-count reads —
/// <see cref="LocalDataService.GetQueryDurationTrendAsync"/>, <see cref="LocalDataService.GetProcedureDurationTrendAsync"/>
/// and <see cref="LocalDataService.GetExecutionCountTrendAsync"/> — now bucket server-side instead of returning one
/// row per collection. <see cref="DeltaFamilyUnknowableRowReadTests"/> already pins the #3540/#3541 unrated-row
/// contract against these same reads at 1-minute (singleton) buckets and must keep passing unchanged; these tests
/// pin the NEW bucketing behavior the old suite cannot reach: the bucket-width parameter is really in the SQL, a
/// dense window stays under <see cref="TrendBudget.Chart"/>'s point budget, a multi-collection bucket sums work
/// over seconds rather than averaging the per-collection rates, and an unrated collection sharing a bucket with a
/// rated one contributes nothing to that bucket's rate.
/// </summary>
public sealed class PerformanceTrendDurationBucketingTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4234;
    private const string ServerName = "trend-bucket-e2e";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public PerformanceTrendDurationBucketingTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    /* ---- source checks: the bucket width is really in the statement (ruling item 6, bullet 1) --------------- */

    [Fact]
    public void DurationAndExecutionCountTrendSql_CarryTheBucketWidthParameter_AndKeepTheDatabaseFilterNumbering()
    {
        var queryNoFilter = LocalDataService.DurationTrendChartSql("v_query_stats", "", widthParamIndex: 4);
        Assert.Contains("time_bucket(to_minutes(CAST($4 AS INTEGER))", queryNoFilter);
        Assert.Contains("FROM v_query_stats", queryNoFilter);
        Assert.Contains("COUNT(*) AS collection_count", queryNoFilter);
        Assert.Contains("MIN(collection_time) AS first_collection_time", queryNoFilter);
        /* No HAVING clause of its own (a bucket with nothing rated still keeps its row) -- proven functionally
           by the MergedBucket tests below, not by scanning for the keyword: this read's own doc comment
           legitimately uses the word "HAVING" in English prose. */

        /* A two-value database filter occupies $4/$5; the width must move to $6 without disturbing them. */
        var procWithFilter = LocalDataService.DurationTrendChartSql("v_procedure_stats", " AND database_name IN ($4, $5)", widthParamIndex: 6);
        Assert.Contains("database_name IN ($4, $5)", procWithFilter);
        Assert.Contains("time_bucket(to_minutes(CAST($6 AS INTEGER))", procWithFilter);
        Assert.Contains("FROM v_procedure_stats", procWithFilter);

        var execNoFilter = LocalDataService.ExecutionCountTrendChartSql("", widthParamIndex: 4);
        Assert.Contains("time_bucket(to_minutes(CAST($4 AS INTEGER))", execNoFilter);
        Assert.Contains("FROM v_query_stats", execNoFilter);
        Assert.Contains("COUNT(*) AS collection_count", execNoFilter);
    }

    /* ---- budget: a dense 7-day window still stays near the chart's point budget (bullet 2) ------------------ */

    [Fact]
    public async Task QueryDurationTrend_DenseSevenDayWindow_StaysWithinTheChartPointBudget()
    {
        var baseTime = MinuteFloor(DateTime.UtcNow.AddHours(-100));
        const int collections = 1600;
        await SeedManyQueryStatsAsync(baseTime, collections);

        var points = await _dataService.GetQueryDurationTrendAsync(ServerId, hoursBack: 168);

        Assert.True(points.Count <= TrendBudget.Chart.AutoPoints, $"{points.Count} points exceeded the {TrendBudget.Chart.AutoPoints}-point chart budget.");
        Assert.True(points.Count < collections, $"{points.Count} points did not compress {collections} one-minute-apart collections at all.");
    }

    [Fact]
    public async Task ProcedureDurationTrend_DenseSevenDayWindow_StaysWithinTheChartPointBudget()
    {
        var baseTime = MinuteFloor(DateTime.UtcNow.AddHours(-100));
        const int collections = 1600;
        await SeedManyProcedureStatsAsync(baseTime, collections);

        var points = await _dataService.GetProcedureDurationTrendAsync(ServerId, hoursBack: 168);

        Assert.True(points.Count <= TrendBudget.Chart.AutoPoints, $"{points.Count} points exceeded the {TrendBudget.Chart.AutoPoints}-point chart budget.");
        Assert.True(points.Count < collections, $"{points.Count} points did not compress {collections} one-minute-apart collections at all.");
    }

    [Fact]
    public async Task ExecutionCountTrend_DenseSevenDayWindow_StaysWithinTheChartPointBudget()
    {
        var baseTime = MinuteFloor(DateTime.UtcNow.AddHours(-100));
        const int collections = 1600;
        await SeedManyQueryStatsAsync(baseTime, collections);

        var points = await _dataService.GetExecutionCountTrendAsync(ServerId, hoursBack: 168);

        Assert.True(points.Count <= TrendBudget.Chart.AutoPoints, $"{points.Count} points exceeded the {TrendBudget.Chart.AutoPoints}-point chart budget.");
        Assert.True(points.Count < collections, $"{points.Count} points did not compress {collections} one-minute-apart collections at all.");
    }

    /* ---- singleton buckets equal the old per-collection read, off the minute grid, with a database filter
       (bullet 3) ------------------------------------------------------------------------------------------- */

    [Fact]
    public async Task QueryDurationTrend_SingletonBuckets_MatchTheOldPerCollectionRead_WithADatabaseFilter()
    {
        var minuteFloor = MinuteFloor(DateTime.UtcNow.AddMinutes(-50));
        var t1 = minuteFloor.AddSeconds(37);
        var t2 = t1.AddMinutes(12);
        var excluded = t1.AddMinutes(5);

        await SeedQueryStatAsync(t1, "0xA", "AppDb", deltaExecutions: 12, deltaElapsedUs: 360_000, interval: 30);
        await SeedQueryStatAsync(t2, "0xB", "AppDb", deltaExecutions: 100, deltaElapsedUs: 250_000, interval: 25);
        await SeedQueryStatAsync(excluded, "0xC", "OtherDb", deltaExecutions: 999_999, deltaElapsedUs: 999_999_000, interval: 1);

        var points = await _dataService.GetQueryDurationTrendAsync(ServerId, hoursBack: 1, databaseNames: new[] { "AppDb" });

        Assert.Equal(2, points.Count);
        Assert.Equal(new[] { t1, t2 }, points.Select(p => p.CollectionTime).ToArray());

        Assert.Equal(12.0, points[0].Value!.Value, precision: 6);
        Assert.Equal(0.4, points[0].ExecutionsPerSecond!.Value, precision: 6);
        Assert.Equal(0L, points[0].ExecutionCount);

        Assert.Equal(10.0, points[1].Value!.Value, precision: 6);
        Assert.Equal(4.0, points[1].ExecutionsPerSecond!.Value, precision: 6);
        Assert.Equal(4L, points[1].ExecutionCount);

        /* #4234: only Value/ExecutionCount/ExecutionsPerSecond are filled for a chart point — the MCP-only
           bucket fields stay null, as they always have for these three reads. */
        Assert.Null(points[0].FirstCollectionTime);
        Assert.Null(points[0].PeakElapsedMsPerSecond);
        Assert.Null(points[0].UnratedInBucket);
    }

    [Fact]
    public async Task ProcedureDurationTrend_SingletonBuckets_MatchTheOldPerCollectionRead_WithADatabaseFilter()
    {
        var minuteFloor = MinuteFloor(DateTime.UtcNow.AddMinutes(-50));
        var t1 = minuteFloor.AddSeconds(37);
        var t2 = t1.AddMinutes(12);
        var excluded = t1.AddMinutes(5);

        await SeedProcedureAsync(t1, "usp_A", "AppDb", deltaExecutions: 12, deltaElapsedUs: 360_000, interval: 30);
        await SeedProcedureAsync(t2, "usp_B", "AppDb", deltaExecutions: 100, deltaElapsedUs: 250_000, interval: 25);
        await SeedProcedureAsync(excluded, "usp_C", "OtherDb", deltaExecutions: 999_999, deltaElapsedUs: 999_999_000, interval: 1);

        var points = await _dataService.GetProcedureDurationTrendAsync(ServerId, hoursBack: 1, databaseNames: new[] { "AppDb" });

        Assert.Equal(2, points.Count);
        Assert.Equal(new[] { t1, t2 }, points.Select(p => p.CollectionTime).ToArray());

        Assert.Equal(12.0, points[0].Value!.Value, precision: 6);
        Assert.Equal(0.4, points[0].ExecutionsPerSecond!.Value, precision: 6);

        Assert.Equal(10.0, points[1].Value!.Value, precision: 6);
        Assert.Equal(4.0, points[1].ExecutionsPerSecond!.Value, precision: 6);
    }

    [Fact]
    public async Task ExecutionCountTrend_SingletonBuckets_MatchTheOldPerCollectionRead_WithADatabaseFilter()
    {
        var minuteFloor = MinuteFloor(DateTime.UtcNow.AddMinutes(-50));
        var t1 = minuteFloor.AddSeconds(37);
        var t2 = t1.AddMinutes(12);
        var excluded = t1.AddMinutes(5);

        await SeedQueryStatAsync(t1, "0xA", "AppDb", deltaExecutions: 12, deltaElapsedUs: 360_000, interval: 30);
        await SeedQueryStatAsync(t2, "0xB", "AppDb", deltaExecutions: 100, deltaElapsedUs: 250_000, interval: 25);
        await SeedQueryStatAsync(excluded, "0xC", "OtherDb", deltaExecutions: 999_999, deltaElapsedUs: 999_999_000, interval: 1);

        var points = await _dataService.GetExecutionCountTrendAsync(ServerId, hoursBack: 1, databaseNames: new[] { "AppDb" });

        Assert.Equal(2, points.Count);
        Assert.Equal(new[] { t1, t2 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(0.4, points[0].Value!.Value, precision: 6);
        Assert.Equal(4.0, points[1].Value!.Value, precision: 6);
    }

    /* ---- a merged bucket sums work over seconds (never averages the rates), and an unrated collection
       contributes nothing when it shares a bucket with a rated one; alone, it is a kept NULL point
       (bullets 4 and 5) ----------------------------------------------------------------------------------- */

    [Fact]
    public async Task QueryDurationTrend_MergedBucketSumsWork_UnratedAloneIsNull_UnratedInAMixIsIgnored()
    {
        var m1 = MinuteFloor(DateTime.UtcNow.AddMinutes(-40));
        var m2 = m1.AddMinutes(3);
        var m3 = m1.AddMinutes(6);

        /* m1: two RATED collections in the same minute, different seconds -- 10 ms/sec and 30 ms/sec alone,
           .5/sec and 2/sec alone. Summed: 700/30 = 23.3333 ms/sec, 45/30 = 1.5 execs/sec -- neither the mean
           of the two per-collection rates. */
        await SeedQueryStatAsync(m1.AddSeconds(5), "0xM1a", "AppDb", deltaExecutions: 5, deltaElapsedUs: 100_000, interval: 10);
        await SeedQueryStatAsync(m1.AddSeconds(45), "0xM1b", "AppDb", deltaExecutions: 40, deltaElapsedUs: 600_000, interval: 20);

        /* m2: one UNRATED collection alone in its bucket (a restart marker) -- kept, with a NULL rate. */
        await SeedQueryStatAsync(m2.AddSeconds(10), "0xM2", "AppDb", deltaExecutions: 777, deltaElapsedUs: 777_000, interval: 0);

        /* m3: one unrated collection beside one rated one -- the bucket's rate is the rated collection's own,
           unmoved by the unrated sibling's much larger deltas. */
        await SeedQueryStatAsync(m3.AddSeconds(5), "0xM3unrated", "AppDb", deltaExecutions: 999, deltaElapsedUs: 999_000, interval: 0);
        await SeedQueryStatAsync(m3.AddSeconds(45), "0xM3rated", "AppDb", deltaExecutions: 30, deltaElapsedUs: 150_000, interval: 15);

        var points = await _dataService.GetQueryDurationTrendAsync(ServerId, hoursBack: 1);

        Assert.Equal(3, points.Count);

        /* Not every bucket in this call is a singleton (m1 and m3 each hold two collections), so every point
           stamps at its bucket's start, not at a raw collection time -- including m2, itself a true singleton. */
        Assert.Equal(new[] { m1, m2, m3 }, points.Select(p => p.CollectionTime).ToArray());

        Assert.Equal(700.0 / 30.0, points[0].Value!.Value, precision: 6);
        Assert.Equal(45.0 / 30.0, points[0].ExecutionsPerSecond!.Value, precision: 6);

        Assert.False(points[1].HasRate);
        Assert.Null(points[1].Value);
        Assert.Null(points[1].ExecutionsPerSecond);

        Assert.Equal(10.0, points[2].Value!.Value, precision: 6);
        Assert.Equal(2.0, points[2].ExecutionsPerSecond!.Value, precision: 6);
    }

    [Fact]
    public async Task ProcedureDurationTrend_MergedBucketSumsWork_UnratedAloneIsNull_UnratedInAMixIsIgnored()
    {
        var m1 = MinuteFloor(DateTime.UtcNow.AddMinutes(-40));
        var m2 = m1.AddMinutes(3);
        var m3 = m1.AddMinutes(6);

        await SeedProcedureAsync(m1.AddSeconds(5), "usp_M1a", "AppDb", deltaExecutions: 5, deltaElapsedUs: 100_000, interval: 10);
        await SeedProcedureAsync(m1.AddSeconds(45), "usp_M1b", "AppDb", deltaExecutions: 40, deltaElapsedUs: 600_000, interval: 20);
        await SeedProcedureAsync(m2.AddSeconds(10), "usp_M2", "AppDb", deltaExecutions: 777, deltaElapsedUs: 777_000, interval: 0);
        await SeedProcedureAsync(m3.AddSeconds(5), "usp_M3unrated", "AppDb", deltaExecutions: 999, deltaElapsedUs: 999_000, interval: 0);
        await SeedProcedureAsync(m3.AddSeconds(45), "usp_M3rated", "AppDb", deltaExecutions: 30, deltaElapsedUs: 150_000, interval: 15);

        var points = await _dataService.GetProcedureDurationTrendAsync(ServerId, hoursBack: 1);

        Assert.Equal(3, points.Count);
        Assert.Equal(new[] { m1, m2, m3 }, points.Select(p => p.CollectionTime).ToArray());

        Assert.Equal(700.0 / 30.0, points[0].Value!.Value, precision: 6);
        Assert.Equal(45.0 / 30.0, points[0].ExecutionsPerSecond!.Value, precision: 6);

        Assert.False(points[1].HasRate);
        Assert.Null(points[1].Value);

        Assert.Equal(10.0, points[2].Value!.Value, precision: 6);
        Assert.Equal(2.0, points[2].ExecutionsPerSecond!.Value, precision: 6);
    }

    [Fact]
    public async Task ExecutionCountTrend_MergedBucketSumsWork_UnratedAloneIsNull_UnratedInAMixIsIgnored()
    {
        var m1 = MinuteFloor(DateTime.UtcNow.AddMinutes(-40));
        var m2 = m1.AddMinutes(3);
        var m3 = m1.AddMinutes(6);

        await SeedQueryStatAsync(m1.AddSeconds(5), "0xM1a", "AppDb", deltaExecutions: 5, deltaElapsedUs: 100_000, interval: 10);
        await SeedQueryStatAsync(m1.AddSeconds(45), "0xM1b", "AppDb", deltaExecutions: 40, deltaElapsedUs: 600_000, interval: 20);
        await SeedQueryStatAsync(m2.AddSeconds(10), "0xM2", "AppDb", deltaExecutions: 777, deltaElapsedUs: 777_000, interval: 0);
        await SeedQueryStatAsync(m3.AddSeconds(5), "0xM3unrated", "AppDb", deltaExecutions: 999, deltaElapsedUs: 999_000, interval: 0);
        await SeedQueryStatAsync(m3.AddSeconds(45), "0xM3rated", "AppDb", deltaExecutions: 30, deltaElapsedUs: 150_000, interval: 15);

        var points = await _dataService.GetExecutionCountTrendAsync(ServerId, hoursBack: 1);

        Assert.Equal(3, points.Count);
        Assert.Equal(new[] { m1, m2, m3 }, points.Select(p => p.CollectionTime).ToArray());

        Assert.Equal(45.0 / 30.0, points[0].Value!.Value, precision: 6);
        Assert.Null(points[1].Value);
        Assert.Equal(2.0, points[2].Value!.Value, precision: 6);
    }

    /* ---- seeding ---------------------------------------------------------------------------------------- */

    private static DateTime MinuteFloor(DateTime value)
    {
        var truncated = new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute));
        return DateTime.SpecifyKind(truncated, DateTimeKind.Unspecified);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private static object IntervalValue(int? interval) => interval.HasValue ? interval.Value : DBNull.Value;

    private async Task SeedQueryStatAsync(DateTime at, string queryHash, string databaseName, long deltaExecutions, long deltaElapsedUs, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_stats
            (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
             execution_count, total_worker_time, total_elapsed_time,
             delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, $6, '0xPLAN', '0xSQL', '0xPLANH', 0, 0, 0, $7, 0, $8, $9)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, databaseName, queryHash, deltaExecutions, deltaElapsedUs, IntervalValue(interval) })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedProcedureAsync(DateTime at, string objectName, string databaseName, long deltaExecutions, long deltaElapsedUs, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO procedure_stats
            (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
             execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_physical_reads, total_logical_writes,
             delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, 'dbo', $6, 'PROCEDURE', 0, 0, 0, 0, 0, 0, $7, 0, $8, $9)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, databaseName, objectName, deltaExecutions, deltaElapsedUs, IntervalValue(interval) })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Bulk-inserts <paramref name="count"/> RATED, one-minute-apart query_stats collections starting at
    /// <paramref name="baseTime"/> in a single statement (a generate_series cross join), so the budget tests seed
    /// thousands of rows without thousands of round trips.</summary>
    private async Task SeedManyQueryStatsAsync(DateTime baseTime, int count)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_stats
            (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
             execution_count, total_worker_time, total_elapsed_time,
             delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
            SELECT
                -2000000 - g, $1 + to_minutes(CAST(g AS INTEGER)), $2, $3, 'AppDb', '0xBUDGET', '0xPLAN', '0xSQL', '0xPLANH',
                0, 0, 0, 60, 0, 6000, 60
            FROM generate_series(0, $4) AS s(g)";
        foreach (var v in new object[] { baseTime, ServerId, ServerName, count - 1 })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The procedure_stats twin of <see cref="SeedManyQueryStatsAsync"/>.</summary>
    private async Task SeedManyProcedureStatsAsync(DateTime baseTime, int count)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO procedure_stats
            (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
             execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_physical_reads, total_logical_writes,
             delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
            SELECT
                -3000000 - g, $1 + to_minutes(CAST(g AS INTEGER)), $2, $3, 'AppDb', 'dbo', 'usp_Budget', 'PROCEDURE',
                0, 0, 0, 0, 0, 0, 60, 0, 6000, 60
            FROM generate_series(0, $4) AS s(g)";
        foreach (var v in new object[] { baseTime, ServerId, ServerName, count - 1 })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}
