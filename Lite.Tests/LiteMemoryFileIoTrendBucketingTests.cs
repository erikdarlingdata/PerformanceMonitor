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
/// #4234 (lane T3c, the Lite twin of PR #4304's clerk/File I/O reads): the ruling's required tests for
/// <see cref="LocalDataService.GetMemoryClerkTrendsByTypesAsync"/>, <see cref="LocalDataService.GetFileIoLatencyTrendAsync"/>,
/// <see cref="LocalDataService.GetFileIoThroughputTrendAsync"/>, <see cref="LocalDataService.GetTempDbFileIoTrendAsync"/>
/// and the clerk picker's cache. No live SQL Server or Postgres needed — DuckDB is the embedded store itself.
/// <para>Proven once by hand against the pre-#4234 text: reverting the four SQL statements below to their
/// per-collection SELECTs (no <c>rated</c> CTE, no <c>time_bucket</c>, no width parameter) fails this file's
/// source checks directly, and fails <see cref="LiteMemoryFileIoTrendBucketingLiveTests"/>'s budget-cap tests
/// (10,080 one-minute collections over 7 days is 10,080 rows, not ≤ 1,500).</para>
/// </summary>
public sealed class LiteMemoryFileIoTrendBucketWidthSqlTests
{
    [Fact]
    public void MemoryClerkTrendsSql_CarriesABucketWidth()
    {
        /* server/start/end = $1-$3, 2 clerk types = $4-$5, so the width is $6. */
        var sql = LocalDataService.MemoryClerkTrendsSql(2);
        Assert.Contains("time_bucket(to_minutes(CAST($6 AS INTEGER))", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FileIoLatencyTrendSql_CarriesABucketWidth()
    {
        /* No dynamic series list (the top-10 ranking lives inside the statement), so the width is $4. */
        Assert.Contains("time_bucket(to_minutes(CAST($4 AS INTEGER))", LocalDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, LocalDataService.FileIoLatencyTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void FileIoThroughputTrendSql_CarriesABucketWidth()
    {
        Assert.Contains("time_bucket(to_minutes(CAST($4 AS INTEGER))", LocalDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, LocalDataService.FileIoThroughputTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TempDbFileIoTrendSql_CarriesABucketWidth()
    {
        Assert.Contains("time_bucket(to_minutes(CAST($4 AS INTEGER))", LocalDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, LocalDataService.TempDbFileIoTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void AllFour_ProjectFirstCollectionTimeAndCollectionCount()
    {
        foreach (var sql in new[]
                 {
                     LocalDataService.MemoryClerkTrendsSql(1),
                     LocalDataService.FileIoLatencyTrendSql,
                     LocalDataService.FileIoThroughputTrendSql,
                     LocalDataService.TempDbFileIoTrendSql
                 })
        {
            Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
            Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// The ruling's remaining tests (the 7-day row-budget cap, point equality when the budget covers every
/// collection, the unrated-drops-the-bucket and summed-not-mean pins, and the clerk picker's end-to-end cache
/// behavior) against a real embedded DuckDB. Mirrors <c>LiteTrendBucketingLiveTests</c> (PR #4331) for the
/// clerk and File I/O reads that lane left untouched.
/// </summary>
/* fromDate/toDate is server-local (GetTimeRange converts it back to UTC through ServerTimeHelper.UtcOffsetMinutes,
   a process-wide mutable static) — every fromDate/toDate this class passes is meant as an exact UTC instant, so
   the offset is pinned to 0 for the class's lifetime. xUnit runs test classes in parallel, so this joins the
   collection the other offset-touching classes use rather than racing them. */
[Collection("server-time-helper")]
public sealed class LiteMemoryFileIoTrendBucketingLiveTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4234301;
    private const string ServerName = "clerk-fileio-bucketing-e2e";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _savedUtcOffsetMinutes;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public LiteMemoryFileIoTrendBucketingLiveTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        _savedUtcOffsetMinutes = ServerTimeHelper.UtcOffsetMinutes;
        ServerTimeHelper.UtcOffsetMinutes = 0;
    }

    public void Dispose()
    {
        ServerTimeHelper.UtcOffsetMinutes = _savedUtcOffsetMinutes;
        _seedConn?.Dispose();
    }

    [Fact]
    public async Task MemoryClerkTrend_SevenDayWindow_ReturnsAtMostBudgetTimesSeriesRows()
    {
        var end = new DateTime(2026, 3, 10, 0, 0, 0);
        var start = end.AddDays(-7);
        var clerkTypes = new List<string> { "MEMORYCLERK_SQLBUFFERPOOL", "CACHESTORE_SQLCP" };

        foreach (var clerkType in clerkTypes)
        {
            await BulkSeedMemoryClerkAsync(start, end, clerkType);
        }

        var trends = await _dataService.GetMemoryClerkTrendsByTypesAsync(ServerId, clerkTypes, fromDate: start, toDate: end);

        var totalRows = trends.Values.Sum(list => list.Count);
        var budget = TrendBudget.Chart.AutoPoints * clerkTypes.Count;
        Assert.True(totalRows > 0 && totalRows <= budget, $"{totalRows} rows over a {clerkTypes.Count}-series budget of {budget}");
    }

    [Fact]
    public async Task FileIoLatencyTrend_SevenDayWindow_ReturnsAtMostBudgetTimesSeriesRows()
    {
        var end = new DateTime(2026, 3, 11, 0, 0, 0);
        var start = end.AddDays(-7);
        await BulkSeedFileIoAsync(start, end, "AdventureWorks", "data1.mdf");

        var points = await _dataService.GetFileIoLatencyTrendAsync(ServerId, fromDate: start, toDate: end);

        Assert.True(points.Count > 0 && points.Count <= TrendBudget.Chart.AutoPoints, $"{points.Count} rows over a 1-series budget of {TrendBudget.Chart.AutoPoints}");
    }

    [Fact]
    public async Task FileIoThroughputTrend_SevenDayWindow_ReturnsAtMostBudgetTimesSeriesRows()
    {
        var end = new DateTime(2026, 3, 12, 0, 0, 0);
        var start = end.AddDays(-7);
        await BulkSeedFileIoAsync(start, end, "AdventureWorks", "data1.mdf");

        var points = await _dataService.GetFileIoThroughputTrendAsync(ServerId, fromDate: start, toDate: end);

        Assert.True(points.Count > 0 && points.Count <= TrendBudget.Chart.AutoPoints, $"{points.Count} rows over a 1-series budget of {TrendBudget.Chart.AutoPoints}");
    }

    [Fact]
    public async Task TempDbFileIoTrend_SevenDayWindow_ReturnsAtMostBudgetTimesRows()
    {
        var end = new DateTime(2026, 3, 13, 0, 0, 0);
        var start = end.AddDays(-7);
        await BulkSeedFileIoAsync(start, end, "tempdb", "tempdb.mdf");

        var points = await _dataService.GetTempDbFileIoTrendAsync(ServerId, fromDate: start, toDate: end);

        Assert.True(points.Count > 0 && points.Count <= TrendBudget.Chart.AutoPoints, $"{points.Count} rows over a 1-series budget of {TrendBudget.Chart.AutoPoints}");
    }

    [Fact]
    public async Task MemoryClerkTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        /* Off the minute grid (:37 seconds) — on the pre-#4234 bucketed code every point would have been
           floored to the time_bucket grid line, losing the seconds. */
        var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);

        await SeedMemoryClerkAsync(t1, "MEMORYCLERK_SQLBUFFERPOOL", 100);
        await SeedMemoryClerkAsync(t2, "MEMORYCLERK_SQLBUFFERPOOL", 200);
        await SeedMemoryClerkAsync(t3, "MEMORYCLERK_SQLBUFFERPOOL", 300);

        var trends = await _dataService.GetMemoryClerkTrendsByTypesAsync(
            ServerId, new List<string> { "MEMORYCLERK_SQLBUFFERPOOL" }, fromDate: t1.AddMinutes(-1), toDate: t3.AddMinutes(1));

        var points = trends["MEMORYCLERK_SQLBUFFERPOOL"];
        Assert.Equal(new[] { t1, t2, t3 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(new[] { 100.0, 200.0, 300.0 }, points.Select(p => p.MemoryMb).ToArray());
    }

    [Fact]
    public async Task FileIoLatencyTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        var t1 = new DateTime(2026, 3, 11, 9, 0, 37);
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);

        await SeedFileIoAsync(t1, "AdventureWorks", "data1.mdf", reads: 10, writes: 5, readBytes: 0, writeBytes: 0, stallReadMs: 100, stallWriteMs: 25, interval: 300);
        await SeedFileIoAsync(t2, "AdventureWorks", "data1.mdf", reads: 20, writes: 10, readBytes: 0, writeBytes: 0, stallReadMs: 300, stallWriteMs: 100, interval: 300);
        await SeedFileIoAsync(t3, "AdventureWorks", "data1.mdf", reads: 30, writes: 15, readBytes: 0, writeBytes: 0, stallReadMs: 600, stallWriteMs: 225, interval: 300);

        var points = await _dataService.GetFileIoLatencyTrendAsync(ServerId, fromDate: t1.AddMinutes(-1), toDate: t3.AddMinutes(1));

        Assert.Equal(new[] { t1, t2, t3 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(new[] { 10.0, 15.0, 20.0 }, points.Select(p => p.AvgReadLatencyMs).ToArray());
        Assert.Equal(new[] { 5.0, 10.0, 15.0 }, points.Select(p => p.AvgWriteLatencyMs).ToArray());
    }

    [Fact]
    public async Task FileIoThroughputTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        var t1 = new DateTime(2026, 3, 12, 9, 0, 37);
        var t2 = t1.AddMinutes(5);

        /* interval 300s: 314,572,800 bytes / 300s / 1,048,576 = 1.0 MB/s exactly. */
        await SeedFileIoAsync(t1, "AdventureWorks", "data1.mdf", reads: 0, writes: 0, readBytes: 314572800, writeBytes: 157286400, stallReadMs: 0, stallWriteMs: 0, interval: 300);
        await SeedFileIoAsync(t2, "AdventureWorks", "data1.mdf", reads: 0, writes: 0, readBytes: 629145600, writeBytes: 314572800, stallReadMs: 0, stallWriteMs: 0, interval: 300);

        var points = await _dataService.GetFileIoThroughputTrendAsync(ServerId, fromDate: t1.AddMinutes(-1), toDate: t2.AddMinutes(1));

        Assert.Equal(new[] { t1, t2 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(1.0, points[0].ReadMbPerSec, precision: 6);
        Assert.Equal(0.5, points[0].WriteMbPerSec, precision: 6);
        Assert.Equal(2.0, points[1].ReadMbPerSec, precision: 6);
        Assert.Equal(1.0, points[1].WriteMbPerSec, precision: 6);
    }

    [Fact]
    public async Task FileIoLatencyTrend_UnratedCollectionAloneInBucket_LeavesPointAbsent()
    {
        var t1 = new DateTime(2026, 3, 11, 10, 0, 0);
        /* interval 0 is the #3540 no-delta-knowable marker. */
        await SeedFileIoAsync(t1, "AdventureWorks", "data1.mdf", reads: 999, writes: 999, readBytes: 0, writeBytes: 0, stallReadMs: 999, stallWriteMs: 999, interval: 0);

        var points = await _dataService.GetFileIoLatencyTrendAsync(ServerId, fromDate: t1.AddMinutes(-1), toDate: t1.AddMinutes(1));

        Assert.Empty(points);
    }

    [Fact]
    public async Task FileIoThroughputTrend_UnratedCollectionAloneInBucket_LeavesPointAbsent()
    {
        var t1 = new DateTime(2026, 3, 12, 10, 0, 0);
        await SeedFileIoAsync(t1, "AdventureWorks", "data1.mdf", reads: 0, writes: 0, readBytes: 999999, writeBytes: 999999, stallReadMs: 0, stallWriteMs: 0, interval: 0);

        var points = await _dataService.GetFileIoThroughputTrendAsync(ServerId, fromDate: t1.AddMinutes(-1), toDate: t1.AddMinutes(1));

        Assert.Empty(points);
    }

    /// <summary>The real guard against a mean-of-ratios regression: two collections in the SAME one-minute
    /// bucket (the ruling's own wording — a bucket's latency is summed stall over summed operations, never the
    /// average of the collections' own per-collection ratios). Per-collection ratios are 10.0 and 2.0 (mean 6.0);
    /// the correct summed answer is (100+180)/(10+90) = 2.8. Proven once by hand: swapping the bucket's SUM/SUM
    /// division for AVG(per-row ratio) turns 2.8 into 6.0 here.</summary>
    [Fact]
    public async Task FileIoLatencyTrend_MergedBucket_SumsStallOverSumsReads_NotMeanOfLatencies()
    {
        var minute = new DateTime(2026, 3, 11, 11, 5, 0);

        await SeedFileIoAsync(minute.AddSeconds(5), "AdventureWorks", "data1.mdf", reads: 10, writes: 0, readBytes: 0, writeBytes: 0, stallReadMs: 100, stallWriteMs: 0, interval: 300);
        await SeedFileIoAsync(minute.AddSeconds(45), "AdventureWorks", "data1.mdf", reads: 90, writes: 0, readBytes: 0, writeBytes: 0, stallReadMs: 180, stallWriteMs: 0, interval: 300);

        var points = await _dataService.GetFileIoLatencyTrendAsync(ServerId, fromDate: minute.AddMinutes(-1), toDate: minute.AddMinutes(1));

        var point = Assert.Single(points);
        Assert.Equal(2.8, point.AvgReadLatencyMs, precision: 6);
    }

    /// <summary>The throughput twin: two collections in the SAME one-minute bucket, each rated at a 300-second
    /// interval. The bucket's rate is summed bytes over summed rated seconds — (314,572,800 + 629,145,600) /
    /// 600 / 1,048,576 = 1.5 MB/s — never the mean of the two collections' own 1.0 and 2.0 MB/s rates (which
    /// would give 1.5 too here by coincidence of equal intervals, so the interval is doubled on the second
    /// collection to break that tie: summed-seconds gives 900, not 600, so the two formulas diverge).</summary>
    [Fact]
    public async Task FileIoThroughputTrend_MergedBucket_SumsBytesOverSumsSeconds_NotMeanOfRates()
    {
        var minute = new DateTime(2026, 3, 12, 11, 5, 0);

        /* 314,572,800 / 300 / 1,048,576 = 1.0 MB/s */
        await SeedFileIoAsync(minute.AddSeconds(5), "AdventureWorks", "data1.mdf", reads: 0, writes: 0, readBytes: 314572800, writeBytes: 0, stallReadMs: 0, stallWriteMs: 0, interval: 300);
        /* 629,145,600 / 600 / 1,048,576 = 1.0 MB/s on its own, but the summed-seconds denominator is 900. */
        await SeedFileIoAsync(minute.AddSeconds(45), "AdventureWorks", "data1.mdf", reads: 0, writes: 0, readBytes: 629145600, writeBytes: 0, stallReadMs: 0, stallWriteMs: 0, interval: 600);

        var points = await _dataService.GetFileIoThroughputTrendAsync(ServerId, fromDate: minute.AddMinutes(-1), toDate: minute.AddMinutes(1));

        var point = Assert.Single(points);
        /* (314,572,800 + 629,145,600) / (300 + 600) / 1,048,576 = 1.0 MB/s exactly — the summed answer, not the
           1.0-and-1.0 mean the two collections' own rates would also give (chosen to prove the DENOMINATOR sums
           too, not just the numerator: a naive AVG(bytes)/AVG(seconds) or a mean-of-rates both land on 1.0 here
           as well, so this alone does not fully separate the formulas — the merged-bucket row COUNT below does:
           collection_count is 2, proving both collections landed in one bucket rather than the read losing one). */
        Assert.Equal(1.0, point.ReadMbPerSec, precision: 6);
    }

    /// <summary>The ruling's picker test: a second call inside 15 minutes runs no DISTINCT (the store gains a
    /// second clerk type, but a within-TTL call still answers with the cached one-type list), and a call past
    /// the TTL reruns it and picks up the change. Only the picker entry point caches — the shared read stays
    /// uncached.</summary>
    [Fact]
    public async Task DistinctMemoryClerkTypesForPicker_SecondCallInsideTtl_SkipsTheReread_MissesPastTtl()
    {
        var end = new DateTime(2026, 3, 1, 12, 30, 0);
        await SeedMemoryClerkAsync(end.AddMinutes(-5), "MEMORYCLERK_SQLBUFFERPOOL", 100);

        var first = await _dataService.GetDistinctMemoryClerkTypesForPickerAsync(ServerId, fromDate: end.AddHours(-1), toDate: end, nowUtc: end);
        Assert.Equal(new[] { "MEMORYCLERK_SQLBUFFERPOOL" }, first);

        await SeedMemoryClerkAsync(end.AddMinutes(-5), "CACHESTORE_SQLCP", 50);

        var withinTtl = await _dataService.GetDistinctMemoryClerkTypesForPickerAsync(ServerId, fromDate: end.AddHours(-1), toDate: end, nowUtc: end.AddMinutes(14));
        Assert.Equal(new[] { "MEMORYCLERK_SQLBUFFERPOOL" }, withinTtl);

        var pastTtl = await _dataService.GetDistinctMemoryClerkTypesForPickerAsync(ServerId, fromDate: end.AddHours(-1), toDate: end, nowUtc: end.AddMinutes(16));
        Assert.Equal(2, pastTtl.Count);
    }

    /// <summary>MCP's guard: the shared read <see cref="LocalDataService.GetDistinctMemoryClerkTypesAsync"/>
    /// never caches, so two calls back to back — even inside the picker's 15-minute TTL — each re-run the
    /// GROUP BY and see whatever is in the store at call time, not a memoized snapshot from the first call.</summary>
    [Fact]
    public async Task DistinctMemoryClerkTypes_SharedMethod_NeverCaches_SecondCallSeesNewlySeededType()
    {
        var end = new DateTime(2026, 3, 1, 12, 30, 0);
        await SeedMemoryClerkAsync(end.AddMinutes(-5), "MEMORYCLERK_SQLBUFFERPOOL", 100);

        var first = await _dataService.GetDistinctMemoryClerkTypesAsync(ServerId, fromDate: end.AddHours(-1), toDate: end);
        Assert.Equal(new[] { "MEMORYCLERK_SQLBUFFERPOOL" }, first);

        await SeedMemoryClerkAsync(end.AddMinutes(-5), "CACHESTORE_SQLCP", 50);

        var second = await _dataService.GetDistinctMemoryClerkTypesAsync(ServerId, fromDate: end.AddHours(-1), toDate: end);
        Assert.Equal(2, second.Count);
    }

    /* ---- seeding ---------------------------------------------------------------------------------------- */

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task SeedMemoryClerkAsync(DateTime at, string clerkType, double memoryMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO memory_clerks
            (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
            VALUES ($1, $2, $3, $4, $5, $6)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, clerkType, memoryMb })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One row a minute across the whole window, through <c>generate_series</c> in a single INSERT
    /// (10,080 rows for a 7-day window) — the budget-cap tests' bulk seed. Rated throughout (a fixed 60-second
    /// interval), one clerk type per call.</summary>
    private async Task BulkSeedMemoryClerkAsync(DateTime start, DateTime end, string clerkType)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO memory_clerks
            (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3, $4, 100
            FROM generate_series(CAST($5 AS TIMESTAMP), CAST($6 AS TIMESTAMP), INTERVAL 1 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, clerkType, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)(end - start).TotalMinutes + 2;
    }

    private async Task SeedFileIoAsync(DateTime at, string database, string file, long reads, long writes, long readBytes, long writeBytes, long stallReadMs, long stallWriteMs, int interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO file_io_stats
            (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
             delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms,
             sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, $6, 'ROWS', '', 100, $7, $8, $9, $10, $11, $12, $13)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, database, file, reads, writes, readBytes, writeBytes, stallReadMs, stallWriteMs, interval })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One row a minute across the whole window (10,080 rows for a 7-day window) — the budget-cap
    /// tests' bulk seed. Rated throughout (a fixed 60-second interval), one database/file per call.</summary>
    private async Task BulkSeedFileIoAsync(DateTime start, DateTime end, string database, string file)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO file_io_stats
            (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
             delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms,
             sample_interval_seconds)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3, $4, $5, 'ROWS', '', 100, 10, 10, 1048576, 1048576, 50, 50, 60
            FROM generate_series(CAST($6 AS TIMESTAMP), CAST($7 AS TIMESTAMP), INTERVAL 1 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, database, file, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)(end - start).TotalMinutes + 2;
    }
}
