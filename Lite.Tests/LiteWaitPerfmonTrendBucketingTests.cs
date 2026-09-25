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
/// #4234 (lane T3b, the Lite twin of PR #4304): the ruling's four required tests for
/// <see cref="LocalDataService.GetWaitStatsTrendsByTypesAsync"/> / <c>GetPerfmonTrendsByCountersAsync</c> and their
/// two picker reads. No live SQL Server or Postgres needed — DuckDB is the embedded store itself.
/// <para>Proven once by hand against the pre-#4234 text: reverting <see cref="LocalDataService.WaitTrendsSql"/> /
/// <c>PerfmonTrendsSql</c> to the per-collection SELECT (no <c>rated</c> CTE, no <c>time_bucket</c>, no width
/// parameter) fails <see cref="LiteTrendBucketWidthSqlTests"/>'s source checks directly, and fails
/// <see cref="LiteTrendBucketingLiveTests.WaitTrend_SevenDayWindow_ReturnsAtMostBudgetTimesSeriesRows"/> /
/// its Perfmon twin (10,080 one-minute collections over 7 days is 10,080 rows per series, not ≤ 1,500).</para>
/// </summary>
public sealed class LiteTrendBucketWidthSqlTests
{
    [Fact]
    public void WaitTrendsSql_CarriesABucketWidth()
    {
        /* server/start/end = $1-$3, 2 wait types = $4-$5, so the width is $6. */
        var sql = LocalDataService.WaitTrendsSql(2);
        Assert.Contains("time_bucket(to_minutes(CAST($6 AS INTEGER))", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PerfmonTrendsSql_CarriesABucketWidth()
    {
        var sql = LocalDataService.PerfmonTrendsSql(2);
        Assert.Contains("time_bucket(to_minutes(CAST($6 AS INTEGER))", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitTrendsSql_And_PerfmonTrendsSql_ProjectFirstCollectionTimeAndCollectionCount()
    {
        foreach (var sql in new[] { LocalDataService.WaitTrendsSql(1), LocalDataService.PerfmonTrendsSql(1) })
        {
            Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
            Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// <see cref="LiteNameListCache"/>'s hit/miss rules, exercised directly — no store needed since the cache never
/// touches DuckDB itself. Mirrors Darling's <c>ViewerNameListCacheTests</c> (#4234, PR #4304) so the two SKUs'
/// picker caches are pinned the same way.
/// </summary>
public sealed class LiteNameListCacheTests
{
    private static readonly TimeSpan Length = TimeSpan.FromHours(24);

    [Fact]
    public void TryGet_MissesInitially()
    {
        var cache = new LiteNameListCache();
        var now = DateTime.UtcNow;
        Assert.False(cache.TryGet(1, Length, now, now, out _));
    }

    [Fact]
    public void TryGet_HitsWithinTtl_SameWindowEnd()
    {
        var cache = new LiteNameListCache();
        var fetchedAt = new DateTime(2026, 3, 1, 12, 0, 0);
        var end = new DateTime(2026, 3, 1, 11, 0, 0);
        var names = new List<string> { "CXPACKET" };
        cache.Set(1, Length, end, names, fetchedAt);

        /* The ruling's own pin: a second refresh inside 15 minutes runs no DISTINCT — this IS that refresh,
           proven at the mechanism that prevents the query: TryGet returns the cached list. */
        Assert.True(cache.TryGet(1, Length, end, fetchedAt.AddMinutes(14), out var cached));
        Assert.Same(names, cached);
    }

    [Fact]
    public void TryGet_MissesAtFifteenMinutes_WallClockExpiry()
    {
        var cache = new LiteNameListCache();
        var fetchedAt = new DateTime(2026, 3, 1, 12, 0, 0);
        var end = new DateTime(2026, 3, 1, 11, 0, 0);
        cache.Set(1, Length, end, new List<string> { "CXPACKET" }, fetchedAt);

        /* One after 15 minutes DOES run the DISTINCT again — the ruling's other half of the same pin. */
        Assert.False(cache.TryGet(1, Length, end, fetchedAt.AddMinutes(15), out _));
    }

    [Fact]
    public void TryGet_Misses_WhenCachedWindowEndIsFarFromRequestedEnd_DespiteFreshFetch()
    {
        var cache = new LiteNameListCache();
        var now = new DateTime(2026, 3, 1, 12, 0, 0);
        cache.Set(1, TimeSpan.FromDays(7), now, new List<string> { "CXPACKET" }, now);

        var lastMonthsEnd = now.AddDays(-30);
        Assert.False(cache.TryGet(1, TimeSpan.FromDays(7), lastMonthsEnd, now, out _));
    }

    [Fact]
    public void TryGet_Hits_WhenRequestedEndIsSlightlyBeforeCachedEnd_TwoSidedCheck()
    {
        var cache = new LiteNameListCache();
        var now = new DateTime(2026, 3, 1, 12, 0, 0);
        cache.Set(1, Length, now, new List<string> { "CXPACKET" }, now);

        Assert.True(cache.TryGet(1, Length, now.AddMinutes(-5), now, out _));
    }
}

/// <summary>
/// The ruling's remaining two tests (the 7-day row-budget cap, and point equality when the budget covers every
/// collection) plus the pickers' end-to-end cache behavior, against a real embedded DuckDB.
/// </summary>
/* GetWaitStatsTrendsByTypesAsync / GetPerfmonTrendsByCountersAsync's fromDate/toDate overload is server-local
   (GetTimeRange converts it back to UTC through ServerTimeHelper.UtcOffsetMinutes, a process-wide mutable
   static) — every fromDate/toDate this class passes is meant as an exact UTC instant, so the offset is pinned
   to 0 for the class's lifetime. xUnit runs test classes in parallel, so this joins the collection the other
   offset-touching classes use rather than racing them. */
[Collection("server-time-helper")]
public sealed class LiteTrendBucketingLiveTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4234300;
    private const string ServerName = "trend-bucketing-e2e";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _savedUtcOffsetMinutes;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public LiteTrendBucketingLiveTests(SharedDuckDbFixture fixture)
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
    public async Task WaitTrend_SevenDayWindow_ReturnsAtMostBudgetTimesSeriesRows()
    {
        var end = new DateTime(2026, 3, 10, 0, 0, 0);
        var start = end.AddDays(-7);
        var waitTypes = new List<string> { "CXPACKET", "WRITELOG" };

        foreach (var waitType in waitTypes)
        {
            await BulkSeedWaitAsync(start, end, waitType);
        }

        var trends = await _dataService.GetWaitStatsTrendsByTypesAsync(ServerId, waitTypes, fromDate: start, toDate: end);

        var totalRows = trends.Values.Sum(list => list.Count);
        var budget = TrendBudget.Chart.AutoPoints * waitTypes.Count;
        Assert.True(totalRows > 0 && totalRows <= budget, $"{totalRows} rows over a {waitTypes.Count}-series budget of {budget}");
    }

    [Fact]
    public async Task PerfmonTrend_SevenDayWindow_ReturnsAtMostBudgetTimesSeriesRows()
    {
        var end = new DateTime(2026, 3, 10, 0, 0, 0);
        var start = end.AddDays(-7);
        var counters = new List<string> { "Batch Requests/sec", "Page life expectancy" };

        foreach (var counter in counters)
        {
            await BulkSeedPerfmonAsync(start, end, counter);
        }

        var trends = await _dataService.GetPerfmonTrendsByCountersAsync(ServerId, counters, fromDate: start, toDate: end);

        var totalRows = trends.Values.Sum(list => list.Count);
        var budget = TrendBudget.Chart.AutoPoints * counters.Count;
        Assert.True(totalRows > 0 && totalRows <= budget, $"{totalRows} rows over a {counters.Count}-series budget of {budget}");
    }

    [Fact]
    public async Task WaitTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        /* Off the minute grid (:37 seconds) — on the pre-#4234 bucketed code every point would have been
           floored to the time_bucket grid line, losing the seconds. */
        var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);

        await SeedWaitAsync(t1, "CXPACKET", deltaMs: 300, deltaSignal: 30, deltaTasks: 10, interval: 300);
        await SeedWaitAsync(t2, "CXPACKET", deltaMs: 600, deltaSignal: 60, deltaTasks: 20, interval: 300);
        await SeedWaitAsync(t3, "CXPACKET", deltaMs: 900, deltaSignal: 90, deltaTasks: 30, interval: 300);

        var trends = await _dataService.GetWaitStatsTrendsByTypesAsync(
            ServerId, new List<string> { "CXPACKET" }, fromDate: t1.AddMinutes(-1), toDate: t3.AddMinutes(1));

        var points = trends["CXPACKET"];
        Assert.Equal(new[] { t1, t2, t3 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(1.0, points[0].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(2.0, points[1].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(3.0, points[2].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(30.0, points[0].AvgMsPerWait, precision: 6);
        Assert.Equal(30.0, points[1].AvgMsPerWait, precision: 6);
    }

    [Fact]
    public async Task PerfmonTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);

        await SeedPerfmonAsync(t1, "Batch Requests/sec", cntr: 1000, delta: 300, interval: 300);
        await SeedPerfmonAsync(t2, "Batch Requests/sec", cntr: 1300, delta: 600, interval: 300);
        await SeedPerfmonAsync(t3, "Batch Requests/sec", cntr: 1900, delta: 900, interval: 300);

        var trends = await _dataService.GetPerfmonTrendsByCountersAsync(
            ServerId, new List<string> { "Batch Requests/sec" }, fromDate: t1.AddMinutes(-1), toDate: t3.AddMinutes(1));

        var points = trends["Batch Requests/sec"];
        Assert.Equal(new[] { t1, t2, t3 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(new long[] { 1000, 1300, 1900 }, points.Select(p => p.Value).ToArray());
        Assert.Equal(new long?[] { 300, 600, 900 }, points.Select(p => p.DeltaValue).ToArray());
        Assert.Equal(new long?[] { 300, 300, 300 }, points.Select(p => p.SampleIntervalSeconds).ToArray());
    }

    /// <summary>The ruling's picker test: a second call inside 15 minutes runs no DISTINCT (the store gains a
    /// second wait type, but a within-TTL call still answers with the cached one-type list), and a call past
    /// the TTL reruns it and picks up the change. Only the picker entry point takes <c>nowUtc</c> / caches at
    /// all — <see cref="LocalDataService.GetDistinctWaitTypesAsync"/> itself stays uncached for MCP.</summary>
    [Fact]
    public async Task DistinctWaitTypesForPicker_SecondCallInsideTtl_SkipsTheReread_MissesPastTtl()
    {
        var end = new DateTime(2026, 3, 1, 12, 30, 0);
        await SeedWaitAsync(end.AddMinutes(-5), "CXPACKET", deltaMs: 100, deltaSignal: 10, deltaTasks: 5, interval: 60);

        var first = await _dataService.GetDistinctWaitTypesForPickerAsync(ServerId, hoursBack: 1, asOfUtc: end, nowUtc: end);
        Assert.Equal(new[] { "CXPACKET" }, first);

        await SeedWaitAsync(end.AddMinutes(-5), "WRITELOG", deltaMs: 50, deltaSignal: 5, deltaTasks: 2, interval: 60);

        var withinTtl = await _dataService.GetDistinctWaitTypesForPickerAsync(ServerId, hoursBack: 1, asOfUtc: end, nowUtc: end.AddMinutes(14));
        Assert.Equal(new[] { "CXPACKET" }, withinTtl);

        var pastTtl = await _dataService.GetDistinctWaitTypesForPickerAsync(ServerId, hoursBack: 1, asOfUtc: end, nowUtc: end.AddMinutes(16));
        Assert.Equal(2, pastTtl.Count);
    }

    /// <summary>The Perfmon picker's same two halves of the ruling's cache pin.</summary>
    [Fact]
    public async Task DistinctPerfmonCountersForPicker_SecondCallInsideTtl_SkipsTheReread_MissesPastTtl()
    {
        var end = new DateTime(2026, 3, 1, 12, 30, 0);
        await SeedPerfmonAsync(end.AddMinutes(-5), "Batch Requests/sec", cntr: 1000, delta: 300, interval: 300);

        var first = await _dataService.GetDistinctPerfmonCountersForPickerAsync(ServerId, hoursBack: 1, asOfUtc: end, nowUtc: end);
        Assert.Equal(new[] { "Batch Requests/sec" }, first);

        await SeedPerfmonAsync(end.AddMinutes(-5), "Page life expectancy", cntr: 500, delta: null, interval: null);

        var withinTtl = await _dataService.GetDistinctPerfmonCountersForPickerAsync(ServerId, hoursBack: 1, asOfUtc: end, nowUtc: end.AddMinutes(14));
        Assert.Equal(new[] { "Batch Requests/sec" }, withinTtl);

        var pastTtl = await _dataService.GetDistinctPerfmonCountersForPickerAsync(ServerId, hoursBack: 1, asOfUtc: end, nowUtc: end.AddMinutes(16));
        Assert.Equal(2, pastTtl.Count);
    }

    /// <summary>MCP's guard: the shared read <see cref="LocalDataService.GetDistinctWaitTypesAsync"/> never
    /// caches, so two calls back to back — even inside the picker's 15-minute TTL — each re-run the DISTINCT and
    /// see whatever is in the store at call time, not a memoized snapshot from the first call.</summary>
    [Fact]
    public async Task DistinctWaitTypes_SharedMethod_NeverCaches_SecondCallSeesNewlySeededType()
    {
        var end = new DateTime(2026, 3, 1, 12, 30, 0);
        await SeedWaitAsync(end.AddMinutes(-5), "CXPACKET", deltaMs: 100, deltaSignal: 10, deltaTasks: 5, interval: 60);

        var first = await _dataService.GetDistinctWaitTypesAsync(ServerId, hoursBack: 1, asOfUtc: end);
        Assert.Equal(new[] { "CXPACKET" }, first);

        await SeedWaitAsync(end.AddMinutes(-5), "WRITELOG", deltaMs: 50, deltaSignal: 5, deltaTasks: 2, interval: 60);

        var second = await _dataService.GetDistinctWaitTypesAsync(ServerId, hoursBack: 1, asOfUtc: end);
        Assert.Equal(2, second.Count);
    }

    /// <summary>The Perfmon twin: <see cref="LocalDataService.GetDistinctPerfmonCountersAsync"/> never caches.</summary>
    [Fact]
    public async Task DistinctPerfmonCounters_SharedMethod_NeverCaches_SecondCallSeesNewlySeededCounter()
    {
        var end = new DateTime(2026, 3, 1, 12, 30, 0);
        await SeedPerfmonAsync(end.AddMinutes(-5), "Batch Requests/sec", cntr: 1000, delta: 300, interval: 300);

        var first = await _dataService.GetDistinctPerfmonCountersAsync(ServerId, hoursBack: 1, asOfUtc: end);
        Assert.Equal(new[] { "Batch Requests/sec" }, first);

        await SeedPerfmonAsync(end.AddMinutes(-5), "Page life expectancy", cntr: 500, delta: null, interval: null);

        var second = await _dataService.GetDistinctPerfmonCountersAsync(ServerId, hoursBack: 1, asOfUtc: end);
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

    private async Task SeedWaitAsync(DateTime at, string waitType, long deltaMs, long deltaSignal, long deltaTasks, int interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, $8, $9)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, waitType, deltaTasks, deltaMs, deltaSignal, interval })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedPerfmonAsync(DateTime at, string counterName, long cntr, long? delta, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO perfmon_stats
            (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
             cntr_value, delta_cntr_value, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, 'SQLServer:Test', $5, '', $6, $7, $8)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, counterName, cntr, (object?)delta ?? DBNull.Value, (object?)interval ?? DBNull.Value })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One row a minute across the whole window, through <c>generate_series</c> in a single INSERT
    /// (10,080 rows for a 7-day window) — the budget-cap tests' bulk seed. Rated throughout (a fixed 60-second
    /// interval) so the read's own point count, not an unrated-collection drop, is what the assertion checks.</summary>
    private async Task BulkSeedWaitAsync(DateTime start, DateTime end, string waitType)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3, $4, 0, 0, 0, 5, 100, 10, 60
            FROM generate_series(CAST($5 AS TIMESTAMP), CAST($6 AS TIMESTAMP), INTERVAL 1 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, waitType, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)(end - start).TotalMinutes + 2;
    }

    private async Task BulkSeedPerfmonAsync(DateTime start, DateTime end, string counterName)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO perfmon_stats
            (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
             cntr_value, delta_cntr_value, sample_interval_seconds)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3, 'SQLServer:Test', $4, '', 1000, 60, 60
            FROM generate_series(CAST($5 AS TIMESTAMP), CAST($6 AS TIMESTAMP), INTERVAL 1 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, counterName, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)(end - start).TotalMinutes + 2;
    }
}
