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
/// Lite's twin of Darling's <c>PerfmonTotalInstanceLiveTests</c>: a perfmon counter whose instances include
/// <c>_Total</c> (Transactions/sec on the Databases object) is plotted from its <c>_Total</c> row alone, because
/// that row already is the sum of the other instances and adding them again doubles every point, while a counter
/// with no <c>_Total</c> row (Page life expectancy per Buffer Node) still sums every instance. Pinned against an
/// embedded DuckDB through all three Lite perfmon trend reads: the single-counter chart read, the batched chart
/// read and the MCP's bucketed read.
/// </summary>
[Collection("server-time-helper")]
public sealed class LitePerfmonTotalInstanceLiveTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -447644763;
    private const string ServerName = "lite-perfmon-total-instance-e2e";
    private const string TransactionsPerSec = "Transactions/sec";
    private const string PageLifeExpectancy = "Page life expectancy";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _savedUtcOffsetMinutes;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public LitePerfmonTotalInstanceLiveTests(SharedDuckDbFixture fixture)
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
    public async Task ACounterWithATotalRow_PlotsTheTotal_AndACounterWithout_SumsItsInstances()
    {
        var t0 = new DateTime(2026, 3, 15, 9, 0, 0);
        var times = Enumerable.Range(0, 3).Select(i => t0.AddMinutes(i)).ToArray();

        foreach (var at in times)
        {
            /* Transactions/sec on the Databases object: _Total is 100 a minute, and the two databases it totals
               are 60 and 40. Summing all three rows reads 200. */
            await PlantAsync(at, "SQLServer:Databases", TransactionsPerSec, "_Total", 100, 100, 60, PerfmonCounterTypes.PerfCounterBulkCount);
            await PlantAsync(at, "SQLServer:Databases", TransactionsPerSec, "tenant_a", 60, 60, 60, PerfmonCounterTypes.PerfCounterBulkCount);
            await PlantAsync(at, "SQLServer:Databases", TransactionsPerSec, "tenant_b", 40, 40, 60, PerfmonCounterTypes.PerfCounterBulkCount);

            /* Page life expectancy on the Buffer Node object: two NUMA nodes, no _Total row. */
            await PlantAsync(at, "SQLServer:Buffer Node", PageLifeExpectancy, "000", 500, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
            await PlantAsync(at, "SQLServer:Buffer Node", PageLifeExpectancy, "001", 700, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
        }

        var startUtc = times[0].AddMinutes(-1);
        var endUtc = times[^1].AddMinutes(1);

        /* The single-counter chart read. */
        var single = await _dataService.GetPerfmonTrendAsync(ServerId, TransactionsPerSec, fromDate: startUtc, toDate: endUtc);
        Assert.Equal(3, single.Count);
        Assert.All(single, p => Assert.Equal(100, p.Value));
        Assert.All(single, p => Assert.Equal(100, p.DeltaValue));

        var singlePle = await _dataService.GetPerfmonTrendAsync(ServerId, PageLifeExpectancy, fromDate: startUtc, toDate: endUtc);
        Assert.Equal(3, singlePle.Count);
        Assert.All(singlePle, p => Assert.Equal(1200, p.Value));

        /* The batched chart read. A 5-minute window buckets at the ladder's narrowest width, so each collection
           is its own point. */
        var trends = await _dataService.GetPerfmonTrendsByCountersAsync(
            ServerId, new List<string> { TransactionsPerSec, PageLifeExpectancy }, fromDate: startUtc, toDate: endUtc);

        Assert.True(trends.TryGetValue(TransactionsPerSec, out var transactions), "Transactions/sec did not plot at all");
        Assert.Equal(3, transactions!.Count);
        Assert.All(transactions, p => Assert.Equal(100, p.Value));
        Assert.All(transactions, p => Assert.Equal(100, p.DeltaValue));

        Assert.True(trends.TryGetValue(PageLifeExpectancy, out var ple), "Page life expectancy did not plot at all");
        Assert.Equal(3, ple!.Count);
        Assert.All(ple, p => Assert.Equal(1200, p.Value));

        /* The MCP's bucketed read, one bucket per collection. */
        var buckets = await _dataService.GetPerfmonBucketsAsync(ServerId, TransactionsPerSec, hoursBack: 1, asOfUtc: endUtc, bucketMinutes: 1);
        Assert.Equal(3, buckets.Points.Count);
        Assert.All(buckets.Points, p => Assert.Equal(100, p.MaxValue));
        Assert.All(buckets.Points, p => Assert.Equal(100, p.RatedDelta));

        var pleBuckets = await _dataService.GetPerfmonBucketsAsync(ServerId, PageLifeExpectancy, hoursBack: 1, asOfUtc: endUtc, bucketMinutes: 1);
        Assert.Equal(3, pleBuckets.Points.Count);
        Assert.All(pleBuckets.Points, p => Assert.Equal(1200, p.MaxValue));
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

    private async Task PlantAsync(DateTime at, string objectName, string counterName, string instanceName, long value, long? delta, int? intervalSeconds, int cntrType)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO perfmon_stats
            (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
             cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        foreach (var v in new object[]
        {
            _nextId--, at, ServerId, ServerName, objectName, counterName, instanceName, value,
            (object?)delta ?? DBNull.Value, (object?)intervalSeconds ?? DBNull.Value, cntrType
        })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }
}
