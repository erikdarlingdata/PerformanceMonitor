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
/// with no <c>_Total</c> row (Page life expectancy per Buffer Node) still sums every instance. The choice is made
/// per object: a counter name that two objects share (Transaction Delay on the Database Replica and Database
/// Mirroring objects, #7416) takes the <c>_Total</c> row of the object that has one and the instance sum of the
/// object that does not, then adds the two. Pinned against an embedded DuckDB through all three Lite perfmon
/// trend reads: the single-counter chart read, the batched chart read and the MCP's bucketed read.
/// </summary>
[Collection("server-time-helper")]
public sealed class LitePerfmonTotalInstanceLiveTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -447644763;
    private const string ServerName = "lite-perfmon-total-instance-e2e";
    private const string TransactionsPerSec = "Transactions/sec";
    private const string PageLifeExpectancy = "Page life expectancy";
    private const string TransactionDelay = "Transaction Delay";
    private const string LockWaits = "Lock waits";

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
        /* The interval is one sweep gap repeated on each of the three instance rows (#2234): MAX reads 60, a SUM over
           them would read 180. */
        Assert.All(single, p => Assert.Equal(60, p.SampleIntervalSeconds));
        /* Every row under the name agrees on the rate type, so the point carries it. */
        Assert.All(single, p => Assert.Equal(PerfmonCounterTypes.PerfCounterBulkCount, p.CntrType));

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
        Assert.All(transactions, p => Assert.Equal(60, p.SampleIntervalSeconds));
        Assert.All(transactions, p => Assert.Equal(PerfmonCounterTypes.PerfCounterBulkCount, p.CntrType));

        Assert.True(trends.TryGetValue(PageLifeExpectancy, out var ple), "Page life expectancy did not plot at all");
        Assert.Equal(3, ple!.Count);
        Assert.All(ple, p => Assert.Equal(1200, p.Value));

        /* The MCP's bucketed read, one bucket per collection. */
        var buckets = await _dataService.GetPerfmonBucketsAsync(ServerId, TransactionsPerSec, hoursBack: 1, asOfUtc: endUtc, bucketMinutes: 1);
        Assert.Equal(3, buckets.Points.Count);
        Assert.All(buckets.Points, p => Assert.Equal(100, p.MaxValue));
        Assert.All(buckets.Points, p => Assert.Equal(100, p.RatedDelta));
        /* One collection per bucket, so the bucket's rated seconds is that collection's interval alone. */
        Assert.All(buckets.Points, p => Assert.Equal(60, p.RatedSeconds));
        Assert.All(buckets.Points, p => Assert.Equal(PerfmonCounterTypes.PerfCounterBulkCount, p.CntrType));

        var pleBuckets = await _dataService.GetPerfmonBucketsAsync(ServerId, PageLifeExpectancy, hoursBack: 1, asOfUtc: endUtc, bucketMinutes: 1);
        Assert.Equal(3, pleBuckets.Points.Count);
        Assert.All(pleBuckets.Points, p => Assert.Equal(1200, p.MaxValue));
    }

    /// <summary>One counter name on two objects, only one of which carries <c>_Total</c>: the point is that
    /// object's <c>_Total</c> plus the other object's instance sum. A choice made per counter name would see
    /// the one <c>_Total</c> row and drop the second object's rows altogether.</summary>
    [Fact]
    public async Task ACounterSharedByTwoObjects_TakesTheTotalOfTheOneThatHasIt_AndSumsTheOther()
    {
        var t0 = new DateTime(2026, 3, 15, 9, 0, 0);
        var times = Enumerable.Range(0, 3).Select(i => t0.AddMinutes(i)).ToArray();

        foreach (var at in times)
        {
            /* Transaction Delay is a gauge (no delta, no interval). The Database Replica object carries _Total = 100
               beside the two databases it totals, 60 and 40; the Database Mirroring object carries two databases,
               30 and 20, and no _Total. The point is 100 + 50 = 150. Summing every row reads 250; taking the one
               _Total row for the whole counter name reads 100. */
            await PlantAsync(at, "SQLServer:Database Replica", TransactionDelay, "_Total", 100, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
            await PlantAsync(at, "SQLServer:Database Replica", TransactionDelay, "tenant_a", 60, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
            await PlantAsync(at, "SQLServer:Database Replica", TransactionDelay, "tenant_b", 40, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
            await PlantAsync(at, "SQLServer:Database Mirroring", TransactionDelay, "tenant_c", 30, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
            await PlantAsync(at, "SQLServer:Database Mirroring", TransactionDelay, "tenant_d", 20, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
        }

        var startUtc = times[0].AddMinutes(-1);
        var endUtc = times[^1].AddMinutes(1);

        var single = await _dataService.GetPerfmonTrendAsync(ServerId, TransactionDelay, fromDate: startUtc, toDate: endUtc);
        Assert.Equal(3, single.Count);
        Assert.All(single, p => Assert.Equal(150, p.Value));
        /* A gauge's rows store no delta, and the sum across objects keeps that NULL rather than making a 0. */
        Assert.All(single, p => Assert.Null(p.DeltaValue));

        var trends = await _dataService.GetPerfmonTrendsByCountersAsync(
            ServerId, new List<string> { TransactionDelay }, fromDate: startUtc, toDate: endUtc);
        Assert.True(trends.TryGetValue(TransactionDelay, out var delay), "Transaction Delay did not plot at all");
        Assert.Equal(3, delay!.Count);
        Assert.All(delay, p => Assert.Equal(150, p.Value));
        Assert.All(delay, p => Assert.Null(p.DeltaValue));

        var buckets = await _dataService.GetPerfmonBucketsAsync(ServerId, TransactionDelay, hoursBack: 1, asOfUtc: endUtc, bucketMinutes: 1);
        Assert.Equal(3, buckets.Points.Count);
        Assert.All(buckets.Points, p => Assert.Equal(150, p.MaxValue));
    }

    /// <summary>One counter name on two objects, where one object's instances carry different types and the other's
    /// agree on one: the point has no type. The type is decided across every instance row summed into the point, so
    /// one object's agreement cannot type a sum that another object's mixed rows are part of. The Wait Statistics
    /// object is the real mixed family (a rate, a gauge and an average under one counter name). The second object
    /// stands for any object that shares the name (#7416): the shape is what is pinned, not the catalogue.</summary>
    [Fact]
    public async Task AMixedTypeObjectBesideAUniformOne_LeavesThePointUntyped()
    {
        var t0 = new DateTime(2026, 3, 15, 9, 0, 0);
        var times = Enumerable.Range(0, 3).Select(i => t0.AddMinutes(i)).ToArray();

        foreach (var at in times)
        {
            /* Lock waits on the Wait Statistics object: a rate, a gauge and an average, 10 + 20 + 30 = 60, no _Total.
               The second object agrees on the rate type, 5 + 7 = 12, no _Total. The point is 72 and has no type:
               typing it by the one object that agrees would call the whole sum a rate. */
            await PlantAsync(at, "SQLServer:Wait Statistics", LockWaits, "Waits started per second", 10, 10, 60, PerfmonCounterTypes.PerfCounterBulkCount);
            await PlantAsync(at, "SQLServer:Wait Statistics", LockWaits, "Waits in progress", 20, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
            await PlantAsync(at, "SQLServer:Wait Statistics", LockWaits, "Average wait time (ms)", 30, null, null, PerfmonCounterTypes.PerfAverageBulk);
            await PlantAsync(at, "SQLServer:Locks", LockWaits, "tenant_a", 5, 5, 60, PerfmonCounterTypes.PerfCounterBulkCount);
            await PlantAsync(at, "SQLServer:Locks", LockWaits, "tenant_b", 7, 7, 60, PerfmonCounterTypes.PerfCounterBulkCount);
        }

        var startUtc = times[0].AddMinutes(-1);
        var endUtc = times[^1].AddMinutes(1);

        var single = await _dataService.GetPerfmonTrendAsync(ServerId, LockWaits, fromDate: startUtc, toDate: endUtc);
        Assert.Equal(3, single.Count);
        Assert.All(single, p => Assert.Equal(72, p.Value));
        Assert.All(single, p => Assert.Null(p.CntrType));

        var trends = await _dataService.GetPerfmonTrendsByCountersAsync(
            ServerId, new List<string> { LockWaits }, fromDate: startUtc, toDate: endUtc);
        Assert.True(trends.TryGetValue(LockWaits, out var waits), "Lock waits did not plot at all");
        Assert.Equal(3, waits!.Count);
        Assert.All(waits, p => Assert.Equal(72, p.Value));
        Assert.All(waits, p => Assert.Null(p.CntrType));

        var buckets = await _dataService.GetPerfmonBucketsAsync(ServerId, LockWaits, hoursBack: 1, asOfUtc: endUtc, bucketMinutes: 1);
        Assert.Equal(3, buckets.Points.Count);
        Assert.All(buckets.Points, p => Assert.Equal(72, p.MaxValue));
        Assert.All(buckets.Points, p => Assert.Null(p.CntrType));
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
