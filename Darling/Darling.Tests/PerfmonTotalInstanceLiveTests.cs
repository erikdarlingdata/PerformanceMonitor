/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A perfmon counter whose instances include <c>_Total</c> (the Databases and Locks objects: Transactions/sec,
/// Log Flushes/sec, Lock Requests/sec and the rest) is plotted from its <c>_Total</c> row alone, because
/// <c>_Total</c> already is the sum of the other instances and adding them again doubles every point. A counter
/// with no <c>_Total</c> row (the Buffer Node object's per-node instances) still sums every instance. The choice
/// is made per object: a counter name that two objects share (Transaction Delay on the Database Replica and
/// Database Mirroring objects, #7416) takes the <c>_Total</c> row of the object that has one and the instance sum
/// of the object that does not, then adds the two. Pinned against a live store through both Darling perfmon
/// trend reads: the Viewer's batched chart read (<see cref="ViewerDataService.GetPerfmonTrendsByCountersAsync"/>)
/// and the MCP's bucketed read (<see cref="DarlingTrendReader.GetPerfmonBucketsAsync"/>).
/// </summary>
[Collection("live-postgres")]
public sealed class PerfmonTotalInstanceLiveTests
{
    private const int ServerId = -447644762;
    private const string ServerName = "perfmon-total-instance-e2e";
    private const string TransactionsPerSec = "Transactions/sec";
    private const string PageLifeExpectancy = "Page life expectancy";
    private const string TransactionDelay = "Transaction Delay";
    private const string LockWaits = "Lock waits";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ACounterWithATotalRow_PlotsTheTotal_AndACounterWithout_SumsItsInstances_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the perfmon _Total round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var t0 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
            var times = Enumerable.Range(0, 3).Select(i => t0.AddMinutes(i)).ToArray();

            foreach (var at in times)
            {
                /* Transactions/sec on the Databases object: _Total is 100 a minute, and the two databases it
                   totals are 60 and 40. Summing all three rows reads 200. */
                await PlantAsync(connection, ct, at, "SQLServer:Databases", TransactionsPerSec, "_Total", 100, 100, 60, PerfmonCounterTypes.PerfCounterBulkCount);
                await PlantAsync(connection, ct, at, "SQLServer:Databases", TransactionsPerSec, "tenant_a", 60, 60, 60, PerfmonCounterTypes.PerfCounterBulkCount);
                await PlantAsync(connection, ct, at, "SQLServer:Databases", TransactionsPerSec, "tenant_b", 40, 40, 60, PerfmonCounterTypes.PerfCounterBulkCount);

                /* Page life expectancy on the Buffer Node object: two NUMA nodes, no _Total row. */
                await PlantAsync(connection, ct, at, "SQLServer:Buffer Node", PageLifeExpectancy, "000", 500, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
                await PlantAsync(connection, ct, at, "SQLServer:Buffer Node", PageLifeExpectancy, "001", 700, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
            }

            var startUtc = times[0].AddMinutes(-1);
            var endUtc = times[^1].AddMinutes(1);

            /* The Viewer's chart read. A 5-minute window buckets at the ladder's narrowest width (1 minute), so
               each collection is its own point. */
            await using (var viewer = new ViewerDataService(cs!))
            {
                var trends = await viewer.GetPerfmonTrendsByCountersAsync(
                    ServerId, new List<string> { TransactionsPerSec, PageLifeExpectancy }, startUtc, endUtc, ct);

                Assert.True(trends.TryGetValue(TransactionsPerSec, out var transactions), "Transactions/sec did not plot at all");
                Assert.Equal(3, transactions!.Count);
                Assert.All(transactions, p => Assert.Equal(100, p.Value));
                Assert.All(transactions, p => Assert.Equal(100, p.DeltaValue));
                /* Every row under the name agrees on the rate type, so the point carries it. */
                Assert.All(transactions, p => Assert.Equal(PerfmonCounterTypes.PerfCounterBulkCount, p.CntrType));

                Assert.True(trends.TryGetValue(PageLifeExpectancy, out var ple), "Page life expectancy did not plot at all");
                Assert.Equal(3, ple!.Count);
                Assert.All(ple, p => Assert.Equal(1200, p.Value));
            }

            /* The MCP's bucketed read, one bucket per collection. */
            await using (var postgres = NpgsqlDataSource.Create(cs!))
            {
                var transactions = await DarlingTrendReader.GetPerfmonBucketsAsync(postgres, ServerId, TransactionsPerSec, startUtc, endUtc, bucketMinutes: 1, ct);
                Assert.Equal(3, transactions.Points.Count);
                Assert.All(transactions.Points, p => Assert.Equal(100, p.MaxValue));
                Assert.All(transactions.Points, p => Assert.Equal(100, p.RatedDelta));
                Assert.All(transactions.Points, p => Assert.Equal(PerfmonCounterTypes.PerfCounterBulkCount, p.CntrType));

                var ple = await DarlingTrendReader.GetPerfmonBucketsAsync(postgres, ServerId, PageLifeExpectancy, startUtc, endUtc, bucketMinutes: 1, ct);
                Assert.Equal(3, ple.Points.Count);
                Assert.All(ple.Points, p => Assert.Equal(1200, p.MaxValue));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>One counter name on two objects, only one of which carries <c>_Total</c>: the point is that
    /// object's <c>_Total</c> plus the other object's instance sum. A choice made per counter name would see
    /// the one <c>_Total</c> row and drop the second object's rows altogether.</summary>
    [Fact]
    public async Task ACounterSharedByTwoObjects_TakesTheTotalOfTheOneThatHasIt_AndSumsTheOther_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the perfmon _Total round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var t0 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
            var times = Enumerable.Range(0, 3).Select(i => t0.AddMinutes(i)).ToArray();

            foreach (var at in times)
            {
                /* Transaction Delay is a gauge (no delta, no interval). The Database Replica object carries _Total
                   = 100 beside the two databases it totals, 60 and 40; the Database Mirroring object carries two
                   databases, 30 and 20, and no _Total. The point is 100 + 50 = 150. Summing every row reads 250;
                   taking the one _Total row for the whole counter name reads 100. */
                await PlantAsync(connection, ct, at, "SQLServer:Database Replica", TransactionDelay, "_Total", 100, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
                await PlantAsync(connection, ct, at, "SQLServer:Database Replica", TransactionDelay, "tenant_a", 60, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
                await PlantAsync(connection, ct, at, "SQLServer:Database Replica", TransactionDelay, "tenant_b", 40, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
                await PlantAsync(connection, ct, at, "SQLServer:Database Mirroring", TransactionDelay, "tenant_c", 30, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
                await PlantAsync(connection, ct, at, "SQLServer:Database Mirroring", TransactionDelay, "tenant_d", 20, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
            }

            var startUtc = times[0].AddMinutes(-1);
            var endUtc = times[^1].AddMinutes(1);

            await using (var viewer = new ViewerDataService(cs!))
            {
                var trends = await viewer.GetPerfmonTrendsByCountersAsync(
                    ServerId, new List<string> { TransactionDelay }, startUtc, endUtc, ct);

                Assert.True(trends.TryGetValue(TransactionDelay, out var delay), "Transaction Delay did not plot at all");
                Assert.Equal(3, delay!.Count);
                Assert.All(delay, p => Assert.Equal(150, p.Value));
                /* A gauge's rows store no delta, and the sum across objects keeps that NULL rather than making a 0. */
                Assert.All(delay, p => Assert.Null(p.DeltaValue));
            }

            await using (var postgres = NpgsqlDataSource.Create(cs!))
            {
                var delay = await DarlingTrendReader.GetPerfmonBucketsAsync(postgres, ServerId, TransactionDelay, startUtc, endUtc, bucketMinutes: 1, ct);
                Assert.Equal(3, delay.Points.Count);
                Assert.All(delay.Points, p => Assert.Equal(150, p.MaxValue));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>One counter name on two objects, where one object's instances carry different types and the other's
    /// agree on one: the point has no type. The type is decided across every instance row summed into the point, so
    /// one object's agreement cannot type a sum that another object's mixed rows are part of. The Wait Statistics
    /// object is the real mixed family (a rate, a gauge and an average under one counter name). The second object
    /// stands for any object that shares the name (#7416): the shape is what is pinned, not the catalogue.</summary>
    [Fact]
    public async Task AMixedTypeObjectBesideAUniformOne_LeavesThePointUntyped_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the perfmon _Total round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var t0 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
            var times = Enumerable.Range(0, 3).Select(i => t0.AddMinutes(i)).ToArray();

            foreach (var at in times)
            {
                /* Lock waits on the Wait Statistics object: a rate, a gauge and an average, 10 + 20 + 30 = 60, no
                   _Total. The second object agrees on the rate type, 5 + 7 = 12, no _Total. The point is 72 and has
                   no type: typing it by the one object that agrees would call the whole sum a rate. */
                await PlantAsync(connection, ct, at, "SQLServer:Wait Statistics", LockWaits, "Waits started per second", 10, 10, 60, PerfmonCounterTypes.PerfCounterBulkCount);
                await PlantAsync(connection, ct, at, "SQLServer:Wait Statistics", LockWaits, "Waits in progress", 20, null, null, PerfmonCounterTypes.PerfCounterLargeRawCount);
                await PlantAsync(connection, ct, at, "SQLServer:Wait Statistics", LockWaits, "Average wait time (ms)", 30, null, null, PerfmonCounterTypes.PerfAverageBulk);
                await PlantAsync(connection, ct, at, "SQLServer:Locks", LockWaits, "tenant_a", 5, 5, 60, PerfmonCounterTypes.PerfCounterBulkCount);
                await PlantAsync(connection, ct, at, "SQLServer:Locks", LockWaits, "tenant_b", 7, 7, 60, PerfmonCounterTypes.PerfCounterBulkCount);
            }

            var startUtc = times[0].AddMinutes(-1);
            var endUtc = times[^1].AddMinutes(1);

            await using (var viewer = new ViewerDataService(cs!))
            {
                var trends = await viewer.GetPerfmonTrendsByCountersAsync(
                    ServerId, new List<string> { LockWaits }, startUtc, endUtc, ct);

                Assert.True(trends.TryGetValue(LockWaits, out var waits), "Lock waits did not plot at all");
                Assert.Equal(3, waits!.Count);
                Assert.All(waits, p => Assert.Equal(72, p.Value));
                Assert.All(waits, p => Assert.Null(p.CntrType));
            }

            await using (var postgres = NpgsqlDataSource.Create(cs!))
            {
                var waits = await DarlingTrendReader.GetPerfmonBucketsAsync(postgres, ServerId, LockWaits, startUtc, endUtc, bucketMinutes: 1, ct);
                Assert.Equal(3, waits.Points.Count);
                Assert.All(waits.Points, p => Assert.Equal(72, p.MaxValue));
                Assert.All(waits.Points, p => Assert.Null(p.CntrType));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static Task PlantAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at,
        string objectName, string counterName, string instanceName, long value, long? delta, int? intervalSeconds, int cntrType) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, objectName, counterName, instanceName, value,
            (object?)delta ?? DBNull.Value, (object?)intervalSeconds ?? DBNull.Value, cntrType);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM perfmon_stats WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(ServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
