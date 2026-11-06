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
/// with no <c>_Total</c> row (the Buffer Node object's per-node instances) still sums every instance. Pinned
/// against a live store through both Darling perfmon trend reads: the Viewer's batched chart read
/// (<see cref="ViewerDataService.GetPerfmonTrendsByCountersAsync"/>) and the MCP's bucketed read
/// (<see cref="DarlingTrendReader.GetPerfmonBucketsAsync"/>).
/// </summary>
[Collection("live-postgres")]
public sealed class PerfmonTotalInstanceLiveTests
{
    private const int ServerId = -447644762;
    private const string ServerName = "perfmon-total-instance-e2e";
    private const string TransactionsPerSec = "Transactions/sec";
    private const string PageLifeExpectancy = "Page life expectancy";

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
