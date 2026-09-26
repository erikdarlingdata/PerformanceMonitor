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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4394 part B2 live pin: the Viewer's raw <c>v_query_stats</c> FinOps sums must not count a plan's
/// first-collection (zero-interval) row, the same rule the hourly successors already bake into their CREATE
/// (<see cref="TimescaleSupport.IntervalHonestSourceFilter"/>). Before this change raw and hourly disagreed
/// on the same window whenever a zero-interval row carried a nonzero delta. The collector doesn't write
/// that shape (a zero interval comes with zero deltas), but planting one is what makes the filter
/// observable. Seeds one ordinary row plus one zero-interval row with a deliberately large CPU value, then
/// asserts the large value is absent from every affected reader's totals.
/// #1776 own-store: this test seeds and cleans up its own server_id row set only.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerFinOpsIntervalHonestLiveTests
{
    private const string ServerName = "darling-finops-interval-honest-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string DbName = "IntervalHonestDb";

    /* The zero-interval row's CPU is set far above the ordinary row's so any leak into a sum is unmissable. */
    private const long ZeroIntervalCpuUs = 50_000_000L;
    private const long OrdinaryCpuUs = 100_000L;

    [Fact]
    public async Task RawFinOpsSums_ExcludeTheZeroIntervalRow_AcrossEveryAffectedReader()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps interval-honest test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-1);

            /* A planted zero-interval row with a nonzero delta. The collector doesn't write this shape
               (a zero interval comes with zero deltas), but planting it is what makes the filter
               observable: without the filter, raw would count it and hourly would not. */
            await PlantQueryAsync(connection, ct, at, sampleIntervalSeconds: 0, cpuUs: ZeroIntervalCpuUs, execCount: 1L);
            /* An ordinary row: normal delta over a real 60s sample interval. */
            await PlantQueryAsync(connection, ct, at, sampleIntervalSeconds: 60, cpuUs: OrdinaryCpuUs, execCount: 10L);

            var resourceUsage = await viewer.GetDatabaseResourceUsageAsync(ServerId, hoursBack: 24, ct);
            var row = Assert.Single(resourceUsage.Where(r => r.DatabaseName == DbName));
            Assert.Equal(OrdinaryCpuUs / 1000L, row.CpuTimeMs);

            var (byTotal, byAvg) = await viewer.GetTopResourceConsumersAsync(ServerId, hoursBack: 24, topN: 10, ct);
            var totalRow = Assert.Single(byTotal.Where(r => r.DatabaseName == DbName));
            Assert.Equal(OrdinaryCpuUs / 1000L, totalRow.CpuTimeMs);
            var avgRow = Assert.Single(byAvg.Where(r => r.DatabaseName == DbName));
            Assert.Equal(OrdinaryCpuUs / 1000L, avgRow.TotalCpuTimeMs);

            var expensive = await viewer.GetExpensiveQueriesAsync(ServerId, hoursBack: 24, topN: 20, ct);
            Assert.DoesNotContain(expensive, q => q.TotalCpuMs >= ZeroIntervalCpuUs / 1000L);
            var expensiveRow = Assert.Single(expensive.Where(q => q.DatabaseName == DbName));
            Assert.Equal(OrdinaryCpuUs / 1000L, expensiveRow.TotalCpuMs);

            var highImpact = await viewer.GetHighImpactQueriesAsync(ServerId, hoursBack: 24, ct);
            Assert.DoesNotContain(highImpact, q => q.TotalCpuMs >= ZeroIntervalCpuUs / 1000.0m);
            var highImpactRow = Assert.Single(highImpact.Where(q => q.DatabaseName == DbName));
            Assert.Equal(OrdinaryCpuUs / 1000.0m, highImpactRow.TotalCpuMs);

            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// A database whose ONLY in-window row is the zero-interval (first-collection) row, with a recent
    /// <c>last_execution_time</c>, must stay non-idle: the interval-honest filter drops the row from the
    /// execution-count SUM only, never from <c>MAX(last_execution_time)</c>, which is the only evidence of
    /// activity the idle test reads.
    /// </summary>
    [Fact]
    public async Task IdleDatabases_StaysNonIdle_WhenTheOnlyInWindowRowIsZeroInterval()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps interval-honest test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-1);

            /* One database with a recent size row and its ONLY query_stats row being zero-interval. */
            await PlantSizeAsync(connection, ct, at, DbName);
            await PlantQueryAsync(connection, ct, at, sampleIntervalSeconds: 0, cpuUs: ZeroIntervalCpuUs, execCount: 1L, lastExecutionTime: at);

            var idle = await viewer.GetIdleDatabasesAsync(ServerId, daysBack: 7, ct);

            Assert.DoesNotContain(idle, r => r.DatabaseName == DbName);

            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantQueryAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at,
        int sampleIntervalSeconds, long cpuUs, long execCount, DateTime? lastExecutionTime = null)
    {
        var queryHash = "0xQH" + Guid.NewGuid().ToString("N")[..12];
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, delta_execution_count, delta_worker_time, sample_interval_seconds,
                                       last_execution_time, query_text)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, DbName,
            queryHash, execCount, cpuUs, sampleIntervalSeconds,
            (object?)lastExecutionTime ?? at, "SELECT 1;");
    }

    private static async Task PlantSizeAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at, string databaseName)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name,
                                                total_size_mb)
              VALUES ($1,$2,$3,$4,$5,$6)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, databaseName, 100m);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM query_stats WHERE server_id = {ServerId}");
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM database_size_stats WHERE server_id = {ServerId}");
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM servers WHERE server_id = {ServerId}");
    }
}
