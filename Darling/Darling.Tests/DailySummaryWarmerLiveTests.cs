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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8370: after a service start the closed-day block of <c>get_daily_summary_range</c> is built by the warmer, not by a
/// server's first visitor. The cache starts cold, the warmer runs one pass over two seeded servers, and the first
/// <c>get_daily_summary_range</c> call for each is a cache hit for the closed days: a wait row planted on a closed day
/// AFTER the pass is not in the answer, exactly as a second read within a block's lifetime does not see one (#4232).
/// Gated on DARLING_TEST_PG.
/// </summary>
[Collection("live-postgres")]
public sealed class DailySummaryWarmerLiveTests
{
    private const string ServerA = "daily-summary-warmer-a";
    private const string ServerB = "daily-summary-warmer-b";
    private static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerA);
    private static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerB);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WarmerPass_MakesEachServersFirstRangeRead_ACacheHitForTheClosedDays()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerB, ct);

            var today = DateTime.UtcNow.Date;
            var closedDay = today.AddDays(-2); // well past the two-hour grace: closed on every read below
            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(10), 1000, ct);
            await PlantWaitAsync(connection, ServerIdB, ServerB, closedDay.AddHours(10), 3000, ct);

            /* Cold, as after a service start. */
            DarlingHealthReader.ResetRangeCacheForTests();

            var warmer = Warmers.Create(
                servers: () => [(ServerIdA, ServerA), (ServerIdB, ServerB)],
                warm: (serverId, name, token) => Warmers.WarmAsync(dataSource, serverId, name, token),
                enabled: () => true,
                admission: null);
            await Warmers.RunPassAsync(warmer, ct);

            /* Land a second row on the closed day of each server AFTER the pass. A visitor whose first read is served
               from the warmed block does not see it; a cold first read does. */
            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(11), 4000, ct);
            await PlantWaitAsync(connection, ServerIdB, ServerB, closedDay.AddHours(11), 4000, ct);

            Assert.Equal(1000m, await ClosedDayWaitMsAsync(dataSource, ServerA, closedDay));
            Assert.Equal(3000m, await ClosedDayWaitMsAsync(dataSource, ServerB, closedDay));

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#8451: the service's own shape. The warmer reads through the worker's pool, and a visitor's
    /// <c>get_daily_summary_range</c> goes through the web host's own pool over the same store, built by the product's
    /// host-pool constructor with the web host's application name. The block the pass built must serve that first read: a
    /// block the web host's read does not find leaves every server's first visitor on the cold read, warmer or not.</summary>
    [Fact]
    public async Task WarmerPass_ThroughTheWorkersPool_ServesTheWebHostsFirstRangeRead()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var workerPool = NpgsqlDataSource.Create(cs!);
        await using var webPool = DarlingWorker.CreateHostStoreDataSource(cs!, DarlingManagedPostgres.WebApplicationName);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerB, ct);

            var today = DateTime.UtcNow.Date;
            var closedDay = today.AddDays(-2);
            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(10), 1000, ct);
            await PlantWaitAsync(connection, ServerIdB, ServerB, closedDay.AddHours(10), 3000, ct);

            DarlingHealthReader.ResetRangeCacheForTests();

            var warmer = Warmers.Create(
                servers: () => [(ServerIdA, ServerA), (ServerIdB, ServerB)],
                warm: (serverId, name, token) => Warmers.WarmAsync(workerPool, serverId, name, token),
                enabled: () => true,
                admission: null);
            await Warmers.RunPassAsync(warmer, ct);

            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(11), 4000, ct);
            await PlantWaitAsync(connection, ServerIdB, ServerB, closedDay.AddHours(11), 4000, ct);

            Assert.Equal(1000m, await ClosedDayWaitMsAsync(webPool, ServerA, closedDay));
            Assert.Equal(3000m, await ClosedDayWaitMsAsync(webPool, ServerB, closedDay));

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The closed day's total wait in ms from the FIRST <c>get_daily_summary_range</c> call this test makes for
    /// the server: the Overview's own read, 31 days ending today with no anchor.</summary>
    private static async Task<decimal> ClosedDayWaitMsAsync(NpgsqlDataSource dataSource, string serverName, DateTime closedDay)
    {
        var json = await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, serverName, 31);
        using var doc = JsonDocument.Parse(json);
        var days = doc.RootElement.GetProperty("days").EnumerateArray().ToArray();
        var row = Assert.Single(days, d => d.GetProperty("summary_date").GetString() == closedDay.ToString("yyyy-MM-dd"));
        var waitMs = row.GetProperty("total_wait_time_sec").GetDecimal() * 1000m;
        Assert.True(waitMs is 1000m or 3000m,
            $"{serverName}: the first get_daily_summary_range read after the warmer pass was a cache MISS for {closedDay:yyyy-MM-dd}: it read {waitMs} ms of wait from the store, the seeded row plus the one planted after the pass.");
        return waitMs;
    }

    private static async Task PlantWaitAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime atUtc, long waitMs, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(atUtc), serverId, serverName, "CXPACKET", 5L, waitMs);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var serverId in new[] { ServerIdA, ServerIdB })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM wait_stats WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", serverId);
        }
    }
}
