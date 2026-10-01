/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Storage Growth grid compares each database's current size with its size 7 and 30 days ago. A database with
/// no row in a past snapshot has no growth to report for that window, so the growth, the daily rate and the percent
/// read as unknown (null, shown as n/a), never as 0. A database with history reads as it always did.
/// </summary>
[Collection("live-postgres")]
public sealed class StorageGrowthNoHistoryLiveTests
{
    private const string ServerName = "darling-storage-growth-no-history";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task ADatabaseWithNoPastRowReadsGrowthAsUnknown_NotZero()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live storage growth test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteRowsAsync(connection, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* fresh: only the newest snapshot. weekold: also a snapshot 8 days back. full: snapshots 31 and 8 days back. */
            await SeedAsync(connection, "fresh", now, 500m, ct);
            await SeedAsync(connection, "weekold", now, 500m, ct);
            await SeedAsync(connection, "weekold", now.AddDays(-8), 400m, ct);
            await SeedAsync(connection, "full", now, 500m, ct);
            await SeedAsync(connection, "full", now.AddDays(-8), 450m, ct);
            await SeedAsync(connection, "full", now.AddDays(-31), 300m, ct);

            await using var viewer = new ViewerDataService(cs!);
            var rows = await viewer.GetStorageGrowthAsync(ServerId, ct);

            var fresh = Assert.Single(rows, r => r.DatabaseName == "fresh");
            Assert.Equal(500m, fresh.CurrentSizeMb);
            Assert.Null((object?)fresh.Growth7dMb);
            Assert.Null((object?)fresh.Growth30dMb);
            Assert.Null((object?)fresh.DailyGrowthRateMb);
            Assert.Null((object?)fresh.GrowthPct30d);

            var weekOld = Assert.Single(rows, r => r.DatabaseName == "weekold");
            Assert.Equal(100m, (decimal?)weekOld.Growth7dMb);
            Assert.Null((object?)weekOld.Growth30dMb);
            Assert.Equal(100m / 7m, weekOld.DailyGrowthRateMb!.Value, 4);
            Assert.Null((object?)weekOld.GrowthPct30d);

            var full = Assert.Single(rows, r => r.DatabaseName == "full");
            Assert.Equal(50m, (decimal?)full.Growth7dMb);
            Assert.Equal(200m, (decimal?)full.Growth30dMb);
            Assert.Equal(200m / 30m, full.DailyGrowthRateMb!.Value, 4);
            Assert.Equal(200m * 100m / 300m, full.GrowthPct30d!.Value, 4);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task SeedAsync(NpgsqlConnection connection, string database, DateTime stamp, decimal totalMb, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(@"
INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb, max_size_mb, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)", connection);
        cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
        cmd.Parameters.AddWithValue(Naive(stamp));
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(ServerName);
        cmd.Parameters.AddWithValue(database);
        cmd.Parameters.AddWithValue(5);
        cmd.Parameters.AddWithValue(1);
        cmd.Parameters.AddWithValue("ROWS");
        cmd.Parameters.AddWithValue(database + ".mdf");
        cmd.Parameters.AddWithValue("D:\\" + database + ".mdf");
        cmd.Parameters.AddWithValue(totalMb);
        cmd.Parameters.AddWithValue(totalMb - 10m);
        cmd.Parameters.AddWithValue(-1m);
        cmd.Parameters.AddWithValue("D:\\");
        cmd.Parameters.AddWithValue(5000m);
        cmd.Parameters.AddWithValue(2500m);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using (var cmd = new NpgsqlCommand("DELETE FROM database_size_stats WHERE server_id = $1;", connection))
        {
            cmd.Parameters.AddWithValue(ServerId);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        using (var cmd = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1;", connection))
        {
            cmd.Parameters.AddWithValue(ServerId);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
