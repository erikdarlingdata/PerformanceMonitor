/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The Storage cost and storage-total reads equal what the viewer shows: <c>DarlingServer.MonthlyCostUsd</c> from the
/// server list, and the allocated and free totals over the latest database-size read.
/// </summary>
public sealed class FinOpsUtilizationFiguresLiveTests
{
    private const string ServerNameA = "darling-util-figures-a";
    private const string ServerNameB = "darling-util-figures-b";
    private const string ServerNameC = "darling-util-figures-c";

    [Fact]
    public async Task StorageTotalsAndMonthlyCost_EqualTheViewers()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live utilization-figures pin.");

        var ct = TestContext.Current.CancellationToken;
        var idA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
        var idB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);
        var idC = ServerIdHelper.GetDeterministicHashCode(ServerNameC);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            foreach (var (id, name, cost) in new[] { (idA, ServerNameA, (decimal?)1234.56m), (idB, ServerNameB, (decimal?)null), (idC, ServerNameC, (decimal?)10m) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, monthly_cost_usd, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, $3, $4, $4)",
                    id, name, (object?)cost ?? DBNull.Value, DarlingMcpTestData.Naive(DateTime.UtcNow));
            }

            var stamp = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5));
            var older = DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(-3));
            /* Server A at the latest snapshot: a normal file, a Hyperscale log with no total, an Azure sibling row
               (no file id), and a file with no used size. An older snapshot must not count. */
            await SeedAsync(connection, ct, idA, ServerNameA, stamp, "appdb", 1, "ROWS", 1000m, 400m);
            await SeedAsync(connection, ct, idA, ServerNameA, stamp, "appdb", 2, "LOG", null, 50m);
            await SeedAsync(connection, ct, idA, ServerNameA, stamp, "(whole database)", null, "ROWS", 128m, 23m);
            await SeedAsync(connection, ct, idA, ServerNameA, stamp, "otherdb", 3, "ROWS", 500m, null);
            await SeedAsync(connection, ct, idA, ServerNameA, older, "appdb", 1, "ROWS", 9999m, 1m);
            /* Server C has only a stale snapshot, older than the two-day window. */
            await SeedAsync(connection, ct, idC, ServerNameC, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-9)), "olddb", 1, "ROWS", 200m, 80m);

            var viewer = new ViewerDataService(scratch.ConnectionString);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            try
            {
                foreach (var id in new[] { idA, idB, idC })
                {
                    var rows = await viewer.GetDatabaseSizeLatestAsync(id, cancellationToken: ct);
                    var totals = await FinOpsUtilizationFigures.GetLatestStorageTotalsAsync(source, id, 30, ct);
                    Assert.Equal(rows.Count > 0, totals is not null);
                    if (totals is { } t)
                    {
                        Assert.Equal(DatabaseSizeRow.AllocatedTotalMb(rows), t.AllocatedMb);
                        Assert.Equal(DatabaseSizeRow.FreeTotalMb(rows), t.FreeMb);
                    }
                }

                Assert.Null(await FinOpsUtilizationFigures.GetLatestStorageTotalsAsync(source, idB, 30, ct));
                var c = await FinOpsUtilizationFigures.GetLatestStorageTotalsAsync(source, idC, 30, ct);
                Assert.Equal((200m, 120m), c!.Value);
                var a = await FinOpsUtilizationFigures.GetLatestStorageTotalsAsync(source, idA, 30, ct);
                Assert.Equal((1628m, 705m), a!.Value);

                var servers = await viewer.GetServersAsync(ct);
                foreach (var id in new[] { idA, idB, idC })
                {
                    var expected = servers.Find(s => s.ServerId == id)!.MonthlyCostUsd;
                    Assert.Equal(expected, await FinOpsUtilizationFigures.GetMonthlyCostUsdAsync(source, id, 30, ct));
                }
                Assert.Equal(1234.56m, await FinOpsUtilizationFigures.GetMonthlyCostUsdAsync(source, idA, 30, ct));
                Assert.Equal(0m, await FinOpsUtilizationFigures.GetMonthlyCostUsdAsync(source, idB, 30, ct));
                Assert.Equal(0m, await FinOpsUtilizationFigures.GetMonthlyCostUsdAsync(source, 12345, 30, ct));
            }
            finally
            {
                await viewer.DisposeAsync();
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static Task SeedAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, int serverId, string serverName,
        DateTime when, string database, int? fileId, string fileType, decimal? total, decimal? used) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id,
     file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $8, NULL, $9, $10)",
            CollectionIdGenerator.Next(), when, serverId, serverName, database, (object?)fileId ?? DBNull.Value,
            fileType, database + "_" + fileType, (object?)total ?? DBNull.Value, (object?)used ?? DBNull.Value);
}
