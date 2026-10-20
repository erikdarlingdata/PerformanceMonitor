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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6215: "pairs moved materially against their own baseline" fired on an install a couple of days old. The reads
/// already required three prior calendar days, but a calendar day is a bucket, and an install's first bucket is a
/// few hours of data counted as a whole day, so three buckets could sit on under three days of history. Both the
/// paging read and the digest's movers read now also require the pair to span three days between its first and
/// last observation. Each fixture has the SAME move; only the age of the first observation differs, so the
/// exclusion is attributable to age and to nothing else.
/// </summary>
[Collection("live-postgres")]
public sealed class CollectorCostFreshInstallBaselineTests
{
    private const string ServerName = "darling-costage-6215-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string FreshInstall = "fresh_install_6215";
    private const string SettledInstall = "settled_install_6215";

    private static readonly DateTime Day0 = new(2026, 9, 15, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AFreshInstallsFirstHours_AreNotABaseline_ForEitherTheDigestOrThePage()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the fresh-install baseline test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* Both collectors have FOUR calendar buckets (three prior days and the latest) and the identical
               10x move on the latest one, whose last observation is 12:00. The fresh one's first bucket holds
               one observation at 22:00 - two hours of a day - so its first-to-last span is 2 days 14 hours:
               three buckets on under three days. The settled one's first observation is 10:00 that day:
               3 days 2 hours. */
            await InsertCostAsync(connection, ct, Day0.AddDays(-3).AddHours(10), FreshInstall, 100, 100_000);
            await InsertCostAsync(connection, ct, Day0.AddDays(-3).AddHours(-2), SettledInstall, 100, 100_000);
            foreach (var collector in new[] { FreshInstall, SettledInstall })
            {
                foreach (var back in new[] { 2, 1 })
                {
                    await InsertCostAsync(connection, ct, Day0.AddDays(-back), collector, 100, 100_000);
                }

                await InsertCostAsync(connection, ct, Day0, collector, 100, 1_000_000);
            }

            var movers = await DarlingCollectorCostReader.GetCostMoversAsync(
                postgres, Day0.AddDays(-13), DarlingSelfAlertEvaluator.CostRegressionAddedMsFloor,
                maxRows: 1_000, cancellationToken: ct);
            var regressions = await DarlingCollectorCostReader.GetCostRegressionsAsync(
                postgres, Day0.AddDays(-13), baselineFloorMs: 1_000, factor: 2.0,
                addedMsFloor: DarlingSelfAlertEvaluator.CostRegressionAddedMsFloor, cancellationToken: ct);

            var mineMovers = movers.Where(m => m.ServerId == ServerId).Select(m => m.CollectorName).ToList();
            var mineRegressions = regressions.Where(r => r.ServerId == ServerId).Select(r => r.CollectorName).ToList();

            Assert.Equal(new[] { SettledInstall }, mineMovers);
            Assert.Equal(new[] { SettledInstall }, mineRegressions);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertCostAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime metricTime,
        string collector, int runs, long totalSqlMs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.collector_cost
    (metric_time, server_id, database_name, collector_name, run_count, total_sql_ms, max_sql_ms, total_storage_ms, total_rows)
VALUES ($1, $2, NULL, $3, $4, $5, $6, 0, 0);",
            DarlingMcpTestData.Naive(metricTime), ServerId, collector, runs, totalSqlMs, totalSqlMs / runs);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            "DELETE FROM collect.collector_cost WHERE server_id = " + ServerId + "; " +
            "DELETE FROM servers WHERE server_id = " + ServerId + ";", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
