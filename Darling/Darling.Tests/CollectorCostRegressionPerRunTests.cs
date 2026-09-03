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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2846 — the Collector Cost Regression predicate must compare cost PER RUN, not the day's total.
///
/// <para><b>The defect this pins.</b> Total daily cost is <c>runs x cost-per-run</c>. Comparing totals cannot
/// distinguish "each run got more expensive" from "the same work ran more often", so a collector whose cadence
/// RECOVERED — strictly an improvement — reported as a regression. On prod-pos-use1-monitor-01 that fired 3,259
/// times across 612 (server, collector) pairs in one day, and 53% of those pairs had per-run cost going DOWN.
/// Running the shipped predicate against that store, the total-cost rule matched 610 pairs and the per-run rule
/// matched 14.</para>
///
/// <para><b>Why this has to be a live-Postgres test.</b> The whole defect lives in
/// <see cref="DarlingCollectorCostReader.RegressionSql"/> — the C# either side of it is correct and was correct
/// before. A text assertion on the query would pass against any SQL containing the right words, and an
/// in-memory test of <c>ApplyCostRegressionsAsync</c> only ever sees rows the SQL already decided to return. So
/// the property is asserted by planting rows and running the SHIPPED query against a real store.</para>
///
/// <para><b>The fixture is adversarial on purpose.</b> Both collectors have identical baselines and identical
/// latest-day TOTALS (3x baseline). They differ only in run count. Under the pre-#2846 rule both are reported;
/// under the per-run rule only the genuinely-slower one is. Reverting the fix therefore fails on
/// <c>cadence_recovered</c> specifically, rather than on some incidental difference.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CollectorCostRegressionPerRunTests
{
    private const string ServerName = "darling-costregression-2846-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* A collector whose cadence recovered: three times the runs, IDENTICAL cost per run. */
    private const string CadenceRecovered = "cadence_recovered_2846";

    /* A collector genuinely three times slower per run, at unchanged cadence. */
    private const string PerRunSlower = "per_run_slower_2846";

    /* Fixed dates, not offsets from now(): date_trunc('day') buckets the rows, so a test running near midnight
       UTC could otherwise split a "day" across two buckets and stop being deterministic. Nothing else writes
       this server_id, so latest_day is whichever day this fixture says it is. */
    private static readonly DateTime Day0 = new(2026, 6, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task PerRunRule_IgnoresCadenceRecovery_ButReportsAGenuinelySlowerRun()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the cost-regression per-run test.");

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

            /* Four prior days, both collectors identical: 1,000 runs costing 100,000 ms => 100 ms/run. */
            for (var back = 4; back >= 1; back--)
            {
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), CadenceRecovered, 1000, 100_000);
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), PerRunSlower, 1000, 100_000);
            }

            /* Latest day. Both totals are 300,000 ms — 3x the 100,000 ms/day baseline — so the PRE-#2846
               total-cost rule reports BOTH. They differ only in how that total was reached. */
            await InsertCostAsync(connection, ct, Day0, CadenceRecovered, 3000, 300_000);  /* 100 ms/run: flat */
            await InsertCostAsync(connection, ct, Day0, PerRunSlower,     1000, 300_000);  /* 300 ms/run: 3x   */

            var regressions = await DarlingCollectorCostReader.GetCostRegressionsAsync(
                postgres, Day0.AddDays(-10), baselineFloorMs: 1000, factor: 2.0, ct);

            var mine = regressions.Where(r => r.ServerId == ServerId).ToList();

            /* The property: cadence recovery is not a cost regression. */
            Assert.DoesNotContain(mine, r => r.CollectorName == CadenceRecovered);

            /* ...and a real per-run regression is still caught, so the fix cannot be "report nothing". */
            var slower = Assert.Single(mine, r => r.CollectorName == PerRunSlower);
            Assert.Equal(300.0, slower.LatestMsPerRun, 3);
            Assert.Equal(100.0, slower.BaselineMsPerRun, 3);
            Assert.Equal(1000, slower.LatestRuns);

            /* The daily totals are still carried for the alert text, and are IDENTICAL between the two
               collectors — which is exactly why the old rule could not tell them apart. */
            Assert.Equal(300_000, slower.LatestMs);

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
