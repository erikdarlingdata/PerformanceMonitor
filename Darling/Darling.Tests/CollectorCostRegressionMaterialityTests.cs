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
/// #3316 — a per-run cost regression must also be worth reporting.
///
/// <para><b>The defect this pins.</b> The predicate had two gates measuring different units: a floor on the
/// average daily TOTAL (<c>baseline_ms &gt;= $2</c>) and a ratio on cost PER RUN. A total-cost floor is cleared
/// by VOLUME, so it does not constrain the quantity the ratio tests — a collector averaging 3 ms per run clears
/// a 1000 ms/day floor on run count alone and is then judged by a ratio on that 3 ms. The measured firing
/// doubled 3.0 to 6.1 ms per run across 50 runs, which adds 0.16 s of collection time a day.</para>
///
/// <para><b>That firing was TRUTHFUL, which is why the fix is a materiality gate and not a precision one.</b>
/// Both sides of the ratio are means over many runs, so integer-millisecond quantization averages down to
/// hundredths of a millisecond and the doubling was real. The alert was correct and unactionable. So the third
/// gate asks what the regression COSTS — the per-run rise times the volume it is paid on — which is
/// unit-consistent with the ratio. A minimum per-run BASELINE would have been the wrong fix: it would exclude a
/// 3 ms collector that runs 100,000 times a day, where the same doubling costs five minutes daily.</para>
///
/// <para><b>Why this has to be a live-Postgres test.</b> Same reason as
/// <see cref="CollectorCostRegressionPerRunTests"/>: the gate is a line of
/// <see cref="DarlingCollectorCostReader.RegressionSql"/>. A text assertion on the query would pass against any
/// SQL containing the right words, and an in-memory test of the apply half only sees rows the SQL already chose
/// to return. The property is asserted by planting rows and running the SHIPPED query against a real store.</para>
///
/// <para><b>The fixture is adversarial on purpose.</b> Both collectors have an IDENTICAL per-run baseline
/// (100 ms), an IDENTICAL latest per-run cost (300 ms), the IDENTICAL 3x ratio, and a constant cadence — so
/// #2846's per-run rule reports both, and both clear the daily-total floor. They differ in one thing only:
/// volume, and therefore what the regression costs (4 s/day versus 40 s/day against a 5 s floor). Reverting the
/// materiality predicate therefore fails on <c>volume_trivial</c> specifically, rather than on some incidental
/// difference between the two.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CollectorCostRegressionMaterialityTests
{
    private const string ServerName = "darling-costmateriality-3316-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* 20 runs/day. A real 3x per-run rise that adds (300 - 100) * 20 = 4,000 ms/day. */
    private const string VolumeTrivial = "volume_trivial_3316";

    /* 200 runs/day. The SAME 3x per-run rise, adding (300 - 100) * 200 = 40,000 ms/day. */
    private const string VolumeMaterial = "volume_material_3316";

    /* The floor under test, between the two fixtures with 25% and 8x margin. Passed explicitly rather than
       read from the evaluator's private constant: this pins the QUERY's property, which is what the SQL can
       get wrong, and stays meaningful if the production value is retuned. */
    private const long AddedMsFloor = 5_000;

    /* Fixed dates, not offsets from now(): date_trunc('day') buckets the rows, so a test running near midnight
       UTC could otherwise split a "day" across two buckets and stop being deterministic. */
    private static readonly DateTime Day0 = new(2026, 6, 17, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task MaterialityFloor_DropsATrivialRise_ButKeepsTheSameRiseAtVolume()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the cost-regression materiality test.");

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

            /* Four prior days at 100 ms/run for both. Daily totals are 2,000 and 20,000 ms, so BOTH clear the
               1,000 ms/day eligibility floor and both reach the gate under test. */
            for (var back = 4; back >= 1; back--)
            {
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), VolumeTrivial, 20, 2_000);
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), VolumeMaterial, 200, 20_000);
            }

            /* Latest day: both rise to exactly 300 ms/run at unchanged cadence. Identical ratio, identical
               per-run figures; only the volume the rise is paid on differs. */
            await InsertCostAsync(connection, ct, Day0, VolumeTrivial, 20, 6_000);
            await InsertCostAsync(connection, ct, Day0, VolumeMaterial, 200, 60_000);

            var regressions = await DarlingCollectorCostReader.GetCostRegressionsAsync(
                postgres, Day0.AddDays(-10), baselineFloorMs: 1_000, factor: 2.0,
                addedMsFloor: AddedMsFloor, cancellationToken: ct);

            var mine = regressions.Where(r => r.ServerId == ServerId).ToList();

            /* The property: a truthful 3x that costs 4 s/day is not worth an alert. */
            Assert.DoesNotContain(mine, r => r.CollectorName == VolumeTrivial);

            /* ...and the SAME 3x at volume still is, so the fix cannot be "report nothing". */
            var material = Assert.Single(mine, r => r.CollectorName == VolumeMaterial);

            /* Both collectors' per-run figures are identical, which is what makes the exclusion above
               attributable to volume and nothing else. */
            Assert.Equal(300.0, material.LatestMsPerRun, 3);
            Assert.Equal(100.0, material.BaselineMsPerRun, 3);
            Assert.Equal(200, material.LatestRuns);

            /* The derived cost the gate compares, and the figure the alert text now states. */
            Assert.Equal(40_000.0, material.AddedMsPerDay, 3);

            bodySucceeded = true;
        }
        finally
        {
            /* RunAsync, not RunOwnedAsync: it opens the connection AND sets search_path to
               "collect, config, public", which the unqualified `servers` delete below depends on. */
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
