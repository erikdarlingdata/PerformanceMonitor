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
/// #3462 — a relative regression must also be ABSOLUTELY material, at a floor the fleet's own cost
/// distribution justifies, on both delivery surfaces.
///
/// <para><b>The defect this pins.</b> #3316's floor was calibrated on a 41-hour sample that put real
/// regressions at 21.8-22.6 s/day and truthful-unactionable ones at 0.16-3.6 s/day, and 5 s claimed the
/// empty band with 4x headroom. On 2026-09-15 a production server paged HOURLY on a 5.5x
/// <c>database_size_stats</c> regression worth 8.7 seconds of collection time all day — clearing the ratio,
/// the dispersion bound AND the 5 s floor — while the same day's genuine exhibits added 224 and 377 s/day.
/// The unactionable mass reaches at least 8.7 s/day, so 5 s sits below the real empty band, not inside it.
/// The recalibrated floor is derived from the fleet distribution of per-collector daily cost (medians of
/// 13.7 and 13.1 s/day on two measured stores of one production store class) rather than from that one card;
/// its declaration carries the full derivation.</para>
///
/// <para><b>Unlike the sibling suites, the live fixtures here run at the SHIPPED floor, read from the
/// evaluator's constant.</b> The siblings pass explicit values because they pin the QUERY's mechanics, which
/// survive any retune. This suite pins the VALUE: the 2026-09-15 exhibits bracket it, so a retune below the
/// noise exhibit's added cost re-admits the firing that produced the issue and a retune above the genuine
/// exhibits drops the catches the metric exists for — both go red here rather than shipping silently.</para>
///
/// <para><b>The discriminating pair is a ratio-preserving rescale, the floor's analogue of #3441's
/// count-preserving permutation.</b> The noise exhibit and its at-scale twin have the SAME twelve baseline
/// days, the SAME 13 runs on every day, the SAME bimodal shape, and per-run costs that differ by an exact
/// factor of 32 — so the mean ratio, the p95 ratio and the day count are IDENTICAL by construction, and
/// every relative figure the pre-#3462 predicate consumed cannot tell them apart. One is excluded and the
/// other reported, so the exclusion is attributable to absolute magnitude and to nothing else.</para>
///
/// <para><b>The genuine exhibits are reconstructed at their recorded figures, with one honest caveat.</b>
/// <c>plan_correction</c> at 10,138.8 ms/run, 3.2x, ~223 s/day added and <c>query_stats</c> at
/// 9,932.8 ms/run over 38 runs are the issue's own numbers. The issue recorded the query_stats day's TOTAL
/// (377 s) rather than its baseline, so the baseline here is chosen at 3,000 ms/run — and the conclusion is
/// insensitive to that choice: at the loosest baseline the ratio gate admits (4,966.4 ms/run, exactly 2.0x)
/// the added cost is still 188,723 ms/day, 12.6x the floor.</para>
///
/// <para><b>Why this has to be a live-Postgres test.</b> Same reason as its three siblings: the floor is a
/// line of <see cref="DarlingCollectorCostReader.RegressionSql"/> (and, since #3462, of
/// <see cref="DarlingCollectorCostReader.MoverSql"/>). A text assertion would pass against any SQL
/// containing the right words, and the apply half only sees rows the SQL already chose to return.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CollectorCostMaterialityFloorTests
{
    private const string ServerName = "darling-costfloor-3462-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* The 2026-09-15 noise exhibit: 668.5 ms/run against a ~123 ms/run baseline over 13 hourly runs —
       5.4x the mean, past the p95 bound, and worth 7,088 ms of added collection time a day. Eleven baseline
       days at 1,582 ms over 13 runs (121.7 ms/run) and one at 1,819 (139.9 ms/run, the p95/worst day the
       measured card reported as its 279.8 ms threshold's origin). */
    private const string NoiseExhibit = "database_size_noise_3462";

    /* The ratio-preserving rescale: every per-day total is the noise exhibit's x32, on the same 13 runs, so
       every RELATIVE figure is identical and the added cost is 226,824 ms/day. */
    private const string NoiseAtScale = "database_size_noise_at_scale_3462";

    /* The same day's genuine exhibits. plan_correction: baseline 3,168 ms/run over 32 runs/day, latest
       324,442 ms over 32 runs = 10,138.8 ms/run, 3.2x, adding 223,066 ms/day. query_stats: baseline
       3,000 ms/run over 38 runs/day, latest 377,446 ms over 38 runs = 9,932.8 ms/run — the recorded
       377 s/day of monitoring overhead — adding 263,446 ms/day. */
    private const string PlanCorrectionExhibit = "plan_correction_3462";
    private const string QueryStatsExhibit = "query_stats_3462";

    /* The production ratio factor and eligibility floor, restated locally like the siblings do; the
       materiality floor is deliberately NOT restated — it is the constant under test. */
    private const double Factor = 2.0;
    private const long BaselineFloorMs = 1_000;

    /* Fixed dates, not offsets from now(): date_trunc('day') buckets the rows, so a test running near midnight
       UTC could otherwise split a "day" across two buckets and stop being deterministic. */
    private static readonly DateTime Day0 = new(2026, 9, 15, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The floor's own calibration, pinned without a store so it cannot drift silently even where
    /// the live half is skipped. Both edges are MEASURED, in the floor's own unit: the lower is the
    /// noisiest truthful-unactionable firing's ADDED cost (2026-09-15: 7,088 ms/day — the card's whole-day
    /// total was 8.7 s, but the floor gates the delta), the upper is the smallest real catch #3316
    /// recorded (21.8 s/day). A floor outside that band either re-admits a firing an operator answered
    /// with "it's a few hundred ms. what's the point?" or starts eating real catches.</summary>
    [Fact]
    public void TheShippedFloor_SitsInsideTheMeasuredEmptyBand_AndTheWorstCatchClearsItByOrdersOfMagnitude()
    {
        Assert.True(DarlingSelfAlertEvaluator.CostRegressionAddedMsFloor > 7_088,
            "the floor re-admits the 2026-09-15 noise exhibit");
        Assert.True(DarlingSelfAlertEvaluator.CostRegressionAddedMsFloor < 21_800,
            "the floor eats #3316's smallest measured real catch");

        /* #2150's reconstruction — the worst regression this metric ever had to find, 37-100 min/run
           against a 4.8 s baseline — computed rather than asserted by fiat: at the band's low end it adds
           (2,220,000 - 4,800) x 12 = 26,582,400 ms/day, which clears this floor 1,772 times over. A floor
           that endangered it would have failed the band assertion long before this line. */
        Assert.True((2_220_000L - 4_800L) * 12 >= DarlingSelfAlertEvaluator.CostRegressionAddedMsFloor * 1_000,
            "the #2150 catch no longer clears the floor with three orders of magnitude to spare");

        /* The paging read's floor conjunct, pinned as text here because the live half below cannot tell
           WHICH conjunct excluded a row — this is the line whose removal the fixtures would misattribute.
           The movers read's twin conjunct is pinned in CollectorCostDigestTests, beside the read it gates. */
        Assert.Contains(
            "(sc.latest_ms_per_run - sc.baseline_ms_per_run) * sc.latest_runs >= $4",
            DarlingCollectorCostReader.RegressionSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheFloor_DropsTheNoiseExhibit_AndKeepsItsRescaledTwin_AndTheGenuineCatches()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the cost-regression materiality-floor test.");

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

            /* Twelve prior days for the twins (eleven quiet, one worst — the measured card's shape), four
               for the exhibits whose recorded history was days, not weeks. */
            for (var back = 12; back >= 1; back--)
            {
                var noiseDay = back == 1 ? 1_819L : 1_582L;
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), NoiseExhibit, 13, noiseDay);
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), NoiseAtScale, 13, noiseDay * 32);

                if (back <= 4)
                {
                    await InsertCostAsync(connection, ct, Day0.AddDays(-back), PlanCorrectionExhibit, 32, 101_376);
                    await InsertCostAsync(connection, ct, Day0.AddDays(-back), QueryStatsExhibit, 38, 114_000);
                }
            }

            /* Latest day: the twins land on per-run figures exactly 32x apart, everything else identical. */
            await InsertCostAsync(connection, ct, Day0, NoiseExhibit, 13, 8_690);
            await InsertCostAsync(connection, ct, Day0, NoiseAtScale, 13, 278_080);
            await InsertCostAsync(connection, ct, Day0, PlanCorrectionExhibit, 32, 324_442);
            await InsertCostAsync(connection, ct, Day0, QueryStatsExhibit, 38, 377_446);

            var regressions = await DarlingCollectorCostReader.GetCostRegressionsAsync(
                postgres, Day0.AddDays(-13), baselineFloorMs: BaselineFloorMs, factor: Factor,
                addedMsFloor: DarlingSelfAlertEvaluator.CostRegressionAddedMsFloor, cancellationToken: ct);

            var mine = regressions.Where(r => r.ServerId == ServerId).ToList();

            /* The property: 5.4x of nothing is not a page and not a digest line. This row clears the mean
               ratio (668.5 vs 123.2 ms/run), the p95 bound (668.5 vs 279.8) and #3316's old 5,000 ms floor
               (7,088 added) — the floor under test is the only thing standing between it and a reader. */
            Assert.DoesNotContain(mine, r => r.CollectorName == NoiseExhibit);

            /* The rescaled twin: identical days, runs, mean ratio and p95 ratio, reported — so the
               exclusion above is magnitude and nothing else, and the fix is not "stop testing 5x days". */
            var scaled = Assert.Single(mine, r => r.CollectorName == NoiseAtScale);
            Assert.Equal(21_390.769, scaled.LatestMsPerRun, 3);
            Assert.Equal(3_942.769, scaled.BaselineMsPerRun, 3);
            Assert.Equal(226_824.0, scaled.AddedMsPerDay, 1);

            /* The genuine same-day exhibits, at their recorded magnitudes — the catches the floor must
               never touch. */
            var plan = Assert.Single(mine, r => r.CollectorName == PlanCorrectionExhibit);
            Assert.Equal(10_138.8125, plan.LatestMsPerRun, 3);
            Assert.Equal(223_066.0, plan.AddedMsPerDay, 1);

            var qs = Assert.Single(mine, r => r.CollectorName == QueryStatsExhibit);
            Assert.Equal(9_932.789, qs.LatestMsPerRun, 3);
            Assert.Equal(263_446.0, qs.AddedMsPerDay, 1);

            /* Three reported, one excluded: the exclusion is an exclusion, not a filter that ate the day. */
            Assert.Equal(3, mine.Count);

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

    /// <summary>The digest half of #3462, against the SHIPPED movers read: an immaterial move is not a
    /// line, a material move still is in BOTH directions, and the denominator the digest states is the
    /// count that survived the floor. The riser pair reuses #3316's volume construction — identical per-run
    /// figures, only the run count differs — so the exclusion is attributable to what the move is WORTH,
    /// and the faller pins that the floor gates on magnitude rather than on rise: a collector that got
    /// 45 s/day cheaper is the half of the series the paging condition structurally cannot report, and
    /// #3462 must not lose it while dropping the noise.</summary>
    [Fact]
    public async Task TheMoversRead_DropsAnImmaterialMove_AndKeepsMaterialOnes_InBothDirections()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the cost-mover materiality-floor test.");

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

            for (var back = 4; back >= 1; back--)
            {
                /* 100 -> 300 ms/run at 20 runs is +4,000 ms/day; the same rise at 200 runs is +40,000. */
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), "mover_immaterial_3462", 20, 2_000);
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), "mover_material_up_3462", 200, 20_000);
                /* 1,000 -> 100 ms/run at 50 runs is -45,000 ms/day: material, and negative. */
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), "mover_material_down_3462", 50, 50_000);
            }

            await InsertCostAsync(connection, ct, Day0, "mover_immaterial_3462", 20, 6_000);
            await InsertCostAsync(connection, ct, Day0, "mover_material_up_3462", 200, 60_000);
            await InsertCostAsync(connection, ct, Day0, "mover_material_down_3462", 50, 5_000);

            var movers = await DarlingCollectorCostReader.GetCostMoversAsync(
                postgres, Day0.AddDays(-13), DarlingSelfAlertEvaluator.CostRegressionAddedMsFloor,
                maxRows: 1_000, cancellationToken: ct);

            var mine = movers.Where(m => m.ServerId == ServerId).ToList();

            /* The property: a truthful 3x worth 4 s/day is not digest clutter either (#3462's "the same
               noise in a quieter channel"). It satisfies every other eligibility rule the read has. */
            Assert.DoesNotContain(mine, m => m.CollectorName == "mover_immaterial_3462");

            var up = Assert.Single(mine, m => m.CollectorName == "mover_material_up_3462");
            Assert.Equal(40_000.0, up.AddedMsPerDay, 1);

            var down = Assert.Single(mine, m => m.CollectorName == "mover_material_down_3462");
            Assert.Equal(-45_000.0, down.AddedMsPerDay, 1);

            /* Ranked by magnitude, so the 45 s faller outranks the 40 s riser — the sort key and the gate
               are the same expression, asserted here on the shipped ORDER BY's output. */
            Assert.True(mine.IndexOf(down) < mine.IndexOf(up), "the larger magnitude did not rank first");

            /* The denominator the digest will print is the count that SURVIVED the floor, taken from the
               same answer the rows came out of — both rows must carry one figure, and it cannot be smaller
               than the material rows it accompanies. (>= rather than ==: the count is store-wide by
               design, and a shared test store may carry other servers' pairs.) */
            Assert.Equal(up.EligiblePairs, down.EligiblePairs);
            Assert.True(up.EligiblePairs >= 2, "eligible_pairs undercounts the material movers");

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
