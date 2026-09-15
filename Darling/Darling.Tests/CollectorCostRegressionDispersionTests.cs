/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3440 — the ratio has to be measured against the baseline's own UPPER EDGE, not its mean.
///
/// <para><b>The defect this pins.</b> A heavy collector's own run-to-run spread already exceeds
/// <c>CostRegressionFactor</c>. Measured per-run <c>p95/avg</c> on one production fleet: 2.03x and 2.95x for
/// <c>index_object_stats</c> on two servers, 4.74x for <c>procedure_stats</c>, 5.04x for <c>query_store</c>.
/// Against a MEAN baseline at a factor of 2.0, a perfectly normal upper-mode day on any of those clears the
/// ratio by construction, so the alert cannot tell "this collector got slower" from "this collector had a
/// normal slow day" — which is the entire question it exists to answer. The firing that produced this issue
/// was 17,548 ms/run against a 6,477 ms mean on a once-daily collector whose own worst run that week was
/// 17,935 ms.</para>
///
/// <para><b>#3316's materiality floor cannot screen these, which is why the fix is a second bound and not a
/// retune of the first.</b> The expense that makes a collector bimodal also makes its upper mode's excess
/// large: the measured <c>query_store</c> spread adds 16,534 ms on one p95 run, 3.3x the 5,000 ms floor. This
/// fixture therefore runs at the PRODUCTION floor rather than at zero, so an exclusion here is attributable
/// to dispersion and to nothing else.</para>
///
/// <para><b>Why this has to be a live-Postgres test.</b> Same reason as
/// <see cref="CollectorCostRegressionPerRunTests"/> and <see cref="CollectorCostRegressionMaterialityTests"/>:
/// the bound is a line of <see cref="DarlingCollectorCostReader.RegressionSql"/>. A text assertion on the
/// query would pass against any SQL containing the right words, and an in-memory test of the apply half only
/// sees rows the SQL already chose to return. The property is asserted by planting rows and running the
/// SHIPPED query against a real store.</para>
///
/// <para><b>The fixture is adversarial on purpose, and the first two collectors are a count-preserving
/// permutation of each other.</b> <c>bimodal_normal</c> and <c>tight_regressed</c> have the SAME number of
/// baseline days (12), the SAME number of baseline runs (12), the SAME baseline total (84,000 ms), therefore
/// the same run-weighted mean of 7,000 ms/run — and the SAME latest day, 17,500 ms over one run. Every figure
/// the pre-#3440 predicate consumed is identical, including <c>AddedMsPerDay</c> at 10,500 ms. They differ
/// only in how the baseline's 84,000 ms is DISTRIBUTED across its twelve days. One is reported and the other
/// is not, so the exclusion cannot be attributed to magnitude, ratio, volume, materiality or cadence.</para>
///
/// <para><b>And it asserts both directions, because a fixture that only showed the false positive going away
/// would pass a fix that disabled the alert.</b> <c>bimodal_regressed</c> carries the IDENTICAL bimodal
/// baseline as <c>bimodal_normal</c> and a latest day just past the bound, and must still be reported:
/// a real regression on a variable collector stays detectable rather than being excluded from detection.
/// <c>tight_regressed</c> is the single-population case, where the new bound must be inert.</para>
///
/// <para><b><c>interpolation_would_report</c> pins the INSTRUMENT, not just the behaviour.</b> Its baseline is
/// eleven days at 4,000 ms/run and one at 37,000, so <c>percentile_disc(0.95)</c> returns 37,000 — a day that
/// happened — while <c>percentile_cont(0.95)</c> interpolates to 18,850, a figure no day ever cost. Its
/// latest day of 40,000 ms/run sits above the interpolated bound and below the discrete one, so swapping DISC
/// for CONT in the shipped query turns this exclusion into a firing and this test red. That is #2460's
/// reasoning for the per-run p95 <c>get_collection_health</c> serves, applied here at the grain of a day.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class CollectorCostRegressionDispersionTests
{
    private const string ServerName = "darling-costdispersion-3440-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* Baseline: nine days at 4,000 ms/run and three at 16,000 — mean 7,000, daily p95 16,000, bound 32,000.
       Latest day 17,500 ms/run is 2.5x the mean and inside the collector's own upper mode. */
    private const string BimodalNormal = "bimodal_normal_3440";

    /* The count-preserving permutation of BimodalNormal's baseline: twelve days at 7,000 ms/run. Same mean,
       same runs, same total, no dispersion — so the bound is 14,000 and the same 17,500 IS reported. */
    private const string TightRegressed = "tight_regressed_3440";

    /* BimodalNormal's baseline exactly, with a latest day past the 32,000 bound. */
    private const string BimodalRegressed = "bimodal_regressed_3440";

    /* Eleven days at 4,000 and one at 37,000: percentile_disc gives 37,000 (bound 74,000),
       percentile_cont gives 18,850 (bound 37,700). The latest day of 40,000 falls between them. */
    private const string InterpolationWouldReport = "interpolation_would_report_3440";

    /* The production factor and materiality floor, passed explicitly rather than read from the evaluator's
       private constants: this pins the QUERY's property, which is what the SQL can get wrong, and stays
       meaningful if either production value is retuned. */
    private const double Factor = 2.0;
    private const long AddedMsFloor = 5_000;

    /* Fixed dates, not offsets from now(): date_trunc('day') buckets the rows, so a test running near midnight
       UTC could otherwise split a "day" across two buckets and stop being deterministic. */
    private static readonly DateTime Day0 = new(2026, 6, 24, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task DispersionBound_DropsAnUpperModeDay_ButKeepsARealRegressionOnTheSameBaseline()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the cost-regression dispersion test.");

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

            /* Twelve prior days at one run each, so a day's per-run cost IS that day's total — the shape of
               the daily-cadence collector the measured firing came from. */
            for (var back = 12; back >= 1; back--)
            {
                var bimodalDay = back <= 3 ? 16_000L : 4_000L;
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), BimodalNormal, bimodalDay);
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), BimodalRegressed, bimodalDay);
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), TightRegressed, 7_000L);
                await InsertCostAsync(connection, ct, Day0.AddDays(-back), InterpolationWouldReport,
                    back == 1 ? 37_000L : 4_000L);
            }

            /* Latest day. BimodalNormal and TightRegressed land on the IDENTICAL figure against the IDENTICAL
               mean, so the pre-#3440 predicate cannot separate them at all. */
            await InsertCostAsync(connection, ct, Day0, BimodalNormal, 17_500L);
            await InsertCostAsync(connection, ct, Day0, TightRegressed, 17_500L);
            await InsertCostAsync(connection, ct, Day0, BimodalRegressed, 32_100L);
            await InsertCostAsync(connection, ct, Day0, InterpolationWouldReport, 40_000L);

            var regressions = await DarlingCollectorCostReader.GetCostRegressionsAsync(
                postgres, Day0.AddDays(-13), baselineFloorMs: 1_000, factor: Factor,
                addedMsFloor: AddedMsFloor, cancellationToken: ct);

            var mine = regressions.Where(r => r.ServerId == ServerId).ToList();

            /* The property: a day inside the collector's own upper mode is not a cost regression. 17,500 is
               2.5x the 7,000 mean, so the mean-ratio conjunct alone reports it. */
            Assert.DoesNotContain(mine, r => r.CollectorName == BimodalNormal);

            /* The permutation twin. Identical mean, identical latest day, identical added cost — reported,
               because its baseline has no upper mode for 17,500 to belong to. */
            var tight = Assert.Single(mine, r => r.CollectorName == TightRegressed);
            Assert.Equal(17_500.0, tight.LatestMsPerRun, 3);
            Assert.Equal(7_000.0, tight.BaselineMsPerRun, 3);
            Assert.Equal(7_000.0, tight.BaselineP95MsPerRun, 3);
            Assert.Equal(14_000.0, tight.ThresholdMsPerRun(Factor), 3);

            /* This row is also the proof that #3316's floor did not do the excluding above: the two
               collectors' latest day, run count and mean are identical, so BimodalNormal's added cost is
               this same figure, and it is twice the 5,000 ms floor the query was given. */
            Assert.Equal(10_500.0, tight.AddedMsPerDay, 3);

            /* Both directions. The SAME bimodal baseline, a latest day past the bound: still reported, so
               the fix is not "stop testing variable collectors". */
            var regressed = Assert.Single(mine, r => r.CollectorName == BimodalRegressed);
            Assert.Equal(7_000.0, regressed.BaselineMsPerRun, 3);
            Assert.Equal(16_000.0, regressed.BaselineP95MsPerRun, 3);
            Assert.Equal(32_000.0, regressed.ThresholdMsPerRun(Factor), 3);
            Assert.Equal(32_100.0, regressed.LatestMsPerRun, 3);

            /* The instrument. DISC puts the bound at 74,000 and CONT would put it at 37,700; 40,000 is
               between them, so this exclusion fails if the shipped query interpolates. */
            Assert.DoesNotContain(mine, r => r.CollectorName == InterpolationWouldReport);

            /* Nothing else on this server, so the two exclusions are exclusions and not a filter that
               dropped everything. */
            Assert.Equal(2, mine.Count);

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


    /// <summary>#3440: both conjuncts have to stay in the shipped predicate, and the mean one LOOKS removable.
    ///
    /// <para>At the shipped baseline window the mean conjunct is entailed by the p95 one for every possible
    /// row, so no fixture above or anywhere else can exercise it. <c>percentile_disc(0.95)</c> over N rows
    /// returns 1-based rank <c>ceil(0.95 * N)</c>, which equals N for every N up to 19 — so on a baseline of
    /// at most 13 days <c>baseline_p95_ms_per_run</c> is exactly the MAXIMUM of the daily per-run costs. And
    /// <c>baseline_ms_per_run</c> is <c>sum(sql_ms) / sum(runs)</c>, which is algebraically the run-weighted
    /// mean of that same population and so can never exceed its maximum. Hence
    /// <c>baseline_p95_ms_per_run &gt;= baseline_ms_per_run</c> unconditionally, and
    /// <see cref="DarlingCollectorCostReader.CostRegression.ThresholdMsPerRun"/>'s <c>Math.Max</c> always
    /// resolves to the p95 side on any row the query can return.</para>
    ///
    /// <para>The mean conjunct is retained anyway, and that is the point of this pin. The property the
    /// change rests on — that this predicate selects a SUBSET of what it selected before — then holds by
    /// CONSTRUCTION, because a conjunct was added and none was removed, rather than by an entailment whose
    /// only precondition is a window constant in a different file. Above 19 baseline days DISC stops
    /// returning the maximum, the entailment ends, and a predicate that had dropped the mean conjunct as
    /// dead would silently loosen at that moment with nothing red to say so. The window assertion below is
    /// what reports that boundary being crossed.</para></summary>
    [Fact]
    public void BothConjunctsStayShipped_AndTheEntailmentsWindowPreconditionStillHolds()
    {
        var sql = DarlingCollectorCostReader.RegressionSql;
        Assert.Contains("sc.latest_ms_per_run > sc.baseline_ms_per_run * $3", sql, StringComparison.Ordinal);
        Assert.Contains("sc.latest_ms_per_run > sc.baseline_p95_ms_per_run * $3", sql, StringComparison.Ordinal);

        /* The window the evaluator passes as $1, read from the source rather than restated, because a
           restated constant is the thing that goes stale. */
        var evaluator = RepoFile.ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs");
        var declaration = Regex.Match(
            evaluator, @"CostRegressionBaselineWindow\s*=\s*TimeSpan\.FromDays\((?<days>\d+)\)");
        Assert.True(declaration.Success, "CostRegressionBaselineWindow's declaration was not found");

        var windowDays = int.Parse(declaration.Groups["days"].Value, CultureInfo.InvariantCulture);
        var maxBaselineDays = windowDays - 1;   /* the latest day is excluded from its own baseline */
        var discRank = (int)Math.Ceiling(0.95 * maxBaselineDays);

        /* While DISC's rank is the population size, the p95 IS the maximum and the mean conjunct is dead
           weight kept deliberately. When this stops holding, the mean conjunct becomes load-bearing and the
           paragraph above is the thing to read. */
        Assert.Equal(maxBaselineDays, discRank);
    }

    /* One run per day, so total_sql_ms IS that day's per-run cost. */
    private static async Task InsertCostAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime metricTime,
        string collector, long totalSqlMs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.collector_cost
    (metric_time, server_id, database_name, collector_name, run_count, total_sql_ms, max_sql_ms, total_storage_ms, total_rows)
VALUES ($1, $2, NULL, $3, 1, $4, $4, 0, 0);",
            DarlingMcpTestData.Naive(metricTime), ServerId, collector, totalSqlMs);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            "DELETE FROM collect.collector_cost WHERE server_id = " + ServerId + "; " +
            "DELETE FROM servers WHERE server_id = " + ServerId + ";", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
