/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5630: the PLAN_REGRESSION fact does not compare two plans compiled for different inputs. A parameter-sensitive query
/// (its cheap plan served a small location, its slow plan a large one) and a statement with OPTION (RECOMPILE) (each call
/// gets its own plan) are not plan choices that got worse, and the advice to force the cheap plan made the slow case
/// slower.
///
/// <para>Live, over seeded rows only: Query Store stats (the raw slice and the interval table), statement text, and
/// stored plans with small ShowPlanXML fixtures that carry <c>ParameterCompiledValue</c>. The fixtures are the four
/// reported cases (a recompiling lookup, two parameter-sensitive manifest lists, and the single-hash control) plus a real
/// regression with the SAME compiled values, which must still report; two queries whose inputs cannot be checked stay
/// reported and are counted. Every reader of the list is asserted: the offenders on the context, the regressed-queries
/// drill-down, and the force-plan targets built from it.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PlanRegressionInputsLiveTests
{
    private const int ServerId = -563001;
    private const int CapServerId = -563002;
    private const int ReplicaServerId = -563003;
    private const int FailServerId = -563004;
    private const string Db = "inputsdb";
    private const int LiveTimeoutSeconds = 60;

    private const long RecompileLookup = 1;
    private const long ManifestsB = 2;
    private const long ManifestsA = 3;
    private const long ManifestsControl = 4;
    private const long RealRegression = 5;
    private const long NoPlansStored = 6;
    private const long Withheld = 7;
    private const long OutputGrew = 8;
    private const long StatsFlip = 9;
    private const long RowsUnknown = 10;

    private const string StatsEarly = "2026-01-05T10:00:00";
    private const string StatsLate = "2026-01-09T10:00:00";

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    /// <summary>A synthetic plan. <paramref name="statsDate"/> adds an OptimizerStatsUsage with three statistics last updated then; null adds none.</summary>
    private static string PlanFor(string statement, string? location, string? statsDate = null) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"" + System.Security.SecurityElement.Escape(statement) + "\" StatementId=\"1\" StatementType=\"SELECT\">"
        + "<QueryPlan CachedPlanSize=\"16\">"
        + (statsDate is null ? string.Empty
            : "<OptimizerStatsUsage>"
              + string.Concat(new[] { "stat_a", "stat_b", "stat_c" }.Select(n =>
                  "<StatisticsInfo Database=\"[inputsdb]\" Schema=\"[dbo]\" Table=\"[widgets]\" Statistics=\"[" + n
                  + "]\" ModificationCount=\"0\" SamplingPercent=\"100\" LastUpdate=\"" + statsDate + "\"/>"))
              + "</OptimizerStatsUsage>")
        + (location is null ? string.Empty
            : "<ParameterList><ColumnReference Column=\"@location_id\" ParameterDataType=\"int\" ParameterCompiledValue=\""
              + location + "\" ParameterRuntimeValue=\"" + location + "\"/></ParameterList>")
        + "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";

    private static AnalysisContext NewContext(int serverId, DateTime periodStart, DateTime periodEnd) => new()
    {
        ServerId = serverId,
        ServerName = "RegrInputsSrv",
        TimeRangeStart = periodStart,
        TimeRangeEnd = periodEnd,
        ServerUtcOffset = TimeSpan.Zero,
    };

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task APlanRegressionThatIsOnlyAChangeOfInputIsNotReported_AgainstDevPostgres(bool intervalTable, bool daily)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5630 plan-inputs test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = TruncateToSeconds(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc));
            var periodStart = periodEnd.AddHours(-4);

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await SeedAsync(connection, periodStart, periodEnd, intervalTable, ct);
                if (daily)
                {
                    /* Build the closed days, so the read takes them from the per-day totals (the live half of the read holds the rest). */
                    var windowDay = periodStart.Date;
                    for (var d = -PgFactCollector.PlanRegressionWindowDays; d <= -3; d++)
                    {
                        await PlanRegressionDaily.BuildDayAsync(connection, ServerId, DateOnly.FromDateTime(windowDay.AddDays(d)), periodEnd, ct);
                    }

                    /* A built day carries rowcount_sum: the sum of rows times calls for a plan whose intervals all counted rows, and
                       NULL for the plan with an interval that did not (query 10's older plan). */
                    await using var sums = new NpgsqlCommand(
                        "SELECT plan_id, rowcount_sum FROM collect.plan_regression_daily WHERE server_id = $1 AND plan_id IN (81, 101, 51) ORDER BY plan_id", connection);
                    sums.Parameters.AddWithValue(ServerId);
                    await using var sumReader = await sums.ExecuteReaderAsync(ct);
                    var seen = new Dictionary<long, decimal?>();
                    while (await sumReader.ReadAsync(ct))
                    {
                        seen[sumReader.GetInt64(0)] = sumReader.IsDBNull(1) ? null : sumReader.GetDecimal(1);
                    }

                    Assert.Equal(2_500m, seen[81]);
                    Assert.Equal(49_800m, seen[51]);
                    Assert.Null(seen[101]);
                }
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var pass = NewContext(ServerId, periodStart, periodEnd);
            var fact = (await new PgFactCollector(postgres).CollectFactsAsync(pass))
                .Single(f => f.Key == "PLAN_REGRESSION");

            /* The path the read took is the one the case names. */
            Assert.Equal(intervalTable, pass.PlanRegressionReadsIntervalTable);
            Assert.Equal(daily, pass.PlanRegressionWindowStart is not null);

            /* Kept: the two whose inputs cannot be checked (no stored plans; a statement the filter withheld), the statistics flip,
               the real regression with the same compiled values, and the one whose older row count is unknown. Out: the recompiling
               lookup, both lists that differ only by value, and the plan whose cost followed a 26,000-row output. The control
               never regressed. */
            var expected = new[] { Withheld, NoPlansStored, StatsFlip, RealRegression, RowsUnknown };
            Assert.NotNull(pass.PlanRegressionOffenders);
            Assert.Equal(
                expected.Select(q => new PlanRegressionOffender(Db, q)),
                pass.PlanRegressionOffenders!);
            Assert.Equal(5, fact.Metadata["offender_count"]);
            Assert.Equal(Withheld, fact.Metadata["worst_query_id"]);
            Assert.Equal(4, fact.Metadata["cross_input_excluded_count"]);
            Assert.Equal(3, fact.Metadata["inputs_unverified_count"]);
            /* The worst kept offender is the withheld statement, which was not checked. */
            Assert.Equal(0, fact.Metadata["worst_input_basis"]);

            /* Every other reader of the list agrees: the drill-down and the force-plan targets built from it. */
            var finding = new AnalysisFinding
            {
                RootFactKey = "PLAN_REGRESSION",
                StoryPath = "PLAN_REGRESSION",
                PathKeys = ["PLAN_REGRESSION"],
                Severity = 1.0,
            };
            await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], pass);

            var rows = JsonSerializer.SerializeToElement(finding.DrillDown!["regressed_queries"]).EnumerateArray().ToList();
            Assert.Equal(
                expected.OrderBy(q => q),
                rows.Select(r => r.GetProperty("query_id").GetInt64()).OrderBy(q => q));

            var targets = FactRemediation.ExtractPlanRegressionTargets(finding);
            Assert.All(targets, t => Assert.Contains(t.QueryId, expected));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    [Theory]
    [InlineData(StatsFlip, 2)]
    [InlineData(RealRegression, 1)]
    public async Task TheWorstInputBasisIsTheWorstKeptOffenders_AgainstDevPostgres(long query, int basis)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5631 input basis test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = TruncateToSeconds(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc));
            var periodStart = periodEnd.AddHours(-4);

            /* The plan whose cost followed its output ranks first by factor and is dropped, so the worst KEPT offender is the
               query under test. */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await SeedAsync(connection, periodStart, periodEnd, false, ct, only: [OutputGrew, query]);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var pass = NewContext(ServerId, periodStart, periodEnd);
            var fact = (await new PgFactCollector(postgres).CollectFactsAsync(pass))
                .Single(f => f.Key == "PLAN_REGRESSION");

            Assert.Equal(query, fact.Metadata["worst_query_id"]);
            Assert.Equal(1, fact.Metadata["cross_input_excluded_count"]);
            Assert.Equal(0, fact.Metadata["inputs_unverified_count"]);
            Assert.Equal(basis, fact.Metadata["worst_input_basis"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    [Fact]
    public async Task TheTwentyOffenderCutIsMadeAfterTheInputCheck_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5630 cut test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = TruncateToSeconds(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc));
            var periodStart = periodEnd.AddHours(-4);

            /* Twenty-five recompiling queries rank ABOVE twenty-two real regressions. A cut of 20 made before the check
               would keep only recompiling queries, drop them all, and report nothing. */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                for (long q = 100; q < 125; q++)
                {
                    await InsertStatsAsync(connection, CapServerId, q, 1000 + q * 10, "0xBEST" + q, 1_000, periodStart.AddDays(-5), periodStart, periodEnd, false, ct);
                    await InsertStatsAsync(connection, CapServerId, q, 1001 + q * 10, "0xLATE" + q, 1_000_000 + q, periodEnd, periodStart, periodEnd, false, ct);
                    await InsertTextAsync(connection, CapServerId, q, "SELECT 1 OPTION (RECOMPILE)", periodEnd, ct);
                }

                for (long q = 200; q < 222; q++)
                {
                    await InsertStatsAsync(connection, CapServerId, q, 1000 + q * 10, "0xBEST" + q, 200_000, periodStart.AddDays(-5), periodStart, periodEnd, false, ct);
                    await InsertStatsAsync(connection, CapServerId, q, 1001 + q * 10, "0xLATE" + q, 600_000 + (q * 1_000), periodEnd, periodStart, periodEnd, false, ct);
                    await InsertTextAsync(connection, CapServerId, q, "SELECT 2", periodEnd, ct);
                }
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var pass = NewContext(CapServerId, periodStart, periodEnd);
            var fact = (await new PgFactCollector(postgres).CollectFactsAsync(pass))
                .Single(f => f.Key == "PLAN_REGRESSION");

            Assert.Equal(20, fact.Metadata["offender_count"]);
            Assert.Equal(25, fact.Metadata["cross_input_excluded_count"]);
            Assert.Equal(
                Enumerable.Range(202, 20).Select(q => (long)q).OrderBy(q => q),
                pass.PlanRegressionOffenders!.Select(o => o.QueryId).OrderBy(q => q));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    [Fact]
    public async Task AQueryOneReplicaExcludes_LeavesBeforeTheCut_AndAPlaceGoesToTheNextQuery_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5630 replica test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = TruncateToSeconds(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc));
            var periodStart = periodEnd.AddHours(-4);
            const string Text = "SELECT o.order_id FROM dbo.orders AS o WHERE o.customer_id = @location_id";
            const long Split = 399;

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                /* Twenty-two queries that stay; they share one pair of plans (compiled for the same value). */
                await InsertPlanAsync(connection, 5001, PlanFor(Text, "(7)"), gz: true, periodEnd, ct, ReplicaServerId);
                await InsertPlanAsync(connection, 5002, PlanFor(Text, "(7)"), gz: true, periodEnd, ct, ReplicaServerId);
                for (long q = 300; q < 322; q++)
                {
                    await InsertStatsAsync(connection, ReplicaServerId, q, 5001, "0xBESTSHARED", 200_000, periodStart.AddDays(-5), periodStart, periodEnd, false, ct);
                    await InsertStatsAsync(connection, ReplicaServerId, q, 5002, "0xLATESHARED", 600_000 + (q * 1_000), periodEnd, periodStart, periodEnd, false, ct);
                    await InsertTextAsync(connection, ReplicaServerId, q, Text, periodEnd, ct);
                }

                /* Query 399 on two replicas. The primary's plans were compiled for the same value and rank first; the
                   secondary's were compiled for different values over unchanged statistics and rank last, below the cut of the others. */
                await InsertPlanAsync(connection, 5011, PlanFor(Text, "(7)"), gz: true, periodEnd, ct, ReplicaServerId);
                await InsertPlanAsync(connection, 5012, PlanFor(Text, "(7)"), gz: true, periodEnd, ct, ReplicaServerId);
                await InsertPlanAsync(connection, 5013, PlanFor(Text, "(7)", StatsEarly), gz: true, periodEnd, ct, ReplicaServerId);
                await InsertPlanAsync(connection, 5014, PlanFor(Text, "(1)", StatsEarly), gz: true, periodEnd, ct, ReplicaServerId);
                await InsertStatsAsync(connection, ReplicaServerId, Split, 5011, "0xP1", 200_000, periodStart.AddDays(-5), periodStart, periodEnd, false, ct, "PRIMARY");
                await InsertStatsAsync(connection, ReplicaServerId, Split, 5012, "0xP2", 5_000_000, periodEnd, periodStart, periodEnd, false, ct, "PRIMARY");
                await InsertStatsAsync(connection, ReplicaServerId, Split, 5013, "0xS1", 200_000, periodStart.AddDays(-5), periodStart, periodEnd, false, ct, "SECONDARY");
                await InsertStatsAsync(connection, ReplicaServerId, Split, 5014, "0xS2", 500_000, periodEnd, periodStart, periodEnd, false, ct, "SECONDARY");
                await InsertTextAsync(connection, ReplicaServerId, Split, Text, periodEnd, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var pass = NewContext(ReplicaServerId, periodStart, periodEnd);
            var fact = (await new PgFactCollector(postgres).CollectFactsAsync(pass))
                .Single(f => f.Key == "PLAN_REGRESSION");

            /* The query leaves whole, before the cut, so twenty of the twenty-two others fill the twenty places. */
            Assert.Equal(20, fact.Metadata["offender_count"]);
            Assert.Equal(1, fact.Metadata["cross_input_excluded_count"]);
            Assert.Equal(20, pass.PlanRegressionOffenders!.Count);
            Assert.DoesNotContain(pass.PlanRegressionOffenders, o => o.QueryId == Split);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    [Fact]
    public async Task PlansThatCannotBeRead_AreOneEventForThePass_AndDoNotMarkTheFamilyNotRead_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #5630 plan-read failure test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = TruncateToSeconds(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc));
            var periodStart = periodEnd.AddHours(-4);
            const string Text = "SELECT o.order_id FROM dbo.orders AS o WHERE o.customer_id = @location_id";

            /* Three regressed queries whose stored plans are not readable: every plan read fails. */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                for (long q = 400; q < 403; q++)
                {
                    await InsertPlanAsync(connection, q * 10 + 1, PlanFor(Text, "(7)"), gz: false, periodEnd, ct, FailServerId, corruptGzip: true);
                    await InsertPlanAsync(connection, q * 10 + 2, PlanFor(Text, "(7)"), gz: false, periodEnd, ct, FailServerId, corruptGzip: true);
                    await InsertStatsAsync(connection, FailServerId, q, q * 10 + 1, "0xBEST" + q, 200_000, periodStart.AddDays(-5), periodStart, periodEnd, false, ct);
                    await InsertStatsAsync(connection, FailServerId, q, q * 10 + 2, "0xLATE" + q, 600_000 + (q * 1_000), periodEnd, periodStart, periodEnd, false, ct);
                    await InsertTextAsync(connection, FailServerId, q, Text, periodEnd, ct);
                }
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var pass = NewContext(FailServerId, periodStart, periodEnd);
            var fact = (await new PgFactCollector(postgres).CollectFactsAsync(pass))
                .Single(f => f.Key == "PLAN_REGRESSION");

            /* The check could not run, so the three stay, counted unverified, as before the check existed. */
            Assert.Equal(3, fact.Metadata["offender_count"]);
            Assert.Equal(0, fact.Metadata["cross_input_excluded_count"]);
            Assert.Equal(3, fact.Metadata["inputs_unverified_count"]);

            /* The fact was produced and carries the gap, so the family is not recorded as unread, and certainly not once
               per failed read. */
            Assert.DoesNotContain(pass.CollectionFailures, f => f.Read == "CollectPlanRegressionFactsAsync");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    /* ── The seed ──────────────────────────────────────────────────────────────────────────────────────
       Each query has a cheap plan that last ran five days back and a costlier plan running at the window's end. */
    private static async Task SeedAsync(
        NpgsqlConnection connection, DateTime periodStart, DateTime periodEnd, bool intervalTable, CancellationToken ct,
        long[]? only = null)
    {
        bool On(long queryId) => only is null || only.Contains(queryId);
        var best = periodStart.AddDays(-5);
        const string ManifestText = "SELECT TOP (50) m.manifest_id FROM dbo.manifest AS m WHERE m.location_id = @location_id ORDER BY m.amount DESC";
        const string LookupText = "SELECT i.item_id FROM dbo.item AS i WHERE i.package_id = @package_id OPTION (RECOMPILE)";
        const string RealText = "SELECT o.order_id FROM dbo.orders AS o WHERE o.customer_id = @location_id";

        {
            if (intervalTable)
            {
                /* The table is read only when its oldest row reaches below the window's start; one old, steady query does it. */
                await InsertStatsAsync(connection, ServerId, 99, 991, "0xOLD", 2_000, periodStart.AddDays(-16), periodStart, periodEnd, intervalTable, ct);
            }

            /* 1: a recompiling lookup, three plan hashes, the slow one 400x the cheap one. Its plans carry the SAME compiled
               values: it is the hint that excludes it. */
            if (On(RecompileLookup))
            {
                await InsertStatsAsync(connection, ServerId, RecompileLookup, 11, "0xA1", 1_000, best, periodStart, periodEnd, intervalTable, ct);
                await InsertStatsAsync(connection, ServerId, RecompileLookup, 12, "0xA2", 5_000, best.AddHours(1), periodStart, periodEnd, intervalTable, ct);
                await InsertStatsAsync(connection, ServerId, RecompileLookup, 13, "0xA3", 400_000, periodEnd, periodStart, periodEnd, intervalTable, ct);
                await InsertTextAsync(connection, ServerId, RecompileLookup, LookupText, periodEnd, ct);
                foreach (var planId in new long[] { 11, 12, 13 })
                {
                    await InsertPlanAsync(connection, planId, PlanFor(LookupText, null), gz: true, periodEnd, ct);
                }
            }

            /* 2 and 3: parameter-sensitive lists, two hashes, compiled for different locations over the same statistics: only the
               values differ, so a real change of input and not a plan regression. */
            if (On(ManifestsB) || On(ManifestsA))
            {
                foreach (var (queryId, bestPlan, latestPlan, latestCpu) in new[] { (ManifestsB, 21L, 22L, 100_000L), (ManifestsA, 31L, 32L, 50_000L) })
                {
                    await InsertStatsAsync(connection, ServerId, queryId, bestPlan, "0xB" + bestPlan, 1_000, best, periodStart, periodEnd, intervalTable, ct);
                    await InsertStatsAsync(connection, ServerId, queryId, latestPlan, "0xB" + latestPlan, latestCpu * 10, periodEnd, periodStart, periodEnd, intervalTable, ct);
                    await InsertTextAsync(connection, ServerId, queryId, ManifestText, periodEnd, ct);
                    await InsertPlanAsync(connection, bestPlan, PlanFor(ManifestText, "(201)", StatsEarly), gz: true, periodEnd, ct);
                    await InsertPlanAsync(connection, latestPlan, PlanFor(ManifestText, "(1)", StatsEarly), gz: queryId == ManifestsB, periodEnd, ct);
                }
            }

            /* 4: the control: one hash for every call, so there is nothing to compare. */
            if (On(ManifestsControl))
            {
                await InsertStatsAsync(connection, ServerId, ManifestsControl, 41, "0xC1", 200_000, periodEnd, periodStart, periodEnd, intervalTable, ct);
                await InsertTextAsync(connection, ServerId, ManifestsControl, ManifestText, periodEnd, ct);
                await InsertPlanAsync(connection, 41, PlanFor(ManifestText, "(1)"), gz: true, periodEnd, ct);
            }

            /* 5: a real regression: two hashes, the SAME compiled values, the latest 3x the best. */
            if (On(RealRegression))
            {
                await InsertStatsAsync(connection, ServerId, RealRegression, 51, "0xD1", 200_000, best, periodStart, periodEnd, intervalTable, ct, avgRowcount: 996);
                await InsertStatsAsync(connection, ServerId, RealRegression, 52, "0xD2", 600_000, periodEnd, periodStart, periodEnd, intervalTable, ct, avgRowcount: 996);
                await InsertTextAsync(connection, ServerId, RealRegression, RealText, periodEnd, ct);
                await InsertPlanAsync(connection, 51, PlanFor(RealText, "(7)"), gz: true, periodEnd, ct);
                await InsertPlanAsync(connection, 52, PlanFor(RealText, "(7)"), gz: true, periodEnd, ct);
            }

            /* 6: no plans stored: the check cannot run, so the query stays and is counted. */
            if (On(NoPlansStored))
            {
                await InsertStatsAsync(connection, ServerId, NoPlansStored, 61, "0xE1", 200_000, best, periodStart, periodEnd, intervalTable, ct);
                await InsertStatsAsync(connection, ServerId, NoPlansStored, 62, "0xE2", 800_000, periodEnd, periodStart, periodEnd, intervalTable, ct);
                await InsertTextAsync(connection, ServerId, NoPlansStored, RealText, periodEnd, ct);
            }

            /* 7: a statement the filter withheld: its text and its compiled values are the placeholder, so two placeholders
               must never compare Same. The plans go through the real filter, as the collector stores them. */
            if (On(Withheld))
            {
                var secret = "CREATE LOGIN [w5630] WITH PASSWORD = N'S3cret-5630'";
                await InsertStatsAsync(connection, ServerId, Withheld, 71, "0xF1", 200_000, best, periodStart, periodEnd, intervalTable, ct);
                await InsertStatsAsync(connection, ServerId, Withheld, 72, "0xF2", 1_000_000, periodEnd, periodStart, periodEnd, intervalTable, ct);
                await InsertTextAsync(connection, ServerId, Withheld, SensitiveStatements.PlaceholderText, periodEnd, ct);
                await InsertPlanAsync(connection, 71, SensitiveStatements.Xml(PlanFor(secret, "(5)"))!, gz: true, periodEnd, ct);
                await InsertPlanAsync(connection, 72, SensitiveStatements.Xml(PlanFor(secret, "(5)"))!, gz: true, periodEnd, ct);
            }

        {
            const string WidgetText = "SELECT w.widget_id FROM dbo.widgets AS w WHERE w.owner_id = @owner_id";

            /* 8: a plan whose cost followed a bigger output. The newer plan returns 26,000 rows per call against 50, and the cost per
               row is no worse (factor 187, 520x the rows). No plan is stored: the rows alone decide, so a read of plans would
               leave it kept as unverified instead. */
            if (On(OutputGrew))
            {
                await InsertStatsAsync(connection, ServerId, OutputGrew, 81, "0xG1", 2_000, best, periodStart, periodEnd, intervalTable, ct, avgRowcount: 50);
                await InsertStatsAsync(connection, ServerId, OutputGrew, 82, "0xG2", 374_000, periodEnd, periodStart, periodEnd, intervalTable, ct, avgRowcount: 26_000);
                await InsertTextAsync(connection, ServerId, OutputGrew, WidgetText, periodEnd, ct);
            }

            /* 9: a statistics flip. Three compiled values differ (here one), the statistics were all updated between the compiles,
               and the output is flat (0 rows): kept, with basis 2. */
            if (On(StatsFlip))
            {
                await InsertStatsAsync(connection, ServerId, StatsFlip, 91, "0xH1", 200_000, best, periodStart, periodEnd, intervalTable, ct, avgRowcount: 0);
                await InsertStatsAsync(connection, ServerId, StatsFlip, 92, "0xH2", 700_000, periodEnd, periodStart, periodEnd, intervalTable, ct, avgRowcount: 0);
                await InsertTextAsync(connection, ServerId, StatsFlip, WidgetText, periodEnd, ct);
                await InsertPlanAsync(connection, 91, PlanFor(WidgetText, "(3)", StatsEarly), gz: true, periodEnd, ct);
                await InsertPlanAsync(connection, 92, PlanFor(WidgetText, "(9)", StatsLate), gz: true, periodEnd, ct);
            }

            /* 10: the older plan has an interval with no row count (avg_rowcount NULL), so its rows per call are unknown on every
               read path, the daily path included (its rowcount_sum is NULL for the day). The newer plan returns 26,000 rows, which
               would drop it were the older count known; unknown, no output is assumed, so it stays, unverified. */
            if (On(RowsUnknown))
            {
                await InsertStatsAsync(connection, ServerId, RowsUnknown, 101, "0xI1", 200_000, best, periodStart, periodEnd, intervalTable, ct);
                await InsertStatsAsync(connection, ServerId, RowsUnknown, 102, "0xI2", 500_000, periodEnd, periodStart, periodEnd, intervalTable, ct, avgRowcount: 26_000);
                await InsertTextAsync(connection, ServerId, RowsUnknown, WidgetText, periodEnd, ct);
            }
        }
        }

        if (intervalTable)
        {
            await using var coverage = new NpgsqlCommand(
                "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES ($1, $2, $2) "
                + "ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since", connection);
            coverage.Parameters.AddWithValue(ServerId);
            coverage.Parameters.AddWithValue(NpgsqlDbType.Timestamp, periodEnd.Date.AddDays(-40));
            await coverage.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task InsertStatsAsync(
        NpgsqlConnection connection, int serverId, long queryId, long planId, string planHash, long cpuUs, DateTime lastExec,
        DateTime periodStart, DateTime periodEnd, bool intervalTable, CancellationToken ct, string? replicaRole = null, long? avgRowcount = null)
    {
        var firstExec = lastExec.AddHours(-1);
        if (intervalTable)
        {
            await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count, avg_rowcount
)
VALUES ($1, $2, $3, $4, NULL, $4, $5, $6, $7, $8, 50, $9, $10, $11, false, 0, $12)", connection)
            { CommandTimeout = LiveTimeoutSeconds };
            cmd.Parameters.AddWithValue(serverId);
            cmd.Parameters.AddWithValue(Db);
            cmd.Parameters.AddWithValue(queryId);
            cmd.Parameters.AddWithValue(planId);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, periodEnd.AddMinutes(-5));
            cmd.Parameters.AddWithValue(planHash);
            cmd.Parameters.AddWithValue("0xQH" + queryId.ToString(CultureInfo.InvariantCulture));
            cmd.Parameters.AddWithValue(cpuUs);
            cmd.Parameters.AddWithValue(cpuUs + 20_000);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, lastExec);
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)avgRowcount ?? DBNull.Value });
            await cmd.ExecuteNonQueryAsync(ct);
            return;
        }

        await using var raw = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     query_hash, query_plan_hash, execution_count,
     avg_cpu_time_us, avg_duration_us, is_forced_plan, force_failure_count, avg_rowcount)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $16, $8, $9, $10, $11, $12, $13, 50, $14, $15, false, 0, $17)", connection)
        { CommandTimeout = LiveTimeoutSeconds };
        raw.Parameters.AddWithValue(CollectionIdGenerator.Next());
        raw.Parameters.AddWithValue(periodEnd.AddMinutes(-5));
        raw.Parameters.AddWithValue(serverId);
        raw.Parameters.AddWithValue("RegrInputsSrv");
        raw.Parameters.AddWithValue(Db);
        raw.Parameters.AddWithValue(queryId);
        raw.Parameters.AddWithValue(planId);
        raw.Parameters.AddWithValue(planId);
        raw.Parameters.AddWithValue(firstExec);
        raw.Parameters.AddWithValue(firstExec);
        raw.Parameters.AddWithValue(lastExec);
        raw.Parameters.AddWithValue("0xQH" + queryId.ToString(CultureInfo.InvariantCulture));
        raw.Parameters.AddWithValue(planHash);
        raw.Parameters.AddWithValue(cpuUs);
        raw.Parameters.AddWithValue(cpuUs + 20_000);
        raw.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)replicaRole ?? DBNull.Value });
        raw.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)avgRowcount ?? DBNull.Value });
        await raw.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertTextAsync(
        NpgsqlConnection connection, int serverId, long queryId, string text, DateTime lastSeen, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1, $2, $3, $4, $5)",
            connection) { CommandTimeout = LiveTimeoutSeconds };
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(Db);
        cmd.Parameters.AddWithValue(queryId);
        cmd.Parameters.AddWithValue(text);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, lastSeen);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertPlanAsync(
        NpgsqlConnection connection, long planId, string plan, bool gz, DateTime lastSeen, CancellationToken ct,
        int serverId = ServerId, bool corruptGzip = false)
    {
        /* A plan is stored once under its digest. The fixtures differ per plan_id by a comment, so no digest is shared
           between plans of different queries. */
        var stored = plan + "<!-- w5630 plan " + planId.ToString(CultureInfo.InvariantCulture) + " -->";
        await using (var dim = new NpgsqlCommand(
            gz || corruptGzip
                ? "INSERT INTO query_plan_dim (digest, query_plan_gz, last_seen) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
                : "INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            connection) { CommandTimeout = LiveTimeoutSeconds })
        {
            dim.Parameters.AddWithValue(PayloadDimensions.Digest(stored));
            if (corruptGzip)
            {
                /* Bytes that are not a gzip stream: reading the plan back throws. */
                dim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 } });
            }
            else if (gz)
            {
                dim.Parameters.AddWithValue(PayloadDimensions.CompressContent(stored));
            }
            else
            {
                dim.Parameters.AddWithValue(stored);
            }

            dim.Parameters.AddWithValue(NpgsqlDbType.Timestamp, lastSeen);
            await dim.ExecuteNonQueryAsync(ct);
        }

        await using var map = new NpgsqlCommand(
            "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) VALUES ($1, $2, $3, $4, $5, $6)",
            connection) { CommandTimeout = LiveTimeoutSeconds };
        map.Parameters.AddWithValue(serverId);
        map.Parameters.AddWithValue(Db);
        map.Parameters.AddWithValue(planId);
        map.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = PayloadDimensions.Digest(stored) });
        map.Parameters.AddWithValue("0x5630");
        map.Parameters.AddWithValue(NpgsqlDbType.Timestamp, lastSeen);
        await map.ExecuteNonQueryAsync(ct);
    }

    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var server in new[] { ServerId, CapServerId, ReplicaServerId, FailServerId })
        {
            foreach (var table in new[]
            {
                "query_store_stats",
                "collect.query_store_interval_latest",
                "collect.query_store_interval_latest_coverage",
                "collect.query_store_text",
                /* #5812: the days the daily case built. Left behind, a rerun on the same database finds them valid under the
                   next case's coverage claim, and the interval-table case reads the daily path. */
                "collect.plan_regression_daily",
                "collect.plan_regression_daily_built",
            })
            {
                await using var cmd = new NpgsqlCommand("DELETE FROM " + table + " WHERE server_id = $1", connection) { CommandTimeout = LiveTimeoutSeconds };
                cmd.Parameters.AddWithValue(server);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }

        /* The plans this class stored, through its map; a plan's dimension row goes with its last map row. */
        foreach (var server in new[] { ServerId, ReplicaServerId, FailServerId })
        {
            await using var dims = new NpgsqlCommand(
                "DELETE FROM query_plan_dim WHERE digest IN (SELECT digest FROM collect.query_store_plan_map WHERE server_id = $1)", connection)
            { CommandTimeout = LiveTimeoutSeconds };
            dims.Parameters.AddWithValue(server);
            await dims.ExecuteNonQueryAsync(ct);

            await using var maps = new NpgsqlCommand("DELETE FROM collect.query_store_plan_map WHERE server_id = $1", connection) { CommandTimeout = LiveTimeoutSeconds };
            maps.Parameters.AddWithValue(server);
            await maps.ExecuteNonQueryAsync(ct);
        }
    }
}
