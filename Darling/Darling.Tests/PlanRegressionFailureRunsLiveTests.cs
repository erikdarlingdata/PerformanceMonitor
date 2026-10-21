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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6267 (Erik, 2026-10-21): the PLAN_REGRESSION fact counted Regular runs only, so a query whose runs turned into timeouts
/// (Aborted) or errors (Exception) while its Regular cost stayed flat was invisible. Seeded through the real write path, read
/// through the real collector, on the wide table's gate and on the raw fallback: a query whose timeouts or errors rise past
/// #6260's gates (<see cref="PlanRegressionFailureRule.MinRecentRuns"/> runs, <see cref="PlanRegressionFailureRule.RateMultiple"/>
/// times the baseline's per-hour rate or a baseline of none) raises the fact with the figures, a falling rate and a count below
/// the minimum do not, and no cost figure appears because no plan's cost regressed.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and then works entirely inside it. */
[Collection("gap-cache-serial")]
public sealed class PlanRegressionFailureRunsLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "failruns";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public Task ARisingTimeoutOrErrorRate_RaisesTheFact_WhenCostIsFlat_OnTheWideTable() => RunAsync(wideCovered: true, costFactor: null);

    [Fact]
    public Task ARisingTimeoutOrErrorRate_RaisesTheFact_WhenCostIsFlat_OnTheRawFallback() => RunAsync(wideCovered: false, costFactor: null);

    /// <summary>A plan whose cost regressed 3x AND a query whose timeouts rose: the fact carries both sets of metadata and
    /// keeps the larger value, which here is the failures' (<see cref="PlanRegressionFailureRule.ValueHigh"/>, 6, over 3).</summary>
    [Fact]
    public Task ACostRegressionAndAFailureRise_KeepBothSetsOfMetadata_AndTheLargerValue_FailuresLarger() => RunAsync(wideCovered: true, costFactor: 3);

    /// <summary>The same with the cost regressed 12x: the larger value is the cost factor's, 12, over the failures' 6.</summary>
    [Fact]
    public Task ACostRegressionAndAFailureRise_KeepBothSetsOfMetadata_AndTheLargerValue_CostLarger() => RunAsync(wideCovered: true, costFactor: 12);

    private static async Task RunAsync(bool wideCovered, double? costFactor)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #6267 failure-runs test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'failruns', 'failruns', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        var now = LiveClock.Now();
        var end = now.AddTicks(-(now.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "failruns-host", CollectionTime = DateTime.SpecifyKind(now, DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "failruns", Host = "failruns-host" },
            ConnectionString = "Server=failruns-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "failruns-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long id = 7000;
        async Task W(string type, DateTime first, long queryId, long executions, long? planId = null, long avgCpuUs = 1_000)
        {
            id++;
            var row = new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = planId ?? queryId * 10,
                ExecutionTypeDesc = type,
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(30),
                QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + (planId ?? queryId).ToString("X8", CultureInfo.InvariantCulture),
                QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
                ExecutionCount = executions,
                AvgCpuTimeUs = avgCpuUs,
                AvgDurationUs = 2_000,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = id,
                IntervalStartTimeUtc = first,
            };
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, [row], server, first.AddMinutes(40), context, ct);
        }

        var b1 = end.AddDays(-5);
        var b2 = end.AddDays(-3);
        var r2 = end.AddHours(-5);
        var r3 = end.AddHours(-3);

        /* The table's floor: an interval before the read's window, so the wide gate finds the table covers it. */
        await W("Regular", end.AddDays(-17), 90, 5);

        /* Every query runs the SAME plan at the SAME cost before and during the recent window: no plan's cost regressed. */
        foreach (var q in new long[] { 10, 11, 12, 13, 16 })
        {
            await W("Regular", b1, q, 100);
            await W("Regular", r2, q, 100);
        }

        /* 10: timeouts 0 -> 4 recent: flagged, HIGH (the baseline had none). */
        await W("Aborted", r2, 10, 2);
        await W("Aborted", r3, 10, 2);
        /* 11: 80 baseline timeouts (about 0.26 an hour) and 3 recent (0.125 an hour): the rate fell, not flagged. */
        await W("Aborted", b1, 11, 40);
        await W("Aborted", b2, 11, 40);
        await W("Aborted", r2, 11, 3);
        /* 12: two recent timeouts, below the minimum of FailureMinRecentRuns: not flagged. */
        await W("Aborted", r2, 12, 2);
        /* 13: errors 0 -> 5 recent: flagged. */
        await W("Exception", r2, 13, 2);
        await W("Exception", r3, 13, 3);
        /* 16: 168 baseline errors (about 0.54 an hour) and 5 recent (0.21 an hour): not flagged. */
        await W("Exception", b1, 16, 84);
        await W("Exception", b2, 16, 84);
        await W("Exception", r2, 16, 5);

        /* 20 (only when a cost regression is asked for): a cheap plan in the baseline and a costlier one now, both with the
           executions and the CPU the cost read needs (25 runs a plan, 10 seconds of CPU in the newer plan). */
        if (costFactor is { } factor)
        {
            await W("Regular", b1, 20, 100, planId: 201, avgCpuUs: 100_000);
            await W("Regular", r2, 20, 100, planId: 202, avgCpuUs: (long)(100_000 * factor));
        }

        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, end.AddDays(-20), end, null, null, ct);
        if (wideCovered)
        {
            await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, end.AddDays(-19), ct);
        }

        var pass = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "failruns-host",
            TimeRangeStart = end.AddHours(-1),
            TimeRangeEnd = end,
            ServerUtcOffset = TimeSpan.Zero,
        };
        var facts = await new PgFactCollector(postgres).CollectFactsAsync(pass);
        var fact = Assert.Single(facts, f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(costFactor is { } f ? Math.Max(f, PlanRegressionFailureRule.ValueHigh) : PlanRegressionFailureRule.ValueHigh, fact.Value);
        Assert.Equal(2, fact.Metadata["failure_offender_count"]);
        Assert.Equal(1, fact.Metadata["timeout_offender_count"]);
        Assert.Equal(10, fact.Metadata["worst_timeout_query_id"]);
        Assert.Equal(4, fact.Metadata["worst_timeout_recent_runs"]);
        Assert.Equal(0, fact.Metadata["worst_timeout_baseline_runs"]);
        Assert.Equal(1, fact.Metadata["error_offender_count"]);
        Assert.Equal(13, fact.Metadata["worst_error_query_id"]);
        Assert.Equal(5, fact.Metadata["worst_error_recent_runs"]);

        /* The cost figures are there only when a plan's cost regressed, and then they are query 20's. */
        if (costFactor is { } expectedFactor)
        {
            Assert.Equal(1, fact.Metadata["offender_count"]);
            Assert.Equal(expectedFactor, fact.Metadata["worst_regression_factor"], 3);
            Assert.Equal(20, fact.Metadata["worst_query_id"]);
            Assert.Equal(new PlanRegressionOffender(Db, 20), Assert.Single(pass.PlanRegressionOffenders!));
        }
        else
        {
            Assert.Equal(0, fact.Metadata["offender_count"]);
            Assert.False(fact.Metadata.ContainsKey("worst_regression_factor"));
            Assert.Empty(pass.PlanRegressionOffenders ?? []);
        }

        /* The drill-down lists the failing queries next to the cost offender (#6267): query 10 and query 13 with their recent
           and baseline counts, and the cost row only when a plan's cost regressed. A drill-down that lists the cost offenders
           alone leaves both failure-only queries out. */
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            PathKeys = ["PLAN_REGRESSION"],
            Severity = 1.0,
        };
        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], pass);
        Assert.NotNull(finding.DrillDown);
        var failureRows = JsonSerializer.SerializeToElement(finding.DrillDown![PlanRegressionFailureRule.DrillDownKey]).EnumerateArray().ToList();
        Assert.Equal([10L, 13L], failureRows.Select(r => r.GetProperty("query_id").GetInt64()).Order());
        var timeouts = failureRows.Single(r => r.GetProperty("query_id").GetInt64() == 10);
        Assert.Equal(4, timeouts.GetProperty("recent_timeouts").GetInt64());
        Assert.Equal(0, timeouts.GetProperty("baseline_timeouts").GetInt64());
        Assert.True(timeouts.GetProperty("timeouts_rose").GetBoolean());
        Assert.False(timeouts.GetProperty("errors_rose").GetBoolean());
        var errors = failureRows.Single(r => r.GetProperty("query_id").GetInt64() == 13);
        Assert.Equal(5, errors.GetProperty("recent_errors").GetInt64());
        Assert.Equal(0, errors.GetProperty("baseline_errors").GetInt64());
        Assert.True(errors.GetProperty("errors_rose").GetBoolean());
        if (costFactor is null)
        {
            /* A fact raised by failures alone: no cost row, and the cost read is not run over "every query". */
            Assert.False(finding.DrillDown.ContainsKey("regressed_queries"));
        }
        else
        {
            var cost = Assert.Single(JsonSerializer.SerializeToElement(finding.DrillDown["regressed_queries"]).EnumerateArray());
            Assert.Equal(20, cost.GetProperty("query_id").GetInt64());
        }
    }
}
