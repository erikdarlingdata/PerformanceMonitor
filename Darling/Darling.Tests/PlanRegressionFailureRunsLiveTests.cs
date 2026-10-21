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
    public Task ARisingTimeoutOrErrorRate_RaisesTheFact_WhenCostIsFlat_OnTheWideTable() => RunAsync(wideCovered: true);

    [Fact]
    public Task ARisingTimeoutOrErrorRate_RaisesTheFact_WhenCostIsFlat_OnTheRawFallback() => RunAsync(wideCovered: false);

    private static async Task RunAsync(bool wideCovered)
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
        async Task W(string type, DateTime first, long queryId, long executions)
        {
            id++;
            var row = new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = queryId * 10,
                ExecutionTypeDesc = type,
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(30),
                QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
                ExecutionCount = executions,
                AvgCpuTimeUs = 1_000,
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

        Assert.Equal(PlanRegressionFailureRule.ValueHigh, fact.Value);
        Assert.Equal(2, fact.Metadata["failure_offender_count"]);
        Assert.Equal(1, fact.Metadata["timeout_offender_count"]);
        Assert.Equal(10, fact.Metadata["worst_timeout_query_id"]);
        Assert.Equal(4, fact.Metadata["worst_timeout_recent_runs"]);
        Assert.Equal(0, fact.Metadata["worst_timeout_baseline_runs"]);
        Assert.Equal(1, fact.Metadata["error_offender_count"]);
        Assert.Equal(13, fact.Metadata["worst_error_query_id"]);
        Assert.Equal(5, fact.Metadata["worst_error_recent_runs"]);

        /* No plan's cost regressed, so the fact carries no cost figure and names no cost offender. */
        Assert.Equal(0, fact.Metadata["offender_count"]);
        Assert.False(fact.Metadata.ContainsKey("worst_regression_factor"));
        Assert.Empty(pass.PlanRegressionOffenders ?? []);
    }
}
