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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7417: the plan regression card, the drill-down behind it and the force statement all rank by the CPU the regression
/// lost, and the lost-CPU floor applies on the CPU path only.
///
/// <para>Driven through the two collectors in the order the analysis service runs them, then through
/// <see cref="FactRemediation.ExtractPlanRegressionTargets"/> on the drill-down's rows, because the defect was in the
/// seam: the fact ranked by CPU lost while the drill-down it feeds still ranked by factor, so the first force target was
/// not the fact's lead offender and, past five offenders, the lead could be missing from the targets altogether.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PlanRegressionLostCpuRankingLiveTests
{
    /// <summary>Distinctive fake id: a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -741702;
    private const string TestServerName = "LostCpuRankSrv";
    private const string Db = "LostCpuRankDb";
    private const int LiveTimeoutSeconds = 60;

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private sealed record Plan(long QueryId, long PlanId, string Hash, long Execs, double CpuUs, double DurUs, bool Latest);

    [Fact]
    public async Task TheForceTargetsLeadWithTheQueryThatLostTheMostCpu_NotTheHighestFactor_AgainstDevPostgres()
    {
        var (fact, targets) = await RunAsync(
        [
            /* Query 1: 21 us to 455 us (21.7x) over 30,000 runs: loses 13 CPU-seconds. */
            new(1, 11, "A1", 30_000, 21, 21, false), new(1, 12, "B1", 30_000, 455, 455, true),
            /* Query 2: 74 ms to 1.2 s (16.2x) over 30 runs: loses 33.8 CPU-seconds. */
            new(2, 21, "A2", 30, 74_000, 74_000, false), new(2, 22, "B2", 30, 1_200_000, 1_200_000, true),
        ]);

        Assert.NotNull(fact);
        Assert.Equal(2.0, fact!.Metadata["worst_query_id"]);
        Assert.Equal([2L, 1L], targets.Select(t => t.QueryId));
    }

    [Fact]
    public async Task WithMoreThanFiveOffenders_TheLeadIsStillAForceTarget_AndIsFirst_AgainstDevPostgres()
    {
        var plans = new List<Plan>();
        /* Queries 1 to 6: big factors (29x down to 24x) but small losses (14 to 11.5 CPU-seconds). */
        for (long q = 1; q <= 6; q++)
        {
            plans.Add(new(q, (q * 10) + 1, "A" + q, 500, 1_000, 1_000, false));
            plans.Add(new(q, (q * 10) + 2, "B" + q, 500, 1_000 * (30 - q), 1_000 * (30 - q), true));
        }

        /* Query 7: only 2.5x, but 1,000 runs lose 150 CPU-seconds. By factor alone it is the seventh. */
        plans.Add(new(7, 71, "A7", 1_000, 100_000, 100_000, false));
        plans.Add(new(7, 72, "B7", 1_000, 250_000, 250_000, true));

        var (fact, targets) = await RunAsync(plans);

        Assert.NotNull(fact);
        Assert.Equal(7.0, fact!.Metadata["offender_count"]);
        Assert.Equal(7.0, fact.Metadata["worst_query_id"]);
        Assert.Equal(5, targets.Count);
        Assert.Equal(7L, targets[0].QueryId);
        Assert.Equal(2.5, fact.Metadata["lead_regression_factor"], 6);
        Assert.Equal(2.5, fact.Value, 6);
    }

    [Fact]
    public async Task ADurationPathRow_KeepsTheOldBar_AndRanksBelowTheLargerCpuLoss_AgainstDevPostgres()
    {
        var (fact, targets) = await RunAsync(
        [
            new(2, 21, "A2", 30, 74_000, 74_000, false), new(2, 22, "B2", 30, 1_200_000, 1_200_000, true),
            /* Query 4 (the duration path): 10x slower in duration, CPU only 1.3x. The latest plan burned 39 CPU-seconds and
               lost 9, under the lost-CPU floor. A statement slowed by waits is not a CPU loss and must stay listed. */
            new(4, 41, "A4", 300, 100_000, 100_000, false), new(4, 42, "B4", 300, 130_000, 1_000_000, true),
            /* Query 3 (the CPU path): 2.5x over 100,000 runs, 10 CPU-seconds burned and 6 lost. Under the floor. */
            new(3, 31, "A3", 100_000, 40, 40, false), new(3, 32, "B3", 100_000, 100, 100, true),
        ]);

        Assert.NotNull(fact);
        Assert.Equal(2.0, fact!.Metadata["offender_count"]);
        Assert.Equal([2L, 4L], targets.Select(t => t.QueryId));
        Assert.Equal(5.0, targets[1].RegressionFactor, 6);
    }

    /// <summary>Seeds the plans, runs the fact and then the drill-down on one context, and returns the fact and the force
    /// targets built from the drill-down's rows. A null fact means the test was skipped for want of a store.</summary>
    private static async Task<(Fact? Fact, IReadOnlyList<ForcePlanTarget> Targets)> RunAsync(IReadOnlyList<Plan> plans)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #7417 plan regression ranking test.");

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
                foreach (var plan in plans)
                {
                    await SeedPlanAsync(connection, plan, periodEnd, ct);
                }
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = periodStart,
                TimeRangeEnd = periodEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            var fact = (await new PgFactCollector(postgres).CollectFactsAsync(context)).Single(f => f.Key == "PLAN_REGRESSION");

            var finding = new AnalysisFinding
            {
                RootFactKey = "PLAN_REGRESSION",
                StoryPath = "PLAN_REGRESSION",
                PathKeys = ["PLAN_REGRESSION"],
                /* Past the display gate: below it the expensive drill-downs are skipped wholesale. */
                Severity = 1.0,
            };
            await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

            var targets = FactRemediation.ExtractPlanRegressionTargets(finding);

            bodySucceeded = true;
            return (fact, targets);
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    private static async Task SeedPlanAsync(NpgsqlConnection connection, Plan plan, DateTime periodEnd, CancellationToken ct)
    {
        var lastExec = plan.Latest ? periodEnd : periodEnd.AddDays(-5);
        var firstExec = lastExec.AddHours(-1);
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     query_hash, query_plan_hash, execution_count,
     avg_cpu_time_us, avg_duration_us, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', NULL, $8, $9, $10, $11, $12, $13, $14, $15, $16, false, 0)", connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        };
        cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
        cmd.Parameters.AddWithValue(periodEnd.AddMinutes(-9));
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(TestServerName);
        cmd.Parameters.AddWithValue(Db);
        cmd.Parameters.AddWithValue(plan.QueryId);
        cmd.Parameters.AddWithValue(plan.PlanId);
        cmd.Parameters.AddWithValue(plan.Latest ? 11L : 1L);
        cmd.Parameters.AddWithValue(firstExec);
        cmd.Parameters.AddWithValue(firstExec);
        cmd.Parameters.AddWithValue(lastExec);
        cmd.Parameters.AddWithValue("0xQH" + plan.QueryId.ToString(CultureInfo.InvariantCulture));
        cmd.Parameters.AddWithValue(plan.Hash);
        cmd.Parameters.AddWithValue(plan.Execs);
        cmd.Parameters.AddWithValue((long)plan.CpuUs);
        cmd.Parameters.AddWithValue((long)plan.DurUs);
        await cmd.ExecuteNonQueryAsync(ct);
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
        await using var cmd = new NpgsqlCommand("DELETE FROM query_store_stats WHERE server_id = $1", connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        };
        cmd.Parameters.AddWithValue(TestServerId);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
