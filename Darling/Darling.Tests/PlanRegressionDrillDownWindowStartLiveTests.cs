/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5448's drill-down wiring: the regressed-queries drill-down starts its window at
/// <see cref="AnalysisContext.PlanRegressionWindowStart"/> when the fact set it, and at the exact edge
/// (<c>TimeRangeStart - 14 days</c>) when it is null.
///
/// <para>The seed has one query with a cheap plan whose only interval ran at 03:00 on the first day of the window, and an
/// expensive plan that ran yesterday. The exact edge of this analysis is 10:00 that first day, so the cheap plan is
/// before it and the comparison needs the day-aligned edge M (00:00) to see it: with
/// <c>PlanRegressionWindowStart = M</c> the drill-down reports the regression with the cheap plan as the best plan, and with
/// it null the cheap plan is outside the window, the query has one plan and nothing regressed. A drill-down that ignored the
/// field would give the second answer in both cases; one that always used M would give the first in both.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionDrillDownWindowStartLiveTests
{
    private const int ServerId = -5448301;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheDrillDown_StartsAtPlanRegressionWindowStartWhenSet_AndAtTheExactEdgeWhenNull()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 drill-down wiring test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var today = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        var timeRangeStart = today.AddHours(10);
        var windowFloor = today.AddDays(-14);
        Assert.Equal(windowFloor, PgFactCollector.PlanRegressionWindowFloor(timeRangeStart.AddDays(-14)));

        await InsertIntervalAsync(connection, planId: 1, first: windowFloor.AddHours(3), executions: 100, cpuUs: 1000, ct);
        await InsertIntervalAsync(connection, planId: 2, first: today.AddDays(-1).AddHours(8), executions: 1000, cpuUs: 50000, ct);

        /* The fact set M: the cheap plan's day is in the window. */
        var withFloor = NewContext(timeRangeStart);
        withFloor.PlanRegressionReadsIntervalTable = true;
        withFloor.PlanRegressionWindowStart = windowFloor;
        var rows = await DrillDownRowsAsync(postgres, withFloor);
        var row = Assert.Single(rows);
        Assert.Equal(1L, row.GetProperty("best_plan_id").GetInt64());
        Assert.Equal("hash1", row.GetProperty("best_plan_hash").GetString());
        Assert.Equal(7L, row.GetProperty("query_id").GetInt64());

        /* No day totals this pass: the exact edge, as before #5448. The cheap plan ran before it. */
        var withoutFloor = NewContext(timeRangeStart);
        withoutFloor.PlanRegressionReadsIntervalTable = true;
        withoutFloor.PlanRegressionWindowStart = null;
        Assert.Empty(await DrillDownRowsAsync(postgres, withoutFloor));
    }

    private static AnalysisContext NewContext(DateTime timeRangeStart) => new()
    {
        ServerId = ServerId,
        ServerName = "RegrWindowSrv",
        TimeRangeStart = timeRangeStart,
        TimeRangeEnd = timeRangeStart.AddHours(1),
        ServerUtcOffset = TimeSpan.Zero,
    };

    private static async Task InsertIntervalAsync(
        NpgsqlConnection connection, long planId, DateTime first, long executions, long cpuUs, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
VALUES (@s, 'wsdb', 7, @p, NULL, @p, @f, @f + interval '35 minutes', 'hash' || @p, 'qh7', @e, @c, @c * 3,
        @f + interval '30 minutes', false, 0)", connection);
        command.Parameters.AddWithValue("s", ServerId);
        command.Parameters.AddWithValue("p", planId);
        command.Parameters.AddWithValue("f", NpgsqlDbType.Timestamp, first);
        command.Parameters.AddWithValue("e", executions);
        command.Parameters.AddWithValue("c", cpuUs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<List<JsonElement>> DrillDownRowsAsync(NpgsqlDataSource postgres, AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            PathKeys = ["PLAN_REGRESSION"],
            /* Past the display gate: below it the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("regressed_queries", out var raw))
            return [];

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }
}
