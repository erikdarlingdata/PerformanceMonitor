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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5506: the regressions read takes its baseline's closed days from <c>collect.plan_regression_daily</c> (#5448) and the
/// rest live, and the answer is the same as an all-live weighted read of the same window. Seeded through the real write
/// path over more than 7 days (raw, the wide table and the latest table together), the real builder fills the daily
/// totals, and the facts are: the served rows equal the all-live rows and a hand-worked weighted figure; a day whose
/// built row is stale is read live (its totals are poisoned, so reading them would show); a day with no built row is read
/// live; a valid day IS read from the totals (poisoned, the answer moves); an execution just either side of midnight,
/// of the window start, is counted exactly once; and the output carries no reads fields.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it. It shares the gap-cache collection with the other
   regressions table tests, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsDailyBaselineLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "qsDaily";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task SeedAsync(NpgsqlDataSource postgres, DateTime day0, DateTime start, CancellationToken ct)
    {
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsdaily-host", CollectionTime = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsdaily", Host = "qsdaily-host" },
            ConnectionString = "Server=qsdaily-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsdaily-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long intervalId = 7000;
        /* One interval, written once, last execution 30 minutes after its first. Its day, for the daily totals and for the
           read's split, is the day of that last execution. */
        async Task IntervalAsync(DateTime first, long queryId, long planId, long executions, long cpuUs)
        {
            intervalId++;
            var row = new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = planId,
                ExecutionTypeDesc = "Regular",
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(30),
                QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + planId.ToString("X8", CultureInfo.InvariantCulture),
                QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
                ExecutionCount = executions,
                AvgCpuTimeUs = cpuUs,
                AvgDurationUs = cpuUs * 2,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = intervalId,
                IntervalStartTimeUtc = first,
            };
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, [row], server, first.AddMinutes(40), context, ct);
        }

        /* The table's floor: an interval before the baseline, outside both arms, so the gate's #5541 edge holds. */
        await IntervalAsync(day0.AddDays(-3).AddHours(10), 30, 301, 5, 1_000);

        /* Query 10, the regressing one. Day names are day0 + n, so D-7 is day0 + 1. The baseline is [start - 7 days, start). */
        await IntervalAsync(day0.AddDays(1).AddHours(-0.33), 10, 101, 5, 4_000);      /* first D-8 23:40, last D-7 00:10: the leading seam, built day D-7 */
        await IntervalAsync(day0.AddDays(2).AddHours(10), 10, 101, 100, 1_000);       /* D-6 10:00 */
        await IntervalAsync(day0.AddDays(4).AddHours(-0.33), 10, 101, 1, 9_000);      /* first D-5 23:40, last D-4 00:10: a seam inside the run, day D-4 */
        await IntervalAsync(day0.AddDays(5).AddHours(10), 10, 101, 10, 2_000);        /* D-3 10:00 */
        await IntervalAsync(day0.AddDays(6).AddHours(-0.33), 10, 101, 7, 3_000);      /* first D-2 23:40, last D-1 00:10: the trailing seam, an open day read live */
        await IntervalAsync(start.AddMinutes(-50), 10, 101, 20, 1_500);               /* last = start - 20 min: baseline, live */
        await IntervalAsync(start.AddMinutes(-10), 10, 102, 50, 5_000);               /* last = start + 20 min: recent, never baseline */
        await IntervalAsync(start.AddHours(2), 10, 102, 10, 4_000);                   /* recent */
        /* Query 20 does not regress. */
        foreach (var hours in new[] { 24.0 * 2 + 10, 24.0 * 5 + 10, 24.0 * 7 + 3, 24.0 * 7 + 9 })
        {
            await IntervalAsync(day0.AddHours(hours), 20, 201, 8, 3_000);
        }
    }

    private static string Describe(DarlingQueryStoreRegressionReader.RegressionRow r) => string.Join("|",
        r.DatabaseName, r.QueryId, Math.Round(r.BaselineDurationMs, 6), Math.Round(r.RecentDurationMs, 6), Math.Round(r.BaselineCpuMs, 6),
        Math.Round(r.RecentCpuMs, 6), Math.Round(r.AdditionalDurationMs, 6), r.BaselineExecCount, r.RecentExecCount,
        r.BaselinePlanCount, r.RecentPlanCount, r.Severity, r.QueryTextSample, r.LastExecutionTime);

    /// <summary>One form of the statement run by hand with a given plan: the all-live reference is <see cref="QueryStoreRegressionBaseline.AllLive"/>.</summary>
    private static async Task<List<string>> RunAsync(
        NpgsqlConnection connection, string sql, DateTime start, DateTime end, DateTime baselineStart, QueryStoreRegressionBaseline.Plan plan, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
        command.Parameters.Add(DatabaseFilter.One(Db).Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 50 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.LowEnd });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.HighStart });
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(Describe(new DarlingQueryStoreRegressionReader.RegressionRow(
                reader.GetString(0), reader.GetInt64(1), reader.GetDouble(2), reader.GetDouble(3), null, reader.GetDouble(5), reader.GetDouble(6),
                null, reader.GetDouble(8), reader.GetInt64(9), reader.GetInt64(10), reader.GetInt32(11), reader.GetInt32(12),
                reader.GetString(13), reader.GetString(14), reader.IsDBNull(15) ? null : reader.GetDateTime(15))));
        }

        return rows;
    }

    private static async Task<QueryStoreRegressionBaseline.Plan> ResolveAsync(NpgsqlDataSource postgres, DateTime baselineStart, DateTime start, CancellationToken ct)
    {
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        return await QueryStoreRegressionBaseline.ResolveAsync(connection, transaction, ServerId, baselineStart, start, 60, null, ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static string DayText(DateTime d) => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    [Fact]
    public async Task TheBaselineFromTheDailyTotals_IsTheAllLiveWeightedBaseline_AtEverySeam()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'qsdaily', 'qsdaily', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        /* The window starts at 06:00 yesterday, so at any hour of the day the baseline holds D-7 through D-2 whole, and its
           edges (D-8, D-1 up to 06:00) are partial days. day0 is D-8's midnight. */
        var today = LiveClock.Now().Date;
        var start = today.AddDays(-1).AddHours(6);
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);
        var day0 = today.AddDays(-8);
        await SeedAsync(postgres, day0, start, ct);
        var end = LiveClock.Now();
        end = end.AddTicks(-(end.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, baselineStart.AddDays(-5), end, null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(-3), ct);
        await ExecAsync(connection, $"UPDATE collect.query_store_interval_latest_coverage SET filled_since = TIMESTAMP '{baselineStart.AddDays(-3):yyyy-MM-dd HH:mm:ss}' WHERE server_id = {ServerId}", ct);
        Assert.Equal(1L, await ScalarAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_latest_coverage WHERE server_id = {ServerId}", ct));

        /* The real builder fills the daily totals. */
        var built = await PlanRegressionDaily.RunTickAsync(postgres, LiveClock.Now(), NullLogger.Instance, ct);
        Assert.True(built.Built > 0, "the builder built no day");

        string Day(int offset) => DayText(today.AddDays(offset));
        var wholeDays = Enumerable.Range(-7, 6).Select(o => DateOnly.FromDateTime(today.AddDays(o))).ToArray();

        /* The reference: the same window read all live, from the wide table and from raw. Hand-worked for query 10 (Regular,
           weighted per execution): baseline 143 executions and 200,000 us of CPU = 1.398601 ms, so the day-seam intervals
           (first 23:40, last 00:10) count once each, in the day of their last execution. Recent 60 executions, (50 x 5000 + 10 x
           4000) / 60 = 4.833333 ms: the interval that began 10 minutes before the window start ended inside it, so it is recent
           and not baseline. */
        var allLive = QueryStoreRegressionBaseline.AllLive(start);
        var reference = await RunAsync(connection, DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: false), start, end, baselineStart, allLive, ct);
        var rawReference = await RunAsync(connection, DarlingQueryStoreRegressionReader.BuildSql(wide: false, useDailyTotals: false), start, end, baselineStart, allLive, ct);
        Assert.Equal(reference, rawReference);
        var only = Assert.Single(reference);
        var fields = only.Split('|');
        Assert.Equal("10", fields[1]);
        Assert.Equal(200_000.0 / 143 / 1000, double.Parse(fields[4], CultureInfo.InvariantCulture), 5);
        Assert.Equal(143L, long.Parse(fields[7], CultureInfo.InvariantCulture));
        Assert.Equal(60L, long.Parse(fields[8], CultureInfo.InvariantCulture));
        Assert.Equal(290_000.0 / 60 / 1000, double.Parse(fields[5], CultureInfo.InvariantCulture), 5);

        async Task<List<string>> ServedAsync()
        {
            var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(postgres, ServerId, start, end, DatabaseFilter.One(Db), 50, baselineStart, ct);
            return served.Rows.Select(Describe).ToList();
        }

        /* 1. Fresh: D-7 through D-2 come from the daily totals, and the served rows are the all-live rows. */
        var plan = await ResolveAsync(postgres, baselineStart, start, ct);
        Assert.Equal(wholeDays, plan.RunDays);
        Assert.Equal(reference, await ServedAsync());
        var fromTotals = await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql, start, end, baselineStart, plan, ct);
        Assert.Equal(reference, fromTotals);
        var fromTotalsRaw = await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, start, end, baselineStart, plan, ct);
        Assert.Equal(reference, fromTotalsRaw);

        /* 2. A stale built day is read live. Its totals are poisoned and its late_seq has moved, so reading them would show. */
        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 5 WHERE server_id = {ServerId} AND day = DATE '{Day(-6)}'", ct);
        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily_built SET late_seq = late_seq + 1 WHERE server_id = {ServerId} AND day = DATE '{Day(-6)}'", ct);
        plan = await ResolveAsync(postgres, baselineStart, start, ct);
        Assert.DoesNotContain(DateOnly.FromDateTime(today.AddDays(-6)), plan.RunDays);
        Assert.Equal(reference, await ServedAsync());
        Assert.Equal(reference, await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql, start, end, baselineStart, plan, ct));
        Assert.Equal(reference, await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, start, end, baselineStart, plan, ct));

        /* 3. A day with no built row is read live: the row of D-4, the day that holds the seam interval, is gone. */
        await ExecAsync(connection, $"DELETE FROM collect.plan_regression_daily_built WHERE server_id = {ServerId} AND day = DATE '{Day(-4)}'", ct);
        await ExecAsync(connection, $"DELETE FROM collect.plan_regression_daily WHERE server_id = {ServerId} AND day = DATE '{Day(-4)}'", ct);
        plan = await ResolveAsync(postgres, baselineStart, start, ct);
        Assert.DoesNotContain(DateOnly.FromDateTime(today.AddDays(-4)), plan.RunDays);
        Assert.Equal(reference, await ServedAsync());
        Assert.Equal(reference, await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql, start, end, baselineStart, plan, ct));
        Assert.Equal(reference, await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, start, end, baselineStart, plan, ct));

        /* 4. A VALID day is read from the totals: poison one and the answer moves, so the built table is what is read. */
        var valid = Assert.Single(plan.RunDays, d => d == DateOnly.FromDateTime(today.AddDays(-3)));
        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 5 WHERE server_id = {ServerId} AND day = DATE '{valid:yyyy-MM-dd}'", ct);
        var poisoned = await ServedAsync();
        Assert.NotEqual(reference, poisoned);
        /* The executions did not move, only the cost the poisoned day carried. */
        Assert.Equal(143L, long.Parse(poisoned[0].Split('|')[7], CultureInfo.InvariantCulture));
    }
}
