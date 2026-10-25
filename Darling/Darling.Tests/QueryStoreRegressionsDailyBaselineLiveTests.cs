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

    /// <summary>The re-collected interval of the extra fixture (query 41): its identity, so the second snapshot upserts the first.</summary>
    private const long RecollectedIntervalId = 7900;

    private static async Task SeedAsync(NpgsqlDataSource postgres, DateTime day0, DateTime start, CancellationToken ct, bool extra = false)
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
        async Task IntervalAsync(DateTime first, long queryId, long planId, long executions, long cpuUs, long? reuseIntervalId = null, int lastMinutes = 30)
        {
            if (reuseIntervalId is null)
            {
                intervalId++;
            }

            var row = new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = planId,
                ExecutionTypeDesc = "Regular",
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(lastMinutes),
                QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + planId.ToString("X8", CultureInfo.InvariantCulture),
                QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
                ExecutionCount = executions,
                AvgCpuTimeUs = cpuUs,
                AvgDurationUs = cpuUs * 2,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = reuseIntervalId ?? intervalId,
                IntervalStartTimeUtc = first,
            };
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, [row], server, first.AddMinutes(lastMinutes + 10), context, ct);
        }

        /* The table's floor: an interval before the baseline, outside both arms, so the gate's #5541 edge holds. */
        await IntervalAsync(day0.AddDays(-3).AddHours(10), 30, 301, 5, 1_000);

        /* Query 10, the regressing one. Day names are day0 + n, so D-7 is day0 + 1. The baseline is [start - 7 days, start). */
        await IntervalAsync(day0.AddDays(1).AddHours(-0.33), 10, 101, 5, 4_000);      /* first D-8 23:40, last D-7 00:10: the leading seam, built day D-7 */
        await IntervalAsync(day0.AddDays(2).AddHours(10), 10, 101, 100, 1_000);       /* D-6 10:00 */
        await IntervalAsync(day0.AddDays(4).AddHours(-0.33), 10, 101, 1, 9_000);      /* first D-5 23:40, last D-4 00:10: a seam inside the run, day D-4 */
        await IntervalAsync(day0.AddDays(5).AddHours(10), 10, 101, 10, 2_000);        /* D-3 10:00 */
        await IntervalAsync(day0.AddDays(6).AddHours(-0.33), 10, 101, 7, 3_000);      /* first D-3 23:40, last D-2 00:10: the trailing seam, on the last run day D-2, so the daily totals serve it */
        await IntervalAsync(start.AddMinutes(-50), 10, 101, 20, 1_500);               /* last = start - 20 min: baseline, live */
        await IntervalAsync(start.AddMinutes(-10), 10, 102, 50, 5_000);               /* last = start + 20 min: recent, never baseline */
        await IntervalAsync(start.AddHours(2), 10, 102, 10, 4_000);                   /* recent */
        /* Query 20 does not regress. */
        foreach (var hours in new[] { 24.0 * 2 + 10, 24.0 * 5 + 10, 24.0 * 7 + 3, 24.0 * 7 + 9 })
        {
            await IntervalAsync(day0.AddHours(hours), 20, 201, 8, 3_000);
        }

        if (extra)
        {
            /* Query 40, two plans in the baseline: plan 401 on D-6 (a built day) and plan 402 just before the window start (read
               live), one plan recently. Its CPU triples recently, so it is listed. */
            await IntervalAsync(day0.AddDays(2).AddHours(11), 40, 401, 20, 1_000);
            await IntervalAsync(start.AddMinutes(-50), 40, 402, 20, 1_000);
            await IntervalAsync(start.AddHours(3), 40, 401, 20, 3_000);

            /* Query 41, an interval re-collected with a growing count after the day was built, and its last execution crossing
               midnight: first D-5 23:00, first snapshot last D-5 23:30 with 10 executions; the second (written after the build,
               see RecollectAsync) last D-4 00:30 with 25. */
            await IntervalAsync(day0.AddDays(3).AddHours(23), 41, 411, 10, 1_000, RecollectedIntervalId);
            await IntervalAsync(start.AddHours(4), 41, 411, 10, 5_000);

            /* Query 10 again, a seam interval on the open day after the run: first D-2 23:40, last D-1 00:10. D-1 is no run day
               and its last execution is before the window start (D-1 06:00), so the live baseline serves it, from the high
               range. Its first execution is below D-1's midnight, so only a high range start below the next midnight reaches it
               (#5506): the latest form's raised bound, the next midnight minus the edge margin. The shared seed has no such row. */
            await IntervalAsync(day0.AddDays(7).AddHours(-0.33), 10, 101, 4, 6_000);
        }
    }

    /// <summary>The second snapshot of query 41's interval: the same identity, a larger count, a last execution after midnight.</summary>
    private static async Task RecollectAsync(NpgsqlDataSource postgres, DateTime day0, CancellationToken ct)
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
        var first = day0.AddDays(3).AddHours(23);
        var row = new QueryStoreCollector.Row
        {
            DatabaseName = Db,
            QueryId = 41,
            PlanId = 411,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = first,
            LastExecutionTime = first.AddMinutes(90),
            QueryHash = "0x" + 41.ToString("X8", CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + 411.ToString("X8", CultureInfo.InvariantCulture),
            QueryText = "SELECT 41",
            ExecutionCount = 25,
            AvgCpuTimeUs = 1_000,
            AvgDurationUs = 2_000,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = RecollectedIntervalId,
            IntervalStartTimeUtc = first,
        };
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, [row], server, first.AddMinutes(100), context, ct);
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
        command.Parameters.Add(DatabaseFilter.All.ExcludeParameter());
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

    /// <summary>A store with the window's rows seeded and the daily totals built (the fixture of the first test, optionally with
    /// the extra queries), ready for a read. The scratch database is the caller's to dispose.</summary>
    private sealed class Fixture : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public required DateTime Today { get; init; }
        public required DateTime Start { get; init; }
        public required DateTime End { get; init; }
        public required DateTime BaselineStart { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static async Task<Fixture> BuildFixtureAsync(bool extra, bool timescale, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (timescale)
        {
            Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for the raw floor test");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'qsdaily', 'qsdaily', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);
        var today = LiveClock.Now().Date;
        var start = today.AddDays(-1).AddHours(6);
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);
        await SeedAsync(postgres, today.AddDays(-8), start, ct, extra);
        var end = LiveClock.Now();
        end = end.AddTicks(-(end.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, baselineStart.AddDays(-5), end, null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(-3), ct);
        await ExecAsync(connection, $"UPDATE collect.query_store_interval_latest_coverage SET filled_since = TIMESTAMP '{baselineStart.AddDays(-3):yyyy-MM-dd HH:mm:ss}' WHERE server_id = {ServerId}", ct);
        var built = await PlanRegressionDaily.RunTickAsync(postgres, LiveClock.Now(), NullLogger.Instance, ct);
        Assert.True(built.Built > 0, "the builder built no day");
        return new Fixture { Scratch = scratch, Connection = connection, Postgres = postgres, Today = today, Start = start, End = end, BaselineStart = baselineStart };
    }

    private static async Task<List<string>> AllLiveAsync(Fixture f, CancellationToken ct) =>
        await RunAsync(f.Connection, DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: false), f.Start, f.End, f.BaselineStart, QueryStoreRegressionBaseline.AllLive(f.Start), ct);

    private static async Task<List<string>> ServedRowsAsync(Fixture f, CancellationToken ct) =>
        (await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(f.Postgres, ServerId, f.Start, f.End, DatabaseFilter.One(Db), 50, f.BaselineStart, ct)).Rows.Select(Describe).ToList();

    /// <summary>
    /// Review of #6260 (F1, #5506): a day reads valid when its build saw every late row, but a late batch whose apply into
    /// <c>query_store_interval_latest</c> failed is pending, and its trigger bump rolled back with the failed apply. The wide
    /// table holds those executions, the totals do not. With a pending batch for the server no built day is used, the way
    /// PLAN_REGRESSION gates its own built days.
    /// </summary>
    [Fact]
    public async Task WithAPendingLatestBatchForTheServer_NoBuiltDayIsUsed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: false, timescale: false, ct);
        var reference = await AllLiveAsync(f, ct);
        Assert.Single(reference);

        /* The built days would be used, and a poisoned total shows it. */
        var plan = await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct);
        Assert.NotEmpty(plan.RunDays);
        var day = plan.RunDays[^1];
        await ExecAsync(f.Connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 5 WHERE server_id = {ServerId} AND day = DATE '{day:yyyy-MM-dd}'", ct);
        Assert.NotEqual(reference, await ServedRowsAsync(f, ct));

        /* A pending batch for the server: the baseline is read live, the poisoned total is not read, the answer is the all-live one. */
        await ExecAsync(f.Connection, $"INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at, failure) VALUES ({ServerId}, TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', '{Db}', TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', 'x')", ct);
        plan = await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct);
        Assert.Empty(plan.RunDays);
        Assert.Equal(reference, await ServedRowsAsync(f, ct));
    }

    /// <summary>
    /// Review of #6260 (F2, #5506): the raw arm's live rows start at raw's oldest chunk, and the tool's <c>effective_start</c>
    /// says nothing older was read. A whole valid day below that floor must not come from the totals. With the clamp the run
    /// days all start at or after the floor; without it (the wide arm's call) the older days are taken.
    /// </summary>
    [Fact]
    public async Task OnTheRawArm_DaysBelowTheRawChunkFloor_AreNotTakenFromTheTotals()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: false, timescale: true, ct);

        /* Raw loses its oldest chunks: the floor now sits inside the baseline, at about D-3. */
        await ExecAsync(f.Connection, $"SELECT drop_chunks('collect.query_store_stats', older_than => TIMESTAMP '{f.Today.AddDays(-3):yyyy-MM-dd HH:mm:ss}')", ct);
        var rawFloor = (DateTime)(await ScalarAsync(f.Connection,
            "SELECT MIN(ch.range_start) AT TIME ZONE 'UTC' FROM timescaledb_information.chunks AS ch WHERE ch.hypertable_schema = 'collect' AND ch.hypertable_name = 'query_store_stats'", ct))!;
        Assert.True(rawFloor > f.BaselineStart.AddDays(2), "the floor must sit inside the baseline for this test to mean anything");

        async Task<QueryStoreRegressionBaseline.Plan> PlanAsync(bool clamp)
        {
            await using var connection = await f.Postgres.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
            return await QueryStoreRegressionBaseline.ResolveAsync(connection, transaction, ServerId, f.BaselineStart, f.Start, 60, null, ct, clampToRawFloor: clamp);
        }

        var unclamped = await PlanAsync(clamp: false);
        Assert.Contains(DateOnly.FromDateTime(f.Today.AddDays(-7)), unclamped.RunDays);

        var clamped = await PlanAsync(clamp: true);
        Assert.NotEmpty(clamped.RunDays);
        Assert.All(clamped.RunDays, day => Assert.True(day.ToDateTime(TimeOnly.MinValue) >= rawFloor, $"{day} is below the raw floor {rawFloor:o}"));
        Assert.DoesNotContain(DateOnly.FromDateTime(f.Today.AddDays(-7)), clamped.RunDays);
    }

    /// <summary>
    /// A query with more than one plan in the baseline (one on a built day, one read live) counts both, and an interval
    /// re-collected with a growing count whose last execution crossed midnight after the build is counted once, in the day of
    /// its latest snapshot, whatever the totals held for it. Both equal the all-live read of the same window.
    /// </summary>
    [Fact]
    public async Task MorePlansAcrossTheBuiltAndLiveParts_AndAnIntervalRecollectedAcrossMidnight_MatchTheAllLiveRead()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: true, timescale: false, ct);
        await RecollectAsync(f.Postgres, f.Today.AddDays(-8), ct);

        var reference = await AllLiveAsync(f, ct);
        var served = await ServedRowsAsync(f, ct);
        Assert.Equal(reference, served);

        string[] Row(List<string> rows, long queryId) => rows.Single(r => r.StartsWith(Db + "|" + queryId.ToString(CultureInfo.InvariantCulture) + "|", StringComparison.Ordinal)).Split('|');

        /* Query 40: plan 401 on a built day and plan 402 live give 2 plans in the baseline and 1 recent. Describe fields:
           7 baseline executions, 9 plans. */
        var q40 = Row(served, 40);
        Assert.Equal("40", q40[1]);
        Assert.Equal(40L, long.Parse(q40[7], CultureInfo.InvariantCulture));
        Assert.Equal(2, int.Parse(q40[9], CultureInfo.InvariantCulture));
        Assert.Equal(1, int.Parse(q40[10], CultureInfo.InvariantCulture));

        /* Query 41: the interval counts 25 executions, once. */
        var q41 = Row(served, 41);
        Assert.Equal(25L, long.Parse(q41[7], CultureInfo.InvariantCulture));
        Assert.Equal(10L, long.Parse(q41[8], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// #5506: where latest's coverage holds the whole read and no batch is pending, the table read takes its three Regular arms
    /// from <c>query_store_interval_latest</c>, and the rows equal the wide read's. A poisoned latest row proves which table
    /// was read: the served rows move, in the MCP read and in the viewer's grid. A pending latest batch, or latest's coverage
    /// starting after the baseline start, sends the arms back to the wide table, and the poisoned row is not read.
    /// </summary>
    [Fact]
    public async Task TheRegularArms_ReadLatestWhereItsCoverageHolds_AndFallBackToWideOnAPendingBatchOrLateCoverage()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: true, timescale: false, ct);
        var reference = await AllLiveAsync(f, ct);
        Assert.NotEmpty(reference);

        /* A measured 60-minute interval, so the reads take the three-hour margin and the latest form raises its high range
           start to the last run day's next midnight (D-1) minus three hours (D-2 21:00). The extra seed's query 10 interval
           (first D-2 23:40, last D-1 00:10) is read only from the high live range, and its first execution is above that raised
           bound and below D-1's midnight: it must still be counted, and a bound at or above the midnight loses it. */
        await ExecAsync(f.Connection, $"""
            INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
            SELECT row_number() OVER (), c.c, {ServerId}, 'qsdaily', '{Db}', 60
            FROM generate_series(TIMESTAMP '{f.BaselineStart.AddDays(-5):yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{f.End:yyyy-MM-dd HH:mm:ss}', interval '6 hours') AS c(c)
            """, ct);
        Assert.Equal(TimeSpan.FromHours(3), await QueryStoreIntervalWide.ResolveEdgeMarginAsync(f.Connection, ServerId, f.BaselineStart, f.End, 60, ct));

        async Task<List<string>> ViewerRowsAsync()
        {
            await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(f.Scratch.ConnectionString);
            var rows = await viewer.TryGetQueryStoreRegressionsFromTableAsync(ServerId, f.Start, f.End, null, [Db], f.BaselineStart, null, ct);
            Assert.NotNull(rows);
            return rows.Select(r => string.Join("|",
                r.DatabaseName, r.QueryId, Math.Round(r.BaselineDurationMs, 6), Math.Round(r.RecentDurationMs, 6), Math.Round(r.BaselineCpuMs, 6),
                Math.Round(r.RecentCpuMs, 6), Math.Round(r.AdditionalDurationMs, 6), r.BaselineExecCount, r.RecentExecCount,
                r.BaselinePlanCount, r.RecentPlanCount, r.Severity, r.QueryTextSample, r.LastExecutionTime)).ToList();
        }

        /* The gate allows latest, and both forms of the statement give the same rows, with the run days and all live. */
        var plan = await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct);
        Assert.True(plan.RegularFromLatest);
        Assert.NotEmpty(plan.RunDays);

        /* The raised bound is D-1's midnight minus the margin, and the seam interval's first execution (D-2 23:40) lies between
           it and the midnight. The latest form read with that bound gives the all-live rows. The same read with the bound at the
           next midnight (no margin) loses the interval, so the rows move: the margin term is what keeps it. */
        var latestSql = DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: true, regularFromLatest: true);
        var nextMidnight = f.Today.AddDays(-1);
        var raised = QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, TimeSpan.FromHours(3));
        Assert.Equal(nextMidnight.AddHours(-3), raised.HighStart);
        Assert.Equal(reference, await RunAsync(f.Connection, latestSql, f.Start, f.End, f.BaselineStart, raised, ct));
        Assert.NotEqual(reference, await RunAsync(f.Connection, latestSql, f.Start, f.End, f.BaselineStart, plan with { HighStart = nextMidnight }, ct));
        Assert.Equal(reference, await RunAsync(f.Connection, DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: true, regularFromLatest: true), f.Start, f.End, f.BaselineStart, plan, ct));
        Assert.Equal(reference, await RunAsync(f.Connection, DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: false, regularFromLatest: true), f.Start, f.End, f.BaselineStart, QueryStoreRegressionBaseline.AllLive(f.Start), ct));
        Assert.Equal(reference, await ServedRowsAsync(f, ct));
        var viewerReference = await ViewerRowsAsync();
        Assert.Equal(reference, viewerReference);

        /* Poison the seam interval in latest only. Only the baseline's high live arm reads it (its last execution is before the
           window start), so the served rows move if that arm reads latest. Then restore it. */
        var seamPoison = $"server_id = {ServerId} AND query_id = 10 AND first_execution_time < TIMESTAMP '{nextMidnight:yyyy-MM-dd HH:mm:ss}' AND first_execution_time >= TIMESTAMP '{nextMidnight.AddHours(-1):yyyy-MM-dd HH:mm:ss}'";
        Assert.Equal(1L, await ScalarAsync(f.Connection, $"SELECT count(*) FROM collect.query_store_interval_latest WHERE {seamPoison}", ct));
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest SET avg_cpu_time_us = avg_cpu_time_us * 5 WHERE {seamPoison}", ct);
        Assert.Equal(reference, await AllLiveAsync(f, ct));
        Assert.NotEqual(reference, await ServedRowsAsync(f, ct));
        Assert.NotEqual(viewerReference, await ViewerRowsAsync());
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest SET avg_cpu_time_us = avg_cpu_time_us / 5 WHERE {seamPoison}", ct);
        Assert.Equal(reference, await ServedRowsAsync(f, ct));

        /* Poison the recent intervals in latest only: the served rows move, so the recent arm reads latest too. */
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest SET avg_cpu_time_us = avg_cpu_time_us * 5 WHERE server_id = {ServerId} AND last_execution_time >= TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}'", ct);
        Assert.Equal(reference, await AllLiveAsync(f, ct));
        Assert.NotEqual(reference, await ServedRowsAsync(f, ct));
        Assert.NotEqual(viewerReference, await ViewerRowsAsync());

        /* Latest's coverage starts after the baseline start: the arms read wide, and the rows are the wide rows again. */
        var filledSince = (DateTime)(await ScalarAsync(f.Connection, $"SELECT filled_since FROM collect.query_store_interval_latest_coverage WHERE server_id = {ServerId}", ct))!;
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest_coverage SET filled_since = TIMESTAMP '{f.BaselineStart.AddHours(1):yyyy-MM-dd HH:mm:ss}' WHERE server_id = {ServerId}", ct);
        Assert.False((await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct)).RegularFromLatest);
        Assert.Equal(reference, await ServedRowsAsync(f, ct));
        Assert.Equal(viewerReference, await ViewerRowsAsync());

        /* Coverage restored, then a pending latest batch: the arms read wide again. */
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest_coverage SET filled_since = TIMESTAMP '{filledSince:yyyy-MM-dd HH:mm:ss.ffffff}' WHERE server_id = {ServerId}", ct);
        Assert.True((await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct)).RegularFromLatest);
        Assert.NotEqual(reference, await ServedRowsAsync(f, ct));
        await ExecAsync(f.Connection, $"INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at, failure) VALUES ({ServerId}, TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', '{Db}', TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', 'x')", ct);
        Assert.False((await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct)).RegularFromLatest);
        Assert.Equal(reference, await ServedRowsAsync(f, ct));
        Assert.Equal(viewerReference, await ViewerRowsAsync());
    }
}
