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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #5794: the PLAN_REGRESSION drill-down over per-day totals (<see cref="PgDrillDownCollector.RegressedQueriesDailySql"/>)
/// returns exactly the rows the interval-table read (<see cref="PgDrillDownCollector.RegressedQueriesTableSql"/>) returns
/// over the same store. The latest table is promoted, so the seed lands in the legacy partition (the window's first two
/// days), the daily partitions and the open day. The closed days are built by the real builder and the open day and the
/// day after it partially, through a watermark, as a current store builds them. The table route's rows are the product's
/// own answer before any day is built (it falls back to the table read then); the daily route's are its answer after.
///
/// <para>The seed covers rows that straddle midnight (at the window's first day, into a closed day and into the open day),
/// per-row CPU that varies by hour, so a row counted twice or lost moves an average, and a regressed query whose newest
/// rows carry no query hash and no text, with no Query Store text row, so the hash and text lookups have to find the
/// group's other rows to flag its plan-cache signature as the table read does. A last pair of reads proves the open-day
/// half and the watermarks are load-bearing: with the live floor a day too high, or the partial days read as whole, the
/// rows differ.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP
   its own database through ScratchPostgres and works entirely inside it. */
public sealed class PlanRegressionDrillDownDailyParityLiveTests
{
    private const int ServerId = -5794001;
    private const string Db = "db";
    private const string OtherDb = "db_two";

    /* The analysis window is the hour before the kit's fixed Now, so M (the comparison window's day-aligned start) is 14 days
       before that, and the open day is Now's. */
    private static readonly DateTime TimeRangeEnd = DateTime.SpecifyKind(Now, DateTimeKind.Unspecified);
    private static readonly DateTime TimeRangeStart = TimeRangeEnd.AddHours(-1);
    private static readonly DateTime WindowStart = PgFactCollector.PlanRegressionWindowFloor(TimeRangeStart.AddDays(-14));
    private static readonly DateTime OpenDay = TimeRangeEnd.Date;

    /* The open day's watermark: its rows that start before 11:00 are in its totals, the rest are live. */
    private static readonly DateTime Watermark = OpenDay.AddHours(11);

    /* Four offenders in the first database and one steady one in the second, which never regresses (#6376 review). The
       second database also has a regressed query 1, so (OtherDb, 1) is a CROSS pair: its database and its query id are both
       on the list, but not as a pair. The table route matches the two arrays independently and returns it; the daily route
       matches pairs and must not. And one query that regressed the same way but is not on the fact's list. */
    private static readonly long[] Offenders = { 1, 2, 3, 4 };
    private static readonly PlanRegressionOffender[] Pairs =
        [.. Offenders.Select(q => new PlanRegressionOffender(Db, q)), new PlanRegressionOffender(OtherDb, SteadyOtherQuery)];
    private const long SteadyOtherQuery = 20;
    private const long NotAnOffender = 9;

    /* The offender that carries a non-NULL replica_role (PRIMARY) and has a SECONDARY copy of its latest plan. */
    private const long PrimaryRoleQuery = 4;

    [Fact]
    public async Task TheDailyRoute_ReturnsTheTableRoutesRows_WithWholeAndPartialDays_AndWithoutOffendersFallsBack()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await SeedAsync(connection, ct);

        var homes = await LeavesAsync(connection, ct);
        Assert.Contains(Latest.Name + "_legacy", homes);
        Assert.Contains(Latest.DayPartition(OpenDay).Split('.')[^1], homes);
        Assert.True(homes.Count >= 10, $"the seed landed in {homes.Count} partition(s): {string.Join(", ", homes)}");

        /* ---- the table route: no day is built, so the product reads the interval table ---- */
        var tableAll = await DrillDownAsync(postgres, Pairs);
        var table = OnPairs(tableAll);
        Assert.Equal(Offenders, table.Select(r => r.GetProperty("query_id").GetInt64()).OrderBy(q => q).ToArray());
        var two = table.Single(r => r.GetProperty("query_id").GetInt64() == 2);
        Assert.True(two.GetProperty("parameter_sensitivity_cofired").GetBoolean(), "query 2's plan-cache signature must be flagged");
        Assert.Equal("offender 2", two.GetProperty("query_text").GetString());
        Assert.Equal("text one", table.Single(r => r.GetProperty("query_id").GetInt64() == 1).GetProperty("query_text").GetString());

        /* A non-NULL replica_role: query 4's regressed rows are the primary's, and its secondary rows carry a newer hash on
           another plan. The hash lookup has to stay on the latest plan's replica, or it picks up the secondary's hash and
           loses the plan-cache flag. */
        var four = table.Single(r => r.GetProperty("query_id").GetInt64() == 4);
        Assert.Equal("PRIMARY", four.GetProperty("replica_role").GetString());
        Assert.True(four.GetProperty("parameter_sensitivity_cofired").GetBoolean(), "query 4's plan-cache signature must be flagged");

        /* The one real difference between the routes (#6376 review): the table route matches database and query id as two
           independent lists, so the cross pair is in its rows. */
        Assert.Contains(tableAll, r => r.GetProperty("database").GetString() == OtherDb && r.GetProperty("query_id").GetInt64() == 1);
        Assert.DoesNotContain(table, r => r.GetProperty("database").GetString() == OtherDb);

        /* ---- every closed day built whole by the builder, the open day and tomorrow partially ---- */
        for (var day = WindowStart; day < OpenDay; day = day.AddDays(1))
        {
            Assert.NotNull(await PlanRegressionDaily.BuildDayAsync(connection, ServerId, DateOnly.FromDateTime(day), TimeRangeEnd, ct));
        }

        await BuildPartialAsync(connection, DateOnly.FromDateTime(OpenDay), Watermark, ct);
        await BuildPartialAsync(connection, DateOnly.FromDateTime(OpenDay.AddDays(1)), Watermark, ct);
        var built = await BuiltDaysAsync(connection, ct);
        Assert.Equal(16, built.Count);
        Assert.Equal(Watermark, built.Single(b => b.Day == DateOnly.FromDateTime(OpenDay)).Through);
        Assert.Equal(Watermark, PgFactCollector.PlanRegressionLiveFloorWithWatermarks(WindowStart, built));
        var daily = await DrillDownAsync(postgres, Pairs);
        Assert.Equal(table.Select(r => r.GetRawText()), daily.Select(r => r.GetRawText()));
        Assert.DoesNotContain(daily, r => r.GetProperty("database").GetString() == OtherDb);

        /* The daily route really ran: doubling one built total moves the product's answer. */
        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 2 WHERE server_id = {ServerId} AND query_id = 3 AND day = DATE '{OpenDay.AddDays(-3):yyyy-MM-dd}'", ct);
        Assert.NotEqual(table.Select(r => r.GetRawText()), (await DrillDownAsync(postgres, Pairs)).Select(r => r.GetRawText()));
        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum / 2 WHERE server_id = {ServerId} AND query_id = 3 AND day = DATE '{OpenDay.AddDays(-3):yyyy-MM-dd}'", ct);

        /* ---- a closed day not built: the live floor drops to the day before it, and the rows are the same ---- */
        var gap = DateOnly.FromDateTime(OpenDay.AddDays(-6));
        await ExecAsync(connection, $"DELETE FROM collect.plan_regression_daily_built WHERE server_id = {ServerId} AND day = DATE '{gap:yyyy-MM-dd}'", ct);
        var gapped = await BuiltDaysAsync(connection, ct);
        Assert.Equal(15, gapped.Count);
        Assert.Equal(gap.AddDays(-1).ToDateTime(TimeOnly.MinValue), PgFactCollector.PlanRegressionLiveFloorWithWatermarks(WindowStart, gapped));
        Assert.Equal(table.Select(r => r.GetRawText()), (await DrillDownAsync(postgres, Pairs)).Select(r => r.GetRawText()));
        Assert.NotNull(await PlanRegressionDaily.BuildDayAsync(connection, ServerId, gap, TimeRangeEnd, ct));

        /* ---- not vacuous: the open-day half and the watermarks carry rows ---- */
        var direct = await DailyRowsAsync(connection, built, liveFloor: Watermark, ct);
        Assert.Equal(4, direct.Count);
        Assert.NotEqual(direct, await DailyRowsAsync(connection, built, liveFloor: Watermark.AddDays(1), ct));
        var asWhole = built.Select(b => new PlanRegressionDaily.BuiltDay(b.Day, null)).ToList();
        Assert.NotEqual(direct, await DailyRowsAsync(connection, asWhole, liveFloor: Watermark, ct));

        /* ---- no offenders: the table read, unrestricted, so the query that is not an offender is in it ---- */
        var unrestricted = await DrillDownAsync(postgres, null);
        Assert.Contains(unrestricted, r => r.GetProperty("query_id").GetInt64() == NotAnOffender);
        Assert.Equal(unrestricted.Select(r => r.GetRawText()), (await DrillDownAsync(postgres, Array.Empty<PlanRegressionOffender>())).Select(r => r.GetRawText()));
        Assert.Equal(await TableRowsAsync(connection, ct), await RowsOfAsync(unrestricted));

        /* ---- #6665: the monitor's own statement, stored only in the text table, is left out of both routes ---- */
        await ExecAsync(connection, $"UPDATE collect.query_store_text SET query_sql_text = '/* PerformanceMonitorLite */ SELECT own' WHERE server_id = {ServerId} AND database_name = '{Db}' AND query_id = 1", ct);
        Assert.DoesNotContain(OnPairs(await DrillDownAsync(postgres, Pairs)), r => r.GetProperty("query_id").GetInt64() == 1);
        await ExecAsync(connection, $"DELETE FROM collect.plan_regression_daily_built WHERE server_id = {ServerId}", ct);
        var tableWithoutOwn = OnPairs(await DrillDownAsync(postgres, Pairs));
        Assert.DoesNotContain(tableWithoutOwn, r => r.GetProperty("query_id").GetInt64() == 1);
        Assert.Contains(tableWithoutOwn, r => r.GetProperty("query_id").GetInt64() == 2);
    }

    /// <summary>
    /// When the built-days read fails, the drill-down's transaction is aborted. It is disposed, and the table read runs
    /// outside it on the same connection, and returns the table route's rows (#6376 review). The built days are readable and
    /// the daily route is the answer first (a doubled total moves it); then the built-days table is renamed away, so the
    /// built-days read fails with 42P01. The rows must come back as the table route's, not as an error or an empty answer,
    /// and the failure is warned about once.
    /// </summary>
    [Fact]
    public async Task WhenTheBuiltDaysReadFails_TheDrillDownStillReturnsTheTableRoutesRows()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await SeedAsync(connection, ct);

        var table = OnPairs(await DrillDownAsync(postgres, Pairs));
        Assert.Equal(Offenders, table.Select(r => r.GetProperty("query_id").GetInt64()).OrderBy(q => q).ToArray());

        for (var day = WindowStart; day < OpenDay; day = day.AddDays(1))
        {
            Assert.NotNull(await PlanRegressionDaily.BuildDayAsync(connection, ServerId, DateOnly.FromDateTime(day), TimeRangeEnd, ct));
        }

        /* The daily route answers, and a changed total shows it: the table route's rows are no longer what comes back. */
        var quiet = new ListLogger();
        Assert.Equal(table.Select(r => r.GetRawText()), (await DrillDownAsync(postgres, Pairs, quiet)).Select(r => r.GetRawText()));
        Assert.False(quiet.HasAtLeast(LogLevel.Warning), quiet.Dump());
        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 2 WHERE server_id = {ServerId} AND query_id = 3 AND day = DATE '{OpenDay.AddDays(-3):yyyy-MM-dd}'", ct);
        Assert.NotEqual(table.Select(r => r.GetRawText()), (await DrillDownAsync(postgres, Pairs)).Select(r => r.GetRawText()));

        /* The built-days read now fails. The totals are still doubled, so only the table read can give the table route's rows. */
        await ExecAsync(connection, "ALTER TABLE collect.plan_regression_daily_built RENAME TO plan_regression_daily_built_gone", ct);
        var logger = new ListLogger();
        var fallback = await DrillDownAsync(postgres, Pairs, logger);

        Assert.Contains(logger.Lines, l => l.Level == LogLevel.Warning && l.Message.Contains("built-days read failed", StringComparison.Ordinal));
        Assert.Equal(table.Select(r => r.GetRawText()), OnPairs(fallback).Select(r => r.GetRawText()));
    }

    /* ---------------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// Promotes the latest table as of M, so its legacy partition holds first executions below M + 2 days, creates the days
    /// ahead of Now, and plants one interval every two hours from M to the open day's noon for queries 1 to 4 and 9: a cheap
    /// plan the first seven days and an expensive one since, CPU varying by hour. Query 1 adds three intervals that straddle
    /// midnight and has a Query Store text row; query 2's open-day rows carry no hash and its even hours no text. Queries 2 and
    /// 3 carry the plan-cache signature in the analysis window.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await PromoteAsync(connection, Latest, WindowStart, NullLogger.Instance, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Latest, Now, NullLogger.Instance, ct);
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
        var first = Stamp(WindowStart);
        var last = Stamp(OpenDay.AddHours(12));
        var switchAt = Stamp(WindowStart.AddDays(7));
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_latest
    (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
     collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     last_execution_time, is_forced_plan, force_failure_count, query_text, avg_rowcount)
SELECT {ServerId}, o.dbn, q, q * 10 + CASE WHEN h < timestamp '{switchAt}' THEN 1 ELSE 2 END,
       CASE WHEN q = {PrimaryRoleQuery} THEN 'PRIMARY' END,
       extract(epoch FROM h)::bigint / 3600, h, h + interval '65 minutes',
       'p' || q || CASE WHEN h < timestamp '{switchAt}' THEN 'a' ELSE 'b' END,
       CASE WHEN q = 2 AND h >= timestamp '{Stamp(OpenDay)}' THEN NULL ELSE 'qh' || q END,
       100,
       CASE WHEN q = {SteadyOtherQuery} THEN 5000 + extract(hour FROM h)::bigint * 5
            WHEN h < timestamp '{switchAt}' THEN 5000 + extract(hour FROM h)::bigint * 5
            ELSE 50000 + q * 1000 + extract(hour FROM h)::bigint * 10 + extract(day FROM h)::bigint END,
       CASE WHEN q = {SteadyOtherQuery} OR h < timestamp '{switchAt}' THEN 8000 ELSE 90000 + extract(hour FROM h)::bigint END,
       h + interval '59 minutes', false, 0,
       CASE WHEN q = 2 AND extract(hour FROM h)::int % 4 = 0 THEN NULL ELSE 'offender ' || q END, 10
FROM generate_series(timestamp '{first}', timestamp '{last}', interval '2 hours') AS h
CROSS JOIN (VALUES {string.Join(", ", Offenders.Append(NotAnOffender).Select(x => $"('{Db}', {x})"))}, ('{OtherDb}', 1), ('{OtherDb}', {SteadyOtherQuery})) AS o (dbn, q)
ORDER BY o.dbn, q, h;", ct);

        /* Query 4 has a second replica: SECONDARY rows of its latest plan only (so no regression of its own), newer than the
           primary's, with a different hash. The primary's row must flag the plan-cache signature of ITS hash, so the hash
           lookup has to stay on the primary's rows. */
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_latest
    (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
     collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     last_execution_time, is_forced_plan, force_failure_count, query_text, avg_rowcount)
SELECT {ServerId}, '{Db}', {PrimaryRoleQuery}, {PrimaryRoleQuery * 10 + 2}, 'SECONDARY', 800000 + n, timestamp '{Stamp(OpenDay.AddHours(13))}' + n * interval '10 minutes',
       timestamp '{Stamp(OpenDay.AddHours(14))}', 'p{PrimaryRoleQuery}b', 'qh{PrimaryRoleQuery}s', 100, 55000, 90000,
       timestamp '{Stamp(OpenDay.AddHours(13))}' + n * interval '10 minutes' + interval '9 minutes', false, 0, 'offender {PrimaryRoleQuery}', 10
FROM generate_series(0, 2) AS n;", ct);

        /* Query 1's straddles: into the window's first day (the first execution in the legacy partition, on M - 1), into a
           closed day, and into the open day below its watermark. */
        foreach (var (start, plan) in new[] { (WindowStart.AddMinutes(-20), 11L), (OpenDay.AddDays(-5).AddMinutes(-20), 12L), (OpenDay.AddMinutes(-20), 12L) })
        {
            await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_latest
    (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
     collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     last_execution_time, is_forced_plan, force_failure_count, query_text, avg_rowcount)
VALUES ({ServerId}, '{Db}', 1, {plan}, NULL, 900000 + {plan}, timestamp '{Stamp(start)}', timestamp '{Stamp(start.AddMinutes(75))}',
        'p1{(plan == 11 ? "a" : "b")}', 'qh1', 300, {(plan == 11 ? 4000 : 70000)}, 9000, timestamp '{Stamp(start.AddMinutes(55))}', false, 0, 'offender 1', 10);", ct);
        }

        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);
        await ExecAsync(connection, $"INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ({ServerId}, '{Db}', 1, 'text one', timestamp '{Stamp(TimeRangeEnd)}')", ct);
        await ExecAsync(connection,
            $"INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES ({ServerId}, timestamp '{Stamp(WindowStart.AddDays(-5))}', timestamp '{Stamp(TimeRangeEnd)}') "
            + "ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since", ct);

        foreach (var q in new[] { 2L, 3L, PrimaryRoleQuery })
        {
            await using var cache = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, creation_time,
     execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb, min_spills, max_spills, query_text,
     delta_execution_count)
VALUES ($1, $2, $3, 'regr-5794', $4, $5, $6, $7, 100, 20000, 20000000, 1024, 1024, 0, 0, 'select', 50)", connection);
            cache.Parameters.AddWithValue(CollectionIdGenerator.Next());
            cache.Parameters.AddWithValue(NpgsqlDbType.Timestamp, TimeRangeEnd.AddMinutes(-5));
            cache.Parameters.AddWithValue(ServerId);
            cache.Parameters.AddWithValue(Db);
            cache.Parameters.AddWithValue("qh" + q.ToString(CultureInfo.InvariantCulture));
            cache.Parameters.AddWithValue("p" + q.ToString(CultureInfo.InvariantCulture) + "b");
            cache.Parameters.AddWithValue(NpgsqlDbType.Timestamp, TimeRangeStart.AddDays(-3));
            await cache.ExecuteNonQueryAsync(ct);
        }

        await ExecAsync(connection, $"ANALYZE {Latest.Parent}", ct);
    }

    /// <summary>The rows of the first database only: the rows both routes owe for the fact's pairs.</summary>
    private static List<JsonElement> OnPairs(List<JsonElement> rows) => rows.Where(r => r.GetProperty("database").GetString() == Db).ToList();

    private static string Stamp(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private static async Task<List<string>> LeavesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var leaves = new List<string>();
        await using var command = new NpgsqlCommand(
            $"SELECT DISTINCT c.relname FROM {Latest.Parent} AS t JOIN pg_class AS c ON c.oid = t.tableoid WHERE t.server_id = {ServerId}", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            leaves.Add(reader.GetString(0));

        return leaves;
    }

    /// <summary>A partial build of one day through <paramref name="watermark"/>: the builder's own statements (its insert and
    /// its stamp), with the watermark given rather than read off the store's clock, which is weeks past the seed.</summary>
    private static async Task BuildPartialAsync(NpgsqlConnection connection, DateOnly day, DateTime watermark, CancellationToken ct)
    {
        var d = day.ToDateTime(TimeOnly.MinValue);
        await using (var ensure = new NpgsqlCommand(PlanRegressionDaily.EnsureBuiltSql, connection))
        {
            ensure.Parameters.AddWithValue(ServerId);
            ensure.Parameters.AddWithValue(NpgsqlDbType.Date, day);
            await ensure.ExecuteNonQueryAsync(ct);
        }

        long seq;
        await using (var read = new NpgsqlCommand("SELECT late_seq FROM collect.plan_regression_daily_built WHERE server_id = $1 AND day = $2", connection))
        {
            read.Parameters.AddWithValue(ServerId);
            read.Parameters.AddWithValue(NpgsqlDbType.Date, day);
            seq = (long)(await read.ExecuteScalarAsync(ct))!;
        }

        long rows;
        await using (var build = new NpgsqlCommand(PlanRegressionDaily.BuildDaySql, connection))
        {
            build.Parameters.AddWithValue(ServerId);
            build.Parameters.AddWithValue(NpgsqlDbType.Date, day);
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, d.AddDays(1));
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, d.AddDays(-1));
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, watermark);
            rows = (long)(await build.ExecuteScalarAsync(ct))!;
        }

        await using var mark = new NpgsqlCommand(PlanRegressionDaily.MarkBuiltSql, connection);
        mark.Parameters.AddWithValue(ServerId);
        mark.Parameters.AddWithValue(NpgsqlDbType.Date, day);
        mark.Parameters.AddWithValue(seq);
        mark.Parameters.AddWithValue(NpgsqlDbType.Timestamp, TimeRangeEnd);
        mark.Parameters.AddWithValue(rows);
        mark.Parameters.AddWithValue(NpgsqlDbType.Timestamp, watermark);
        Assert.Equal(1, await mark.ExecuteNonQueryAsync(ct));
    }

    private static async Task<List<PlanRegressionDaily.BuiltDay>> BuiltDaysAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await QueryStoreIntervalLatest.ReadBuiltDaysAsync(connection, null, ServerId, WindowStart, 60, null, _ => false, ct);

    private static AnalysisContext NewContext(IEnumerable<PlanRegressionOffender>? offenders) => new()
    {
        ServerId = ServerId,
        ServerName = "regr-5794",
        TimeRangeStart = TimeRangeStart,
        TimeRangeEnd = TimeRangeEnd,
        ServerUtcOffset = TimeSpan.Zero,
        PlanRegressionReadsIntervalTable = true,
        PlanRegressionWindowStart = WindowStart,
        PlanRegressionOffenders = offenders?.ToList(),
    };

    /// <summary>The drill-down's rows as the product builds them, through the pass's own entry point.</summary>
    private static async Task<List<JsonElement>> DrillDownAsync(NpgsqlDataSource postgres, IEnumerable<PlanRegressionOffender>? offenders, ILogger? logger = null)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            PathKeys = ["PLAN_REGRESSION"],
            /* Past the display gate: below it the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres, logger: logger).EnrichFindingsAsync([finding], NewContext(offenders));
        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("regressed_queries", out var raw))
            return [];

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    /// <summary>The (database, query id, latest plan, best plan) of each product row, to compare with a direct read.</summary>
    private static Task<List<string>> RowsOfAsync(List<JsonElement> rows) => Task.FromResult(rows
        .Select(r => string.Join("|", r.GetProperty("database").GetString(), r.GetProperty("query_id").GetInt64().ToString(CultureInfo.InvariantCulture),
            r.GetProperty("latest_plan_hash").GetString(), r.GetProperty("best_plan_hash").GetString()))
        .ToList());

    /// <summary>The table read with no offenders, bound as the collector binds it: its first four columns per row.</summary>
    private static async Task<List<string>> TableRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PgDrillDownCollector.RegressedQueriesTableSql.Replace(PgSecondaryReplicaScope.Marker, "", StringComparison.Ordinal), connection);
        Bind(command, offenders: null);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, WindowStart.AddDays(-1));
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add(string.Join("|", reader.GetString(0), Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture), reader.GetString(2), reader.GetString(5)));

        return rows;
    }

    /// <summary>The daily read with the offenders, bound as the collector binds it but with the live floor given, every
    /// column of every row.</summary>
    private static async Task<List<string>> DailyRowsAsync(
        NpgsqlConnection connection, IReadOnlyList<PlanRegressionDaily.BuiltDay> builtDays, DateTime liveFloor, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PgDrillDownCollector.RegressedQueriesDailySql.Replace(PgSecondaryReplicaScope.Marker, "", StringComparison.Ordinal), connection);
        Bind(command, Offenders);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, WindowStart.AddDays(-1));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = builtDays.Select(b => b.Day).ToArray() });
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, liveFloor);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = builtDays.Select(b => b.ThroughOrDayEnd).ToArray() });
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
                values[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "";

            rows.Add(string.Join("|", values));
        }

        return rows;
    }

    /// <summary>$1 to $6, as the collector binds them.</summary>
    private static void Bind(NpgsqlCommand command, long[]? offenders)
    {
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, WindowStart);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, TimeRangeStart);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, TimeRangeEnd);
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = offenders is null ? DBNull.Value : Enumerable.Repeat(Db, offenders.Length).ToArray(),
        });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            Value = offenders is null ? DBNull.Value : offenders,
        });
    }
}
