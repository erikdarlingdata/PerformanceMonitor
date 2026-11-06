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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the long-window read of <c>get_query_store_top</c> (#5094): whole built UTC days come from
/// <c>collect.query_store_top_daily</c>, the partial edge days and any unbuilt day from the wide table, in one statement
/// (<see cref="DarlingDataReader.QueryStoreTopDailyTableSql"/>). With no late change to a built day the answer equals the
/// interval-table read (<see cref="DarlingDataReader.QueryStoreTopTableSql"/>) value for value, doubles included
/// bit for bit; the one documented difference is a row written into a built day after that day was built. Every seed
/// is at a fixed past date, every clock is passed explicitly, and each live fact mints its own scratch database.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live fact here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it, so it cannot race live
   collection. */
public sealed class QueryStoreTopDailyReadLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the daily summary read's live pins (each mints its own scratch database).";
    private const int ServerId = 1;
    private const int Top = 30;
    private const int RetentionDays = 9;

    private static readonly DateTime End = new(2026, 1, 14, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime BuildNow = new(2026, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheDailyStatement_SharesTheTableReadsTail_AndItsPredicates()
    {
        var daily = DarlingDataReader.QueryStoreTopDailyTableSql.ReplaceLineEndings("\n");
        var table = DarlingDataReader.QueryStoreTopTableSql.ReplaceLineEndings("\n");
        var tail = table[table.IndexOf("SELECT\n    p.database_name,", StringComparison.Ordinal)..];   /* #5815: the page now selects from p */
        Assert.EndsWith(tail, daily, StringComparison.Ordinal);
        Assert.Contains("$7::text IS NULL OR module_name = $7", daily, StringComparison.Ordinal);
        Assert.Contains("LIMIT $10", daily, StringComparison.Ordinal);   /* #5313: the candidate limit, bound after the two dates */
    }

    [Fact]
    public void TheLongestRun_TakesTheLongest_TiesToTheLatest_AndNothingFromNothing()
    {
        static DateOnly D(int day) => new(2026, 1, day);
        Assert.Null(DarlingDataReader.LongestBuiltRun(Array.Empty<DateOnly>()));
        Assert.Equal((D(8), D(11)), DarlingDataReader.LongestBuiltRun(new[] { D(8), D(9), D(10), D(12), D(13) }));
        Assert.Equal((D(11), D(14)), DarlingDataReader.LongestBuiltRun(new[] { D(8), D(9), D(11), D(12), D(13) }));
        Assert.Equal((D(11), D(13)), DarlingDataReader.LongestBuiltRun(new[] { D(8), D(9), D(11), D(12) }));
        Assert.Equal((D(9), D(10)), DarlingDataReader.LongestBuiltRun(new[] { D(9) }));
    }

    [Fact]
    public void TheWholeDays_AreTheMidnightsInsideTheWindow()
    {
        var (first, end) = DarlingDataReader.WholeDays(new DateTime(2026, 1, 11, 6, 0, 0), new DateTime(2026, 1, 14, 6, 0, 0));
        Assert.Equal((new DateOnly(2026, 1, 12), new DateOnly(2026, 1, 14)), (first, end));
        (first, end) = DarlingDataReader.WholeDays(new DateTime(2026, 1, 11, 0, 0, 0), new DateTime(2026, 1, 14, 0, 0, 0));
        Assert.Equal((new DateOnly(2026, 1, 11), new DateOnly(2026, 1, 14)), (first, end));
    }

    /* ───────────────────────────── live ───────────────────────────── */

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string At(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    /// <summary>The wide table for server 1 from 2026-01-05 to 2026-01-14 12:00, one row per query per hour for most
    /// hours (a row at every midnight among them), with nulls where the facts need them. 40 queries: query 40 has every
    /// average NULL, queries divisible by 7 have a NULL cpu on odd hours, queries divisible by 4 change module
    /// between hours, queries divisible by 5 are Aborted, and queries divisible by 6 carry a replica role.</summary>
    private const string SeedSql = @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
 avg_physical_io_reads, avg_rowcount, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT
    ct, 1, 'db' || (q % 3), q, q, CASE WHEN q % 5 = 0 THEN 'Aborted' ELSE 'Regular' END,
    ct - interval '10 minutes', ct - make_interval(mins => h % 7),
    CASE WHEN q % 4 = 0 THEN CASE WHEN h % 2 = 0 THEN 'dbo.m1' ELSE NULL END WHEN q % 4 = 1 THEN 'dbo.m2' ELSE NULL END,
    'h' || q, 1 + (q * 13 + h * 7) % 23,
    CASE WHEN q = 40 THEN NULL ELSE 1000 + (q * 7919 + h * 104729) % 90001 END,
    CASE WHEN q = 40 OR (q % 7 = 0 AND h % 2 = 1) THEN NULL ELSE 10 + (q * 31 + h * 17) % 5003 END,
    CASE WHEN q = 40 THEN NULL ELSE 100 + (q * 101 + h * 13) % 500009 END,
    CASE WHEN q = 40 THEN NULL ELSE (q * 3 + h) % 97 END,
    CASE WHEN q = 40 THEN NULL ELSE (q + h * 5) % 211 END,
    CASE WHEN q = 40 THEN NULL ELSE 1 + (q * 29 + h * 3) % 1009 END,
    'ph' || (h % 5), CASE WHEN q % 6 = 0 THEN 'PRIMARY' ELSE NULL END,
    q * 100000 + h, ct - interval '10 minutes'
FROM
(
    SELECT q, h, TIMESTAMP '2026-01-05 00:00:00' + make_interval(hours => h) AS ct
    FROM generate_series(1, 40) AS q
    CROSS JOIN generate_series(0, 24 * 9 + 12) AS h
    WHERE (q * 7 + h) % 5 <> 0 OR h % 24 = 0
) AS s;";

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, SeedSql, ct);
        await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (1, 'srv1', TRUE)", ct);
        /* applied_through at or before the read's literal end, or the gate refuses the table (clause 4). */
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES (1, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00')", ct);
    }

    /// <summary>One extra wide-table row for query <paramref name="queryId"/> with a duration large enough to rank first.</summary>
    private static async Task InsertRowAsync(NpgsqlConnection connection, DateTime collectionTime, long queryId, CancellationToken ct, long duration = 90_000_000, DateTime? firstExecution = null)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
 avg_rowcount, query_plan_hash, runtime_stats_interval_id, interval_start_time_utc)
VALUES (@ct, 1, 'db0', @q, @q, 'Regular', COALESCE(@fet, @ct - interval '10 minutes'), @ct, 'h' || @q, 1000, @dur, 5, 5, 5, 5, 5, 'late', @q, @ct - interval '10 minutes')", connection);
        command.Parameters.Add(new NpgsqlParameter("ct", NpgsqlDbType.Timestamp) { Value = collectionTime });
        command.Parameters.Add(new NpgsqlParameter("fet", NpgsqlDbType.Timestamp) { Value = (object?)firstExecution ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("q", NpgsqlDbType.Bigint) { Value = queryId });
        command.Parameters.Add(new NpgsqlParameter("dur", NpgsqlDbType.Bigint) { Value = duration });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task BuildAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        var result = await QueryStoreTopDaily.RunTickAsync(source, BuildNow, RetentionDays, NullLogger.Instance, ct);
        Assert.Equal(0, result.Failed);
        Assert.Equal(10, result.Built);
    }

    private static async Task RunLiveAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct);
            await BuildAsync(source, ct);
            await body(connection, source, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static DarlingDataReader.QueryStoreRow ToRow(object?[] v)
    {
        static T? Get<T>(object? value) => value is null or DBNull ? default : (T)value;
        return new DarlingDataReader.QueryStoreRow(
            Get<string>(v[0]) ?? "", Get<long?>(v[1]) ?? 0, Get<long?>(v[2]) ?? 0, Get<string>(v[3]) ?? "", Get<string>(v[4]) ?? "",
            Get<string>(v[5]) ?? "", Get<string>(v[6]), Get<long?>(v[7]) ?? 0, Get<double?>(v[8]) ?? 0, Get<double?>(v[9]) ?? 0,
            Get<double?>(v[10]) ?? 0, Get<double?>(v[11]) ?? 0, Get<double?>(v[12]) ?? 0, Get<double?>(v[13]) ?? 0,
            Get<DateTime?>(v[14]), Get<string>(v[15]) ?? "", Get<string>(v[16]));
    }

    /// <summary>A row as text with every double as its bit pattern, so "equal" means bit-equal.</summary>
    private static string Format(DarlingDataReader.QueryStoreRow r)
    {
        static string B(double d) => BitConverter.DoubleToInt64Bits(d).ToString("x", CultureInfo.InvariantCulture);
        return string.Join('|', r.DatabaseName, r.QueryId, r.PlanId, r.QueryHash, r.QueryPlanHash, r.ExecutionTypeDesc, r.ModuleName ?? "<null>",
            r.TotalExecutions, B(r.AvgDurationMs), B(r.AvgCpuTimeMs), B(r.AvgLogicalReads), B(r.AvgLogicalWrites), B(r.AvgPhysicalReads),
            B(r.AvgRowcount), r.LastExecutionTime is { } t ? At(t) : "<null>", r.QueryText, r.ReplicaRole ?? "<null>");
    }

    private static List<string> Format(IEnumerable<DarlingDataReader.QueryStoreRow> rows) => rows.Select(Format).ToList();

    /// <summary>Runs <paramref name="sql"/> (either statement) and returns its raw rows, NULLs kept.</summary>
    private static async Task<List<object?[]>> RunSqlAsync(
        NpgsqlConnection connection, string sql, DateTime readStart, DateTime end, string? db, string? outcome, string? module,
        (DateOnly, DateOnly)? span, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = readStart });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
        command.Parameters.Add(PerformanceMonitor.Darling.Storage.DatabaseFilter.One(db).Parameter());  /* #5245: the list predicate binds one text[] */
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)outcome ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)module ?? DBNull.Value });
        if (span is var (s, e))
        {
            command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = s, NpgsqlDbType = NpgsqlDbType.Date });
            command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = e, NpgsqlDbType = NpgsqlDbType.Date });
        }

        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = PerformanceMonitor.Darling.Storage.TopFill.FirstCandidates(Top) });  /* #5313: the round's candidate limit, bound last */

        var rows = new List<object?[]>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(values);
        }

        return rows;
    }

    /// <summary>Today's read (<see cref="DarlingDataReader.QueryStoreTopTableSql"/>) of the same window, on the same connection.</summary>
    private static async Task<List<string>> TodaysReadAsync(
        NpgsqlConnection connection, DateTime readStart, DateTime end, string? db, string? outcome, string? module, CancellationToken ct) =>
        Format((await RunSqlAsync(connection, DarlingDataReader.QueryStoreTopTableSql, readStart, end, db, outcome, module, null, ct)).Select(ToRow));

    private static async Task<DarlingDataReader.QueryStoreTopRead> RouteAsync(
        NpgsqlDataSource source, DateTime start, DateTime end, string? db, string? outcome, string? module, CancellationToken ct) =>
        await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, ServerId, start, end, Top, db, outcome, module, ct);

    public static IEnumerable<object?[]> Cells()
    {
        foreach (var hours in new[] { 72, 168 })
        {
            yield return new object?[] { hours, null, null, null };
            yield return new object?[] { hours, "db1", null, null };
            yield return new object?[] { hours, null, "Aborted", null };
            yield return new object?[] { hours, null, null, "dbo.m1" };
            yield return new object?[] { hours, "db0", "Regular", "dbo.m2" };
        }
    }

    [Theory]
    [MemberData(nameof(Cells))]
    public async Task TheRoute_EqualsTodaysTableRead_BitForBit_WhenNothingChangedAfterTheBuild(int hours, string? db, string? outcome, string? module)
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = End.AddHours(-hours);
            var read = await RouteAsync(source, start, End, db, outcome, module, ct);

            Assert.NotNull(read.Table);
            Assert.True(read.DailyDaysUsed > 0);
            Assert.Equal((hours == 72 ? 2 : 6), read.DailyDaysUsed);
            Assert.Equal(new DateOnly(2026, 1, 14), read.DailySpan!.Value.EndExclusive);
            Assert.NotEmpty(read.Rows);

            var today = await TodaysReadAsync(connection, read.Table!.Value.ReadStart, End, db, outcome, module, ct);
            Assert.Equal(today, Format(read.Rows));
        });
    }

    /// <summary>One interval row for the skew fixture: <paramref name="executions"/> runs averaging <paramref name="avgDurationUs"/>.</summary>
    private static async Task InsertSkewRowAsync(NpgsqlConnection connection, DateTime collectionTime, long queryId, long executions, long avgDurationUs, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
 avg_rowcount, query_plan_hash, runtime_stats_interval_id, interval_start_time_utc)
VALUES (@ct, 1, 'dbskew', @q, @q, 'Regular', @ct - interval '10 minutes', @ct, 'h' || @q, @ec, @dur, @dur, @dur, 0, 0, 1, 'skew', @q * 1000 + EXTRACT(HOUR FROM @ct)::bigint, @ct - interval '10 minutes')", connection);
        command.Parameters.Add(new NpgsqlParameter("ct", NpgsqlDbType.Timestamp) { Value = collectionTime });
        command.Parameters.Add(new NpgsqlParameter("q", NpgsqlDbType.Bigint) { Value = queryId });
        command.Parameters.Add(new NpgsqlParameter("ec", NpgsqlDbType.Bigint) { Value = executions });
        command.Parameters.Add(new NpgsqlParameter("dur", NpgsqlDbType.Bigint) { Value = avgDurationUs });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// #7169: a plan with one 10 s execution in one interval and 10,000 executions of 1 ms in another averages
    /// (10,000,000 + 10,000 * 1,000) / 10,001 = about 2 ms and totals about 20 s. The old read averaged the two interval
    /// averages (about 5 s) and ranked on <c>SUM(execution_count) * AVG(avg)</c> (about 50,000 s), so it put this plan
    /// above a plan that really ran 30 s. The long-window route (built days) and the table read must agree on both the
    /// average and the order.
    /// </summary>
    [Fact]
    public async Task AveragesAreWeightedByExecutions_AndTheRankIsTotalDuration_InBothTheRouteAndTheTableRead()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var day = new DateOnly(2026, 1, 10);
            var d = day.ToDateTime(TimeOnly.MinValue);
            /* Query 900: the skew case, total about 20 s. Query 901: 10,000 executions at 3 ms, total 30 s, so it ranks first. */
            await InsertSkewRowAsync(connection, d.AddHours(3), 900, 1, 10_000_000, ct);
            await InsertSkewRowAsync(connection, d.AddHours(4), 900, 10_000, 1_000, ct);
            await InsertSkewRowAsync(connection, d.AddHours(3), 901, 10_000, 3_000, ct);
            await QueryStoreTopDaily.BuildDayAsync(connection, ServerId, day, 2, BuildNow, ct);

            var start = End.AddHours(-168);
            var read = await RouteAsync(source, start, End, "dbskew", null, null, ct);
            Assert.True(read.DailyDaysUsed > 0);
            var today = (await RunSqlAsync(connection, DarlingDataReader.QueryStoreTopTableSql, read.Table!.Value.ReadStart, End, "dbskew", null, null, null, ct)).Select(ToRow).ToList();

            foreach (var rows in new[] { read.Rows.ToList(), today })
            {
                Assert.Equal(new long[] { 901, 900 }, rows.Select(r => r.QueryId).ToArray());
                var skew = rows.Single(r => r.QueryId == 900);
                Assert.Equal(10_001, skew.TotalExecutions);
                Assert.Equal((10_000_000.0 + 10_000.0 * 1_000.0) / 10_001.0 / 1000.0, skew.AvgDurationMs, 6);
                Assert.Equal(skew.AvgDurationMs, skew.AvgCpuTimeMs, 6);
                Assert.Equal(skew.AvgDurationMs * 1000.0, skew.AvgLogicalReads, 3);
                Assert.Equal(3.0, rows.Single(r => r.QueryId == 901).AvgDurationMs, 6);
            }

            /* The Viewer's grid (its own copy of the statement, built from the same expressions) agrees. */
            await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(connection.ConnectionString);
            var grid = await viewer.GetQueryStoreTopQueriesAsync(ServerId, start, End, Top, new[] { "dbskew" }, cancellationToken: ct);
            Assert.Equal(new long[] { 901, 900 }, grid.Select(r => r.QueryId).ToArray());
            Assert.Equal((10_000_000.0 + 10_000.0 * 1_000.0) / 10_001.0 / 1000.0, grid.Single(r => r.QueryId == 900).AvgDurationMs, 6);
        });
    }

    /// <summary>
    /// #7169: a day built before V240 has NULL <c>*_wsum</c> columns beside a non-NULL <c>execution_count_sum</c>. Read as a
    /// built day it adds its executions to the divisor and nothing to the numerator, so the average comes out low. The reader
    /// must treat that day as not built and read it from the wide table. Query 900 on a 72 h window (whole days 12 and 13):
    /// day 12 is built with weighted sums (2,000 runs at 2 ms and 2,000 at 8 ms, 4,000 runs, 20 s), day 13 is built and then
    /// has every weighted sum cleared (5,000 runs at 4 ms and 5,000 at 6 ms, 10,000 runs, 50 s) with its raw rows still in
    /// the wide table. Exact answer: 70 s over 14,000 runs = 5 ms. Day 13 read from the built table would give
    /// 20 s over 14,000 runs = about 1.43 ms.
    /// </summary>
    [Fact]
    public async Task ABuiltDayWithNullWeightedSums_IsReadFromTheWideTable_SoTheAverageIsExactAcrossOldAndNewDays()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var newDay = new DateOnly(2026, 1, 12);
            var oldDay = new DateOnly(2026, 1, 13);
            var d12 = newDay.ToDateTime(TimeOnly.MinValue);
            var d13 = oldDay.ToDateTime(TimeOnly.MinValue);
            await InsertSkewRowAsync(connection, d12.AddHours(3), 900, 2_000, 2_000, ct);
            await InsertSkewRowAsync(connection, d12.AddHours(4), 900, 2_000, 8_000, ct);
            await InsertSkewRowAsync(connection, d13.AddHours(3), 900, 5_000, 4_000, ct);
            await InsertSkewRowAsync(connection, d13.AddHours(4), 900, 5_000, 6_000, ct);
            await QueryStoreTopDaily.BuildDayAsync(connection, ServerId, newDay, 2, BuildNow, ct);
            await QueryStoreTopDaily.BuildDayAsync(connection, ServerId, oldDay, 2, BuildNow, ct);

            /* What a pre-V240 builder wrote: the summary rows and the built row are there, the weighted sums are not. */
            await ExecAsync(connection, @"
UPDATE collect.query_store_top_daily
SET avg_duration_us_wsum = NULL, avg_cpu_time_us_wsum = NULL, avg_logical_io_reads_wsum = NULL,
    avg_logical_io_writes_wsum = NULL, avg_physical_io_reads_wsum = NULL, avg_rowcount_wsum = NULL
WHERE server_id = 1 AND day = DATE '2026-01-13'", ct);

            var read = await RouteAsync(source, End.AddHours(-72), End, "dbskew", null, null, ct);

            /* Only day 12 counts as built, so day 13 is not in the summary span. */
            Assert.Equal(1, read.DailyDaysUsed);
            Assert.Equal((newDay, oldDay), read.DailySpan);
            var row = Assert.Single(read.Rows);
            Assert.Equal(900, row.QueryId);
            Assert.Equal(14_000, row.TotalExecutions);
            Assert.Equal(5.0, row.AvgDurationMs);
        });
    }

    [Fact]
    public async Task AFaultReadingTheBuiltDays_AfterTheGateChoseTheTable_NotesFallbackRaw_AndAnswersFromRaw()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            await ExecAsync(connection, "DROP TABLE collect.query_store_top_daily_built", ct);
            await ExecAsync(connection, @"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT 1, TIMESTAMP '2026-01-10 00:30:00' + (i * interval '1 hour'), 1, 'srv1', 'db0', 100 + i, 1000 + i,
    'Regular', TIMESTAMP '2026-01-10 00:00:00' + (i * interval '1 hour'), 'mod', md5('q' || i), 10 + i, 500 + i, 300, 900, 700, 'PRIMARY', 77 + i,
    TIMESTAMP '2026-01-10 00:00:00' + (i * interval '1 hour')
FROM generate_series(0, 9) AS i", ct);
            var start = End.AddHours(-168);
            var logger = new CapturingTestLogger();
            using var scope = ReadScope.Open(logger);

            var read = await RouteAsync(source, start, End, null, null, null, ct);

            Assert.Null(read.Table);
            Assert.Equal(0, read.DailyDaysUsed);
            Assert.Equal(ReadFallback.FallbackRaw, scope.Fallback);
            var raw = Format((await RunSqlAsync(connection, DarlingDataReader.QueryStoreTopSql, start, End, null, null, null, null, ct)).Select(ToRow));
            Assert.NotEmpty(raw);
            Assert.Equal(raw, Format(read.Rows));
        });
    }

    [Fact]
    public async Task WithNoBuiltDay_TheReadIsTodaysStatement_AndNoSummaryIsUsed()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            await ExecAsync(connection, "DELETE FROM collect.query_store_top_daily; DELETE FROM collect.query_store_top_daily_built", ct);
            var read = await RouteAsync(source, End.AddHours(-168), End, null, null, null, ct);

            Assert.Equal(0, read.DailyDaysUsed);
            Assert.Null(read.DailySpan);
            Assert.Equal(await TodaysReadAsync(connection, read.Table!.Value.ReadStart, End, null, null, null, ct), Format(read.Rows));
        });
    }

    private const long FirstOwn = 5000;

    /// <summary>#7736: more of the monitor's own statements than the read has candidates, each ranking above every real query (the
    /// longest duration), in the unbuilt part of the window, with their text (the marker) in the text table and their keys backfilled.</summary>
    private static async Task SeedOwnRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var count = TopFill.FinalCandidates(Top) + 20;
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
 avg_rowcount, query_plan_hash, runtime_stats_interval_id, interval_start_time_utc)
SELECT TIMESTAMP '2026-01-14 03:00:00', 1, 'db0', q, q, 'Regular', TIMESTAMP '2026-01-14 02:50:00', TIMESTAMP '2026-01-14 03:00:00',
       'own' || q, 1000, 900000000, 5, 5, 5, 5, 5, 'own', q, TIMESTAMP '2026-01-14 02:50:00'
FROM generate_series({FirstOwn}, {FirstOwn + count - 1}) AS q;
INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen)
SELECT 1, 'db0', q, '(@cutoff_time datetime2(7))SELECT /* {PerformanceMonitor.Collectors.QueryStoreCollector.SelfQueryMarker} */ qsrs.plan_id', TIMESTAMP '2026-01-14 03:00:00'
FROM generate_series({FirstOwn}, {FirstOwn + count - 1}) AS q;", ct);
        Assert.NotNull(await QueryStoreSelfKeys.BackfillAsync(connection, ct));
        Assert.True(await QueryStoreSelfKeys.ReadyAsync(connection, null, 30, ct));
    }

    /// <summary>
    /// #7736: the MCP daily read (and, with no built day, the interval-table read) leaves the monitor's own statements out in
    /// <c>ranked</c>, ahead of its candidate limit. More of them than candidates, ranking first: left to the tail they would use up every
    /// candidate and the page would come back empty, so a real page here is the proof that the slot is filled and its columns resolve.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MoreOfTheMonitorsOwnRowsThanCandidates_StillFillTheTablePage_OnTheDailyAndTheIntervalTableRoute(bool daily)
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            if (!daily)
            {
                await ExecAsync(connection, "DELETE FROM collect.query_store_top_daily; DELETE FROM collect.query_store_top_daily_built", ct);
            }

            await SeedOwnRowsAsync(connection, ct);

            /* Not vacuous: with the gate closed the own statements still take every candidate, so the tail leaves the one candidate that is not the monitor's (query 40, whose NULL durations sort first), a short page. */
            await ExecAsync(connection, QueryStoreSelfKeys.ReopenBackfillSql, ct);
            var gateClosed = await RouteAsync(source, End.AddHours(-168), End, null, null, null, ct);
            Assert.NotNull(gateClosed.Table);
            Assert.Single(gateClosed.Rows);
            Assert.NotNull(await QueryStoreSelfKeys.BackfillAsync(connection, ct));

            var read = await RouteAsync(source, End.AddHours(-168), End, null, null, null, ct);

            Assert.NotNull(read.Table);
            Assert.Equal(daily, read.DailyDaysUsed > 0);
            Assert.Equal(Top, read.Rows.Count);
            Assert.DoesNotContain(read.Rows, r => r.QueryId >= FirstOwn);
        });
    }

    private static async Task<System.Text.Json.JsonElement> ToolAnswerAsync(NpgsqlDataSource source, int hours, CancellationToken ct) =>
        System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(
            source, "srv1", hours, Top, null, "2026-01-14T06:00:00Z", null, null, false, 400, ct)).RootElement.Clone();

    [Fact]
    public async Task TheTool_SaysApproximate_WithTheSummaryDays_WhenABuiltDayWasUsed()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var answer = await ToolAnswerAsync(source, 72, ct);

            Assert.True(answer.GetProperty("approximate").GetBoolean());
            Assert.Equal(DarlingMcpDataTools.QueryStoreApproximationNote, answer.GetProperty("approximation_note").GetString());
            var days = answer.GetProperty("summary_days");
            Assert.Equal("2026-01-12", days.GetProperty("from").GetString());
            Assert.Equal("2026-01-14", days.GetProperty("to_exclusive").GetString());
            await Task.CompletedTask;
        });
    }

    [Fact]
    public async Task TheTool_SaysNotApproximate_AndCarriesNoNote_WhenNoBuiltDayWasUsed()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            await ExecAsync(connection, "DELETE FROM collect.query_store_top_daily; DELETE FROM collect.query_store_top_daily_built", ct);
            var answer = await ToolAnswerAsync(source, 72, ct);

            Assert.False(answer.GetProperty("approximate").GetBoolean());
            Assert.Equal(System.Text.Json.JsonValueKind.Null, answer.GetProperty("approximation_note").ValueKind);
            Assert.Equal(System.Text.Json.JsonValueKind.Null, answer.GetProperty("summary_days").ValueKind);
        });
    }

    [Fact]
    public async Task AGapInTheBuiltDays_UsesTheLongestRun_AndStillEqualsTodaysRead()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            /* 168 h from 2026-01-07 06:00: whole days 8..13 are built. Losing the 10th leaves 8-9 and 11-13, and the longer run is used. */
            await ExecAsync(connection, "DELETE FROM collect.query_store_top_daily WHERE day = DATE '2026-01-10'; DELETE FROM collect.query_store_top_daily_built WHERE day = DATE '2026-01-10'", ct);
            var read = await RouteAsync(source, End.AddHours(-168), End, null, null, null, ct);

            Assert.Equal(3, read.DailyDaysUsed);
            Assert.Equal((new DateOnly(2026, 1, 11), new DateOnly(2026, 1, 14)), read.DailySpan);
            Assert.Equal(await TodaysReadAsync(connection, read.Table!.Value.ReadStart, End, null, null, null, ct), Format(read.Rows));
        });
    }

    [Fact]
    public async Task ALateWriteIntoABuiltDay_IsMissedByTheRoute_AndPresentInTodaysRead()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = End.AddHours(-168);
            var before = await RouteAsync(source, start, End, null, null, null, ct);

            await InsertRowAsync(connection, new DateTime(2026, 1, 10, 10, 30, 0), 9001, ct);

            var route = await RouteAsync(source, start, End, null, null, null, ct);
            var today = await TodaysReadAsync(connection, route.Table!.Value.ReadStart, End, null, null, null, ct);

            /* The route still answers as it did before the write; today's read has the new query, ranked first. */
            Assert.Equal(Format(before.Rows), Format(route.Rows));
            Assert.DoesNotContain(route.Rows, r => r.QueryId == 9001);
            Assert.True(today.Any(t => t.Contains("|9001|", StringComparison.Ordinal)), string.Join("\n", today.Take(3)));
            Assert.Equal(route.Rows.Count, today.Count);
        });
    }

    [Fact]
    public async Task TheEdges_AreReadExactlyFromTheWideTable_AtBothBoundaries()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            /* A 72 h window ending 2026-01-14 06:00 holds the whole days 12 and 13 (the summary's [S, E) is [12, 14)).
               Written after the build, so a row the summary covers is missed and a row an edge covers is read. */
            var s = new DateTime(2026, 1, 12, 0, 0, 0);
            await InsertRowAsync(connection, s.AddSeconds(-1), 9101, ct);          // last second of the first edge: read
            await InsertRowAsync(connection, s, 9102, ct);                         // exactly S: inside the summary's first day, missed
            await InsertRowAsync(connection, new DateTime(2026, 1, 14, 0, 0, 0), 9103, ct); // exactly E: the trailing edge, read
            await InsertRowAsync(connection, End, 9104, ct);                       // exactly $3, inclusive: read
            await InsertRowAsync(connection, End.AddSeconds(1), 9105, ct);         // past $3: in neither
            await InsertRowAsync(connection, End.AddHours(-72).AddSeconds(-1), 9106, ct); // before ReadStart: in neither

            var readStart = End.AddHours(-72);
            await InsertRowAsync(connection, readStart, 9107, ct);                 // exactly ReadStart ($2 inclusive): read, by both routes
            /* The oldest first_execution_time the invariant allows for a row collected at E + 1 h (the stricter trailing floor). */
            var trailing = new DateTime(2026, 1, 14, 1, 0, 0);
            await InsertRowAsync(connection, trailing, 9108, ct,
                firstExecution: trailing - (QueryStoreIntervalWide.IntervalSpanMargin + PerformanceMonitor.Collectors.WatermarkPolicy.MaxCatchup) + TimeSpan.FromSeconds(1));

            var route = await RouteAsync(source, readStart, End, null, null, null, ct);
            Assert.Equal((new DateOnly(2026, 1, 12), new DateOnly(2026, 1, 14)), route.DailySpan);
            Assert.Equal(readStart, route.Table!.Value.ReadStart);
            var ids = route.Rows.Select(r => r.QueryId).ToHashSet();
            Assert.Contains(9101L, ids);
            Assert.DoesNotContain(9102L, ids);
            Assert.Contains(9103L, ids);
            Assert.Contains(9104L, ids);
            Assert.DoesNotContain(9105L, ids);
            Assert.DoesNotContain(9106L, ids);
            Assert.Contains(9107L, ids);
            Assert.Contains(9108L, ids);

            /* Today's read has everything the window holds except the two outside it. */
            var today = (await RunSqlAsync(connection, DarlingDataReader.QueryStoreTopTableSql, route.Table!.Value.ReadStart, End, null, null, null, null, ct))
                .Select(v => (long)v[1]!).ToHashSet();
            Assert.Contains(9102L, today);
            Assert.DoesNotContain(9105L, today);
            Assert.DoesNotContain(9106L, today);
            Assert.Contains(9107L, today);
            Assert.Contains(9108L, today);
        });
    }

    /// <summary>
    /// #5882: the daily statement's two edge reads take the narrowed first_execution_time margin (the trailing edge anchors on
    /// <c>$9::date</c>, not on <c>$2</c>). With a built run of whole days, a server of 60-minute intervals (a 3-hour margin)
    /// gets the rows, in the order, the 26-hour statement gets. The edges hold a straddler each (first executed under two hours
    /// before it was collected, across the window start and across midnight). Non-vacuity: a row that breaks the 60-minute
    /// invariant, 20 hours before its collection, is read by the 26-hour statement and dropped by the narrowed one at each edge.
    /// </summary>
    [Fact]
    public async Task TheDailyArm_WithANarrowedMargin_ReturnsTheWideMarginsRows_OverABuiltRunOfDays()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var readStart = End.AddHours(-72);                                     // 2026-01-11 06:00; days 12 and 13 are built
            var midnight = new DateTime(2026, 1, 14, 0, 0, 0);
            await InsertRowAsync(connection, readStart.AddHours(1), 9201, ct, firstExecution: readStart.AddHours(-1));      // leading edge, begun before the window
            await InsertRowAsync(connection, midnight.AddMinutes(30), 9202, ct, firstExecution: midnight.AddHours(-2));     // trailing edge, begun before midnight

            var margin = QueryStoreIntervalWide.EdgeMarginFor(TimeSpan.FromMinutes(60));
            Assert.Equal(TimeSpan.FromHours(3), margin);
            Assert.NotEqual(QueryStoreIntervalWide.PurgeEdgeMargin, margin);
            var wideDaily = DarlingDataReader.QueryStoreTopDailyTableSql;
            var narrowDaily = QueryStoreIntervalWide.WithEdgeMargin(wideDaily, QueryStoreIntervalWide.EdgeMarginSqlFor(margin));
            Assert.NotEqual(wideDaily, narrowDaily);

            var span = (new DateOnly(2026, 1, 12), new DateOnly(2026, 1, 14));
            var wideRows = Format((await RunSqlAsync(connection, wideDaily, readStart, End, null, null, null, span, ct)).Select(ToRow));
            var narrowRows = Format((await RunSqlAsync(connection, narrowDaily, readStart, End, null, null, null, span, ct)).Select(ToRow));
            Assert.Equal(Top, wideRows.Count);
            Assert.Equal(wideRows, narrowRows);
            var ids = (await RunSqlAsync(connection, narrowDaily, readStart, End, null, null, null, span, ct)).Select(v => (long)v[1]!).ToHashSet();
            Assert.Contains(9201L, ids);
            Assert.Contains(9202L, ids);

            /* Rows that break the 60-minute invariant: only the 26-hour statement reads them. */
            await InsertRowAsync(connection, readStart.AddHours(1), 9203, ct, firstExecution: readStart.AddHours(-19));
            await InsertRowAsync(connection, midnight.AddMinutes(30), 9204, ct, firstExecution: midnight.AddHours(-19).AddMinutes(30));
            var wideIds = (await RunSqlAsync(connection, wideDaily, readStart, End, null, null, null, span, ct)).Select(v => (long)v[1]!).ToHashSet();
            var narrowIds = (await RunSqlAsync(connection, narrowDaily, readStart, End, null, null, null, span, ct)).Select(v => (long)v[1]!).ToHashSet();
            Assert.Contains(9203L, wideIds);
            Assert.Contains(9204L, wideIds);
            Assert.DoesNotContain(9203L, narrowIds);
            Assert.DoesNotContain(9204L, narrowIds);
        });
    }

    [Fact]
    public async Task ARowMovedFromABuiltDayIntoTheTrailingEdge_IsCountedInBothByTheRoute_AndOnceByTodaysRead()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            /* Day 13 is rebuilt with the row in it (pass 1), then the interval resumes and the row's collection_time moves to
               the next day, which is the trailing edge (read from the wide table). The summary still holds it for day 13. */
            await InsertRowAsync(connection, new DateTime(2026, 1, 13, 23, 30, 0), 9201, ct);
            await ExecAsync(connection, "DELETE FROM collect.query_store_top_daily WHERE day = DATE '2026-01-13'; DELETE FROM collect.query_store_top_daily_built WHERE day = DATE '2026-01-13'", ct);
            var tick = await QueryStoreTopDaily.RunTickAsync(source, BuildNow, RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(1, tick.Built);
            await ExecAsync(connection, "UPDATE collect.query_store_interval_wide SET collection_time = TIMESTAMP '2026-01-14 00:30:00' WHERE query_id = 9201", ct);

            var route = await RouteAsync(source, End.AddHours(-72), End, null, null, null, ct);
            var today = (await RunSqlAsync(connection, DarlingDataReader.QueryStoreTopTableSql, route.Table!.Value.ReadStart, End, null, null, null, null, ct))
                .Select(ToRow).Single(r => r.QueryId == 9201);

            Assert.Equal(1000L, today.TotalExecutions);
            Assert.Equal(2000L, route.Rows.Single(r => r.QueryId == 9201).TotalExecutions);
        });
    }

    private static async Task<System.Text.Json.JsonElement> FilteredToolAnswerAsync(NpgsqlDataSource source, string? outcome, string? module, CancellationToken ct) =>
        System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(
            source, "srv1", 72, Top, null, "2026-01-14T06:00:00Z", outcome, module, false, 400, ct)).RootElement.Clone();

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheEmptyAnswers_CarryTheApproximateFlag_OnlyWhenBuiltDaysWereUsed(bool withBuiltDays)
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            if (!withBuiltDays)
                await ExecAsync(connection, "DELETE FROM collect.query_store_top_daily; DELETE FROM collect.query_store_top_daily_built", ct);

            var module = await FilteredToolAnswerAsync(source, null, "dbo.nothing_matches_this", ct);
            var outcome = await FilteredToolAnswerAsync(source, "Exception", null, ct);

            Assert.Equal("empty", module.GetProperty("status").GetString());
            Assert.Equal("empty", outcome.GetProperty("status").GetString());
            if (withBuiltDays)
            {
                var hints = module.GetProperty("hints");
                Assert.True(hints.GetProperty("approximate").GetBoolean());
                Assert.Equal(DarlingMcpDataTools.QueryStoreApproximationNote, hints.GetProperty("approximation_note").GetString());
                hints = outcome.GetProperty("hints");
                Assert.True(hints.GetProperty("approximate").GetBoolean());
                Assert.Equal(DarlingMcpDataTools.QueryStoreApproximationNote, hints.GetProperty("approximation_note").GetString());
            }
            else
            {
                Assert.False(module.GetProperty("hints").TryGetProperty("approximate", out _));
                Assert.False(module.GetProperty("hints").TryGetProperty("approximation_note", out _));
                /* #7010: the empty answer carries the window notice under hints, but not the approximation flag. */
                if (outcome.TryGetProperty("hints", out var outcomeHints))
                {
                    Assert.False(outcomeHints.TryGetProperty("approximate", out _));
                    Assert.False(outcomeHints.TryGetProperty("approximation_note", out _));
                }
            }
            await Task.CompletedTask;
        });
    }

    [Fact]
    public async Task AnAllNullAverageColumn_IsNullInBothReads_WithoutADivisionError()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = End.AddHours(-168);
            var plan = (await RouteAsync(source, start, End, null, null, null, ct)).Table!.Value;
            var span = (new DateOnly(2026, 1, 8), new DateOnly(2026, 1, 14));
            var route = await RunSqlAsync(connection, DarlingDataReader.QueryStoreTopDailyTableSql, plan.ReadStart, End, null, null, null, span, ct);
            var today = await RunSqlAsync(connection, DarlingDataReader.QueryStoreTopTableSql, plan.ReadStart, End, null, null, null, null, ct);

            var routeRow = route.Single(v => (long)v[1]! == 40);
            var todayRow = today.Single(v => (long)v[1]! == 40);
            for (var column = 8; column <= 13; column++)
            {
                Assert.True(routeRow[column] is DBNull, $"route column {column}");
                Assert.True(todayRow[column] is DBNull, $"today column {column}");
            }

            Assert.Equal(todayRow[7], routeRow[7]);
        });
    }
}
