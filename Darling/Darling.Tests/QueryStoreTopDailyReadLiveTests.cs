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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
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

    /// <summary>SHA-256 of each pre-split statement, line endings normalized, taken from the source before
    /// <c>QueryStoreTopSuffix</c> was divided into its ranked head and its tail.</summary>
    private const string RawSqlHash = "03D5BDDC31B30371CC683DD025FCBE3F62F3374BCAC4DA88111FC244867B26BE";
    private const string TableSqlHash = "AD0855103894947DCFAE5F7754F307AEDC3EE989DD57C570C839CC44F4F3FED1";

    private static string Hash(string sql) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql.ReplaceLineEndings("\n"))));

    [Fact]
    public void TheRawAndTableStatements_AreByteIdenticalToTheirPreSplitText()
    {
        Assert.Equal(RawSqlHash, Hash(DarlingDataReader.QueryStoreTopSql));
        Assert.Equal(TableSqlHash, Hash(DarlingDataReader.QueryStoreTopTableSql));
    }

    [Fact]
    public void TheDailyStatement_SharesTheTableReadsTail_AndItsPredicates()
    {
        var daily = DarlingDataReader.QueryStoreTopDailyTableSql.ReplaceLineEndings("\n");
        var table = DarlingDataReader.QueryStoreTopTableSql.ReplaceLineEndings("\n");
        var tail = table[table.IndexOf("SELECT\n    r.database_name,", StringComparison.Ordinal)..];
        Assert.EndsWith(tail, daily, StringComparison.Ordinal);
        Assert.Contains("$7::text IS NULL OR module_name = $7", daily, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4 + 5", daily, StringComparison.Ordinal);
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
    private static async Task InsertRowAsync(NpgsqlConnection connection, DateTime collectionTime, long queryId, CancellationToken ct, long duration = 90_000_000)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
 avg_rowcount, query_plan_hash, runtime_stats_interval_id, interval_start_time_utc)
VALUES (@ct, 1, 'db0', @q, @q, 'Regular', @ct - interval '10 minutes', @ct, 'h' || @q, 1000, @dur, 5, 5, 5, 5, 5, 'late', @q, @ct - interval '10 minutes')", connection);
        command.Parameters.Add(new NpgsqlParameter("ct", NpgsqlDbType.Timestamp) { Value = collectionTime });
        command.Parameters.Add(new NpgsqlParameter("q", NpgsqlDbType.Bigint) { Value = queryId });
        command.Parameters.Add(new NpgsqlParameter("dur", NpgsqlDbType.Bigint) { Value = duration });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task BuildAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        var result = await QueryStoreTopDaily.RunTickAsync(source, BuildNow, RetentionDays, NullLogger.Instance, ct);
        Assert.Equal(0, result.Failed);
        Assert.Equal(7, result.Built);
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
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)outcome ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)module ?? DBNull.Value });
        if (span is var (s, e))
        {
            command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = s, NpgsqlDbType = NpgsqlDbType.Date });
            command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = e, NpgsqlDbType = NpgsqlDbType.Date });
        }

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

            var route = await RouteAsync(source, End.AddHours(-72), End, null, null, null, ct);
            Assert.Equal((new DateOnly(2026, 1, 12), new DateOnly(2026, 1, 14)), route.DailySpan);
            var ids = route.Rows.Select(r => r.QueryId).ToHashSet();
            Assert.Contains(9101L, ids);
            Assert.DoesNotContain(9102L, ids);
            Assert.Contains(9103L, ids);
            Assert.Contains(9104L, ids);
            Assert.DoesNotContain(9105L, ids);
            Assert.DoesNotContain(9106L, ids);

            /* Today's read has everything the window holds except the two outside it. */
            var today = (await RunSqlAsync(connection, DarlingDataReader.QueryStoreTopTableSql, route.Table!.Value.ReadStart, End, null, null, null, null, ct))
                .Select(v => (long)v[1]!).ToHashSet();
            Assert.Contains(9102L, today);
            Assert.DoesNotContain(9105L, today);
            Assert.DoesNotContain(9106L, today);
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
