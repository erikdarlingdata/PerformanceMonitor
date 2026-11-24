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
using System.Text.Json;
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
/// Pins <see cref="DarlingDataReader.QueryStoreWindowHasRowsAsync"/> (#8570), the existence probe the empty answers of
/// <c>get_query_store_top</c> and <c>get_query_store_aborts</c> ask in place of a top-1 page. Its answer equals "the top-1
/// read returned a row" on every tier (raw, the interval table, the daily summary with the interval table's edges, with the
/// full <c>first_execution_time</c> floor and with the narrower one a server of short intervals resolves), for
/// ordinary activity, an empty window, a window that holds only the monitor's own statements (by the key table and by the
/// marker in the text), only WAITFOR statements, rows of a database outside the list, and rows outside the window. The one
/// place the two differ is pinned too: more WAITFOR groups ranking first than the top read keeps candidates, where the top-1
/// page is empty and the window still holds a run. Its cost is pinned in text lookups, counted from the statement's own
/// plan. Every seed is at a fixed past date, every clock is passed explicitly, and each live fact mints its own scratch
/// database.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live fact here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it, so it cannot race live
   collection. */
public sealed class QueryStoreWindowHasRowsLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the Query Store window probe's live pins (each mints its own scratch database).";
    private const int RetentionDays = 9;
    private const string AsOf = "2026-01-14T06:00:00Z";
    private const string WaitforText = "WAITFOR DELAY '00:00:05'";
    private const string MeasuredZero = "measured zero";

    /* Hour 0 of the interval table's seed. The hours used below: 150 is the 72-hour window's start (01-11 06:00), 168 and 216
       are the midnights of the whole days inside it (01-12 and 01-14), 222 is its end (01-14 06:00). */
    private static readonly DateTime SeedStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private const int LastSeedHour = 24 * 9 + 12;
    private static readonly DateTime End = new(2026, 1, 14, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime BuildNow = new(2026, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);

    private static readonly string OwnText =
        "(@cutoff_time datetime2(7))SELECT /* " + PerformanceMonitor.Collectors.QueryStoreCollector.SelfQueryMarker + " */ qsrs.plan_id";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* ───────────────────────────── live ───────────────────────────── */

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task RunLiveAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
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
            await body(connection, source, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>Server <paramref name="serverId"/>, named <c>srv{id}</c>. With <paramref name="tableCoverage"/> the interval
    /// table's gate can choose the table for it: coverage filled since 2026-01-01 and applied through a time before every
    /// window's literal end.</summary>
    private static async Task RegisterServerAsync(NpgsqlConnection connection, int serverId, bool tableCoverage, CancellationToken ct)
    {
        await ExecAsync(connection, $"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({serverId}, 'srv{serverId}', TRUE)", ct);
        if (tableCoverage)
        {
            await ExecAsync(connection,
                $"INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES ({serverId}, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00')", ct);
        }
    }

    /// <summary>Raw rows: <paramref name="queries"/> statements from <paramref name="firstQuery"/> in one database, each with
    /// <paramref name="intervals"/> intervals ten minutes apart ending at the one that starts at <paramref name="intervalStart"/>.
    /// A row is collected five minutes after its interval starts unless <paramref name="collected"/> says when.</summary>
    private static async Task SeedRawAsync(
        NpgsqlConnection connection, int serverId, string database, long firstQuery, int queries, DateTime intervalStart, CancellationToken ct,
        string? inlineText = null, long avgDurationUs = 1000, int intervals = 1, DateTime? collected = null)
    {
        await using var command = new NpgsqlCommand($@"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
     is_forced_plan, runtime_stats_interval_id, interval_start_time_utc, query_text)
SELECT 1, @collected - make_interval(mins => 10 * i), {serverId}, 'srv{serverId}', '{database}', q, q, 'Regular',
       @started - make_interval(mins => 10 * i), @collected - make_interval(mins => 10 * i), 'h' || q, 10, {avgDurationUs}, 5,
       FALSE, 1000 - i, @started - make_interval(mins => 10 * i), @text
FROM generate_series({firstQuery}, {firstQuery + queries - 1}) AS q
CROSS JOIN generate_series(0, {intervals - 1}) AS i", connection);
        command.Parameters.Add(new NpgsqlParameter("started", NpgsqlDbType.Timestamp) { Value = intervalStart });
        command.Parameters.Add(new NpgsqlParameter("collected", NpgsqlDbType.Timestamp) { Value = collected ?? intervalStart.AddMinutes(5) });
        command.Parameters.Add(new NpgsqlParameter("text", NpgsqlDbType.Text) { Value = (object?)inlineText ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Interval-table rows: <paramref name="queries"/> statements from <paramref name="firstQuery"/> in one database,
    /// one row an hour from seed hour <paramref name="fromHour"/> through <paramref name="toHour"/>. Each row's interval
    /// starts at its collection time, so the interval-table read and the daily summary place it in the same window.</summary>
    private static async Task SeedWideAsync(
        NpgsqlConnection connection, int serverId, string database, long firstQuery, int queries, int fromHour, int toHour, CancellationToken ct,
        long avgDurationUs = 1000)
    {
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
 avg_rowcount, query_plan_hash, runtime_stats_interval_id, interval_start_time_utc)
SELECT ct, {serverId}, '{database}', q, q, 'Regular', ct - interval '10 minutes', ct,
       'h' || q, 10, {avgDurationUs}, 5, 5, 5, 5, 5, 'ph', q * 100000 + h, ct
FROM
(
    SELECT q, h, TIMESTAMP '{SeedStart.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}' + make_interval(hours => h) AS ct
    FROM generate_series({firstQuery}, {firstQuery + queries - 1}) AS q
    CROSS JOIN generate_series({fromHour}, {toHour}) AS h
) AS s;", ct);
    }

    /// <summary>The stored text of <paramref name="queries"/> statements from <paramref name="firstQuery"/>. The store's
    /// trigger records the key of a text that carries the monitor's marker.</summary>
    private static async Task SeedTextAsync(NpgsqlConnection connection, int serverId, string database, long firstQuery, int queries, string text, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($@"
INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen)
SELECT {serverId}, '{database}', q, @text, TIMESTAMP '2026-01-14 03:00:00'
FROM generate_series({firstQuery}, {firstQuery + queries - 1}) AS q", connection);
        command.Parameters.Add(new NpgsqlParameter("text", NpgsqlDbType.Text) { Value = text });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task FillSelfKeysAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        Assert.NotNull(await QueryStoreSelfKeys.BackfillAsync(connection, ct));
        Assert.True(await QueryStoreSelfKeys.ReadyAsync(connection, null, 30, ct));
    }

    private static async Task ReopenSelfKeysAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, QueryStoreSelfKeys.ReopenBackfillSql, ct);
        Assert.False(await QueryStoreSelfKeys.ReadyAsync(connection, null, 30, ct));
    }

    private static async Task<(bool Any, string? Source, ReadFallback? Fallback)> ProbeAsync(
        NpgsqlDataSource source, int serverId, DateTime start, DateTime end, DatabaseFilter databases, CancellationToken ct)
    {
        using var scope = ReadScope.Open(new CapturingTestLogger());
        var any = await DarlingDataReader.QueryStoreWindowHasRowsAsync(source, serverId, start, end, databases, ct);
        return (any, scope.Scope.Source, scope.Fallback);
    }

    /// <summary>How many times the probe's statement over <paramref name="serverId"/>'s raw rows looks a text up: the rows
    /// its scans of <c>query_store_text</c> return, read from the statement's own plan under EXPLAIN ANALYZE. Every
    /// statement seeded for it has one row in that table, so one row returned is one lookup.</summary>
    private static async Task<long> TextLookupsAsync(
        NpgsqlDataSource source, int serverId, DateTime start, DateTime end, bool selfKeysReady, CancellationToken ct)
    {
        var sql = DarlingDataReader.QueryStoreWindowProbeSql(DarlingDataReader.QueryStoreWindowProbeRawRowsSql, selfKeysReady);
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection);
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingDataReader.QueryStoreWindowProbeSampleRows });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        return ExplainBuffers.RowsRead(await explain.ExecuteScalarAsync(ct), new HashSet<string>(StringComparer.Ordinal) { "query_store_text" });
    }

    /// <summary>The probe's answer and "the top-1 read returned a row" are both <paramref name="expected"/>, and the probe
    /// read the tier named by <paramref name="expectedSource"/>.</summary>
    private static async Task AssertAnswersAsTopOneAsync(
        NpgsqlDataSource source, int serverId, DateTime start, DateTime end, DatabaseFilter databases, bool expected, string expectedSource,
        string what, CancellationToken ct)
    {
        var top = await DarlingDataReader.GetQueryStoreTopAsync(source, serverId, start, end, 1, databases, ct);
        var (any, probeSource, fallback) = await ProbeAsync(source, serverId, start, end, databases, ct);
        Assert.True(expected == top.Count > 0, $"{what}: the top-1 read returned {top.Count} row(s), expected a row: {expected}");
        Assert.True(expected == any, $"{what}: the probe answered {any}, expected {expected}");
        Assert.Equal(expectedSource, probeSource);
        Assert.Null(fallback);
    }

    /// <summary>
    /// The cost of a window the probe cannot answer from its first rows, in text lookups. A window of fewer rows than the
    /// sample is answered by the sample alone: one lookup for each row and no second pass. A larger window of only WAITFOR
    /// statements costs at most one lookup for each sampled row and one for each distinct statement, however many intervals
    /// a statement has. With the keys ready, a statement of the monitor's own is left out by its key and costs no lookup,
    /// in the sample and among the distinct statements; with the keys not ready its text is tested as a WAITFOR
    /// statement's is.
    /// </summary>
    [Fact]
    public async Task TheProbe_LooksATextUpOnceForEachSampledRowAndEachStatement_AndNeverForAKeyOfTheMonitorsOwn()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = End.AddHours(-6);
            var at = End.AddHours(-2);
            const int sample = DarlingDataReader.QueryStoreWindowProbeSampleRows;
            const int few = sample / 2;
            const int statements = 200;
            const int own = 100;
            for (var server = 1; server <= 3; server++)
            {
                await RegisterServerAsync(connection, server, tableCoverage: false, ct);
            }

            /* 1: fewer WAITFOR statements than the sample holds rows, one interval each. 2: 200 WAITFOR statements, three
               intervals each, and 100 of the monitor's own. 3: 200 of the monitor's own statements, one interval each. */
            await SeedRawAsync(connection, 1, "db0", 1, few, at, ct);
            await SeedTextAsync(connection, 1, "db0", 1, few, WaitforText, ct);
            await SeedRawAsync(connection, 2, "db0", 1, statements, at, ct, intervals: 3);
            await SeedTextAsync(connection, 2, "db0", 1, statements, WaitforText, ct);
            await SeedRawAsync(connection, 2, "db0", 1000, own, at, ct, intervals: 3);
            await SeedTextAsync(connection, 2, "db0", 1000, own, OwnText, ct);
            await SeedRawAsync(connection, 3, "db0", 1, statements, at, ct);
            await SeedTextAsync(connection, 3, "db0", 1, statements, OwnText, ct);
            await FillSelfKeysAsync(connection, ct);

            for (var server = 1; server <= 3; server++)
            {
                Assert.False((await ProbeAsync(source, server, start, End, DatabaseFilter.All, ct)).Any, $"server {server} holds no showable row");
            }

            Assert.Equal(few, await TextLookupsAsync(source, 1, start, End, selfKeysReady: true, ct));
            Assert.InRange(await TextLookupsAsync(source, 2, start, End, selfKeysReady: true, ct), statements, sample + statements);
            Assert.InRange(await TextLookupsAsync(source, 2, start, End, selfKeysReady: false, ct), statements + own, sample + statements + own);
            Assert.Equal(0, await TextLookupsAsync(source, 3, start, End, selfKeysReady: true, ct));
            Assert.InRange(await TextLookupsAsync(source, 3, start, End, selfKeysReady: false, ct), statements, sample + statements);
        });
    }

    [Fact]
    public async Task OnTheRawTier_TheProbeAnswersAsTheTopOneRead()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = End.AddHours(-6);
            var at = End.AddHours(-2);
            for (var server = 1; server <= 9; server++)
            {
                await RegisterServerAsync(connection, server, tableCoverage: false, ct);
            }

            /* 1: ordinary activity with no stored text. 2: only the monitor's own statements, their text in the text table.
               3: only WAITFOR, the text inline in the rows. 4: nothing. 5: the monitor's own, WAITFOR and one ordinary statement.
               6: the monitor's own by key alone (the text is deleted below). 7: rows in one database, dbx. 8: intervals that
               started before the window and were collected inside it. 9: more WAITFOR statements than the probe samples rows,
               three intervals each. */
            await SeedRawAsync(connection, 1, "db0", 1, 3, at, ct);
            await SeedRawAsync(connection, 2, "db0", 1, 3, at, ct);
            await SeedTextAsync(connection, 2, "db0", 1, 3, OwnText, ct);
            await SeedRawAsync(connection, 3, "db0", 1, 3, at, ct, inlineText: WaitforText);
            await SeedRawAsync(connection, 5, "db0", 1, 2, at, ct);
            await SeedTextAsync(connection, 5, "db0", 1, 2, OwnText, ct);
            await SeedRawAsync(connection, 5, "db0", 3, 1, at, ct, inlineText: WaitforText);
            await SeedRawAsync(connection, 5, "db0", 4, 1, at, ct);
            await SeedRawAsync(connection, 6, "db0", 1, 3, at, ct);
            await SeedTextAsync(connection, 6, "db0", 1, 3, OwnText, ct);
            await SeedRawAsync(connection, 7, "dbx", 1, 3, at, ct);
            await SeedRawAsync(connection, 8, "db0", 1, 3, start.AddMinutes(-30), ct, collected: start.AddMinutes(10));
            await SeedRawAsync(connection, 9, "db0", 1, 200, at, ct, intervals: 3);
            await SeedTextAsync(connection, 9, "db0", 1, 200, WaitforText, ct);
            await FillSelfKeysAsync(connection, ct);
            await ExecAsync(connection, "DELETE FROM collect.query_store_text WHERE server_id = 6", ct);

            const string raw = ReadScope.SourceRaw;
            var all = DatabaseFilter.All;
            await AssertAnswersAsTopOneAsync(source, 1, start, End, all, true, raw, "ordinary activity", ct);
            await AssertAnswersAsTopOneAsync(source, 2, start, End, all, false, raw, "only the monitor's own statements, keys ready", ct);
            await AssertAnswersAsTopOneAsync(source, 3, start, End, all, false, raw, "only WAITFOR", ct);
            await AssertAnswersAsTopOneAsync(source, 4, start, End, all, false, raw, "an empty window", ct);
            await AssertAnswersAsTopOneAsync(source, 5, start, End, all, true, raw, "own, WAITFOR and one ordinary statement", ct);
            await AssertAnswersAsTopOneAsync(source, 6, start, End, all, false, raw, "the monitor's own by key alone, keys ready", ct);
            await AssertAnswersAsTopOneAsync(source, 7, start, End, all, true, raw, "every database", ct);
            await AssertAnswersAsTopOneAsync(source, 7, start, End, DatabaseFilter.One("dbx"), true, raw, "the database that holds the rows", ct);
            await AssertAnswersAsTopOneAsync(source, 7, start, End, DatabaseFilter.One("dby"), false, raw, "a database outside the rows", ct);
            await AssertAnswersAsTopOneAsync(source, 7, start, End, DatabaseFilter.Of(new[] { "dby", "dbx" }), true, raw, "a list that names the rows' database", ct);
            await AssertAnswersAsTopOneAsync(source, 7, start, End, DatabaseFilter.Of(new[] { "dby", "dbz" }), false, raw, "a list that does not name it", ct);
            await AssertAnswersAsTopOneAsync(source, 8, start, End, all, false, raw, "intervals that started before the window", ct);
            await AssertAnswersAsTopOneAsync(source, 9, start, End, all, false, raw, "200 WAITFOR statements", ct);

            /* A window the rows are not in, and a long window the gate sends to raw (the servers have no interval-table coverage). */
            await AssertAnswersAsTopOneAsync(source, 1, End.AddHours(-30), End.AddHours(-24), all, false, raw, "a window before the rows", ct);
            await AssertAnswersAsTopOneAsync(source, 1, End.AddHours(-72), End, all, true, raw, "72 hours with no table coverage", ct);
            await AssertAnswersAsTopOneAsync(source, 4, End.AddHours(-72), End, all, false, raw, "72 empty hours with no table coverage", ct);

            /* The keys not ready: the text test leaves the monitor's own statements out, and a key alone excludes nothing. */
            await ReopenSelfKeysAsync(connection, ct);
            await AssertAnswersAsTopOneAsync(source, 2, start, End, all, false, raw, "only the monitor's own statements, keys not ready", ct);
            await AssertAnswersAsTopOneAsync(source, 5, start, End, all, true, raw, "own, WAITFOR and one ordinary statement, keys not ready", ct);
            await AssertAnswersAsTopOneAsync(source, 6, start, End, all, true, raw, "the monitor's own by key alone, keys not ready", ct);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OnTheIntervalTable_AndOnTheDailySummary_TheProbeAnswersAsTheTopOneRead(bool daily)
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = End.AddHours(-72);
            for (var server = 1; server <= 7; server++)
            {
                await RegisterServerAsync(connection, server, tableCoverage: true, ct);
            }

            /* 1: ordinary activity every hour in db0, and one database for each part of the window: dblead in the leading edge
               (01-11), dbmid in the whole days (01-12), dbtrail in the trailing edge (01-14 before 06:00), dbold before the window
               and dbafter after it. 2: only the monitor's own statements. 3: only WAITFOR. 4: nothing. 5: own, WAITFOR and one
               ordinary statement. 6: the monitor's own by key alone. 7: rows before the window and after it, none inside. */
            await SeedWideAsync(connection, 1, "db0", 1, 3, 0, LastSeedHour, ct);
            await SeedWideAsync(connection, 1, "dblead", 11, 1, 156, 160, ct);
            await SeedWideAsync(connection, 1, "dbmid", 12, 1, 180, 182, ct);
            await SeedWideAsync(connection, 1, "dbtrail", 13, 1, 218, 220, ct);
            await SeedWideAsync(connection, 1, "dbold", 14, 1, 80, 82, ct);
            await SeedWideAsync(connection, 1, "dbafter", 15, 1, 224, 226, ct);
            await SeedWideAsync(connection, 2, "db0", 1, 3, 0, LastSeedHour, ct);
            await SeedTextAsync(connection, 2, "db0", 1, 3, OwnText, ct);
            await SeedWideAsync(connection, 3, "db0", 1, 3, 0, LastSeedHour, ct);
            await SeedTextAsync(connection, 3, "db0", 1, 3, WaitforText, ct);
            await SeedWideAsync(connection, 5, "db0", 1, 5, 0, LastSeedHour, ct);
            await SeedTextAsync(connection, 5, "db0", 1, 2, OwnText, ct);
            await SeedTextAsync(connection, 5, "db0", 3, 2, WaitforText, ct);
            await SeedWideAsync(connection, 6, "db0", 1, 3, 0, LastSeedHour, ct);
            await SeedTextAsync(connection, 6, "db0", 1, 3, OwnText, ct);
            await SeedWideAsync(connection, 7, "db0", 1, 3, 0, 100, ct);
            await SeedWideAsync(connection, 7, "db0", 1, 3, 224, LastSeedHour, ct);
            await FillSelfKeysAsync(connection, ct);
            await ExecAsync(connection, "DELETE FROM collect.query_store_text WHERE server_id = 6", ct);

            if (daily)
            {
                var built = await QueryStoreTopDaily.RunTickAsync(source, BuildNow, RetentionDays, NullLogger.Instance, ct);
                Assert.Equal(0, built.Failed);
                Assert.True(built.Built > 0);
                /* dbmid's whole days are in the summary alone. */
                await ExecAsync(connection, "DELETE FROM collect.query_store_interval_wide WHERE server_id = 1 AND database_name = 'dbmid'", ct);
            }

            /* The tier the top read serves each server's window from: the table, with built days exactly when they were built. */
            foreach (var server in new[] { 1, 2, 3, 5, 6 })
            {
                var read = await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, server, start, End, 1, DatabaseFilter.All, null, null, ct);
                Assert.True(read.Table is not null, $"server {server}: the gate chose raw");
                Assert.True(daily == read.DailyDaysUsed > 0, $"server {server}: {read.DailyDaysUsed} built day(s) used");
            }

            const string table = ReadScope.SourceIntervalTable;
            var all = DatabaseFilter.All;
            await AssertAnswersAsTopOneAsync(source, 1, start, End, all, true, table, "ordinary activity", ct);
            await AssertAnswersAsTopOneAsync(source, 1, start, End, DatabaseFilter.One("dblead"), true, table, "rows in the leading edge alone", ct);
            await AssertAnswersAsTopOneAsync(source, 1, start, End, DatabaseFilter.One("dbmid"), true, table, "rows in the whole days alone", ct);
            await AssertAnswersAsTopOneAsync(source, 1, start, End, DatabaseFilter.One("dbtrail"), true, table, "rows in the trailing edge alone", ct);
            await AssertAnswersAsTopOneAsync(source, 1, start, End, DatabaseFilter.One("dbold"), false, table, "rows before the window alone", ct);
            await AssertAnswersAsTopOneAsync(source, 1, start, End, DatabaseFilter.One("dbafter"), false, table, "rows after the window alone", ct);
            await AssertAnswersAsTopOneAsync(source, 1, start, End, DatabaseFilter.One("dbnone"), false, table, "a database with no rows", ct);
            await AssertAnswersAsTopOneAsync(source, 1, start, End, DatabaseFilter.Of(new[] { "dbold", "dbtrail" }), true, table, "a list with one database in the window", ct);
            await AssertAnswersAsTopOneAsync(source, 1, start, End, DatabaseFilter.Of(new[] { "dbold", "dbafter" }), false, table, "a list with no database in the window", ct);
            await AssertAnswersAsTopOneAsync(source, 2, start, End, all, false, table, "only the monitor's own statements, keys ready", ct);
            await AssertAnswersAsTopOneAsync(source, 3, start, End, all, false, table, "only WAITFOR", ct);
            await AssertAnswersAsTopOneAsync(source, 4, start, End, all, false, table, "an empty window", ct);
            await AssertAnswersAsTopOneAsync(source, 5, start, End, all, true, table, "own, WAITFOR and one ordinary statement", ct);
            await AssertAnswersAsTopOneAsync(source, 6, start, End, all, false, table, "the monitor's own by key alone, keys ready", ct);
            await AssertAnswersAsTopOneAsync(source, 7, start, End, all, false, table, "rows before and after the window", ct);

            /* The keys not ready: the text test leaves the monitor's own statements out, and a key alone excludes nothing. */
            await ReopenSelfKeysAsync(connection, ct);
            await AssertAnswersAsTopOneAsync(source, 2, start, End, all, false, table, "only the monitor's own statements, keys not ready", ct);
            await AssertAnswersAsTopOneAsync(source, 5, start, End, all, true, table, "own, WAITFOR and one ordinary statement, keys not ready", ct);
            await AssertAnswersAsTopOneAsync(source, 6, start, End, all, true, table, "the monitor's own by key alone, keys not ready", ct);

            /* A fault after the gate chose the table: the probe notes the fallback and answers from raw, which holds nothing
               here, as the top-1 read does. */
            DarlingDataReader.TestOnlyTableReadFault.Value = () => new InvalidOperationException("injected table read fault");
            try
            {
                var (any, probeSource, fallback) = await ProbeAsync(source, 1, start, End, all, ct);
                Assert.False(any);
                Assert.Equal(ReadScope.SourceRaw, probeSource);
                Assert.Equal(ReadFallback.FallbackRaw, fallback);
                Assert.Empty(await DarlingDataReader.GetQueryStoreTopAsync(source, 1, start, End, 1, all, ct));
            }
            finally
            {
                DarlingDataReader.TestOnlyTableReadFault.Value = null;
            }
        });
    }

    /// <summary>One interval-table row of statement <paramref name="queryId"/> in <paramref name="database"/>, its
    /// interval starting at <paramref name="intervalStart"/>, collected at <paramref name="collected"/> and first executed
    /// at <paramref name="firstExecution"/>.</summary>
    private static async Task SeedWideRowAsync(
        NpgsqlConnection connection, int serverId, string database, long queryId, DateTime intervalStart, DateTime collected,
        DateTime firstExecution, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
 avg_rowcount, query_plan_hash, runtime_stats_interval_id, interval_start_time_utc)
VALUES (@collected, {serverId}, '{database}', {queryId}, {queryId}, 'Regular', @first, @collected,
        'h{queryId}', 10, 1000, 5, 5, 5, 5, 5, 'ph', {queryId}, @started)", connection);
        command.Parameters.Add(new NpgsqlParameter("collected", NpgsqlDbType.Timestamp) { Value = collected });
        command.Parameters.Add(new NpgsqlParameter("first", NpgsqlDbType.Timestamp) { Value = firstExecution });
        command.Parameters.Add(new NpgsqlParameter("started", NpgsqlDbType.Timestamp) { Value = intervalStart });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// A server whose Query Store intervals are 60 minutes resolves a <c>first_execution_time</c> floor of three hours
    /// before the read (<see cref="QueryStoreIntervalWide.ResolveEdgeMarginAsync"/>) in place of 26, and the probe reads with
    /// that floor as the top read does. A row first executed exactly at the floor counts on both tiers, and one a minute
    /// before it counts on neither, though the 26-hour floor would take it. The daily read's trailing edge carries its own
    /// floor, three hours before its first instant <c>E</c>: a row a minute below that floor counts on the interval table,
    /// whose single floor sits at the window start, and not on the daily read.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WithTheNarrowerEdgeMarginOfShortIntervals_TheProbeAnswersAsTheTopOneRead_AtTheFloorAndJustBelowIt(bool daily)
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            const int server = 1;
            var start = End.AddHours(-72);
            var trailingEdge = End.Date;
            var margin = QueryStoreIntervalWide.EdgeMarginFor(TimeSpan.FromMinutes(60));
            Assert.True(margin < QueryStoreIntervalWide.PurgeEdgeMargin);
            var databases = new[] { "db0", "dbfloor", "dbbelow", "dbtrailfloor", "dbtrailbelow" };

            await RegisterServerAsync(connection, server, tableCoverage: true, ct);
            await SeedWideAsync(connection, server, "db0", 1, 3, 0, LastSeedHour, ct);

            /* A 60-minute interval length for every database the server has rows for, captured every six hours from two
               days before the window, so the oldest capture is more than a day before the read starts. */
            await ExecAsync(connection, $"""
                INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
                SELECT row_number() OVER (), c.c, {server}, 'srv{server}', d.d, 60
                FROM generate_series(timestamp '{start.AddDays(-2):yyyy-MM-dd HH:mm}', timestamp '{End:yyyy-MM-dd HH:mm}', interval '6 hours') AS c(c)
                CROSS JOIN unnest(ARRAY['{string.Join("', '", databases)}']) AS d(d)
                """, ct);

            var readStart = (await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, server, start, End, 1, DatabaseFilter.All, null, null, ct))
                .Table?.ReadStart;
            Assert.NotNull(readStart);
            var floor = readStart.Value - margin;
            var trailingFloor = trailingEdge - margin;
            var minute = TimeSpan.FromMinutes(1);
            await SeedWideRowAsync(connection, server, "dbfloor", 21, readStart.Value, readStart.Value.AddMinutes(2), floor, ct);
            await SeedWideRowAsync(connection, server, "dbbelow", 22, readStart.Value, readStart.Value.AddMinutes(2), floor - minute, ct);
            await SeedWideRowAsync(connection, server, "dbtrailfloor", 23, trailingEdge, trailingEdge.AddMinutes(2), trailingFloor, ct);
            await SeedWideRowAsync(connection, server, "dbtrailbelow", 24, trailingEdge, trailingEdge.AddMinutes(2), trailingFloor - minute, ct);
            await FillSelfKeysAsync(connection, ct);

            if (daily)
            {
                var built = await QueryStoreTopDaily.RunTickAsync(source, BuildNow, RetentionDays, NullLogger.Instance, ct);
                Assert.Equal(0, built.Failed);
                Assert.True(built.Built > 0);
            }

            /* The table serves from the same start, the daily read's days are 01-12 and 01-13 (its trailing edge starts at
               01-14 00:00), and the server's margin is the narrower one. */
            var read = await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, server, start, End, 1, DatabaseFilter.All, null, null, ct);
            Assert.Equal(readStart, read.Table?.ReadStart);
            if (daily)
            {
                Assert.Equal(new DateOnly(2026, 1, 12), read.DailySpan?.Start);
                Assert.Equal(DateOnly.FromDateTime(trailingEdge), read.DailySpan?.EndExclusive);
            }
            else
            {
                Assert.Equal(0, read.DailyDaysUsed);
            }

            Assert.Equal(margin, await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, server, readStart.Value, End, 30, ct));

            const string table = ReadScope.SourceIntervalTable;
            await AssertAnswersAsTopOneAsync(source, server, start, End, DatabaseFilter.One("dbfloor"), true, table, "first executed at the floor", ct);
            await AssertAnswersAsTopOneAsync(source, server, start, End, DatabaseFilter.One("dbbelow"), false, table, "first executed a minute below the floor", ct);
            await AssertAnswersAsTopOneAsync(source, server, start, End, DatabaseFilter.One("dbtrailfloor"), true, table, "first executed at the trailing edge's floor", ct);
            await AssertAnswersAsTopOneAsync(source, server, start, End, DatabaseFilter.One("dbtrailbelow"), !daily, table, "first executed a minute below the trailing edge's floor", ct);
            await AssertAnswersAsTopOneAsync(source, server, start, End, DatabaseFilter.Of(new[] { "dbbelow", "dbtrailfloor" }), true, table, "a list with one row inside the floors", ct);
        });
    }

    private static async Task<JsonElement> TopToolAsync(NpgsqlDataSource source, string server, DatabaseFilter databases, string? outcome, string? module, CancellationToken ct) =>
        JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(
            source, server, 6, 10, databases, AsOf, outcome, module, false, 400, ct)).RootElement.Clone();

    private static async Task<JsonElement> AbortsToolAsync(NpgsqlDataSource source, string server, CancellationToken ct) =>
        JsonDocument.Parse(await DarlingMcpQueryStoreAbortsTools.GetQueryStoreAborts(
            source, server, 6, 20, DatabaseFilter.All, AsOf, false, 400, ct)).RootElement.Clone();

    private static string Message(JsonElement answer) => answer.GetProperty("message").GetString() ?? "";

    /// <summary>
    /// The one place the probe and the top-1 read differ. The top read ranks, keeps <see cref="TopFill.FinalCandidates"/>
    /// candidates and then drops the WAITFOR ones, so with more WAITFOR groups ranking first than candidates its page is
    /// empty; the window still holds an ordinary run, and the probe says so. One WAITFOR group fewer than candidates and
    /// the ordinary run is a candidate: both answer alike.
    /// </summary>
    [Fact]
    public async Task WithMoreWaitforGroupsThanCandidates_TheTopOnePageIsEmpty_AndTheProbeStillSeesTheOrdinaryRun()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = End.AddHours(-6);
            var at = End.AddHours(-2);
            var candidates = TopFill.FinalCandidates(1);
            await RegisterServerAsync(connection, 1, tableCoverage: false, ct);
            await RegisterServerAsync(connection, 2, tableCoverage: false, ct);
            await SeedRawAsync(connection, 1, "db0", 1, candidates + 1, at, ct, avgDurationUs: 900_000_000);
            await SeedTextAsync(connection, 1, "db0", 1, candidates + 1, WaitforText, ct);
            await SeedRawAsync(connection, 1, "db0", 5000, 1, at, ct);
            await SeedRawAsync(connection, 2, "db0", 1, candidates - 1, at, ct, avgDurationUs: 900_000_000);
            await SeedTextAsync(connection, 2, "db0", 1, candidates - 1, WaitforText, ct);
            await SeedRawAsync(connection, 2, "db0", 5000, 1, at, ct);

            Assert.Empty(await DarlingDataReader.GetQueryStoreTopAsync(source, 1, start, End, 1, DatabaseFilter.All, ct));
            Assert.True((await ProbeAsync(source, 1, start, End, DatabaseFilter.All, ct)).Any);
            await AssertAnswersAsTopOneAsync(source, 2, start, End, DatabaseFilter.All, true, ReadScope.SourceRaw, "one WAITFOR group fewer than candidates", ct);

            /* The tool's empty answer for that window: no run ended in an exception, and the window holds runs. */
            var answer = await TopToolAsync(source, "srv1", DatabaseFilter.All, "Exception", null, ct);
            Assert.Equal("empty", answer.GetProperty("status").GetString());
            Assert.Contains(MeasuredZero, Message(answer), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task TheTools_SayMeasuredZero_WhereTheWindowHoldsRuns_AndNotWhereItHoldsNone()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            await RegisterServerAsync(connection, 1, tableCoverage: false, ct);
            await RegisterServerAsync(connection, 2, tableCoverage: false, ct);
            await SeedRawAsync(connection, 1, "db0", 1, 3, End.AddHours(-2), ct);

            /* Runs in the window, none matching the filter: a measured zero. */
            var outcome = await TopToolAsync(source, "srv1", DatabaseFilter.All, "Exception", null, ct);
            Assert.Equal("empty", outcome.GetProperty("status").GetString());
            Assert.Contains(MeasuredZero, Message(outcome), StringComparison.Ordinal);
            var module = await TopToolAsync(source, "srv1", DatabaseFilter.All, null, "dbo.nothing_matches_this", ct);
            Assert.Equal("empty", module.GetProperty("status").GetString());
            Assert.Contains(MeasuredZero, Message(module), StringComparison.Ordinal);
            var aborts = await AbortsToolAsync(source, "srv1", ct);
            Assert.Equal("empty", aborts.GetProperty("status").GetString());
            Assert.Contains(MeasuredZero, Message(aborts), StringComparison.Ordinal);

            /* No run in the scope asked for: the filtered read's empty answer comes from the chain below the probe. */
            var otherDatabase = await TopToolAsync(source, "srv1", DatabaseFilter.One("dbnone"), "Exception", null, ct);
            Assert.DoesNotContain(MeasuredZero, Message(otherDatabase), StringComparison.Ordinal);
            var none = await TopToolAsync(source, "srv2", DatabaseFilter.All, "Exception", null, ct);
            Assert.Equal("unavailable", none.GetProperty("status").GetString());
            Assert.DoesNotContain(MeasuredZero, Message(none), StringComparison.Ordinal);
            var noModule = await TopToolAsync(source, "srv2", DatabaseFilter.All, null, "dbo.nothing_matches_this", ct);
            Assert.Equal("unavailable", noModule.GetProperty("status").GetString());
            Assert.DoesNotContain(MeasuredZero, Message(noModule), StringComparison.Ordinal);
            var noAborts = await AbortsToolAsync(source, "srv2", ct);
            Assert.NotEqual("ok", noAborts.GetProperty("status").GetString());
            Assert.DoesNotContain(MeasuredZero, Message(noAborts), StringComparison.Ordinal);
        });
    }
}
