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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Daily Summary's calendar-month read (the hourly tier) counted unique queries with a distinct over (day, query_hash) on the interval
/// hourly rollup: about 6.3 million rows sorted, and spilled, on the busiest server of a test store. It now counts each closed day the interval
/// daily holds from the daily, in about 24 times fewer rows, and reads the hourly only for the other days. The result must not change: the whole
/// reply (every day's unique queries included) is compared with the raw statement and with the statement as it was, EXCEPT both ways, over a window
/// whose yesterday the daily has not refreshed, whose one middle day is missing from the daily, and whose edges cut a day, on the interval hourly
/// alone and on the stitched pair. The plan must read hourly rows only for the days no daily holds. A store without the interval daily gets the
/// statement as it was.
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): each test mints its own scratch database through <see cref="ScratchPostgres"/> and drops it whole.</para>
/// </summary>
public sealed class DailySummaryHourlyTierDailyCountLiveTests
{
    private const int ServerId = 8549;
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8549 tests.";

    [Fact]
    public async Task TheMonthRead_CountsClosedDaysFromTheDaily_AndEqualsTheRawStatementAndTheOldText()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await PrepareAsync(scratch, connection, ct);

        var now = LiveClock.Now();
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        var yesterday = today.AddDays(-1);
        var hourEnd = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(-1);
        await SeedAsync(connection, today, now, ct);

        /* The interval hourly holds every closed hour. The interval daily refreshes through the day before yesterday, as the product's own
           daily refresh ends a day short of now: yesterday is a closed day the daily has not reached. */
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, today.AddDays(-8), hourEnd, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, today.AddDays(-8), yesterday, ct);
        var daily = await ResolveAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {daily} WHERE server_id = {ServerId} AND query_hash = 'RST_ONLY_D2'", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {daily} WHERE server_id = {ServerId} AND bucket >= '{Ts(yesterday)}'", ct));
        Assert.True(await ScalarAsync<long>(connection, $"SELECT count(*) FROM {daily} WHERE server_id = {ServerId} AND bucket = '{Ts(today.AddDays(-2))}'", ct) > 0);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(rollups.QueryGrainIntervalDaily);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        var oldCoverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups with { QueryGrainIntervalDaily = false }, ct);
        Assert.True(coverage.HasQueryStatsIntervalDaily);
        Assert.False(oldCoverage.HasQueryStatsIntervalDaily);

        var start = today.AddDays(-6);
        var end = today.AddDays(1);
        var newSql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage, start);
        var oldSql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, oldCoverage, start);

        /* Every day of the window, yesterday (not in the daily) and today (open) included. The days differ in their counts, so a day read from
           the wrong place shows. */
        var rows = await AssertSameAsync(connection, DailySummarySql.RangeSql, newSql, start, end, ct);
        await AssertSameAsync(connection, oldSql, newSql, start, end, ct);
        Assert.True(rows >= 7, $"the window holds a row per day, got {rows}");
        Assert.True(await ScalarAsync<long>(connection, "SELECT count(DISTINCT unique_queries) FROM qs_b", ct) >= 5, "the seed gives the days different counts");

        /* Windows that cut a day: the first and the last day read the hourly, clamped, to the microsecond. */
        await AssertSameAsync(connection, DailySummarySql.RangeSql, newSql, today.AddDays(-5).AddHours(6).AddMinutes(30).AddTicks(10), yesterday.AddHours(12).AddMinutes(30).AddTicks(6543210), ct);
        await AssertSameAsync(connection, DailySummarySql.RangeSql, newSql, today.AddDays(-5).AddHours(6).AddMinutes(30).AddTicks(10), today.AddHours(2).AddMinutes(17).AddTicks(1234560), ct);

        /* Hourly rows the plan reads. The closed days the daily holds cost none of them: only the days it lacks (yesterday, today) are read, and the
           old text reads every day. A per-day probe of the ceiling and the not-carried days reads a row or two a day, hence the slack. */
        var hourlyChunks = await ChunkNamesAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, ct);
        Assert.NotEmpty(hourlyChunks);
        var lacking = await ScalarAsync<long>(connection, $"SELECT count(*) FROM {await ResolveAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, ct)} WHERE server_id = {ServerId} AND bucket >= '{Ts(yesterday)}'", ct);
        var oldRead = await RowsReadAsync(connection, oldSql, start, end, hourlyChunks, ct);
        var newRead = await RowsReadAsync(connection, newSql, start, end, hourlyChunks, ct);
        Assert.True(oldRead > lacking * 3, $"the old text reads the window's hourly rows, {oldRead}, against {lacking} for the days the daily lacks");
        Assert.True(newRead <= lacking + 60, $"the new text read {newRead} hourly rows, more than the {lacking} of the days the daily lacks");

        /* A closed day in the middle that the daily does not hold (a refresh that never reached it): that day reads the hourly, and the
           reply is still the raw statement's. */
        var hole = today.AddDays(-4);
        await ExecuteAsync(connection, $"DELETE FROM {daily} WHERE bucket = '{Ts(hole)}'", ct);
        await AssertSameAsync(connection, DailySummarySql.RangeSql, newSql, start, end, ct);
        await AssertSameAsync(connection, oldSql, newSql, start, end, ct);

        /* A store without the interval daily gets the statement as it was, and runs. */
        await ExecuteAsync(connection, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.QueryStatsIntervalDailyView}", ct);
        var without = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, await TimescaleSupport.DetectRollupsAsync(dataSource, ct), ct);
        Assert.False(without.HasQueryStatsIntervalDaily);
        Assert.Equal(oldSql, DailySummarySql.RangeSqlFor(RetentionTier.Hourly, without, start));
        await AssertSameAsync(connection, DailySummarySql.RangeSql, DailySummarySql.RangeSqlFor(RetentionTier.Hourly, without, start), start, end, ct);
    }

    [Fact]
    public async Task TheStitchedMonthRead_CountsTheSuccessorsClosedDaysFromItsDaily_AndEqualsTheRawStatementAndTheOldText()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await PrepareAsync(scratch, connection, ct);

        var now = LiveClock.Now();
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        var yesterday = today.AddDays(-1);
        var hourEnd = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(-1);
        await SeedAsync(connection, today, now, ct);

        /* The legacy pair holds the days before the boundary and the interval pair from it: the window's older days read the legacy hourly, the
           newer ones the interval daily. The interval daily stops at yesterday as before. */
        var boundary = today.AddDays(-3);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, today.AddDays(-8), boundary, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, today.AddDays(-8), boundary, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, boundary, hourEnd, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, boundary, yesterday, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        var oldCoverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups with { QueryGrainIntervalDaily = false }, ct);
        var start = today.AddDays(-6);
        var end = today.AddDays(1);
        Assert.Equal(boundary, coverage.StitchFloor(TimescaleSupport.QueryStatsHourlyView, RollupCoverage.StitchTier.Hourly, start));
        var newSql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage, start);
        var oldSql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, oldCoverage, start);

        await AssertSameAsync(connection, DailySummarySql.RangeSql, newSql, start, end, ct);
        await AssertSameAsync(connection, oldSql, newSql, start, end, ct);
        await AssertSameAsync(connection, oldSql, newSql, today.AddDays(-5).AddHours(6).AddMinutes(30).AddTicks(10), yesterday.AddHours(12).AddMinutes(30).AddTicks(6543210), ct);

        var hourlyChunks = await ChunkNamesAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, ct);
        var lacking = await ScalarAsync<long>(connection, $"SELECT count(*) FROM {await ResolveAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, ct)} WHERE server_id = {ServerId} AND bucket >= '{Ts(yesterday)}'", ct);
        var oldRead = await RowsReadAsync(connection, oldSql, start, end, hourlyChunks, ct);
        var newRead = await RowsReadAsync(connection, newSql, start, end, hourlyChunks, ct);
        Assert.True(oldRead > lacking * 2, $"the old text reads the successor's closed days from its hourly, {oldRead}, against {lacking} for the days its daily lacks");
        Assert.True(newRead <= lacking + 60, $"the new text read {newRead} interval hourly rows, more than the {lacking} of the days the interval daily lacks");
    }

    /// <summary>Opens <paramref name="connection"/>, migrates, enables TimescaleDB on a connection of the probe's own (#1922) and builds the rollups.</summary>
    private static async Task PrepareAsync(ScratchPostgres scratch, NpgsqlConnection connection, CancellationToken ct)
    {
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "The live #8549 tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
    }

    /// <summary>
    /// Nine days of hourly rows for two servers: up to forty hashes an hour on two databases, the number of hashes varying with the day so every
    /// day has its own unique-query count, and the hashes repeating across hours so a day's count is not its row count. Then rows that pin the
    /// cases: a hash whose only rows on a day the interval rollups cover are restart rows (<c>sample_interval_seconds = 0</c>), which neither the interval hourly nor
    /// the daily built from it holds, a hash seen only late in the open day, and a NULL hash.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime today, DateTime nowUtc, CancellationToken ct)
    {
        await ExecuteAsync(connection, $"""
            INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                             delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
            SELECT 854900000 + row_number() OVER (ORDER BY h.t, s.sid, n.n), h.t + (n.n * interval '1 second'), s.sid, 'month-read-server', 'db' || (n.n % 2),
                   'B' || ((extract(epoch FROM h.t)::bigint / 3600) % 5) || '_' || n.n,
                   '0x01', 10, 10, 1, 120
            FROM generate_series('{Ts(today.AddDays(-8))}'::timestamp, '{Ts(nowUtc)}'::timestamp, interval '1 hour') AS h(t)
            CROSS JOIN (VALUES ({ServerId}), (8550)) AS s(sid)
            CROSS JOIN generate_series(1, 40) AS n(n)
            WHERE n.n <= 6 + (extract(day FROM h.t)::int % 17)
            """, ct);

        var planted = new (DateTime At, string? Hash, int Interval)[]
        {
            (today.AddDays(-2).AddHours(12).AddMinutes(25), "RST_ONLY_D2", 0),
            (today.AddDays(-1).AddHours(7).AddMinutes(25), "RST_ONLY_D1", 0),
            (today.AddHours(0).AddMinutes(5), "OPEN_DAY_ONLY", 120),
            (today.AddDays(-3).AddHours(9).AddMinutes(40), null, 120),
        };
        var id = 854999000L;
        foreach (var (at, hash, interval) in planted)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                                 delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
                VALUES ($1, $2, $3, 'month-read-server', 'db0', $4, '0x01', $6, $6, $6, $5)
                """, connection);
            insert.Parameters.AddWithValue(id++);
            insert.Parameters.AddWithValue(at);
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(hash is null ? DBNull.Value : hash);
            insert.Parameters.AddWithValue(interval);
            insert.Parameters.AddWithValue(interval == 0 ? 0L : 10L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await ExecuteAsync(connection, "ANALYZE collect.query_stats", ct);
    }

    /// <summary>EXCEPT both ways between two statements over [start, end), whole rows; returns the row count. The first statement's rows stay in <c>qs_a</c>, the second's in <c>qs_b</c>.</summary>
    private static async Task<long> AssertSameAsync(
        NpgsqlConnection connection, string sqlA, string sqlB, DateTime start, DateTime end, CancellationToken ct)
    {
        const string types = "int, timestamp, timestamp, timestamp";
        var args = $"{ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}'";
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS pg_temp.qs_a; DROP TABLE IF EXISTS pg_temp.qs_b; DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE qs_a_q({types}) AS {sqlA}", ct);
        await ExecuteAsync(connection, $"PREPARE qs_b_q({types}) AS {sqlB}", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE qs_a AS EXECUTE qs_a_q({args})", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE qs_b AS EXECUTE qs_b_q({args})", ct);

        var rows = await ScalarAsync<long>(connection, "SELECT count(*) FROM qs_a", ct);
        Assert.Equal(rows, await ScalarAsync<long>(connection, "SELECT count(*) FROM qs_b", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM qs_a EXCEPT SELECT * FROM qs_b) AS x", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM qs_b EXCEPT SELECT * FROM qs_a) AS x", ct));
        return rows;
    }

    /// <summary>The rows the scans over <paramref name="relations"/> returned when <paramref name="sql"/> runs over [start, end).</summary>
    private static async Task<long> RowsReadAsync(
        NpgsqlConnection connection, string sql, DateTime start, DateTime end, ISet<string> relations, CancellationToken ct)
    {
        await ExecuteAsync(connection, "DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE qs_rows_q(int, timestamp, timestamp, timestamp) AS {sql}", ct);
        var plan = await ScalarAsync<object>(connection,
            $"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE qs_rows_q({ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}')", ct);
        return ExplainBuffers.RowsRead(plan, relations);
    }

    /// <summary>The chunk names of <paramref name="view"/>'s materialization hypertable, which the plan's scan nodes carry.</summary>
    private static async Task<HashSet<string>> ChunkNamesAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, view, ct);
        Assert.NotNull(materialization);
        var names = new HashSet<string>(StringComparer.Ordinal);
        await using var chunks = new NpgsqlCommand("SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_schema = $1 AND hypertable_name = $2", connection);
        chunks.Parameters.AddWithValue(materialization!.Value.Schema);
        chunks.Parameters.AddWithValue(materialization.Value.Name);
        await using var reader = await chunks.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private static Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
        => ExecuteAsync(connection, $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, '{Ts(from)}'::timestamp, '{Ts(to)}'::timestamp)", ct);

    private static async Task<string> ResolveAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, view, ct);
        Assert.NotNull(materialization);
        return $"\"{materialization!.Value.Schema}\".\"{materialization.Value.Name}\"";
    }

    private static string Ts(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
