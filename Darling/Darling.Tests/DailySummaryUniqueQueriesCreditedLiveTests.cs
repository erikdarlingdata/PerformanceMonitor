/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling.
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
/// #8651: the Daily Summary's <c>unique_queries</c> counts queries with credited executions in the window, the rule the interval
/// rollups were built on. A restart row (<c>sample_interval_seconds = 0</c>: a first sighting, an uncredited counter reset or a gap, stored
/// beside zero deltas) credits nothing, so a hash whose only rows in the window are restart rows is left out. A hash with any credited row
/// counts once, and a row from before the column (NULL interval) counts. The raw statement, the Hourly tier's and the Daily tier's statements
/// on an interval-honest store all give the same count.
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): the test mints its own scratch database through <see cref="ScratchPostgres"/> and
/// drops it whole.</para>
/// </summary>
public sealed class DailySummaryUniqueQueriesCreditedLiveTests
{
    private const int ServerId = 8651;
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8651 tests.";

    [Fact]
    public async Task RawStatement_LeavesOutAHashWhoseOnlyRowsAreRestartRows_AndCountsAHashWithAnyCreditedRowOnce()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var today = DateTime.SpecifyKind(LiveClock.Now().Date, DateTimeKind.Unspecified);
        var day = today.AddDays(-1);
        await PlantAsync(connection, 865100000L, new (DateTime At, string? Hash, int? Interval)[]
        {
            (day.AddHours(1), "CREDITED", 120),
            (day.AddHours(2), "CREDITED", 120),
            (day.AddHours(3), "RESTART_THEN_CREDITED", 0),
            (day.AddHours(4), "RESTART_THEN_CREDITED", 120),
            (day.AddHours(5), "PRE_COLUMN", null),
            (day.AddHours(6), "RESTART_ONLY", 0),
            (day.AddHours(7), "RESTART_ONLY", 0),
            (day.AddHours(8), "RESTART_ONLY_TOO", 0),
            (day.AddHours(9), null, 0),
        }, ct);

        /* Restart rows credit nothing: every delta they carry is 0. */
        Assert.Equal(0L, await ScalarAsync<long>(connection,
            $"SELECT COALESCE(sum(delta_execution_count + delta_worker_time + delta_elapsed_time), 0)::bigint FROM collect.query_stats WHERE server_id = {ServerId} AND sample_interval_seconds = 0", ct));

        var counted = await UniqueQueriesAsync(connection, DailySummarySql.RangeSql, day, today, ct);
        Assert.Equal(3L, counted);
        Assert.Equal(await CreditedHashesAsync(connection, day, today, ct), counted);
    }

    [Fact]
    public async Task EveryTier_GivesTheSameCount_WhenARestartOnlyHashSitsOnADayTheIntervalDailyHoldsAndOnARawDay()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "The live #8651 tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var now = LiveClock.Now();
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        var yesterday = today.AddDays(-1);
        var heldDay = today.AddDays(-3);
        var hourEnd = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(-1);

        await ExecuteAsync(connection, $"""
            INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                             delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
            SELECT 865200000 + row_number() OVER (ORDER BY g.t), g.t, {ServerId}, 'credited-server', 'db',
                   'Q' || ((extract(epoch FROM g.t)::bigint / 3600) % 31) || '_' || ((extract(epoch FROM g.t)::bigint / 60) % 3),
                   '0x01', 10, 10, 1, 120
            FROM generate_series('{Ts(today.AddDays(-6))}'::timestamp, '{Ts(now)}'::timestamp, interval '2 minutes') AS g(t)
            """, ct);

        /* A restart-only hash on a day the interval daily holds, one on yesterday (a day the daily lacks, so raw and the hourly read it) and one on
           today. A hash with a restart row and a credited row counts once. */
        await PlantAsync(connection, 865290000L, new (DateTime At, string? Hash, int? Interval)[]
        {
            (heldDay.AddHours(5).AddMinutes(25), "RST_HELD_DAY", 0),
            (heldDay.AddHours(6).AddMinutes(25), "RST_AND_CREDITED", 0),
            (heldDay.AddHours(7).AddMinutes(25), "RST_AND_CREDITED", 120),
            (yesterday.AddHours(5).AddMinutes(25), "RST_YESTERDAY", 0),
            (yesterday.AddHours(6).AddMinutes(25), "RST_AND_CREDITED", 0),
            (today.AddMinutes(25), "RST_TODAY", 0),
        }, ct);
        await ExecuteAsync(connection, "ANALYZE collect.query_stats", ct);

        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, today.AddDays(-6), hourEnd, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, today.AddDays(-6), yesterday, ct);
        Assert.True(await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM collect.{TimescaleSupport.QueryStatsIntervalDailyView} WHERE server_id = {ServerId} AND bucket = '{Ts(heldDay)}'", ct) > 0, "the interval daily holds the day");
        Assert.Equal(0L, await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM collect.{TimescaleSupport.QueryStatsIntervalDailyView} WHERE server_id = {ServerId} AND bucket >= '{Ts(yesterday)}'", ct));

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, await TimescaleSupport.DetectRollupsAsync(dataSource, ct), ct);
        var windowStart = today.AddDays(-5);
        var dailySql = DailySummarySql.RangeSqlFor(RetentionTier.Daily, coverage, windowStart);
        var hourlySql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, coverage, windowStart);
        Assert.Contains(TimescaleSupport.QueryStatsIntervalDailyView, dailySql, StringComparison.Ordinal);
        Assert.Contains(TimescaleSupport.QueryStatsIntervalHourlyView, hourlySql, StringComparison.Ordinal);

        var planted = 0;
        for (var day = windowStart; day <= today; day = day.AddDays(1))
        {
            var credited = await CreditedHashesAsync(connection, day, day.AddDays(1), ct);
            var all = await ScalarAsync<long>(connection,
                $"SELECT count(DISTINCT query_hash) FROM collect.query_stats WHERE server_id = {ServerId} AND collection_time >= '{Ts(day)}' AND collection_time < '{Ts(day.AddDays(1))}'", ct);
            planted += all > credited ? 1 : 0;

            Assert.Equal(credited, await UniqueQueriesAsync(connection, DailySummarySql.RangeSql, day, day.AddDays(1), ct));
            Assert.Equal(credited, await UniqueQueriesAsync(connection, hourlySql, day, day.AddDays(1), ct));
            Assert.Equal(credited, await UniqueQueriesAsync(connection, dailySql, day, day.AddDays(1), ct));
        }

        Assert.Equal(3, planted);
    }

    private static async Task PlantAsync(NpgsqlConnection connection, long firstId, IEnumerable<(DateTime At, string? Hash, int? Interval)> rows, CancellationToken ct)
    {
        var id = firstId;
        foreach (var (at, hash, interval) in rows)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                                 delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
                VALUES ($1, $2, $3, 'credited-server', 'db', $4, '0x01', $6, $6, $6, $5)
                """, connection);
            insert.Parameters.AddWithValue(id++);
            insert.Parameters.AddWithValue(at);
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(hash is null ? DBNull.Value : hash);
            insert.Parameters.Add(new NpgsqlParameter { Value = interval is null ? DBNull.Value : interval.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
            insert.Parameters.AddWithValue(interval == 0 ? 0L : 10L);
            await insert.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>The sum of <c>unique_queries</c> the statement returns over [start, end).</summary>
    private static async Task<long> UniqueQueriesAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS pg_temp.qs_sum; DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE qs_sum_q(int, timestamp, timestamp, timestamp) AS {sql}", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE qs_sum AS EXECUTE qs_sum_q({ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}')", ct);
        return await ScalarAsync<long>(connection, "SELECT COALESCE(sum(unique_queries), 0)::bigint FROM qs_sum", ct);
    }

    /// <summary>The distinct hashes with a credited row (<c>sample_interval_seconds</c> not 0) in [start, end).</summary>
    private static Task<long> CreditedHashesAsync(NpgsqlConnection connection, DateTime start, DateTime end, CancellationToken ct)
        => ScalarAsync<long>(connection,
            $"SELECT count(DISTINCT query_hash) FROM collect.query_stats WHERE server_id = {ServerId} AND collection_time >= '{Ts(start)}' AND collection_time < '{Ts(end)}' "
            + "AND sample_interval_seconds IS DISTINCT FROM 0", ct);

    private static Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
        => ExecuteAsync(connection, $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, '{Ts(from)}'::timestamp, '{Ts(to)}'::timestamp)", ct);

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
