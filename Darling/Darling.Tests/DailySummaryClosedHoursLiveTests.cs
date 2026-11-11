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
/// #8549: the daily rollup's refresh leaves yesterday out until the next UTC midnight, so the Daily Summary's <c>queries</c> part read that
/// closed day from raw <c>query_stats</c>. It now reads the hourly rollup for the hours the hourly holds and raw only for the hours neither
/// rollup holds, plus the open day's last hours. The result must stay the raw statement's: EXCEPT both ways over a window whose
/// yesterday is missing from the daily rollup and partly missing from the hourly one, on the legacy daily, on the stitched daily, on
/// windows that start and end inside an hour (microseconds kept) and on a daily that holds nothing, and the hourly tier's statement is
/// unchanged. The raw rows the plan reads (<see cref="ExplainBuffers.RowsRead"/>) stay at the hours no rollup holds. A hash seen only in a
/// restart row (<c>sample_interval_seconds = 0</c>, which the interval-honest hourly skips) is still counted, as the raw tail counted it, and
/// the stitched daily's legacy-hourly branch, for hours before the boundary day, runs in a case of its own.
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): the test mints its own scratch database through <see cref="ScratchPostgres"/> and
/// drops it whole.</para>
/// </summary>
public sealed class DailySummaryClosedHoursLiveTests
{
    private const int ServerId = 8549;
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8549 tests.";

    private static readonly RollupCoverage NoCoverage = new(
        new Dictionary<string, DateTime>(StringComparer.Ordinal), new Dictionary<string, DateTime>(StringComparer.Ordinal), RollupAvailability.None);

    [Fact]
    public async Task TheQueriesPart_EqualsRaw_ForAClosedDayTheDailyLacks_AndReadsRawRowsOnlyForTheHoursNoRollupHolds()
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

        var start = today.AddDays(-5);
        var end = today.AddDays(1);

        /* Nothing is rolled up: the daily has no ceiling, the closed-hours member reads nothing and the raw tail reads the window as it did. */
        var coldRows = await AssertSameRowsAsync(connection, RetentionTier.Daily, NoCoverage, start, end, ct);
        Assert.True(coldRows >= 6, $"the window has a row per day, got {coldRows}");

        /* The hourly rollup holds every closed hour, the daily only the days before yesterday (its refresh stops at yesterday 01:00). */
        await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, today.AddDays(-6), hourEnd, ct);
        await AssertSameRowsAsync(connection, RetentionTier.Hourly, NoCoverage, start, end, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, today.AddDays(-6), yesterday, ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.QueryStatsDailyView} WHERE bucket >= '{Ts(yesterday)}'", ct));
        Assert.True(await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.QueryStatsDailyView} WHERE server_id = {ServerId} AND bucket = '{Ts(today.AddDays(-2))}'", ct) > 0);

        /* Every hour of yesterday held: the closed day costs no raw row. */
        await AssertSameRowsAsync(connection, RetentionTier.Daily, NoCoverage, start, end, ct);

        /* The hourly rollup partly missing for yesterday: one hour in the middle and its last three hours. */
        var hourly = await ResolveAsync(connection, TimescaleSupport.QueryStatsHourlyView, ct);
        await ExecuteAsync(connection, $"DELETE FROM {hourly} WHERE bucket = '{Ts(yesterday.AddHours(9))}' OR (bucket >= '{Ts(yesterday.AddHours(21))}' AND bucket < '{Ts(today)}')", ct);

        var rows = await AssertSameRowsAsync(connection, RetentionTier.Daily, NoCoverage, start, end, ct);
        Assert.True(rows >= 6);

        /* Windows that start and end inside an hour, a held hour cut by the window included, to the microsecond. */
        await AssertSameRowsAsync(connection, RetentionTier.Daily, NoCoverage, yesterday.AddHours(6).AddMinutes(30).AddTicks(10), yesterday.AddHours(12).AddMinutes(30).AddTicks(6543210), ct);
        await AssertSameRowsAsync(connection, RetentionTier.Daily, NoCoverage, yesterday.AddHours(6).AddMinutes(30).AddTicks(10), today.AddHours(2).AddMinutes(17).AddTicks(1234560), ct);

        /* Raw rows the plan reads. The hours no rollup holds, counted off the tables themselves, are all it may read. The raw statement reads the window. */
        var rawChunks = new HashSet<string>(StringComparer.Ordinal);
        await using (var chunks = new NpgsqlCommand("SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_stats'", connection))
        await using (var reader = await chunks.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rawChunks.Add(reader.GetString(0));
            }
        }

        Assert.NotEmpty(rawChunks);
        var allowed = await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM collect.query_stats AS q WHERE q.server_id = {ServerId} AND q.collection_time >= '{Ts(yesterday)}' AND q.collection_time < '{Ts(end)}' "
            + $"AND NOT EXISTS (SELECT 1 FROM collect.{TimescaleSupport.QueryStatsHourlyView} AS h WHERE h.server_id = {ServerId} AND h.bucket = date_trunc('hour', q.collection_time))", ct);
        var yesterdayRows = await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM collect.query_stats WHERE server_id = {ServerId} AND collection_time >= '{Ts(yesterday)}' AND collection_time < '{Ts(today)}'", ct);
        var rawRows = await RowsReadAsync(connection, DailySummarySql.RangeSql, start, end, rawChunks, ct);
        var routedRows = await RowsReadAsync(connection, DailySummarySql.RangeSqlFor(RetentionTier.Daily, NoCoverage, start), start, end, rawChunks, ct);
        Assert.True(allowed > 0 && allowed < yesterdayRows, $"the seed leaves hours no rollup holds ({allowed}) fewer than the rows of yesterday ({yesterdayRows})");
        Assert.True(rawRows > yesterdayRows, $"the raw statement reads the window, {rawRows} rows");
        Assert.True(routedRows <= allowed, $"the routed statement read {routedRows} raw query_stats rows, more than the {allowed} of the hours no rollup holds");

        /* Hourly tier: its relations read raw as their source, so nothing sits between them and raw and the closed-hours member is not in the text. */
        Assert.DoesNotContain("AS held", DailySummarySql.RangeSqlFor(RetentionTier.Hourly, NoCoverage, start), StringComparison.Ordinal);
        Assert.Contains("AS held", DailySummarySql.RangeSqlFor(RetentionTier.Daily, NoCoverage, start), StringComparison.Ordinal);

        /* The stitched daily: the interval-honest successor holds the last three days, so the boundary sits inside the window and yesterday is
           again missing from its daily and partly from its hourly. */
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, today.AddDays(-3), hourEnd, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, today.AddDays(-3), yesterday, ct);
        var successor = await ResolveAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, ct);
        await ExecuteAsync(connection, $"DELETE FROM {successor} WHERE bucket = '{Ts(yesterday.AddHours(9))}' OR (bucket >= '{Ts(yesterday.AddHours(21))}' AND bucket < '{Ts(today)}')", ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(rollups.QueryGrainIntervalDaily);
        var stitched = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        Assert.Equal(today.AddDays(-3), stitched.StitchFloor(TimescaleSupport.QueryStatsDailyView, RollupCoverage.StitchTier.Daily, start));
        Assert.Contains("queries_ceiling_successor", DailySummarySql.RangeSqlFor(RetentionTier.Daily, stitched, start), StringComparison.Ordinal);

        await AssertSameRowsAsync(connection, RetentionTier.Daily, stitched, start, end, ct);
        await AssertSameRowsAsync(connection, RetentionTier.Daily, stitched, yesterday.AddHours(6).AddMinutes(30).AddTicks(10), yesterday.AddHours(12).AddMinutes(30).AddTicks(6543210), ct);
        var stitchedRows = await RowsReadAsync(connection, DailySummarySql.RangeSqlFor(RetentionTier.Daily, stitched, start), start, end, rawChunks, ct);
        Assert.True(stitchedRows <= allowed, $"the stitched statement read {stitchedRows} raw query_stats rows, more than the {allowed} of the hours no rollup holds");
    }

    [Fact]
    public async Task TheQueriesPart_CountsAHashSeenOnlyInARestartRow_AsTheRawTailDid_OnEveryShapeOfDaily()
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

        /* Restart rows: no counter was knowable, so every delta is 0 and the interval is 0. The two window edges are yesterday 14:30 and 19:30
           (a microsecond is ten ticks). Hours 9 and 21 to 23 of yesterday are missing from the hourly below, so RST_RAW sits in a raw hour and
           the others in held hours. */
        var windowStart = yesterday.AddHours(14).AddMinutes(30);
        var windowEnd = yesterday.AddHours(19).AddMinutes(30);
        await PlantAsync(connection, 854998000L, new (DateTime At, string? Hash, int Interval)[]
        {
            (yesterday.AddHours(11).AddMinutes(25), "RST_HELD", 0),
            (yesterday.AddHours(22).AddMinutes(25), "RST_RAW", 0),
            (yesterday.AddHours(15).AddMinutes(25), "RST_AND_NORMAL", 0),
            (yesterday.AddHours(16).AddMinutes(25), "RST_AND_NORMAL", 120),
            (yesterday.AddHours(17).AddMinutes(25), null, 0),
            (yesterday.AddHours(13).AddMinutes(20), "RST_BEFORE", 0),
            (yesterday.AddHours(20).AddMinutes(20), "RST_AFTER", 0),
            (windowStart.AddTicks(-10), "RST_EDGE_OUT", 0),
            (windowStart, "RST_EDGE_IN", 0),
            (windowEnd.AddTicks(-10), "RST_END_IN", 0),
            (windowEnd, "RST_END_OUT", 0),
        }, ct);

        /* Legacy shape. Its hourly has no filter and keeps restart rows, so this is the control. */
        await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, today.AddDays(-6), hourEnd, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, today.AddDays(-6), yesterday, ct);
        var legacyHourly = await ResolveAsync(connection, TimescaleSupport.QueryStatsHourlyView, ct);
        await ExecuteAsync(connection, $"DELETE FROM {legacyHourly} WHERE bucket = '{Ts(yesterday.AddHours(9))}' OR (bucket >= '{Ts(yesterday.AddHours(21))}' AND bucket < '{Ts(today)}')", ct);
        var start = today.AddDays(-5);
        var end = today.AddDays(1);
        await AssertSameRowsAsync(connection, RetentionTier.Daily, NoCoverage, start, end, ct);
        await AssertSameRowsAsync(connection, RetentionTier.Daily, NoCoverage, windowStart, windowEnd, ct);

        /* Stitched shape: the interval-honest successor holds the last three days. A hash seen only in a restart row of a held hour (RST_HELD,
           RST_AND_NORMAL) or of a raw hour (RST_RAW) of yesterday is counted, and the rows outside a window are not. */
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, today.AddDays(-3), hourEnd, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, today.AddDays(-3), yesterday, ct);
        var intervalHourly = await ResolveAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, ct);
        await ExecuteAsync(connection, $"DELETE FROM {intervalHourly} WHERE bucket = '{Ts(yesterday.AddHours(9))}' OR (bucket >= '{Ts(yesterday.AddHours(21))}' AND bucket < '{Ts(today)}')", ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {intervalHourly} WHERE query_hash LIKE 'RST%' AND query_hash <> 'RST_AND_NORMAL'", ct));

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var stitched = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, await TimescaleSupport.DetectRollupsAsync(dataSource, ct), ct);
        Assert.Contains("queries_ceiling_successor", DailySummarySql.RangeSqlFor(RetentionTier.Daily, stitched, start), StringComparison.Ordinal);
        await AssertSameRowsAsync(connection, RetentionTier.Daily, stitched, start, end, ct);
        await AssertSameRowsAsync(connection, RetentionTier.Daily, stitched, windowStart, windowEnd, ct);

        /* The interval-honest daily alone: the successor reaches the window start, so the legacy daily drops out of the statement. */
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, today.AddDays(-6), today.AddDays(-3), ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, today.AddDays(-6), today.AddDays(-3), ct);
        var alone = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, await TimescaleSupport.DetectRollupsAsync(dataSource, ct), ct);
        var aloneSql = DailySummarySql.RangeSqlFor(RetentionTier.Daily, alone, start);
        Assert.DoesNotContain("queries_ceiling_successor", aloneSql, StringComparison.Ordinal);
        Assert.Contains(TimescaleSupport.QueryStatsIntervalDailyView, aloneSql, StringComparison.Ordinal);
        await AssertSameRowsAsync(connection, RetentionTier.Daily, alone, start, end, ct);
        await AssertSameRowsAsync(connection, RetentionTier.Daily, alone, windowStart, windowEnd, ct);

        /* The disagreement that was already there: a day the interval daily HOLDS leaves a restart-only hash out and raw counts it. The routed
           statement keeps the daily's count for that day, one fewer than the raw statement's. */
        var heldDay = today.AddDays(-2);
        await PlantAsync(connection, 854997000L, new (DateTime At, string? Hash, int Interval)[] { (heldDay.AddHours(12).AddMinutes(25), "RST_IN_A_HELD_DAY", 0) }, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, today.AddDays(-6), yesterday, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, today.AddDays(-6), yesterday, ct);
        var rawCount = await UniqueQueriesAsync(connection, DailySummarySql.RangeSql, heldDay, yesterday, ct);
        var routedCount = await UniqueQueriesAsync(connection, DailySummarySql.RangeSqlFor(RetentionTier.Daily, alone, heldDay), heldDay, yesterday, ct);
        Assert.Equal(rawCount - 1, routedCount);
    }

    /// <summary>
    /// The stitched daily's legacy-hourly branch. Hours before the boundary day read the legacy hourly and hours from it the interval one, and
    /// that holds only while a server's successor daily has nothing at or after the boundary, so the statement's raw_from is the legacy
    /// daily's and lies before it. The test gives the other server successor rows (which sets the fleet-wide boundary) and takes this
    /// server's out, then stops its legacy daily two days before the boundary. The raw rows the plan reads must then stay at the hours no
    /// rollup holds: the legacy hourly holds the day before the boundary, so a probe or a read that asked the interval hourly for those hours
    /// would fall through to raw and fail the row ceiling.
    /// </summary>
    [Fact]
    public async Task TheStitchedQueriesPart_ReadsTheHoursBeforeTheBoundaryDay_FromTheLegacyHourly()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await PrepareAsync(scratch, connection, ct);

        var now = LiveClock.Now();
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        var hourEnd = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(-1);
        await SeedAsync(connection, today, now, ct);

        var boundary = today.AddDays(-3);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, today.AddDays(-6), hourEnd, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsDailyView, today.AddDays(-6), today.AddDays(-1), ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, boundary, hourEnd, ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, boundary, today.AddDays(-1), ct);
        var intervalDaily = await ResolveAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, ct);
        var legacyDaily = await ResolveAsync(connection, TimescaleSupport.QueryStatsDailyView, ct);
        await ExecuteAsync(connection, $"DELETE FROM {intervalDaily} WHERE server_id = {ServerId}", ct);
        await ExecuteAsync(connection, $"DELETE FROM {legacyDaily} WHERE server_id = {ServerId} AND bucket >= '{Ts(today.AddDays(-4))}'", ct);
        Assert.True(await ScalarAsync<long>(connection, $"SELECT count(*) FROM {intervalDaily} WHERE server_id <> {ServerId}", ct) > 0);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, await TimescaleSupport.DetectRollupsAsync(dataSource, ct), ct);
        var start = today.AddDays(-5);
        var end = today.AddDays(1);
        Assert.Equal(boundary, coverage.StitchFloor(TimescaleSupport.QueryStatsDailyView, RollupCoverage.StitchTier.Daily, start));
        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Daily, coverage, start);
        Assert.Contains("queries_ceiling_successor", sql, StringComparison.Ordinal);

        await AssertSameRowsAsync(connection, RetentionTier.Daily, coverage, start, end, ct);
        await AssertSameRowsAsync(connection, RetentionTier.Daily, coverage, today.AddDays(-4).AddHours(6).AddMinutes(30).AddTicks(10), today.AddDays(-2).AddHours(12).AddMinutes(30).AddTicks(6543210), ct);

        var rawChunks = new HashSet<string>(StringComparer.Ordinal);
        await using (var chunks = new NpgsqlCommand("SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_stats'", connection))
        await using (var reader = await chunks.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rawChunks.Add(reader.GetString(0));
            }
        }

        Assert.NotEmpty(rawChunks);
        var windowRows = await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM collect.query_stats WHERE server_id = {ServerId} AND collection_time >= '{Ts(today.AddDays(-4))}' AND collection_time < '{Ts(end)}'", ct);
        var allowed = await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM collect.query_stats AS q WHERE q.server_id = {ServerId} AND q.collection_time >= '{Ts(today.AddDays(-4))}' AND q.collection_time < '{Ts(end)}' "
            + $"AND NOT EXISTS (SELECT 1 FROM collect.{TimescaleSupport.QueryStatsHourlyView} AS h WHERE h.server_id = {ServerId} AND h.bucket = date_trunc('hour', q.collection_time) AND h.bucket < '{Ts(boundary)}') "
            + $"AND NOT EXISTS (SELECT 1 FROM collect.{TimescaleSupport.QueryStatsIntervalHourlyView} AS h WHERE h.server_id = {ServerId} AND h.bucket = date_trunc('hour', q.collection_time) AND h.bucket >= '{Ts(boundary)}')", ct);
        Assert.True(allowed > 0 && allowed * 10 < windowRows, $"the seed leaves {allowed} rows in hours no rollup holds, of {windowRows} from the legacy daily's ceiling");
        var routedRows = await RowsReadAsync(connection, sql, start, end, rawChunks, ct);
        Assert.True(routedRows <= allowed, $"the stitched statement read {routedRows} raw query_stats rows, more than the {allowed} of the hours no rollup holds");
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

    private static async Task PlantAsync(NpgsqlConnection connection, long firstId, IEnumerable<(DateTime At, string? Hash, int Interval)> rows, CancellationToken ct)
    {
        var id = firstId;
        foreach (var (at, hash, interval) in rows)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                                 delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
                VALUES ($1, $2, $3, 'closed-hours-server', 'db', $4, '0x01', $6, $6, $6, $5)
                """, connection);
            insert.Parameters.AddWithValue(id++);
            insert.Parameters.AddWithValue(at);
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(hash is null ? DBNull.Value : hash);
            insert.Parameters.AddWithValue(interval);
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

    /// <summary>EXCEPT both ways between the raw statement and the routed statement over [start, end), whole rows; returns the row count.</summary>
    private static async Task<long> AssertSameRowsAsync(
        NpgsqlConnection connection, RetentionTier tier, RollupCoverage coverage, DateTime start, DateTime end, CancellationToken ct)
    {
        const string types = "int, timestamp, timestamp, timestamp";
        var args = $"{ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}'";
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS pg_temp.qs_raw; DROP TABLE IF EXISTS pg_temp.qs_roll; DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE qs_raw_q({types}) AS {DailySummarySql.RangeSql}", ct);
        await ExecuteAsync(connection, $"PREPARE qs_roll_q({types}) AS {DailySummarySql.RangeSqlFor(tier, coverage, start)}", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE qs_raw AS EXECUTE qs_raw_q({args})", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE qs_roll AS EXECUTE qs_roll_q({args})", ct);

        var rows = await ScalarAsync<long>(connection, "SELECT count(*) FROM qs_raw", ct);
        Assert.Equal(rows, await ScalarAsync<long>(connection, "SELECT count(*) FROM qs_roll", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM qs_raw EXCEPT SELECT * FROM qs_roll) AS x", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM qs_roll EXCEPT SELECT * FROM qs_raw) AS x", ct));
        return rows;
    }

    private static async Task<long> RowsReadAsync(
        NpgsqlConnection connection, string sql, DateTime start, DateTime end, ISet<string> rawChunks, CancellationToken ct)
    {
        await ExecuteAsync(connection, "DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE qs_rows_q(int, timestamp, timestamp, timestamp) AS {sql}", ct);
        var plan = await ScalarAsync<object>(connection,
            $"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE qs_rows_q({ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}')", ct);
        return ExplainBuffers.RowsRead(plan, rawChunks);
    }

    /// <summary>
    /// Seven days of queries every two minutes for two servers, three hashes an hour that change from hour to hour, then rows that pin the cases:
    /// a hash seen in a held hour and in a raw hour of yesterday (counted once), one seen only in a hour no rollup will hold, one only in a held
    /// hour, one NULL hash on each side, and rows a microsecond either side of the window edges the test uses. Every row carries a measured
    /// interval, so the interval-honest hourly holds exactly what the legacy one does.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime today, DateTime nowUtc, CancellationToken ct)
    {
        await ExecuteAsync(connection, $"""
            INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                             delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
            SELECT 854900000 + row_number() OVER (ORDER BY g.t, s.sid), g.t, s.sid, 'closed-hours-server', 'db',
                   'Q' || ((extract(epoch FROM g.t)::bigint / 3600) % 31) || '_' || ((extract(epoch FROM g.t)::bigint / 60) % 3),
                   '0x01', 10, 10, 1, 120
            FROM generate_series('{Ts(today.AddDays(-6))}'::timestamp, '{Ts(nowUtc)}'::timestamp, interval '2 minutes') AS g(t)
            CROSS JOIN (VALUES ({ServerId}), (8550)) AS s(sid)
            """, ct);

        var yesterday = today.AddDays(-1);
        var planted = new (DateTime At, string? Hash)[]
        {
            (yesterday.AddHours(8).AddMinutes(30), "BOTH"),
            (yesterday.AddHours(9).AddMinutes(30), "BOTH"),
            (yesterday.AddHours(9).AddMinutes(20), "RAW_ONLY"),
            (yesterday.AddHours(10).AddMinutes(20), "HELD_ONLY"),
            (yesterday.AddHours(22).AddMinutes(15), "LATE_ONLY"),
            (yesterday.AddHours(9).AddMinutes(40), null),
            (yesterday.AddHours(10).AddMinutes(40), null),
            (yesterday.AddHours(12).AddMinutes(10), "IN_PART"),
            (yesterday.AddHours(12).AddMinutes(30).AddTicks(6543200), "EDGE_IN"),
            (yesterday.AddHours(12).AddMinutes(30).AddTicks(6543210), "EDGE_OUT"),
            (yesterday.AddHours(12).AddMinutes(45), "OUT_PART"),
            (yesterday.AddHours(6).AddMinutes(30), "START_OUT"),
            (yesterday.AddHours(6).AddMinutes(30).AddTicks(10), "START_IN"),
        };

        var id = 854999000L;
        foreach (var (at, hash) in planted)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                                 delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
                VALUES ($1, $2, $3, 'closed-hours-server', 'db', $4, '0x01', 10, 10, 1, 120)
                """, connection);
            insert.Parameters.AddWithValue(id++);
            insert.Parameters.AddWithValue(at);
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(hash is null ? DBNull.Value : hash);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await ExecuteAsync(connection, "ANALYZE collect.query_stats", ct);
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
