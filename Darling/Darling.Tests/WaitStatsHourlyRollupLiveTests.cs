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
/// #8824: the Daily Summary's wait part reads <c>collect.wait_stats_hourly</c> for the hours of a day the daily rollup does not hold, and
/// raw <c>wait_stats</c> only for the remaining hours. The rollups are the same sum the raw statement runs, so the two statements must
/// return the same rows: EXCEPT both ways over a window with a partial hour at both edges, an hour the hourly refresh has not reached and
/// a hole in the middle. A store without the hourly rollup keeps the shape it had. The refresh policy and the retention policy exist.
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): each test mints its own scratch database through <see cref="ScratchPostgres"/> and
/// drops it whole.</para>
/// </summary>
public sealed class WaitStatsHourlyRollupLiveTests
{
    private const int ServerId = 8824;
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8824 tests.";

    private static readonly RollupAvailability DailyOnly = new(false, false, false, false, false, false, false, false, WaitStatsDaily: true);
    private static readonly RollupAvailability DailyAndHourly = new(false, false, false, false, false, false, false, false, WaitStatsDaily: true, WaitStatsHourly: true);

    [Fact]
    public async Task TheHourlyRead_EqualsTheRawStatement_AcrossPartialEdgeHours_AnUnreachedHour_AndAHole_AndReadsRawOnlyForThose()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = await BuildTimescaleStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);

        var today = LiveClock.Now().Date;
        var firstDay = today.AddDays(-3);
        var yesterday = today.AddDays(-1);
        var unreached = yesterday.AddHours(20);

        /* Nothing refreshed yet (WITH NO DATA): every day reads raw, so the hourly shape must still answer the raw statement's rows. */
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.WaitStatsHourlyView}", ct));
        await AssertSameRowsAsync(connection, firstDay, today.AddDays(1), ct);

        /* The hourly refresh reaches yesterday 20:00 and no further: the hours from there on, and today, are the ones it has not reached. */
        await ExecuteAsync(connection, $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.WaitStatsHourlyView}', '{Ts(firstDay)}', '{Ts(unreached)}')", ct);
        Assert.True(await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.WaitStatsHourlyView} WHERE bucket >= '{Ts(firstDay)}' AND bucket < '{Ts(unreached)}'", ct) > 100, "the refresh filled the hours");
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.WaitStatsHourlyView} WHERE bucket >= '{Ts(unreached)}'", ct));

        /* A hole in the middle of the hourly rollup: that hour reads raw. */
        var hole = yesterday.AddHours(5);
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, TimescaleSupport.WaitStatsHourlyView, ct);
        Assert.NotNull(materialization);
        await ExecuteAsync(connection, $"DELETE FROM \"{materialization!.Value.Schema}\".\"{materialization.Value.Name}\" WHERE bucket = '{Ts(hole)}'", ct);

        /* Whole days, then a window with a partial hour at both edges. */
        await AssertSameRowsAsync(connection, firstDay, today.AddDays(1), ct);
        var start = firstDay.AddHours(2).AddMinutes(30);
        var end = yesterday.AddHours(22).AddMinutes(10);
        var rows = await AssertSameRowsAsync(connection, start, end, ct);
        Assert.True(rows >= 3, $"the window has a row per day, got {rows}");

        /* The statement may read raw rows only for the partial first hour, the hole, and the hours from the unreached one to the window's end. */
        var waitChunks = new HashSet<string>(StringComparer.Ordinal);
        await using (var chunks = new NpgsqlCommand("SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'wait_stats'", connection))
        await using (var reader = await chunks.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                waitChunks.Add(reader.GetString(0));
            }
        }

        var allowed = await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM collect.wait_stats WHERE server_id = {ServerId} AND ((collection_time >= '{Ts(firstDay.AddHours(2))}' AND collection_time < '{Ts(firstDay.AddHours(3))}') " +
            $"OR (collection_time >= '{Ts(hole)}' AND collection_time < '{Ts(hole.AddHours(1))}') OR (collection_time >= '{Ts(unreached)}' AND collection_time < '{Ts(end)}'))", ct);
        var rawRows = await RawWaitRowsAsync(connection, RawSql(start), start, end, waitChunks, ct);
        var hourlyRows = await RawWaitRowsAsync(connection, HourlySql(start), start, end, waitChunks, ct);
        Assert.True(rawRows.AtMost > allowed * 5, $"the raw statement reads the whole window ({rawRows.AtMost} rows at most), far more than the edge, hole and unreached hours hold ({allowed})");
        Assert.True(hourlyRows.AtLeast <= allowed, $"the hourly statement read {hourlyRows.AtLeast} raw wait_stats rows at least, more than the edge, hole and unreached hours hold ({allowed})");
    }

    [Fact]
    public async Task AStoreWithoutTheHourlyRollup_KeepsTheDailyShape_AndEqualsTheRawStatement()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = await BuildTimescaleStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);

        var today = LiveClock.Now().Date;
        var firstDay = today.AddDays(-3);
        await ExecuteAsync(connection, $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.WaitStatsHourlyView}', '{Ts(firstDay)}', '{Ts(today.AddDays(-1).AddHours(20))}')", ct);

        var daily = DailySummarySql.RangeSqlFor(RetentionTier.Daily, Coverage(DailyOnly), firstDay);
        Assert.DoesNotContain("wait_stats_hourly", daily, StringComparison.Ordinal);
        Assert.Contains("wait_stats_hourly", HourlySql(firstDay), StringComparison.Ordinal);

        /* The relation is in the store but the coverage says it is not: the read must not name it, and must still answer the raw rows. */
        const string types = "int, timestamp, timestamp, timestamp";
        var start = firstDay.AddHours(1);
        var end = today.AddDays(1);
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS pg_temp.ws_raw; DROP TABLE IF EXISTS pg_temp.ws_roll; DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE ws_raw_q({types}) AS {RawSql(start)}", ct);
        await ExecuteAsync(connection, $"PREPARE ws_roll_q({types}) AS {DailySummarySql.RangeSqlFor(RetentionTier.Daily, Coverage(DailyOnly), start)}", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE ws_raw AS EXECUTE ws_raw_q({Args(start, end)})", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE ws_roll AS EXECUTE ws_roll_q({Args(start, end)})", ct);
        Assert.True(await ScalarAsync<long>(connection, "SELECT count(*) FROM ws_raw", ct) > 0);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM ws_raw EXCEPT SELECT * FROM ws_roll) AS x", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM ws_roll EXCEPT SELECT * FROM ws_raw) AS x", ct));
    }

    [Fact]
    public async Task TheRefreshPolicyAndTheRetentionPolicy_AreCreated_AndTheProbeSeesTheRelation()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = await BuildTimescaleStoreAsync(scratch, ct);
        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, TimescaleSupport.RetentionSweepPass.Startup, ct);

        Assert.True(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationIsContinuousAggregateSql(TimescaleSupport.WaitStatsHourlyView), ct), "a TimescaleDB store holds the continuous aggregate");

        await using var refresh = new NpgsqlCommand(
            "SELECT schedule_interval::text, config->>'start_offset', config->>'end_offset' FROM timescaledb_information.jobs " +
            $"WHERE hypertable_schema = 'collect' AND hypertable_name = '{TimescaleSupport.WaitStatsHourlyView}' AND proc_name = 'policy_refresh_continuous_aggregate'", connection);
        await using (var reader = await refresh.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct), "the refresh policy exists");
            Assert.Equal("01:00:00", reader.GetString(0));
            Assert.Equal("4 days", reader.GetString(1));
            Assert.Equal("01:00:00", reader.GetString(2));
        }

        await using var retention = new NpgsqlCommand(
            "SELECT config->>'drop_after' FROM timescaledb_information.jobs " +
            $"WHERE hypertable_schema = 'collect' AND hypertable_name = '{TimescaleSupport.WaitStatsHourlyView}' AND proc_name = 'policy_retention'", connection);
        Assert.Equal(TimescaleSupport.WaitStatsHourlyRetentionInterval, (string?)await retention.ExecuteScalarAsync(ct));

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var availability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(availability.WaitStatsHourly);
        Assert.True(availability.WaitStatsDaily);
    }

    [Fact]
    public async Task APlainStore_ReadsThePlainHourlyView_AndGetsTheSameRowsAsTheRawStatement()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.False(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationIsContinuousAggregateSql(TimescaleSupport.WaitStatsHourlyView), ct), "wait_stats is a plain table here, so the relation is the plain view");
        await SeedAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        Assert.True((await TimescaleSupport.DetectRollupsAsync(dataSource, ct)).WaitStatsHourly, "the plain view is the relation the probe finds");

        const string raw = "SELECT server_id, date_trunc('hour', collection_time) AS bucket, wait_type, SUM(delta_wait_time_ms) AS delta_wait_time_ms FROM collect.wait_stats WHERE delta_wait_time_ms > 0 GROUP BY 1, 2, 3";
        const string view = "SELECT server_id, bucket, wait_type, delta_wait_time_ms FROM collect.wait_stats_hourly";
        Assert.True(await ScalarAsync<long>(connection, $"SELECT count(*) FROM ({raw}) AS x", ct) > 100);
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM (({raw}) EXCEPT ({view})) AS x", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM (({view}) EXCEPT ({raw})) AS x", ct));
    }

    private static string RawSql(DateTime start) => DailySummarySql.RangeSqlFor(RetentionTier.Daily, Coverage(RollupAvailability.None), start);

    private static string HourlySql(DateTime start) => DailySummarySql.RangeSqlFor(RetentionTier.Daily, Coverage(DailyAndHourly), start);

    private static RollupCoverage Coverage(RollupAvailability availability)
        => new(new Dictionary<string, DateTime>(StringComparer.Ordinal), new Dictionary<string, DateTime>(StringComparer.Ordinal), availability);

    private static string Ts(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    private static string Args(DateTime start, DateTime end)
        => $"{ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}'";

    /// <summary>EXCEPT both ways between the raw statement and the hourly statement over [start, end); returns the row count.</summary>
    private static async Task<long> AssertSameRowsAsync(NpgsqlConnection connection, DateTime start, DateTime end, CancellationToken ct)
    {
        const string types = "int, timestamp, timestamp, timestamp";
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS pg_temp.ws_raw; DROP TABLE IF EXISTS pg_temp.ws_roll; DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE ws_raw_q({types}) AS {RawSql(start)}", ct);
        await ExecuteAsync(connection, $"PREPARE ws_roll_q({types}) AS {HourlySql(start)}", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE ws_raw AS EXECUTE ws_raw_q({Args(start, end)})", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE ws_roll AS EXECUTE ws_roll_q({Args(start, end)})", ct);

        var rows = await ScalarAsync<long>(connection, "SELECT count(*) FROM ws_raw", ct);
        Assert.Equal(rows, await ScalarAsync<long>(connection, "SELECT count(*) FROM ws_roll", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM ws_raw EXCEPT SELECT * FROM ws_roll) AS x", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM ws_roll EXCEPT SELECT * FROM ws_raw) AS x", ct));
        return rows;
    }

    /// <summary>The raw <c>wait_stats</c> rows the statement's plan read (rows of the scans over the table's chunks).</summary>
    private static async Task<(long AtLeast, long AtMost)> RawWaitRowsAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, ISet<string> waitChunks, CancellationToken ct)
    {
        await ExecuteAsync(connection, "DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE ws_rows_q(int, timestamp, timestamp, timestamp) AS {sql}", ct);
        var plan = await ScalarAsync<object>(connection, $"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE ws_rows_q({Args(start, end)})", ct);
        return (ExplainBuffers.RowsReadAtLeast(plan, waitChunks), ExplainBuffers.RowsReadAtMost(plan, waitChunks));
    }

    private static async Task<NpgsqlConnection> BuildTimescaleStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #8824 tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        return connection;
    }

    /// <summary>Four days of waits every 5 minutes for two servers and five wait types, two of them one wait type under two spellings (the
    /// trailing space the Daily Summary merges), some zero deltas the rollups filter, from three days ago midnight to
    /// <see cref="LiveClock.Now"/>: the days the tests read are cut from the same clock, so yesterday holds every hour the tests name at any
    /// pinned hour (#9809).</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var seed = new NpgsqlCommand(@"
INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds)
SELECT 882400000 + row_number() OVER (), t, s.server_id, 'wait-hourly-server', w.wait_type, 5,
       CASE WHEN (extract(epoch FROM t)::bigint / 60 + w.k) % 11 = 0 THEN 0 ELSE ((extract(epoch FROM t)::bigint / 60 * w.k * 37 + w.k * 101 + s.server_id) % 997) + 1 END,
       60
FROM generate_series(date_trunc('day', $1::timestamp) - interval '3 days', $1::timestamp, interval '5 minutes') AS t
CROSS JOIN (VALUES (8824), (8825)) AS s(server_id)
CROSS JOIN (VALUES ('SOS_SCHEDULER_YIELD', 1), ('PAGEIOLATCH_SH ', 2), ('PAGEIOLATCH_SH', 3), ('CXCONSUMER', 4), ('WRITELOG', 5)) AS w(wait_type, k)", connection) { CommandTimeout = 300 };
        seed.Parameters.AddWithValue(LiveClock.Now());
        await seed.ExecuteNonQueryAsync(ct);
    }

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
