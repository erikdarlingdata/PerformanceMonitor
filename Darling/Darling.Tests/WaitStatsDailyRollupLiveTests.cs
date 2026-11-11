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
/// #8549: the Daily Summary's wait part reads <c>collect.wait_stats_daily</c> for the closed days it holds and raw
/// <c>wait_stats</c> for every other day. The rollup is the same sum the raw statement runs, so the two statements must return the
/// same rows: EXCEPT both ways over a window that crosses rollup days, the open day and a day missing from the rollup, then over a
/// window that does not start at midnight. The rollup read must also touch a fraction of the pages (<see cref="ExplainBuffers"/>, never
/// a time), fall back to the raw shape when the relation is absent or empty, and be there on a fresh, an upgraded and a plain store.
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): each test mints its own scratch database through <see cref="ScratchPostgres"/> and
/// drops it whole.</para>
/// </summary>
public sealed class WaitStatsDailyRollupLiveTests
{
    private const int ServerId = 8549;
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8549 tests.";

    private static readonly RollupAvailability WithRollup = new(false, false, false, false, false, false, false, false, WaitStatsDaily: true);

    [Fact]
    public async Task TheRollupRead_EqualsTheRawStatement_AcrossRollupDays_TheOpenDay_AndAMissingDay_AndReadsFarFewerPages()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = await BuildTimescaleStoreAsync(scratch, ct);
        await SeedThirtyDaysAsync(connection, ct, minutes: 1);

        var today = LiveClock.Now().Date;
        var start = today.AddDays(-29);
        var end = today.AddDays(1);

        /* The aggregate exists but holds nothing yet (WITH NO DATA, the sweep's backfill has not run): every day reads raw. */
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.WaitStatsDailyView}", ct));
        await AssertSameRowsAsync(connection, start, end, ct);

        Assert.True(await TimescaleSupport.BackfillWaitStatsDailyAsync(connection, null, LiveClock.Now(), ct), "the backfill fills the closed days");
        Assert.False(await TimescaleSupport.BackfillWaitStatsDailyAsync(connection, null, LiveClock.Now(), ct), "and converges: a second pass finds the coverage it needs");
        Assert.True(await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.WaitStatsDailyView} WHERE server_id = {ServerId} AND bucket = '{Ts(today.AddDays(-1))}'", ct) > 0,
            "yesterday is a closed day and is in the rollup");
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.WaitStatsDailyView} WHERE bucket >= '{Ts(today)}'", ct));

        /* A hole in the middle of the rollup: that day reads raw. */
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, TimescaleSupport.WaitStatsDailyView, ct);
        Assert.NotNull(materialization);
        await ExecuteAsync(connection, $"DELETE FROM \"{materialization!.Value.Schema}\".\"{materialization.Value.Name}\" WHERE bucket = '{Ts(today.AddDays(-10))}'", ct);

        var rows = await AssertSameRowsAsync(connection, start, end, ct);
        Assert.True(rows >= 30, $"the window has a row per day, got {rows}");

        /* A window that does not start or end at midnight: the partial days sum only the rows inside the window, so they read raw. */
        await AssertSameRowsAsync(connection, today.AddDays(-12).AddHours(14).AddMinutes(30), today.AddHours(9).AddMinutes(10), ct);

        /* The closed days read from the rollup: the plan names it, and the statement reads a fraction of the raw statement's pages. */
        var rawPages = await DataPagesAsync(connection, RawSql(start), start, end, ct);
        var rollupSql = RollupSql(start);
        Assert.Contains("wait_stats_daily", rollupSql, StringComparison.Ordinal);
        var rollupPages = await DataPagesAsync(connection, rollupSql, start, end, ct);
        Assert.True(rollupPages * 3 <= rawPages, $"the rollup read touches {rollupPages} pages against the raw statement's {rawPages}");
    }

    [Fact]
    public async Task TheHourlyTier_ReadsTheRollupToo_AndEqualsTheRawStatement()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = await BuildTimescaleStoreAsync(scratch, ct);
        await SeedThirtyDaysAsync(connection, ct);

        var today = LiveClock.Now().Date;
        var start = today.AddDays(-29);
        var end = today.AddDays(1);
        Assert.Contains("wait_stats_daily", RollupSql(start, RetentionTier.Hourly), StringComparison.Ordinal);
        Assert.DoesNotContain("wait_stats_daily", RawSql(start, RetentionTier.Hourly), StringComparison.Ordinal);

        /* Empty aggregate: every day reads raw. */
        await AssertSameRowsAsync(connection, start, end, ct, RetentionTier.Hourly);

        /* Filled: the closed days read the rollup. */
        Assert.True(await TimescaleSupport.BackfillWaitStatsDailyAsync(connection, null, LiveClock.Now(), ct));
        var rows = await AssertSameRowsAsync(connection, start, end, ct, RetentionTier.Hourly);
        Assert.True(rows >= 30, $"the window has a row per day, got {rows}");

        /* A hole in the rollup, and a window that starts and ends inside a day: those days read raw. */
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, TimescaleSupport.WaitStatsDailyView, ct);
        Assert.NotNull(materialization);
        await ExecuteAsync(connection, $"DELETE FROM \"{materialization!.Value.Schema}\".\"{materialization.Value.Name}\" WHERE bucket = '{Ts(today.AddDays(-10))}'", ct);
        await AssertSameRowsAsync(connection, start, end, ct, RetentionTier.Hourly);
        await AssertSameRowsAsync(connection, today.AddDays(-12).AddHours(14).AddMinutes(30), today.AddHours(9).AddMinutes(10), ct, RetentionTier.Hourly);
    }

    [Fact]
    public async Task WithoutTheRelation_TheReadFallsBackToTheRawShape_AndStillRuns()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = await BuildTimescaleStoreAsync(scratch, ct);
        await SeedThirtyDaysAsync(connection, ct);
        await ExecuteAsync(connection, TimescaleSupport.DropRetiredBaselineRelationSql(TimescaleSupport.WaitStatsDailyView), ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var availability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.False(availability.WaitStatsDaily, "the probe sees no relation");

        var start = LiveClock.Now().Date.AddDays(-29);
        var coverage = new RollupCoverage(new Dictionary<string, DateTime>(StringComparer.Ordinal), new Dictionary<string, DateTime>(StringComparer.Ordinal), availability);
        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Daily, coverage, start);
        Assert.DoesNotContain("wait_stats_daily", sql, StringComparison.Ordinal);
        Assert.True(await ReadRowCountAsync(connection, sql, start, LiveClock.Now().Date.AddDays(1), ct) >= 30);
    }

    [Fact]
    public async Task AFreshStore_HasThePlainViewFromTheRung_AndTheAggregateOnceTheTableIsAHypertable()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationExistsSql(TimescaleSupport.WaitStatsDailyView), ct), "the rung creates the relation on every store");
        Assert.Equal(StorageVersion.SchemaVersion, await ScalarAsync<int>(connection, "SELECT max(version) FROM darling_schema_version", ct));

        Assert.SkipWhen(!await TimescaleSupport.TryEnableAsync(connection, null, ct), "The live #8549 fresh-store test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        Assert.True(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationIsContinuousAggregateSql(TimescaleSupport.WaitStatsDailyView), ct),
            "the sweep replaces the plain view with the aggregate once wait_stats is a hypertable");
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = 'collect' AND hypertable_name = '{TimescaleSupport.WaitStatsDailyView}' AND proc_name = 'policy_refresh_continuous_aggregate'", ct));
    }

    [Fact]
    public async Task AnUpgradedStore_GetsTheEmptyAggregateFromTheRung_ThenItsPolicyAndBackfillFromTheSweep()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = await BuildTimescaleStoreAsync(scratch, ct);
        await SeedThirtyDaysAsync(connection, ct);

        await ExecuteAsync(connection, TimescaleSupport.DropRetiredBaselineRelationSql(TimescaleSupport.WaitStatsDailyView), ct);
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 258, ct);
        await ExecuteAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 259", ct);
        Assert.Equal(258, await ScalarAsync<int>(connection, "SELECT max(version) FROM darling_schema_version", ct));

        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationIsContinuousAggregateSql(TimescaleSupport.WaitStatsDailyView), ct), "V259 creates the aggregate where wait_stats is a hypertable");
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.WaitStatsDailyView}", ct));

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        Assert.Equal(1L, await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = 'collect' AND hypertable_name = '{TimescaleSupport.WaitStatsDailyView}' AND proc_name = 'policy_refresh_continuous_aggregate'", ct));
        Assert.True(await TimescaleSupport.BackfillWaitStatsDailyAsync(connection, null, LiveClock.Now(), ct));
        Assert.True(await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{TimescaleSupport.WaitStatsDailyView}", ct) > 0);
    }

    [Fact]
    public async Task APlainStore_ReadsThePlainView_AndGetsTheSameRowsAsTheRawStatement()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.False(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationIsContinuousAggregateSql(TimescaleSupport.WaitStatsDailyView), ct), "wait_stats is a plain table here, so the relation is the plain view");
        await SeedThirtyDaysAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        Assert.True((await TimescaleSupport.DetectRollupsAsync(dataSource, ct)).WaitStatsDaily, "the plain view is the relation the probe finds");
        Assert.False(await TimescaleSupport.BackfillWaitStatsDailyAsync(connection, null, LiveClock.Now(), ct), "a plain view has no materialization to fill");

        const string raw = "SELECT server_id, date_trunc('day', collection_time) AS bucket, wait_type, SUM(delta_wait_time_ms) AS delta_wait_time_ms FROM collect.wait_stats WHERE delta_wait_time_ms > 0 GROUP BY 1, 2, 3";
        const string view = "SELECT server_id, bucket, wait_type, delta_wait_time_ms FROM collect.wait_stats_daily";
        Assert.True(await ScalarAsync<long>(connection, $"SELECT count(*) FROM ({raw}) AS x", ct) > 100);
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM (({raw}) EXCEPT ({view})) AS x", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM (({view}) EXCEPT ({raw})) AS x", ct));
    }

    private static string RawSql(DateTime start, RetentionTier tier = RetentionTier.Daily) => DailySummarySql.RangeSqlFor(tier, Coverage(RollupAvailability.None), start);

    private static string RollupSql(DateTime start, RetentionTier tier = RetentionTier.Daily) => DailySummarySql.RangeSqlFor(tier, Coverage(WithRollup), start);

    private static RollupCoverage Coverage(RollupAvailability availability)
        => new(new Dictionary<string, DateTime>(StringComparer.Ordinal), new Dictionary<string, DateTime>(StringComparer.Ordinal), availability);

    private static string Ts(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    private static string Args(DateTime start, DateTime end)
        => $"{ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}'";

    /// <summary>EXCEPT both ways between the raw statement and the rollup statement over [start, end); returns the row count.</summary>
    private static async Task<long> AssertSameRowsAsync(NpgsqlConnection connection, DateTime start, DateTime end, CancellationToken ct, RetentionTier tier = RetentionTier.Daily)
    {
        const string types = "int, timestamp, timestamp, timestamp";
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS pg_temp.ws_raw; DROP TABLE IF EXISTS pg_temp.ws_roll; DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE ws_raw_q({types}) AS {RawSql(start, tier)}", ct);
        await ExecuteAsync(connection, $"PREPARE ws_roll_q({types}) AS {RollupSql(start, tier)}", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE ws_raw AS EXECUTE ws_raw_q({Args(start, end)})", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE ws_roll AS EXECUTE ws_roll_q({Args(start, end)})", ct);

        var rows = await ScalarAsync<long>(connection, "SELECT count(*) FROM ws_raw", ct);
        Assert.Equal(rows, await ScalarAsync<long>(connection, "SELECT count(*) FROM ws_roll", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM ws_raw EXCEPT SELECT * FROM ws_roll) AS x", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM (SELECT * FROM ws_roll EXCEPT SELECT * FROM ws_raw) AS x", ct));
        return rows;
    }

    private static async Task<long> DataPagesAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await ExecuteAsync(connection, "DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE ws_pages_q(int, timestamp, timestamp, timestamp) AS {sql}", ct);
        var plan = await ScalarAsync<object>(connection, $"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE ws_pages_q({Args(start, end)})", ct);
        return ExplainBuffers.DataPages(plan);
    }

    private static async Task<long> ReadRowCountAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await ExecuteAsync(connection, "DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE ws_count_q(int, timestamp, timestamp, timestamp) AS {sql}", ct);
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS pg_temp.ws_count", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE ws_count AS EXECUTE ws_count_q({Args(start, end)})", ct);
        return await ScalarAsync<long>(connection, "SELECT count(*) FROM ws_count", ct);
    }

    private static async Task<NpgsqlConnection> BuildTimescaleStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipWhen(!await TimescaleSupport.TryEnableAsync(connection, null, ct), "The live #8549 tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        return connection;
    }

    /// <summary>Thirty days of waits every 20 minutes for two servers and five wait types, two of them one wait type under two spellings
    /// (the trailing space the Daily Summary merges), some zero deltas the rollup filters, ending now. Compresses the chunks older than two
    /// days where the store can.</summary>
    private static async Task SeedThirtyDaysAsync(NpgsqlConnection connection, CancellationToken ct, int minutes = 20)
    {
        await ExecuteAsync(connection, @"
INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds)
SELECT 854900000 + row_number() OVER (), t, s.server_id, 'wait-rollup-server', w.wait_type, 5,
       CASE WHEN (extract(epoch FROM t)::bigint / 60 + w.k) % 11 = 0 THEN 0 ELSE ((extract(epoch FROM t)::bigint / 60 * w.k * 37 + w.k * 101 + s.server_id) % 997) + 1 END,
       60
FROM generate_series(date_trunc('day', now() AT TIME ZONE 'utc') - interval '29 days', now() AT TIME ZONE 'utc', make_interval(mins => $1)) AS t
CROSS JOIN (VALUES (8549), (8550)) AS s(server_id)
CROSS JOIN (VALUES ('SOS_SCHEDULER_YIELD', 1), ('PAGEIOLATCH_SH ', 2), ('PAGEIOLATCH_SH', 3), ('CXCONSUMER', 4), ('WRITELOG', 5)) AS w(wait_type, k)", ct, minutes);

        if (await ScalarAsync<bool>(connection, "SELECT to_regclass('timescaledb_information.hypertables') IS NOT NULL", ct)
            && await ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'wait_stats')", ct))
        {
            await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
            await ExecuteAsync(connection,
                "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('collect.wait_stats', older_than => (now() AT TIME ZONE 'utc') - interval '2 days') AS c", ct);
        }
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct, int? parameter = null)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        if (parameter is { } value)
        {
            command.Parameters.AddWithValue(value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
