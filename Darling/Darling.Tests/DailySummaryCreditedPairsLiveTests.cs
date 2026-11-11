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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8651: the Daily Summary's credited unique-queries count reads the <c>(day, query_hash)</c> pairs index-only and takes away the
/// restart-only pairs (<c>DailySummarySql.CreditedPairs</c>), instead of filtering the pair read on <c>sample_interval_seconds</c>, a column
/// <c>idx_query_stats_server_hash_time</c> does not hold. The simple filter moved the read from an index-only scan to heap fetches (a busy
/// server's open day: 252 ms warm to 432 ms). These tests pin the plan (the routed raw tail on an uncompressed chunk, with the heap paths
/// off the table as the planner's costing is not what is under test) and the result (the same per-day counts as the credited-row ground
/// truth, on seeds with a restart-only hash, a hash with a restart row and a credited row on one day, and a hash whose restart row and
/// credited row fall on different days).
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): each test mints its own scratch database through <see cref="ScratchPostgres"/> and
/// drops it whole.</para>
/// </summary>
public sealed class DailySummaryCreditedPairsLiveTests
{
    private const int ServerId = 86510;
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8651 plan tests.";

    private static readonly RollupCoverage NoCoverage = new(
        new Dictionary<string, DateTime>(StringComparer.Ordinal), new Dictionary<string, DateTime>(StringComparer.Ordinal), RollupAvailability.None);

    [Fact]
    public async Task TheRoutedRawTail_ReadsThePairsIndexOnly_AndTheRestartReadUsesThePartialIndex_OnAnUncompressedChunk()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "The live #8651 plan tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await ExecuteAsync(connection, "CREATE INDEX IF NOT EXISTS idx_query_stats_server_hash_time ON collect.query_stats (server_id, query_hash, collection_time DESC)", ct);
        await ExecuteAsync(connection, "CREATE INDEX IF NOT EXISTS " + PgTableTuning.QueryStatsRestartRowIndexName
            + " ON collect.query_stats (collection_time) WHERE sample_interval_seconds = 0", ct);

        var now = LiveClock.Now();
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        var yesterday = today.AddDays(-1);

        /* Yesterday is rolled up, the open day is not: the hourly rollup's newest bucket ends at today, so the raw tail is the open day. */
        await SeedAsync(connection, yesterday, today, 3000, 800, 997, ct);
        await SeedAsync(connection, today, now.AddMinutes(-1), 60000, 6000, 3001, ct);
        await PlantAsync(connection, today, ct);
        await ExecuteAsync(connection,
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStatsHourlyView}'::regclass, '{Ts(yesterday.AddDays(-1))}'::timestamp, '{Ts(today)}'::timestamp)", ct);
        await VacuumChunksAsync(connection, ct);

        var start = yesterday;
        var end = today.AddDays(1);
        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Hourly, NoCoverage, start);

        /* The result first: the open day's count is the credited distinct hashes, a restart-only hash left out and a hash with a credited row kept. */
        var expectedToday = await ScalarAsync<long>(connection,
            $"SELECT count(DISTINCT query_hash) FROM collect.query_stats WHERE server_id = {ServerId} AND collection_time >= '{Ts(today)}' AND collection_time < '{Ts(end)}' "
            + "AND sample_interval_seconds IS DISTINCT FROM 0", ct);
        var allToday = await ScalarAsync<long>(connection,
            $"SELECT count(DISTINCT query_hash) FROM collect.query_stats WHERE server_id = {ServerId} AND collection_time >= '{Ts(today)}' AND collection_time < '{Ts(end)}'", ct);
        Assert.True(allToday > expectedToday, "the seed holds restart-only hashes, so the credited count is lower than the all-rows count");
        Assert.Equal(expectedToday, await DayCountAsync(connection, sql, start, end, today, ct));

        /* The plan. Heap paths off: with them on the planner may still pick a bitmap scan on a chunk this small, which is its costing, not the
           shape's. With them off, the pair read can only take the index-only scan if no column outside the index is named. */
        await ExecuteAsync(connection, "SET enable_seqscan = off; SET enable_bitmapscan = off", ct);
        var plan = await PlanAsync(connection, sql, start, end, ct);
        var nodes = new List<(string Type, string Index, double Loops, double Rows)>();
        Collect(plan.RootElement[0].GetProperty("Plan"), nodes);
        Assert.Contains(nodes, n => n.Type == "Index Only Scan" && n.Index.EndsWith("idx_query_stats_server_hash_time", StringComparison.Ordinal)
            && n.Loops > 0 && n.Rows >= 30000);
        Assert.Contains(nodes, n => n.Index.EndsWith(PgTableTuning.QueryStatsRestartRowIndexName, StringComparison.Ordinal) && n.Loops > 0);
        await ExecuteAsync(connection, "RESET enable_seqscan; RESET enable_bitmapscan", ct);
    }

    [Fact]
    public async Task APlainStoresRawStatement_GivesTheCreditedCountPerDay_AndPlansTheIndexOnlyScan()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "CREATE INDEX IF NOT EXISTS idx_query_stats_server_hash_time ON collect.query_stats (server_id, query_hash, collection_time DESC)", ct);
        await ExecuteAsync(connection, "CREATE INDEX IF NOT EXISTS " + PgTableTuning.QueryStatsRestartRowIndexName
            + " ON collect.query_stats (collection_time) WHERE sample_interval_seconds = 0", ct);

        var now = LiveClock.Now();
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        var start = today.AddDays(-6);
        await SeedAsync(connection, start, today, 20000, 3000, 499, ct);
        /* On two days: a restart-only hash, a hash with a restart and a credited row on one day, and a hash whose restart row and credited row
           fall on different days (counted on the credited day only). A NULL interval counts as credited. */
        await PlantAsync(connection, start.AddDays(1), ct);
        await PlantAsync(connection, start.AddDays(3), ct);
        await InsertAsync(connection, 861000L, start.AddDays(2).AddHours(7), "PRE_COLUMN", null, ct);
        await LiveVisibilityMap.VacuumUntilAllVisibleAsync(connection, "collect.query_stats", ct);
        await ExecuteAsync(connection, "ANALYZE collect.query_stats", ct);

        var sql = DailySummarySql.RangeSqlFor(RetentionTier.Raw, NoCoverage, start);
        Assert.NotEqual(DailySummarySql.RangeSql, sql);
        var end = today;

        /* The per-day counts equal the credited-row ground truth, both ways. */
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS pg_temp.cp_actual; DROP TABLE IF EXISTS pg_temp.cp_expected; DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE cp_q(int, timestamp, timestamp, timestamp) AS {sql}", ct);
        await ExecuteAsync(connection,
            $"CREATE TEMP TABLE cp_actual AS EXECUTE cp_q({ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}')", ct);
        await ExecuteAsync(connection,
            $"CREATE TEMP TABLE cp_expected AS SELECT date_trunc('day', collection_time) AS d, count(DISTINCT query_hash)::bigint AS c FROM collect.query_stats "
            + $"WHERE server_id = {ServerId} AND collection_time >= '{Ts(start)}' AND collection_time < '{Ts(end)}' AND sample_interval_seconds IS DISTINCT FROM 0 GROUP BY 1", ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection,
            "SELECT count(*) FROM (SELECT d, c FROM cp_expected EXCEPT SELECT day, unique_queries FROM cp_actual WHERE unique_queries > 0) x", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection,
            "SELECT count(*) FROM (SELECT day, unique_queries::bigint FROM cp_actual WHERE unique_queries > 0 EXCEPT SELECT d, c FROM cp_expected) x", ct));
        Assert.True(await ScalarAsync<long>(connection, "SELECT count(*) FROM cp_expected", ct) >= 6);

        /* The plan, heap paths off as above: the pair read is index-only and the restart read takes the partial index. */
        await ExecuteAsync(connection, "SET enable_seqscan = off; SET enable_bitmapscan = off", ct);
        var plan = await PlanAsync(connection, sql, start, end, ct);
        var nodes = new List<(string Type, string Index, double Loops, double Rows)>();
        Collect(plan.RootElement[0].GetProperty("Plan"), nodes);
        Assert.Contains(nodes, n => n.Type == "Index Only Scan" && n.Index == "idx_query_stats_server_hash_time" && n.Loops > 0);
        Assert.Contains(nodes, n => n.Index == PgTableTuning.QueryStatsRestartRowIndexName && n.Loops > 0);
    }

    private static void Collect(JsonElement node, List<(string Type, string Index, double Loops, double Rows)> into)
    {
        var type = node.GetProperty("Node Type").GetString() ?? string.Empty;
        var index = node.TryGetProperty("Index Name", out var name) ? name.GetString() ?? string.Empty : string.Empty;
        var loops = node.TryGetProperty("Actual Loops", out var l) ? l.GetDouble() : 0;
        var rows = node.TryGetProperty("Actual Rows", out var r) ? r.GetDouble() : 0;
        into.Add((type, index, loops, rows));
        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                Collect(child, into);
            }
        }
    }

    private static async Task<JsonDocument> PlanAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await ExecuteAsync(connection, "DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE cp_plan_q(int, timestamp, timestamp, timestamp) AS {sql}", ct);
        var plan = await ScalarAsync<object>(connection,
            $"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE cp_plan_q({ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}')", ct);
        return JsonDocument.Parse(plan.ToString()!);
    }

    /// <summary>The statement's <c>unique_queries</c> for one day.</summary>
    private static async Task<long> DayCountAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, DateTime day, CancellationToken ct)
    {
        await ExecuteAsync(connection, "DROP TABLE IF EXISTS pg_temp.cp_day; DEALLOCATE ALL", ct);
        await ExecuteAsync(connection, $"PREPARE cp_day_q(int, timestamp, timestamp, timestamp) AS {sql}", ct);
        await ExecuteAsync(connection, $"CREATE TEMP TABLE cp_day AS EXECUTE cp_day_q({ServerId}, '{Ts(start)}', '{Ts(end)}', '{Ts(EventWindowFloor.For(start))}')", ct);
        return await ScalarAsync<long>(connection, $"SELECT COALESCE(sum(unique_queries), 0)::bigint FROM cp_day WHERE day = '{Ts(day)}'", ct);
    }

    /// <summary>
    /// Rows spread evenly over [from, to), <paramref name="hashes"/> distinct hashes, one row in <paramref name="restartEvery"/> a restart row.
    /// </summary>
    private static Task SeedAsync(NpgsqlConnection connection, DateTime from, DateTime to, int rows, int hashes, int restartEvery, CancellationToken ct)
        => ExecuteAsync(connection, $"""
            INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                             delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
            SELECT 861100000 + (extract(epoch FROM '{Ts(from)}'::timestamp)::bigint % 100000) * 1000 + n,
                   '{Ts(from)}'::timestamp + ((n::double precision / {rows}) * ('{Ts(to)}'::timestamp - '{Ts(from)}'::timestamp)),
                   {ServerId}, 'credited-pairs-server', 'db', 'Q' || (n % {hashes}), '0x01', 10, 10, 1,
                   CASE WHEN n % {restartEvery} = 0 THEN 0 ELSE 120 END
            FROM generate_series(0, {rows - 1}) AS g(n)
            """, ct);

    /// <summary>A restart-only hash, a hash with both kinds of row on the day, and a hash restarted on the day and credited on the next.</summary>
    private static async Task PlantAsync(NpgsqlConnection connection, DateTime day, CancellationToken ct)
    {
        var id = 861200000L + (long)(day - new DateTime(2020, 1, 1)).TotalDays * 10;
        await InsertAsync(connection, id++, day.AddHours(1), "RESTART_ONLY", 0, ct);
        await InsertAsync(connection, id++, day.AddHours(2), "RESTART_ONLY", 0, ct);
        await InsertAsync(connection, id++, day.AddHours(3), "BOTH_KINDS", 0, ct);
        await InsertAsync(connection, id++, day.AddHours(4), "BOTH_KINDS", 120, ct);
        await InsertAsync(connection, id++, day.AddHours(5), "RESTART_THEN_NEXT_DAY", 0, ct);
        await InsertAsync(connection, id, day.AddDays(1).AddHours(5), "RESTART_THEN_NEXT_DAY", 120, ct);
    }

    private static async Task InsertAsync(NpgsqlConnection connection, long id, DateTime at, string hash, int? interval, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand("""
            INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                                             delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
            VALUES ($1, $2, $3, 'credited-pairs-server', 'db', $4, '0x01', 1, 1, 1, $5)
            """, connection);
        insert.Parameters.AddWithValue(id);
        insert.Parameters.AddWithValue(at);
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(hash);
        insert.Parameters.AddWithValue(interval is null ? DBNull.Value : interval.Value);
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Vacuum each uncompressed chunk until every page is marked, so the index-only scan is priced as autovacuum leaves a settled chunk (#8204).</summary>
    private static async Task VacuumChunksAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var names = new List<string>();
        await using (var list = new NpgsqlCommand(
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_stats' AND NOT is_compressed", connection))
        await using (var reader = await list.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                names.Add(reader.GetString(0));
            }
        }

        foreach (var name in names)
        {
            await LiveVisibilityMap.VacuumUntilAllVisibleAsync(connection, name, ct);
            await ExecuteAsync(connection, $"ANALYZE {name}", ct);
        }
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
        var value = (await command.ExecuteScalarAsync(ct))!;
        return typeof(T) == typeof(object) ? (T)value : (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
    }
}
