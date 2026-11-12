/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #8549: the Viewer's Top Queries by Duration grid took 9.8 s on a day window of a busy field store, 8.3 s of it one statement that
/// wrote 95,272 temp blocks (about 745 MB). The statement ranked the window's groups in the same pass that built each group's wide row
/// (forty aggregates, a plan flag and the newest text pick), so a hash aggregate over many thousands of groups carried the wide row for
/// every group and spilled it. It now ranks on the grouping key and the sum, then builds the wide row for the kept groups only.
/// <para>The test runs the statement at the smallest work_mem, so a spill shows on a seed of a few thousand groups, and bounds the temp
/// blocks it writes against the ranking alone (the narrow aggregate every shape has to pay). It also checks the answer: the NULL-hash
/// group, the hash two host objects share and the ranking order are summed and ordered as before.</para>
/// </summary>
public sealed class TopQueriesRankingSpill8549LiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8549 tests (they mint their own scratch databases).";
    private const int ServerId = 8549;

    private static async Task<NpgsqlConnection> OpenStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);
        if (scratch.TimescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }

        await PgTableTuning.ApplyAsync(connection, null, ct);
        return connection;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static NpgsqlCommand TopQueries(NpgsqlConnection connection, string sql, DateTime start, DateTime end)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, 5);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, 10);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        return command;
    }

    /// <summary>The statement's temp blocks written: a node's own count already holds its children's, so the root's is the total.</summary>
    private static long TempBlocksWritten(string explainJson)
    {
        using var document = JsonDocument.Parse(explainJson);
        var root = document.RootElement[0].GetProperty("Plan");
        return root.TryGetProperty("Temp Written Blocks", out var written) ? written.GetInt64() : 0;
    }

    [Fact]
    public async Task TopQueries_RanksOnTheKeyAndTheSum_ThenBuildsTheWideRowForTheKeptGroups()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* 180 collections of 1,600 rows over the last 24 hours: 1,597 ordinary hashes, each with a 400-character text, plus a NULL-hash group
           and one hash two host objects share. The three big groups win; every ordinary row is far under them. */
        var end = LiveClock.AnchoredEnd();
        var start = end.AddHours(-24);
        var from = end.AddHours(-23).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        var to = end.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        await ExecuteAsync(connection, @"
INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, creation_time, last_execution_time,
  execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_logical_writes, total_physical_reads, total_rows, total_spills,
  min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time, min_dop, max_dop, sql_handle, plan_handle, query_text,
  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills,
  sample_interval_seconds, host_object_name)
SELECT 849000000 + row_number() OVER (), c.ts::timestamp, " + ServerId + @", 'top-8549', 'db' || (r % 10),
  CASE WHEN r = 0 THEN NULL WHEN r IN (1, 2) THEN '0xshared' ELSE '0x' || md5('q' || r) END,
  '0x' || md5('p' || r), c.ts::timestamp - interval '3 days', c.ts::timestamp - interval '10 seconds',
  1000, 1000000, 2000000, 5000, 10, 20, 100, 0, 10, 100000, 20, 200000, 1, 4, '0x' || md5('s' || r), '0x' || md5('pl' || r),
  'SELECT ' || repeat('col' || r || ', ', 50) || ' FROM dbo.t' || r,
  5, 100, CASE WHEN r = 0 THEN 9000000 WHEN r = 1 THEN 8000000 WHEN r = 2 THEN 7000000 ELSE 100 + r END, 10, 1, 1, 10, 0,
  120, CASE WHEN r = 1 THEN 'dbo.a' WHEN r = 2 THEN 'dbo.b' END
FROM generate_series('" + from + "'::timestamp, '" + to + @"'::timestamp, interval '8 minutes') AS c(ts)
CROSS JOIN generate_series(0, 1599) AS r", ct);
        var collections = await ScalarAsync(connection, $"SELECT count(DISTINCT collection_time) FROM query_stats WHERE server_id = {ServerId}", ct);
        await ExecuteAsync(connection, "VACUUM (ANALYZE) query_stats", ct);

        /* The answer: the NULL-hash group and each host object of the shared hash are summed once, in rank order. */
        await using var viewer = new ViewerDataService(connection.ConnectionString);
        var rows = await viewer.GetTopQueriesByCpuAsync(ServerId, start, end, 5, cancellationToken: ct);
        Assert.Equal(new[] { collections * 9_000_000, collections * 8_000_000, collections * 7_000_000 }, rows.Take(3).Select(r => r.TotalElapsedUs).ToArray());
        Assert.Equal(new string?[] { null, "dbo.a", "dbo.b" }, rows.Take(3).Select(r => r.HostObjectName).ToArray());
        Assert.Equal(5, rows.Count);
        Assert.StartsWith("SELECT col0, ", rows[0].QueryText, StringComparison.Ordinal);

        /* The spill: both statements run at the smallest work_mem, so a hash aggregate of more than a few hundred groups goes to disk. */
        await ExecuteAsync(connection, "SET work_mem = '64kB'; SET max_parallel_workers_per_gather = 0", ct);
        await using var ranking = new NpgsqlCommand(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT database_name, query_hash, host_object_name, SUM(delta_elapsed_time) AS s FROM query_stats "
            + "WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3 GROUP BY database_name, query_hash, host_object_name "
            + "HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0 ORDER BY SUM(delta_elapsed_time) DESC, database_name, query_hash, host_object_name LIMIT 10",
            connection);
        ranking.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
        ranking.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        ranking.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        var rankingOnly = TempBlocksWritten(Convert.ToString(await ranking.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!);

        await using var explain = TopQueries(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + ViewerDataService.TopQueriesSql, start, end);
        var statement = TempBlocksWritten(Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!);

        Assert.True(rankingOnly > 0, "the ranking alone did not spill at 64kB, so the seed is too small to tell the statements apart");
        Assert.True(statement <= rankingOnly * 2.5,
            $"the statement wrote {statement} temp blocks; ranking the same window on the key and the sum writes {rankingOnly}. The wide row must be built for the kept groups, not for every group of the window");
    }
}
