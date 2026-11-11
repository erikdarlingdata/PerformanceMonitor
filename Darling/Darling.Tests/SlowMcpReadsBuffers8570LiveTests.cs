/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #8570: two reads that took four to five seconds on a first call.
/// <para><c>get_top_queries_by_cpu</c>: the statement's second pass, which builds the wide row of each of the 25 winners, joined the
/// winners to a second scan of the whole 24-hour window, so it read every page of the window twice. It now asks for the winners' query
/// hashes through the (server_id, query_hash, collection_time) index. The test bounds the pages the statement reads
/// (<see cref="ExplainBuffers.DataPages(object?)"/>) against the pages one pass over the window reads, and checks the answer for a NULL
/// query hash and for a hash two winners share.</para>
/// <para><c>get_forced_plans</c>: the per-database count of forces in the newest capture built the newest-row-per-force step over every
/// capture row of the window, materializing and sorting all of them to keep a few hundred. It now reads the rows from each database's newest
/// marker on. The pages are the same either way (the window is read once), so the test asserts the statement's shape: no sort and no CTE
/// scan over thousands of rows.</para>
/// </summary>
public sealed class SlowMcpReadsBuffers8570LiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8570 buffer tests (they mint their own scratch databases).";
    private const int ServerId = 8570;

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

    private static async Task<long> ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task TopQueries_SecondPass_ProbesTheWinnersRows_AndAnswersTheSame()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* 120 collections over the last 24 hours, 400 rows each: 3,000 ordinary hashes, one NULL-hash group, and one hash that two host
           objects share. The three big groups are the winners; every ordinary row is under them. */
        var end = DateTime.UtcNow;
        var start = end.AddHours(-24);
        await ExecuteAsync(connection, @"
INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, creation_time, last_execution_time,
  execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_logical_writes, total_physical_reads, total_rows, total_spills,
  min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time, min_dop, max_dop, sql_handle, plan_handle,
  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills,
  sample_interval_seconds, host_object_name)
SELECT 880000000 + row_number() OVER (), c.ts::timestamp, " + ServerId + @", 'top-8570', 'db' || (r % 10),
  CASE WHEN r = 0 THEN NULL WHEN r IN (1, 2) THEN '0xshared' ELSE '0x' || md5('q' || r) END,
  '0x' || md5('p' || r), c.ts::timestamp - interval '3 days', c.ts::timestamp - interval '10 seconds',
  1000, 1000000, 2000000, 5000, 10, 20, 100, 0, 10, 100000, 20, 200000, 1, 4, '0x' || md5('s' || r), '0x' || md5('pl' || r),
  5, CASE WHEN r = 0 THEN 9000000 WHEN r = 1 THEN 8000000 WHEN r = 2 THEN 7000000 ELSE 100 + r END, 1000, 10, 1, 1, 10, 0,
  120, CASE WHEN r = 1 THEN 'dbo.a' WHEN r = 2 THEN 'dbo.b' END
FROM generate_series(now() at time zone 'utc' - interval '23 hours', now() at time zone 'utc' - interval '1 minute', interval '11 minutes 30 seconds') AS c(ts)
CROSS JOIN generate_series(0, 399) AS r", ct);
        var collections = await ScalarLongAsync(connection, $"SELECT count(DISTINCT collection_time) FROM query_stats WHERE server_id = {ServerId}", ct);
        await ExecuteAsync(connection, "VACUUM (ANALYZE) query_stats", ct);

        /* The answer. A row of the NULL-hash group, and each of the two host objects of the shared hash, is summed once. */
        await using var postgres = NpgsqlDataSource.Create(connection.ConnectionString);
        var top = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, ServerId, start, end, 5, DatabaseFilter.All, cancellationToken: ct);
        Assert.Equal(new[] { collections * 9_000_000, collections * 8_000_000, collections * 7_000_000 }, top.Take(3).Select(r => r.TotalCpuUs).ToArray());
        Assert.Equal(new long?[] { 5 * collections, 5 * collections, 5 * collections }, top.Take(3).Select(r => (long?)r.TotalExecutions).ToArray());
        Assert.Equal(new string?[] { null, "dbo.a", "dbo.b" }, top.Take(3).Select(r => r.HostObjectName).ToArray());

        /* The pages: one pass over the window, read as a plain aggregate with no index to read it by, against the whole statement. */
        await ExecuteAsync(connection, "SET enable_indexscan = off; SET enable_bitmapscan = off; SET max_parallel_workers_per_gather = 0", ct);
        long onePass;
        await using (var reference = new NpgsqlCommand(
            $"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT sum(delta_worker_time), sum(delta_elapsed_time), sum(delta_logical_reads), max(last_execution_time) FROM query_stats WHERE server_id = {ServerId} AND collection_time >= $1 AND collection_time <= $2",
            connection))
        {
            reference.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
            reference.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
            onePass = ExplainBuffers.DataPages(await reference.ExecuteScalarAsync(ct));
        }

        await ExecuteAsync(connection, "RESET enable_indexscan; RESET enable_bitmapscan; RESET max_parallel_workers_per_gather", ct);

        var sql = QueryTagReadJoin.Apply(TopRankings.Apply(DarlingDataReader.TopQueriesSql, TopRanking.Cpu, hourly: false), ServerId, null);
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
        explain.Parameters.AddWithValue(ServerId);
        explain.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        explain.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        explain.Parameters.AddWithValue(5);
        explain.Parameters.Add(DatabaseFilter.All.Parameter());
        explain.Parameters.AddWithValue(0);
        explain.Parameters.AddWithValue(10);
        explain.Parameters.Add(DatabaseFilter.All.ExcludeParameter());
        var pages = ExplainBuffers.DataPages(await explain.ExecuteScalarAsync(ct));

        /* Measured on this seed and on a 360,000-row window: the old statement read the window twice (73,100 pages against 36,700 for one
           pass), the probes read it once and a few hundred pages more. 1.5 passes sits between. */
        Assert.True(pages < onePass * 1.5, $"the statement read {pages} pages, one pass over the window reads {onePass}");
    }

    [Fact]
    public async Task ForcedPlanCounts_ReadTheRowsFromTheNewestMarkerOn_NotTheWholeWindowTwice()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* Three databases of 20 forces each, captured every 6 minutes for 24 hours (240 captures: 15,000 rows). Force 99 of db1 was
           forced for the first 120 captures and then removed, so it is in the window but not in the newest capture and is not counted. */
        await ExecuteAsync(connection, @"
INSERT INTO query_store_forced_plans (collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, query_plan_hash, plan_forcing_type, force_failure_count)
SELECT 770000000 + row_number() OVER (), c.ts::timestamp, " + ServerId + @", 'forced-8570', 'db' || d, f, d * 1000 + f, '0x' || md5('p' || f), 'MANUAL', 0
FROM generate_series(now() at time zone 'utc' - interval '24 hours', now() at time zone 'utc' - interval '1 minute', interval '6 minutes') WITH ORDINALITY AS c(ts, n)
CROSS JOIN generate_series(1, 3) AS d
CROSS JOIN LATERAL (SELECT f FROM generate_series(1, 20) AS f UNION ALL SELECT NULL UNION ALL SELECT 99 WHERE d = 1 AND c.n <= 120) AS x(f)", ct);
        await ExecuteAsync(connection, "VACUUM (ANALYZE) query_store_forced_plans", ct);

        var end = DateTime.UtcNow;
        var start = end.AddHours(-24);
        var sql = DarlingForcedPlanStateReader.CountsSql(DatabaseFilter.All);

        NpgsqlCommand Bind(string text)
        {
            var command = new NpgsqlCommand(text, connection);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
            command.Parameters.Add(DatabaseFilter.All.Parameter());
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = DBNull.Value });
            return command;
        }

        await using (var counts = Bind(sql))
        await using (var reader = await counts.ExecuteReaderAsync(ct))
        {
            var found = new System.Collections.Generic.List<(string, long)>();
            while (await reader.ReadAsync(ct))
                found.Add((reader.GetString(0), reader.GetInt64(1)));
            Assert.Equal(new[] { ("db1", 20L), ("db2", 20L), ("db3", 20L) }, found.OrderBy(f => f.Item1, StringComparer.Ordinal).ToArray());
        }

        await using var explain = Bind("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql);
        using var document = JsonDocument.Parse(Convert.ToString(await explain.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture)!);
        var widest = WidestSortOrCteScan(document.RootElement[0].GetProperty("Plan"));

        /* Measured on this seed (15,000 rows) and on 440,000 rows: the old statement's CTE scan and sort carried every capture row of
           the window (15,000 here, 432,000 there, 1,000 ms), the new one's largest sort or CTE scan is a few rows. */
        Assert.True(widest < 1000, $"the statement sorted or scanned a CTE of {widest} rows");
    }

    private static long WidestSortOrCteScan(JsonElement plan)
    {
        var widest = 0L;
        var type = plan.GetProperty("Node Type").GetString();
        if (type is "Sort" or "CTE Scan" or "Incremental Sort")
        {
            widest = (long)(plan.GetProperty("Actual Rows").GetDouble() * plan.GetProperty("Actual Loops").GetDouble());
        }

        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
                widest = Math.Max(widest, WidestSortOrCteScan(child));
        }

        return widest;
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }
}
