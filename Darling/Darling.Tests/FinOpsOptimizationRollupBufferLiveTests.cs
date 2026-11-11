/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #8552: the FinOps Optimization read took 12 to 30 seconds on a server's first visit on a production store, because the
/// most-expensive-queries statement read every raw <c>query_stats</c> row of the 24-hour window (a parallel sequential scan of
/// each chunk the window touches, shared by every server) while the hourly rollup already holds the same sums. The bound here is
/// not a time: it is the buffers the whole read touches (the shared blocks hit and read that <c>pg_stat_statements</c> counts for
/// the store's database), against the pages of the raw table. A read that scans the window's raw rows touches most of the table's
/// pages. A read that takes the whole hours from the hourly rollup and only the two edge hours from raw touches a few percent.
/// The ranking and the sums must not move: the test compares the read with the raw sums it replaces.
/// </summary>
public sealed class FinOpsOptimizationRollupBufferLiveTests
{
    private const int ServerA = 8552;
    private const int ServerB = 8553;
    private const int StatementsPerServer = 300;
    private const int StatementsPerSample = 150;
    private const int SampleMinutes = 2;
    private const int HoursSeeded = 36;
    private const int TopN = 20;

    /* The read may touch at most this share of the raw table's pages. The raw window scan touches about two thirds of them (24 of the
       36 seeded hours, every page shared by both servers), the rollup route about 7% (the two edge hours, the rollup rows and the
       per-statement lookups). */
    private const double MaxShareOfTablePages = 0.30;

    [Fact]
    public async Task TheRead_TakesWholeHoursFromTheRollup_AndTouchesAFractionOfTheRawPages()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live Optimization rollup buffer test.");
        var ct = TestContext.Current.CancellationToken;

        await using (var probe = new NpgsqlConnection(baseConnectionString))
        {
            await probe.OpenAsync(ct);
            await using var preloadCmd = new NpgsqlCommand("SELECT current_setting('shared_preload_libraries')", probe);
            var preload = (string)(await preloadCmd.ExecuteScalarAsync(ct))!;
            Assert.SkipWhen(!preload.Split(',').Select(s => s.Trim()).Contains("pg_stat_statements"),
                $"pg_stat_statements is not in shared_preload_libraries ('{preload}'): the buffer count reads it.");
        }

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        var end = new DateTime(DateTime.UtcNow.Ticks - DateTime.UtcNow.Ticks % TimeSpan.TicksPerMinute, DateTimeKind.Unspecified);
        var cutoff = end.AddHours(-24);
        long dbId;
        double tablePages;

        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            var timescale = await TimescaleSupport.TryEnableAsync(setup, null, ct);
            Assert.SkipWhen(!timescale, "The rollup route reads a continuous aggregate: TimescaleDB is required.");
            await TimescaleSupport.ConvertToHypertablesAsync(setup, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(setup, null, ct);
            await Exec(setup, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            await Exec(setup, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);

            foreach (var server in new[] { ServerA, ServerB })
            {
                await using var register = new NpgsqlCommand(
                    "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ($1, $2, $2, TRUE, 15, $3, $3)", setup);
                register.Parameters.AddWithValue(server);
                register.Parameters.AddWithValue("rollup-buffers-8552-" + server);
                register.Parameters.AddWithValue(end);
                await register.ExecuteNonQueryAsync(ct);
            }

            await SeedAsync(setup, end, ct);

            /* The whole hours before the current one are materialized, as the store's refresh policy keeps them. */
            var wholeHours = new DateTime(end.Year, end.Month, end.Day, end.Hour, 0, 0, DateTimeKind.Unspecified);
            await using (var refresh = new NpgsqlCommand(
                $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStatsIoHourlyView}'::regclass, $1::timestamp, $2::timestamp)", setup))
            {
                refresh.Parameters.AddWithValue(end.AddHours(-HoursSeeded - 24));
                refresh.Parameters.AddWithValue(wholeHours);
                await refresh.ExecuteNonQueryAsync(ct);
            }

            /* The index the service's tuning pass creates: the read's text and plan lookups probe it, and a store without it reads raw. */
            await Exec(setup, "CREATE INDEX IF NOT EXISTS idx_query_stats_server_hash_time ON collect.query_stats (server_id, query_hash, collection_time DESC)", ct);
            await Exec(setup, "ANALYZE collect.query_stats", ct);
            await using (var size = new NpgsqlCommand("SELECT hypertable_size('collect.query_stats') / 8192.0, (SELECT oid FROM pg_database WHERE datname = current_database())::bigint", setup))
            await using (var reader = await size.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                tablePages = Convert.ToDouble(reader.GetValue(0));
                dbId = Convert.ToInt64(reader.GetValue(1));
            }
        }

        var bodySucceeded = false;
        try
        {
            await using var counter = new NpgsqlConnection(scratch.ConnectionString);
            await counter.OpenAsync(ct);

            List<ExpensiveQuery> rows = new();
            long buffers = 0;
            var run = await PgStatementTable.RunAsync(counter, ct, async pass =>
            {
                await using (var reset = new NpgsqlCommand("SELECT pg_stat_statements_reset(0, $1, 0)", counter))
                {
                    reset.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
                    await reset.ExecuteNonQueryAsync(ct);
                }

                /* A data source of its own: the rollup availability and coverage probe is cached per data source, and its statements belong to the read. */
                await using (var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString))
                {
                    rows = await DarlingFinOpsOptimizationReader.GetExpensiveQueriesAsync(dataSource, ServerA, cutoff, TopN, 120, ct);
                }

                await pass.BeforeReadAsync(ct);

                await using var sum = new NpgsqlCommand(
                    "SELECT COALESCE(SUM(shared_blks_hit + shared_blks_read), 0)::bigint FROM pg_stat_statements WHERE dbid = $1 AND query NOT LIKE '%pg_stat_statements%'", counter);
                sum.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
                buffers = (long)(await sum.ExecuteScalarAsync(ct))!;
            });
            PgStatementTable.AssertNoPassInside(run);

            Assert.Equal(TopN, rows.Count);
            Assert.True(
                buffers < tablePages * MaxShareOfTablePages,
                $"The Optimization read touched {buffers:N0} buffers, {buffers / tablePages:P1} of the raw table's {tablePages:N0} pages: it must take the whole hours of the window from the hourly rollup (limit {MaxShareOfTablePages:P0}).");

            /* The ranking and the CPU sums are the raw sums the read replaces: the same statements in the same order, to the millisecond. */
            var expected = new List<(string Hash, long CpuMs)>();
            await using (var raw = new NpgsqlCommand(@"
SELECT query_hash, SUM(delta_worker_time) / 1000.0
FROM collect.query_stats
WHERE server_id = $1 AND collection_time >= $2 AND delta_worker_time > 0 AND sample_interval_seconds IS DISTINCT FROM 0
GROUP BY query_hash
ORDER BY SUM(delta_worker_time) DESC, query_hash
LIMIT $3", counter))
            {
                raw.Parameters.AddWithValue(ServerA);
                raw.Parameters.AddWithValue(cutoff);
                raw.Parameters.AddWithValue(TopN);
                await using var reader = await raw.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    expected.Add((reader.GetString(0), Convert.ToInt64(reader.GetValue(1))));
                }
            }

            Assert.Equal(expected.Select(e => e.Hash), rows.Select(r => r.QueryHash));
            Assert.Equal(expected.Select(e => e.CpuMs), rows.Select(r => r.TotalCpuMs));
            Assert.All(rows, r =>
            {
                Assert.False(string.IsNullOrEmpty(r.FullQueryText));
                Assert.NotNull(r.QueryPlanXml);
            });

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /* Two servers, one sample every two minutes for 36 hours, 150 of each server's 300 statements per sample, in collection order so
       the two servers' rows share every heap page, as they do on a store with many servers. Texts and plans sit in the dimensions by digest. */
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        await Exec(connection, $@"
CREATE TEMP TABLE seedstmt AS
SELECT s AS server_id, k AS stmt, 'Db' || (k % 10) AS db,
       '0x0200000' || substr(md5('h' || s || '-' || (k / 2)), 1, 57) AS sql_handle,
       '0x' || substr(md5('q' || s || '-' || k), 1, 16) AS query_hash,
       sha256(convert_to('text-' || s || '-' || k, 'UTF8')) AS text_digest,
       sha256(convert_to('plan-' || s || '-' || k, 'UTF8')) AS plan_digest
FROM (VALUES ({ServerA}), ({ServerB})) AS srv(s), generate_series(0, {StatementsPerServer - 1}) AS k;

INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
SELECT text_digest, 'SELECT /* seed ' || server_id || '-' || stmt || ' */ ' || repeat('col' || stmt || ', ', 40) || ' FROM dbo.t' || stmt, now() AT TIME ZONE 'utc'
FROM seedstmt;

INSERT INTO collect.query_plan_dim (digest, query_plan_xml, query_plan_gz, last_seen)
SELECT plan_digest, '<ShowPlanXML/>', NULL, now() AT TIME ZONE 'utc'
FROM seedstmt;

CREATE TEMP SEQUENCE seed_cid;", ct);

        var start = end.AddHours(-HoursSeeded);
        for (var hour = start; hour < end; hour = hour.AddHours(1))
        {
            await using var batch = new NpgsqlCommand($@"
INSERT INTO collect.query_stats (
    collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
    creation_time, last_execution_time, execution_count, total_worker_time, total_elapsed_time, total_logical_reads,
    total_logical_writes, total_physical_reads, total_clr_time, total_rows, total_spills,
    min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time, min_physical_reads, max_physical_reads,
    min_rows, max_rows, min_dop, max_dop, min_grant_kb, max_grant_kb, min_used_grant_kb, max_used_grant_kb,
    min_ideal_grant_kb, max_ideal_grant_kb, min_reserved_threads, max_reserved_threads, min_used_threads, max_used_threads,
    min_spills, max_spills, sql_handle, plan_handle,
    delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes,
    delta_physical_reads, delta_rows, delta_spills, plan_generation_num, sample_interval_seconds, host_object_name,
    statement_start_offset, statement_end_offset, query_text_digest, query_plan_digest)
SELECT
    nextval('seed_cid'), m.ct, st.server_id, 'rollup-buffers-8552-' || st.server_id, st.db, st.query_hash, '0x' || substr(md5('p' || st.stmt), 1, 16),
    m.ct - interval '3 days', m.ct - interval '10 seconds', 1000 + m.i, 5000000 + m.i * 1000, 9000000 + m.i * 1000, 800000 + m.i * 100,
    100, 5000, 0, 20000, 0,
    10, 90000, 10, 99000, 0, 40, 0, 500, 1, 4, 1024, 8192, 512, 4096,
    1024, 8192, 1, 4, 1, 4,
    0, 0, st.sql_handle, '0x06000' || substr(md5('ph' || st.stmt), 1, 57),
    1 + (r.v * 20)::int, 1 + (r.v * 50000)::bigint, (r.v * 80000)::bigint, (r.v * 100000)::bigint, (r.v * 100)::bigint,
    (r.v * 50)::bigint, (r.v * 500)::bigint, 0, 1, {SampleMinutes * 60}, 'dbo.seed_proc_' || (st.stmt % 50),
    (st.stmt % 9) * 120, (st.stmt % 9) * 120 + 119, st.text_digest, st.plan_digest
FROM (SELECT g AS i, $1::timestamp + g * interval '{SampleMinutes} minutes' AS ct FROM generate_series(0, {60 / SampleMinutes - 1}) AS g) AS m
CROSS JOIN (VALUES ({ServerA}), ({ServerB})) AS srv(sid)
CROSS JOIN generate_series(0, {StatementsPerSample - 1}) AS k
JOIN seedstmt st ON st.server_id = srv.sid AND st.stmt = ((k * 2) + (EXTRACT(EPOCH FROM m.ct)::bigint / {SampleMinutes * 60})::int) % {StatementsPerServer}
CROSS JOIN LATERAL (SELECT random() AS v) AS r
WHERE m.ct < $2::timestamp
ORDER BY m.ct, srv.sid, k", connection);
            batch.Parameters.AddWithValue(hour);
            batch.Parameters.AddWithValue(end);
            await batch.ExecuteNonQueryAsync(ct);
        }
    }
}
