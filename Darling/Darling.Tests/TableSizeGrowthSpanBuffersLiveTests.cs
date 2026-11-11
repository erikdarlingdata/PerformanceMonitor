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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #8570: <c>get_table_index_sizes</c> took 7 seconds on a first call because the statement's <c>span</c> step took the
/// store's newest, oldest and four cutoff-window snapshot times with <c>MIN</c>/<c>MAX</c> aggregates over every
/// <c>index_object_stats</c> row of the server, so it read every index row of every retained snapshot to find six
/// timestamps. The statement now asks the (server_id, collection_time) index for each of them, one descent each.
/// This test seeds a store with many snapshots of a few thousand index rows each, checks the statement's answer
/// against the snapshot times the seed knows, and bounds the pages the statement reads (<see cref="ExplainBuffers.DataPages(object?)"/>):
/// the whole statement reads about 110 pages where the aggregate form read about 690.
/// </summary>
public sealed class TableSizeGrowthSpanBuffersLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live table-size buffer test (it mints its own scratch database).";
    private const int ServerId = 8570;
    private const string ServerName = "span-8570";
    private const int Snapshots = 300;
    private const int RowsPerSnapshot = 800;

    /* The newest snapshot, then one a day back. */
    private static readonly DateTime Latest = new(2026, 11, 9, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Read = new(2026, 11, 9, 14, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task TheStatement_FindsItsSnapshotsByIndexProbes_NotByAggregatingEveryIndexRow()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        /* The service's own order, so the plan is production's: the hypertable conversion where TimescaleDB is present, then the
           tuning list. */
        if (scratch.TimescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }

        await PgTableTuning.ApplyAsync(connection, null, ct);

        await using (var seed = new NpgsqlCommand(
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
              SELECT 880000000 + row_number() OVER (), $1::timestamp - make_interval(days => s.age), $2, $3, 'Hist', 'dbo', t.n, 'T' || t.n, 1, 'PK_T' || t.n, 'CLUSTERED',
                     100 + (300 - s.age), 99, 1000, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0
              FROM generate_series(0, $4 - 1) AS s(age)
              CROSS JOIN generate_series(1, $5) AS t(n)", connection))
        {
            seed.Parameters.AddWithValue(Latest);
            seed.Parameters.AddWithValue(ServerId);
            seed.Parameters.AddWithValue(ServerName);
            seed.Parameters.AddWithValue(Snapshots);
            seed.Parameters.AddWithValue(RowsPerSnapshot);
            await seed.ExecuteNonQueryAsync(ct);
        }

        /* VACUUM sets the visibility map, which production's autovacuum keeps set: an index-only walk skips the heap. */
        await using (var analyze = new NpgsqlCommand("VACUUM (ANALYZE) index_object_stats", connection))
        {
            await analyze.ExecuteNonQueryAsync(ct);
        }

        var cutoff7d = Read.AddDays(-7);
        var cutoff30d = Read.AddDays(-30);

        /* The answer: Latest is age 0, the 7-day cutoff (2 November 14:00) is 8 hours after the 2 November 06:00 snapshot (age 7)
           and 16 hours before the 3 November one, so the nearer, age 7, is the baseline; the 30-day cutoff the same way (age 30).
           The oldest snapshot is age 119. */
        await using var postgres = NpgsqlDataSource.Create(connectionString);
        var rows = await DarlingTableSizeGrowthReader.GetAsync(
            postgres, ServerId, cutoff7d, cutoff30d, RowsPerSnapshot + 1, DatabaseFilter.All, commandTimeoutSeconds: 60, ct);
        Assert.Equal(RowsPerSnapshot, rows.Count);
        var first = rows[0];
        Assert.Equal(Latest, first.LatestSnapshotTime);
        Assert.Equal(Latest.AddDays(-7), first.Snapshot7dTime);
        Assert.Equal(Latest.AddDays(-30), first.Snapshot30dTime);
        Assert.Equal(Latest.AddDays(-(Snapshots - 1)), first.EarliestSnapshotTime);
        Assert.Equal(Snapshots - 1, first.DaysOfData);

        /* The pages the whole statement reads, as it runs in production (two timestamps and the tolerance as parameters). The root node's
           buffer total is not used: it also counts the catalog buffers a backend reads to rebuild its cache of the 300 chunks, and a cache
           reset from another test creating a database on the same server took it from 176 to 4,402 over the same plan (CI counted 4,430). */
        var pages = await StatementPagesAsync(connection, cutoff7d, cutoff30d, ct);

        /* Measured on the seed (240,000 index rows over 300 snapshots, all in cache): 688 pages with the aggregate form of span, 108 to 112
           with the probes on every run, warm or cold. 300 sits between, and grows with the retained snapshots only on the old form. */
        Assert.True(pages < 300, $"the statement read {pages} pages");
    }

    private static async Task<long> StatementPagesAsync(NpgsqlConnection connection, DateTime cutoff7d, DateTime cutoff30d, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingTableSizeGrowthReader.Sql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(cutoff7d);
        command.Parameters.AddWithValue(cutoff30d);
        command.Parameters.AddWithValue(RowsPerSnapshot + 1);
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.AddWithValue(GrowthBaselineReach.Tolerance.TotalSeconds);
        return ExplainBuffers.DataPages(await command.ExecuteScalarAsync(ct));
    }
}
