/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5072: <see cref="DarlingFinOpsIndexAnalysisReader.IndexObjectStatsLatestSql"/> returns each database's newest
/// snapshot. An index only an older cycle carries was dropped and is not shown; a database that left collection
/// scope still shows its last snapshot; another server's rows never leak in; a same-instant re-run resolves to the
/// newest collection_id. Fixed past anchors, never the wall clock. With TimescaleDB every chunk but the newest is
/// compressed, so the anchor runs over compressed chunks too; without it the same facts run on a plain table.
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres. */
public sealed class FinOpsIndexAnalysisLatestSnapshotLiveTests
{
    private const int ServerA = 5072001;
    private const int ServerB = 5072002;
    private const int DbX = 5;
    private const int DbY = 6;

    [Fact]
    public async Task EachDatabasesNewestSnapshot_DropsDroppedIndexes_KeepsAScopedOutDatabase_AndNeverLeaksAServer()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the latest-snapshot live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        if (timescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            await Exec(connection, "SELECT set_chunk_time_interval('collect.index_object_stats', INTERVAL '1 day')", ct);
        }

        var t = new DateTime(2026, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);

        /* Server A, database X: i1 in every cycle, i2 dropped after T-1d, i1 written twice at T. */
        await Insert(connection, ServerA, DbX, 1, "i1", t.AddDays(-2), 1, 10, ct);
        await Insert(connection, ServerA, DbX, 2, "i2", t.AddDays(-2), 2, 20, ct);
        await Insert(connection, ServerA, DbX, 1, "i1", t.AddDays(-1), 3, 11, ct);
        await Insert(connection, ServerA, DbX, 2, "i2", t.AddDays(-1), 4, 21, ct);
        await Insert(connection, ServerA, DbX, 1, "i1", t, 5, 12, ct);
        await Insert(connection, ServerA, DbX, 1, "i1", t, 6, 13, ct);
        /* Server A, database Y: left scope. j2 dropped before its last cycle. */
        await Insert(connection, ServerA, DbY, 1, "j1", t.AddDays(-5), 7, 30, ct);
        await Insert(connection, ServerA, DbY, 2, "j2", t.AddDays(-5), 8, 40, ct);
        await Insert(connection, ServerA, DbY, 1, "j1", t.AddDays(-3), 9, 31, ct);
        /* Server B: must not leak. */
        await Insert(connection, ServerB, DbX, 1, "b1", t, 10, 99, ct);
        await Insert(connection, ServerB, DbY, 1, "b2", t.AddDays(-4), 11, 98, ct);

        if (timescaleEnabled)
        {
            var compressed = await CompressAllButNewestAsync(connection, ct);
            Assert.True(compressed > 0, "no chunk was compressed; the compressed-chunk half did not run");
        }

        var a = await ReadAsync(connection, ServerA, ct);
        Assert.Equal(2, a.Count);
        Assert.Equal(12 + 1, a[(DbX, "i1")]);
        Assert.Equal(31, a[(DbY, "j1")]);
        Assert.DoesNotContain((DbX, "i2"), a.Keys);
        Assert.DoesNotContain((DbY, "j2"), a.Keys);

        var b = await ReadAsync(connection, ServerB, ct);
        Assert.Equal(2, b.Count);
        Assert.Equal(99, b[(DbX, "b1")]);
        Assert.Equal(98, b[(DbY, "b2")]);
    }

    private static async Task<Dictionary<(int Db, string Index), long>> ReadAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        var rows = new Dictionary<(int, string), long>();
        await using var command = new NpgsqlCommand(DarlingFinOpsIndexAnalysisReader.IndexObjectStatsLatestSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            /* Ordinals 1 (database_id), 6 (index_name) and 24 (user_seeks) of the read contract. */
            rows.Add((reader.GetInt32(1), reader.GetString(6)), reader.GetInt64(24));
        }

        return rows;
    }

    /* user_seeks carries the row's identity; the tie-break row (collection_id 6) is seeded with 13, but the answer
       asserts 12 + 1 so the expected value reads as "the higher collection_id's value". */
    private static async Task Insert(NpgsqlConnection c, int serverId, int dbId, int indexId, string name,
        DateTime at, long collectionId, long seeks, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, "
            + "schema_name, object_id, table_name, index_id, index_name, user_seeks, partition_count) "
            + "VALUES ($1, $2, $3, 'srv', 'db' || $4, $4, 'dbo', 100, 'T', $5, $6, $7, 1)", c);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = collectionId });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = at });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = dbId });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = indexId });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Varchar, Value = name });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = seeks });
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> CompressAllButNewestAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await Exec(connection, "ALTER TABLE collect.index_object_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);
        var chunks = new List<string>();
        await using (var list = new NpgsqlCommand(
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks "
            + "WHERE hypertable_schema = 'collect' AND hypertable_name = 'index_object_stats' AND NOT is_compressed "
            + "ORDER BY range_end DESC OFFSET 1", connection))
        await using (var reader = await list.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        foreach (var chunk in chunks)
        {
            await using var compress = new NpgsqlCommand("SELECT compress_chunk($1::regclass, if_not_compressed => true)", connection);
            compress.Parameters.AddWithValue(chunk);
            await compress.ExecuteNonQueryAsync(ct);
        }

        return chunks.Count;
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
