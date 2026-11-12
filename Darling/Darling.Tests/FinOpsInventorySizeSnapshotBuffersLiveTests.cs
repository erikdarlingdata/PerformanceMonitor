/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570 LIVE: the Server Inventory fleet read (<see cref="DarlingFinOpsInventoryReader.ServerMetricsSql"/>, shared by the
/// <c>get_finops_inventory</c> tool and the Viewer's inventory grid) took 12 seconds on a 42-server store because its two reads of the
/// newest database-size snapshot were planned as hash joins over EVERY chunk of <c>database_size_stats</c>, so the read grew with the
/// table's history instead of with one snapshot per server. The bound here is a buffer count, not a time: the statement's executed
/// plan must touch a small fraction of the pages the table's history occupies.
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class FinOpsInventorySizeSnapshotBuffersLiveTests
{
    private const int Servers = 3;
    private const int DatabasesPerServer = 60;
    private const int HistoryDays = 10;

    [Fact]
    public async Task ServerMetricsSql_ReadsTheNewestSizeSnapshotPerServer_NotTheTableHistory()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8570 buffer test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live #8570 test needs TimescaleDB: the history it measures is a hypertable's chunks.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        await ExecAsync(connection,
            "INSERT INTO servers (server_id, server_name) SELECT s, 'size-buffers-' || s FROM generate_series(1, $1) AS s", ct, Servers);

        /* One snapshot per server per hour for ten days, sixty databases each: ten one-day chunks, one snapshot of which (per server) the read needs. */
        var newest = new DateTime(2026, 6, 20, 12, 0, 0, DateTimeKind.Unspecified);
        await ExecAsync(connection,
            @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb, used_size_mb)
              SELECT (s::bigint * 100000000) + (h::bigint * 1000) + d, $1 - make_interval(hours => h), s, 'size-buffers-' || s, 'db' || d, 1000, 400
              FROM generate_series(1, $2) AS s, generate_series(0, $3 * 24 - 1) AS h, generate_series(1, $4) AS d",
            ct, newest, Servers, HistoryDays, DatabasesPerServer);
        await ExecAsync(connection, "ANALYZE database_size_stats", ct);

        var historyRows = (long)(await ScalarAsync(connection, "SELECT count(*) FROM database_size_stats", ct))!;
        Assert.Equal((long)Servers * HistoryDays * 24 * DatabasesPerServer, historyRows);

        var now = newest.AddMinutes(10);
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingFinOpsInventoryReader.ServerMetricsSql, connection);
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddHours(-24), NpgsqlDbType = NpgsqlDbType.Timestamp });
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddDays(-7), NpgsqlDbType = NpgsqlDbType.Timestamp });
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.Date.AddDays(-7), NpgsqlDbType = NpgsqlDbType.Timestamp });
        explain.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 7 });
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.Date, NpgsqlDbType = NpgsqlDbType.Timestamp });

        /* The pages the scans read, not the root node's buffer total: the total also counts the catalog buffers a backend reads to rebuild its
           cache of the ten chunks, and a cache reset from another test creating a database on the same server brings those back (#8570). */
        var buffers = ExplainBuffers.DataPages(await explain.ExecuteScalarAsync(ct));

        /* The table's history is about 14,400 rows per server day-chunk set: reading all of it twice (the old hash joins) costs 513 buffers on the
           old statement. One snapshot per server (60 rows each) through the (server_id, collection_time) index is 46. */
        Assert.True(buffers < 200,
            $"The inventory fleet read touched {buffers} pages over {historyRows} history rows; reading only the newest snapshot per server stays far under that.");
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}


