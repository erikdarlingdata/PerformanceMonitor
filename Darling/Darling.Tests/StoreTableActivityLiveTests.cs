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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9413: the hourly per-table activity capture and its read. Two captures an hour apart give the hour's differences, a hypertable with compressed
/// chunks is one row (its chunks and compressed chunks roll up to it and none is a row of its own), the first capture writes no rows, a counter that
/// fell between captures (a statistics reset) is counted from the reset, and the read ranks the tables by rows written and by blocks read.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class StoreTableActivityLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipMessage = "Set DARLING_TEST_PG to a Postgres connection string to run the #9413 live tests.";

    private const string SeedSql = @"
SELECT _timescaledb_functions.stop_background_workers();
CREATE SCHEMA zz9413;
CREATE TABLE zz9413.pt (id integer PRIMARY KEY, v text);
CREATE TABLE zz9413.ht (ts timestamptz NOT NULL, dev integer NOT NULL, v double precision);
SELECT create_hypertable('zz9413.ht', 'ts', chunk_time_interval => interval '1 day');
ALTER TABLE zz9413.ht SET (timescaledb.compress, timescaledb.compress_segmentby = 'dev');
INSERT INTO zz9413.ht SELECT now() - (d || ' days')::interval, d, 1 FROM generate_series(0, 4) AS d;
SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('zz9413.ht', older_than => now() - interval '1 day') AS c;
ANALYZE;";

    /* The writes and the flush of their counters share one command: PostgreSQL 15+ sends a backend's counters to the shared statistics up to a second
       after it goes idle, and pg_stat_force_next_flush() makes this transaction's end send them. */
    private static async Task ExecAsync(NpgsqlDataSource store, string sql, CancellationToken ct)
    {
        await using var command = store.CreateCommand(sql + ";SELECT pg_stat_force_next_flush();");
        command.CommandTimeout = 300;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> CaptureAsync(string connectionString, DateTime at, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        return await StoreTableActivity.CaptureAsync(connection, timescaleAvailable: true, at, logger: null, commandTimeoutSeconds: 120, ct);
    }

    private static async Task<List<Dictionary<string, object>>> ActivityAsync(NpgsqlDataSource store, DateTime at, CancellationToken ct)
    {
        var rows = new List<Dictionary<string, object>>();
        await using var command = store.CreateCommand("SELECT table_schema, table_name, table_kind, interval_seconds, relations_active, relations_scanned, tup_inserted, tup_updated, tup_deleted, heap_blks_read + heap_blks_hit + idx_blks_read + idx_blks_hit AS blocks FROM collect.store_table_activity WHERE metric_time = $1");
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(Enumerable.Range(0, reader.FieldCount).ToDictionary(i => reader.GetName(i), i => reader.GetValue(i)));
        }

        return rows;
    }

    [Fact]
    public async Task TwoCaptures_GiveTheHoursDifferences_AndCompressedChunksRollUpToTheirHypertable()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var bodySucceeded = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(store, SeedSql, ct);
            var t0 = new DateTime(2026, 11, 20, 10, 0, 0, DateTimeKind.Utc);
            var t1 = t0.AddHours(1);
            var t2 = t0.AddHours(2);

            // The first capture has nothing to compare with: it writes the baseline and no activity rows.
            Assert.Equal(0, await CaptureAsync(scratch.ConnectionString, t0, ct));
            Assert.Empty(await ActivityAsync(store, t0, ct));

            // The hour: 10 rows into the plain table, 5 into the hypertable, and a read of every chunk of the hypertable, compressed ones included.
            await ExecAsync(store, "INSERT INTO zz9413.pt SELECT g, 'x' FROM generate_series(1, 10) AS g", ct);
            await ExecAsync(store, "INSERT INTO zz9413.ht SELECT now(), 9, 2 FROM generate_series(1, 5)", ct);
            await ExecAsync(store, "SELECT count(*) FROM zz9413.ht", ct);
            Assert.True(await CaptureAsync(scratch.ConnectionString, t1, ct) > 0);

            var hour = await ActivityAsync(store, t1, ct);
            var plain = Assert.Single(hour, r => (string)r["table_name"] == "pt");
            Assert.Equal("table", (string)plain["table_kind"]);
            Assert.Equal(10L, (long)plain["tup_inserted"]);
            Assert.Equal(3600, (int)plain["interval_seconds"]);

            var hyper = Assert.Single(hour, r => (string)r["table_name"] == "ht");
            Assert.Equal("hypertable", (string)hyper["table_kind"]);
            Assert.Equal(5L, (long)hyper["tup_inserted"]);
            Assert.True((long)hyper["blocks"] > 0);
            Assert.True((int)hyper["relations_scanned"] >= 3, $"relations scanned: {hyper["relations_scanned"]}");
            Assert.True((int)hyper["relations_active"] >= (int)hyper["relations_scanned"]);
            Assert.DoesNotContain(hour, r => (string)r["table_schema"] == "_timescaledb_internal");

            // The read ranks the hour by rows written (the plain table wrote 10, the hypertable 5) and by blocks read.
            using (var byWrites = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, 2, "rows_written", null, 10, null, ct, t1)))
            {
                var tables = byWrites.RootElement.GetProperty("tables").EnumerateArray().Select(t => t.GetProperty("table_name").GetString()).ToList();
                Assert.True(tables.IndexOf("pt") >= 0 && tables.IndexOf("pt") < tables.IndexOf("ht"), string.Join(",", tables));
            }

            using (var byBlocks = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, 2, "blocks_read", null, 10, null, ct, t1)))
            {
                var first = byBlocks.RootElement.GetProperty("tables").EnumerateArray().First();
                Assert.True(first.GetProperty("blocks_read").GetInt64() >= 0);
                Assert.Equal("blocks_read", byBlocks.RootElement.GetProperty("rank_by").GetString());
            }

            using (var series = JsonDocument.Parse(await DarlingStoreTableActivityRead.GetAsync(store, 2, null, "zz9413.ht", 10, null, ct, t1)))
            {
                var point = Assert.Single(series.RootElement.GetProperty("series").EnumerateArray());
                Assert.Equal(5L, point.GetProperty("tuples_inserted").GetInt64());
            }

            // A statistics reset between captures: the counters read below their baseline and count from the reset.
            await ExecAsync(store, "SELECT pg_stat_reset()", ct);
            await ExecAsync(store, "INSERT INTO zz9413.pt SELECT g, 'y' FROM generate_series(100, 102) AS g", ct);
            Assert.True(await CaptureAsync(scratch.ConnectionString, t2, ct) > 0);
            var afterReset = Assert.Single(await ActivityAsync(store, t2, ct), r => (string)r["table_name"] == "pt");
            Assert.Equal(3L, (long)afterReset["tup_inserted"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
