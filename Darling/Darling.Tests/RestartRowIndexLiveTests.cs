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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: the restart-row partial indexes on <c>collect.query_stats</c> and <c>collect.procedure_stats</c>
/// (<see cref="PgTableTuning.QueryStatsRestartRowIndexName"/>, <see cref="PgTableTuning.ProcedureStatsRestartRowIndexName"/>)
/// exist after the start-path tuning pass, hold only the restart rows, are read by
/// <see cref="IntervalRollupRestartRows"/> on an uncompressed chunk, and the pass is idempotent. Own scratch
/// database per test.
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres. */
public sealed class RestartRowIndexLiveTests
{
    private const int RestartEvery = 500;
    private const int RowsPerTable = 5000;

    [Fact]
    public async Task BothIndexes_ExistWithThePartialPredicate_AndTheSetupIsIdempotent()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the restart-row index live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await TuningStartPasses.ConvergeAsync(connection, ct);

        await AssertIndexAsync(connection, PgTableTuning.QueryStatsRestartRowIndexName, "collect.query_stats", ct);
        await AssertIndexAsync(connection, PgTableTuning.ProcedureStatsRestartRowIndexName, "collect.procedure_stats", ct);

        var oidsBefore = await IndexOidsAsync(connection, ct);
        var second = await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);
        Assert.True(second > 0, "the second pass should run");
        Assert.Equal(oidsBefore, await IndexOidsAsync(connection, ct));
        Assert.Equal(2, oidsBefore.Count);
    }

    [Fact]
    public async Task EachReader_PlansOnItsPartialIndex_OnAnUncompressedChunk_AndReturnsExactlyTheRestartRows()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the restart-row index live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.SkipUnless(timescaleEnabled, "TimescaleDB is not available; the chunk-level plan needs a hypertable.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        /* A fixed anchor, 23:50 UTC on a fixed past day, not the wall clock. The seed reaches back 7,000 seconds
           from this instant, and chunks are one day aligned to UTC midnight, so a seed ending at the real "now"
           straddles two daily chunks between 00:00 and about 02:00 UTC. On either chunk a Seq Scan can then
           cost less than the partial index, and the no-heap-scan assertion fails. Ending at 23:50 keeps the
           whole seed inside one day. */
        var utcNow = new DateTime(2026, 1, 15, 23, 50, 0, DateTimeKind.Unspecified);
        var windowStart = utcNow.AddHours(-2);

        await SeedAsync(connection, "collect.query_stats", utcNow, ct);
        await SeedAsync(connection, "collect.procedure_stats", utcNow, ct);

        /* The whole seed must sit in one daily chunk: no row may fall before the anchor's date, and the
           chunk must hold a realistic number of rows. */
        foreach (var table in new[] { "collect.query_stats", "collect.procedure_stats" })
        {
            await using var older = new NpgsqlCommand("SELECT count(*) FROM " + table + " WHERE collection_time < $1", connection);
            older.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = utcNow.Date });
            var olderRows = Convert.ToInt64(await older.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
            Assert.True(olderRows == 0, table + " has " + olderRows + " seeded rows before " + utcNow.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "; the seed must sit inside one daily chunk");

            await using var newest = new NpgsqlCommand("SELECT count(*) FROM " + table + " WHERE collection_time >= $1", connection);
            newest.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = utcNow.Date });
            var newestRows = Convert.ToInt64(await newest.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
            Assert.True(newestRows >= 1000, table + " holds only " + newestRows + " seeded rows; the seed must be a realistic day");
        }

        await TuningStartPasses.ConvergeAsync(connection, ct);
        await Exec(connection, "ANALYZE collect.query_stats", ct);
        await Exec(connection, "ANALYZE collect.procedure_stats", ct);

        var expected = RowsPerTable / RestartEvery;
        await AssertReaderAsync(connection, IntervalRollupRestartRows.QueryStatsRestartRowsSql, PgTableTuning.QueryStatsRestartRowIndexName, windowStart, utcNow, expected, ct);
        await AssertReaderAsync(connection, IntervalRollupRestartRows.ProcedureStatsRestartRowsSql, PgTableTuning.ProcedureStatsRestartRowIndexName, windowStart, utcNow, expected, ct);

        /* The end is exclusive: a window ending at the newest restart row's own time drops it. */
        var newestRestart = await ScalarAsync(connection, "SELECT max(collection_time) FROM collect.query_stats WHERE sample_interval_seconds = 0", ct);
        var clipped = await RunAsync(connection, IntervalRollupRestartRows.QueryStatsRestartRowsSql, windowStart, (DateTime)newestRestart!, ct);
        Assert.Equal(expected - 1, clipped);
    }

    [Fact]
    public void TheRestartRowReads_CarryPresenceOnly_NoWorkerTimeColumn()
    {
        foreach (var sql in new[] { IntervalRollupRestartRows.QueryStatsRestartRowsSql, IntervalRollupRestartRows.ProcedureStatsRestartRowsSql })
        {
            Assert.DoesNotContain("delta_worker_time", sql, StringComparison.Ordinal);
            Assert.Contains("r.collection_time, r.sample_interval_seconds", sql, StringComparison.Ordinal);
        }
    }

    private static async Task AssertIndexAsync(NpgsqlConnection connection, string name, string table, CancellationToken ct)
    {
        await using var def = new NpgsqlCommand(
            "SELECT i.indisvalid, pg_get_indexdef(i.indexrelid) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            + "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relname = $1", connection);
        def.Parameters.AddWithValue(name);
        await using var reader = await def.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the start path should have built " + name);
        Assert.True(reader.GetBoolean(0), name + " should be valid");
        var text = reader.GetString(1);
        Assert.Contains("ON " + table, text, StringComparison.Ordinal);
        Assert.Contains("(collection_time)", text, StringComparison.Ordinal);
        Assert.Contains("WHERE (sample_interval_seconds = 0)", text, StringComparison.Ordinal);
    }

    private static async Task<List<uint>> IndexOidsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var oids = new List<uint>();
        await using var cmd = new NpgsqlCommand(
            "SELECT c.oid::oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            + "WHERE n.nspname = 'collect' AND c.relname IN ($1, $2) ORDER BY c.relname", connection);
        cmd.Parameters.AddWithValue(PgTableTuning.QueryStatsRestartRowIndexName);
        cmd.Parameters.AddWithValue(PgTableTuning.ProcedureStatsRestartRowIndexName);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            oids.Add(reader.GetFieldValue<uint>(0));
        }

        return oids;
    }

    /// <summary>
    /// Mostly normal rows (sample_interval_seconds 60) with every 500th a restart row, all inside the last two
    /// hours, so they land in the newest chunk, which stays uncompressed.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, string table, DateTime utcNow, CancellationToken ct)
    {
        var extra = table.EndsWith("procedure_stats", StringComparison.Ordinal)
            ? "object_name, schema_name"
            : "sql_handle, database_name";
        var extraValues = table.EndsWith("procedure_stats", StringComparison.Ordinal)
            ? "'proc_' || (g % 50), 'dbo'"
            : "'0x' || md5(g::text), 'db_' || (g % 4)";
        await using var insert = new NpgsqlCommand(
            "INSERT INTO " + table + " (collection_id, collection_time, server_id, server_name, " + extra + ", delta_worker_time, sample_interval_seconds) "
            + "SELECT 1, $1 - make_interval(secs => (g % 7000)), 1 + g % 3, 'restart-row-index-e2eA', " + extraValues + ", g, "
            + "CASE WHEN g % " + RestartEvery.ToString(CultureInfo.InvariantCulture) + " = 0 THEN 0 ELSE 60 END "
            + "FROM generate_series(1, " + RowsPerTable.ToString(CultureInfo.InvariantCulture) + ") AS g", connection);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = utcNow });
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static NpgsqlCommand Bind(string sql, NpgsqlConnection connection, DateTime start, DateTime end)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
        return command;
    }

    private static async Task<int> RunAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = Bind(sql, connection, start, end);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = 0;
        while (await reader.ReadAsync(ct))
        {
            Assert.Equal(0, reader.GetInt32(reader.FieldCount - 1));
            rows++;
        }

        return rows;
    }

    private static async Task AssertReaderAsync(NpgsqlConnection connection, string sql, string indexName, DateTime start, DateTime end, int expectedRows, CancellationToken ct)
    {
        Assert.Equal(expectedRows, await RunAsync(connection, sql, start, end, ct));

        await using var explain = Bind("EXPLAIN (FORMAT JSON) " + sql, connection, start, end);
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        var reads = new List<(string NodeType, string Index)>();
        var heapScans = new List<string>();
        Collect(doc.RootElement[0].GetProperty("Plan"), reads, heapScans);

        Assert.True(reads.Count > 0, "no index read found in the plan:\n" + json);
        Assert.All(reads, r => Assert.Contains(indexName, r.Index, StringComparison.Ordinal));
        Assert.True(heapScans.Count == 0, "the plan scans the heap (" + string.Join(", ", heapScans) + "):\n" + json);
    }

    /// <summary>
    /// Nodes a plan of a restart-row read may contain: index reads, the bitmap heap fetch that follows a bitmap
    /// index scan, and the plumbing that joins chunks. Any other node, a Seq Scan, a Tid Scan, a Sample Scan or a
    /// Custom Scan that decompresses a chunk, fails the plan. This test seeds one uncompressed chunk, so it says
    /// nothing about compressed chunks.
    /// </summary>
    private static readonly HashSet<string> AllowedNodes = new(StringComparer.Ordinal)
    {
        "Index Scan", "Index Only Scan", "Bitmap Index Scan", "Bitmap Heap Scan", "Append", "Result", "Custom Scan (ChunkAppend)",
    };

    /// <summary>
    /// Walks the plan. Every node must be on the allow-list; every index node must read the chunk's copy of the
    /// partial index (the chunk index name carries the index name). A Bitmap Heap Scan is fine when its child is
    /// the Bitmap Index Scan on that index.
    /// </summary>
    private static void Collect(JsonElement node, List<(string, string)> reads, List<string> heapScans)
    {
        var type = node.GetProperty("Node Type").GetString()!;
        if (node.TryGetProperty("Custom Plan Provider", out var provider) && type == "Custom Scan")
        {
            type += " (" + provider.GetString() + ")";
        }

        if (!AllowedNodes.Contains(type))
        {
            heapScans.Add(type + " on " + (node.TryGetProperty("Relation Name", out var r) ? r.GetString() : "?"));
        }
        else if (type is "Index Scan" or "Index Only Scan" or "Bitmap Index Scan")
        {
            reads.Add((type, node.GetProperty("Index Name").GetString()!));
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Collect(child, reads, heapScans);
            }
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        return await cmd.ExecuteScalarAsync(ct);
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
