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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4972: the legacy-row check (<see cref="QueryStoreIntervalWide.HasLegacyRowSql"/>, clause 6 of
/// <c>ReadsTableAsync</c>) gets an index read of the partial index
/// <see cref="PgTableTuning.LegacyRowIndexName"/>, which the start-path tuning pass builds. Own scratch
/// database per test.
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres. */
public sealed class QueryStoreLegacyRowIndexLiveTests
{
    private const int ServerA = 4972001;
    private const int ServerB = 4972002;

    private static readonly Regex NodeLabel = new(@"^\s*(->\s*)?(?<text>.+)$", RegexOptions.Compiled);

    /// <summary>
    /// Two servers' interval-stamped rows over three days, the older chunks compressed where TimescaleDB is
    /// present. The shipped check, EXPLAINed with its real bound parameter types, reads the partial index
    /// (Index Only Scan, Index Scan or Bitmap Index Scan) for every uncompressed chunk and never filters a heap scan on the NULL test; and it
    /// answers false until a NULL-start row lands for one server.
    /// </summary>
    [Fact]
    public async Task TheShippedCheck_PlansOnThePartialIndex_AndAnswersExactly()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the legacy-row index live test.");

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
        }

        await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);

        var rawNow = DateTime.UtcNow;
        var utcNow = DateTime.SpecifyKind(new DateTime(rawNow.Ticks - (rawNow.Ticks % 10)), DateTimeKind.Unspecified);
        var windowStart = utcNow.AddDays(-3);

        /* Per-pass contiguous batches, one server after the other, every 30 minutes for three days. */
        for (var pass = 143; pass >= 0; pass--)
        {
            var collectionTime = utcNow.AddMinutes(-2 - pass * 30);
            await SeedPassAsync(connection, ServerA, collectionTime, 30, ct);
            await SeedPassAsync(connection, ServerB, collectionTime, 30, ct);
        }

        var compressed = 0;
        if (timescaleEnabled)
        {
            compressed = await CompressOlderChunksAsync(connection, ct);
        }

        await Exec(connection, "ANALYZE collect.query_store_stats", ct);

        var plan = await ExplainShippedCheckAsync(connection, ServerA, windowStart, utcNow, ct);
        AssertPlanUsesThePartialIndex(plan, timescaleEnabled, compressed);

        Assert.False(await RunShippedCheckAsync(connection, ServerA, windowStart, utcNow, ct));
        Assert.False(await RunShippedCheckAsync(connection, ServerB, windowStart, utcNow, ct));

        await InsertNullStartRowAsync(connection, ServerA, utcNow.AddMinutes(-10), ct);
        Assert.True(await RunShippedCheckAsync(connection, ServerA, windowStart, utcNow, ct));
        Assert.False(await RunShippedCheckAsync(connection, ServerB, windowStart, utcNow, ct));
    }

    /// <summary>
    /// The first start-path pass leaves the index with its exact definition and valid; a second pass issues
    /// no CREATE (a writer's ROW EXCLUSIVE would make any CREATE fail under the lock_timeout and drop it
    /// from the count); and with the index dropped, the hourly pass does not build it.
    /// </summary>
    [Fact]
    public async Task TheStartPath_BuildsTheIndexOnce_AndTheHourlyPassNeverBuildsIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the legacy-row index live test.");

        var ct = TestContext.Current.CancellationToken;
        PgTableTuning.ResetHourlyMissingWarnings();
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        await PgTableTuning.ApplyAsync(body, NullLogger.Instance, ct);

        await using (var def = new NpgsqlCommand(
            "SELECT i.indisvalid, pg_get_indexdef(i.indexrelid) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
            + "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relname = $1", body))
        {
            def.Parameters.AddWithValue(PgTableTuning.LegacyRowIndexName);
            await using var reader = await def.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct), "the start path should have built the index");
            Assert.True(reader.GetBoolean(0), "the index should be valid");
            var text = reader.GetString(1);
            Assert.Contains("ON collect.query_store_stats", text, StringComparison.Ordinal);
            Assert.Contains("(server_id, collection_time)", text, StringComparison.Ordinal);
            Assert.Contains("WHERE (interval_start_time_utc IS NULL)", text, StringComparison.Ordinal);
        }

        await using (var holder = new NpgsqlConnection(scratch.ConnectionString))
        {
            await holder.OpenAsync(ct);
            await Exec(holder, "BEGIN", ct);
            try
            {
                await Exec(holder, "LOCK TABLE collect.query_stats, collect.query_store_stats, collect.store_metrics IN ROW EXCLUSIVE MODE", ct);
                await Exec(body, "SET lock_timeout = '2s'", ct);

                var applied = await PgTableTuning.ApplyAsync(body, NullLogger.Instance, ct);

                Assert.Equal(PgTableTuning.Statements.Count, applied);
            }
            finally
            {
                await Exec(holder, "ROLLBACK", CancellationToken.None);
            }
        }

        await Exec(body, "RESET lock_timeout", ct);
        await Exec(body, "DROP INDEX collect." + PgTableTuning.LegacyRowIndexName, ct);
        await PgTableTuning.ApplyAsync(body, NullLogger.Instance, hourly: true, ct);
        await using var exists = new NpgsqlCommand("SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", body);
        exists.Parameters.AddWithValue(PgTableTuning.LegacyRowIndexName);
        Assert.Null(await exists.ExecuteScalarAsync(ct));
    }

    private static void AssertPlanUsesThePartialIndex(string plan, bool timescaleEnabled, int compressedChunks)
    {
        var lines = plan.ReplaceLineEndings("\n").Split('\n', StringSplitOptions.RemoveEmptyEntries);
        var nodes = new List<(string Label, List<string> Details)>();
        foreach (var line in lines)
        {
            var text = NodeLabel.Match(line).Groups["text"].Value.TrimEnd();
            var isDetail = text.Contains(": ", StringComparison.Ordinal) && !line.TrimStart().StartsWith("->", StringComparison.Ordinal)
                && nodes.Count > 0;
            if (isDetail)
            {
                nodes[^1].Details.Add(text);
            }
            else
            {
                nodes.Add((text, new List<string>()));
            }
        }

        var scans = nodes.Where(n => n.Label.Contains("Scan", StringComparison.Ordinal)
            && (n.Label.Contains("query_store_stats", StringComparison.Ordinal)
                || n.Label.Contains("_hyper_", StringComparison.Ordinal))
            && !n.Label.Contains("_compressed", StringComparison.Ordinal)).ToList();
        Assert.NotEmpty(scans);

        /* Which index read the planner picks depends on visibility-map state and the platform; any read of
           the partial index is the fix. A Bitmap Heap Scan counts when its child is a Bitmap Index Scan on it. */
        var idx = PgTableTuning.LegacyRowIndexName;
        /* Chunk indexes are renamed with the chunk's prefix, so the label carries the name, not starts with it. */
        var indexReads = scans.Where(n => n.Label.Contains(idx, StringComparison.Ordinal)
            && (n.Label.StartsWith("Index Only Scan using ", StringComparison.Ordinal)
                || n.Label.StartsWith("Index Scan using ", StringComparison.Ordinal)
                || n.Label.StartsWith("Bitmap Index Scan on ", StringComparison.Ordinal))).ToList();
        Assert.NotEmpty(indexReads);

        foreach (var scan in scans)
        {
            var columnar = scan.Label.Contains("ColumnarScan", StringComparison.Ordinal);
            var isIndexRead = indexReads.Contains(scan);
            var isBitmapHeap = scan.Label.StartsWith("Bitmap Heap Scan on ", StringComparison.Ordinal)
                && IsFollowedByBitmapIndexScan(nodes, scan, idx);
            Assert.True(columnar || isIndexRead || isBitmapHeap, "a chunk is read by a heap scan instead of the partial index:\n" + plan);
            /* A row Filter carrying the NULL test is the heap-filtered shape this index replaces. The columnar
               scan's vectorized filter is a different line label and is fine. */
            Assert.DoesNotContain(scan.Details, d => d.StartsWith("Filter:", StringComparison.Ordinal)
                && d.Contains("interval_start_time_utc IS NULL", StringComparison.Ordinal));
        }

        if (timescaleEnabled && compressedChunks > 0)
        {
            Assert.Contains(scans, n => n.Label.Contains("ColumnarScan", StringComparison.Ordinal));
        }
    }

    private static bool IsFollowedByBitmapIndexScan(List<(string Label, List<string> Details)> nodes, (string Label, List<string> Details) heap, string idx)
    {
        var at = nodes.IndexOf(heap);
        return at >= 0 && at + 1 < nodes.Count
            && nodes[at + 1].Label.StartsWith("Bitmap Index Scan on ", StringComparison.Ordinal)
            && nodes[at + 1].Label.Contains(idx, StringComparison.Ordinal);
    }

    private static async Task<int> CompressOlderChunksAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await Exec(connection, "ALTER TABLE collect.query_store_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);
        var chunks = new List<string>();
        await using (var list = new NpgsqlCommand(
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks "
            + "WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_store_stats' AND NOT is_compressed "
            + "ORDER BY range_end DESC OFFSET 1", connection))
        await using (var reader = await list.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        var done = 0;
        foreach (var chunk in chunks)
        {
            await using var compress = new NpgsqlCommand("SELECT compress_chunk($1::regclass, if_not_compressed => true)", connection);
            compress.Parameters.AddWithValue(chunk);
            await compress.ExecuteNonQueryAsync(ct);
            done++;
        }

        return done;
    }

    private static async Task SeedPassAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, int rows, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(
            "INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, "
            + "execution_count, avg_duration_us, interval_start_time_utc) "
            + "SELECT $1, $2, $3, 'legacy-row-index-e2e', 'db_' || (g % 4), 1000 + g, (1000 + g) * 10, 10 + g, 500 + g, $2 "
            + "FROM generate_series(1, $4) AS g", connection);
        insert.Parameters.AddWithValue(1L);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(rows);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertNullStartRowAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(
            "INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, "
            + "execution_count, avg_duration_us) VALUES ($1, $2, $3, 'legacy-row-index-e2e', 'db_0', 1, 10, 1, 1)", connection);
        insert.Parameters.AddWithValue(1L);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
        insert.Parameters.AddWithValue(serverId);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static NpgsqlCommand BindShippedCheck(string sql, NpgsqlConnection connection, int serverId, DateTime start, DateTime end)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
        return command;
    }

    private static async Task<bool> RunShippedCheckAsync(NpgsqlConnection connection, int serverId, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = BindShippedCheck(QueryStoreIntervalWide.HasLegacyRowSql, connection, serverId, start, end);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<string> ExplainShippedCheckAsync(NpgsqlConnection connection, int serverId, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = BindShippedCheck("EXPLAIN (COSTS OFF) " + QueryStoreIntervalWide.HasLegacyRowSql, connection, serverId, start, end);
        var lines = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(reader.GetString(0));
        }

        return string.Join("\n", lines);
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
