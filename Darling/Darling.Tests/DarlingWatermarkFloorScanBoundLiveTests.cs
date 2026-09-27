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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4469's runtime proof, driven ONLY through the product's own call path
/// (<see cref="DarlingWorker.ReadCollectorWatermarksAsync"/>, the exact overload the connect / reload seed
/// path calls): seeds <c>collection_log</c> across many days so most chunks are old and compressed, snapshots
/// every chunk relation's <c>pg_stat_user_tables</c> counters (heap AND, for a compressed chunk, its
/// <c>compression_settings.compress_relid</c> relation), calls the method, force-flushes this backend's
/// pending counters, and asserts NOT ONE chunk older than the floor (minus a chunk width of slack) shows a
/// new scan. This does not reference <see cref="DarlingWorker.ReadCollectorWatermarksSql"/> or
/// <see cref="DarlingWorker.WatermarkFloorLookback"/> at all, so it compiles unmodified against a build that
/// predates both: on that build the method still runs the unbounded <c>MAX(collection_time) GROUP BY</c>,
/// which cannot answer without reading every chunk, so this fails at RUNTIME there rather than at compile
/// time.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingWatermarkFloorScanBoundLiveTests
{
    private const int LiveServerId = -469200;
    private const string ServerName = "WM-SCANBOUND-4469-SRV";

    [Fact]
    public async Task ReadCollectorWatermarksAsync_NeverScansAChunkOlderThanTheFloor()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live watermark scan-bound test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* #1922: probe on its own connection. The service's own runtime conversion for collection_log, which
           sits outside the collector catalog and so outside ConvertToHypertablesAsync. */
        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        if (timescaleEnabled)
        {
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        }

        await DeleteLiveRowsAsync(connection, ct);

        /* Stop TimescaleDB's background jobs (compression policy, telemetry) AND autovacuum on this
           hypertable, so neither a scheduler tick nor an autovacuum/autoanalyze triggered by the seed's own
           inserts can independently scan an old chunk between the two snapshots and be mistaken for the
           read under test — the same race the plan-shape file's own live seeds guard against. */
        using (var stopWorkers = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stopWorkers.ExecuteScalarAsync(ct);
        }
        using (var noAutovacuum = new NpgsqlCommand("ALTER TABLE collection_log SET (autovacuum_enabled = false)", connection))
        {
            await noAutovacuum.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            var nowUtc = DateTime.UtcNow;
            long logId = 4_469_200_000;

            /* 30 days of history, one row per collector per day — most chunks close to a day wide and old
               enough to compress, the field shape (30 of 32 chunks compressed). The newest row per collector
               sits inside the 2-day floor. */
            for (var daysAgo = 29; daysAgo >= 0; daysAgo--)
            {
                var at = Whole(nowUtc.AddDays(-daysAgo));
                await InsertLogAsync(connection, ct, logId++, "wait_stats", at);
                await InsertLogAsync(connection, ct, logId++, "index_object_stats", at);
            }

            await CompressOldChunksAsync(connection, ct);

            /* Every chunk relation this hypertable now has, and — for a compressed one — the relation its
               compressed data actually lives in, since a compressed chunk's own heap carries no rows and no
               scan; the read TimescaleDB issues against compressed data lands on compress_relid. */
            var chunkRelations = await ListChunkRelationsAsync(connection, ct);
            Assert.True(chunkRelations.Count >= 5, $"expected several chunks from 30 days of daily rows; got {chunkRelations.Count}");

            var oldFloorSlack = nowUtc.AddDays(-2).AddDays(-1); /* the 2-day floor minus a chunk width of slack */
            var oldChunks = new List<string>();
            foreach (var (schema, name, rangeEnd) in chunkRelations)
            {
                if (rangeEnd < oldFloorSlack)
                {
                    oldChunks.Add($"{schema}.{name}");
                }
            }
            Assert.True(oldChunks.Count >= 3, $"expected several chunks to fall outside the floor; got {oldChunks.Count}");

            /* Compute the unbounded oracle BEFORE the scan-counter snapshot: the oracle itself is an
               unbounded MAX(collection_time) scan (deliberately, since it is what the old, replaced
               statement did), so running it between "before" and "after" would touch the old chunks itself
               and make this test fail for a reason that has nothing to do with the method under test. */
            var expectedWait = await ScalarMaxAsync(connection, ct, "wait_stats");
            var expectedIndex = await ScalarMaxAsync(connection, ct, "index_object_stats");

            /* Flush THIS connection's own pending counters too — it is the backend that ran the seed's
               inserts and the compression, so its pending stats must land before "before" is captured or
               they show up as spurious growth in "after" instead. */
            using (var ownFlush = new NpgsqlCommand("SELECT pg_stat_force_next_flush()", connection))
            {
                await ownFlush.ExecuteScalarAsync(ct);
            }
            await ForceStatsFlushEverywhereAsync(connectionString!, ct);
            var before = await SnapshotScanCountersAsync(connection, oldChunks, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            /* THE PRODUCT'S OWN CALL PATH — no manual statement standing in for it. */
            var watermarks = await DarlingWorker.ReadCollectorWatermarksAsync(postgres, LiveServerId, logger: null, ct);

            Assert.Equal(expectedWait, watermarks["wait_stats"].Ticks);
            Assert.Equal(expectedIndex, watermarks["index_object_stats"].Ticks);

            /* Force a report from the SAME NpgsqlDataSource's pool the read under test used — pooling means
               the very next OpenConnectionAsync on THIS data source is overwhelmingly likely to hand back the
               identical physical backend the read just returned, so this is the direct flush for that
               backend, not a fallback settle. */
            await using (var sameSourceFlush = await postgres.OpenConnectionAsync(ct))
            {
                using var flushCommand = new NpgsqlCommand("SELECT pg_stat_force_next_flush()", sameSourceFlush);
                await flushCommand.ExecuteScalarAsync(ct);
            }
            await ForceStatsFlushEverywhereAsync(connectionString!, ct);
            var after = await SnapshotScanCountersAsync(connection, oldChunks, ct);

            foreach (var chunk in oldChunks)
            {
                Assert.Equal(before[chunk], after[chunk]);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteLiveRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime Whole(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

    private static async Task InsertLogAsync(NpgsqlConnection connection, CancellationToken ct, long logId, string collector, DateTime time)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 0, 'SUCCESS', NULL, 0, 0, 0)", connection);
        command.Parameters.AddWithValue(logId);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(time);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarMaxAsync(NpgsqlConnection connection, CancellationToken ct, string collector)
    {
        using var command = new NpgsqlCommand(
            "SELECT MAX(collection_time) FROM collection_log WHERE server_id = $1 AND collector_name = $2", connection);
        command.Parameters.AddWithValue(LiveServerId);
        command.Parameters.AddWithValue(collector);
        var value = (DateTime)(await command.ExecuteScalarAsync(ct))!;
        return DateTime.SpecifyKind(value, DateTimeKind.Utc).Ticks;
    }

    private static async Task CompressOldChunksAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Compress every chunk that closed more than a day ago — the field shape (30 of 32 compressed).
           Best-effort per chunk: a chunk already compressed, or one still open, is skipped rather than
           failing the seed. */
        using var chunkList = new NpgsqlCommand(
            "SELECT show_chunks('collection_log', older_than => INTERVAL '1 day')::text", connection);
        await using var reader = await chunkList.ExecuteReaderAsync(ct);
        var chunks = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(reader.GetString(0));
        }
        await reader.DisposeAsync();

        foreach (var chunk in chunks)
        {
            try
            {
                using var compress = new NpgsqlCommand($"SELECT compress_chunk('{chunk}', if_not_compressed => true)", connection);
                await compress.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException)
            {
                /* Already compressed, or not eligible yet — fine either way for this seed's purpose. */
            }
        }
    }

    /// <summary>Every chunk relation <c>collection_log</c> now has, plus each one's range_end (the local
    /// name is enough since this connection's own search_path resolves the bare hypertable name).</summary>
    private static async Task<List<(string Schema, string Name, DateTime RangeEnd)>> ListChunkRelationsAsync(
        NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
SELECT chunk_schema, chunk_name, range_end AT TIME ZONE 'UTC'
FROM timescaledb_information.chunks
WHERE hypertable_name = 'collection_log'
ORDER BY range_end", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<(string, string, DateTime)>();
        while (await reader.ReadAsync(ct))
        {
            result.Add((reader.GetString(0), reader.GetString(1), DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc)));
        }
        return result;
    }

    /// <summary>For each chunk relation, the seq_scan + idx_scan against its own heap, PLUS — if it is
    /// compressed — the same pair against the relation <c>compression_settings.compress_relid</c> names,
    /// since that is where a compressed chunk's actual data scan lands.</summary>
    private static async Task<Dictionary<string, (long Seq, long Idx)>> SnapshotScanCountersAsync(
        NpgsqlConnection connection, List<string> chunkQualifiedNames, CancellationToken ct)
    {
        var result = new Dictionary<string, (long Seq, long Idx)>();
        foreach (var qualified in chunkQualifiedNames)
        {
            var parts = qualified.Split('.', 2);
            using var command = new NpgsqlCommand(@"
SELECT
    COALESCE((SELECT seq_scan FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2), 0)
        + COALESCE((
            SELECT s2.seq_scan
            FROM _timescaledb_catalog.compression_settings cs
            JOIN pg_stat_user_tables s2 ON s2.relid = cs.compress_relid
            WHERE cs.relid = format('%I.%I', $1, $2)::regclass), 0) AS seq_scan,
    COALESCE((SELECT idx_scan FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2), 0)
        + COALESCE((
            SELECT s2.idx_scan
            FROM _timescaledb_catalog.compression_settings cs
            JOIN pg_stat_user_tables s2 ON s2.relid = cs.compress_relid
            WHERE cs.relid = format('%I.%I', $1, $2)::regclass), 0) AS idx_scan", connection);
            command.Parameters.AddWithValue(parts[0]);
            command.Parameters.AddWithValue(parts[1]);
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            result[qualified] = (reader.GetInt64(0), reader.GetInt64(1));
        }
        return result;
    }

    /// <summary>Forces the test's own connection AND a fresh connection through the pooled data source (the
    /// backend the product method under test actually runs on) to report their pending cumulative-stats
    /// counters immediately, rather than waiting on PostgreSQL's once-per-second throttle — the same
    /// de-flake <see cref="QueryStoreIntervalWideGridLiveTests"/> uses for the identical reason.</summary>
    private static async Task ForceStatsFlushEverywhereAsync(string connectionString, CancellationToken ct)
    {
        using var own = new NpgsqlConnection(connectionString);
        await own.OpenAsync(ct);
        using var ownFlush = new NpgsqlCommand("SELECT pg_stat_force_next_flush()", own);
        await ownFlush.ExecuteScalarAsync(ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString);
        await using var pooled = await postgres.OpenConnectionAsync(ct);
        await using var pooledFlush = new NpgsqlCommand("SELECT pg_stat_force_next_flush()", pooled);
        await pooledFlush.ExecuteScalarAsync(ct);
    }

    private static async Task DeleteLiveRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sid = LiveServerId.ToString(CultureInfo.InvariantCulture);
        using var cleanup = new NpgsqlCommand($"DELETE FROM collection_log WHERE server_id = {sid};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
