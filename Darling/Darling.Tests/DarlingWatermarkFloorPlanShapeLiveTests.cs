/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4469: the connect-path watermark read (<see cref="DarlingWorker.ReadCollectorWatermarksAsync"/>) used to
/// run <c>SELECT collector_name, MAX(collection_time) FROM collection_log WHERE server_id = $1 GROUP BY
/// collector_name</c> with no bound on <c>collection_time</c>, so TimescaleDB had to read every retained
/// chunk — compressed ones included — before it could know the newest instant per collector.
/// Back-to-back field EXPLAINs on the busiest measured store: the unbounded statement, cold, took
/// 4,775 ms (3,166 hit + 21,483 read buffers, 13,957 ms of parallel-worker I/O read time). Adding a
/// literal 2-day floor removed the 30 older compressed chunks from that scan, but those chunks had
/// only accounted for about 11% of the cold I/O (~1,540 ms); the other 89% (~12,417 ms) was a Bitmap
/// Heap Scan over the two newest, uncompressed chunks, which the floor leaves unchanged. (The bounded
/// run's own 69 ms / 20,703-buffers-all-hit number was measured warm, right after the unbounded run
/// had cached the same pages, so it is not a clean before/after for the floor's effect.)
///
/// <para>This class proves the SAME thing against a rig with compressed chunks: (1) the bounded statement
/// returns the identical MAX-per-collector rows the unbounded statement returns, for collectors whose true
/// last run sits inside the floor; and (2) the bounded statement's plan touches a bounded, not
/// retention-sized, chunk count — seeded at 2x the chunks and shown not to grow.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingWatermarkFloorPlanShapeLiveTests
{
    private const int LiveServerId = -469100;
    private const string ServerName = "WM-FLOOR-4469-SRV";

    /// <summary>The pre-#4469 shape, kept here as the row-correctness oracle — identical text to what shipped
    /// on <c>origin/dev</c> before this fix.</summary>
    private const string OldUnboundedSql =
        "SELECT collector_name, MAX(collection_time) FROM collection_log WHERE server_id = $1 GROUP BY collector_name";

    [Fact]
    public async Task BoundedRead_MatchesUnboundedOracle_AcrossManyCompressedChunks()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live watermark floor plan test.");

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

        var bodySucceeded = false;
        try
        {
            var nowUtc = DateTime.UtcNow;
            long logId = 4_469_000_000;

            /* 30 days of history, one row per collector per day, most of it old enough to compress —
               the field shape (30 of 32 chunks compressed). The newest row per collector sits INSIDE the
               2-day floor, so the bounded read's answer must equal the unbounded oracle's for both. */
            for (var daysAgo = 29; daysAgo >= 0; daysAgo--)
            {
                var at = Whole(nowUtc.AddDays(-daysAgo));
                await InsertLogAsync(connection, ct, logId++, "wait_stats", at);
                await InsertLogAsync(connection, ct, logId++, "index_object_stats", at);
            }

            await CompressOldChunksAsync(connection, ct);

            var expectedWait = await ScalarMaxAsync(connection, ct, OldUnboundedSql, "wait_stats");
            var expectedIndex = await ScalarMaxAsync(connection, ct, OldUnboundedSql, "index_object_stats");

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var watermarks = await DarlingWorker.ReadCollectorWatermarksAsync(postgres, LiveServerId, logger: null, ct);

            Assert.Equal(expectedWait, watermarks["wait_stats"].Ticks);
            Assert.Equal(expectedIndex, watermarks["index_object_stats"].Ticks);

            /* The plan-shape half: EXPLAIN the bounded statement and prove the chunk count is bounded by the
               floor, not the 30 days of retention just seeded. */
            var floor = Whole(nowUtc) - DarlingWorker.WatermarkFloorLookback;
            var boundedPlan = await ExplainAsync(connection, DarlingWorker.ReadCollectorWatermarksSql, floor, ct);
            var boundedChunks = PlanChunkScans.DistinctChunkCount(boundedPlan);

            Assert.True(boundedChunks is >= 1 and <= 3,
                $"expected the 2-day floor to touch at most a couple of chunks (touched={boundedChunks}):\n{boundedPlan}");

            /* Seed a SECOND server at 2x the chunk count (60 days) and prove the bounded plan's chunk count
               does not grow with retention — the property #4469 exists to establish, not just "is small
               today". */
            const int wideServerId = LiveServerId - 1;
            for (var daysAgo = 59; daysAgo >= 0; daysAgo--)
            {
                var at = Whole(nowUtc.AddDays(-daysAgo));
                await InsertLogAsync(connection, ct, logId++, "wait_stats", at, wideServerId);
            }
            await CompressOldChunksAsync(connection, ct);

            var widePlan = await ExplainAsync(connection, DarlingWorker.ReadCollectorWatermarksSql, floor, ct, wideServerId);
            var wideChunks = PlanChunkScans.DistinctChunkCount(widePlan);
            Assert.True(wideChunks <= boundedChunks + 1,
                $"expected doubling retained history to leave the bounded plan's chunk count essentially unchanged (30d={boundedChunks}, 60d={wideChunks}):\n{widePlan}");

            await DeleteLiveRowsAsync(connection, ct, wideServerId);

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

    private static async Task InsertLogAsync(
        NpgsqlConnection connection, CancellationToken ct, long logId, string collector, DateTime time, int? serverId = null)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 0, 'SUCCESS', NULL, 0, 0, 0)", connection);
        command.Parameters.AddWithValue(logId);
        command.Parameters.AddWithValue(serverId ?? LiveServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(time);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarMaxAsync(NpgsqlConnection connection, CancellationToken ct, string sql, string collector)
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
        var chunks = new System.Collections.Generic.List<string>();
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

    private static async Task<string> ExplainAsync(
        NpgsqlConnection connection, string sql, DateTime floor, CancellationToken ct, int? serverId = null)
    {
        using var command = new NpgsqlCommand("EXPLAIN (COSTS OFF) " + sql, connection);
        command.Parameters.AddWithValue(serverId ?? LiveServerId);
        command.Parameters.AddWithValue(floor);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var sb = new System.Text.StringBuilder();
        while (await reader.ReadAsync(ct))
        {
            sb.AppendLine(reader.GetString(0));
        }
        return sb.ToString();
    }

    private static async Task DeleteLiveRowsAsync(NpgsqlConnection connection, CancellationToken ct, int? serverId = null)
    {
        var sid = (serverId ?? LiveServerId).ToString(CultureInfo.InvariantCulture);
        using var cleanup = new NpgsqlCommand($"DELETE FROM collection_log WHERE server_id = {sid};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
