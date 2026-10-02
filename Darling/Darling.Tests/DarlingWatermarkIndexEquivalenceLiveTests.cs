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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Store rung V150 (#4469, #4477): the per-collector index-only lookup
/// (<see cref="DarlingWorker.ReadCollectorWatermarksSql"/>) must answer IDENTICALLY to the GROUP BY form it
/// replaces, floor and all — the win is plan shape (one index descent per collector instead of a scan of
/// every row in range), not a different answer. This seeds several collectors across several servers, with
/// old chunks compressed, and asserts <see cref="DarlingWorker.ReadCollectorWatermarksAsync"/> (the
/// product's own call path) returns the same per-collector MAX(collection_time) the bounded GROUP BY form
/// would, including the "absent when never run within the floor" contract.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingWatermarkIndexEquivalenceLiveTests
{
    private const int LiveServerId = -150100;
    private const string ServerName = "WM-EQUIV-V150-SRV";

    [Fact]
    public async Task ReadCollectorWatermarksAsync_MatchesTheBoundedGroupByOracle()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live watermark index equivalence test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

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
            long logId = 4_150_100_000;

            /* Several real collector names (a mix of cadences), 10 days of history so most chunks
               compress, so the equivalence holds across the compressed/uncompressed boundary too. */
            var collectors = new[] { "wait_stats", "index_object_stats", "query_stats", "memory_stats" };
            foreach (var collector in collectors)
            {
                for (var daysAgo = 9; daysAgo >= 0; daysAgo--)
                {
                    var at = Whole(nowUtc.AddDays(-daysAgo));
                    await InsertLogAsync(connection, ct, logId++, collector, at);
                }
            }

            /* One collector that ran, but only OUTSIDE the floor — the absent-when-never-run-within-floor
               contract this rung must not regress (#4469). */
            await InsertLogAsync(connection, ct, logId++, "session_stats", Whole(nowUtc.AddDays(-9)));

            await CompressOldChunksAsync(connection, ct);

            var floor = DateTime.SpecifyKind(nowUtc - DarlingWorker.WatermarkFloorLookback, DateTimeKind.Unspecified);

            /* The oracle: the exact bounded GROUP BY form this rung replaces, run directly. */
            var expected = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
            using (var oracle = new NpgsqlCommand(
                "SELECT collector_name, MAX(collection_time) FROM collection_log WHERE server_id = $1 AND collection_time >= $2 GROUP BY collector_name",
                connection))
            {
                oracle.Parameters.AddWithValue(LiveServerId);
                oracle.Parameters.AddWithValue(floor);
                await using var reader = await oracle.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    if (reader.IsDBNull(1))
                    {
                        continue;
                    }
                    expected[reader.GetString(0)] = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
                }
            }

            Assert.True(expected.ContainsKey("wait_stats"));
            Assert.False(expected.ContainsKey("session_stats"), "the outside-the-floor run must not appear in the oracle either");

            /* THE PRODUCT'S OWN CALL PATH — no manual statement standing in for it. */
            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var actual = await DarlingWorker.ReadCollectorWatermarksAsync(postgres, LiveServerId, logger: null, ct);

            foreach (var (name, ticks) in expected)
            {
                Assert.True(actual.ContainsKey(name), $"expected '{name}' in the actual read");
                Assert.Equal(ticks.Ticks, actual[name].Ticks);
            }

            Assert.False(actual.ContainsKey("session_stats"), "outside-the-floor run must be absent from the real read too");

            /* A collector CollectorScheduleDefaults knows about, but never seeded — absent, same as today. */
            Assert.False(actual.ContainsKey("cpu_utilization"));

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
        NpgsqlConnection connection, CancellationToken ct, long logId, string collector, DateTime time)
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

    private static async Task CompressOldChunksAsync(NpgsqlConnection connection, CancellationToken ct)
    {
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

    private static async Task DeleteLiveRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sid = LiveServerId.ToString(CultureInfo.InvariantCulture);
        using var cleanup = new NpgsqlCommand($"DELETE FROM collection_log WHERE server_id = {sid};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
