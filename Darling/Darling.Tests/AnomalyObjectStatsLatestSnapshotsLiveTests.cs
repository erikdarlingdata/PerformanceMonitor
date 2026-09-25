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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4196: <see cref="PgAnomalyDetector.ObjectGrowthSql"/> / <see cref="PgAnomalyDetector.ObjectContentionSql"/>'s
/// <c>snaps</c> CTE (the latest-two-distinct-<c>collection_time</c> lookup) against a real store, seeded so the
/// newest snapshot is shared across several OTHER servers — the exact fleet-contamination shape the issue
/// measured (a <c>Custom Scan (SkipScan)</c> over the chunk's bare <c>collection_time</c> index, filtering out
/// every other server's rows one at a time). V142's <c>idx_index_object_stats_server_time</c> turns this into a
/// direct index probe; this class pins the PLAN (no other server's rows read) and proves the read still returns
/// exactly what it returned before — including a growth tie and a server with only one snapshot ever.
///
/// <para>Sentinel server ids, cleaned up in <c>finally</c> — the same contract as the sibling live suites.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class AnomalyObjectStatsLatestSnapshotsLiveTests
{
    private const int ServerA = -9196; // two snapshots, one clear grower
    private const int ServerB = -9197; // same two snapshot times as A, a growth tie and a contention delta
    private const int ServerC = -9198; // one snapshot ever - no prior, so zero facts
    private const int FirstFiller = -9206; // 8 filler servers sharing A/B's newest snapshot time
    private const int LastFiller = -9199;

    [Fact]
    public async Task LatestTwoSnapshotsRead_TouchesOnlyItsOwnServersRows_AndReturnsTheSameFactsAsBefore()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4196 object-stats read test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescale = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        if (timescale)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var prior = Micro(DateTime.UtcNow.AddDays(-2));
            var latest = Micro(DateTime.UtcNow);

            /* Filler fleet: 8 other servers, each with rows at the SAME `latest` time as A/B - the newest
               chunk's fleet contamination the issue measured. */
            long collectionId = 1;
            for (var s = FirstFiller; s <= LastFiller; s++)
            {
                for (var o = 1; o <= 40; o++)
                {
                    await InsertAsync(connection, collectionId++, latest, s, "db0", 1, o, 1, 10m, 0, 0, ct);
                }
            }

            /* ServerA: prior 100mb -> latest 150mb (growth 50mb/50%); a second, smaller object grows less. */
            await InsertAsync(connection, collectionId++, prior, ServerA, "db1", 1, 101, 1, 100m, 0, 0, ct);
            await InsertAsync(connection, collectionId++, latest, ServerA, "db1", 1, 101, 1, 150m, 0, 0, ct);
            await InsertAsync(connection, collectionId++, prior, ServerA, "db1", 1, 102, 1, 100m, 0, 0, ct);
            await InsertAsync(connection, collectionId++, latest, ServerA, "db1", 1, 102, 1, 105m, 0, 0, ct);

            /* ServerB: shares A's two snapshot times. A growth TIE between two objects (both +40mb), and a
               lock-wait delta for the contention read. */
            await InsertAsync(connection, collectionId++, prior, ServerB, "db1", 1, 201, 1, 60m, 100, 0, ct);
            await InsertAsync(connection, collectionId++, latest, ServerB, "db1", 1, 201, 1, 100m, 900, 2, ct);
            await InsertAsync(connection, collectionId++, prior, ServerB, "db1", 1, 202, 1, 60m, 0, 0, ct);
            await InsertAsync(connection, collectionId++, latest, ServerB, "db1", 1, 202, 1, 100m, 0, 0, ct);

            /* ServerC: one snapshot ever (latest only) - no prior to compare against. */
            await InsertAsync(connection, collectionId++, latest, ServerC, "db1", 1, 301, 1, 100m, 0, 0, ct);

            using (var analyze = new NpgsqlCommand("ANALYZE collect.index_object_stats;", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            /* ---- plan shape: the read for A touches only A's rows -------------------------------------- */

            var snapsSql = "WITH snaps AS (SELECT DISTINCT collection_time FROM v_index_object_stats WHERE server_id = $1 ORDER BY collection_time DESC LIMIT 2) SELECT * FROM snaps";
            /* #4196: pre-fix, this plans as a Sort over every one of this server's rows (or, on a bigger seed,
               a SkipScan with a server_id Filter that also reads every OTHER server's rows in the chunk - the
               issue's own measured shape). Proven red against this exact assertion by disabling V142 on this
               rig: without the index, the plan never mentions it. */
            var plan = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + snapsSql, ServerA, ct);
            Assert.Contains("idx_index_object_stats_server_time", plan, StringComparison.Ordinal);

            /* ---- correctness: same two facts the pre-#4196 shape would have found ---------------------- */

            var growthA = await ReadGrowthAsync(connection, ServerA, ct);
            Assert.NotNull(growthA);
            Assert.Equal("db1", growthA!.Value.Database);
            Assert.Null(growthA.Value.Table);
            Assert.Equal(100.00m, growthA.Value.PriorMb);
            Assert.Equal(150.00m, growthA.Value.CurrentMb);
            Assert.Equal(50.00m, growthA.Value.GrowthMb);

            var growthB = await ReadGrowthAsync(connection, ServerB, ct);
            Assert.NotNull(growthB);
            /* Both B objects tie at +40mb; the read picks one deterministically (ORDER BY growth_mb DESC LIMIT
               1 with no explicit tiebreak) - what matters here is that it is ONE of the tied pair, at the tied
               delta, not that a specific object_id wins. */
            Assert.Equal(40.00m, growthB!.Value.GrowthMb);
            Assert.Null(growthB.Value.Table);

            var contentionB = await ReadContentionAsync(connection, ServerB, ct);
            Assert.Equal(800L, contentionB);

            Assert.Null(await ReadGrowthAsync(connection, ServerC, ct));
            Assert.Null(await ReadContentionAsync(connection, ServerC, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTime, int serverId, string databaseName,
        int databaseId, int objectId, int indexId, decimal reservedMb, long rowLockWaitMs, long lockPromotions,
        CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.index_object_stats
(collection_id, collection_time, server_id, server_name, database_name, database_id, object_id, index_id,
 reserved_mb, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue(collectionId);
        cmd.Parameters.AddWithValue(collectionTime);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue("srv" + serverId.ToString(CultureInfo.InvariantCulture));
        cmd.Parameters.AddWithValue(databaseName);
        cmd.Parameters.AddWithValue(databaseId);
        cmd.Parameters.AddWithValue(objectId);
        cmd.Parameters.AddWithValue(indexId);
        cmd.Parameters.AddWithValue(reservedMb);
        cmd.Parameters.AddWithValue(rowLockWaitMs);
        cmd.Parameters.AddWithValue(lockPromotions);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(string Database, string? Schema, string? Table, decimal PriorMb, decimal CurrentMb, decimal GrowthMb)?> ReadGrowthAsync(
        NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgAnomalyDetector.ObjectGrowthSql, connection);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(0.0);
        cmd.Parameters.AddWithValue(0.0);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return (
            reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.GetFieldValue<decimal>(3),
            reader.GetFieldValue<decimal>(4),
            reader.GetFieldValue<decimal>(5));
    }

    private static async Task<long?> ReadContentionAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgAnomalyDetector.ObjectContentionSql, connection);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(0.0);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return Convert.ToInt64(reader.GetValue(4));
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, int serverId, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(serverId);
        var plan = new StringBuilder();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    /// <summary>PostgreSQL <c>timestamp</c> is microsecond-resolution; .NET ticks are 100 ns.</summary>
    private static DateTime Micro(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % 10)), DateTimeKind.Unspecified);

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(
            "DELETE FROM collect.index_object_stats WHERE server_id BETWEEN $1 AND $2", connection);
        cmd.Parameters.AddWithValue(FirstFiller);
        cmd.Parameters.AddWithValue(ServerA);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
