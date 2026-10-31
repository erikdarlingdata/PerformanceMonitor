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
/// #6968: the latest-sample reads start at a floor taken from the newest row they last returned. The floor must never change the
/// row that comes back, so each shape is checked against the unbounded read it replaces: a server with fresh rows, one offline
/// for days (its last row must still come back), one with no rows, one that comes back after being offline, and one whose rows
/// were purged under the floor.
/// </summary>
[Collection("live-postgres")]
public sealed class NewestRowFloorLiveTests
{
    /// <summary>Sentinel server ids, negative so they cannot collide with a registered server.</summary>
    private const int Online = -9681, Stale = -9682, Empty = -9683, Purged = -9684;

    private const int LiveTimeoutSeconds = 60;

    [Fact]
    public async Task TempDbRead_ReturnsTheUnboundedReadsRow_ForEveryServerShape()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-sample floor test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(dataSource);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);
            var now = DateTime.UtcNow;
            await InsertTempDbAsync(connection, Online, now.AddMinutes(-1), ct);
            await InsertTempDbAsync(connection, Online, now, ct);
            await InsertTempDbAsync(connection, Stale, now.AddDays(-5).AddMinutes(-1), ct);
            await InsertTempDbAsync(connection, Stale, now.AddDays(-5), ct);
            await InsertTempDbAsync(connection, Purged, now.AddMinutes(-3), ct);

            /* Three reads each: the first has no floor (the unbounded read), the next two ride on the floor the first left. */
            for (var pass = 0; pass < 3; pass++)
            {
                foreach (var serverId in new[] { Online, Stale, Empty, Purged })
                {
                    await AssertReadMatchesOracleAsync(adapter, connection, serverId, ct);
                }
            }

            Assert.NotNull(await adapter.GetTempDbSpaceAsync(Stale.ToString(CultureInfo.InvariantCulture), ct));
            Assert.Null(await adapter.GetTempDbSpaceAsync(Empty.ToString(CultureInfo.InvariantCulture), ct));

            /* The offline server comes back: the floor sits at its old row and the new one is after it. */
            await InsertTempDbAsync(connection, Stale, now, ct);
            await AssertReadMatchesOracleAsync(adapter, connection, Stale, ct);

            /* The empty server gets its first row: the floor from the empty read is before it. */
            await InsertTempDbAsync(connection, Empty, DateTime.UtcNow, ct);
            await AssertReadMatchesOracleAsync(adapter, connection, Empty, ct);
            Assert.NotNull(await adapter.GetTempDbSpaceAsync(Empty.ToString(CultureInfo.InvariantCulture), ct));

            /* The rows under a server's floor are purged: the bounded read finds nothing, the unbounded one finds nothing, and a
               row that arrives afterwards is found again. */
            await DeleteSentinelsAsync(connection, ct, only: Purged);
            await AssertReadMatchesOracleAsync(adapter, connection, Purged, ct);
            Assert.Null(await adapter.GetTempDbSpaceAsync(Purged.ToString(CultureInfo.InvariantCulture), ct));
            await InsertTempDbAsync(connection, Purged, DateTime.UtcNow, ct);
            await AssertReadMatchesOracleAsync(adapter, connection, Purged, ct);
            Assert.NotNull(await adapter.GetTempDbSpaceAsync(Purged.ToString(CultureInfo.InvariantCulture), ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }

    [Fact]
    public async Task CpuSinceRead_ReturnsTheUnboundedReadsRow_ForAServerOfflineForDays()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-sample floor test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);
            var now = DateTime.UtcNow;
            /* One poll of three samples under one collection_time (the tiebreak case), five days old, then a fresh server. */
            foreach (var (serverId, collected) in new[] { (Stale, now.AddDays(-5)), (Online, now) })
            {
                for (var sample = 0; sample < 3; sample++)
                {
                    using var insert = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, 'sentinel', $4, $5, $6)", connection) { CommandTimeout = LiveTimeoutSeconds };
                    insert.Parameters.AddWithValue((long)Math.Abs(serverId) * 10 + sample);
                    insert.Parameters.AddWithValue(DateTime.SpecifyKind(collected, DateTimeKind.Unspecified));
                    insert.Parameters.AddWithValue(serverId);
                    insert.Parameters.AddWithValue(DateTime.SpecifyKind(collected.AddMinutes(sample - 2), DateTimeKind.Unspecified));
                    insert.Parameters.AddWithValue(10 + sample);
                    insert.Parameters.AddWithValue(20 + sample);
                    await insert.ExecuteNonQueryAsync(ct);
                }
            }

            foreach (var serverId in new[] { Stale, Online })
            {
                var (full, floor) = await ReadCpuAsync(connection, DarlingWorker.LatestCpuSql, serverId, null, ct);
                var (bounded, _) = await ReadCpuAsync(connection, DarlingWorker.LatestCpuSinceSql, serverId, floor, ct);
                Assert.NotNull(floor);
                Assert.Equal(full, bounded);
                Assert.Equal((12, 22), bounded);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }

    private static async Task AssertReadMatchesOracleAsync(DarlingAlertReadAdapter adapter, NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var oracle = new NpgsqlCommand(
            "SELECT collection_time FROM tempdb_stats WHERE server_id = $1 ORDER BY collection_time DESC LIMIT 1", connection) { CommandTimeout = LiveTimeoutSeconds };
        oracle.Parameters.AddWithValue(serverId);
        var expected = await oracle.ExecuteScalarAsync(ct) as DateTime?;

        var actual = await adapter.GetTempDbSpaceAsync(serverId.ToString(CultureInfo.InvariantCulture), ct);

        if (expected is null)
        {
            Assert.Null(actual);
        }
        else
        {
            Assert.NotNull(actual);
            Assert.Equal(DateTime.SpecifyKind(expected.Value, DateTimeKind.Utc), actual!.CollectionTimeUtc);
        }
    }

    private static async Task<((int Sql, int Other)? Row, DateTime? CollectionTime)> ReadCpuAsync(
        NpgsqlConnection connection, string sql, int serverId, DateTime? since, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = LiveTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        if (since is { } floor)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = floor });
        }

        using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ((reader.GetInt32(0), reader.GetInt32(1)), reader.GetDateTime(4)) : (null, null);
    }

    private static async Task InsertTempDbAsync(NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, total_reserved_mb, unallocated_mb)
VALUES ($1, $2, $3, 'sentinel', 100, 900)", connection) { CommandTimeout = LiveTimeoutSeconds };
        command.Parameters.AddWithValue(collectionTimeUtc.Ticks);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct, int? only = null)
    {
        var filter = only is { } one
            ? string.Create(CultureInfo.InvariantCulture, $"server_id = {one}")
            : string.Create(CultureInfo.InvariantCulture, $"server_id IN ({Online}, {Stale}, {Empty}, {Purged})");
        foreach (var table in new[] { "tempdb_stats", "cpu_utilization_stats" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE {filter};", connection) { CommandTimeout = LiveTimeoutSeconds };
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
