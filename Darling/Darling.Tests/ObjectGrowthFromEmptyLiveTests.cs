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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7897: <see cref="PgAnomalyDetector.ObjectGrowthSql"/> against a real store, with the detector's own thresholds. A table that
/// held no pages in the earlier snapshot and holds 500 MB in the latest comes back as the grower, with no percent (a percent of
/// nothing has no value); the same table reaching 50 MB stays under the MB floor and comes back as nothing.
///
/// <para>Sentinel server ids, cleaned up in <c>finally</c>, the same contract as the sibling live suites.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ObjectGrowthFromEmptyLiveTests
{
    private const int ServerOverFloor = -9212; // 0 MB -> 500 MB
    private const int ServerUnderFloor = -9211; // 0 MB -> 50 MB

    [Fact]
    public async Task GrowthFromEmpty_FiresOverTheMbFloor_WithNoPercent_AndNotUnderIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #7897 object growth read test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var prior = Micro(DateTime.UtcNow.AddDays(-1));
            var latest = Micro(DateTime.UtcNow);

            long collectionId = 1;
            await InsertAsync(connection, collectionId++, prior, ServerOverFloor, 401, 0m, ct);
            await InsertAsync(connection, collectionId++, latest, ServerOverFloor, 401, 500m, ct);
            await InsertAsync(connection, collectionId++, prior, ServerUnderFloor, 402, 0m, ct);
            await InsertAsync(connection, collectionId++, latest, ServerUnderFloor, 402, 50m, ct);

            var over = await ReadGrowthAsync(connection, ServerOverFloor, ct);
            Assert.NotNull(over);
            Assert.Equal(0.00m, over!.Value.PriorMb);
            Assert.Equal(500.00m, over.Value.CurrentMb);
            Assert.Equal(500.00m, over.Value.GrowthMb);
            Assert.Null(over.Value.GrowthPct);

            Assert.Null(await ReadGrowthAsync(connection, ServerUnderFloor, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTime, int serverId, int objectId, decimal reservedMb,
        CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.index_object_stats
(collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id, table_name, index_id,
 reserved_mb, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1, $2, $3, $4, 'db1', 1, 'dbo', $5, 'Staging', 1, $6, 0, 0)";
        using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue(collectionId);
        cmd.Parameters.AddWithValue(collectionTime);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue("srv" + serverId.ToString(CultureInfo.InvariantCulture));
        cmd.Parameters.AddWithValue(objectId);
        cmd.Parameters.AddWithValue(reservedMb);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The detector's read with the detector's own thresholds, so the floor under test is the shipped one.</summary>
    private static async Task<(decimal PriorMb, decimal CurrentMb, decimal GrowthMb, decimal? GrowthPct)?> ReadGrowthAsync(
        NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgAnomalyDetector.ObjectGrowthSql, connection);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(AnomalyThresholds.ObjectGrowthMbThreshold);
        cmd.Parameters.AddWithValue(AnomalyThresholds.ObjectGrowthPctThreshold);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return (
            reader.GetFieldValue<decimal>(3),
            reader.GetFieldValue<decimal>(4),
            reader.GetFieldValue<decimal>(5),
            reader.IsDBNull(6) ? null : Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture));
    }

    /// <summary>PostgreSQL <c>timestamp</c> is microsecond-resolution; .NET ticks are 100 ns.</summary>
    private static DateTime Micro(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % 10)), DateTimeKind.Unspecified);

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(
            "DELETE FROM collect.index_object_stats WHERE server_id BETWEEN $1 AND $2", connection);
        cmd.Parameters.AddWithValue(ServerOverFloor);
        cmd.Parameters.AddWithValue(ServerUnderFloor);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
