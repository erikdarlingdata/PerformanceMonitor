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
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: reads only the scratch database this class mints. */
/// <summary>
/// An analysis pass replayed for an earlier window reads the server as it was at that window's end. Its latest-capture reads (the
/// server settings, the server's version and properties, and the two newest object snapshots the growth and contention anomalies
/// compare) take the newest capture at or before the window's end, so a capture stored after the end does not change what the pass
/// says. Each read is run twice over the same rows, once ending at the window's end and once ending after the later capture.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalysisReadsEndAtTheWindowLiveTests
{
    private const int ServerId = -9471;
    private const string ServerName = "analysis-window-end";

    private static readonly DateTime s_before = new(2026, 3, 1, 8, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime s_windowEnd = new(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime s_after = new(2026, 3, 1, 16, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task TheServerSettings_AreTheCaptureAtTheWindowEnd_NotOneStoredAfterIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenAsync(ct);
        var connection = scratch.Connection;

        const string insert = @"
INSERT INTO collect.server_config
    (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, 'max degree of parallelism', $5, $5, TRUE, TRUE)";
        await ExecAsync(connection, ct, insert, 1L, s_windowEnd, ServerId, ServerName, 4L);
        await ExecAsync(connection, ct, insert, 2L, s_after, ServerId, ServerName, 8L);

        Assert.Equal(4L, await MaxDopAsync(connection, s_windowEnd, ct));
        Assert.Equal(8L, await MaxDopAsync(connection, s_after, ct));
    }

    [Fact]
    public async Task TheServerVersionAndProperties_AreTheCaptureAtTheWindowEnd_NotOneStoredAfterIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenAsync(ct);
        var connection = scratch.Connection;

        const string insert = @"
INSERT INTO collect.server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition)
VALUES ($1, $2, $3, $4, 'Edition', $5, 'RTM', 3)";
        await ExecAsync(connection, ct, insert, 1L, s_windowEnd, ServerId, ServerName, "15.0.2000.5");
        await ExecAsync(connection, ct, insert, 2L, s_after, ServerId, ServerName, "16.0.1000.6");

        Assert.Equal(15, await ScalarAsync(connection, PgFactCollector.ServerMetadataSql, "major_version", s_windowEnd, ct));
        Assert.Equal(16, await ScalarAsync(connection, PgFactCollector.ServerMetadataSql, "major_version", s_after, ct));
        Assert.Equal("15.0.2000.5", await ScalarAsync(connection, PgFactCollector.ServerPropertiesSql, "product_version", s_windowEnd, ct));
        Assert.Equal("16.0.1000.6", await ScalarAsync(connection, PgFactCollector.ServerPropertiesSql, "product_version", s_after, ct));
    }

    [Fact]
    public async Task TheObjectAnomalies_CompareTheTwoNewestSnapshotsAtTheWindowEnd_NotOnesStoredAfterIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await OpenAsync(ct);
        var connection = scratch.Connection;

        /* One object: 100 MB and 100 ms of row lock waits, then 150 MB and 900 ms at the window's end, then 400 MB and 5,000 ms after it. */
        const string insert = @"
INSERT INTO collect.index_object_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, object_id, index_id,
     reserved_mb, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1, $2, $3, $4, 'db1', 1, 101, 1, $5, $6, 0)";
        await ExecAsync(connection, ct, insert, 1L, s_before, ServerId, ServerName, 100m, 100L);
        await ExecAsync(connection, ct, insert, 2L, s_windowEnd, ServerId, ServerName, 150m, 900L);
        await ExecAsync(connection, ct, insert, 3L, s_after, ServerId, ServerName, 400m, 5000L);

        Assert.Equal((100m, 150m), await GrowthAsync(connection, s_windowEnd, ct));
        Assert.Equal((150m, 400m), await GrowthAsync(connection, s_after, ct));
        Assert.Equal(800L, await ContentionAsync(connection, s_windowEnd, ct));
        Assert.Equal(4100L, await ContentionAsync(connection, s_after, ct));
    }

    /// <summary>A migrated scratch database and one connection to it. The test is skipped when no live store is configured.</summary>
    private static async Task<Scratch> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live analysis window-end tests (each mints its own scratch database).");

        var database = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return new Scratch(database, connection);
    }

    private sealed class Scratch(ScratchPostgres database, NpgsqlConnection connection) : IAsyncDisposable
    {
        public NpgsqlConnection Connection { get; } = connection;

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await database.DisposeAsync();
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] values)
    {
        using var cmd = new NpgsqlCommand(sql, connection);
        foreach (var value in values) cmd.Parameters.AddWithValue(value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long?> MaxDopAsync(NpgsqlConnection connection, DateTime windowEnd, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgFactCollector.ServerConfigSql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(windowEnd);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetString(0) == "max degree of parallelism")
            {
                return Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
            }
        }

        return null;
    }

    /// <summary>The named column of the one row a latest-capture read answers for a window ending at <paramref name="windowEnd"/>.</summary>
    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, string column, DateTime windowEnd, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(windowEnd);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? reader.GetValue(reader.GetOrdinal(column)) : null;
    }

    /// <summary>The prior and current size of the object the growth anomaly reports, with both thresholds at zero.</summary>
    private static async Task<(decimal PriorMb, decimal CurrentMb)?> GrowthAsync(NpgsqlConnection connection, DateTime windowEnd, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgAnomalyDetector.ObjectGrowthSql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(0.0);
        cmd.Parameters.AddWithValue(0.0);
        cmd.Parameters.AddWithValue(windowEnd);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? (reader.GetFieldValue<decimal>(3), reader.GetFieldValue<decimal>(4)) : null;
    }

    /// <summary>The row lock wait the contention anomaly reports between the two snapshots, with its threshold at zero.</summary>
    private static async Task<long?> ContentionAsync(NpgsqlConnection connection, DateTime windowEnd, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgAnomalyDetector.ObjectContentionSql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(0.0);
        cmd.Parameters.AddWithValue(windowEnd);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture) : null;
    }
}
