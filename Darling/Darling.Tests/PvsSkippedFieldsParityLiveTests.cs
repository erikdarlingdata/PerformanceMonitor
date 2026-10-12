/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Gated live parity (DARLING_TEST_PG): on one seeded store, <c>get_pvs_stats</c> reports the same Aborted Lag,
/// Cleanup state and three Skipped counters the desktop viewer's own read computes. Every time is a fixed anchor.
/// A counter the server never reported is a null, never a 0.
/// </summary>
[Collection("live-postgres")]
public sealed class PvsSkippedFieldsParityLiveTests
{
    private const int ServerId = -484301;
    private const string ServerName = "pvs-parity-484301";
    private static readonly DateTime Collected = new(2026, 8, 1, 4, 6, 37, DateTimeKind.Unspecified);
    private static readonly DateTime CleanerStart = new(2026, 8, 1, 4, 6, 30, DateTimeKind.Unspecified);
    private static readonly DateTime CleanerEnd = new(2026, 8, 1, 4, 6, 35, DateTimeKind.Unspecified);

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.pvs_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }

    private static Task InsertAsync(
        NpgsqlConnection connection, CancellationToken ct, long id, string db, decimal size,
        long active, long aborted, object? offStart, object? offEnd, object? skipSecondary, object? skipSnapshot, object? skipAborted) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.pvs_stats
(
    collection_id, collection_time, server_id, server_name, database_name, database_id,
    is_accelerated_database_recovery_on, pvs_filegroup_id,
    persistent_version_store_size_mb, online_index_version_store_size_mb, database_data_size_mb,
    current_aborted_transaction_count, oldest_active_transaction_id, oldest_aborted_transaction_id,
    offrow_version_cleaner_start_time, offrow_version_cleaner_end_time,
    pvs_off_row_page_skipped_low_water_mark, pvs_off_row_page_skipped_min_useful_xts,
    pvs_off_row_page_skipped_oldest_aborted_xdesid
)
VALUES ($1, $2, $3, $4, $5, 15, TRUE, 1, $6, 0, 1280, 3, $7, $8, $9, $10, $11, $12, $13)",
            id, Collected, ServerId, ServerName, db, size, active, aborted,
            offStart ?? DBNull.Value, offEnd ?? DBNull.Value,
            skipSecondary ?? DBNull.Value, skipSnapshot ?? DBNull.Value, skipAborted ?? DBNull.Value);

    [Fact]
    public async Task TheToolAndTheViewerRead_AgreeOnLagCleanupAndSkippedCounters()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the PVS parity live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* Idle cleaner, all three counters reported (one a real 0), both ids set. */
            await InsertAsync(connection, ct, 910001, "PvsParityBusy", 900m, 97388, 1244, CleanerStart, CleanerEnd, 10L, 0L, 60L);
            /* Cleaner mid-run, the ids at the DMV's zero sentinel, no counter ever reported. */
            await InsertAsync(connection, ct, 910002, "PvsParityQuiet", 100m, 0, 0, CleanerStart, null, null, null, null);
            /* Cleaner never ran. */
            await InsertAsync(connection, ct, 910003, "PvsParityFresh", 50m, 500, 0, null, null, null, null, null);

            await using var postgres = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(cs)
            {
                SearchPath = PgSchemaGenerator.SearchPath,
            }.ConnectionString);
            using var doc = JsonDocument.Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, server_name: ServerName, cancellationToken: ct));
            var tool = doc.RootElement.GetProperty("databases").EnumerateArray()
                .ToDictionary(d => d.GetProperty("database_name").GetString()!);

            await using var viewer = new ViewerDataService(cs!);
            var rows = await viewer.GetPvsStatsLatestAsync(ServerId, cancellationToken: ct);
            Assert.Equal(3, rows.Count);

            static long? Num(JsonElement d, string key) =>
                d.TryGetProperty(key, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetInt64() : null;

            foreach (var row in rows)
            {
                var d = tool[row.DatabaseName];
                Assert.Equal(row.AbortedTransactionLag, Num(d, "aborted_transaction_lag"));
                Assert.Equal(row.CleanupState, d.GetProperty("cleanup_state").GetString());
                Assert.Equal(row.SkippedLowWaterMark, Num(d, "skipped_low_water_mark"));
                Assert.Equal(row.SkippedMinUsefulXts, Num(d, "skipped_min_useful_xts"));
                Assert.Equal(row.SkippedOldestAborted, Num(d, "skipped_oldest_aborted"));
            }

            var busy = tool["PvsParityBusy"];
            Assert.Equal(96144L, Num(busy, "aborted_transaction_lag"));
            Assert.Equal("Idle", busy.GetProperty("cleanup_state").GetString());
            Assert.Equal(0L, Num(busy, "skipped_min_useful_xts"));
            var quiet = tool["PvsParityQuiet"];
            Assert.Equal("Running", quiet.GetProperty("cleanup_state").GetString());
            Assert.Null(Num(quiet, "aborted_transaction_lag"));
            Assert.Null(Num(quiet, "skipped_low_water_mark"));
            Assert.Equal("Never run", tool["PvsParityFresh"].GetProperty("cleanup_state").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteAsync);
        }
    }
}
