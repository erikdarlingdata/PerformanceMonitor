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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_table_index_sizes over planted daily snapshots: a store with a gap between its recent snapshots and the rest
/// of its history resolves both the 7-day and the 30-day cutoff to the same old snapshot, and the reply must then
/// refuse the 7-day figure (covers_7d false, growth_7d_mb null, the history note naming the snapshot) while the
/// 30-day one, whose baseline is within reach, stands. A store with a snapshot every day reports the two figures
/// over their own windows.
/// </summary>
[Collection("live-postgres")]
public sealed class TableSizeGrowthBaselineReachLiveTests
{
    private const string ServerName = "darling-growth-reach-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "Hist";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AGapBetweenTheRecentSnapshotsAndTheRest_RefusesThe7DayFigure_AndKeepsThe30DayOne()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live growth-reach test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var latest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-2);

            /* Snapshots for the last five days and from 41 days back to 84; nothing in between. TableA grows 3 MB a
               day from 100 MB at the earliest snapshot; Young was created 20 days ago, so it is in no snapshot older
               than that. */
            await SeedAsync(connection, ct, latest, Enumerable.Range(0, 85).Where(age => age < 5 || age > 40));

            using (var doc = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, ServerName, Db, ct)))
            {
                var history = doc.RootElement.GetProperty("history");
                Assert.False(history.GetProperty("covers_7d").GetBoolean());
                Assert.True(history.GetProperty("covers_30d").GetBoolean());
                Assert.Equal(84, history.GetProperty("history_days_available").GetInt32());
                var note = history.GetProperty("note").GetString();
                Assert.NotNull(note);
                Assert.Contains(latest.AddDays(-41).ToString("o"), note, StringComparison.Ordinal);
                Assert.Contains("growth_7d_mb", note, StringComparison.Ordinal);

                var a = Table(doc, "TableA");
                Assert.Equal(JsonValueKind.Null, a.GetProperty("growth_7d_mb").ValueKind);
                Assert.Equal(123, a.GetProperty("growth_30d_mb").GetDouble());
                Assert.Equal(252, a.GetProperty("growth_over_available_history_mb").GetDouble());
                Assert.Equal(84, a.GetProperty("growth_window_days").GetInt32());
                Assert.Contains("growth_7d_mb is null", a.GetProperty("growth_note").GetString(), StringComparison.Ordinal);

                var young = Table(doc, "Young");
                Assert.Equal(JsonValueKind.Null, young.GetProperty("growth_7d_mb").ValueKind);
                Assert.Equal(JsonValueKind.Null, young.GetProperty("growth_30d_mb").ValueKind);
                var youngNote = young.GetProperty("growth_note").GetString()!;
                Assert.DoesNotContain("newer than 7 days", youngNote, StringComparison.Ordinal);
                Assert.Contains("created since", youngNote, StringComparison.Ordinal);
            }

            /* The same store with a snapshot every day: each figure spans its own window. */
            await DeleteRowsAsync(connection, ct, keepServer: true);
            await SeedAsync(connection, ct, latest, Enumerable.Range(0, 85));

            using (var doc = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, ServerName, Db, ct)))
            {
                var history = doc.RootElement.GetProperty("history");
                Assert.True(history.GetProperty("covers_7d").GetBoolean());
                Assert.True(history.GetProperty("covers_30d").GetBoolean());
                Assert.Equal(JsonValueKind.Null, history.GetProperty("note").ValueKind);

                var a = Table(doc, "TableA");
                Assert.Equal(21, a.GetProperty("growth_7d_mb").GetDouble());
                Assert.Equal(90, a.GetProperty("growth_30d_mb").GetDouble());
                Assert.Equal(JsonValueKind.Null, a.GetProperty("growth_note").ValueKind);

                var young = Table(doc, "Young");
                Assert.Equal(7, young.GetProperty("growth_7d_mb").GetDouble());
                Assert.Equal(JsonValueKind.Null, young.GetProperty("growth_30d_mb").ValueKind);
                Assert.Contains("created since", young.GetProperty("growth_note").GetString(), StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static JsonElement Table(JsonDocument doc, string name) =>
        doc.RootElement.GetProperty("tables").EnumerateArray().Single(t => t.GetProperty("table_name").GetString() == name);

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, DateTime latest, IEnumerable<int> agesInDays)
    {
        foreach (var age in agesInDays)
        {
            var t = latest.AddDays(-age);
            await InsertAsync(connection, ct, t, 1, "TableA", 100m + 3m * (84 - age));
            if (age <= 20)
                await InsertAsync(connection, ct, t, 2, "Young", 10m + 1m * (20 - age));
        }
    }

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t, int objectId, string table, decimal reservedMb) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, Db, "dbo", objectId, table, 1, "PK_" + table, "CLUSTERED",
            reservedMb, reservedMb - 1, 1000L, 1L, 1L, 1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct, bool keepServer = false)
    {
        var sql = $"DELETE FROM index_object_stats WHERE server_id = {ServerId};";
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
