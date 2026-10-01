/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
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
/// On a database with optimized locking, writers wait on transaction-ID locks, which the per-index row and
/// page lock counters behind <c>get_object_locking</c> do not count. The payload carries
/// <c>optimized_locking_note</c> (the shared <see cref="OptimizedLockingNote.Text"/>) only when the newest stored
/// database-config snapshot has <c>is_optimized_locking_on</c> true for some database on the server; a false or
/// unknown flag gives JSON null. The existing <c>note</c> field is untouched.
/// </summary>
[Collection("live-postgres")]
public sealed class OptimizedLockingNoteLivePostgresTests
{
    private const string ServerName = "darling-optimized-locking-note-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheNote_RidesOnlyWhenTheNewestSnapshotHasTheFlagOnForSomeDatabase()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live optimized-locking note test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
            await SeedContendedIndexAsync(connection, ct, t);

            /* Every database NULL (unknown): no note. */
            await SeedConfigAsync(connection, ct, t, "Sales", null);
            await SeedConfigAsync(connection, ct, t, "Billing", null);
            Assert.Null(await NoteAsync(postgres));

            /* One database true in the newest snapshot: the shared text. */
            var t2 = t.AddMinutes(5);
            await SeedConfigAsync(connection, ct, t2, "Sales", false);
            await SeedConfigAsync(connection, ct, t2, "Billing", true);
            Assert.Equal(OptimizedLockingNote.Text, await NoteAsync(postgres));

            /* A newer snapshot with every flag false: no note, though an older snapshot said true. */
            var t3 = t2.AddMinutes(2);
            await SeedConfigAsync(connection, ct, t3, "Sales", false);
            await SeedConfigAsync(connection, ct, t3, "Billing", false);
            Assert.Null(await NoteAsync(postgres));

            /* No contended index rows at all, but the flag is on: the empty status still carries the note. */
            await SeedConfigAsync(connection, ct, t3.AddMinutes(1), "Billing", true);
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM index_object_stats WHERE server_id = {ServerId}");
            using (var empty = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, ServerName)))
            {
                Assert.Equal("unavailable", empty.RootElement.GetProperty("status").GetString());
                Assert.Contains(OptimizedLockingNote.Text, empty.RootElement.GetProperty("message").GetString());
                Assert.Equal(OptimizedLockingNote.Text,
                    empty.RootElement.GetProperty("hints").GetProperty("optimized_locking_note").GetString());
            }
            await SeedContendedIndexAsync(connection, ct, t);

            /* The existing completeness note keeps its words. */
            using var doc = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, ServerName));
            Assert.Equal("Complete: every index with lock/latch contention at the latest snapshot is included.",
                doc.RootElement.GetProperty("note").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task<string?> NoteAsync(NpgsqlDataSource postgres)
    {
        var json = await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, ServerName);
        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("optimized_locking_note", out var note),
            "the payload has no optimized_locking_note field");
        return note.ValueKind == JsonValueKind.Null ? null : note.GetString();
    }

    private static Task SeedContendedIndexAsync(NpgsqlConnection connection, CancellationToken ct, DateTime capture)
        => DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)",
            CollectionIdGenerator.Next(), capture, ServerId, ServerName, "Sales", "dbo", 1000, "Orders",
            1, "PK_Orders", "CLUSTERED", 100m, 100m, 10_000L, 5L, 500L, 1L, 100L, 0L, 10L, 10L);

    private static Task SeedConfigAsync(NpgsqlConnection connection, CancellationToken ct, DateTime capture, string database, bool? optimizedLockingOn)
        => DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level, is_optimized_locking_on)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            CollectionIdGenerator.Next(), DateTime.SpecifyKind(capture, DateTimeKind.Unspecified), ServerId, ServerName, database, "ONLINE", 170,
            (object?)optimizedLockingOn ?? DBNull.Value);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM index_object_stats WHERE server_id = {ServerId}; DELETE FROM database_config WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>Source pins for the surfaces that show the optimized-locking note: the web descriptor and the two
/// WPF loaders.</summary>
public sealed class OptimizedLockingNoteSurfacePinTests
{
    [Fact]
    public void WebObjectContentionPanel_CarriesTheNoteKey()
    {
        var js = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var at = js.IndexOf("\"get_object_locking\"", StringComparison.Ordinal);
        Assert.True(at > 0, "the Object Contention panel is missing");
        var end = js.IndexOf("\n      ),", at, StringComparison.Ordinal);
        Assert.Contains("optimized_locking_note", js.Substring(at, end - at), StringComparison.Ordinal);
    }

    [Fact]
    public void ViewerLockingLoader_DrivesTheNoteFromTheFlagRead()
    {
        var src = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Locking.cs");
        var body = MethodBody(src, "private async Task LoadFinOpsIndexLockingGridAsync()");
        Assert.Contains("OptimizedLockingNote", body, StringComparison.Ordinal);
        Assert.Contains("FinOpsOptimizedLockingNote.Visibility", body, StringComparison.Ordinal);
    }

    [Fact]
    public void LiteLockingLoader_DrivesTheNoteFromTheFlagRead()
    {
        var src = RepoFile.ReadRepoFile("Lite", "Controls", "FinOpsTab.Locking.cs");
        var body = MethodBody(src, "private async Task LoadIndexLockingGridAsync(int serverId)");
        Assert.Contains("OptimizedLockingNote", body, StringComparison.Ordinal);
        Assert.Contains("OptimizedLockingNoteText.Visibility", body, StringComparison.Ordinal);
    }

    private static string MethodBody(string src, string declaration)
    {
        var at = src.IndexOf(declaration, StringComparison.Ordinal);
        Assert.True(at >= 0 && src.IndexOf(declaration, at + 1, StringComparison.Ordinal) < 0, declaration + " must match exactly once");
        var next = src.IndexOf("\n    private ", at + declaration.Length, StringComparison.Ordinal);
        return src.Substring(at, (next < 0 ? src.Length : next) - at);
    }
}
