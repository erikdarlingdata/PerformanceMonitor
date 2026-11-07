/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */
/// <summary>
/// #8075 against a real store: an Aurora target reads aurora_stat_statements(), which needs no extension, so when the
/// latest extension inventory shows pg_stat_statements created in no database the unknown eviction note names the
/// missing extension and the CREATE EXTENSION that reads the counter. A stock target, an Aurora target whose inventory
/// shows the extension created somewhere, and a target with no inventory keep the old-major-or-old-version sentence,
/// and a recorded counter is reported as before on either engine.
/// </summary>
public sealed class PgStatementEvictionAuroraLiveTests
{
    private const string ServerName = "pg-evictions-aurora-e2e";
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheUnknownNote_OnAuroraWithoutTheExtension_NamesTheMissingExtension_AndKeepsTheOldCausesElsewhere()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live #8075 eviction note.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var serverId = ServerIdHelper.GetDeterministicHashCode(ServerName);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, ServerName, ct);
            var now = DateTime.UtcNow;
            await SeedStatsAsync(connection, ct, serverId, now.AddMinutes(-10));
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* Aurora, PostgreSQL 16, and no inventory yet: nothing says where the extension stands, so no claim. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE servers SET engine_kind = $1, postgres_major_version = 16 WHERE server_id = $2", MonitoredEngineKind.AuroraPostgres, serverId);
            var noInventory = await DarlingPgStatementReader.GetEvictionInfoAsync(dataSource, serverId, now.AddHours(-4), now, ct);
            Assert.False(noInventory.Known);
            AssertOldCauses(PgStatementEvictionNote.Build(noInventory));

            /* The inventory says pg_stat_statements is available and created in neither database the collector saw. */
            await InventoryAsync(connection, ct, serverId, now.AddHours(-2), "postgres", "available", null);
            await InventoryAsync(connection, ct, serverId, now.AddHours(-2), "appdb", "available", null);
            var aurora = await DarlingPgStatementReader.GetEvictionInfoAsync(dataSource, serverId, now.AddHours(-4), now, ct);
            Assert.False(aurora.Known);
            var note = PgStatementEvictionNote.Build(aurora);
            Assert.NotNull(note);
            Assert.Contains("unknown", note, StringComparison.Ordinal);
            Assert.Contains("CREATE EXTENSION pg_stat_statements", note, StringComparison.Ordinal);
            Assert.Contains("aurora_stat_statements()", note, StringComparison.Ordinal);
            Assert.DoesNotContain("before 14", note, StringComparison.Ordinal);
            Assert.DoesNotContain("below 1.9", note, StringComparison.Ordinal);
            Assert.DoesNotContain("ALTER EXTENSION", note, StringComparison.Ordinal);

            /* The tool carries the same note and flags the cause for the web's short sentence. */
            var evictions = JsonDocument.Parse(await DarlingMcpPgStatementTools.GetPgTopQueries(dataSource, ServerName, 4)).RootElement.GetProperty("evictions");
            Assert.False(evictions.GetProperty("known").GetBoolean());
            Assert.Equal(note, evictions.GetProperty("note").GetString());
            Assert.True(evictions.TryGetProperty("aurora_without_extension", out var flag) && flag.ValueKind == JsonValueKind.True,
                "evictions carries no aurora_without_extension flag");

            /* A newer inventory row says it IS created in one database: the collector's database is not known, so no claim. */
            await InventoryAsync(connection, ct, serverId, now.AddHours(-1), "appdb", "installed", "1.10");
            var createdSomewhere = await DarlingPgStatementReader.GetEvictionInfoAsync(dataSource, serverId, now.AddHours(-4), now, ct);
            Assert.False(createdSomewhere.Known);
            AssertOldCauses(PgStatementEvictionNote.Build(createdSomewhere));

            /* Stock PostgreSQL with the same created-nowhere inventory: the collector there reads the extension's own view, so
               the old causes stand. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "DELETE FROM collect.pg_extension_availability WHERE server_id = $1 AND state = 'installed'", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE servers SET engine_kind = $1 WHERE server_id = $2", MonitoredEngineKind.Postgres, serverId);
            var stock = await DarlingPgStatementReader.GetEvictionInfoAsync(dataSource, serverId, now.AddHours(-4), now, ct);
            Assert.False(stock.Known);
            AssertOldCauses(PgStatementEvictionNote.Build(stock));

            /* Back on Aurora, a run that recorded the counter: known, and the passes sentence as before. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE servers SET engine_kind = $1 WHERE server_id = $2", MonitoredEngineKind.AuroraPostgres, serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, error_message) VALUES ($1, $2, $3, 'pg_statement_stats', $4, 'SUCCESS', $5)",
                CollectionIdGenerator.Next(), serverId, ServerName, DarlingMcpTestData.Naive(now.AddMinutes(-20)), "statements_dealloc=2");
            var known = await DarlingPgStatementReader.GetEvictionInfoAsync(dataSource, serverId, now.AddHours(-4), now, ct);
            Assert.True(known.Known);
            Assert.Equal(2, known.EvictionPasses);
            var knownNote = PgStatementEvictionNote.Build(known);
            Assert.NotNull(knownNote);
            Assert.Contains("evicted entries 2 time(s)", knownNote, StringComparison.Ordinal);
            Assert.DoesNotContain("CREATE EXTENSION", knownNote, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static void AssertOldCauses(string? note)
    {
        Assert.NotNull(note);
        Assert.Contains("unknown", note, StringComparison.Ordinal);
        Assert.Contains("before 14", note, StringComparison.Ordinal);
        Assert.Contains("below 1.9", note, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE EXTENSION", note, StringComparison.Ordinal);
    }

    private static async Task InventoryAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, DateTime at, string database, string state, string? installedVersion) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.pg_extension_availability
    (collection_id, collection_time, server_id, server_name, database_name, extension_name, state, installed_version, default_version, is_monitoring_relevant, comment)
VALUES ($1, $2, $3, $4, $5, 'pg_stat_statements', $6, $7, '1.10', TRUE, 'Per-statement execution counters.')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, ServerName, database, state, installedVersion);

    private static async Task SeedStatsAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, DateTime at) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned,
     shared_blks_hit, shared_blks_read, temp_blks_read, temp_blks_written, wal_bytes,
     delta_calls, delta_total_exec_time_ms, delta_rows)
VALUES ($1, $2, $3, $4, 42, 16384, 10, TRUE, 100, 5000, 91.5, 250, 10, 5, 0, 0, 0, 10, 500, 20)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, ServerName);
}
