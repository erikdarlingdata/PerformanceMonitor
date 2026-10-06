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
/// #5245 (part of #5244), PR6 lane R1: the database filter on the three config reads. Each tool's internal overload takes a
/// <see cref="DatabaseFilter"/>, so an overload call with [A, B] over a seed of A, B and C returns only A's and B's rows.
/// get_database_config_changes filters in SQL (<c>database_config</c>) and its one-name consumer is the empty answer, which
/// must name the databases it looked at instead of claiming the SERVER has too few captures. get_database_scoped_config and
/// get_query_store_health keep their in-memory filter (settled item 3), made list-aware. A whitespace-only name is every
/// database on all three (M3). The public methods still pass one name or none, so no MCP schema, dispatch or tools/list changes.
/// </summary>
[Collection("live-postgres")]
public sealed class ConfigDatabaseFilterLiveTests
{
    private const string ServerName = "darling-config-database-filter";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string A = "CfgFilterA";
    private const string B = "CfgFilterB";
    private const string C = "CfgFilterC";
    // One capture only: a change needs two.
    private const string OneCapture = "CfgFilterOneCapture";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DatabaseFilter Of(params string[] names) => DatabaseFilter.Of(names);

    private static List<string> ChangeDatabases(JsonElement root) =>
        root.GetProperty("changes").EnumerateArray().Select(r => r.GetProperty("database_name").GetString()!).Distinct().OrderBy(x => x, StringComparer.Ordinal).ToList();

    private static List<string> GroupDatabases(JsonElement root) =>
        root.GetProperty("databases").EnumerateArray().Select(r => r.GetProperty("database_name").GetString()!).OrderBy(x => x, StringComparer.Ordinal).ToList();

    private static Task<string> Changes(NpgsqlDataSource postgres, DatabaseFilter filter, int hoursBack = 168) =>
        DarlingMcpConfigHistoryTools.GetDatabaseConfigChanges(postgres, ServerName, hoursBack, filter, null, null, CancellationToken.None);

    [Fact]
    public async Task TheDatabaseConfigChangesOverload_WithAListOfDatabases_ReturnsOnlyThoseDatabases_AndTheEmptyAnswerNamesThem_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database filter test.");

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
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            // A, B and C each changed recovery model between two captures; OneCapture has just the one.
            foreach (var db in new[] { A, B, C })
            {
                await PlantDatabaseConfigAsync(connection, now.AddHours(-30), db, "FULL", ct);
                await PlantDatabaseConfigAsync(connection, now.AddHours(-3), db, "SIMPLE", ct);
            }

            await PlantDatabaseConfigAsync(connection, now.AddHours(-3), OneCapture, "FULL", ct);

            // No filter is every database; [A, B] is A and B; the order of the list does not matter.
            Assert.Equal([A, B, C], ChangeDatabases(Root(await Changes(postgres, DatabaseFilter.All))));
            Assert.Equal([A, B], ChangeDatabases(Root(await Changes(postgres, Of(A, B)))));
            Assert.Equal([A, C], ChangeDatabases(Root(await Changes(postgres, Of(C, A)))));
            Assert.Equal([B], ChangeDatabases(Root(await Changes(postgres, DatabaseFilter.One(B)))));

            // M3: a blank or whitespace-only name is every database, never a literal name that matches nothing.
            Assert.Equal([A, B, C], ChangeDatabases(Root(await Changes(postgres, DatabaseFilter.One("   ")))));
            Assert.Equal([A, B, C], ChangeDatabases(Root(await Changes(postgres, Of("  ", "")))));

            // The public method (no database parameter) is the unfiltered read, byte for byte.
            Assert.Equal(await Changes(postgres, DatabaseFilter.All),
                await DarlingMcpConfigHistoryTools.GetDatabaseConfigChanges(postgres, ServerName, 168));

            // The empty answers are the one-name consumers. A database with one capture: the server HAS captures, so the
            // answer must say it is the chosen database that has too few.
            var one = Root(await Changes(postgres, DatabaseFilter.One(OneCapture)));
            Assert.Equal("empty", one.GetProperty("status").GetString());
            Assert.Equal(1, one.GetProperty("hints").GetProperty("snapshot_count").GetInt32());
            Assert.Contains("for database " + OneCapture + " on this server", one.GetProperty("message").GetString()!, StringComparison.Ordinal);

            // Two names that were never captured: the plural, and no claim about the server.
            var none = Root(await Changes(postgres, Of("CfgFilterMissingOne", "CfgFilterMissingTwo")));
            Assert.Equal("empty", none.GetProperty("status").GetString());
            Assert.Equal(0, none.GetProperty("hints").GetProperty("snapshot_count").GetInt32());
            Assert.Contains("for the chosen databases on this server", none.GetProperty("message").GetString()!, StringComparison.Ordinal);
            Assert.DoesNotContain("CfgFilterMissingOne", none.GetProperty("message").GetString()!, StringComparison.Ordinal);

            // Captures exist for the chosen database but nothing changed inside the window: the sentence names the databases.
            var quiet = Root(await Changes(postgres, Of(A, B), hoursBack: 1));
            Assert.Equal("empty", quiet.GetProperty("status").GetString());
            Assert.Contains("for the chosen databases.", quiet.GetProperty("message").GetString()!, StringComparison.Ordinal);
            var quietOne = Root(await Changes(postgres, DatabaseFilter.One(A), hoursBack: 1));
            Assert.Contains("for database " + A + ".", quietOne.GetProperty("message").GetString()!, StringComparison.Ordinal);

            // Unfiltered, the same quiet window keeps the sentence it always had.
            var quietAll = Root(await Changes(postgres, DatabaseFilter.All, hoursBack: 1));
            Assert.EndsWith("across the captured snapshots.", quietAll.GetProperty("message").GetString()!, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheScopedConfigAndQueryStoreHealthOverloads_KeepTheirInMemoryFilter_ListAware_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database filter test.");

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
            var captured = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-20));

            foreach (var db in new[] { A, B, C })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO database_scoped_config (config_id, capture_time, server_id, server_name, database_name, configuration_name, value, value_for_secondary)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    CollectionIdGenerator.Next(), captured, ServerId, ServerName, db, "MAXDOP", "8", null);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
                    CollectionIdGenerator.Next(), captured, ServerId, ServerName, db, "READ_WRITE", "READ_WRITE", 0, 100L, 1000L, "AUTO", 30L, 200L, 60L, "AUTO", "ON");
            }

            Task<string> Scoped(DatabaseFilter f) =>
                DarlingMcpConfigHistoryTools.GetDatabaseScopedConfig(postgres, ServerName, f, CancellationToken.None);
            Task<string> Health(DatabaseFilter f) =>
                DarlingMcpConfigHistoryTools.GetQueryStoreHealth(postgres, ServerName, f, CancellationToken.None);

            foreach (var read in new Func<DatabaseFilter, Task<string>>[] { Scoped, Health })
            {
                Assert.Equal([A, B, C], GroupDatabases(Root(await read(DatabaseFilter.All))));
                Assert.Equal([A, B], GroupDatabases(Root(await read(Of(A, B)))));
                Assert.Equal([A, C], GroupDatabases(Root(await read(Of(C, A)))));
                Assert.Equal([B], GroupDatabases(Root(await read(DatabaseFilter.One(B)))));

                // The match stays case-insensitive, as the one-name filter always was.
                Assert.Equal([A, B], GroupDatabases(Root(await read(Of(A.ToLowerInvariant(), B.ToUpperInvariant())))));

                // M3: whitespace-only is every database.
                Assert.Equal([A, B, C], GroupDatabases(Root(await read(DatabaseFilter.One("   ")))));

                // Names that match nothing keep the answer they always gave: a data answer with no databases.
                var none = Root(await read(Of("CfgFilterMissingOne", "CfgFilterMissingTwo")));
                Assert.Equal(0, none.GetProperty("database_count").GetInt32());
            }

            // The public methods are the one-name reads: a name, nothing, and whitespace.
            Assert.Equal(await Scoped(DatabaseFilter.One(B)), await DarlingMcpConfigHistoryTools.GetDatabaseScopedConfig(postgres, ServerName, B));
            Assert.Equal(await Scoped(DatabaseFilter.All), await DarlingMcpConfigHistoryTools.GetDatabaseScopedConfig(postgres, ServerName, "  "));
            Assert.Equal(await Health(DatabaseFilter.One(B)), await DarlingMcpConfigHistoryTools.GetQueryStoreHealth(postgres, ServerName, B));
            Assert.Equal(await Health(DatabaseFilter.All), await DarlingMcpConfigHistoryTools.GetQueryStoreHealth(postgres, ServerName, "  "));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>With no rows in the store the unavailable answer is the server's, whatever the filter: a store with no
    /// scoped-config or Query Store rows must not read as "no such database".</summary>
    [Fact]
    public async Task AnEmptyStore_AnswersUnavailableForTheServer_WhateverTheFilter_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database filter test.");

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

            foreach (var filter in new[] { DatabaseFilter.All, DatabaseFilter.One(A), Of(A, B) })
            {
                var scoped = Root(await DarlingMcpConfigHistoryTools.GetDatabaseScopedConfig(postgres, ServerName, filter, CancellationToken.None));
                var health = Root(await DarlingMcpConfigHistoryTools.GetQueryStoreHealth(postgres, ServerName, filter, CancellationToken.None));
                Assert.Equal(scoped.GetProperty("status").GetString(), health.GetProperty("status").GetString());
                Assert.DoesNotContain(A, scoped.GetProperty("message").GetString()!, StringComparison.Ordinal);
                Assert.DoesNotContain(A, health.GetProperty("message").GetString()!, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>One wide sys.databases capture row: the collector's shape with the recovery model set and the rest at
    /// plausible defaults, so only recovery_model moves between captures.</summary>
    private static async Task PlantDatabaseConfigAsync(
        NpgsqlConnection connection, DateTime captureTime, string databaseName, string recoveryModel, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name,
     state_desc, compatibility_level, collation_name, recovery_model, is_read_only,
     is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on,
     is_auto_update_stats_async_on, is_read_committed_snapshot_on, snapshot_isolation_state,
     is_parameterization_forced, is_query_store_on, is_encrypted, is_trustworthy_on, is_db_chaining_on,
     is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on, log_reuse_wait_desc, page_verify_option,
     target_recovery_time_seconds, delayed_durability, is_accelerated_database_recovery_on,
     is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1, $2, $3, $4, $5,
        'ONLINE', 160, 'SQL_Latin1_General_CP1_CI_AS', $6, FALSE,
        FALSE, FALSE, TRUE, TRUE,
        FALSE, TRUE, 'OFF',
        FALSE, TRUE, FALSE, FALSE, FALSE,
        FALSE, FALSE, FALSE, 'NOTHING', 'CHECKSUM',
        60, 'DISABLED', FALSE,
        FALSE, FALSE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(captureTime), ServerId, ServerName, databaseName, recoveryModel);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM database_config WHERE server_id = {ServerId}; " +
            $"DELETE FROM database_scoped_config WHERE server_id = {ServerId}; " +
            $"DELETE FROM query_store_health WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
