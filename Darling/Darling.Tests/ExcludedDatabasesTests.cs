/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Excluded Databases picker's population from the store's collected database list (the fix for
/// the PR #1403 "manual text editor" downscope). Two halves: the pure <see
/// cref="ExcludedDatabasesDialog.BuildDatabaseItems"/> merge (collected ∪ existing exclusions, checked
/// state, not-collected preservation, dedup/order — no store) and the load-bearing store SQL pinned
/// against the generated schema (no live Postgres, matching the ViewerDailySummarySqlTests convention).
/// </summary>
public sealed class ExcludedDatabasesPickerTests
{
    [Fact]
    public void BuildDatabaseItems_CollectedAreCheckboxes_CheckedWhenExcluded_SortedByName()
    {
        var collected = new[] { "AppB", "AppA", "AppC" };   // deliberately unsorted
        var existing = new[] { "AppB" };

        var items = ExcludedDatabasesDialog.BuildDatabaseItems(collected, existing);

        Assert.Equal(new[] { "AppA", "AppB", "AppC" }, items.Select(i => i.Name));   // sorted
        Assert.All(items, i => Assert.True(i.IsCollected));
        Assert.All(items, i => Assert.True(i.IsEnabled));
        Assert.False(items.Single(i => i.Name == "AppA").IsExcluded);
        Assert.True(items.Single(i => i.Name == "AppB").IsExcluded);   // already excluded → checked
        Assert.False(items.Single(i => i.Name == "AppC").IsExcluded);
    }

    [Fact]
    public void BuildDatabaseItems_PreservesExclusionsTheStoreHasNotCollected_AsCheckedNotCollectedRows()
    {
        var collected = new[] { "AppA" };
        var existing = new[] { "AppA", "ZombieDb" };   // ZombieDb no longer collected

        var items = ExcludedDatabasesDialog.BuildDatabaseItems(collected, existing);

        var zombie = items.Single(i => i.Name == "ZombieDb");
        Assert.True(zombie.IsExcluded);          // preserved, checked — a save must not drop it
        Assert.False(zombie.IsCollected);
        Assert.True(zombie.IsEnabled);           // still removable (viewer can't verify liveness)
        Assert.Contains("not collected", zombie.DisplayName, StringComparison.OrdinalIgnoreCase);

        /* Collected rows come first, the not-collected exclusion last. */
        Assert.Equal(new[] { "AppA", "ZombieDb" }, items.Select(i => i.Name));
    }

    [Fact]
    public void BuildDatabaseItems_IsCaseInsensitive_NoDuplicateForDifferentlyCasedExclusion()
    {
        var items = ExcludedDatabasesDialog.BuildDatabaseItems(new[] { "MyDb" }, new[] { "mydb" });

        var item = Assert.Single(items);
        Assert.Equal("MyDb", item.Name);   // collected casing wins, no separate "mydb" row
        Assert.True(item.IsExcluded);
        Assert.True(item.IsCollected);
    }

    [Fact]
    public void BuildDatabaseItems_NoCollectedList_SeedsExistingExclusionsAsManualRows()
    {
        var items = ExcludedDatabasesDialog.BuildDatabaseItems(Array.Empty<string>(), new[] { "X", "Y" });

        Assert.Equal(2, items.Count);
        Assert.All(items, i => Assert.True(i.IsExcluded));
        Assert.All(items, i => Assert.False(i.IsCollected));
    }

    [Fact]
    public void CollectedDatabaseNamesSql_UnionsBothStores_ExcludesSystemDatabases_PgDialect()
    {
        var sql = ViewerDataService.CollectedDatabaseNamesSql;

        Assert.Contains("FROM v_database_config", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_database_size_stats", sql, StringComparison.Ordinal);
        Assert.Contains("UNION", sql, StringComparison.Ordinal);
        Assert.Contains("NOT IN ('master', 'model', 'msdb', 'tempdb')", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY database_name", sql, StringComparison.Ordinal);

        /* Positional param, no bare now(), no N'' literals, no @-params. */
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
    }

    [Fact]
    public void CollectedDatabaseNamesSql_ReadsDatabaseNameColumnThatExistsInBothSourceTables()
    {
        Assert.Contains("database_name", PgSchemaGenerator.CreateTable(DatabaseConfigCollector.Instance), StringComparison.Ordinal);
        Assert.Contains("database_name", PgSchemaGenerator.CreateTable(DatabaseSizeStatsCollector.Instance), StringComparison.Ordinal);
    }
}

/// <summary>
/// Live round-trip for the collected-database reads against a dev Postgres (skipped unless DARLING_TEST_PG
/// is set): the picker's list is the DISTINCT user databases across <c>v_database_config</c> and
/// <c>v_database_size_stats</c>, system databases removed.
/// </summary>
/* #1776: seeds and reads registry + database rows on the SHARED store, so it must serialize with the 63 classes
   that already do — measured failing in the third of three consecutive full-suite runs against one long-lived
   database. Only the live class takes the attribute; the sibling picker tests above touch no store. */
[Collection("live-postgres")]
public sealed class ExcludedDatabasesStoreLiveTests
{
    private const int ServerId = 990_101;
    private const string ServerName = "ExclDbTestServer";

    [Fact]
    public async Task GetCollectedDatabaseNames_ReturnsDistinctUserDatabases_AcrossBothStores_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collected-databases test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await CleanupAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            await InsertServerAsync(connection);
            var now = DateTime.UtcNow;

            /* database_config: two user DBs + a system DB (master, must be filtered). */
            await InsertDatabaseConfigAsync(connection, now, "AppA");
            await InsertDatabaseConfigAsync(connection, now, "AppB");
            await InsertDatabaseConfigAsync(connection, now, "master");
            /* database_size_stats: a DUP (AppB) + a new user DB + a system DB (tempdb). */
            await InsertDatabaseSizeAsync(connection, now, "AppB");
            await InsertDatabaseSizeAsync(connection, now, "AppC");
            await InsertDatabaseSizeAsync(connection, now, "tempdb");

            var names = await viewer.GetCollectedDatabaseNamesAsync(ServerId);

            /* DISTINCT across both stores, system DBs removed, sorted. */
            Assert.Equal(new[] { "AppA", "AppB", "AppC" }, names);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertServerAsync(NpgsqlConnection connection)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO servers (server_id, server_name, display_name) VALUES ($1, $2, $3)", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDatabaseConfigAsync(NpgsqlConnection connection, DateTime captureTimeUtc, string databaseName)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(captureTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(databaseName);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDatabaseSizeAsync(NpgsqlConnection connection, DateTime collectionTimeUtc, string databaseName)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(databaseName);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "database_config", "database_size_stats", "servers" })
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
            command.Parameters.AddWithValue(ServerId);
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
