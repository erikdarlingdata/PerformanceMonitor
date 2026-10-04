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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Pins the web Configuration tab's Database Configuration grid and Scoped Configuration table: the grid shows
/// the desktop grid's columns in its order and wording, every one of them is a key get_database_config emits,
/// and the scoped table reads get_database_scoped_config.
/// </summary>
public sealed class DatabaseConfigWebColumnsTests
{
    private static string Tab() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js")
            .ReplaceLineEndings("\n");

    private static string Block(string source, string start)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, start);
        return source[from..source.IndexOf("\n];", from, StringComparison.Ordinal)];
    }

    private static string[] Labels(string block) =>
        Regex.Matches(block, "label: \"([^\"]+)\"").Select(m => m.Groups[1].Value).ToArray();

    private static string[] Keys(string block) =>
        Regex.Matches(block, "key: \"([a-z_]+)\"").Select(m => m.Groups[1].Value).ToArray();

    [Fact]
    public void TheDatabaseGrid_HasTheDesktopGridsColumns_InItsOrderAndWording()
    {
        var expected = new[]
        {
            "Database", "State", "Compat Level", "Collation", "Recovery Model", "Read Only", "RCSI", "Snapshot Isolation",
            "Auto Create Stats", "Auto Update Stats", "Async Stats Update", "Forced Parameterization", "Query Store",
            "Encrypted", "Trustworthy", "DB Chaining", "Broker", "CDC", "Mixed Pages", "Log Reuse Wait", "Auto Close",
            "Auto Shrink", "Page Verify", "Target Recovery (s)", "Delayed Durability", "ADR", "Memory Optimized",
            "Optimized Locking",
        };
        Assert.Equal(expected, Labels(Block(Tab(), "const DB_CONFIG_COLUMNS = [")));

        /* The desktop headers are the source of the wording: every label above must appear there. */
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");
        var grid = xaml[xaml.IndexOf("x:Name=\"DatabaseConfigGrid\"", StringComparison.Ordinal)..];
        grid = grid[..grid.IndexOf("</DataGrid>", StringComparison.Ordinal)];
        var desktop = Regex.Matches(grid, "TextBlock Text=\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(expected, desktop.Where(h => h != "Collected").ToArray());
    }

    [Fact]
    public void EveryDatabaseGridKey_IsEmittedByGetDatabaseConfig()
    {
        var tool = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpConfigTools.cs");
        var start = tool.IndexOf("public static async Task<string> GetDatabaseConfig(", StringComparison.Ordinal);
        var slice = tool[start..tool.IndexOf("get_trace_flags", start, StringComparison.Ordinal)];
        var keys = Keys(Block(Tab(), "const DB_CONFIG_COLUMNS = ["));
        Assert.Equal(28, keys.Length);
        foreach (var key in keys)
            Assert.Matches("\\b" + key + " = r\\.", slice);
    }

    [Fact]
    public void TheScopedConfigTable_ReadsGetDatabaseScopedConfig_WithTheDesktopColumns()
    {
        var tab = Tab();
        Assert.Contains("readTool(\"get_database_scoped_config\", { server })", tab, StringComparison.Ordinal);
        Assert.Contains("scopedConfigPanel(server),", tab, StringComparison.Ordinal);
        var cols = Block(tab, "const SCOPED_CONFIG_COLUMNS = [");
        Assert.Equal(new[] { "Database", "Setting", "Value", "Value for Secondary", "Collected" }, Labels(cols));
        Assert.Equal(new[] { "database_name", "name", "value", "value_for_secondary", "captured_at" }, Keys(cols));
        Assert.Contains("captured_at: res.data.captured_at", tab, StringComparison.Ordinal);
    }
}

/// <summary>Live parity: get_database_config's 8 added fields equal the store row's, from a seed whose values differ per field.</summary>
[Collection("live-postgres")]
public sealed class DatabaseConfigAddedFieldsLiveTests
{
    private const string ServerName = "darling-dbconfig-added-fields";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheAddedFields_EqualTheStoreRow()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live database-config parity test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var ok = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            /* Each of the 8 flags differs from its neighbours so a swapped mapping shows. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level, collation_name, recovery_model,
    is_read_only, is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on,
    is_read_committed_snapshot_on, snapshot_isolation_state, is_parameterization_forced, is_query_store_on, is_encrypted, is_trustworthy_on,
    is_db_chaining_on, is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on, log_reuse_wait_desc, page_verify_option,
    target_recovery_time_seconds, delayed_durability, is_accelerated_database_recovery_on, is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32)",
                CollectionIdGenerator.Next(), when, ServerId, ServerName, "SeedDb", "ONLINE", 160, "Latin1_General_100_CI_AS", "FULL",
                true, false, false, true, true, false,
                true, "OFF", false, true, false, true,
                false, true, true, false, "NOTHING", "CHECKSUM",
                60, "DISABLED", false, true, false);

            var json = await DarlingMcpConfigTools.GetDatabaseConfig(postgres, ServerName);
            using var doc = JsonDocument.Parse(json);
            var row = doc.RootElement.GetProperty("databases").EnumerateArray().Single();

            var stored = (await DarlingCurrentConfigReader.GetLatestDatabaseConfigAsync(postgres, ServerId, ct)).Rows.Single();
            Assert.Equal(stored.CollationName, row.GetProperty("collation").GetString());
            Assert.Equal(stored.IsReadOnly, row.GetProperty("read_only").GetBoolean());
            Assert.Equal(stored.IsTrustworthyOn, row.GetProperty("trustworthy").GetBoolean());
            Assert.Equal(stored.IsDbChainingOn, row.GetProperty("db_chaining").GetBoolean());
            Assert.Equal(stored.IsBrokerEnabled, row.GetProperty("broker_enabled").GetBoolean());
            Assert.Equal(stored.IsCdcEnabled, row.GetProperty("cdc_enabled").GetBoolean());
            Assert.Equal(stored.IsMixedPageAllocationOn, row.GetProperty("mixed_page_allocation").GetBoolean());
            Assert.Equal(stored.IsMemoryOptimizedEnabled, row.GetProperty("memory_optimized").GetBoolean());

            /* The seed itself, so the parity above cannot pass on two equal defaults. */
            Assert.Equal("Latin1_General_100_CI_AS", row.GetProperty("collation").GetString());
            Assert.True(row.GetProperty("read_only").GetBoolean());
            Assert.True(row.GetProperty("trustworthy").GetBoolean());
            Assert.False(row.GetProperty("db_chaining").GetBoolean());
            Assert.True(row.GetProperty("broker_enabled").GetBoolean());
            Assert.True(row.GetProperty("cdc_enabled").GetBoolean());
            Assert.False(row.GetProperty("mixed_page_allocation").GetBoolean());
            Assert.True(row.GetProperty("memory_optimized").GetBoolean());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, ok, async (cleanup, cleanupCt) => await DeleteAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(
            $"DELETE FROM database_config WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
