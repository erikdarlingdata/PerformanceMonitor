/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite schema v68 (#5640), the twin of Darling's V179: the <c>attention_events</c> collector table and
/// <c>config_mute_rules.client_app_pattern</c>. The collector table comes from the catalog walk that runs right after the
/// migrations (so a database older than v68 gets it there and the v68 step names only the column); the column goes through
/// the shared added-columns list. Pins the ladder entry, the fresh-store shape, and the climb on a real DuckDB file from a
/// v67 database that has neither.
/// </summary>
public sealed class ClientTimeoutsSchemaV68Tests : IDisposable
{
    private readonly string _tempDir;

    public ClientTimeoutsSchemaV68Tests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ClientTimeoutsSchemaV68Tests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public void TheV68Block_AddsTheMuteRuleColumn_ThroughTheSharedList_AndTheSchemaIsAtLeast68()
    {
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 68);

        var source = Lite.Tests.ParitySource.ReadFile("Lite/Database/DuckDbInitializer.cs");
        var start = source.IndexOf("if (fromVersion < 68)", StringComparison.Ordinal);
        Assert.True(start >= 0, "DuckDbInitializer has no v68 block");
        var block = source[start..];

        Assert.Contains("(68, \"config_mute_rules\", \"client_app_pattern\", \"VARCHAR\")", source, StringComparison.Ordinal);
        Assert.Contains("AddMissingColumnsAsync(connection, AddedColumnsForVersion(68))", block, StringComparison.Ordinal);
        Assert.Contains("Running migration to v68", block, StringComparison.Ordinal);
        Assert.Contains("#5640", block, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCollectorTable_IsInTheDuckDbStoredSet_AndAppendsInTheCatalogsColumnOrder()
    {
        var definition = CollectorCatalog.Find("attention");
        Assert.NotNull(definition);
        Assert.Equal("attention_events", definition!.TargetTable);
        Assert.Contains(definition, DuckDbSchemaGenerator.StoredCollectors);

        var ddl = DuckDbSchemaGenerator.CreateTable(definition);
        Assert.Contains("CREATE TABLE IF NOT EXISTS attention_events", ddl, StringComparison.Ordinal);
        Assert.Contains("client_app_name VARCHAR", ddl, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFreshStore_HasTheTableAndTheColumn_AtTheCurrentVersion()
    {
        var dbPath = Path.Combine(_tempDir, "lite-fresh.duckdb");
        using var initializer = new DuckDbInitializer(dbPath);
        await initializer.InitializeAsync();

        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync();

        Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, Convert.ToInt64(await ScalarAsync(conn, "SELECT MAX(version) FROM schema_version")));
        Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(conn, "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'attention_events'")));
        Assert.Equal("VARCHAR:YES", await ScalarAsync(conn,
            "SELECT data_type || ':' || is_nullable FROM information_schema.columns WHERE table_name = 'config_mute_rules' AND column_name = 'client_app_pattern'"));
    }

    [Fact]
    public async Task AV67Database_ClimbsToV68_GainsTheTableAndTheColumn_AndAnOldRuleReadsNull()
    {
        var dbPath = Path.Combine(_tempDir, "lite-v67.duckdb");

        using (var initializer = new DuckDbInitializer(dbPath))
        {
            await initializer.InitializeAsync();
        }

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();

            /* The shape of a v67 database: no attention table (and nothing that depends on it), no mute-rule column, one old rule. */
            foreach (var view in await NamesAsync(conn, "SELECT view_name FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%attention_events%'"))
            {
                await ExecAsync(conn, $"DROP VIEW IF EXISTS {view}");
            }

            foreach (var index in await NamesAsync(conn, "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'attention_events'"))
            {
                await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");
            }

            await ExecAsync(conn, "DROP TABLE attention_events");

            foreach (var index in await NamesAsync(conn, "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'config_mute_rules'"))
            {
                await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");
            }

            await ExecAsync(conn, "ALTER TABLE config_mute_rules DROP COLUMN client_app_pattern");
            await ExecAsync(conn, "INSERT INTO config_mute_rules (id, enabled, created_at_utc, metric_name) VALUES ('old-rule', TRUE, TIMESTAMP '2026-10-01 00:00:00', 'High CPU')");
            await ExecAsync(conn, "DELETE FROM schema_version");
            await ExecAsync(conn, "INSERT INTO schema_version (version) VALUES (67)");
        }

        using (var upgraded = new DuckDbInitializer(dbPath))
        {
            await upgraded.InitializeAsync();
        }

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, Convert.ToInt64(await ScalarAsync(conn, "SELECT MAX(version) FROM schema_version")));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(conn, "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'attention_events'")));
            Assert.Equal("VARCHAR:YES", await ScalarAsync(conn,
                "SELECT data_type || ':' || is_nullable FROM information_schema.columns WHERE table_name = 'config_mute_rules' AND column_name = 'client_app_pattern'"));
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT client_app_pattern FROM config_mute_rules WHERE id = 'old-rule'"));
        }
    }

    private static async Task<List<string>> NamesAsync(DuckDBConnection conn, string sql)
    {
        var names = new List<string>();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }
}
