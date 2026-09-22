/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V138 (#3691) against a live, MIGRATED store: the two things no in-process pin can execute — that
/// <see cref="DarlingPgServerConfigReader.OverrideSql"/> and the widened server-wide statements PARSE and
/// resolve against the real column set, and that <c>get_pg_server_config</c>'s answer changes in exactly one
/// place when override rows are present.
///
/// <para><b>The byte-identity arm is the load-bearing one.</b> A store with only server-wide rows must
/// produce the SAME JSON it produced before this rung — not "the same plus two nulls". The section is
/// attached only when the cluster has overrides, which is the attach pattern the tool family uses for an
/// answer that is not always an answer, and the way to assert it is to compare the whole payload against a
/// run whose only difference is the override rows. So the test reads the tool twice against the same
/// snapshot: once with the two override rows present and once with them deleted, and asserts the second
/// answer carries NO <c>database_overrides</c> key at all and is byte-identical to a third read taken before
/// the overrides were ever planted.</para>
///
/// <para>The rung's DDL, ladder, probe, collector and reader census are
/// <see cref="PgServerConfigScopeRungTests"/>; this file is the round trip.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgServerConfigOverrideLivePostgresTests
{
    private const int ServerId = -138138;
    private const string ServerName = "pg-v138-override-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheOverrideSectionIsPresentOnlyWhenTheClusterHasOverrides_AndTheServerWideAnswerIsUnchanged()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V138 override round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        /* Migrated FIRST: OverrideSql names V138 columns, so a store below this rung cannot even parse it. */
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var snapshot = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-3);

            /* Three server-wide rows: two chosen, one at default. */
            await SeedAsync(connection, ct, snapshot, "work_mem", "4096", "configuration file", "4096", null, null);
            await SeedAsync(connection, ct, snapshot, "statement_timeout", "30000", "configuration file", "0", null, null);
            await SeedAsync(connection, ct, snapshot, "wal_level", "replica", "default", "replica", null, null);

            var beforeOverrides = await DarlingMcpPgServerStateTools.GetPgServerConfig(postgres, ServerName, 50);
            Assert.DoesNotContain("database_overrides", beforeOverrides, StringComparison.Ordinal);

            /* Two override rows in the SAME snapshot: one database-scoped (ALTER DATABASE … SET) and one
               role-scoped (ALTER ROLE … SET). Both name a setting the server-wide rows also carry, which is
               the collision every reader's predicate exists for. */
            await SeedAsync(connection, ct, snapshot, "work_mem", "262144", "database", null, "tenant_db", null);
            await SeedAsync(connection, ct, snapshot, "statement_timeout", "0", "role", null, null, "reporting_role");

            var withOverrides = JsonDocument.Parse(await DarlingMcpPgServerStateTools.GetPgServerConfig(postgres, ServerName, 50)).RootElement;
            Assert.Equal("server_config", withOverrides.GetProperty("status").GetString());

            /* The server-wide section is UNCHANGED by the override rows: three settings, two chosen. A
               reader missing its predicate would report five settings with two duplicate names. */
            Assert.Equal(3, withOverrides.GetProperty("settings_returned").GetInt32());
            Assert.Equal(2, withOverrides.GetProperty("non_default_count").GetInt32());
            Assert.False(withOverrides.GetProperty("truncated").GetBoolean());
            var names = withOverrides.GetProperty("settings").EnumerateArray().Select(s => s.GetProperty("name").GetString()).ToList();
            Assert.Equal(names.Count, names.Distinct(StringComparer.Ordinal).Count());

            /* Both overrides, both scopes, ordered database-first with NULLs last. */
            var overrides = withOverrides.GetProperty("database_overrides").EnumerateArray()
                .Select(o => (
                    Database: o.GetProperty("database_name").ValueKind == JsonValueKind.Null ? null : o.GetProperty("database_name").GetString(),
                    Role: o.GetProperty("role_name").ValueKind == JsonValueKind.Null ? null : o.GetProperty("role_name").GetString(),
                    Name: o.GetProperty("name").GetString(),
                    Setting: o.GetProperty("setting").GetString()))
                .ToList();

            Assert.Equal(new (string?, string?, string?, string?)[]
            {
                ("tenant_db", null, "work_mem", "262144"),
                (null, "reporting_role", "statement_timeout", "0"),
            }, overrides);

            Assert.Contains("ALTER DATABASE", withOverrides.GetProperty("database_overrides_note").GetString(), StringComparison.Ordinal);

            /* Delete the two override rows and the answer must return to the earlier BYTES exactly — the
               section absent, not empty, and nothing else moved. */
            await DeleteOverrideRowsAsync(connection, ct);
            var afterOverrides = await DarlingMcpPgServerStateTools.GetPgServerConfig(postgres, ServerName, 50);
            Assert.DoesNotContain("database_overrides", afterOverrides, StringComparison.Ordinal);
            Assert.Equal(beforeOverrides, afterOverrides);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// The <c>CONFIG_PG_*</c> facts' own read is a dictionary keyed by setting NAME, so this is the shape a
    /// duplicate name breaks hardest: the load either throws on the duplicate key or silently takes whichever
    /// row arrived last. Run verbatim against the same planted snapshot, with the overrides present, it must
    /// return exactly the three server-wide rows.
    /// </summary>
    [Fact]
    public async Task TheFactCollectorsConfigRead_SeesOnlyTheServerWideRows_WithOverridesPresent()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V138 fact-read round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var snapshot = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-3);

            await SeedAsync(connection, ct, snapshot, "work_mem", "4096", "configuration file", "4096", null, null);
            await SeedAsync(connection, ct, snapshot, "shared_buffers", "16384", "configuration file", "16384", null, null);
            await SeedAsync(connection, ct, snapshot, "max_connections", "100", "default", "100", null, null);
            await SeedAsync(connection, ct, snapshot, "work_mem", "262144", "database", null, "tenant_db", null);
            await SeedAsync(connection, ct, snapshot, "work_mem", "1048576", "database+role", null, "tenant_db", "reporting_role");

            /* The server-wide read the shared reader serves the Overview grid and the MCP page from, and the
               override read beside it, both against the real column set: the pair that could only be checked
               by a server. */
            var serverWide = await NamesAsync(connection, ct, DarlingPgServerConfigReader.CurrentConfigSql, includeDefaults: true, limit: 50);
            Assert.Equal(new[] { "max_connections", "shared_buffers", "work_mem" }, serverWide.OrderBy(n => n, StringComparer.Ordinal).ToArray());
            Assert.Equal(serverWide.Count, serverWide.Distinct(StringComparer.Ordinal).Count());

            await using var postgres = NpgsqlDataSource.Create(cs!);
            var rows = await DarlingPgServerConfigReader.GetOverridesAsync(postgres, ServerId, ct);
            Assert.Equal(2, rows.Count);
            /* The both-scoped row is the third shape pg_db_role_setting has, and it must survive the reader's
               record intact rather than collapsing onto one scope. */
            Assert.Contains(rows, r => r.DatabaseName == "tenant_db" && r.RoleName == "reporting_role" && r.Setting == "1048576");
            Assert.Contains(rows, r => r.DatabaseName == "tenant_db" && r.RoleName is null && r.Setting == "262144");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<List<string>> NamesAsync(
        NpgsqlConnection connection, CancellationToken ct, string sql, bool includeDefaults, int limit)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(includeDefaults);
        command.Parameters.AddWithValue(limit);
        using var reader = await command.ExecuteReaderAsync(ct);
        var names = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(reader.GetOrdinal("name")));
        }

        return names;
    }

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime,
        string name, string setting, string source, string? bootVal, string? databaseName, string? roleName)
    {
        using var insert = new NpgsqlCommand(
            """
            INSERT INTO collect.pg_server_config
                (collection_id, collection_time, server_id, server_name, name, setting, source, boot_val,
                 reset_val, sourceline, pending_restart, database_name, role_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $6, 0, false, $9, $10)
            """, connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.Naive(collectionTime));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(name);
        insert.Parameters.AddWithValue(setting);
        insert.Parameters.AddWithValue(source);
        insert.Parameters.AddWithValue((object?)bootVal ?? DBNull.Value);
        insert.Parameters.AddWithValue((object?)databaseName ?? DBNull.Value);
        insert.Parameters.AddWithValue((object?)roleName ?? DBNull.Value);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteOverrideRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            "DELETE FROM collect.pg_server_config WHERE server_id = $1 AND (database_name IS NOT NULL OR role_name IS NOT NULL)", connection);
        cleanup.Parameters.AddWithValue(ServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM collect.pg_server_config WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(ServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
