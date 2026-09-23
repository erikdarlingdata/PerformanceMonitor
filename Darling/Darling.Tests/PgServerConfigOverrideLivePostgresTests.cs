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

            /* The server-wide section is UNCHANGED by the override rows: the page is the tool's default view
               (include_defaults = false) — the two CHOSEN settings; wal_level at its default is counted in the
               snapshot but not paged. A reader missing its predicate would report four settings with two
               duplicate names. */
            Assert.Equal(2, withOverrides.GetProperty("settings_returned").GetInt32());
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
            var serverWide = await NamesAsync(connection, ct, DarlingPgServerConfigReader.CurrentConfigSql, includeDefaults: true, limit: 50, snapshot);
            Assert.Equal(new[] { "max_connections", "shared_buffers", "work_mem" }, serverWide.OrderBy(n => n, StringComparer.Ordinal).ToArray());
            Assert.Equal(serverWide.Count, serverWide.Distinct(StringComparer.Ordinal).Count());

            await using var postgres = NpgsqlDataSource.Create(cs!);
            var rows = await DarlingPgServerConfigReader.GetOverridesAsync(postgres, ServerId, ct);
            Assert.Equal(2, rows.Count);
            /* The both-scoped row is the third shape pg_db_role_setting has, and it must survive the reader's
               record intact rather than collapsing onto one scope. */
            Assert.Contains(rows, r => r.DatabaseName == "tenant_db" && r.RoleName == "reporting_role" && r.Setting == "1048576");
            Assert.Contains(rows, r => r.DatabaseName == "tenant_db" && r.RoleName is null && r.Setting == "262144");
            /* Every override row carries the snapshot it came from — the same newest capture the page read anchors on
               (the latest-anchored-read census requires the anchor projected; this is what it is for). */
            Assert.All(rows, r => Assert.Equal(DarlingMcpTestData.Naive(snapshot), r.CollectionTime));

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
    /// #3937: the change feed over a planted four-snapshot history that straddles the V138 stamp. t0 is before
    /// <c>applied_at</c> (server-wide rows only, as every pre-V138 snapshot is); t1 is the FIRST snapshot at or
    /// after it and already carries two overrides - baseline, the collector started seeing them, so neither is a
    /// SET; t2 moves the server-wide <c>work_mem</c>, moves the database override's value, and adds a
    /// database+role override; t3 removes the role override. Exactly four rows come back, newest first, the
    /// server-wide row ahead of the override rows at the same instant. Then the tool: a server-wide row renders
    /// byte-identically with and without override rows present, and with none present the page carries no
    /// scoped key at all.
    /// </summary>
    [Fact]
    public async Task TheChangeFeed_ReportsOverrideChangesSetsAndResets_AndLeavesTheServerWideRowsUnchanged()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3937 change-feed round trip.");

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

            DateTime appliedAt;
            using (var stamp = new NpgsqlCommand("SELECT applied_at FROM darling_schema_version WHERE version = 138", connection))
            {
                appliedAt = DateTime.SpecifyKind((DateTime)(await stamp.ExecuteScalarAsync(ct))!, DateTimeKind.Utc);
            }

            var t0 = DarlingMcpTestData.TruncateToSeconds(appliedAt).AddHours(-1);
            var t1 = DarlingMcpTestData.TruncateToSeconds(appliedAt).AddSeconds(1);
            var t2 = t1.AddSeconds(1);
            var t3 = t2.AddSeconds(1);

            foreach (var t in new[] { t0, t1, t2, t3 })
            {
                await SeedAsync(connection, ct, t, "work_mem", t < t2 ? "4096" : "8192", "configuration file", "4096", null, null);
                await SeedAsync(connection, ct, t, "statement_timeout", "30000", "configuration file", "0", null, null);
            }

            /* t1: the first post-V138 snapshot, overrides already there - baseline, never "set". */
            await SeedAsync(connection, ct, t1, "work_mem", "262144", "database", null, "tenant_db", null);
            await SeedAsync(connection, ct, t1, "statement_timeout", "0", "role", null, null, "reporting_role");
            /* t2: the database override's value moves, the role override is unchanged, a database+role one appears. */
            await SeedAsync(connection, ct, t2, "work_mem", "524288", "database", null, "tenant_db", null);
            await SeedAsync(connection, ct, t2, "statement_timeout", "0", "role", null, null, "reporting_role");
            await SeedAsync(connection, ct, t2, "work_mem", "1048576", "database+role", null, "tenant_db", "reporting_role");
            /* t3: the role override is RESET; the other two stay. */
            await SeedAsync(connection, ct, t3, "work_mem", "524288", "database", null, "tenant_db", null);
            await SeedAsync(connection, ct, t3, "work_mem", "1048576", "database+role", null, "tenant_db", "reporting_role");

            var rows = await DarlingPgServerConfigReader.GetConfigChangesAsync(postgres, ServerId, t0.AddMinutes(-1), t3.AddMinutes(1), 50, ct);
            Assert.Equal(new (DateTime, string, string?, string?, string?, string?, string)[]
            {
                (DarlingMcpTestData.Naive(t3), "statement_timeout", null, "reporting_role", "0", null, "reset"),
                (DarlingMcpTestData.Naive(t2), "work_mem", null, null, "4096", "8192", "changed"),
                (DarlingMcpTestData.Naive(t2), "work_mem", "tenant_db", null, "262144", "524288", "changed"),
                (DarlingMcpTestData.Naive(t2), "work_mem", "tenant_db", "reporting_role", null, "1048576", "set"),
            }, rows.Select(r => (r.ChangedAtUtc, r.Name, r.DatabaseName, r.RoleName, r.OldValue, r.NewValue, r.ChangeKind)).ToArray());

            /* The tool, anchored just after t3 so the planted history is inside its window on any store age. */
            var asOf = t3.AddMinutes(1).ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);
            var withOverrides = JsonDocument.Parse(await DarlingMcpPgServerStateTools.GetPgServerConfigChanges(postgres, ServerName, 168, 50, asOf)).RootElement;
            Assert.Equal("config_changes", withOverrides.GetProperty("status").GetString());
            Assert.Equal(4, withOverrides.GetProperty("change_count").GetInt32());
            Assert.Contains("change_kind", withOverrides.GetProperty("scoped_changes_note").GetString(), StringComparison.Ordinal);
            var changes = withOverrides.GetProperty("changes").EnumerateArray().ToList();
            Assert.Equal(
                new[] { "changed_at", "name", "role_name", "change_kind", "old_value", "new_value", "source" },
                changes[0].EnumerateObject().Select(p => p.Name).ToArray());
            Assert.Equal(JsonValueKind.Null, changes[0].GetProperty("new_value").ValueKind);
            Assert.Equal(
                new[] { "changed_at", "name", "database_name", "role_name", "change_kind", "old_value", "new_value", "source" },
                changes[3].EnumerateObject().Select(p => p.Name).ToArray());
            Assert.Equal(JsonValueKind.Null, changes[3].GetProperty("old_value").ValueKind);
            Assert.False(changes[2].TryGetProperty("role_name", out _));
            var serverWideWith = changes[1].GetRawText();

            /* Overrides gone: the page is the pre-#3937 page - no scoped key anywhere, the server-wide row's own
               key list, and that row's BYTES the same as when override rows sat beside it. */
            await DeleteOverrideRowsAsync(connection, ct);
            var bare = await DarlingMcpPgServerStateTools.GetPgServerConfigChanges(postgres, ServerName, 168, 50, asOf);
            foreach (var key in new[] { "scoped_changes_note", "change_kind", "database_name", "role_name" })
            {
                Assert.DoesNotContain(key, bare, StringComparison.Ordinal);
            }

            var bareRoot = JsonDocument.Parse(bare).RootElement;
            Assert.Equal(
                new[] { "server", "hours_back", "status", "change_count", "truncated", "note", "changes" },
                bareRoot.EnumerateObject().Select(p => p.Name).ToArray());
            var only = Assert.Single(bareRoot.GetProperty("changes").EnumerateArray().ToList());
            Assert.Equal(
                new[] { "changed_at", "name", "old_value", "new_value", "unit", "source", "context", "description" },
                only.EnumerateObject().Select(p => p.Name).ToArray());
            Assert.Equal(serverWideWith, only.GetRawText());

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
        NpgsqlConnection connection, CancellationToken ct, string sql, bool includeDefaults, int limit, DateTime snapshot)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(includeDefaults);
        command.Parameters.AddWithValue(limit);
        /* #3974: CurrentConfigSql now binds a $4 lower bound; the planted snapshot is minutes old, so the
           day bound finds it directly. */
        command.Parameters.AddWithValue(DarlingPgServerConfigReader.ConfigSnapshotLowerBounds(snapshot.AddMinutes(3))[0]);
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
