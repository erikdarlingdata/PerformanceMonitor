/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9485: the grants on <c>config.investigation_change_windows</c> on a store whose roles came from the shipped <c>provision-roles.sql</c> (a store the
/// operator provisions himself). The script ran before the service created the table, so <c>mcp</c> has no default read on <c>config</c>. The migration
/// grants the read itself, so <c>list_investigation_requests</c> (whose read now joins the table) keeps working with no re-run of the script, and no role
/// can write the table. The fixed-name roles are cluster-wide, so the class is in the <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class InvestigationChangeWindowGrantsLiveTests
{
    /* #1776 own-store: the test mints its own scratch database through ScratchPostgres; the fixed admin/viewer/web/mcp roles are why the class sits in
       the pg-cluster-roles collection. */
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
    }

    [Fact]
    public async Task AStoreProvisionedByTheScript_KeepsListInvestigationRequestsWorkingForMcp_AfterTheServiceMigratesIt()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the change window grant live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True",
                "A cluster-wide admin/viewer/web/mcp role already exists on this rig; the grants would adopt it.");
            Assert.SkipUnless(
                await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct) == "True",
                "The script names the store owner 'darling', and this rig has no such role.");

            const string AdminPassword = "ScriptAdmin9485", ViewerPassword = "ScriptViewer9485", WebPassword = "ScriptWeb9485", McpPassword = "ScriptMcp9485";
            var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                    System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                .Replace("CHANGE_ME_ADMIN_PASSWORD", AdminPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", ViewerPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_WEB_PASSWORD", WebPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", McpPassword, StringComparison.Ordinal);

            /* The store as it was before the rung: no window table and no hold columns. The operator provisions the roles on it. */
            await ExecAsync(owner, @"
ALTER TABLE config.investigation_requests DROP COLUMN IF EXISTS held_window_id, DROP COLUMN IF EXISTS hold_released_at;
DROP TABLE IF EXISTS config.investigation_change_windows;
DELETE FROM darling_schema_version WHERE version >= " + InvestigationChangeWindows.Version.ToString(CultureInfo.InvariantCulture), ct);
            await ExecAsync(owner, script, ct);

            /* The service starts and migrates the store: the rung creates the table as the owner, after the script ran. */
            await PgMigrations.MigrateAsync(owner, ct);

            foreach (var role in s_cleanupRoles)
            {
                Assert.Equal("True", await ScalarAsync(owner, $"SELECT has_table_privilege('{role}', 'config.investigation_change_windows', 'SELECT')", ct));
                Assert.Equal("False", await ScalarAsync(owner,
                    $"SELECT has_table_privilege('{role}', 'config.investigation_change_windows', 'INSERT') OR has_table_privilege('{role}', 'config.investigation_change_windows', 'UPDATE') "
                    + $"OR has_table_privilege('{role}', 'config.investigation_change_windows', 'DELETE')", ct));
            }

            /* The read itself, as the real mcp login. */
            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = "mcp", Password = McpPassword, Pooling = false }.ConnectionString;
            await using var mcpSource = NpgsqlDataSource.Create(mcpString);
            var mcpStore = new InvestigationRequestStore(mcpSource, 30);
            Assert.Empty(await mcpStore.ListAsync(null, null, null, 10, null, ct));
            Assert.Empty((await mcpStore.ReadChangeWindowsAsync(null, 168, 20, 0, ct)).Windows);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "cwgrants9485none", cleanupCt));
        }
    }
}
