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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web host's own store login. The role batch gives the <c>web</c> role every write and EXECUTE the dashboard's pages make and leaves
/// <c>viewer</c> (the login the export verbs hand to a remote read-only seat) with SELECT and the secret-column carve only. Gated on
/// DARLING_TEST_PG. Runs the shipped batch whole on a scratch database, so the roles it creates carry the fixed names and the test skips
/// when the rig already holds them.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class WebStoreLoginLiveTests
{
    /* #1776 own-store: every statement here runs on the scratch database the test created. */
    private static readonly string[] MovedFunctions =
    [
        "edit_monitored_server", "request_snapshot_now", "request_active_queries_live", "request_analyze_now",
        "record_custom_alert_resolution", "insert_known_pattern", "update_known_pattern", "delete_known_pattern",
    ];

    private const string MuteInsert =
        "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id) " +
        "VALUES ('{0}', true, now() at time zone 'utc', 'web login test', 'Blocking', 7)";

    private const string MovedTablesGrantedToViewerByAnEarlierBuild =
        "GRANT INSERT, UPDATE, DELETE ON config.config_mute_rules, config.custom_views, config.custom_alert_rules, " +
        "config.database_state_expected, config.server_tags, config.server_tag_map, config.query_tags, config.query_tag_assignments, " +
        "config.query_tag_rules, config.config_monitored_servers TO viewer; " +
        "GRANT UPDATE (dismissed) ON config.config_alert_log TO viewer; " +
        "GRANT UPDATE (config_version, updated_at) ON config.config_service TO viewer; " +
        "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA config TO viewer";

    [Fact]
    public async Task AfterTheBatch_ViewerReadsOnly_AndWebHoldsEveryWrite_AndAnEarlierViewersWritesAreTakenAwayOnOneRun()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the web store login live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);

        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct),
                "A cluster-wide admin/viewer/web/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var webPassword = DarlingManagedPostgres.GeneratePassword();
        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                var target = ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName);
                string Batch() => DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, target, ScramSha256Verifier.Create(webPassword));

                /* A store provisioned by the earlier batch: viewer holds the writes. The first run creates the roles, and the
                   grants below put the old state on top of it before the second run. */
                await ExecAsync(owner, Batch(), ct);
                await ExecAsync(owner, MovedTablesGrantedToViewerByAnEarlierBuild, ct);
                Assert.True(await ScalarAsync<bool>(owner, "SELECT has_table_privilege('viewer', 'config.config_mute_rules', 'INSERT')", ct));

                await ExecAsync(owner, Batch(), ct);

                foreach (var function in MovedFunctions)
                {
                    var holders = await ScalarAsync<string>(owner,
                        "SELECT COALESCE(string_agg(role || '=' || has_function_privilege(role, p.oid, 'EXECUTE')::text, ',' ORDER BY role), '') " +
                        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace CROSS JOIN (VALUES ('viewer'), ('web')) AS r(role) " +
                        $"WHERE n.nspname = 'config' AND p.proname = '{function}'", ct);
                    Assert.True(holders.Length > 0, $"{function} does not exist in the scratch store");
                    foreach (var entry in holders.Split(','))
                    {
                        var expectWeb = entry.StartsWith("web=", StringComparison.Ordinal);
                        Assert.True(
                            entry.EndsWith(expectWeb ? "true" : "false", StringComparison.Ordinal),
                            $"{function}: {entry} (web must hold EXECUTE, viewer must not)");
                    }
                }
            }

            var viewerString = LoginString(scratch.ConnectionString, "viewer", ProvisioningTestSecrets.ViewerPassword);
            await using (var viewer = new NpgsqlConnection(viewerString))
            {
                await viewer.OpenAsync(ct);
                Assert.Equal(0L, await ScalarAsync<long>(viewer, "SELECT count(*) FROM config.config_mute_rules", ct));
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(viewer, string.Format(MuteInsert, "v1"), ct))).SqlState);
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() =>
                    ExecAsync(viewer, "UPDATE config.config_alert_log SET dismissed = TRUE", ct))).SqlState);
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() =>
                    ExecAsync(viewer, "UPDATE config.config_service SET config_version = config_version", ct))).SqlState);
                Assert.False(await ScalarAsync<bool>(viewer, "SELECT has_table_privilege('config.custom_views', 'INSERT')", ct));
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() =>
                    ExecAsync(viewer, "SELECT config.request_snapshot_now(1, NULL, 'viewer')", ct))).SqlState);
                Assert.False(await ScalarAsync<bool>(viewer, PerformanceMonitor.Darling.Viewer.ViewerDataService.ReadOnlyProbeSql, ct));
            }

            var webString = LoginString(scratch.ConnectionString, "web", webPassword);
            await using (var web = new NpgsqlConnection(webString))
            {
                await web.OpenAsync(ct);
                await ExecAsync(web, string.Format(MuteInsert, "w1"), ct);
                await ExecAsync(web, "UPDATE config.config_alert_log SET dismissed = TRUE WHERE server_id = -7001", ct);
                await ExecAsync(web, "DELETE FROM config.config_mute_rules WHERE id = 'w1'", ct);
                /* The carve is the same as viewer's: a secret column stays unreadable. */
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() =>
                    ExecAsync(web, "SELECT encrypted_password FROM config.config_monitored_servers", ct))).SqlState);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    $"{LiveCleanupBatch.DropOwnedByExistingProductRolesSql} DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    /// <summary>The batch takes large-object creation and the session advisory lock functions away from every role by default, so a
    /// read-only seat cannot store data in the database through <c>lo_from_bytea</c> or hold the migration lock open. The logins that take
    /// a session lock (<c>admin</c> and <c>mcp</c>, through the agent-change undo) keep it, and the transaction-level locks the store's own
    /// functions use stay open to everyone.</summary>
    [Fact]
    public async Task AfterTheBatch_ViewerCannotCreateLargeObjects_OrHoldASessionLock_ButAdminAndMcpCan()
    {
        const long MigrationLockKey = 0x4441524C_494E47; /* PgMigrations.MigrationLockKey, "DARLING" */
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the web store login live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);

        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct),
                "A cluster-wide admin/viewer/web/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var webPassword = DarlingManagedPostgres.GeneratePassword();
        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                var target = ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName);
                await ExecAsync(owner, DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, target, ScramSha256Verifier.Create(webPassword)), ct);
            }

            foreach (var (role, password) in new[] { ("viewer", ProvisioningTestSecrets.ViewerPassword), ("web", webPassword) })
            {
                await using var connection = new NpgsqlConnection(LoginString(scratch.ConnectionString, role, password));
                await connection.OpenAsync(ct);
                foreach (var sql in new[]
                {
                    "SELECT lo_from_bytea(0, 'x'::bytea)",
                    "SELECT lo_create(0)",
                    "SELECT lo_creat(-1)",
                    $"SELECT pg_advisory_lock({MigrationLockKey})",
                    $"SELECT pg_try_advisory_lock({MigrationLockKey})",
                    $"SELECT pg_advisory_lock_shared({MigrationLockKey})",
                    "SELECT pg_advisory_lock(1, 2)",
                })
                {
                    Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, sql, ct))).SqlState);
                }

                /* The transaction-level forms the store's own functions and the stores take are still open to it. */
                await ExecAsync(connection, $"BEGIN; SELECT pg_advisory_xact_lock({MigrationLockKey}); SELECT pg_try_advisory_xact_lock(1, 2); COMMIT", ct);
            }

            foreach (var (role, password) in new[] { ("admin", ProvisioningTestSecrets.AdminPassword), ("mcp", ProvisioningTestSecrets.McpPassword) })
            {
                await using var connection = new NpgsqlConnection(LoginString(scratch.ConnectionString, role, password));
                await connection.OpenAsync(ct);
                await ExecAsync(connection, $"SELECT pg_advisory_lock({MigrationLockKey}); SELECT pg_advisory_unlock({MigrationLockKey})", ct);
                Assert.True(await ScalarAsync<bool>(connection, $"SELECT pg_try_advisory_lock({MigrationLockKey})", ct));
                await ExecAsync(connection, $"SELECT pg_advisory_unlock({MigrationLockKey})", ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    "DROP OWNED BY admin, viewer, web, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    [Fact]
    public void TheBatch_NamesNoPgHbaLineForWeb_AndTheNetworkRoleFieldRefusesIt()
    {
        /* The web login is local to the service host: pg_hba admits only viewer, admin and the alert-source group. */
        Assert.Throws<ArgumentOutOfRangeException>(() => DarlingManagedPostgres.PgHbaRoleField(DarlingManagedPostgres.WebRoleName));
        Assert.Contains("viewer", DarlingManagedPostgres.PgHbaRoleField(DarlingManagedPostgres.ViewerRoleName), StringComparison.Ordinal);
    }

    [Fact]
    public void TheWebCredentialIsItsOwnFile_BesideTheViewerOne()
    {
        var dataDirectory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "store-web-login", "data");
        var web = DarlingManagedPostgres.WebCredentialPathFor(dataDirectory);
        var viewer = DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory);
        Assert.NotEqual(viewer, web);
        Assert.Equal(System.IO.Path.GetDirectoryName(viewer), System.IO.Path.GetDirectoryName(web));
        Assert.Equal("pg-web-credential.dpapi", System.IO.Path.GetFileName(web));
        Assert.Equal("pg-web-credential", DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.WebRoleName));
        Assert.Contains("pg-web-credential", DarlingManagedRoles.CredentialDirectoryFileNames);
    }

    [Fact]
    public void PlanPasswordReassert_ReassertsTheWebRole_OnlyWhenItsStoredVerifierDoesNotAcceptTheFile()
    {
        var password = DarlingManagedPostgres.GeneratePassword();
        var stored = new Dictionary<string, string?>
        {
            ["admin"] = ProvisioningTestSecrets.Admin,
            ["viewer"] = ProvisioningTestSecrets.Viewer,
            ["mcp"] = ProvisioningTestSecrets.Mcp,
        };
        Assert.Equal(
            PasswordReassert.Web,
            DarlingManagedRoles.PlanPasswordReassert(
                stored, ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.ViewerPassword, ProvisioningTestSecrets.McpPassword, password));
        stored["web"] = ScramSha256Verifier.Create(password);
        Assert.Equal(
            PasswordReassert.None,
            DarlingManagedRoles.PlanPasswordReassert(
                stored, ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.ViewerPassword, ProvisioningTestSecrets.McpPassword, password));
    }

    private static string LoginString(string ownerConnectionString, string user, string password) =>
        new NpgsqlConnectionStringBuilder(ownerConnectionString)
        {
            Username = user,
            Password = password,
            SearchPath = "collect,config,public",
            Pooling = false,
        }.ConnectionString;

    private static string OwnerRoleOf(NpgsqlConnection owner)
    {
        using var command = new NpgsqlCommand("SELECT current_user::text", owner);
        return (string)command.ExecuteScalar()!;
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
