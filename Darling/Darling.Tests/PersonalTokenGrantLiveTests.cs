/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V275 (#8737): after managed provisioning no login but the store owner reads or writes <c>config.mcp_personal_token</c>, and a stray GRANT
/// made later still cannot write it, because both owner-only triggers are <c>ENABLE ALWAYS</c>. The roles are cluster-wide, so the class is
/// in the <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class PersonalTokenGrantLiveTests
{
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp", StrayRole];

    private const string StrayRole = "ptk8737stray";

    private const string InsertSql = "INSERT INTO config.mcp_personal_token (token_id, secret_sha256, owner_key, subject, token_name, role, created_at, expires_at) VALUES ('0123456789abcdef', decode(repeat('ab', 32), 'hex'), 'owner', 'subject', 'name', 'reader', now(), now() + interval '1 day')";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token grant live tests.");
        return connectionString!;
    }

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

    private static async Task<NpgsqlConnection> LoginAsync(ScratchPostgres scratch, string role, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = PasswordOf(role), Pooling = false }.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task AssertDeniedAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, sql, ct));
        Assert.Equal("42501", ex.SqlState);
    }

    private static string PasswordOf(string role) => role switch
    {
        "admin" => ProvisioningTestSecrets.AdminPassword,
        "viewer" => ProvisioningTestSecrets.ViewerPassword,
        "web" => ProvisioningTestSecrets.WebPassword,
        _ => ProvisioningTestSecrets.McpPassword,
    };

    /// <summary>Runs the body on a migrated, provisioned scratch store with the owner connection open, then drops the roles.</summary>
    private static async Task WithProvisionedStoreAsync(Func<NpgsqlConnection, ScratchPostgres, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            var exist = await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True";
            Assert.SkipWhen(exist, "A cluster-wide admin/viewer/web/mcp role already exists on this cluster; the grants would adopt it.");
            var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
            await ExecAsync(owner,
                DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName), ProvisioningTestSecrets.Web), ct);
            await body(owner, scratch, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "personaltoken8737none", cleanupCt));
        }
    }

    private static Task AssertCannotSelectAsync(string role) => WithProvisionedStoreAsync(async (owner, scratch, ct) =>
    {
        await using var login = await LoginAsync(scratch, role, ct);
        await AssertDeniedAsync(login, "SELECT token_id FROM config.mcp_personal_token", ct);
    });

    [Fact]
    public Task Web_CannotSelect() => AssertCannotSelectAsync("web");

    [Fact]
    public Task Mcp_CannotSelect() => AssertCannotSelectAsync("mcp");

    [Fact]
    public Task Admin_CannotSelect() => AssertCannotSelectAsync("admin");

    [Fact]
    public Task Viewer_CannotSelect() => AssertCannotSelectAsync("viewer");

    [Fact]
    public Task Web_CannotInsert() => WithProvisionedStoreAsync(async (owner, scratch, ct) =>
    {
        await using var login = await LoginAsync(scratch, "web", ct);
        await AssertDeniedAsync(login, InsertSql, ct);
    });

    [Fact]
    public Task Owner_CanInsertAndSelect() => WithProvisionedStoreAsync(async (owner, scratch, ct) =>
    {
        await ExecAsync(owner, InsertSql, ct);
        Assert.Equal("0123456789abcdef", await ScalarAsync(owner, "SELECT token_id FROM config.mcp_personal_token", ct));
        await ExecAsync(owner, "UPDATE config.mcp_personal_token SET last_used_at = now()", ct);
        await ExecAsync(owner, "DELETE FROM config.mcp_personal_token", ct);
        await ExecAsync(owner, "TRUNCATE config.mcp_personal_token", ct);
    });

    private static async Task GrantStrayAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await ExecAsync(owner, $"CREATE ROLE {StrayRole} NOLOGIN", ct);
        await ExecAsync(owner, $"GRANT USAGE ON SCHEMA config TO {StrayRole}", ct);
        await ExecAsync(owner, $"GRANT SELECT ON config.mcp_personal_token TO {StrayRole}", ct);
        await ExecAsync(owner, $"GRANT UPDATE (token_name) ON config.mcp_personal_token TO {StrayRole}", ct);
        Assert.Equal("True", await ScalarAsync(owner, $"SELECT has_table_privilege('{StrayRole}', 'config.mcp_personal_token', 'SELECT')", ct));
    }

    private static async Task AssertStrayLostEverythingAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        Assert.Equal("False", await ScalarAsync(owner, $"SELECT has_table_privilege('{StrayRole}', 'config.mcp_personal_token', 'SELECT')", ct));
        Assert.Equal("False", await ScalarAsync(owner, $"SELECT has_column_privilege('{StrayRole}', 'config.mcp_personal_token', 'token_name', 'UPDATE')", ct));
        Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM pg_attribute AS a CROSS JOIN LATERAL aclexplode(a.attacl) AS x "
            + $"WHERE a.attrelid = 'config.mcp_personal_token'::regclass AND x.grantee = '{StrayRole}'::regrole", ct));
    }

    /// <summary>A role beyond the four logins that holds a grant on the table (custom default privileges can give one) loses it, table and column
    /// grants both, when the managed-roles batch runs again (the access group tables' pattern).</summary>
    [Fact]
    public Task StrayRoleGrant_IsRevokedByTheManagedBatch() => WithProvisionedStoreAsync(async (owner, scratch, ct) =>
    {
        await GrantStrayAsync(owner, ct);
        var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";

        await ExecAsync(owner,
            DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName), ProvisioningTestSecrets.Web), ct);

        await AssertStrayLostEverythingAsync(owner, ct);
    });

    /// <summary>The same, when the rung's own SQL runs (a store migrated with such a grant already in place).</summary>
    [Fact]
    public Task StrayRoleGrant_IsRevokedByTheRung() => WithProvisionedStoreAsync(async (owner, scratch, ct) =>
    {
        await GrantStrayAsync(owner, ct);

        await ExecAsync(owner, PersonalTokenSchema.CreateSql, ct);

        await AssertStrayLostEverythingAsync(owner, ct);
    });

    /// <summary>E3: a GRANT of every write made by mistake still leaves each write refused by the owner-only trigger.</summary>
    [Fact]
    public Task StrayGrant_StillCannotWrite() => WithProvisionedStoreAsync(async (owner, scratch, ct) =>
    {
        await ExecAsync(owner, InsertSql, ct);
        await ExecAsync(owner, "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON config.mcp_personal_token TO web", ct);
        await using var login = await LoginAsync(scratch, "web", ct);
        await AssertDeniedAsync(login, InsertSql.Replace("0123456789abcdef", "fedcba9876543210", StringComparison.Ordinal), ct);
        await AssertDeniedAsync(login, "UPDATE config.mcp_personal_token SET token_name = 'x'", ct);
        await AssertDeniedAsync(login, "DELETE FROM config.mcp_personal_token", ct);
        await AssertDeniedAsync(login, "TRUNCATE config.mcp_personal_token", ct);
        Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.mcp_personal_token", ct));
    });

    /// <summary>E2: no login but the owner writes a token event to the journal. The web login has no INSERT on it; the mcp and admin logins hold the
    /// writer columns, so the journal's owner-only rule refuses them. Each refusal is 42501.</summary>
    [Theory]
    [InlineData("web", PersonalTokenEvents.Created)]
    [InlineData("web", PersonalTokenEvents.Revoked)]
    [InlineData("web", PersonalTokenEvents.ExpiryCut)]
    [InlineData("mcp", PersonalTokenEvents.Created)]
    [InlineData("mcp", PersonalTokenEvents.Revoked)]
    [InlineData("mcp", PersonalTokenEvents.ExpiryCut)]
    [InlineData("admin", PersonalTokenEvents.Created)]
    [InlineData("admin", PersonalTokenEvents.Revoked)]
    [InlineData("admin", PersonalTokenEvents.ExpiryCut)]
    public Task JournalEvents_AreOwnerOnly(string role, string journalEvent) => WithProvisionedStoreAsync(async (owner, scratch, ct) =>
    {
        await using var login = await LoginAsync(scratch, role, ct);
        await AssertDeniedAsync(login,
            $"INSERT INTO config.investigation_journal (event, actor, detail) VALUES ('{journalEvent}', 'person:eve', '{{}}'::jsonb)", ct);
        Assert.Equal("0", await ScalarAsync(owner, $"SELECT count(*) FROM config.investigation_journal WHERE event = '{journalEvent}'", ct));
    });

    [Fact]
    public Task Triggers_AreEnabledAlways() => WithProvisionedStoreAsync(async (owner, scratch, ct) =>
    {
        Assert.Equal("A,A", await ScalarAsync(owner,
            "SELECT string_agg(tgenabled::text, ',' ORDER BY tgname) FROM pg_trigger WHERE tgrelid = 'config.mcp_personal_token'::regclass AND tgname IN ('trg_mcp_personal_token_owner_only_row', 'trg_mcp_personal_token_owner_only_truncate')", ct));
    });
}
