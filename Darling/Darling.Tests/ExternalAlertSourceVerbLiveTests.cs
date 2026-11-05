/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live proof of the four external alert source verbs (#5654): on a real store, add a source, connect as the new login and
/// submit one alert, list it, rotate and see the old password refused and the new one work, remove it and see the login and its
/// rows gone. The verbs run through <see cref="DarlingCliCommands.RunAlertSourceVerbAsync(string, NpgsqlConnection, string?, string, TextWriter, TextWriter, CancellationToken)"/>,
/// the body the command line uses after it has opened the store.
///
/// <para>A cluster that trusts every connection (the CI cluster) accepts any password, so there the refusal of the old password
/// cannot be observed; the test then checks what it can, that the stored verifier changed and is a SCRAM-SHA-256 verifier. On a
/// cluster that checks passwords it asserts the refusal too.</para>
///
/// <para>The provisioning batch creates the fixed <c>admin</c>, <c>viewer</c> and <c>mcp</c> roles, which are cluster-wide, so the
/// class is in the <c>pg-cluster-roles</c> collection.</para>
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class ExternalAlertSourceVerbLiveTests
{
    /* #1776 own-store: the test mints its own scratch database through ScratchPostgres and a per-run source name; the fixed
       admin/viewer/mcp roles are why the class sits in the pg-cluster-roles collection. */
    private readonly string RunSuffix;

    private readonly string SourceName;

    private readonly string RoleName;

    private readonly string StrayName;

    /* Instance fields: xUnit builds a new instance per test, so no test sees another's roles. */
    public ExternalAlertSourceVerbLiveTests()
    {
        RunSuffix = Guid.NewGuid().ToString("N")[..8];
        SourceName = "live_" + RunSuffix;
        RoleName = ExternalAlertSourceAdmin.RoleNameFor(SourceName);
        StrayName = "stray_" + RunSuffix;
    }

    private const string GroupName = DarlingManagedRoles.AlertSourceGroupRoleName;

    private static async Task<string[]> GroupMembersAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT COALESCE(array_agg(m.rolname::text ORDER BY m.rolname::text COLLATE \"C\"), ARRAY[]::text[]) FROM pg_auth_members AS a JOIN pg_roles AS m ON m.oid = a.member WHERE a.roleid = '{GroupName}'::regrole", owner);
        return (string[])(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string ConnectionStringFrom(string output)
    {
        var match = Regex.Match(output, @"^  (Host=.+)$", RegexOptions.Multiline);
        Assert.True(match.Success, "The output carries a connection string line.");
        return match.Groups[1].Value.TrimEnd('\r');
    }

    private static async Task<long> SubmitAsync(string sourceConnectionString, string key, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(sourceConnectionString) { Pooling = false }.ConnectionString);
        await connection.OpenAsync(ct);
        return await ScalarAsync<long>(connection, $"SELECT config.submit_external_alert('fire', '{key}', 'warning', 'Disk almost full', '93 percent used', 'web01', 93.5)", ct);
    }

    /// <summary>pg_terminate_backend signals the backend and returns; the backend exits a moment later, so this waits up to ten seconds for it.</summary>
    private static async Task AssertSessionsGoneAsync(NpgsqlConnection owner, string predicate, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        long left;
        while ((left = await ScalarAsync<long>(owner, $"SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity WHERE {predicate}", ct)) > 0
            && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100, ct);
        }

        Assert.Equal(0L, left);
    }

    /// <summary>The statements pg_stat_statements recorded that carry this run's login name and a SCRAM verifier, so a failure shows them.</summary>
    private async Task<string> RecordedVerifierStatementsAsync(NpgsqlConnection owner, CancellationToken ct) =>
        await ScalarAsync<string>(owner,
            $"SELECT COALESCE(pg_catalog.string_agg(pg_catalog.left(query, 300), ' | '), '') FROM pg_stat_statements WHERE query LIKE '%{RoleName}%' AND query LIKE '%SCRAM-SHA-256$%'", ct);

    private async Task<string?> VerifierOfAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        try
        {
            return await ScalarAsync<string?>(owner, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{RoleName}'", ct);
        }
        catch (PostgresException ex) when (ex.SqlState == "42501")
        {
            return null;
        }
    }

    [Fact]
    public async Task ASameNamedGroupRoleWithoutTheMarker_StopsProvisioning_AndIsLeftAsItWas()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source verb live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct),
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");

            /* The group is cluster-wide and holds nothing, so one another class left behind is dropped here (the class is in the
               pg-cluster-roles collection, so no other provisioning batch runs meanwhile). */
            await ExecAsync(owner, $"DROP ROLE IF EXISTS {GroupName}", ct);
            await ExecAsync(owner, $"CREATE ROLE {GroupName} NOLOGIN; COMMENT ON ROLE {GroupName} IS 'someone else''s'", ct);
            var ownerRole = new NpgsqlConnectionStringBuilder(scratch.ConnectionString).Username ?? "darling";

            var refused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(owner,
                DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)),
                ct));
            Assert.Contains("already exists and was not created by Darling", refused.MessageText, StringComparison.Ordinal);
            Assert.Equal("someone else's", await ScalarAsync<string>(owner, $"SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = '{GroupName}'", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(["admin", "viewer", "web", "mcp", GroupName], RunSuffix, cleanupCt));
        }
    }

    [Fact]
    public async Task AddRotateListRemove_RunAgainstAStore_AndTheLoginFollowsEachStep()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source verb live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct),
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
            var ownerRole = new NpgsqlConnectionStringBuilder(scratch.ConnectionString).Username ?? "darling";
            await ExecAsync(owner,
                DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)),
                ct);

            async Task<(int Exit, string Output, string Error)> RunAsync(string verb, string? name)
            {
                var output = new StringWriter();
                var error = new StringWriter();
                var exit = await DarlingCliCommands.RunAlertSourceVerbAsync(verb, owner, name, scratch.ConnectionString, output, error, ct);
                return (exit, output.ToString(), error.ToString());
            }

            /* #5654: with pg_stat_statements loaded and utility tracking on for the session, the verbs turn tracking off for their
               own transaction, so no statement the view records carries the login's verifier. */
            var statementsTracked = await ScalarAsync<bool>(owner,
                "SELECT pg_catalog.current_setting('shared_preload_libraries') LIKE '%pg_stat_statements%'", ct);
            if (statementsTracked)
            {
                await ExecAsync(owner, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);
                await ExecAsync(owner, "SET pg_stat_statements.track_utility = on", ct);
            }

            /* Nothing yet. */
            var empty = await RunAsync("--list-alert-sources", null);
            Assert.Equal(0, empty.Exit);
            Assert.Contains("No alert sources", empty.Output);

            /* Add: the login, the registry row, the printed string. */
            var add = await RunAsync("--add-alert-source", SourceName);
            Assert.Equal(0, add.Exit);
            var firstString = ConnectionStringFrom(add.Output);
            var firstPassword = new NpgsqlConnectionStringBuilder(firstString).Password!;
            Assert.Equal(RoleName, new NpgsqlConnectionStringBuilder(firstString).Username);
            Assert.Equal(2, Regex.Matches(add.Output, Regex.Escape(firstPassword)).Count); /* #5654: the local string and the one for another machine */
            Assert.True(await ScalarAsync<bool>(owner, $"SELECT EXISTS (SELECT 1 FROM config.external_alert_source WHERE source_name = '{SourceName}' AND role_name = '{RoleName}')", ct));
            Assert.Equal(DarlingManagedRoles.AlertSourceRoleMarker,
                await ScalarAsync<string>(owner, $"SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = '{RoleName}'", ct));
            Assert.True(await ScalarAsync<bool>(owner,
                $"SELECT rolcanlogin AND NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolinherit AND rolconnlimit = {DarlingManagedRoles.AlertSourceConnectionLimit} FROM pg_roles WHERE rolname = '{RoleName}'", ct));
            /* #5654: the batch made the NOLOGIN group with its marker, and add made the login a member in the same transaction:
               WITH INHERIT FALSE, SET FALSE from PostgreSQL 16, so the membership carries nothing but the pg_hba match. */
            Assert.Equal(DarlingManagedRoles.AlertSourceGroupRoleMarker,
                await ScalarAsync<string>(owner, $"SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = '{GroupName}'", ct));
            Assert.True(await ScalarAsync<bool>(owner,
                $"SELECT NOT rolcanlogin AND NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolinherit FROM pg_roles WHERE rolname = '{GroupName}'", ct));
            Assert.Equal(new[] { RoleName }, await GroupMembersAsync(owner, ct));
            if (owner.PostgreSqlVersion.Major >= 16)
            {
                Assert.True(await ScalarAsync<bool>(owner,
                    $"SELECT bool_and(NOT a.inherit_option AND NOT a.set_option AND NOT a.admin_option) FROM pg_auth_members AS a WHERE a.roleid = '{GroupName}'::regrole", ct));
            }

            Assert.Equal(0L, await ScalarAsync<long>(owner, $"SELECT count(*) FROM pg_auth_members WHERE member = '{GroupName}'::regrole", ct));

            /* A role that is not a registered, marked source is revoked from the group on the next run of the step; the source stays. */
            await ExecAsync(owner, $"CREATE ROLE {StrayName} LOGIN; GRANT {GroupName} TO {StrayName}", ct);
            Assert.Equal(new[] { RoleName, StrayName }.OrderBy(n => n, StringComparer.Ordinal).ToArray(), await GroupMembersAsync(owner, ct));
            await ExecAsync(owner, DarlingManagedRoles.BuildExternalAlertSourceRolesSql("config"), ct);
            Assert.Equal(new[] { RoleName }, await GroupMembersAsync(owner, ct));

            var firstVerifier = await VerifierOfAsync(owner, ct);
            if (firstVerifier is not null)
            {
                Assert.StartsWith("SCRAM-SHA-256$", firstVerifier);
            }

            /* The password reached the server as a verifier only: no statement the statistics view recorded carries it. */
            if (await ScalarAsync<bool>(owner, "SELECT to_regclass('pg_stat_statements') IS NOT NULL", ct))
            {
                Assert.Equal(0, await ScalarAsync<long>(owner, $"SELECT count(*) FROM pg_stat_statements WHERE query LIKE '%{firstPassword}%'", ct));
            }

            if (statementsTracked)
            {
                Assert.Equal(string.Empty, await RecordedVerifierStatementsAsync(owner, ct));
            }

            /* A second add of the same name is refused, and prints no string. */
            var again = await RunAsync("--add-alert-source", SourceName);
            Assert.Equal(1, again.Exit);
            Assert.Contains("already exists", again.Error);
            Assert.DoesNotContain("Host=", again.Output);

            /* The new login submits an alert. */
            var pendingId = await SubmitAsync(firstString, "disk-full", ct);
            Assert.Equal(1, await ScalarAsync<long>(owner, $"SELECT count(*) FROM config.external_alert_pending WHERE pending_id = {pendingId} AND status = 'pending'", ct));

            /* List: the name, the last submit, one waiting alert, no secret. */
            var list = await RunAsync("--list-alert-sources", null);
            Assert.Equal(0, list.Exit);
            Assert.Contains(SourceName, list.Output);
            Assert.DoesNotContain("never", list.Output);
            Assert.DoesNotContain(firstPassword, list.Output);
            Assert.Matches(new Regex(@"\s1\s*$", RegexOptions.Multiline), list.Output);

            /* Whether this cluster checks passwords. */
            bool passwordsChecked;
            try
            {
                await using var wrong = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(firstString) { Password = "not-the-password", Pooling = false }.ConnectionString);
                await wrong.OpenAsync(ct);
                passwordsChecked = false;
            }
            catch (PostgresException ex) when (ex.SqlState == "28P01")
            {
                passwordsChecked = true;
            }

            /* #5654: a session the login opened before rotate is ended by it. */
            await using var heldBeforeRotate = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(firstString) { Pooling = false }.ConnectionString);
            await heldBeforeRotate.OpenAsync(ct);
            Assert.Equal(1, await ScalarAsync<int>(heldBeforeRotate, "SELECT 1", ct));

            /* Rotate: a new password, a new verifier, the old password refused. */
            var rotate = await RunAsync("--rotate-alert-source", SourceName);
            Assert.Equal(0, rotate.Exit);
            Assert.Contains($"Ended 1 open session(s) of login '{RoleName}'", rotate.Output, StringComparison.Ordinal);
            await AssertSessionsGoneAsync(owner, $"usename = '{RoleName}'", ct);
            await Assert.ThrowsAnyAsync<Exception>(() => ScalarAsync<int>(heldBeforeRotate, "SELECT 1", ct));

            if (statementsTracked)
            {
                Assert.Equal(string.Empty, await RecordedVerifierStatementsAsync(owner, ct));
            }
            var secondString = ConnectionStringFrom(rotate.Output);
            var secondPassword = new NpgsqlConnectionStringBuilder(secondString).Password!;
            Assert.NotEqual(firstPassword, secondPassword);
            Assert.Equal(2, Regex.Matches(rotate.Output, Regex.Escape(secondPassword)).Count);
            var secondVerifier = await VerifierOfAsync(owner, ct);
            Assert.Equal(firstVerifier is null, secondVerifier is null);
            if (secondVerifier is not null)
            {
                Assert.StartsWith("SCRAM-SHA-256$", secondVerifier);
                Assert.NotEqual(firstVerifier, secondVerifier);
            }

            if (passwordsChecked)
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(firstString, "after-rotate", ct));
                Assert.Equal("28P01", refused.SqlState);
            }

            Assert.True(await SubmitAsync(secondString, "after-rotate", ct) > pendingId);

            /* Rotating a name that is not a source is refused. */
            var missing = await RunAsync("--rotate-alert-source", "no_such_source");
            Assert.Equal(1, missing.Exit);
            Assert.Contains("No source named", missing.Error);

            /* Remove: the login, the row and the queued alerts go; the count is printed. A session the login holds is ended. */
            var loginOid = await ScalarAsync<uint>(owner, $"SELECT oid FROM pg_roles WHERE rolname = '{RoleName}'", ct);
            await using var heldBeforeRemove = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(secondString) { Pooling = false }.ConnectionString);
            await heldBeforeRemove.OpenAsync(ct);
            var remove = await RunAsync("--remove-alert-source", SourceName);
            Assert.Equal(0, remove.Exit);
            Assert.Contains("dropped", remove.Output);
            Assert.Contains($"Ended 1 open session(s) of login '{RoleName}'", remove.Output, StringComparison.Ordinal);
            await AssertSessionsGoneAsync(owner, $"usesysid = {loginOid}", ct);
            await Assert.ThrowsAnyAsync<Exception>(() => ScalarAsync<int>(heldBeforeRemove, "SELECT 1", ct));
            Assert.Contains("2 queued alert row(s), 2 of them not yet delivered", remove.Output);
            Assert.False(await ScalarAsync<bool>(owner, $"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{RoleName}')", ct));
            Assert.Empty(await GroupMembersAsync(owner, ct));
            Assert.Equal(0, await ScalarAsync<long>(owner, "SELECT count(*) FROM config.external_alert_source", ct));
            Assert.Equal(0, await ScalarAsync<long>(owner, "SELECT count(*) FROM config.external_alert_pending", ct));
            await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(secondString, "after-remove", ct));

            var gone = await RunAsync("--remove-alert-source", SourceName);
            Assert.Equal(1, gone.Exit);
            Assert.Contains("No source named", gone.Error);
            Assert.Contains("No alert sources", (await RunAsync("--list-alert-sources", null)).Output);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(["admin", "viewer", "web", "mcp", RoleName, StrayName, GroupName], RunSuffix, cleanupCt));
        }
    }
}
