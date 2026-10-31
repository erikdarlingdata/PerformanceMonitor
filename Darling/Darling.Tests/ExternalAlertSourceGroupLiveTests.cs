/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The members of <c>darling_alert_sources</c> on a store whose owner has CREATEROLE but is not a superuser (#5654), the common
/// bring-your-own shape. From PostgreSQL 16 such an owner becomes a member of the group, and of each source login, when it
/// creates them, through a grant recorded as made by the bootstrap superuser that it may not revoke; a pg_hba line for
/// <c>+darling_alert_sources</c> admits it. The tests prove that the add verb and the script work for that owner, that every
/// other member is revoked at any depth on the next run, and that what is left is named: by the verbs (with a reject line),
/// by the script, and by the managed pg_hba step, which leaves the alert source line out until the members are only source
/// logins.
///
/// <para>The class creates cluster-wide roles (the group, admin, viewer and mcp for the script), so it is in the
/// <c>pg-cluster-roles</c> collection.</para>
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class ExternalAlertSourceGroupLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres and its own role names (instance
       fields: xUnit builds a new instance per test, so no test sees another's roles); the cluster-wide group and
       admin/viewer/mcp roles are why the class sits in the pg-cluster-roles collection. */
    private const string GroupName = DarlingManagedRoles.AlertSourceGroupRoleName;

    private const string MonitorPassword = "Mon5654Probe";

    /* A role named all: pg_hba reads the bare word as every role, so each pg_hba line that names it must quote it. */
    private const string KeywordRoleName = "all";

    private readonly string Suffix;

    private readonly string OwnerName;

    private readonly string OtherName;

    private readonly string MonitorName;

    private readonly string SourceName;

    private readonly string RoleName;

    private readonly string[] CleanupRoles;

    public ExternalAlertSourceGroupLiveTests()
    {
        Suffix = Guid.NewGuid().ToString("N")[..8];
        OwnerName = "byo_owner_" + Suffix;
        OtherName = "other_" + Suffix;
        MonitorName = "mon_" + Suffix;
        SourceName = "grp_" + Suffix;
        RoleName = ExternalAlertSourceAdmin.RoleNameFor(SourceName);
        CleanupRoles = ["admin", "viewer", "web", "mcp", GroupName, RoleName, OtherName, OwnerName, MonitorName, KeywordRoleName];
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

    /// <summary>
    /// A migrated scratch store with the submit function, and a LOGIN CREATEROLE NOSUPERUSER role that holds every privilege on
    /// the store's objects WITH GRANT OPTION (what an owner needs) but owns none of them, so it acts as a bring-your-own owner.
    /// No admin, viewer or mcp role exists (a store the script never ran on).
    /// </summary>
    private async Task<NpgsqlConnection?> PrepareAsync(ScratchPostgres scratch, NpgsqlConnection superuser, CancellationToken ct, bool mayTurnTrackingOff = true)
    {
        await PgMigrations.MigrateAsync(superuser, ct);
        if (await ScalarAsync<bool>(superuser, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct)
            || superuser.PostgreSqlVersion.Major < 16)
        {
            return null;
        }

        await ExecAsync(superuser, $"DROP ROLE IF EXISTS {GroupName}", ct);
        await ExecAsync(superuser, DarlingManagedRoles.BuildExternalAlertSubmitFunctionSql("config"), ct);
        await ExecAsync(superuser, $@"
CREATE ROLE {OwnerName} LOGIN CREATEROLE NOSUPERUSER;
GRANT ALL ON DATABASE ""{scratch.DatabaseName}"" TO {OwnerName} WITH GRANT OPTION;
GRANT ALL ON SCHEMA config, collect TO {OwnerName} WITH GRANT OPTION;
GRANT ALL ON ALL TABLES IN SCHEMA config, collect TO {OwnerName} WITH GRANT OPTION;
GRANT ALL ON ALL SEQUENCES IN SCHEMA config, collect TO {OwnerName} WITH GRANT OPTION;
GRANT ALL ON FUNCTION config.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature}) TO {OwnerName} WITH GRANT OPTION;", ct);

        /* With pg_stat_statements loaded and utility tracking on, the verbs send no password statement for an owner that may
           not turn the tracking off (#5654), so the owner gets SET on it, as tools/provision-roles.sql tells a DBA to do; a test of that
           refusal passes false. */
        if (mayTurnTrackingOff && await SettingOrNullAsync(superuser, "pg_stat_statements.track_utility", ct) is not null)
        {
            await ExecAsync(superuser, $"GRANT SET ON PARAMETER pg_stat_statements.track_utility TO {OwnerName}", ct);
        }

        var owner = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName, Pooling = false }.ConnectionString);
        await owner.OpenAsync(ct);
        return owner;
    }

    /// <summary>
    /// Takes every role this test made, and the fixed-name ones, out of the cluster; see <see cref="LiveCleanupBatch.RemoveRolesAsync"/>.
    /// A role that cannot be dropped fails the cleanup naming it, and no role ending in this test's suffix is left.
    /// </summary>
    private Task CleanUpAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await new LiveCleanupBatch(cleanup).RemoveRolesAsync(CleanupRoles, Suffix, cleanupCt));

    private static async Task<(int Exit, string Output, string Error)> RunVerbAsync(
        NpgsqlConnection owner, string connectionString, string verb, string name, PostgresConfig postgres, CancellationToken ct)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await DarlingCliCommands.RunAlertSourceVerbAsync(verb, owner, name, connectionString, output, error, ct, postgres);
        return (exit, output.ToString(), error.ToString());
    }

    /// <summary>
    /// Gives <c>config.config_monitored_servers</c> back to the scratch superuser, so the cleanup's DROP OWNED BY on the owner
    /// does not have to drop a table other tables reference.
    /// </summary>
    private static async Task GiveTheServerTableBackAsync(ScratchPostgres scratch)
    {
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(CancellationToken.None);
            await ExecAsync(connection, "ALTER TABLE config.config_monitored_servers OWNER TO SESSION_USER", CancellationToken.None);
        }
        catch (PostgresException)
        {
            /* The table is missing or the body never got that far; the cleanup reports any residue. */
        }
    }

    /// <summary>The shipped script's DO block that takes pg_stat_statements from PUBLIC.</summary>
    private static string ScriptStatementStatsBlock()
    {
        var script = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql").Replace("\r\n", "\n", StringComparison.Ordinal);
        var comment = script.IndexOf("-- pg_stat_statements gives PUBLIC its views and functions;", StringComparison.Ordinal);
        Assert.True(comment > 0, "provision-roles.sql has the pg_stat_statements block.");
        var start = script.IndexOf("DO $do$\n", comment, StringComparison.Ordinal);
        var end = script.IndexOf("END $do$;", start, StringComparison.Ordinal) + "END $do$;".Length;
        return script[start..end];
    }

    /// <summary>The alert source block of the shipped script: the DO block that makes the group.</summary>
    private static string ScriptAlertSourceBlock()
    {
        var script = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql").Replace("\r\n", "\n", StringComparison.Ordinal);
        var create = script.IndexOf("CREATE ROLE darling_alert_sources", StringComparison.Ordinal);
        Assert.True(create > 0, "provision-roles.sql has the block that makes darling_alert_sources.");
        var start = script.LastIndexOf("DO $do$\n", create, StringComparison.Ordinal);
        var end = script.IndexOf("END $do$;", create, StringComparison.Ordinal) + "END $do$;".Length;
        return script[start..end];
    }

    [Fact]
    public async Task ACreateroleOwner_AddsASource_AndEveryMemberBesidesTheSourceLogins_IsRevokedOrNamed()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;

            /* The add verb, run by the CREATEROLE owner on a store with no admin, viewer or mcp role, makes the group and the login
               and sets only what that owner may set. */
            var add = await RunVerbAsync(owner!, ownerConnectionString, "--add-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(add.Exit == 0, add.Error);
            Assert.Contains($"created (login '{RoleName}')", add.Output, StringComparison.Ordinal);
            Assert.True(await ScalarAsync<bool>(superuser,
                $"SELECT rolcanlogin AND NOT rolinherit AND rolconnlimit = {DarlingManagedRoles.AlertSourceConnectionLimit} FROM pg_roles WHERE rolname = '{RoleName}'", ct));
            Assert.True(await ScalarAsync<bool>(superuser, $"SELECT NOT rolcanlogin AND NOT rolinherit FROM pg_roles WHERE rolname = '{GroupName}'", ct));

            /* PostgreSQL 16 made the owner a member of the group and of the login, granted by the bootstrap superuser; the owner
               cannot revoke that, so it is named, with the reject line for a bring-your-own pg_hba.conf. */
            Assert.Equal(new[] { OwnerName }, await DarlingManagedRoles.ReadAlertSourceGroupExtrasAsync(owner!, ct));
            Assert.Contains($"'{OwnerName}'", add.Output, StringComparison.Ordinal);
            Assert.Contains($"  hostssl <database> {OwnerName} <range> reject", add.Output, StringComparison.Ordinal);

            /* A role granted the source login (by the owner, which holds ADMIN on it) is a member of the group through it. The next
               run of the step, by the same owner, walks down through the login and revokes it. */
            await ExecAsync(superuser, $"CREATE ROLE {OtherName} LOGIN", ct);
            await ExecAsync(owner!, $"GRANT {RoleName} TO {OtherName}", ct);
            Assert.Equal(new[] { OtherName, OwnerName }.OrderBy(n => n, StringComparer.Ordinal), await DarlingManagedRoles.ReadAlertSourceGroupExtrasAsync(owner!, ct));
            await ExecAsync(owner!, DarlingManagedRoles.BuildExternalAlertSourceRolesSql("config"), ct);
            Assert.Equal(new[] { OwnerName }, await DarlingManagedRoles.ReadAlertSourceGroupExtrasAsync(owner!, ct));

            /* The managed pg_hba step leaves the alert source line out while that member remains, and keeps the others. */
            string[] networkRoles = ["viewer", DarlingManagedPostgres.AlertSourcesNetworkToken];
            var withExtra = DarlingManagedPostgres.BuildNetworkPgHbaLines(
                DarlingManagedPostgres.AdmittedNetworkRoles(networkRoles, await DarlingManagedRoles.ReadAlertSourceGroupExtrasAsync(superuser, ct)), "192.0.2.0/24");
            Assert.DoesNotContain("+" + GroupName, withExtra, StringComparison.Ordinal);
            Assert.Contains("hostssl darling viewer 192.0.2.0/24 scram-sha-256", withExtra, StringComparison.Ordinal);

            /* The script's block, run by the same owner (the script creates admin, viewer and mcp first): a group that was given
               LOGIN and CREATEROLE is set back, attribute by attribute; an explicit grant of the login is revoked; the owner is
               named with the reject line. */
            await ExecAsync(superuser, "CREATE ROLE admin NOLOGIN; CREATE ROLE viewer NOLOGIN; CREATE ROLE mcp NOLOGIN", ct);
            await ExecAsync(superuser, $"ALTER ROLE {GroupName} LOGIN CREATEROLE", ct);
            await ExecAsync(owner!, $"GRANT {RoleName} TO {OtherName}", ct);
            var notices = new List<string>();
            owner!.Notice += (_, e) => notices.Add(e.Notice.MessageText);
            await ExecAsync(owner, ScriptAlertSourceBlock(), ct);
            Assert.True(await ScalarAsync<bool>(superuser, $"SELECT NOT rolcanlogin AND NOT rolcreaterole FROM pg_roles WHERE rolname = '{GroupName}'", ct),
                string.Join("\n", notices));
            Assert.Equal(new[] { OwnerName }, await DarlingManagedRoles.ReadAlertSourceGroupExtrasAsync(owner, ct));
            Assert.Contains(notices, n => n.Contains($"\"{OwnerName}\"", StringComparison.Ordinal)
                && n.Contains("the role running this script", StringComparison.Ordinal)
                && n.Contains($"hostssl {scratch.DatabaseName} {OwnerName} <range> reject", StringComparison.Ordinal));

            /* A superuser revokes what the owner could not; then the members are exactly the source logins and the line is written. */
            await ExecAsync(superuser, DarlingManagedRoles.BuildExternalAlertSourceRolesSql("config"), ct);
            var clean = await DarlingManagedRoles.ReadAlertSourceGroupExtrasAsync(superuser, ct);
            Assert.Empty(clean);
            Assert.Contains($"hostssl darling +{GroupName} 192.0.2.0/24 scram-sha-256",
                DarlingManagedPostgres.BuildNetworkPgHbaLines(DarlingManagedPostgres.AdmittedNetworkRoles(networkRoles, clean), "192.0.2.0/24"), StringComparison.Ordinal);
            Assert.Equal(new[] { RoleName }, await ScalarAsync<string[]>(superuser,
                $"SELECT array_agg(m.rolname::text) FROM pg_auth_members AS a JOIN pg_roles AS m ON m.oid = a.member WHERE a.roleid = '{GroupName}'::regrole", ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// What a source login can read in the store's own schemas once the managed batch has run (#5654): the submit function and
    /// nothing else. System catalogs are left out (every PostgreSQL login reads those), and so are TimescaleDB's own schemas.
    /// </summary>
    private const string ReadableByLoginSql = @"
WITH objects AS (
   SELECT n.nspname, 'relation ' || n.nspname || '.' || c.relname AS name
   FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
   WHERE c.relkind IN ('r', 'v', 'm', 'p', 'f')
     AND has_schema_privilege($1, n.oid, 'USAGE')
     AND (has_table_privilege($1, c.oid, 'SELECT') OR has_any_column_privilege($1, c.oid, 'SELECT'))
   UNION ALL
   SELECT n.nspname, 'function ' || n.nspname || '.' || p.proname
   FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
   WHERE has_schema_privilege($1, n.oid, 'USAGE')
     AND has_function_privilege($1, p.oid, 'EXECUTE')
     AND NOT EXISTS (SELECT 1 FROM pg_depend AS d WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e'
                     AND d.refobjid <> COALESCE((SELECT e.oid FROM pg_extension AS e WHERE e.extname = 'pg_stat_statements'), 0))
)
SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::text[])
FROM objects
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
  AND nspname NOT LIKE 'pg\_%'
  AND nspname NOT LIKE '\_timescaledb%'
  AND nspname NOT LIKE 'timescaledb%'
  AND nspname <> 'toolkit_experimental'";

    [Fact]
    public async Task ASourceLogin_ReadsNothingOfTheStoresOwnObjects_ButTheSubmitFunction()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
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
            await ExecAsync(owner, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public", ct);
            await ExecAsync(owner, StoreStatementStats.BuildRevokePublicSql("public", withInfoView: true), ct);
            var ownerRole = new NpgsqlConnectionStringBuilder(scratch.ConnectionString).Username ?? "darling";
            await ExecAsync(owner,
                DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)),
                ct);
            var add = await RunVerbAsync(owner, scratch.ConnectionString, "--add-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(add.Exit == 0, add.Error);

            await using var command = new NpgsqlCommand(ReadableByLoginSql, owner);
            command.Parameters.AddWithValue(RoleName);
            var readable = (string[])(await command.ExecuteScalarAsync(ct))!;
            Assert.True(readable.SequenceEqual(new[] { "function config.submit_external_alert" }), string.Join(" | ", readable));
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task ACreateroleOwner_RefusedTheLoginStep_GetsTheStatements_AndNothingOfTheLoginIsKept()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (ADMIN OPTION rules) and no cluster-wide admin/viewer/mcp role.");
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;

            /* A group a superuser made: the owner holds no ADMIN OPTION on it, so it may not change it or grant it. */
            await ExecAsync(superuser, $"CREATE ROLE {GroupName} NOLOGIN NOINHERIT; COMMENT ON ROLE {GroupName} IS '{DarlingManagedRoles.AlertSourceGroupRoleMarker}'", ct);

            var add = await RunVerbAsync(owner!, ownerConnectionString, "--add-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(add.Exit == 0, add.Error);
            Assert.Contains("does not exist yet", add.Output, StringComparison.Ordinal);
            Assert.Contains("nothing of the login is applied yet", add.Output, StringComparison.Ordinal);
            Assert.Contains($"CREATE ROLE \"{RoleName}\"", add.Output, StringComparison.Ordinal);
            Assert.False(await ScalarAsync<bool>(superuser, $"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{RoleName}')", ct));
            Assert.True(await ScalarAsync<bool>(superuser, $"SELECT EXISTS (SELECT 1 FROM config.external_alert_source WHERE source_name = '{SourceName}')", ct));

            /* A login the owner did not make: rotate changes nothing and says so. */
            await ExecAsync(superuser, $"CREATE ROLE {RoleName} LOGIN PASSWORD 'unchanged'; COMMENT ON ROLE {RoleName} IS '{DarlingManagedRoles.AlertSourceRoleMarker}'", ct);
            var before = await ScalarAsync<string>(superuser, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{RoleName}'", ct);
            var rotate = await RunVerbAsync(owner!, ownerConnectionString, "--rotate-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(rotate.Exit == 0, rotate.Error);
            Assert.Contains("nothing is changed yet", rotate.Output, StringComparison.Ordinal);
            Assert.DoesNotContain("no longer works", rotate.Output, StringComparison.Ordinal);
            Assert.Equal(before, await ScalarAsync<string>(superuser, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{RoleName}'", ct));

            /* Remove: the registry row goes, the login is left with the statements that drop it. */
            var remove = await RunVerbAsync(owner!, ownerConnectionString, "--remove-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(remove.Exit == 0, remove.Error);
            Assert.Contains($"DROP ROLE \"{RoleName}\"", remove.Output, StringComparison.Ordinal);
            Assert.False(await ScalarAsync<bool>(superuser, $"SELECT EXISTS (SELECT 1 FROM config.external_alert_source WHERE source_name = '{SourceName}')", ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    private static async Task<string?> SettingOrNullAsync(NpgsqlConnection connection, string setting, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT pg_catalog.current_setting($1, true)", connection);
        command.Parameters.AddWithValue(setting);
        return await command.ExecuteScalarAsync(ct) as string;
    }

    /// <summary>
    /// With <c>pg_stat_statements</c> loaded and utility tracking on, a CREATEROLE owner cannot turn the tracking off (the
    /// setting needs a superuser or a grant to SET it), so the add verb sends no password statement: the source is registered,
    /// the login is not made, nothing in the statement statistics carries a verifier, and the text says why and prints the
    /// statements for a database administrator. Those statements begin with the guards: run as a superuser on a session that
    /// logs every statement and tracks utility statements, they make the login, and neither the statistics nor the session's
    /// log setting keeps the password statement (#5654).
    /// </summary>
    [Fact]
    public async Task ACreateroleOwner_WhoCannotTurnTrackingOff_SendsNoPasswordStatement_AndTheDbaStatementsCarryTheGuards()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct, mayTurnTrackingOff: false);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            var tracking = await SettingOrNullAsync(superuser, "pg_stat_statements.track_utility", ct);
            Assert.SkipWhen(tracking is null || !string.Equals(tracking, "on", StringComparison.OrdinalIgnoreCase),
                "pg_stat_statements must be in shared_preload_libraries with track_utility on (its default) on this cluster.");
            Assert.False(await ScalarAsync<bool>(superuser, $"SELECT rolsuper FROM pg_roles WHERE rolname = '{OwnerName}'", ct));
            await ExecAsync(superuser, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);
            var recorded = $"SELECT count(*) FROM pg_stat_statements WHERE query LIKE '%SCRAM-SHA-256%' AND query LIKE '%{RoleName}%'";
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;

            var add = await RunVerbAsync(owner!, ownerConnectionString, "--add-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(add.Exit == 0, add.Error);
            Assert.Contains($"its login '{RoleName}' does not exist yet", add.Output, StringComparison.Ordinal);
            Assert.Contains("pg_stat_statements.track_utility is on", add.Output, StringComparison.Ordinal);
            Assert.Contains("No password statement was sent.", add.Output, StringComparison.Ordinal);
            Assert.False(await ScalarAsync<bool>(superuser, $"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{RoleName}')", ct));
            Assert.True(await ScalarAsync<bool>(superuser, $"SELECT EXISTS (SELECT 1 FROM config.external_alert_source WHERE role_name = '{RoleName}')", ct));
            Assert.Equal(0L, await ScalarAsync<long>(superuser, recorded, ct));

            /* The printed statements, as a database administrator runs them on a session that logs every statement, once the
               group the script makes exists. */
            await ExecAsync(superuser, DarlingManagedRoles.BuildExternalAlertSourceRolesSql("config"), ct);
            var start = add.Output.IndexOf("-- Run these", StringComparison.Ordinal);
            var endMarker = "WITH INHERIT FALSE, SET FALSE;";
            var end = add.Output.IndexOf(endMarker, start, StringComparison.Ordinal) + endMarker.Length;
            Assert.True(start >= 0 && end > start, add.Output);
            var dbaSql = add.Output[start..end];
            Assert.True(dbaSql.IndexOf("SET pg_stat_statements.track_utility = off;", StringComparison.Ordinal)
                < dbaSql.IndexOf("CREATE ROLE", StringComparison.Ordinal));
            await using (var dba = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Pooling = false }.ConnectionString))
            {
                await dba.OpenAsync(ct);
                await ExecAsync(dba, "SET log_statement = 'all'", ct);
                await ExecAsync(dba, dbaSql, ct);
                Assert.Equal("none", await SettingOrNullAsync(dba, "log_statement", ct));
                Assert.Equal("off", await SettingOrNullAsync(dba, "pg_stat_statements.track_utility", ct));
            }

            Assert.True(await ScalarAsync<bool>(superuser, $"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{RoleName}')", ct));
            Assert.Equal(0L, await ScalarAsync<long>(superuser, recorded, ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// On a store where PUBLIC still holds CREATE on schema public, a source login plants
    /// <c>public.shobj_description(oid, text)</c>, which an unqualified call with a text literal would pick over the catalog
    /// function. The rotate verb and the group read after it, run as the store owner (whose search path has public), do not run
    /// it; the same unqualified call on that owner session does, so the planted function is live (#5654).
    /// </summary>
    [Fact]
    public async Task AFunctionASourceLoginPlantsInPublic_IsNotRunByTheVerbs()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            await ExecAsync(superuser, "GRANT CREATE ON SCHEMA public TO PUBLIC", ct);
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;
            var postgres = new PostgresConfig { Managed = false };

            var add = await RunVerbAsync(owner!, ownerConnectionString, "--add-alert-source", SourceName, postgres, ct);
            Assert.True(add.Exit == 0, add.Error);
            Assert.True(await ScalarAsync<bool>(superuser, $"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{RoleName}')", ct), add.Output);

            await ExecAsync(superuser, $@"
SET ROLE {RoleName};
CREATE FUNCTION public.shobj_description(oid, text) RETURNS text LANGUAGE plpgsql AS $f$
BEGIN
   PERFORM pg_catalog.set_config('darling_test.planted', 'ran', false);
   RETURN pg_catalog.shobj_description($1, $2::name);
END $f$;
RESET ROLE", ct);
            Assert.Contains("public", await SettingOrNullAsync(owner!, "search_path", ct), StringComparison.Ordinal);

            var rotate = await RunVerbAsync(owner!, ownerConnectionString, "--rotate-alert-source", SourceName, postgres, ct);
            Assert.True(rotate.Exit == 0, rotate.Error);
            Assert.Contains("new password set", rotate.Output, StringComparison.Ordinal);
            await DarlingManagedRoles.ReadAlertSourceGroupExtrasAsync(owner!, ct);
            Assert.True(string.IsNullOrEmpty(await SettingOrNullAsync(owner!, "darling_test.planted", ct)),
                "The store owner's verbs ran a function a source login made in public.");

            /* The control: an unqualified call on the same owner session runs the planted function. */
            await ScalarAsync<object>(owner!, "SELECT shobj_description(0::oid, 'pg_authid')", ct);
            Assert.Equal("ran", await SettingOrNullAsync(owner!, "darling_test.planted", ct));
            await ExecAsync(superuser, "DROP FUNCTION public.shobj_description(oid, text)", ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// A step that fails after add or rotate has committed (here the group read after the commit, made to throw) is reported
    /// after the committed result: the exit code is success, the one-time connection string is printed, the text never says
    /// the sources could not be changed, and the login holds the change (#5654).
    /// </summary>
    [Fact]
    public async Task AFailureAfterTheCommit_KeepsTheResultAndThePassword_AndSaysWhichStepFailed()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;
            static Task<IReadOnlyList<string>> Fail(NpgsqlConnection connection, CancellationToken token) =>
                throw new NpgsqlException("connection lost after the commit");
            string verifier = string.Empty;

            foreach (var verb in new[] { "--add-alert-source", "--rotate-alert-source" })
            {
                var output = new StringWriter();
                var error = new StringWriter();
                var exit = await DarlingCliCommands.RunAlertSourceVerbAsync(verb, owner!, SourceName, ownerConnectionString, output, error, ct,
                    new PostgresConfig { Managed = false }, Fail);

                Assert.True(exit == 0, error.ToString());
                Assert.Contains("Connection string (shown ONCE", output.ToString(), StringComparison.Ordinal);
                Assert.Contains($"Username={RoleName}", output.ToString(), StringComparison.Ordinal);
                Assert.Contains("Password=", output.ToString(), StringComparison.Ordinal);
                Assert.Contains("The change above is committed, but the read of which other roles are members of", error.ToString(), StringComparison.Ordinal);
                Assert.Contains("connection lost after the commit", error.ToString(), StringComparison.Ordinal);
                Assert.DoesNotContain("Could not change", output.ToString() + error.ToString(), StringComparison.Ordinal);

                var now = await ScalarAsync<string>(superuser, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{RoleName}'", ct);
                Assert.NotEqual(verifier, now);
                verifier = now;
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// On the store's own database, a login with <c>pg_monitor</c> (which includes <c>pg_read_all_stats</c>) keeps reading the
    /// <c>pg_stat_statements</c> view and its info view after the revoke from PUBLIC, and a source login, which holds neither,
    /// is refused (#5654).
    /// </summary>
    [Fact]
    public async Task OnTheStoreDatabase_AMonitorLoginReadsStatementStatistics_AndASourceLoginDoesNot()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            Assert.SkipWhen(
                await SettingOrNullAsync(superuser, "pg_stat_statements.track", ct) is null,
                "pg_stat_statements must be in shared_preload_libraries on this cluster.");
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;

            await ExecAsync(superuser, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public", ct);
            await ExecAsync(superuser, StoreStatementStats.BuildRevokePublicSql("public", withInfoView: true), ct);
            await ExecAsync(superuser, $"CREATE ROLE {MonitorName} LOGIN PASSWORD '{MonitorPassword}'; GRANT pg_monitor TO {MonitorName}", ct);

            var add = await RunVerbAsync(owner!, ownerConnectionString, "--add-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(add.Exit == 0, add.Error);
            await ExecAsync(superuser, $"ALTER ROLE \"{RoleName}\" PASSWORD '{MonitorPassword}'", ct);

            var views = new[] { "public.pg_stat_statements", "public.pg_stat_statements_info" };
            var monitorCs = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = MonitorName, Password = MonitorPassword, Pooling = false };
            await using (var monitor = new NpgsqlConnection(monitorCs.ConnectionString))
            {
                await monitor.OpenAsync(ct);
                foreach (var view in views)
                {
                    Assert.True(await ScalarAsync<long>(monitor, $"SELECT count(*) FROM {view}", ct) >= 0);
                }
            }

            var sourceCs = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = RoleName, Password = MonitorPassword, Pooling = false };
            await using (var source = new NpgsqlConnection(sourceCs.ConnectionString))
            {
                await source.OpenAsync(ct);
                foreach (var view in views)
                {
                    var refused = await Assert.ThrowsAsync<PostgresException>(() => ScalarAsync<long>(source, $"SELECT count(*) FROM {view}", ct));
                    Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, refused.SqlState);
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>An IPv4 address of this machine that is not loopback, to bind the exposed store to; null when there is none.</summary>
    private static string? NonLoopbackIPv4()
    {
        foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (nic.OperationalStatus != OperationalStatus.Up || nic.NetworkInterfaceType == NetworkInterfaceType.Loopback)
            {
                continue;
            }

            foreach (var unicast in nic.GetIPProperties().UnicastAddresses)
            {
                if (unicast.Address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(unicast.Address)
                    && !unicast.Address.ToString().StartsWith("169.254.", StringComparison.Ordinal))
                {
                    return unicast.Address.ToString();
                }
            }
        }

        return null;
    }

    /// <summary>
    /// On the managed store with network exposure for the alert source group (#5654): a start where the group holds a member that is
    /// not a source login leaves the pg_hba line out, then the role provisioning of the SAME start revokes the member, and the second
    /// network pass writes the line, so the line is in pg_hba.conf and live without another restart.
    /// </summary>
    [Fact]
    public async Task OnTheManagedStore_AStrayGroupMemberTheProvisioningRevokes_DoesNotKeepTheLineOutAfterThatStart_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe) to run the managed start proof.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");
        var listen = NonLoopbackIPv4();
        Assert.SkipWhen(listen is null, "This machine has no non-loopback IPv4 address to expose the store on.");

        var root = Directory.CreateTempSubdirectory("darling-hba-");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = DarlingManagedPostgresTests.FindFreeTcpPort(),
            DataDirectory = Path.Combine(root.FullName, "pg"),
            Network = new PostgresNetworkConfig { Listen = listen, AllowFrom = listen + "/32", Role = DarlingManagedPostgres.AlertSourcesNetworkToken },
        };
        var groupLine = "+" + GroupName;
        var stray = "stray_" + Suffix;
        var firstLog = new CapturingTestLogger();
        var secondLog = new CapturingTestLogger();
        var first = new DarlingManagedPostgres(config, firstLog, runtimeRoot);
        DarlingManagedPostgres? second = null;
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(8));
            var ct = timeout.Token;
            var ownerCs = new NpgsqlConnectionStringBuilder(await first.EnsureRunningAsync(ct)) { Pooling = false };
            var hbaPath = Path.Combine(config.DataDirectory, "pg_hba.conf");

            /* The first start has no group yet, so nothing is left out and the line is written. */
            Assert.True((await File.ReadAllTextAsync(hbaPath, ct)).Contains(groupLine, StringComparison.Ordinal), firstLog.Joined);

            await using (var c = new NpgsqlConnection(ownerCs.ConnectionString))
            {
                await c.OpenAsync(ct);
                await PgMigrations.MigrateAsync(c, ct);
            }

            await using (var dataSource = NpgsqlDataSource.Create(ownerCs.ConnectionString))
            {
                await DarlingManagedRoles.EnsureProvisionedAsync(dataSource, config.DataDirectory, NullLogger.Instance, ct);
                await using var c = await dataSource.OpenConnectionAsync(ct);
                await ExecAsync(c, $"CREATE ROLE {stray} NOLOGIN; GRANT {GroupName} TO {stray}", ct);
            }

            await first.StopIfStartedByThisProcessAsync();

            /* The same port and credentials come back, so a pooled connection to the stopped server would answer the next start. */
            NpgsqlConnection.ClearAllPools();

            /* A start with that member in the group leaves the line out. */
            second = new DarlingManagedPostgres(config, secondLog, runtimeRoot);
            ownerCs = new NpgsqlConnectionStringBuilder(await second.EnsureRunningAsync(ct)) { Pooling = false };
            Assert.False((await File.ReadAllTextAsync(hbaPath, ct)).Contains(groupLine, StringComparison.Ordinal), secondLog.Joined);
            Assert.True(secondLog.Joined.Contains("The pg_hba line for alert sources is left out", StringComparison.Ordinal),
                string.Join(" | ", secondLog.Joined.Split(" | ").Where(l => l.Contains("pg_hba", StringComparison.Ordinal))));

            /* The provisioning of that start revokes the member, then the worker runs the network pass again. */
            await using (var dataSource = NpgsqlDataSource.Create(ownerCs.ConnectionString))
            {
                await DarlingManagedRoles.EnsureProvisionedAsync(dataSource, config.DataDirectory, NullLogger.Instance, ct);
                await using var c = await dataSource.OpenConnectionAsync(ct);
                Assert.Empty(await DarlingManagedRoles.ReadAlertSourceGroupExtrasAsync(c, ct));
            }

            await second.ReconcileNetworkAfterRoleProvisioningAsync(ct);
            Assert.Contains(groupLine, await File.ReadAllTextAsync(hbaPath, ct), StringComparison.Ordinal);
            await using (var c = new NpgsqlConnection(ownerCs.ConnectionString))
            {
                await c.OpenAsync(ct);
                Assert.True(await ScalarAsync<bool>(c,
                    $"SELECT EXISTS (SELECT 1 FROM pg_hba_file_rules WHERE '{groupLine}' = ANY (user_name) AND error IS NULL)", ct));
            }
        }
        finally
        {
            if (second is not null)
            {
                await second.StopIfStartedByThisProcessAsync();
            }

            await first.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// The worker runs the second network pass after the role provisioning of the same start, in the managed branch, and the pass
    /// that records the work for it sits in the reconcile that runs before the collection loop (#5654).
    /// </summary>
    [Fact]
    public void TheWorkerRunsTheSecondNetworkPass_AfterTheRoleProvisioningOfTheSameStart()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs").Replace("\r\n", "\n", StringComparison.Ordinal);
        var loop = worker.IndexOf("private async Task RunCollectionLoopAsync(", StringComparison.Ordinal);
        Assert.True(loop > 0, "RunCollectionLoopAsync could not be located, so this check proves nothing");
        var provisioning = worker.IndexOf("await DarlingManagedRoles.EnsureProvisionedAsync(postgres, dataDirectory", loop, StringComparison.Ordinal);
        var rerun = worker.IndexOf("await reconcileNetworkAfterRoleProvisioning(stoppingToken);", loop, StringComparison.Ordinal);
        var composeBranch = worker.IndexOf("else if (!config.Postgres.Managed && Hosting.DarlingHostBinding.IsRunningInContainer)", loop, StringComparison.Ordinal);
        Assert.True(provisioning > loop, "the managed provisioning call is in the loop method");
        Assert.True(rerun > provisioning, "the second network pass follows the provisioning");
        Assert.True(rerun < composeBranch, "the second network pass is inside the managed branch");
        Assert.Contains(
            "managedPostgres.ReconcileNetworkAfterRoleProvisioningAsync",
            worker[..loop], StringComparison.Ordinal);

        var postgres = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedPostgres.cs").Replace("\r\n", "\n", StringComparison.Ordinal);
        var pass = postgres.IndexOf("private async Task ReconcileNetworkAsync(", StringComparison.Ordinal);
        Assert.True(pass > 0);
        var body = postgres[pass..postgres.IndexOf("await CheckStoreFirewallAsync(", pass, StringComparison.Ordinal)];
        Assert.Matches(@"\n\s+_alertSourceLineRerun = \(binDirectory, fullPlan, ownerConnectionString\);", body);
        Assert.Contains("await ReconcileNetworkAsync(rerun.BinDirectory, rerun.Plan, rerun.OwnerConnectionString, cancellationToken);", postgres, StringComparison.Ordinal);
    }

    /// <summary>
    /// A bring-your-own store whose pg_stat_statements a superuser created (the README's step): the non-superuser owner may
    /// not take it from PUBLIC, so service setup and the script both warn, with the same text, naming the owner of the
    /// extension and the statements it runs; once it runs them, PUBLIC holds nothing, the store owner's reader still reads,
    /// and neither warns again (#5654).
    /// </summary>
    [Fact]
    public async Task ASuperuserOwnedStatementStatsExtension_TheServiceAndTheScriptNameItsOwner_AndTheStatementsThatTakeItFromPublic()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            Assert.SkipWhen(
                await SettingOrNullAsync(superuser, "pg_stat_statements.track", ct) is null,
                "pg_stat_statements must be in shared_preload_libraries on this cluster.");

            await ExecAsync(superuser, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public", ct);
            await ExecAsync(superuser, $"ALTER TABLE config.config_monitored_servers OWNER TO {OwnerName}; CREATE ROLE {OtherName} NOLOGIN", ct);
            var extensionOwner = await ScalarAsync<string>(superuser,
                "SELECT pg_get_userbyid(extowner)::text FROM pg_extension WHERE extname = 'pg_stat_statements'", ct);

            StoreStatementStats.ResetPublicReadsWarning();
            var log = new CapturingTestLogger();
            await StoreStatementStats.EnsureAsync(owner!, "config", [], log, ct);
            Assert.Contains("Warning: Statement statistics: PUBLIC can still read public.pg_stat_statements", log.Joined, StringComparison.Ordinal);
            Assert.Contains($"Only the owner of the extension, {extensionOwner}, or a superuser can revoke that. Have {extensionOwner} run, in this database: "
                + "REVOKE SELECT ON public.pg_stat_statements FROM PUBLIC;", log.Joined, StringComparison.Ordinal);
            Assert.Contains($"GRANT SELECT ON public.pg_stat_statements TO pg_read_all_stats, {OwnerName};", log.Joined, StringComparison.Ordinal);
            Assert.True(await ScalarAsync<bool>(superuser, $"SELECT has_table_privilege('{OtherName}', 'public.pg_stat_statements', 'SELECT')", ct));

            var notices = new List<string>();
            owner!.Notice += (_, e) => notices.Add(e.Notice.MessageText);
            await ExecAsync(owner, ScriptStatementStatsBlock(), ct);
            var warning = Assert.Single(notices, n => n.StartsWith("Statement statistics: PUBLIC can still read", StringComparison.Ordinal));
            Assert.Contains("Warning: " + warning, log.Joined, StringComparison.Ordinal);

            const string marker = "in this database: ";
            await ExecAsync(superuser, warning[(warning.IndexOf(marker, StringComparison.Ordinal) + marker.Length)..], ct);
            Assert.False(await ScalarAsync<bool>(superuser, $"SELECT has_table_privilege('{OtherName}', 'public.pg_stat_statements', 'SELECT')", ct));

            StoreStatementStats.ResetPublicReadsWarning();
            var after = new CapturingTestLogger();
            await StoreStatementStats.EnsureAsync(owner, "config", [], after, ct);
            Assert.DoesNotContain("PUBLIC can still read", after.Joined, StringComparison.Ordinal);
            Assert.True(await ScalarAsync<long>(owner, "SELECT count(*) FROM config.store_statement_stats()", ct) >= 0);
            notices.Clear();
            await ExecAsync(owner, ScriptStatementStatsBlock(), ct);
            Assert.DoesNotContain(notices, n => n.Contains("PUBLIC can still read", StringComparison.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            await GiveTheServerTableBackAsync(scratch);
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// The script run by a superuser takes pg_stat_statements from PUBLIC and grants it to the store owner that is not a
    /// superuser, so that owner's service setup then runs without an error and its reader reads the view (#5654).
    /// </summary>
    [Fact]
    public async Task TheScriptRunByASuperuser_LeavesANonSuperuserOwnersStatementStatsSetupWorking()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            Assert.SkipWhen(
                await SettingOrNullAsync(superuser, "pg_stat_statements.track", ct) is null,
                "pg_stat_statements must be in shared_preload_libraries on this cluster.");

            await ExecAsync(superuser, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public", ct);
            await ExecAsync(superuser, $"ALTER TABLE config.config_monitored_servers OWNER TO {OwnerName}; CREATE ROLE {OtherName} NOLOGIN", ct);

            var notices = new List<string>();
            superuser.Notice += (_, e) => notices.Add(e.Notice.MessageText);
            await ExecAsync(superuser, ScriptStatementStatsBlock(), ct);
            Assert.DoesNotContain(notices, n => n.Contains("PUBLIC can still read", StringComparison.Ordinal));
            Assert.False(await ScalarAsync<bool>(superuser, $"SELECT has_table_privilege('{OtherName}', 'public.pg_stat_statements', 'SELECT')", ct));
            Assert.True(await ScalarAsync<bool>(superuser, $"SELECT has_table_privilege('{OwnerName}', 'public.pg_stat_statements', 'SELECT')", ct));

            StoreStatementStats.ResetPublicReadsWarning();
            for (var start = 0; start < 2; start++)
            {
                var log = new CapturingTestLogger();
                await StoreStatementStats.EnsureAsync(owner!, "config", [], log, ct);
                Assert.DoesNotContain("PUBLIC can still read", log.Joined, StringComparison.Ordinal);
                Assert.True(await ScalarAsync<long>(owner!, "SELECT count(*) FROM config.store_statement_stats()", ct) >= 0);
            }

            bodySucceeded = true;
        }
        finally
        {
            await GiveTheServerTableBackAsync(scratch);
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// A role named all that is a member of the group through the owner is named by the service and the script, and every
    /// reject line for it quotes the name, so it is that one role and not every role (#5654).
    /// </summary>
    [Fact]
    public async Task ARoleNamedAll_InTheGroup_GetsAQuotedRejectLine_FromTheVerbAndTheScript()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;

            var add = await RunVerbAsync(owner!, ownerConnectionString, "--add-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(add.Exit == 0, add.Error);
            await ExecAsync(superuser, $"CREATE ROLE \"{KeywordRoleName}\" NOLOGIN; GRANT {OwnerName} TO \"{KeywordRoleName}\"", ct);

            var rotate = await RunVerbAsync(owner!, ownerConnectionString, "--rotate-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(rotate.Exit == 0, rotate.Error);
            Assert.Contains("  hostssl <database> \"all\" <range> reject", rotate.Output, StringComparison.Ordinal);
            Assert.DoesNotContain("<database> all <range>", rotate.Output, StringComparison.Ordinal);

            await ExecAsync(superuser, "CREATE ROLE admin NOLOGIN; CREATE ROLE viewer NOLOGIN; CREATE ROLE mcp NOLOGIN", ct);
            var notices = new List<string>();
            owner!.Notice += (_, e) => notices.Add(e.Notice.MessageText);
            await ExecAsync(owner, ScriptAlertSourceBlock(), ct);
            Assert.Contains(notices, n => n.Contains($"hostssl {scratch.DatabaseName} \"all\" <range> reject", StringComparison.Ordinal));
            Assert.DoesNotContain(notices, n => n.Contains($"hostssl {scratch.DatabaseName} all <range>", StringComparison.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// The script, run by a non-superuser owner, meets a source login with a membership it may not revoke: it gives that login
    /// no EXECUTE and no membership in the group, and names it, as the add verb does (#5654).
    /// </summary>
    [Fact]
    public async Task TheScript_DoesNotAdmitASourceLogin_WhoseResetItCouldNotFinish()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;

            var add = await RunVerbAsync(owner!, ownerConnectionString, "--add-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(add.Exit == 0, add.Error);
            Assert.True(await ScalarAsync<bool>(superuser, $"SELECT pg_has_role('{RoleName}', '{GroupName}', 'MEMBER')", ct));

            await ExecAsync(superuser, $"GRANT pg_monitor TO \"{RoleName}\"", ct);
            await ExecAsync(superuser, "CREATE ROLE admin NOLOGIN; CREATE ROLE viewer NOLOGIN; CREATE ROLE mcp NOLOGIN", ct);
            var notices = new List<string>();
            owner!.Notice += (_, e) => notices.Add(e.Notice.MessageText);
            await ExecAsync(owner, ScriptAlertSourceBlock(), ct);

            Assert.Contains(notices, n => n.Contains($"Login \"{RoleName}\" is still a member of \"pg_monitor\"", StringComparison.Ordinal));
            Assert.Contains(notices, n => n.Contains($"External alert source login \"{RoleName}\" is not admitted", StringComparison.Ordinal));
            Assert.False(await ScalarAsync<bool>(superuser, $"SELECT pg_has_role('{RoleName}', '{GroupName}', 'MEMBER')", ct));
            Assert.False(await ScalarAsync<bool>(superuser,
                $"SELECT has_function_privilege('{RoleName}', 'config.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature})', 'EXECUTE')", ct));

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// On the managed store the add and rotate verbs take the alert sources line out of pg_hba.conf at once when the group has
    /// another member: the store owner (a superuser there) rewrites the file and reloads it. An owner that may not reload is
    /// told the line stays live until the service restarts (#5654).
    /// </summary>
    [Fact]
    public async Task OnTheManagedStore_TheVerbTakesTheAlertSourceLineOut_OrSaysWhenItStopsBeingAdmitted()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source group live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var dataDirectory = Directory.CreateTempSubdirectory("darling-5654-hba-");
        var bodySucceeded = false;
        try
        {
            await using var superuser = new NpgsqlConnection(scratch.ConnectionString);
            await superuser.OpenAsync(ct);
            await using var owner = await PrepareAsync(scratch, superuser, ct);
            Assert.SkipWhen(owner is null,
                "This needs PostgreSQL 16 or later (a creator's implicit membership) and no cluster-wide admin/viewer/mcp role.");
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;
            var hbaPath = Path.Combine(dataDirectory.FullName, "pg_hba.conf");
            var written = DarlingManagedPostgres.ReconcilePgHba("local all all trust\n",
                DarlingManagedPostgres.BuildNetworkPgHbaLines(["viewer", DarlingManagedPostgres.AlertSourcesNetworkToken], "192.0.2.0/24"));
            await File.WriteAllTextAsync(hbaPath, written, ct);
            var postgres = new PostgresConfig
            {
                Managed = true,
                DataDirectory = dataDirectory.FullName,
                Network = new PostgresNetworkConfig { Listen = "192.0.2.10", AllowFrom = "192.0.2.0/24", Role = "viewer,sources" },
            };

            /* The owner here is not a superuser (PostgreSQL 16 makes it a member of the group it creates, so the group has an
               extra member) and may not call pg_reload_conf(): the note says the line stays live until the service restarts. */
            var add = await RunVerbAsync(owner!, ownerConnectionString, "--add-alert-source", SourceName, postgres, ct);
            Assert.True(add.Exit == 0, add.Error);
            Assert.Contains("could not be taken out of the live pg_hba rules here", add.Output, StringComparison.Ordinal);
            Assert.Contains("until the service restarts", add.Output, StringComparison.Ordinal);

            /* The managed store's owner is a superuser: the line comes out of the file, the viewer line stays, the server reloads. */
            await File.WriteAllTextAsync(hbaPath, written, ct);
            Assert.Null(await DarlingManagedPostgres.DropAlertSourcePgHbaLineAsync(dataDirectory.FullName, superuser, ct));
            var after = await File.ReadAllTextAsync(hbaPath, ct);
            Assert.DoesNotContain("+" + GroupName, after, StringComparison.Ordinal);
            Assert.Contains("hostssl darling viewer 192.0.2.0/24 scram-sha-256", after, StringComparison.Ordinal);
            Assert.Null(await DarlingManagedPostgres.DropAlertSourcePgHbaLineAsync(dataDirectory.FullName, superuser, ct));
            Assert.Equal(after, await File.ReadAllTextAsync(hbaPath, ct));

            bodySucceeded = true;
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(dataDirectory.FullName);
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }
}
