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
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres and per-run role names; the
       cluster-wide group and admin/viewer/mcp roles are why the class sits in the pg-cluster-roles collection. */
    private const string GroupName = DarlingManagedRoles.AlertSourceGroupRoleName;

    private static readonly string Suffix = Guid.NewGuid().ToString("N")[..8];

    private static readonly string OwnerName = "byo_owner_" + Suffix;

    private static readonly string OtherName = "other_" + Suffix;

    private static readonly string MonitorName = "mon_" + Suffix;

    private const string MonitorPassword = "Mon5654Probe";

    private static readonly string SourceName = "grp_" + Suffix;

    private static readonly string RoleName = ExternalAlertSourceAdmin.RoleNameFor(SourceName);

    private static readonly string[] CleanupRoles = ["admin", "viewer", "mcp", GroupName, RoleName, OtherName, OwnerName, MonitorName];

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
    private static async Task<NpgsqlConnection?> PrepareAsync(ScratchPostgres scratch, NpgsqlConnection superuser, CancellationToken ct)
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

        var owner = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName, Pooling = false }.ConnectionString);
        await owner.OpenAsync(ct);
        return owner;
    }

    private static Task CleanUpAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await new LiveCleanupBatch(cleanup).DropRolesAsync($@"
DO $do$
DECLARE
   r text;
BEGIN
   FOR r IN SELECT rolname FROM pg_roles WHERE rolname IN ({string.Join(", ", CleanupRoles.Select(n => $"'{n}'"))}) LOOP
      EXECUTE format('DROP OWNED BY %I', r);
      EXECUTE format('DROP ROLE %I', r);
   END LOOP;
END $do$", CleanupRoles, cleanupCt));

    private static async Task<(int Exit, string Output, string Error)> RunVerbAsync(
        NpgsqlConnection owner, string connectionString, string verb, string name, PostgresConfig postgres, CancellationToken ct)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await DarlingCliCommands.RunAlertSourceVerbAsync(verb, owner, name, connectionString, output, error, ct, postgres);
        return (exit, output.ToString(), error.ToString());
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
    /// setting needs a superuser or a grant to SET it), so the add verb says the statistics may hold the login's password
    /// verifier and prints the statement that clears them (#5654).
    /// </summary>
    [Fact]
    public async Task ACreateroleOwner_WhoCannotTurnTrackingOff_GetsTheStatisticsWarning_AndTheResetStatement()
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
            var tracking = await SettingOrNullAsync(superuser, "pg_stat_statements.track_utility", ct);
            Assert.SkipWhen(tracking is null || !string.Equals(tracking, "on", StringComparison.OrdinalIgnoreCase),
                "pg_stat_statements must be in shared_preload_libraries with track_utility on (its default) on this cluster.");
            Assert.False(await ScalarAsync<bool>(superuser, $"SELECT rolsuper FROM pg_roles WHERE rolname = '{OwnerName}'", ct));
            var ownerConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = OwnerName }.ConnectionString;

            var add = await RunVerbAsync(owner!, ownerConnectionString, "--add-alert-source", SourceName, new PostgresConfig { Managed = false }, ct);
            Assert.True(add.Exit == 0, add.Error);
            Assert.Contains($"created (login '{RoleName}')", add.Output, StringComparison.Ordinal);
            Assert.Contains("cannot turn utility statement tracking off", add.Output, StringComparison.Ordinal);
            Assert.Contains("statement statistics may hold", add.Output, StringComparison.Ordinal);
            Assert.Contains("the login's password verifier", add.Output, StringComparison.Ordinal);
            Assert.Contains("SELECT pg_stat_statements_reset();", add.Output, StringComparison.Ordinal);
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
}

