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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5654: the grants behind <c>config.submit_external_alert</c>, the function another service calls to raise an alert
/// through Darling. Each test runs the product's whole provisioning batch on a scratch store (the compose target keeps
/// the database-level revoke and the owner's name out of the scratch database), registers per-run source logins, and
/// then connects as them, as <c>admin</c> and as the owner, so the answers come from the privileges the roles really
/// hold (the integration tests run as the owner and prove nothing about a grant).
///
/// <para>The batch runs the fixed-name <c>admin</c>, <c>viewer</c> and <c>mcp</c> roles, which are cluster-wide, so the
/// class is in the <c>pg-cluster-roles</c> collection. The source logins carry per-run names. The cluster uses trust
/// authentication in CI, so these tests prove privilege, not authentication.</para>
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class ExternalAlertGrantLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres and its own login names; the
       fixed admin/viewer/mcp roles are why the class sits in the pg-cluster-roles collection. */
    private readonly string RunSuffix;

    private readonly string RoleA;

    private readonly string RoleB;

    private readonly string RoleUnmarked;

    private readonly string RoleSuper;

    private readonly string RoleTableOwner;

    private readonly string[] CleanupRoles;

    /* The names are instance fields: xUnit builds a new instance per test, so no test sees another's roles. */
    public ExternalAlertGrantLiveTests()
    {
        RunSuffix = Guid.NewGuid().ToString("N")[..8];
        RoleA = "ea_src_a_" + RunSuffix;
        RoleB = "ea_src_b_" + RunSuffix;
        RoleUnmarked = "ea_plain_" + RunSuffix;
        RoleSuper = "ea_su_" + RunSuffix;
        RoleTableOwner = "ea_tblown_" + RunSuffix;
        CleanupRoles = ["admin", "viewer", "web", "mcp", RoleA, RoleB, RoleUnmarked, RoleSuper, RoleTableOwner, DarlingManagedRoles.AlertSourceGroupRoleName];
    }

    private const string SourcePassword = "ExtAlertSrcPw0123456789abcdef01";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the external alert grant live tests.");
        return connectionString!;
    }

    private static string OwnerRoleOf(NpgsqlConnection owner) =>
        new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<List<string>> ColumnAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var values = new List<string>();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    private static async Task<NpgsqlConnection> LoginAsync(ScratchPostgres scratch, string role, string password, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Username = role,
            Password = password,
            Pooling = false,
        }.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    /// <summary>Calls the function as the connected login. Every text argument is a parameter, never concatenated.</summary>
    private static async Task<long> SubmitAsync(
        NpgsqlConnection connection, CancellationToken ct,
        string kind = "fire", string? key = "disk-full", string? severity = "warning",
        string? title = "Disk almost full", string? detail = "93 percent used", string? server = "web01", double value = 93.5)
    {
        await using var command = new NpgsqlCommand("SELECT config.submit_external_alert($1, $2, $3, $4, $5, $6, $7)", connection);
        foreach (var text in new[] { kind, key, severity, title, detail, server })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)text ?? DBNull.Value });
        }

        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = value });
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task AssertDeniedAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, sql, ct));
        Assert.Equal("42501", ex.SqlState);
    }

    private static async Task<int> RegisterAsync(NpgsqlConnection owner, string sourceName, string role, CancellationToken ct) =>
        await ScalarAsync<int>(owner,
            $"INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('{sourceName}', '{role}') RETURNING source_id", ct);

    private static async Task CreateSourceRoleAsync(NpgsqlConnection owner, string role, bool marked, CancellationToken ct)
    {
        await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{SourcePassword}'", ct);
        if (marked)
        {
            await ExecAsync(owner, $"COMMENT ON ROLE {role} IS '{DarlingManagedRoles.AlertSourceRoleMarker}'", ct);
        }
    }

    private static async Task RunBatchAsync(NpgsqlConnection owner, ScratchPostgres scratch, CancellationToken ct) =>
        await ExecAsync(owner,
            DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName)),
            ct);

    /// <summary>Migrates a scratch store, makes the two source logins, registers them, and runs the whole batch. Returns the source ids.</summary>
    private async Task<(int IdA, int IdB)> SetUpAsync(NpgsqlConnection owner, ScratchPostgres scratch, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(owner, ct);
        Assert.SkipWhen(
            await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct),
            "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        await CreateSourceRoleAsync(owner, RoleA, marked: true, ct);
        await CreateSourceRoleAsync(owner, RoleB, marked: true, ct);
        var idA = await RegisterAsync(owner, "src_a", RoleA, ct);
        var idB = await RegisterAsync(owner, "src_b", RoleB, ct);
        await RunBatchAsync(owner, scratch, ct);
        return (idA, idB);
    }

    /// <summary>
    /// Takes every role this test made, and the fixed-name ones, out of the cluster; see <see cref="LiveCleanupBatch.RemoveRolesAsync"/>.
    /// A role that cannot be dropped fails the cleanup naming it, and no role ending in this test's suffix is left.
    /// </summary>
    private async Task CleanUpAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await new LiveCleanupBatch(cleanup).RemoveRolesAsync(CleanupRoles, RunSuffix, cleanupCt));

    /// <summary>The roles <paramref name="role"/> is a member of, by name (#5654: a source login's only one is the alert source group).</summary>
    private static async Task<string> MembershipsOfAsync(NpgsqlConnection owner, string role, CancellationToken ct) =>
        await ScalarAsync<string>(owner,
            $"SELECT COALESCE(string_agg(g.rolname::text, ',' ORDER BY g.rolname::text), '') FROM pg_auth_members AS a JOIN pg_roles AS g ON g.oid = a.roleid WHERE a.member = '{role}'::regrole", ct);

    [Fact]
    public async Task ASourceLogin_CanExecuteTheFunction_AndHoldsNoOtherPrivilege()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await SetUpAsync(owner, scratch, ct);

            /* What the login is, read as the owner. */
            Assert.True(await ScalarAsync<bool>(owner,
                $"SELECT rolcanlogin AND NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls AND NOT rolinherit FROM pg_roles WHERE rolname = '{RoleA}'", ct));
            Assert.Equal(DarlingManagedRoles.AlertSourceGroupRoleName, await MembershipsOfAsync(owner, RoleA, ct));

            /* No privilege of any kind on a table, view or sequence in the two Darling schemas, and none on a column. */
            Assert.Empty(await ColumnAsync(owner, $@"
SELECT n.nspname || '.' || c.relname
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname IN ('config', 'collect')
  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND (has_table_privilege('{RoleA}', c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
       OR has_any_column_privilege('{RoleA}', c.oid, 'SELECT, INSERT, UPDATE, REFERENCES'))", ct));
            Assert.Empty(await ColumnAsync(owner, $@"
SELECT n.nspname || '.' || c.relname
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname IN ('config', 'collect') AND c.relkind = 'S'
  AND CASE WHEN c.relkind = 'S' THEN has_sequence_privilege('{RoleA}', c.oid, 'USAGE, SELECT, UPDATE') END", ct));

            /* Exactly one SECURITY DEFINER function it can execute, and it is the submit function. */
            var definers = await ColumnAsync(owner, $@"
SELECT n.nspname || '.' || p.proname
FROM pg_proc AS p
JOIN pg_namespace AS n ON n.oid = p.pronamespace
WHERE p.prosecdef
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND NOT EXISTS (SELECT 1 FROM pg_depend AS d WHERE d.objid = p.oid AND d.deptype = 'e')
  AND has_function_privilege('{RoleA}', p.oid, 'EXECUTE')", ct);
            Assert.Equal("config.submit_external_alert", Assert.Single(definers));

            /* The only other function in the config schema it may run is a trigger function, which cannot be called. */
            Assert.Empty(await ColumnAsync(owner, $@"
SELECT p.proname
FROM pg_proc AS p
JOIN pg_namespace AS n ON n.oid = p.pronamespace
WHERE n.nspname IN ('config', 'collect')
  AND p.proname <> 'submit_external_alert'
  AND p.prorettype <> 'trigger'::regtype
  AND has_function_privilege('{RoleA}', p.oid, 'EXECUTE')", ct));

            /* USAGE on config (for the function), nothing on collect, no CREATE anywhere in the two. */
            Assert.True(await ScalarAsync<bool>(owner, $"SELECT has_schema_privilege('{RoleA}', 'config', 'USAGE') AND NOT has_schema_privilege('{RoleA}', 'config', 'CREATE')", ct));
            Assert.False(await ScalarAsync<bool>(owner, $"SELECT has_schema_privilege('{RoleA}', 'collect', 'USAGE') OR has_schema_privilege('{RoleA}', 'collect', 'CREATE')", ct));

            /* What the login can do, connected as it. */
            await using var source = await LoginAsync(scratch, RoleA, SourcePassword, ct);
            Assert.True(await SubmitAsync(source, ct) > 0);

            await AssertDeniedAsync(source, "SELECT * FROM config.config_notification", ct);
            await AssertDeniedAsync(source, "SELECT * FROM config.config_monitored_servers", ct);
            await AssertDeniedAsync(source, "SELECT * FROM config.password_key", ct);
            await AssertDeniedAsync(source, "SELECT * FROM config.config_alert_log", ct);
            await AssertDeniedAsync(source, "SELECT * FROM config.external_alert_pending", ct);
            await AssertDeniedAsync(source, "SELECT * FROM config.external_alert_source", ct);
            await AssertDeniedAsync(source, "SELECT * FROM config.external_alert_state", ct);
            await AssertDeniedAsync(source,
                "INSERT INTO config.config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value) VALUES (now(), 0, 'x', 'External:src_a:x', 0, 0)", ct);
            await AssertDeniedAsync(source,
                "INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, expires_at) VALUES (1, 'fire', 'k', 'warning', now())", ct);
            await AssertDeniedAsync(source, "UPDATE config.external_alert_source SET max_per_minute = 100000", ct);
            await AssertDeniedAsync(source, "DELETE FROM config.external_alert_pending", ct);
            await AssertDeniedAsync(source, "SELECT config.record_custom_alert_resolution(1, 'x', 'External:src_a:x', 'x')", ct);

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// A temporary table or function named like the function's tables or functions must not stand in for them. The
    /// caller puts <c>pg_temp</c> first on its own search_path, makes a temporary registry that names the caller as the
    /// OTHER source, a temporary pending table and a temporary <c>now()</c>, and calls the function. The row must land
    /// in the real pending table, as the caller's own source, with a real expiry. Without the function's pinned
    /// <c>search_path</c> and the schema on each table name, the temporary registry would hand the caller the other
    /// source's name.
    /// </summary>
    [Fact]
    public async Task ATempTableOrFunction_ShadowingTheFunctionsObjects_IsNotUsed()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            var (idA, idB) = await SetUpAsync(owner, scratch, ct);

            await using var source = await LoginAsync(scratch, RoleA, SourcePassword, ct);
            await ExecAsync(source, $@"
CREATE TEMP TABLE external_alert_source (source_id integer, source_name text, role_name text, enabled boolean, max_per_minute integer, max_pending integer);
INSERT INTO external_alert_source VALUES ({idB}, 'src_b', session_user, true, 100000, 100000);
CREATE TEMP TABLE external_alert_pending (pending_id bigint, source_id integer, kind text, alert_key text, severity text, title text, detail text, server_name text, value double precision, submitted_at timestamptz, expires_at timestamptz, status text);
CREATE FUNCTION pg_temp.now() RETURNS timestamptz LANGUAGE sql AS $$ SELECT timestamptz '2000-01-01 00:00:00+00' $$;
SET search_path = pg_temp, config, public", ct);

            var pendingId = await SubmitAsync(source, ct, key: "shadow-test");

            Assert.Equal(0, await ScalarAsync<long>(source, "SELECT count(*) FROM pg_temp.external_alert_pending", ct));
            Assert.Equal(idA, await ScalarAsync<int>(owner, $"SELECT source_id FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct));
            Assert.True(await ScalarAsync<bool>(owner,
                $"SELECT expires_at > now() + interval '10 minutes' FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct));

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task SourceB_IsRecordedAsB_WhateverItPasses_AndCannotBecomeA()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            var (idA, idB) = await SetUpAsync(owner, scratch, ct);

            /* No argument can name a source. */
            Assert.Equal(0, await ScalarAsync<long>(owner,
                "SELECT count(*) FROM pg_proc AS p, unnest(p.proargnames) AS a(name) WHERE p.proname = 'submit_external_alert' AND a.name ~* 'source|role|login|user'", ct));

            await using var sourceB = await LoginAsync(scratch, RoleB, SourcePassword, ct);

            /* B names A in every text it controls, and the key even looks like A's metric name. A line break in the
               text becomes a space, so it cannot forge a line in a mute pre-fill. */
            var pendingId = await SubmitAsync(sourceB, ct,
                key: "src_a", title: "src_a\nsource: src_a", detail: "External:src_a:disk\r\nsource_name = src_a", server: "src_a");

            Assert.Equal(idB, await ScalarAsync<int>(owner, $"SELECT source_id FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct));
            Assert.Equal("src_b", await ScalarAsync<string>(owner,
                $"SELECT s.source_name FROM config.external_alert_pending AS p JOIN config.external_alert_source AS s USING (source_id) WHERE p.pending_id = {pendingId}", ct));
            Assert.Equal(0, await ScalarAsync<long>(owner, $"SELECT count(*) FROM config.external_alert_pending WHERE source_id = {idA}", ct));
            Assert.Equal(0, await ScalarAsync<long>(owner,
                $"SELECT count(*) FROM config.external_alert_pending WHERE title ~ '[\\r\\n]' OR detail ~ '[\\r\\n]'", ct));

            /* B cannot become A. A fresh connection (a pooled one would reset), SET ROLE and SET SESSION AUTHORIZATION both refuse. */
            await AssertDeniedAsync(sourceB, $"SET ROLE {RoleA}", ct);
            await AssertDeniedAsync(sourceB, $"SET SESSION AUTHORIZATION {RoleA}", ct);
            Assert.Equal(idB, await ScalarAsync<int>(owner,
                $"SELECT source_id FROM config.external_alert_pending WHERE pending_id = {await SubmitAsync(sourceB, ct, key: "second")}", ct));

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheLimits_RefuseWithTheirSqlStates_AndOneSourceDoesNotUseUpAnothers()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            var (idA, idB) = await SetUpAsync(owner, scratch, ct);
            await using var sourceA = await LoginAsync(scratch, RoleA, SourcePassword, ct);
            await using var sourceB = await LoginAsync(scratch, RoleB, SourcePassword, ct);

            /* Rate: 3 a minute, the 4th is refused with DL429 and a hint; the refused call writes nothing; B is unaffected. */
            await ExecAsync(owner, $"UPDATE config.external_alert_source SET max_per_minute = 3 WHERE source_id = {idA}", ct);
            for (var i = 0; i < 3; i++)
            {
                await SubmitAsync(sourceA, ct, key: "rate-" + i);
            }

            var rate = await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(sourceA, ct, key: "rate-3"));
            Assert.Equal("DL429", rate.SqlState);
            Assert.False(string.IsNullOrEmpty(rate.Hint));
            Assert.Equal(3, await ScalarAsync<long>(owner, $"SELECT count(*) FROM config.external_alert_pending WHERE source_id = {idA}", ct));
            Assert.True(await SubmitAsync(sourceB, ct) > 0);

            /* Waiting rows: 2 waiting is the cap; a delivered row no longer counts. */
            await ExecAsync(owner, $"UPDATE config.external_alert_source SET max_per_minute = 1000, max_pending = 2 WHERE source_id = {idB}", ct);
            await SubmitAsync(sourceB, ct, key: "wait-1");
            var pendingLimit = await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(sourceB, ct, key: "wait-2"));
            Assert.Equal("DL429", pendingLimit.SqlState);
            await ExecAsync(owner, $"UPDATE config.external_alert_pending SET status = 'delivered' WHERE source_id = {idB}", ct);
            Assert.True(await SubmitAsync(sourceB, ct, key: "wait-3") > 0);

            /* Shape: a bad kind, severity, key or an over-long text is DL400 and writes nothing. */
            await ExecAsync(owner, $"UPDATE config.external_alert_source SET max_per_minute = 1000, max_pending = 1000 WHERE source_id = {idA}", ct);
            var before = await ScalarAsync<long>(owner, "SELECT count(*) FROM config.external_alert_pending", ct);
            foreach (var call in new Func<Task<long>>[]
            {
                () => SubmitAsync(sourceA, ct, kind: "explode"),
                () => SubmitAsync(sourceA, ct, severity: "info"),
                () => SubmitAsync(sourceA, ct, severity: null),
                () => SubmitAsync(sourceA, ct, key: null),
                () => SubmitAsync(sourceA, ct, key: "has space"),
                () => SubmitAsync(sourceA, ct, key: new string('k', 129)),
                () => SubmitAsync(sourceA, ct, title: new string('t', 201)),
                () => SubmitAsync(sourceA, ct, detail: new string('d', 2001)),
                () => SubmitAsync(sourceA, ct, server: new string('s', 129)),
            })
            {
                var bad = await Assert.ThrowsAsync<PostgresException>(call);
                Assert.Equal("DL400", bad.SqlState);
            }

            Assert.Equal(before, await ScalarAsync<long>(owner, "SELECT count(*) FROM config.external_alert_pending", ct));

            /* The caps themselves pass, and a clear needs no severity. */
            Assert.True(await SubmitAsync(sourceA, ct, key: new string('k', 128), title: new string('t', 200), detail: new string('d', 2000), server: new string('s', 128)) > 0);
            var clearId = await SubmitAsync(sourceA, ct, kind: "clear", severity: null);
            Assert.Equal("warning", await ScalarAsync<string>(owner, $"SELECT severity FROM config.external_alert_pending WHERE pending_id = {clearId}", ct));

            /* A disabled source, and a login that is no source at all, are refused with DL403. */
            await ExecAsync(owner, $"UPDATE config.external_alert_source SET enabled = false WHERE source_id = {idA}", ct);
            Assert.Equal("DL403", (await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(sourceA, ct))).SqlState);
            await ExecAsync(owner, $"DELETE FROM config.external_alert_source WHERE source_id = {idB}", ct);
            Assert.Equal("DL403", (await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(sourceB, ct))).SqlState);

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheAdminRole_CannotWriteTheNewTables_OrCallTheFunction()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            var (idA, _) = await SetUpAsync(owner, scratch, ct);
            long pendingId;
            await using (var sourceA = await LoginAsync(scratch, RoleA, SourcePassword, ct))
            {
                pendingId = await SubmitAsync(sourceA, ct);
            }

            /* The control: admin still writes an ordinary config table, so the refusal below is the revoke and not a
               missing grant on the whole schema. */
            await using var admin = await LoginAsync(scratch, "admin", ProvisioningTestSecrets.AdminPassword, ct);
            await ExecAsync(admin,
                "INSERT INTO config.config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value) VALUES (now(), -5654, 'srv', 'control', 0, 0)", ct);

            await AssertDeniedAsync(admin,
                $"INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, expires_at) VALUES ({idA}, 'fire', 'forged', 'warning', now() + interval '1 hour')", ct);
            await AssertDeniedAsync(admin, $"UPDATE config.external_alert_pending SET source_id = {idA} + 1 WHERE pending_id = {pendingId}", ct);
            await AssertDeniedAsync(admin, "DELETE FROM config.external_alert_pending", ct);
            await AssertDeniedAsync(admin, "INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('forged', 'admin')", ct);
            await AssertDeniedAsync(admin, "UPDATE config.external_alert_source SET role_name = 'admin'", ct);
            await AssertDeniedAsync(admin, "DELETE FROM config.external_alert_source", ct);
            await AssertDeniedAsync(admin, $"INSERT INTO config.external_alert_state (source_id, alert_key) VALUES ({idA}, 'forged')", ct);
            await AssertDeniedAsync(admin, "TRUNCATE config.external_alert_pending", ct);
            await AssertDeniedAsync(admin, "SELECT config.submit_external_alert('fire', 'k', 'warning', 't', 'd', 's', 1)", ct);

            /* It can still read all three, as before. */
            Assert.Equal(1, await ScalarAsync<long>(admin, "SELECT count(*) FROM config.external_alert_pending", ct));
            await ExecAsync(admin, "SELECT 1 FROM config.external_alert_source; SELECT 1 FROM config.external_alert_state", ct);

            /* viewer and mcp: read, no write, no call. */
            foreach (var (role, password) in new[] { ("viewer", ProvisioningTestSecrets.ViewerPassword), ("mcp", ProvisioningTestSecrets.McpPassword) })
            {
                await using var login = await LoginAsync(scratch, role, password, ct);
                Assert.Equal(1, await ScalarAsync<long>(login, "SELECT count(*) FROM config.external_alert_pending", ct));
                await AssertDeniedAsync(login,
                    $"INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, expires_at) VALUES ({idA}, 'fire', 'forged', 'warning', now() + interval '1 hour')", ct);
                await AssertDeniedAsync(login, "SELECT config.submit_external_alert('fire', 'k', 'warning', 't', 'd', 's', 1)", ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task EveryStart_PutsBackWhatASourceLoginLost_AndLeavesAnUnmarkedRoleAlone()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await SetUpAsync(owner, scratch, ct);

            /* A registry row that names a role without the marker, and one that names no role: the batch skips both. */
            await CreateSourceRoleAsync(owner, RoleUnmarked, marked: false, ct);
            await ExecAsync(owner, $"ALTER ROLE {RoleUnmarked} CREATEDB", ct);
            await RegisterAsync(owner, "src_plain", RoleUnmarked, ct);
            await RegisterAsync(owner, "src_gone", "ea_no_such_role_" + RunSuffix, ct);

            /* Someone widens source A since the last start. */
            await ExecAsync(owner, $"ALTER ROLE {RoleA} CREATEDB INHERIT; GRANT pg_read_all_data TO {RoleA}; GRANT SELECT ON config.config_notification TO {RoleA}", ct);

            await RunBatchAsync(owner, scratch, ct);

            Assert.True(await ScalarAsync<bool>(owner,
                $"SELECT NOT rolcreatedb AND NOT rolinherit AND rolcanlogin FROM pg_roles WHERE rolname = '{RoleA}'", ct));
            Assert.Equal(DarlingManagedRoles.AlertSourceGroupRoleName, await MembershipsOfAsync(owner, RoleA, ct));
            Assert.True(await ScalarAsync<bool>(owner,
                $"SELECT has_function_privilege('{RoleA}', 'config.submit_external_alert(text, text, text, text, text, text, double precision)', 'EXECUTE')", ct));

            Assert.False(await ScalarAsync<bool>(owner, $"SELECT pg_has_role('{RoleA}', 'pg_read_all_data', 'USAGE')", ct));
            Assert.False(await ScalarAsync<bool>(owner, $"SELECT has_table_privilege('{RoleA}', 'config.config_notification', 'SELECT')", ct));

            Assert.True(await ScalarAsync<bool>(owner, $"SELECT rolcreatedb FROM pg_roles WHERE rolname = '{RoleUnmarked}'", ct));
            Assert.False(await ScalarAsync<bool>(owner,
                $"SELECT has_function_privilege('{RoleUnmarked}', 'config.submit_external_alert(text, text, text, text, text, text, double precision)', 'EXECUTE')", ct));

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>NaN, Infinity and -Infinity are refused with DL400 (they cannot be written as JSON), and a finite value passes. Run against the managed copy.</summary>
    [Fact]
    public async Task ANonFiniteValue_IsRefusedWithDl400_AndWritesNothing()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await SetUpAsync(owner, scratch, ct);
            await using var sourceA = await LoginAsync(scratch, RoleA, SourcePassword, ct);

            foreach (var value in new[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity })
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(sourceA, ct, value: value));
                Assert.Equal("DL400", refused.SqlState);
            }

            Assert.Equal(0, await ScalarAsync<long>(owner, "SELECT count(*) FROM config.external_alert_pending", ct));
            Assert.True(await SubmitAsync(sourceA, ct, value: -1.5e300) > 0);

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// A source that leaves a transaction open after a submit still serializes its own next submit (the registry row is
    /// locked), but the service's write to the state table, whose foreign key takes a key share on that row, does not wait.
    /// </summary>
    [Fact]
    public async Task ASourceHoldingAnOpenTransaction_DoesNotHoldUpAStateInsert_ButStillSerializesItsOwnSubmits()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            var (idA, _) = await SetUpAsync(owner, scratch, ct);
            await using var holder = await LoginAsync(scratch, RoleA, SourcePassword, ct);
            await using var second = await LoginAsync(scratch, RoleA, SourcePassword, ct);

            await using (var open = await holder.BeginTransactionAsync(ct))
            {
                await SubmitAsync(holder, ct, key: "held");

                /* The state insert (what the drain's first fire for a key does) returns inside the bound. */
                await using var stateWriter = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Pooling = false }.ConnectionString);
                await stateWriter.OpenAsync(ct);
                await ExecAsync(stateWriter, "SET lock_timeout = '3s'", ct);
                await ExecAsync(stateWriter,
                    $"INSERT INTO config.external_alert_state (source_id, alert_key, server_id, firing) VALUES ({idA}, 'held', 0, true)", ct);

                /* The same source's next submit waits on the open one. */
                await ExecAsync(second, "SET lock_timeout = '500ms'", ct);
                var waited = await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(second, ct, key: "next"));
                Assert.Equal("55P03", waited.SqlState);

                await open.RollbackAsync(ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>The rate window, <c>submitted_at</c> and <c>expires_at</c> are read from the clock at the call, not from the start of the caller's transaction.</summary>
    [Fact]
    public async Task ASubmitFromAnEarlierTransaction_IsStampedAtSubmitTime()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await SetUpAsync(owner, scratch, ct);
            await using var sourceA = await LoginAsync(scratch, RoleA, SourcePassword, ct);

            long pendingId;
            double submittedAfterBegin;
            await using (var open = await sourceA.BeginTransactionAsync(ct))
            {
                await ExecAsync(sourceA, "SELECT 1", ct);
                var beganAt = DateTime.UtcNow;
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
                pendingId = await SubmitAsync(sourceA, ct);
                await open.CommitAsync(ct);
                submittedAfterBegin = (await ScalarAsync<DateTime>(owner,
                    $"SELECT submitted_at AT TIME ZONE 'UTC' FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct) - beganAt).TotalSeconds;
            }

            Assert.True(submittedAfterBegin >= 1.5, $"submitted_at was {submittedAfterBegin:F2} s after the transaction began");
            Assert.Equal(900.0, await ScalarAsync<double>(owner,
                $"SELECT extract(epoch FROM expires_at - submitted_at)::float8 FROM config.external_alert_pending WHERE pending_id = {pendingId}", ct), 3);

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    private static async Task<string> OwnerCommentAsync(NpgsqlConnection owner, CancellationToken ct) =>
        await ScalarAsync<string>(owner, $"SELECT coalesce(shobj_description('{OwnerRoleOf(owner)}'::regrole, 'pg_authid'), '')", ct);

    private static async Task RestoreOwnerCommentAsync(NpgsqlConnection owner, string comment, CancellationToken ct) =>
        await ExecAsync(owner, comment.Length == 0
            ? $"COMMENT ON ROLE {OwnerRoleOf(owner)} IS NULL"
            : $"COMMENT ON ROLE {OwnerRoleOf(owner)} IS '{comment.Replace("'", "''", StringComparison.Ordinal)}'", ct);

    /// <summary>The login attributes the source-login step can change, in one string: connection limit, inherit, login, and the sorted memberships.</summary>
    private static Task<string> RoleStateAsync(NpgsqlConnection connection, string role, CancellationToken ct) =>
        ScalarAsync<string>(connection,
            $@"SELECT r.rolconnlimit || '|' || r.rolinherit || '|' || r.rolcanlogin || '|' || r.rolsuper || '|'
                      || coalesce((SELECT string_agg(m.roleid::regrole::text, ',' ORDER BY m.roleid::regrole::text) FROM pg_auth_members AS m WHERE m.member = r.oid), '')
               FROM pg_roles AS r WHERE r.rolname = '{role}'", ct);

    /// <summary>
    /// A submit runs only at READ COMMITTED: inside a REPEATABLE READ or SERIALIZABLE transaction it is refused with DL400 and
    /// writes nothing, and the same call at READ COMMITTED (the default, or an explicit transaction) is accepted.
    /// </summary>
    [Fact]
    public async Task ASubmit_InARepeatableReadOrSerializableTransaction_IsRefusedWithDl400_AndReadCommittedStillWorks()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await SetUpAsync(owner, scratch, ct);
            await using var sourceA = await LoginAsync(scratch, RoleA, SourcePassword, ct);

            foreach (var level in new[] { "REPEATABLE READ", "SERIALIZABLE" })
            {
                await ExecAsync(sourceA, $"BEGIN ISOLATION LEVEL {level}", ct);
                await ExecAsync(sourceA, "SELECT 1", ct);
                var refused = await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(sourceA, ct));
                Assert.Equal("DL400", refused.SqlState);
                Assert.Contains("READ COMMITTED", refused.MessageText, StringComparison.Ordinal);
                await ExecAsync(sourceA, "ROLLBACK", ct);
            }

            Assert.Equal(0, await ScalarAsync<long>(owner, "SELECT count(*) FROM config.external_alert_pending", ct));

            await ExecAsync(sourceA, "BEGIN ISOLATION LEVEL READ COMMITTED", ct);
            Assert.True(await SubmitAsync(sourceA, ct, key: "in-transaction") > 0);
            await ExecAsync(sourceA, "COMMIT", ct);
            Assert.True(await SubmitAsync(sourceA, ct, key: "autocommit") > 0);
            Assert.Equal(2, await ScalarAsync<long>(owner, "SELECT count(*) FROM config.external_alert_pending", ct));

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// A registry row that names the role running the batch, or a superuser, with the marker comment on that role, is skipped
    /// with a WARNING naming the role: its connection limit, inheritance, login flag, superuser flag and memberships stay as
    /// they were. (A marker on admin, viewer or mcp replaces the managed marker those roles carry, and the batch refuses such a
    /// role before this step.)
    /// </summary>
    [Fact]
    public async Task ARegisteredStartRoleOrSuperuser_WithTheMarker_IsSkippedWithAWarning_AndKeepsItsState()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await SetUpAsync(owner, scratch, ct);

            await ExecAsync(owner, $"CREATE ROLE {RoleSuper} LOGIN SUPERUSER INHERIT CONNECTION LIMIT 7; GRANT pg_read_all_data TO {RoleSuper}", ct);
            var protectedRoles = new[] { OwnerRoleOf(owner), RoleSuper };
            var ownerComment = await OwnerCommentAsync(owner, ct);
            try
            {
                var before = new Dictionary<string, string>();
                foreach (var role in protectedRoles)
                {
                    await ExecAsync(owner, $"COMMENT ON ROLE {role} IS '{DarlingManagedRoles.AlertSourceRoleMarker}'", ct);
                    await RegisterAsync(owner, "src_" + role.ToLowerInvariant(), role, ct);
                    before[role] = await RoleStateAsync(owner, role, ct);
                }

                var notices = new List<string>();
                owner.Notice += (_, e) => notices.Add(e.Notice.MessageText);
                await RunBatchAsync(owner, scratch, ct);

                foreach (var role in protectedRoles)
                {
                    Assert.Equal(before[role], await RoleStateAsync(owner, role, ct));
                    Assert.Contains(notices, n => n.Contains($"\"{role}\"", StringComparison.Ordinal) && n.Contains("was skipped", StringComparison.Ordinal));
                }

                /* The ordinary marked logins are still limited as before. */
                Assert.Equal(5, await ScalarAsync<int>(owner, $"SELECT rolconnlimit FROM pg_roles WHERE rolname = '{RoleA}'", ct));
            }
            finally
            {
                await RestoreOwnerCommentAsync(owner, ownerComment, ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// Every start sets the source login's connection limit and takes EXECUTE from a registered role that has lost its
    /// marker; the login connects on a database whose PUBLIC has no CONNECT, so the batch's own CONNECT grant is what lets it in.
    /// </summary>
    [Fact]
    public async Task EveryStart_SetsTheConnectionLimit_TakesExecuteFromALostMarker_AndGrantsConnectWherePublicHasNone()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await SetUpAsync(owner, scratch, ct);

            /* The limit comes back after someone lifts it. */
            Assert.Equal(DarlingManagedRoles.AlertSourceConnectionLimit, await ScalarAsync<int>(owner, $"SELECT rolconnlimit FROM pg_roles WHERE rolname = '{RoleA}'", ct));
            await ExecAsync(owner, $"ALTER ROLE {RoleA} CONNECTION LIMIT -1", ct);
            await RunBatchAsync(owner, scratch, ct);
            Assert.Equal(5, await ScalarAsync<int>(owner, $"SELECT rolconnlimit FROM pg_roles WHERE rolname = '{RoleA}'", ct));

            /* A database whose PUBLIC has no CONNECT: the login is refused until the batch grants it. */
            await ExecAsync(owner, $"REVOKE CONNECT ON DATABASE \"{scratch.DatabaseName}\" FROM PUBLIC; REVOKE CONNECT ON DATABASE \"{scratch.DatabaseName}\" FROM {RoleA}", ct);
            Assert.False(await ScalarAsync<bool>(owner, $"SELECT has_database_privilege('public', '{scratch.DatabaseName}', 'CONNECT')", ct));
            Assert.False(await ScalarAsync<bool>(owner, $"SELECT has_database_privilege('{RoleA}', '{scratch.DatabaseName}', 'CONNECT')", ct));
            var refused = await Assert.ThrowsAsync<PostgresException>(() => LoginAsync(scratch, RoleA, SourcePassword, ct));
            Assert.Equal("42501", refused.SqlState);

            await RunBatchAsync(owner, scratch, ct);
            await using (var sourceA = await LoginAsync(scratch, RoleA, SourcePassword, ct))
            {
                Assert.True(await SubmitAsync(sourceA, ct) > 0);
            }

            /* A role that loses its marker keeps its registry row and loses EXECUTE on the next start. */
            await ExecAsync(owner, $"COMMENT ON ROLE {RoleA} IS NULL", ct);
            await RunBatchAsync(owner, scratch, ct);
            Assert.False(await ScalarAsync<bool>(owner,
                $"SELECT has_function_privilege('{RoleA}', 'config.submit_external_alert(text, text, text, text, text, text, double precision)', 'EXECUTE')", ct));
            await using (var sourceA = await LoginAsync(scratch, RoleA, SourcePassword, ct))
            {
                var denied = await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(sourceA, ct));
                Assert.Equal("42501", denied.SqlState);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// The shipped <c>tools/provision-roles.sql</c>, run on a scratch store (its database-level statements retargeted, which
    /// also leaves PUBLIC without CONNECT there): the same function behavior, the connection limit, and EXECUTE taken from a
    /// role that lost its marker.
    /// </summary>
    [Fact]
    public async Task TheByoScript_SetsTheConnectionLimit_RefusesNonFiniteValues_AndTakesExecuteFromALostMarker()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct),
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the script would refuse it.");
            Assert.SkipUnless(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct),
                "The script names the store owner 'darling', and this rig has no such role.");
            await CreateSourceRoleAsync(owner, RoleA, marked: true, ct);
            await RegisterAsync(owner, "src_a", RoleA, ct);

            var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                    System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                .Replace("CHANGE_ME_ADMIN_PASSWORD", "ScriptAdmin5654", StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", "ScriptViewer5654", StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", "ScriptMcp5654", StringComparison.Ordinal)
                .Replace("CHANGE_ME_WEB_PASSWORD", "ScriptWeb6991", StringComparison.Ordinal);
            await ExecAsync(owner, script, ct);

            Assert.Equal(5, await ScalarAsync<int>(owner, $"SELECT rolconnlimit FROM pg_roles WHERE rolname = '{RoleA}'", ct));
            Assert.Equal(DarlingManagedRoles.AlertSourceGroupRoleName, await MembershipsOfAsync(owner, RoleA, ct));
            Assert.Equal(DarlingManagedRoles.AlertSourceGroupRoleMarker, await ScalarAsync<string>(owner,
                $"SELECT shobj_description(oid, 'pg_authid') FROM pg_roles WHERE rolname = '{DarlingManagedRoles.AlertSourceGroupRoleName}'", ct));
            Assert.False(await ScalarAsync<bool>(owner, $"SELECT has_database_privilege('public', '{scratch.DatabaseName}', 'CONNECT')", ct));

            await using (var sourceA = await LoginAsync(scratch, RoleA, SourcePassword, ct))
            {
                foreach (var value in new[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity })
                {
                    var refused = await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(sourceA, ct, value: value));
                    Assert.Equal("DL400", refused.SqlState);
                }

                Assert.True(await SubmitAsync(sourceA, ct) > 0);
            }

            await ExecAsync(owner, $"ALTER ROLE {RoleA} CONNECTION LIMIT -1; COMMENT ON ROLE {RoleA} IS NULL", ct);
            await ExecAsync(owner, script, ct);
            Assert.Equal(-1, await ScalarAsync<int>(owner, $"SELECT rolconnlimit FROM pg_roles WHERE rolname = '{RoleA}'", ct));
            Assert.False(await ScalarAsync<bool>(owner,
                $"SELECT has_function_privilege('{RoleA}', 'config.submit_external_alert(text, text, text, text, text, text, double precision)', 'EXECUTE')", ct));

            /* The script's copy of the function refuses a REPEATABLE READ transaction with DL400. */
            await ExecAsync(owner, $"COMMENT ON ROLE {RoleA} IS '{DarlingManagedRoles.AlertSourceRoleMarker}'", ct);
            await ExecAsync(owner, script, ct);
            await using (var sourceA = await LoginAsync(scratch, RoleA, SourcePassword, ct))
            {
                await ExecAsync(sourceA, "BEGIN ISOLATION LEVEL REPEATABLE READ", ct);
                await ExecAsync(sourceA, "SELECT 1", ct);
                var refused = await Assert.ThrowsAsync<PostgresException>(() => SubmitAsync(sourceA, ct));
                Assert.Equal("DL400", refused.SqlState);
                await ExecAsync(sourceA, "ROLLBACK", ct);
                Assert.True(await SubmitAsync(sourceA, ct, key: "read-committed") > 0);
            }

            /* And its source-login step skips the role running it and a superuser that carry the marker, with a warning. */
            await ExecAsync(owner, $"CREATE ROLE {RoleSuper} LOGIN SUPERUSER INHERIT CONNECTION LIMIT 7; GRANT pg_read_all_data TO {RoleSuper}", ct);
            var protectedRoles = new[] { OwnerRoleOf(owner), RoleSuper };
            var ownerComment = await OwnerCommentAsync(owner, ct);
            try
            {
                var before = new Dictionary<string, string>();
                foreach (var role in protectedRoles)
                {
                    await ExecAsync(owner, $"COMMENT ON ROLE {role} IS '{DarlingManagedRoles.AlertSourceRoleMarker}'", ct);
                    await RegisterAsync(owner, "src_" + role.ToLowerInvariant(), role, ct);
                    before[role] = await RoleStateAsync(owner, role, ct);
                }

                var notices = new List<string>();
                owner.Notice += (_, e) => notices.Add(e.Notice.MessageText);
                await ExecAsync(owner, script, ct);
                foreach (var role in protectedRoles)
                {
                    Assert.Equal(before[role], await RoleStateAsync(owner, role, ct));
                    Assert.Contains(notices, n => n.Contains($"\"{role}\"", StringComparison.Ordinal) && n.Contains("was skipped", StringComparison.Ordinal));
                }
            }
            finally
            {
                await RestoreOwnerCommentAsync(owner, ownerComment, ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// The queue functions run as the owner of <c>config.config_command</c>, whoever runs the script: here a role that is not the
    /// owner (a superuser on some stores) runs it on a store whose command table belongs to another role, and every definer-rights
    /// function the script creates in <c>config</c> ends up owned by the table owner.
    /// </summary>
    [Fact]
    public async Task TheByoScript_HandsTheQueueFunctionsToTheOwnerOfTheCommandTable()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct),
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the script would refuse it.");
            Assert.SkipUnless(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct),
                "The script names the store owner 'darling', and this rig has no such role.");
            Assert.True(await ScalarAsync<bool>(owner, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user", ct),
                "The script is run by a superuser here, the case the owner step is for.");
            await ExecAsync(owner, $"CREATE ROLE {RoleTableOwner} NOLOGIN; ALTER TABLE config.config_command OWNER TO {RoleTableOwner}", ct);

            var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                    System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                .Replace("CHANGE_ME_ADMIN_PASSWORD", "ScriptAdmin5654", StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", "ScriptViewer5654", StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", "ScriptMcp5654", StringComparison.Ordinal)
                .Replace("CHANGE_ME_WEB_PASSWORD", "ScriptWeb6991", StringComparison.Ordinal);
            await ExecAsync(owner, script, ct);

            foreach (var function in new[]
            {
                "config.request_snapshot_now(integer, text, text)",
                "config.request_active_queries_live(integer, text)",
                "config.request_mcp_command(text, integer, integer, text)",
                "config.read_mcp_command_result(bigint)",
                "config.submit_external_alert(text, text, text, text, text, text, double precision)",
                "config.edit_monitored_server(integer, timestamp, text[], text, text, integer, text, boolean, text, text, text, text, boolean, boolean, numeric)",
            })
            {
                Assert.Equal(RoleTableOwner, await ScalarAsync<string>(owner,
                    $"SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE oid = '{function}'::regprocedure", ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// #6386: the script's own copy of <c>config.request_active_queries_live</c> (the one a store brought along runs; managed
    /// provisioning rebuilds its own) refuses a REPEATABLE READ transaction with DL400 and queues nothing, and a read committed call
    /// from the same login still queues the read. The other behavior facts run against the managed copy
    /// (<c>ActiveQueriesLiveWebTests</c>); no test compares the two texts, so this one runs the script's.
    /// </summary>
    [Fact]
    public async Task TheByoScript_ActiveQueriesLiveFunction_RefusesARepeatableReadTransaction_WithDl400_AndQueuesNothing()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct),
                "A cluster-wide admin/viewer/web/mcp role already exists on this rig; the script would refuse it.");
            Assert.SkipUnless(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct),
                "The script names the store owner 'darling', and this rig has no such role.");
            await ExecAsync(owner,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (4242, 'aq-script', 'aq-script', TRUE)", ct);

            var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                    System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                .Replace("CHANGE_ME_ADMIN_PASSWORD", "ScriptAdmin5654", StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", "ScriptViewer5654", StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", "ScriptMcp5654", StringComparison.Ordinal)
                .Replace("CHANGE_ME_WEB_PASSWORD", "ScriptWeb6991", StringComparison.Ordinal);
            await ExecAsync(owner, script, ct);

            await using (var web = await LoginAsync(scratch, "web", "ScriptWeb6991", ct))
            {
                await ExecAsync(web, "BEGIN ISOLATION LEVEL REPEATABLE READ", ct);
                await ExecAsync(web, "SELECT 1", ct);
                var refused = await Assert.ThrowsAsync<PostgresException>(() =>
                    ScalarAsync<long>(web, "SELECT config.request_active_queries_live(4242, 'web')", ct));
                Assert.Equal("DL400", refused.SqlState);
                Assert.Contains("READ COMMITTED", refused.MessageText, StringComparison.Ordinal);
                await ExecAsync(web, "ROLLBACK", ct);

                Assert.Equal(0L, await ScalarAsync<long>(owner, "SELECT count(*) FROM config.config_command", ct));
                Assert.True(await ScalarAsync<long>(web, "SELECT config.request_active_queries_live(4242, 'web')", ct) > 0);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// A store whose default privileges would give a role write access to new <c>config</c> tables (the state the
    /// provisioning script leaves) gets the three tables with no write privilege for that role, from the rung itself.
    /// </summary>
    [Fact]
    public async Task TheRung_TakesWritePrivilegesFromPublicAndTheAdminRole_OfTheThreeTables()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync<bool>(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct),
                "A cluster-wide admin/viewer/mcp role already exists on this rig.");
            await ExecAsync(owner, "CREATE ROLE admin NOLOGIN", ct);
            await ExecAsync(owner, $@"
ALTER DEFAULT PRIVILEGES IN SCHEMA config GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA config GRANT INSERT ON TABLES TO PUBLIC;
DROP TABLE config.external_alert_state, config.external_alert_pending, config.external_alert_source;
ALTER TABLE config.config_mute_rules DROP COLUMN external_source", ct);
            try
            {
                await ExecAsync(owner, ExternalAlertTables.CreateSql, ct);

                foreach (var table in new[] { "external_alert_source", "external_alert_pending", "external_alert_state" })
                {
                    Assert.False(await ScalarAsync<bool>(owner,
                        $"SELECT has_table_privilege('admin', 'config.{table}', 'INSERT, UPDATE, DELETE, TRUNCATE') OR has_table_privilege('public', 'config.{table}', 'INSERT')", ct));
                    Assert.True(await ScalarAsync<bool>(owner, $"SELECT has_table_privilege('admin', 'config.{table}', 'SELECT')", ct));
                }
            }
            finally
            {
                await ExecAsync(owner, $"ALTER DEFAULT PRIVILEGES IN SCHEMA config REVOKE ALL ON TABLES FROM admin; ALTER DEFAULT PRIVILEGES IN SCHEMA config REVOKE ALL ON TABLES FROM PUBLIC", ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    private static string NormalizeSql(string sql) =>
        Regex.Replace(Regex.Replace(sql, @"(?m)^\s*--.*$", ""), @"\s+", " ").Trim();

    /// <summary>
    /// A self-managed store is told to run <c>tools/provision-roles.sql</c>, so that script is what it gets: the same
    /// function text as the managed batch (comments and spacing set aside), the same grants, and the function's safety
    /// shape pinned as text as well: a pinned search_path with pg_temp last, no dynamic SQL, no argument that names a
    /// source, and the caller looked up by session_user.
    /// </summary>
    [Fact]
    public void TheByoScript_CarriesTheSameSubmitFunction_AsTheManagedBatch_AndTheFunctionHasItsSafeShape()
    {
        var function = DarlingManagedRoles.BuildExternalAlertSubmitFunctionSql("config");
        var byo = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql");
        var batch = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);

        Assert.Contains(NormalizeSql(function), NormalizeSql(byo), StringComparison.Ordinal);
        Assert.Contains(NormalizeSql(function), NormalizeSql(batch), StringComparison.Ordinal);

        Assert.Contains("SET search_path = pg_catalog, pg_temp", function, StringComparison.Ordinal);
        Assert.Contains("SECURITY DEFINER", function, StringComparison.Ordinal);
        Assert.Contains("s.role_name = session_user::text", function, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bEXECUTE\b", function);
        Assert.DoesNotContain("current_user", function, StringComparison.Ordinal);

        /* Every table the body names carries its schema. */
        Assert.DoesNotMatch(@"\b(FROM|INTO|JOIN|UPDATE)\s+(?!config\.)external_alert_", function);

        var signature = Regex.Match(function, @"submit_external_alert\((?<args>.*?)\)\s*RETURNS", RegexOptions.Singleline).Groups["args"].Value;
        Assert.DoesNotMatch(@"(?i)source|role|login|user", signature);

        foreach (var sql in new[] { batch, byo })
        {
            Assert.Contains(
                $"REVOKE ALL ON FUNCTION config.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature}) FROM PUBLIC;", sql, StringComparison.Ordinal);
            Assert.Contains(
                $"REVOKE ALL ON FUNCTION config.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature}) FROM admin, viewer, web, mcp;", sql, StringComparison.Ordinal);
            Assert.Contains("REVOKE ALL ON config.external_alert_source, config.external_alert_pending, config.external_alert_state", sql, StringComparison.Ordinal);
            Assert.Contains("FROM PUBLIC, admin, viewer, web, mcp CASCADE", sql, StringComparison.Ordinal);
            Assert.Contains(DarlingManagedRoles.AlertSourceRoleMarker, sql, StringComparison.Ordinal);
            Assert.Contains("GRANT CONNECT ON DATABASE %I TO %I", sql, StringComparison.Ordinal);
            Assert.Contains("NOINHERIT CONNECTION LIMIT " + DarlingManagedRoles.AlertSourceConnectionLimit, sql, StringComparison.Ordinal);
            Assert.Matches(@"REVOKE ALL ON FUNCTION config\.submit_external_alert\([^)]*\)\s+FROM %I'", sql);
        }

        /* The function: a share-compatible row lock, the call-time clock, and the finite-value check, in both copies. */
        Assert.Contains("FOR NO KEY UPDATE OF s;", function, StringComparison.Ordinal);
        Assert.DoesNotContain("FOR UPDATE OF s", function, StringComparison.Ordinal);
        Assert.DoesNotContain("now()", function, StringComparison.Ordinal);
        Assert.Contains("p_value IN ('NaN'::double precision", function, StringComparison.Ordinal);

        /* The managed batch's table revoke runs after every blanket write grant, and before the password key tables' revoke (which stays the last of its kind) and the membership pass. */
        var tableRevokeAt = batch.IndexOf("REVOKE ALL ON config.external_alert_source", StringComparison.Ordinal);
        Assert.True(tableRevokeAt < batch.IndexOf("REVOKE ALL ON config.legacy_secret_pin_candidate", StringComparison.Ordinal));
        Assert.True(tableRevokeAt > batch.IndexOf("GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config", StringComparison.Ordinal));
        Assert.True(tableRevokeAt < batch.IndexOf("-- 11. Role memberships", StringComparison.Ordinal));
    }
}
