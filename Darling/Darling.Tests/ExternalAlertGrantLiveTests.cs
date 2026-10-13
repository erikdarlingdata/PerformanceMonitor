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
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres and per-run login names; the
       fixed admin/viewer/mcp roles are why the class sits in the pg-cluster-roles collection. */
    private static readonly string RunSuffix = Guid.NewGuid().ToString("N")[..8];

    private static readonly string RoleA = "ea_src_a_" + RunSuffix;

    private static readonly string RoleB = "ea_src_b_" + RunSuffix;

    private static readonly string RoleUnmarked = "ea_plain_" + RunSuffix;

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
    private static async Task<(int IdA, int IdB)> SetUpAsync(NpgsqlConnection owner, ScratchPostgres scratch, CancellationToken ct)
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

    private static async Task CleanUpAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await new LiveCleanupBatch(cleanup).DropRolesAsync($@"
DO $do$
DECLARE
   r text;
BEGIN
   FOR r IN SELECT rolname FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp', '{RoleA}', '{RoleB}', '{RoleUnmarked}') LOOP
      EXECUTE format('DROP OWNED BY %I', r);
      EXECUTE format('DROP ROLE %I', r);
   END LOOP;
END $do$", ["admin", "viewer", "mcp", RoleA, RoleB, RoleUnmarked], cleanupCt));

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
            Assert.Equal(0, await ScalarAsync<long>(owner,
                $"SELECT count(*) FROM pg_auth_members WHERE member = '{RoleA}'::regrole", ct));

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
            Assert.Equal(0, await ScalarAsync<long>(owner, $"SELECT count(*) FROM pg_auth_members WHERE member = '{RoleA}'::regrole", ct));
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
                $"REVOKE ALL ON FUNCTION config.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature}) FROM admin, viewer, mcp;", sql, StringComparison.Ordinal);
            Assert.Contains("REVOKE ALL ON config.external_alert_source, config.external_alert_pending, config.external_alert_state", sql, StringComparison.Ordinal);
            Assert.Contains("FROM PUBLIC, admin, viewer, mcp CASCADE", sql, StringComparison.Ordinal);
            Assert.Contains(DarlingManagedRoles.AlertSourceRoleMarker, sql, StringComparison.Ordinal);
            Assert.Contains("GRANT CONNECT ON DATABASE %I TO %I", sql, StringComparison.Ordinal);
        }

        /* The managed batch's table revoke runs after the password key tables' revoke and every blanket write grant, and before the membership pass. */
        var tableRevokeAt = batch.IndexOf("REVOKE ALL ON config.external_alert_source", StringComparison.Ordinal);
        Assert.True(tableRevokeAt > batch.IndexOf("REVOKE ALL ON config.legacy_secret_pin_candidate", StringComparison.Ordinal));
        Assert.True(tableRevokeAt > batch.IndexOf("GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config", StringComparison.Ordinal));
        Assert.True(tableRevokeAt < batch.IndexOf("-- 11. Role memberships", StringComparison.Ordinal));
    }
}
