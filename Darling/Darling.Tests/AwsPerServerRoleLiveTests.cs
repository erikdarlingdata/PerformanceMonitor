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
using System.Reflection;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another one.
   The login role a fact needs is created for it and dropped in its cleanup. */

/// <summary>
/// The per-server AWS role (#5452) against a store: the rung on a store that stopped below it and on a fresh one, the
/// table's check, the column ACL, the 17-argument edit function and its 15-argument wrapper, and the trigger rule that
/// holds every role but the owner.
/// </summary>
public sealed class AwsPerServerRoleLiveTests
{
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string OtherRole = "arn:aws:iam::123456789012:role/darling-other";
    private const string External = "tenant-1234";
    private const string OtherExternal = "tenant-5678";

    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private static string? BaseConnectionString() => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private static string Lit(string? value) =>
        value is null ? "NULL" : "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static async Task<NpgsqlConnection> OpenOwnerAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(
            new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static Task InsertServerAsync(NpgsqlConnection connection, int id, string? role, string? externalId, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, database, auth, username, engine, port, aws_role_arn, aws_external_id) "
            + $"VALUES ({id}, 'srv-{id}', 'host-{id}', 'postgres', 'sql', 'monitor', 'postgres', 5432, {Lit(role)}, {Lit(externalId)})", ct);

    private static Task<string?> StoredAsync(NpgsqlConnection connection, int id, CancellationToken ct) =>
        TextAsync(connection,
            "SELECT COALESCE(aws_role_arn, '<null>') || ' | ' || COALESCE(aws_external_id, '<null>') || ' | ' || aws_external_id_set::text "
            + $"FROM config.config_monitored_servers WHERE server_id = {id}", ct);

    private static async Task<string?> RefusalAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (PostgresException ex)
        {
            return ex.SqlState;
        }
    }

    private static async Task<string> TokenAsync(NpgsqlConnection connection, int id, CancellationToken ct) =>
        (await TextAsync(connection,
            $"SELECT to_char(modified_at, 'YYYY-MM-DD HH24:MI:SS.US') FROM config.config_monitored_servers WHERE server_id = {id}", ct))!;

    /// <summary>One call of the 17-argument function (the AWS pair last), returning its outcome text.</summary>
    private static async Task<string?> EditAsync(
        NpgsqlConnection connection, int id, string columns, string? role, string? externalId, CancellationToken ct)
    {
        var token = await TokenAsync(connection, id, ct);
        return await TextAsync(connection,
            $"SELECT outcome FROM config.edit_monitored_server({id}, '{token}'::timestamp, ARRAY[{columns}]::text[], "
            + $"NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, {Lit(role)}, {Lit(externalId)})", ct);
    }

    /// <summary>One call of the 15-argument wrapper: no AWS arguments, but the caller may still NAME the AWS columns.</summary>
    private static async Task<string?> LegacyEditAsync(
        NpgsqlConnection connection, int id, string columns, string? name, CancellationToken ct)
    {
        var token = await TokenAsync(connection, id, ct);
        return await TextAsync(connection,
            $"SELECT outcome FROM config.edit_monitored_server({id}, '{token}'::timestamp, ARRAY[{columns}]::text[], "
            + $"{Lit(name)}, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", ct);
    }

    private static async Task<NpgsqlConnection> OpenAsRoleAsync(ScratchPostgres scratch, string role, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Username = role,
            Password = RolePassword,
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task<string> CreateLoginRoleAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        var role = "aws_role_" + Guid.NewGuid().ToString("N")[..8];
        await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
        await ExecAsync(owner, $"GRANT USAGE ON SCHEMA config TO {role}", ct);
        return role;
    }

    private static Task DropLoginRoleAsync(NpgsqlConnection cleanup, string? role) =>
        role is null
            ? Task.CompletedTask
            : ExecAsync(cleanup, $"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}", CancellationToken.None);

    /* ---- the rung -------------------------------------------------------------------------------------- */

    [Fact]
    public async Task TheRung_AppliesToAStoreThatStoppedBelowIt_AndToAFreshOne_AndARerunIsANoOp()
    {
        var baseConnectionString = BaseConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live AWS role tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await OpenOwnerAsync(scratch, ct);

            /* A fresh store took every rung in order, this one included. */
            Assert.Equal("3", await TextAsync(owner,
                "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_monitored_servers' "
                + "AND column_name IN ('aws_role_arn', 'aws_external_id', 'aws_external_id_set')", ct));
            Assert.Equal(AwsPerServerRoleRungTests.RungVersion.ToString(CultureInfo.InvariantCulture),
                await TextAsync(owner, "SELECT max(version) FROM darling_schema_version", ct));
            Assert.Equal(1L.ToString(CultureInfo.InvariantCulture), await TextAsync(owner,
                "SELECT count(*) FROM pg_constraint WHERE conname = 'config_monitored_servers_aws_role_check'", ct));
            Assert.True(await ProbeSentinelAsync(owner, ct), "the viewer's probe must see the role column on a migrated store");

            /* A V167 store: the columns gone, the stamp rolled back, a server already registered. */
            await InsertServerAsync(owner, 8101, null, null, ct);
            await ExecAsync(owner, "ALTER TABLE config.config_monitored_servers DROP COLUMN aws_external_id_set, DROP COLUMN aws_external_id, DROP COLUMN aws_role_arn", ct);
            await ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version >= 168", ct);
            Assert.False(await ProbeSentinelAsync(owner, ct), "the probe must not see the role column once it is gone");

            await PgMigrations.MigrateAsync(owner, ct);

            Assert.True(await ProbeSentinelAsync(owner, ct));
            Assert.Equal("<null> | <null> | false", await StoredAsync(owner, 8101, ct));
            Assert.Equal("168", await TextAsync(owner, "SELECT max(version) FROM darling_schema_version", ct));

            /* Running the rung's text again changes nothing and raises nothing. */
            var rung = PgMigrations.Scripts.Single(s => s.Version == AwsPerServerRoleRungTests.RungVersion).Sql;
            await ExecAsync(owner, rung, ct);
            await ExecAsync(owner, rung, ct);
            Assert.Equal("<null> | <null> | false", await StoredAsync(owner, 8101, ct));
            Assert.Equal("1", await TextAsync(owner,
                "SELECT count(*) FROM pg_constraint WHERE conname = 'config_monitored_servers_aws_role_check'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<bool> ProbeSentinelAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var arity = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetParameters().Length;
        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return reader.GetBoolean(arity - 1);
    }

    /* ---- the check ------------------------------------------------------------------------------------- */

    [Fact]
    public async Task TheCheck_RefusesAnIdWithNoRole_ABadArn_AndASpace_AndAcceptsTheCorpus()
    {
        var baseConnectionString = BaseConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live AWS role tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await OpenOwnerAsync(scratch, ct);
            var id = 8200;

            /* Refused with 23514 (check_violation): an ID with no role, a role that is not an IAM role ARN, a role with a
               space, an ID with a space, an ID that is too short. */
            foreach (var (role, externalId) in new (string?, string?)[]
            {
                (null, External),
                ("not-an-arn", null),
                ("arn:aws:iam::123456789012:user/darling", null),
                ("arn:aws:iam::12345:role/darling", null),
                ("arn:aws:iam::123456789012:role/has space", null),
                ("arn:aws:iam::123456789012:role/", null),
                (Role, "has space"),
                (Role, "x"),
                (Role, new string('x', 1225)),
            })
            {
                id++;
                var state = await RefusalAsync(() => InsertServerAsync(owner, id, role, externalId, ct));
                Assert.Equal("23514", state);
            }

            Assert.Equal("0", await TextAsync(owner, "SELECT count(*) FROM config.config_monitored_servers", ct));

            /* Accepted: no role, a role, a role with an ID, the other partitions, a path, and the longest ID. */
            foreach (var (role, externalId) in new (string?, string?)[]
            {
                (null, null),
                (Role, null),
                (Role, External),
                ("arn:aws-cn:iam::123456789012:role/darling", null),
                ("arn:aws-us-gov:iam::123456789012:role/darling", "ab"),
                ("arn:aws:iam::123456789012:role/monitoring/team/darling-monitor", null),
                ("arn:aws:iam::123456789012:role/name_with+=,.@-chars", "A-b_C+d=e,f.g@h:i/j-k"),
                (Role, new string('x', 1224)),
            })
            {
                id++;
                await InsertServerAsync(owner, id, role, externalId, ct);
            }

            Assert.Equal("8", await TextAsync(owner, "SELECT count(*) FROM config.config_monitored_servers", ct));

            /* The generated flag follows the ID, and an update that would leave an ID with no role is refused too. */
            Assert.Equal("false", await TextAsync(owner, "SELECT aws_external_id_set::text FROM config.config_monitored_servers WHERE server_id = 8211", ct));
            Assert.Equal("true", await TextAsync(owner, "SELECT aws_external_id_set::text FROM config.config_monitored_servers WHERE server_id = 8212", ct));
            Assert.Equal("23514", await RefusalAsync(() =>
                ExecAsync(owner, "UPDATE config.config_monitored_servers SET aws_role_arn = NULL WHERE server_id = 8212", ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /* ---- the column ACL -------------------------------------------------------------------------------- */

    [Fact]
    public async Task AReadOnlyRole_ReadsTheRoleAndTheFlag_ButGets42501OnTheExternalId()
    {
        var baseConnectionString = BaseConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live AWS role tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        string? login = null;
        var bodySucceeded = false;
        try
        {
            await using var owner = await OpenOwnerAsync(scratch, ct);
            await InsertServerAsync(owner, 8301, Role, External, ct);

            /* The grant is the one the managed batch and the script give viewer and mcp: the non-secret list, from the ACL. */
            login = await CreateLoginRoleAsync(owner, ct);
            var table = DarlingManagedRoles.ViewerRestrictedConfigTables.Single(t => t.Table == "config_monitored_servers");
            await ExecAsync(owner, $"GRANT SELECT ({string.Join(", ", table.NonSecretColumns)}) ON config.config_monitored_servers TO {login}", ct);

            await using var reader = await OpenAsRoleAsync(scratch, login, ct);
            Assert.Equal($"{Role} | true",
                await TextAsync(reader, "SELECT aws_role_arn || ' | ' || aws_external_id_set::text FROM config.config_monitored_servers WHERE server_id = 8301", ct));

            Assert.Equal("42501", await RefusalAsync(() =>
                TextAsync(reader, "SELECT aws_external_id FROM config.config_monitored_servers WHERE server_id = 8301", ct)));
            Assert.Equal("42501", await RefusalAsync(() =>
                TextAsync(reader, "SELECT * FROM config.config_monitored_servers WHERE server_id = 8301", ct)));

            bodySucceeded = true;
        }
        finally
        {
            var loginToDrop = login;
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (cleanup, _) => DropLoginRoleAsync(cleanup, loginToDrop));
        }
    }

    /* ---- the edit function and its wrapper -------------------------------------------------------------- */

    [Fact]
    public async Task TheEditFunction_SetsKeepsChangesAndClearsTheRole_AndAnswersTheTwoNewOutcomes()
    {
        var baseConnectionString = BaseConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live AWS role tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await OpenOwnerAsync(scratch, ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config"), ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildEditMonitoredServerLegacyWrapperSql("config"), ct);
            await InsertServerAsync(owner, 8401, null, null, ct);

            /* Setting a role with an ID. */
            Assert.Equal("saved", await EditAsync(owner, 8401, "'aws_role_arn', 'aws_external_id'", Role, External, ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8401, ct));

            /* Left out of p_columns, the role and the ID are kept, whatever values ride along unnamed. */
            Assert.Equal("saved", await EditAsync(owner, 8401, string.Empty, OtherRole, OtherExternal, ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8401, ct));

            /* A new role over a stored ID, with no ID named, would carry the old ID along unseen: refused, nothing written. */
            Assert.Equal("external_id_needed", await EditAsync(owner, 8401, "'aws_role_arn'", OtherRole, null, ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8401, ct));

            /* The same role again is not a change, so it needs no ID. */
            Assert.Equal("saved", await EditAsync(owner, 8401, "'aws_role_arn'", Role, null, ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8401, ct));

            /* A new role with the ID sent again, and with the ID cleared (named, NULL). */
            Assert.Equal("saved", await EditAsync(owner, 8401, "'aws_role_arn', 'aws_external_id'", OtherRole, OtherExternal, ct));
            Assert.Equal($"{OtherRole} | {OtherExternal} | true", await StoredAsync(owner, 8401, ct));
            Assert.Equal("saved", await EditAsync(owner, 8401, "'aws_role_arn', 'aws_external_id'", Role, null, ct));
            Assert.Equal($"{Role} | <null> | false", await StoredAsync(owner, 8401, ct));

            /* The ID alone, for the role that is stored. */
            Assert.Equal("saved", await EditAsync(owner, 8401, "'aws_external_id'", null, External, ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8401, ct));

            /* An ID with a cleared role: refused (the role is named NULL and an ID rides along). */
            Assert.Equal("external_id_needs_role", await EditAsync(owner, 8401, "'aws_role_arn', 'aws_external_id'", null, External, ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8401, ct));

            /* Clearing is explicit: the role named with NULL, which takes the ID with it. */
            Assert.Equal("saved", await EditAsync(owner, 8401, "'aws_role_arn'", null, null, ct));
            Assert.Equal("<null> | <null> | false", await StoredAsync(owner, 8401, ct));

            /* An ID named for a server that has no role: refused, and a plain edit of another column is unaffected. */
            Assert.Equal("external_id_needs_role", await EditAsync(owner, 8401, "'aws_external_id'", null, External, ct));
            Assert.Equal("saved", await EditAsync(owner, 8401, "'aws_external_id'", null, null, ct));
            Assert.Equal("<null> | <null> | false", await StoredAsync(owner, 8401, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheFifteenArgumentWrapper_KeepsTheRole_AndACallerNamingTheAwsColumnsCannotChangeOrClearIt()
    {
        var baseConnectionString = BaseConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live AWS role tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await OpenOwnerAsync(scratch, ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildEditMonitoredServerFunctionSql("config"), ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildEditMonitoredServerLegacyWrapperSql("config"), ct);
            await InsertServerAsync(owner, 8501, Role, External, ct);

            /* An old-shape edit of another column leaves the role and the ID alone. */
            Assert.Equal("saved", await LegacyEditAsync(owner, 8501, "'name'", "renamed", ct));
            Assert.Equal("renamed", await TextAsync(owner, "SELECT name FROM config.config_monitored_servers WHERE server_id = 8501", ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8501, ct));

            /* Naming the AWS columns in p_columns does nothing: there are no arguments to carry a value, and the names are
               stripped, so the wrapper cannot clear the role or the ID. The rest of the edit still applies. */
            Assert.Equal("saved", await LegacyEditAsync(owner, 8501, "'aws_role_arn', 'aws_external_id', 'name'", "renamed-again", ct));
            Assert.Equal("renamed-again", await TextAsync(owner, "SELECT name FROM config.config_monitored_servers WHERE server_id = 8501", ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8501, ct));

            Assert.Equal("saved", await LegacyEditAsync(owner, 8501, "'aws_role_arn'", null, ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8501, ct));
            Assert.Equal("saved", await LegacyEditAsync(owner, 8501, "'aws_external_id'", null, ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8501, ct));

            /* A NULL column list is an empty one. */
            var token = await TokenAsync(owner, 8501, ct);
            Assert.Equal("saved", await TextAsync(owner,
                $"SELECT outcome FROM config.edit_monitored_server(8501, '{token}'::timestamp, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", ct));
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8501, ct));

            /* The two functions are two signatures of one name, and the wrapper is the definer-pinned one. */
            Assert.Equal("2", await TextAsync(owner,
                "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'config' AND p.proname = 'edit_monitored_server'", ct));
            Assert.Equal("True", await TextAsync(owner,
                "SELECT bool_and(prosecdef AND proconfig @> ARRAY['search_path=pg_catalog, pg_temp']) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                + "WHERE n.nspname = 'config' AND p.proname = 'edit_monitored_server'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /* ---- the trigger rule ------------------------------------------------------------------------------ */

    [Fact]
    public async Task ARoleThatIsNotTheOwner_CannotPutANewRoleOverAStoredId_Pw004()
    {
        var baseConnectionString = BaseConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live AWS role tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        string? login = null;
        var bodySucceeded = false;
        try
        {
            await using var owner = await OpenOwnerAsync(scratch, ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildServerPasswordRulesSql("config"), ct);
            await InsertServerAsync(owner, 8601, Role, External, ct);
            await InsertServerAsync(owner, 8602, Role, null, ct);

            login = await CreateLoginRoleAsync(owner, ct);
            await ExecAsync(owner, $"GRANT SELECT, UPDATE ON config.config_monitored_servers TO {login}", ct);

            /* An update here bumps the reload beacon, which a role that edits servers is granted (the managed batch grants it). */
            await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(owner, $"GRANT SELECT, UPDATE ON config.config_service TO {login}", ct);
            await using var role = await OpenAsRoleAsync(scratch, login, ct);

            /* A new role with the stored ID untouched: refused with PW004 and the sentence. */
            var refused = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(role, $"UPDATE config.config_monitored_servers SET aws_role_arn = {Lit(OtherRole)} WHERE server_id = 8601", ct));
            Assert.Equal("PW004", refused.SqlState);
            Assert.Equal(AwsRoleSettings.RoleChangeNeedsExternalIdMessage, refused.MessageText);
            Assert.Equal($"{Role} | {External} | true", await StoredAsync(owner, 8601, ct));

            /* The ID sent with it, or cleared with it, or the same role again, or a server with no ID: all pass. */
            await ExecAsync(role, $"UPDATE config.config_monitored_servers SET aws_role_arn = {Lit(OtherRole)}, aws_external_id = {Lit(OtherExternal)} WHERE server_id = 8601", ct);
            Assert.Equal($"{OtherRole} | {OtherExternal} | true", await StoredAsync(owner, 8601, ct));
            await ExecAsync(role, $"UPDATE config.config_monitored_servers SET aws_role_arn = {Lit(Role)}, aws_external_id = NULL WHERE server_id = 8601", ct);
            Assert.Equal($"{Role} | <null> | false", await StoredAsync(owner, 8601, ct));
            await ExecAsync(role, $"UPDATE config.config_monitored_servers SET aws_role_arn = {Lit(OtherRole)} WHERE server_id = 8602", ct);
            Assert.Equal($"{OtherRole} | <null> | false", await StoredAsync(owner, 8602, ct));
            await ExecAsync(role, "UPDATE config.config_monitored_servers SET name = 'renamed' WHERE server_id = 8602", ct);

            /* The owner is not held to it (the edit function answers external_id_needed first). */
            await InsertServerAsync(owner, 8603, Role, External, ct);
            await ExecAsync(owner, $"UPDATE config.config_monitored_servers SET aws_role_arn = {Lit(OtherRole)} WHERE server_id = 8603", ct);
            Assert.Equal($"{OtherRole} | {External} | true", await StoredAsync(owner, 8603, ct));

            bodySucceeded = true;
        }
        finally
        {
            var loginToDrop = login;
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (cleanup, _) => DropLoginRoleAsync(cleanup, loginToDrop));
        }
    }
}
