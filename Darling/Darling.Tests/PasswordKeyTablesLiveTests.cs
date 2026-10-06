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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live half of the V165 pins (#5366), over a scratch store provisioned by the product's own batch: the owner-only
/// trigger on each of the four tables, with and without grants, the temporary-table lookalike a caller can create, the
/// unique <c>current</c> key, the one <c>pending</c> marker row and the two reads. The roles carry fixed cluster-wide
/// names, so a rig that already has them skips these facts (the same rule the security-split facts follow).
/// </summary>
[Collection("live-postgres")]
public sealed class PasswordKeyTablesLiveTests
{
    /// <summary>One statement per table and verb; each statement reaches the row trigger (or the TRUNCATE trigger) of its table.</summary>
    private static readonly Dictionary<string, string[]> Writes = new()
    {
        ["config.password_key"] =
        [
            "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('00112233445566ff', '\\x01'::bytea, 'RSA-OAEP-SHA256', 'replaced')",
            "UPDATE config.password_key SET algorithm = 'RSA-OAEP-SHA256-x'",
            "DELETE FROM config.password_key",
            "TRUNCATE config.password_key",
        ],
        ["config.password_key_service"] =
        [
            "INSERT INTO config.password_key_service (service_host, key_id, state, updated_at) VALUES ('example-sql-02', NULL, 'ok', now() AT TIME ZONE 'UTC')",
            "UPDATE config.password_key_service SET note = 'x'",
            "DELETE FROM config.password_key_service",
            "TRUNCATE config.password_key_service",
        ],
        ["config.legacy_secret_pin"] =
        [
            "INSERT INTO config.legacy_secret_pin (server_id, slot, value_sha256, binding_sha256) VALUES (2, 'server', '\\x01'::bytea, '\\x02'::bytea)",
            "UPDATE config.legacy_secret_pin SET slot = 'smtp'",
            "DELETE FROM config.legacy_secret_pin",
            "TRUNCATE config.legacy_secret_pin",
        ],
        ["config.legacy_secret_pin_marker"] =
        [
            "INSERT INTO config.legacy_secret_pin_marker (id, state) VALUES (2, 'done')",
            "UPDATE config.legacy_secret_pin_marker SET state = 'done'",
            "DELETE FROM config.legacy_secret_pin_marker",
            "TRUNCATE config.legacy_secret_pin_marker",
        ],
    };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheOwnerWrites_AndEveryOtherRoleIsRefused_WithOrWithoutGrants()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key table live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        Assert.SkipWhen(
            await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) is true,
            "A cluster-wide admin/viewer/mcp role already exists on this rig; the provisioning batch would adopt it.");

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName)), ct);

            /* One row per table, so an UPDATE or DELETE reaches its row trigger. */
            await SeedRowsAsync(owner, ct);

            await using var admin = await OpenAsAsync(scratch, "admin", ProvisioningTestSecrets.AdminPassword, ct);

            /* After the full provisioning batch the second layer has taken admin's write privileges away: refused. */
            foreach (var statement in Writes.Values.SelectMany(v => v))
            {
                Assert.Equal("42501", await SqlStateOfAsync(admin, statement, ct));
            }

            /* The blanket grant re-run, as a later start or a re-run of the script does, and TRUNCATE on top. The privileges
               check now passes, so only the trigger stands in the way. */
            await ExecAsync(owner, "GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin; GRANT TRUNCATE ON ALL TABLES IN SCHEMA config TO admin;", ct);
            foreach (var statement in Writes.Values.SelectMany(v => v))
            {
                Assert.Equal(PasswordKeyTables.OwnerOnlySqlState, await SqlStateOfAsync(admin, statement, ct));
            }

            /* The same for the other two roles holding every privilege. */
            await ExecAsync(owner,
                "GRANT INSERT, UPDATE, DELETE, TRUNCATE ON config.password_key, config.password_key_service, config.legacy_secret_pin, config.legacy_secret_pin_marker TO viewer, mcp;", ct);
            await using var viewer = await OpenAsAsync(scratch, "viewer", ProvisioningTestSecrets.ViewerPassword, ct);
            await using var mcp = await OpenAsAsync(scratch, "mcp", ProvisioningTestSecrets.McpPassword, ct);
            foreach (var statement in Writes.Values.SelectMany(v => v))
            {
                Assert.Equal(PasswordKeyTables.OwnerOnlySqlState, await SqlStateOfAsync(viewer, statement, ct));
                Assert.Equal(PasswordKeyTables.OwnerOnlySqlState, await SqlStateOfAsync(mcp, statement, ct));
            }

            /* Nothing the refused writes tried changed a row. */
            Assert.Equal(1L, await ScalarAsync(owner, "SELECT count(*) FROM config.password_key", ct));
            Assert.Equal(1L, await ScalarAsync(owner, "SELECT count(*) FROM config.password_key_service", ct));
            Assert.Equal(1L, await ScalarAsync(owner, "SELECT count(*) FROM config.legacy_secret_pin", ct));
            Assert.Equal("pending", await ScalarAsync(owner, "SELECT state FROM config.legacy_secret_pin_marker", ct));

            /* The owner's own writes pass, whatever the other roles hold. */
            await ExecAsync(owner, "UPDATE config.password_key SET algorithm = 'RSA-OAEP-SHA256-x'", ct);
            await ExecAsync(owner, "UPDATE config.legacy_secret_pin_marker SET state = 'done'", ct);
            await ExecAsync(owner, "UPDATE config.legacy_secret_pin_marker SET state = 'pending'", ct);
            await ExecAsync(owner, "INSERT INTO config.password_key (key_id, public_key, algorithm, state, replaced_reason) VALUES ('00112233445566ff', '\\x01'::bytea, 'RSA-OAEP-SHA256', 'replaced', 'reset')", ct);
            await ExecAsync(owner, "DELETE FROM config.password_key WHERE key_id = '00112233445566ff'", ct);
            await ExecAsync(owner, "TRUNCATE config.password_key_service", ct);
            Assert.Equal(0L, await ScalarAsync(owner, "SELECT count(*) FROM config.password_key_service", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    "DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    [Fact]
    public async Task ACallerWhoCreatesATemporaryTableNamedLikeACatalogRelation_IsStillRefused_OnEveryTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key table live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        Assert.SkipWhen(
            await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) is true,
            "A cluster-wide admin/viewer/mcp role already exists on this rig; the provisioning batch would adopt it.");

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName)), ct);
            await SeedRowsAsync(owner, ct);

            /* The viewer keeps TEMPORARY through PUBLIC on this store. Give it every write privilege so the trigger is the only
               check left, then let it create a temporary table that is shaped like pg_class and names the viewer as the owner of
               every relation. */
            await ExecAsync(owner,
                "GRANT INSERT, UPDATE, DELETE, TRUNCATE ON config.password_key, config.password_key_service, config.legacy_secret_pin, config.legacy_secret_pin_marker TO viewer;", ct);
            await using var viewer = await OpenAsAsync(scratch, "viewer", ProvisioningTestSecrets.ViewerPassword, ct);
            await ExecAsync(viewer,
                "CREATE TEMP TABLE pg_class (oid oid, relowner oid); " +
                "INSERT INTO pg_temp.pg_class SELECT c.oid, (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'viewer') FROM pg_catalog.pg_class AS c;", ct);

            foreach (var statement in Writes.Values.SelectMany(v => v))
            {
                Assert.Equal(PasswordKeyTables.OwnerOnlySqlState, await SqlStateOfAsync(viewer, statement, ct));
            }

            /* The control: the same lookup written the other way (an unqualified catalog name, and a path that leaves pg_temp
               to be searched first) is passed by the same temporary table, so the facts above can fail. */
            await ExecAsync(owner, @"
CREATE TABLE public.shadow_control (n integer);
CREATE FUNCTION public.shadow_control_guard() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $fn$
BEGIN
    IF NOT pg_has_role(session_user, (SELECT c.relowner FROM pg_class AS c WHERE c.oid = TG_RELID), 'USAGE') THEN
        RAISE EXCEPTION 'refused' USING ERRCODE = 'PW010';
    END IF;
    RETURN NEW;
END;
$fn$;
CREATE TRIGGER shadow_control_trg BEFORE INSERT ON public.shadow_control FOR EACH ROW EXECUTE FUNCTION public.shadow_control_guard();
GRANT INSERT ON public.shadow_control TO viewer;", ct);
            Assert.Null(await SqlStateOfAsync(viewer, "INSERT INTO public.shadow_control VALUES (1)", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    "DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    [Fact]
    public async Task ASecondCurrentKeyFails_AndTheReadsReturnTheCurrentKeyAndTheNewestServiceRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key table live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Null(await PasswordKeyTables.ReadCurrentAsync(connection, ct));
            Assert.Null(await PasswordKeyTables.ReadNewestServiceStateAsync(connection, ct));

            await ExecAsync(connection, "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('aaaaaaaaaaaaaaaa', '\\x0102'::bytea, 'RSA-OAEP-SHA256', 'current')", ct);
            Assert.Equal("23505", await SqlStateOfAsync(connection,
                "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('bbbbbbbbbbbbbbbb', '\\x0304'::bytea, 'RSA-OAEP-SHA256', 'current')", ct));
            Assert.Equal("23514", await SqlStateOfAsync(connection,
                "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('NOT-A-KEY-ID', '\\x0304'::bytea, 'RSA-OAEP-SHA256', 'replaced')", ct));

            /* Replacing the current key frees the slot for the next one. */
            await ExecAsync(connection, "UPDATE config.password_key SET state = 'replaced', replaced_reason = 'rotated', replaced_at = now() AT TIME ZONE 'UTC' WHERE key_id = 'aaaaaaaaaaaaaaaa'", ct);
            await ExecAsync(connection, "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('bbbbbbbbbbbbbbbb', '\\x0304'::bytea, 'RSA-OAEP-SHA256', 'current')", ct);

            var key = await PasswordKeyTables.ReadCurrentAsync(connection, ct);
            Assert.NotNull(key);
            Assert.Equal("bbbbbbbbbbbbbbbb", key.KeyId);
            Assert.Equal(new byte[] { 0x03, 0x04 }, key.Spki);
            Assert.Equal("RSA-OAEP-SHA256", key.Algorithm);

            await ExecAsync(connection, "INSERT INTO config.password_key_service (service_host, key_id, state, note, updated_at) VALUES ('example-sql-01', 'aaaaaaaaaaaaaaaa', 'loading', NULL, '2026-01-05 10:00:00')", ct);
            await ExecAsync(connection, "INSERT INTO config.password_key_service (service_host, key_id, state, note, updated_at) VALUES ('example-sql-02', NULL, 'mismatch', 'a note', '2026-01-05 11:00:00')", ct);
            Assert.Equal("23514", await SqlStateOfAsync(connection,
                "INSERT INTO config.password_key_service (service_host, state, updated_at) VALUES ('example-sql-03', 'unknown', now() AT TIME ZONE 'UTC')", ct));

            var state = await PasswordKeyTables.ReadNewestServiceStateAsync(connection, ct);
            Assert.NotNull(state);
            Assert.Equal("example-sql-02", state.ServiceHost);
            Assert.Null(state.KeyId);
            Assert.Equal("mismatch", state.State);
            Assert.Equal("a note", state.Note);
            Assert.Equal(new DateTime(2026, 1, 5, 11, 0, 0, DateTimeKind.Utc), state.UpdatedAtUtc);
            Assert.Equal(DateTimeKind.Utc, state.UpdatedAtUtc.Kind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task SeedRowsAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await ExecAsync(owner, "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('aaaaaaaaaaaaaaaa', '\\x01'::bytea, 'RSA-OAEP-SHA256', 'current')", ct);
        await ExecAsync(owner, "INSERT INTO config.password_key_service (service_host, key_id, state, updated_at) VALUES ('example-sql-01', 'aaaaaaaaaaaaaaaa', 'ok', now() AT TIME ZONE 'UTC')", ct);
        await ExecAsync(owner, "INSERT INTO config.legacy_secret_pin (server_id, slot, value_sha256, binding_sha256) VALUES (1, 'server', '\\x01'::bytea, '\\x02'::bytea)", ct);
    }

    private static async Task<NpgsqlConnection> OpenAsAsync(ScratchPostgres scratch, string role, string password, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Username = role,
            Password = password,
            SearchPath = "collect,config,public",
            Pooling = false,
        };
        var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    /// <summary>The SQLSTATE the statement fails with, or null when it succeeds.</summary>
    private static async Task<string?> SqlStateOfAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        try
        {
            await ExecAsync(connection, sql, ct);
            return null;
        }
        catch (PostgresException ex)
        {
            return ex.SqlState;
        }
    }

    private static string OwnerRoleOf(NpgsqlConnection owner) =>
        new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
