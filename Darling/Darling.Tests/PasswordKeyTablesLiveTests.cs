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
/// names, so a cluster that already has them skips these facts (the same rule the security-split facts follow).
/// </summary>
[Collection("live-postgres")]
public sealed class PasswordKeyTablesLiveTests
{
    /// <summary>One statement per table and verb; each statement reaches the row trigger (or the TRUNCATE trigger) of its table.</summary>
    private static readonly Dictionary<string, string[]> Writes = new()
    {
        ["config.password_key"] =
        [
            "INSERT INTO config.password_key (key_id, public_key, algorithm, state, replaced_reason, replaced_at) VALUES ('afb6cecb558a0858', decode(repeat('01', 400), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'replaced', 'reset', now() AT TIME ZONE 'UTC')",
            "UPDATE config.password_key SET created_at = created_at",
            "DELETE FROM config.password_key",
            "TRUNCATE config.password_key",
        ],
        ["config.password_key_service"] =
        [
            "INSERT INTO config.password_key_service (service_host, key_id, state, updated_at) VALUES ('example-02', NULL, 'ok', now() AT TIME ZONE 'UTC')",
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
            "A cluster-wide admin/viewer/mcp role already exists on this cluster; the provisioning batch would adopt it.");

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
            await ExecAsync(owner, "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO admin; GRANT TRUNCATE ON ALL TABLES IN SCHEMA config TO admin;", ct);
            foreach (var statement in Writes.Values.SelectMany(v => v))
            {
                await AssertOwnerOnlyRefusalAsync(admin, statement, ct);
            }

            /* The same for the other two roles holding every privilege. */
            await ExecAsync(owner,
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON config.password_key, config.password_key_service, config.legacy_secret_pin, config.legacy_secret_pin_marker TO viewer, mcp;", ct);
            await using var viewer = await OpenAsAsync(scratch, "viewer", ProvisioningTestSecrets.ViewerPassword, ct);
            await using var mcp = await OpenAsAsync(scratch, "mcp", ProvisioningTestSecrets.McpPassword, ct);
            foreach (var statement in Writes.Values.SelectMany(v => v))
            {
                await AssertOwnerOnlyRefusalAsync(viewer, statement, ct);
                await AssertOwnerOnlyRefusalAsync(mcp, statement, ct);
            }

            /* Nothing the refused writes tried changed a row. */
            Assert.Equal(1L, await ScalarAsync(owner, "SELECT count(*) FROM config.password_key", ct));
            Assert.Equal(1L, await ScalarAsync(owner, "SELECT count(*) FROM config.password_key_service", ct));
            Assert.Equal(1L, await ScalarAsync(owner, "SELECT count(*) FROM config.legacy_secret_pin", ct));
            Assert.Equal("pending", await ScalarAsync(owner, "SELECT state FROM config.legacy_secret_pin_marker", ct));

            /* The owner's own writes pass, whatever the other roles hold. */
            await ExecAsync(owner, "UPDATE config.password_key SET created_at = created_at", ct);
            await ExecAsync(owner, "UPDATE config.legacy_secret_pin_marker SET state = 'done'", ct);
            await ExecAsync(owner, "UPDATE config.legacy_secret_pin_marker SET state = 'pending'", ct);
            await ExecAsync(owner, "INSERT INTO config.password_key (key_id, public_key, algorithm, state, replaced_at, replaced_reason) VALUES ('9e7be4ce0f2388a3', decode(repeat('03', 400), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'replaced', now() AT TIME ZONE 'UTC', 'reset')", ct);
            await ExecAsync(owner, "DELETE FROM config.password_key WHERE key_id = '9e7be4ce0f2388a3'", ct);
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
            "A cluster-wide admin/viewer/mcp role already exists on this cluster; the provisioning batch would adopt it.");

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName)), ct);
            await SeedRowsAsync(owner, ct);

            /* Roles here can create temporary tables. Give it every write privilege so the trigger is the only
               check left, then let it create a temporary table that is shaped like pg_class and names the viewer as the owner of
               every relation. */
            await ExecAsync(owner,
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON config.password_key, config.password_key_service, config.legacy_secret_pin, config.legacy_secret_pin_marker TO viewer;", ct);
            await using var viewer = await OpenAsAsync(scratch, "viewer", ProvisioningTestSecrets.ViewerPassword, ct);
            await ExecAsync(viewer,
                "CREATE TEMP TABLE pg_class (oid oid, relowner oid); " +
                "INSERT INTO pg_temp.pg_class SELECT c.oid, (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = 'viewer') FROM pg_catalog.pg_class AS c;", ct);

            foreach (var statement in Writes.Values.SelectMany(v => v))
            {
                await AssertOwnerOnlyRefusalAsync(viewer, statement, ct);
            }

            /* The control. PostgreSQL searches a session's temporary relations first unless pg_temp is listed last in the search
               path, so an unqualified catalog name under a path without pg_temp last finds the temporary table. The same lookup
               written that way is passed by the same temporary table, which shows the facts above can fail. */
            await ExecAsync(owner, @"
CREATE TABLE public.temp_name_control (n integer);
CREATE FUNCTION public.temp_name_control_guard() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $fn$
BEGIN
    IF NOT pg_has_role(session_user, (SELECT c.relowner FROM pg_class AS c WHERE c.oid = TG_RELID), 'USAGE') THEN
        RAISE EXCEPTION 'refused' USING ERRCODE = 'PW010';
    END IF;
    RETURN NEW;
END;
$fn$;
CREATE TRIGGER temp_name_control_trg BEFORE INSERT ON public.temp_name_control FOR EACH ROW EXECUTE FUNCTION public.temp_name_control_guard();
GRANT INSERT ON public.temp_name_control TO viewer;", ct);
            Assert.Null(await SqlStateOfAsync(viewer, "INSERT INTO public.temp_name_control VALUES (1)", ct));

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

            await ExecAsync(connection, "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('afb6cecb558a0858', decode(repeat('01', 400), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'current')", ct);
            Assert.Equal("23505", await SqlStateOfAsync(connection,
                "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('9e7be4ce0f2388a3', decode(repeat('03', 400), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'current')", ct));
            Assert.Equal("23514", await SqlStateOfAsync(connection,
                "INSERT INTO config.password_key (key_id, public_key, algorithm, state, replaced_reason, replaced_at) VALUES ('NOT-A-KEY-ID', decode(repeat('03', 400), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'replaced', 'reset', now() AT TIME ZONE 'UTC')", ct));

            /* The row checks: the one algorithm name, a public key of a plausible size, and the replaced columns that go with the state. */
            const string Columns = "INSERT INTO config.password_key (key_id, public_key, algorithm, state, replaced_reason, replaced_at) VALUES ";
            Assert.Equal("23514", await SqlStateOfAsync(connection, Columns + "(left(encode(sha256(decode(repeat('05', 400), 'hex')), 'hex'), 16), decode(repeat('05', 400), 'hex'), 'RSA-OAEP-SHA256', 'replaced', NULL, now() AT TIME ZONE 'UTC')", ct));
            Assert.Equal("23514", await SqlStateOfAsync(connection, Columns + "(left(encode(sha256(decode(repeat('05', 255), 'hex')), 'hex'), 16), decode(repeat('05', 255), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'replaced', NULL, now() AT TIME ZONE 'UTC')", ct));
            Assert.Equal("23514", await SqlStateOfAsync(connection, Columns + "(left(encode(sha256(decode(repeat('05', 2049), 'hex')), 'hex'), 16), decode(repeat('05', 2049), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'replaced', NULL, now() AT TIME ZONE 'UTC')", ct));
            Assert.Equal("23514", await SqlStateOfAsync(connection, Columns + "(left(encode(sha256(decode(repeat('05', 400), 'hex')), 'hex'), 16), decode(repeat('05', 400), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'replaced', NULL, NULL)", ct));
            Assert.Equal("23514", await SqlStateOfAsync(connection, Columns + "('cccccccccccccccc', decode(repeat('05', 256), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'replaced', 'reset', now() AT TIME ZONE 'UTC')", ct)); // an id that is not the key's own
            Assert.Equal("23514", await SqlStateOfAsync(connection, Columns + "(left(encode(sha256(decode(repeat('05', 256), 'hex')), 'hex'), 16), decode(repeat('05', 256), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'replaced', NULL, now() AT TIME ZONE 'UTC')", ct)); // replaced with no reason
            Assert.Equal("23514", await SqlStateOfAsync(connection, "UPDATE config.password_key SET replaced_at = now() AT TIME ZONE 'UTC' WHERE state = 'current'", ct));
            Assert.Equal("23514", await SqlStateOfAsync(connection, "UPDATE config.password_key SET replaced_reason = 'reset' WHERE state = 'current'", ct));
            Assert.Null(await SqlStateOfAsync(connection, Columns + "(left(encode(sha256(decode(repeat('05', 256), 'hex')), 'hex'), 16), decode(repeat('05', 256), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'replaced', 'reset', now() AT TIME ZONE 'UTC')", ct));
            Assert.Null(await SqlStateOfAsync(connection, "DELETE FROM config.password_key WHERE key_id = 'd85944090257d11d'", ct));

            /* Replacing the current key frees the slot for the next one. */
            await ExecAsync(connection, "UPDATE config.password_key SET state = 'replaced', replaced_reason = 'rotated', replaced_at = now() AT TIME ZONE 'UTC' WHERE key_id = 'afb6cecb558a0858'", ct);
            await ExecAsync(connection, "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('9e7be4ce0f2388a3', decode(repeat('03', 400), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'current')", ct);

            var key = await PasswordKeyTables.ReadCurrentAsync(connection, ct);
            Assert.NotNull(key);
            Assert.Equal("9e7be4ce0f2388a3", key.KeyId);
            Assert.Equal(Enumerable.Repeat((byte)0x03, 400).ToArray(), key.Spki);
            Assert.Equal("RSA3072-OAEP-SHA256/A256GCM", key.Algorithm);

            await ExecAsync(connection, "INSERT INTO config.password_key_service (service_host, key_id, state, note, updated_at) VALUES ('example-01', 'afb6cecb558a0858', 'loading', NULL, '2026-01-05 10:00:00')", ct);
            await ExecAsync(connection, "INSERT INTO config.password_key_service (service_host, key_id, state, note, updated_at) VALUES ('example-02', NULL, 'mismatch', 'a note', '2026-01-05 11:00:00')", ct);
            Assert.Equal("23514", await SqlStateOfAsync(connection,
                "INSERT INTO config.password_key_service (service_host, state, updated_at) VALUES ('example-03', 'unknown', now() AT TIME ZONE 'UTC')", ct));

            var state = await PasswordKeyTables.ReadNewestServiceStateAsync(connection, ct);
            Assert.NotNull(state);
            Assert.Equal("example-02", state.ServiceHost);
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

    private static readonly string[] KeyTables =
    [
        "config.password_key",
        "config.password_key_service",
        "config.legacy_secret_pin",
        "config.legacy_secret_pin_marker",
    ];

    private static ProvisioningTarget TargetFor(bool managed, NpgsqlConnection owner, ScratchPostgres scratch) =>
        managed ? ProvisioningTarget.Managed : ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName);

    private static string ProvisioningBatch(bool managed, NpgsqlConnection owner, ScratchPostgres scratch) =>
        DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
            15, PasswordReassert.All, TargetFor(managed, owner, scratch));

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Provisioning_LeavesNoReadOfThePinTables_AndNoTriggerOrReferencesGrant_AfterAGrantAllRerun(bool managed)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key table live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        Assert.SkipWhen(
            await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) is true,
            "A cluster-wide admin/viewer/mcp role already exists on this cluster; the provisioning batch would adopt it.");
        Assert.SkipWhen(
            managed && (OwnerRoleOf(owner) != DarlingManagedPostgres.UserName
                        || await ScalarAsync(owner, "SELECT NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'darling')", ct) is true),
            "The managed shape names the owner and database 'darling', which this connection does not have.");

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, ProvisioningBatch(managed, owner, scratch), ct);
            await SeedRowsAsync(owner, ct);

            /* The pin tables are read by the store owner only; the key and service-state tables are readable. */
            foreach (var (role, password) in new[]
                     {
                         ("admin", ProvisioningTestSecrets.AdminPassword),
                         ("viewer", ProvisioningTestSecrets.ViewerPassword),
                         ("mcp", ProvisioningTestSecrets.McpPassword),
                     })
            {
                await using var connection = await OpenAsAsync(scratch, role, password, ct);
                Assert.Equal("42501", await SqlStateOfAsync(connection, "SELECT count(*) FROM config.legacy_secret_pin", ct));
                Assert.Equal("42501", await SqlStateOfAsync(connection, "SELECT count(*) FROM config.legacy_secret_pin_marker", ct));

                /* The key and service-state tables are readable by admin only (the desktop Viewer reads them as admin). */
                var expected = role == "admin" ? null : "42501";
                Assert.Equal(expected, await SqlStateOfAsync(connection, "SELECT count(*) FROM config.password_key", ct));
                Assert.Equal(expected, await SqlStateOfAsync(connection, "SELECT count(*) FROM config.password_key_service", ct));
            }

            /* Row security with no policy: even a role handed SELECT on a pin table explicitly counts no rows. */
            await ExecAsync(owner, "GRANT SELECT ON config.legacy_secret_pin, config.legacy_secret_pin_marker TO viewer", ct);
            Assert.True(Convert.ToInt64(await ScalarAsync(owner, "SELECT count(*) FROM config.legacy_secret_pin", ct)) > 0, "the seed put a pin row in");
            Assert.Equal(1L, await ScalarAsync(owner, "SELECT count(*) FROM config.legacy_secret_pin_marker", ct));
            await using (var viewerConnection = await OpenAsAsync(scratch, "viewer", ProvisioningTestSecrets.ViewerPassword, ct))
            {
                Assert.Equal(0L, await ScalarAsync(viewerConnection, "SELECT count(*) FROM config.legacy_secret_pin", ct));
                Assert.Equal(0L, await ScalarAsync(viewerConnection, "SELECT count(*) FROM config.legacy_secret_pin_marker", ct));
            }

            /* A blanket grant to the three roles, as a later grant-all would make. Control: before the batch runs again, admin can
               add its own trigger to every table, so what happens to it below is the batch's doing. The triggers are LEFT in place. */
            await ExecAsync(owner, "GRANT ALL ON ALL TABLES IN SCHEMA config TO admin, viewer, mcp;", ct);
            await using var admin = await OpenAsAsync(scratch, "admin", ProvisioningTestSecrets.AdminPassword, ct);
            for (var i = 0; i < KeyTables.Length; i++)
            {
                Assert.Null(await SqlStateOfAsync(admin, CreateTriggerSql(KeyTables[i], i), ct));
            }

            /* A foreign key onto a key table, made while the owner held the privilege. */
            await ExecAsync(owner, "CREATE TABLE config.temp_name_control_fk (k text REFERENCES config.password_key (key_id))", ct);

            var notices = new List<string>();
            void OnNotice(object? sender, NpgsqlNoticeEventArgs e) => notices.Add(e.Notice.MessageText);
            owner.Notice += OnNotice;
            await ExecAsync(owner, ProvisioningBatch(managed, owner, scratch), ct);
            owner.Notice -= OnNotice;

            Assert.Equal(KeyTables.Length, notices.Count(n => n.Contains("temp_name_control_trg_", StringComparison.Ordinal)));
            Assert.Contains(notices, n => n.Contains("temp_name_control_fk", StringComparison.Ordinal));
            Assert.Equal(0L, await ScalarAsync(owner, "SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgname LIKE 'temp_name_control_trg_%'", ct));
            Assert.Equal(0L, await ScalarAsync(owner, "SELECT count(*) FROM pg_catalog.pg_constraint WHERE contype = 'f' AND conrelid = 'config.temp_name_control_fk'::regclass", ct));
            Assert.Equal(8L, await ScalarAsync(owner,
                "SELECT count(*) FROM pg_catalog.pg_trigger WHERE NOT tgisinternal AND tgrelid = ANY (ARRAY['config.password_key','config.password_key_service','config.legacy_secret_pin','config.legacy_secret_pin_marker']::regclass[])", ct));
            await ExecAsync(owner, "DROP TABLE config.temp_name_control_fk", ct);

            for (var i = 0; i < KeyTables.Length; i++)
            {
                /* No TRIGGER privilege any more: refused for the missing privilege, not by the owner-only trigger. */
                Assert.Equal("42501", await SqlStateOfAsync(admin, CreateTriggerSql(KeyTables[i], i), ct));
                foreach (var role in new[] { "admin", "viewer", "mcp" })
                {
                    Assert.Equal(false, await ScalarAsync(owner, $"SELECT has_table_privilege('{role}', '{KeyTables[i]}', 'INSERT') OR has_table_privilege('{role}', '{KeyTables[i]}', 'UPDATE') OR has_table_privilege('{role}', '{KeyTables[i]}', 'DELETE') OR has_table_privilege('{role}', '{KeyTables[i]}', 'TRUNCATE') OR has_table_privilege('{role}', '{KeyTables[i]}', 'TRIGGER') OR has_table_privilege('{role}', '{KeyTables[i]}', 'REFERENCES')", ct));
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    "DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    private static string CreateTriggerSql(string table, int index) =>
        $"CREATE TRIGGER temp_name_control_trg_{index} BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()";

    [Fact]
    public async Task AWriteUnderReplicaReplicationRole_IsStillRefused_ForANonOwner_AndANonOwnerNeedsAGrantToSetIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key table live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        Assert.SkipWhen(
            await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) is true,
            "A cluster-wide admin/viewer/mcp role already exists on this cluster; the provisioning batch would adopt it.");

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, ProvisioningBatch(false, owner, scratch), ct);
            await SeedRowsAsync(owner, ct);
            await using var viewer = await OpenAsAsync(scratch, "viewer", ProvisioningTestSecrets.ViewerPassword, ct);

            /* Without a grant the role cannot set it at all. */
            Assert.Equal("42501", await SqlStateOfAsync(viewer, "SET session_replication_role = replica", ct));

            /* With the setting granted and every write privilege, the triggers still fire: they are enabled always. */
            await ExecAsync(owner, "GRANT SET ON PARAMETER session_replication_role TO viewer;", ct);
            await ExecAsync(owner,
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON config.password_key, config.password_key_service, config.legacy_secret_pin, config.legacy_secret_pin_marker TO viewer;", ct);
            await ExecAsync(viewer, "SET session_replication_role = replica", ct);
            Assert.Equal("replica", await ScalarAsync(viewer, "SHOW session_replication_role", ct));
            foreach (var statement in Writes.Values.SelectMany(v => v))
            {
                await AssertOwnerOnlyRefusalAsync(viewer, statement, ct);
            }

            Assert.Equal(1L, await ScalarAsync(owner, "SELECT count(*) FROM config.password_key", ct));
            Assert.Equal("pending", await ScalarAsync(owner, "SELECT state FROM config.legacy_secret_pin_marker", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    "REVOKE SET ON PARAMETER session_replication_role FROM viewer; DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    /// <summary>
    /// The statement fails with the owner-only state. The one exception: an UPDATE or DELETE on a pin table, where row security
    /// (on, with no policy) leaves a non-owner no row to change, so the statement succeeds and changes nothing; the callers
    /// check the rows afterwards.
    /// </summary>
    private static async Task AssertOwnerOnlyRefusalAsync(NpgsqlConnection connection, string statement, CancellationToken ct)
    {
        var state = await SqlStateOfAsync(connection, statement, ct);
        var seesNoRows = statement.StartsWith("UPDATE config.legacy_secret_pin", StringComparison.Ordinal)
                         || statement.StartsWith("DELETE FROM config.legacy_secret_pin", StringComparison.Ordinal);
        if (seesNoRows && state is null)
        {
            return;
        }

        Assert.Equal(PasswordKeyTables.OwnerOnlySqlState, state);
    }

    private static async Task SeedRowsAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await ExecAsync(owner, "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ('afb6cecb558a0858', decode(repeat('01', 400), 'hex'), 'RSA3072-OAEP-SHA256/A256GCM', 'current')", ct);
        await ExecAsync(owner, "INSERT INTO config.password_key_service (service_host, key_id, state, updated_at) VALUES ('example-01', 'afb6cecb558a0858', 'ok', now() AT TIME ZONE 'UTC')", ct);
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
