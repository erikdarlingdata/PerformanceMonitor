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
/// The live round-trips for the V8 security split (#1262), gated on DARLING_TEST_PG (a dev
/// Postgres + TimescaleDB connection string; the connecting role must be able to CREATE ROLE and
/// own the store). Proves what the ungated shape tests cannot: after migration the tables really
/// live in collect/config and resolve by their bare names; a least-privilege role can read collect
/// but is DENIED a config write (42501); a table created into collect AFTER the grants auto-inherits
/// SELECT (the ALTER DEFAULT PRIVILEGES proof); and — the design's flagged validation item — an
/// already-COMPRESSED TimescaleDB hypertable survives ALTER TABLE ... SET SCHEMA and stays readable
/// by the least-privilege role.
///
/// <para>Uses distinct <c>sec_admin_test</c>/<c>sec_viewer_test</c> roles (not the real
/// admin/viewer) so a shared dev store running an actual service is never clobbered, and grants on
/// the schemas only (no REVOKE on a named database), so the test does not depend on the store's
/// database name. Every object it creates is cleaned up.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingSecuritySplitLiveTests
{
    private const string AdminRole = "sec_admin_test";
    private const string ViewerRole = "sec_viewer_test";
    private const string McpRole = "sec_mcp_test";
    private const string RolePassword = "SecSplitTestPw0123456789abcdef01"; // alnum, like the real generator

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the security-split live tests.");
        return connectionString!;
    }

    [Fact]
    public async Task V8_MovesTablesToCollectAndConfig_AndBareNamesResolve()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        /* MigrateAsync applies V8 (idempotent) and best-effort sets the database-default search_path. */
        await PgMigrations.MigrateAsync(connection, ct);

        /* A collector table and a metadata table live in collect; a coordination table in config. */
        Assert.Equal("collect", await SchemaOfAsync(connection, "wait_stats", ct));
        Assert.Equal("collect", await SchemaOfAsync(connection, "analysis_findings", ct));
        Assert.Equal("config", await SchemaOfAsync(connection, "config_mute_rules", ct));

        /* The bare, unqualified name every SQL site uses resolves — proof the search_path works, so
           no query file had to be re-qualified. A fresh connection inherits the database default.
           Pooling=false: under parallel xunit, the pool can hand back a physical connection opened
           BEFORE this run's ALTER DATABASE ... SET search_path landed (its session default predates
           it), which fails the bare SELECT with 42P01 — a pool artifact, not what this asserts. A
           genuinely new connection is the thing the comment above claims to prove. */
        var freshConnectionString = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString;
        await using var fresh = new NpgsqlConnection(freshConnectionString);
        await fresh.OpenAsync(ct);
        using var bare = new NpgsqlCommand("SELECT count(*) FROM config_mute_rules", fresh);
        Assert.NotNull(await bare.ExecuteScalarAsync(ct));
    }

    [Fact]
    public async Task V17_ControlPlaneTables_LandInConfigSchema_NotCollect()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* The Stage 1 control-plane tables must be CREATEd directly into config (the admin-writable
           schema), NOT collect — V17 is the first migration to create in config, and an unqualified
           name would resolve to collect (first in the migrate session's search_path). This is the live
           proof of the schema-qualify gotcha the migration guards against. */
        foreach (var table in new[]
        {
            "config_monitored_servers", "config_alert_settings", "config_notification",
            "config_collector_schedules", "config_service", "config_command",
        })
        {
            Assert.Equal("config", await SchemaOfAsync(connection, table, ct));
        }
    }

    [Fact]
    public async Task Roles_AdminWritesConfig_ViewerDenied_AndNewCollectTableAutoGrants()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);

        await CreateTestRolesAndGrantsAsync(owner, ct);
        var bodySucceeded = false;
        try
        {
            var adminString = RoleConnectionString(connectionString, AdminRole);
            var viewerString = RoleConnectionString(connectionString, ViewerRole);

            /* admin reads collect and writes a config table. */
            await using (var admin = new NpgsqlConnection(adminString))
            {
                await admin.OpenAsync(ct);
                await ExecAsync(admin, "SELECT count(*) FROM wait_stats", ct);
                await ExecAsync(admin,
                    "INSERT INTO config_mute_rules (id, enabled, created_at_utc) VALUES ('sec-split-admin', true, now())", ct);
                await ExecAsync(admin, "DELETE FROM config_mute_rules WHERE id = 'sec-split-admin'", ct);
            }

            /* viewer reads collect but is DENIED the same config write — 42501. */
            await using (var viewer = new NpgsqlConnection(viewerString))
            {
                await viewer.OpenAsync(ct);
                await ExecAsync(viewer, "SELECT count(*) FROM wait_stats", ct);

                var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
                    await ExecAsync(viewer,
                        "INSERT INTO config_mute_rules (id, enabled, created_at_utc) VALUES ('sec-split-viewer', true, now())", ct));
                Assert.Equal("42501", denied.SqlState); // insufficient_privilege
            }

            /* ALTER DEFAULT PRIVILEGES proof: a table created into collect AFTER the grants is
               readable by both roles with no explicit per-table grant. */
            await ExecAsync(owner, "CREATE TABLE IF NOT EXISTS collect.sec_split_newtable (id integer)", ct);

            await using (var viewer = new NpgsqlConnection(viewerString))
            {
                await viewer.OpenAsync(ct);
                await ExecAsync(viewer, "SELECT count(*) FROM collect.sec_split_newtable", ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            /* The table's own inner try/finally is gone (#1896) rather than converted: the outer finally runs
               whether or not the block above threw, so dropping it here is the same guarantee with one less
               nesting level and one less cleanup connection — and it now goes through the verified removal
               path instead of a bare DROP that could report success on a closed session. */
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await new LiveCleanupBatch(cleanup).DropTableAsync("sec_split_newtable", cleanupCt);
                await DropTestRolesAsync(cleanup, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task ViewerRole_DeniedSecretColumns_AllowedNonSecretColumns_OnAllThreeTables()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        /* Applies V17 (creates the three credential-bearing config tables) — the carve targets them. */
        await PgMigrations.MigrateAsync(owner, ct);

        await CreateTestRolesAndGrantsAsync(owner, ct);
        var bodySucceeded = false;
        try
        {
            await using var viewer = new NpgsqlConnection(RoleConnectionString(connectionString, ViewerRole));
            await viewer.OpenAsync(ct);

            foreach (var acl in DarlingManagedRoles.ViewerRestrictedConfigTables)
            {
                /* Drift guard (the safety net): the non-secret + secret sets must PARTITION the table's real
                   columns. A migration that adds a column without classifying it here fails HERE — the new
                   column is in neither set — forcing a deliberate secret/non-secret decision before it can
                   ship, which is exactly what a future-secret-column ACL gap would need. */
                var actual = await ColumnsOfConfigTableAsync(owner, acl.Table, ct);
                var classified = acl.NonSecretColumns.Concat(acl.SecretColumns).ToHashSet(StringComparer.Ordinal);
                Assert.True(actual.SetEquals(classified),
                    $"config.{acl.Table}: unclassified column(s) [{string.Join(",", actual.Except(classified))}]; " +
                    $"listed-but-absent [{string.Join(",", classified.Except(actual))}] — update DarlingManagedRoles.ViewerRestrictedConfigTables.");

                /* viewer CAN read every non-secret column together (column privilege is enforced at parse
                   time, so LIMIT 1 exercises it even against an empty table). */
                await ExecAsync(viewer, $"SELECT {string.Join(", ", acl.NonSecretColumns)} FROM config.{acl.Table} LIMIT 1", ct);

                /* viewer is DENIED each secret column — 42501 insufficient_privilege — proving the fail-closed
                   carve actually took (the point of the whole deliverable). */
                foreach (var secret in acl.SecretColumns)
                {
                    var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
                        await ExecAsync(viewer, $"SELECT {secret} FROM config.{acl.Table} LIMIT 1", ct));
                    Assert.Equal("42501", denied.SqlState);
                }

                /* SELECT * is denied too (it touches the secret columns), so a naive read can't leak them. */
                var star = await Assert.ThrowsAsync<PostgresException>(async () =>
                    await ExecAsync(viewer, $"SELECT * FROM config.{acl.Table} LIMIT 1", ct));
                Assert.Equal("42501", star.SqlState);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropTestRolesAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task ViewerRole_CanCrudCustomViews_ButStillDeniedOtherConfigWrites()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        /* Applies V31 (creates config.custom_views) + V17 (config_mute_rules — the "other config table"). */
        await PgMigrations.MigrateAsync(owner, ct);

        await CreateTestRolesAndGrantsAsync(owner, ct);
        /* Own-scoped: a GUID-suffixed hex name (safe to interpolate) so the shared store is never clobbered. */
        var viewName = "cv_sec_" + Guid.NewGuid().ToString("N");
        var bodySucceeded = false;
        try
        {
            await using var viewer = new NpgsqlConnection(RoleConnectionString(connectionString, ViewerRole));
            await viewer.OpenAsync(ct);

            /* #1563: the single viewer write — full CRUD on config.custom_views (IDENTITY id needs no sequence
               grant; definition is jsonb). Each statement proves the INSERT/UPDATE/DELETE grant actually took. */
            await ExecAsync(viewer,
                $"INSERT INTO config.custom_views (name, definition) VALUES ('{viewName}', '{{\"panels\":[]}}'::jsonb)", ct);
            await ExecAsync(viewer,
                $"UPDATE config.custom_views SET description = 'edited', version = version + 1 WHERE name = '{viewName}'", ct);
            await ExecAsync(viewer,
                $"DELETE FROM config.custom_views WHERE name = '{viewName}'", ct);

            /* But viewer is STILL denied a write to any OTHER config table — 42501 insufficient_privilege — so
               the custom_views grant did not accidentally widen into a schema-wide config write. */
            var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
                await ExecAsync(viewer,
                    "INSERT INTO config_mute_rules (id, enabled, created_at_utc) VALUES ('cv-sec-viewer', true, now())", ct));
            Assert.Equal("42501", denied.SqlState);

            bodySucceeded = true;
        }
        finally
        {
            /* Belt-and-suspenders cleanup (the DELETE above already removed it on the happy path). */
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await ExecAsync(cleanup, $"DELETE FROM config.custom_views WHERE name = '{viewName}'", cleanupCt);
                await DropTestRolesAsync(cleanup, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task McpRole_DeniedSecretColumns_GrantedAnalysisInserts_CustomViews_AndAlertTuningWrites()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        /* Applies V17 (the credential-bearing config tables + analysis_muted; the carve targets them) and V31
           (config.custom_views, the #1599 mcp write target). */
        await PgMigrations.MigrateAsync(owner, ct);

        await CreateTestRolesAndGrantsAsync(owner, ct);
        /* Own-scoped GUID name so the mcp custom_views round-trip below never clobbers the shared store. */
        var viewName = "cv_sec_mcp_" + Guid.NewGuid().ToString("N");
        var bodySucceeded = false;
        try
        {
            await using var mcp = new NpgsqlConnection(RoleConnectionString(connectionString, McpRole));
            await mcp.OpenAsync(ct);

            /* Same read surface as viewer: every non-secret column readable, every secret column DENIED
               (42501). Guards against a future refactor dropping BuildViewerColumnAclSql(config, "mcp") and
               silently exposing the credential columns over the network MCP surface (Round 4 #5). */
            foreach (var acl in DarlingManagedRoles.ViewerRestrictedConfigTables)
            {
                await ExecAsync(mcp, $"SELECT {string.Join(", ", acl.NonSecretColumns)} FROM config.{acl.Table} LIMIT 1", ct);
                foreach (var secret in acl.SecretColumns)
                {
                    var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
                        await ExecAsync(mcp, $"SELECT {secret} FROM config.{acl.Table} LIMIT 1", ct));
                    Assert.Equal("42501", denied.SqlState);
                }
            }

            /* The two narrow analysis INSERTs are granted (has_table_privilege avoids needing the tables'
               column shapes) — and NOTHING wider on them: no config_command write (the service-credential
               pivot), no analysis_muted DELETE (no MCP unmute tool), no analysis_findings UPDATE. */
            Assert.True(await HasPrivAsync(mcp, "collect.analysis_findings", "INSERT", ct));
            Assert.True(await HasPrivAsync(mcp, "config.analysis_muted", "INSERT", ct));
            Assert.False(await HasPrivAsync(mcp, "config.config_command", "INSERT", ct));
            Assert.False(await HasPrivAsync(mcp, "config.analysis_muted", "DELETE", ct));
            Assert.False(await HasPrivAsync(mcp, "collect.analysis_findings", "UPDATE", ct));

            /* #1599: mcp gets the SAME single-table custom_views write viewer has (the MCP custom-view tools) —
               proven by a real INSERT/UPDATE/DELETE round-trip as the mcp role (IDENTITY id needs no sequence
               grant; definition is jsonb). */
            await ExecAsync(mcp,
                $"INSERT INTO config.custom_views (name, definition) VALUES ('{viewName}', '{{\"panels\":[]}}'::jsonb)", ct);
            await ExecAsync(mcp,
                $"UPDATE config.custom_views SET description = 'edited', version = version + 1 WHERE name = '{viewName}'", ct);
            await ExecAsync(mcp,
                $"DELETE FROM config.custom_views WHERE name = '{viewName}'", ct);

            /* The MCP alert-tuning writes: full CRUD on the mute rules + UPDATE on the SINGLETON alert-settings
               row (never INSERT/DELETE on it), plus the two beacon columns of config_service. */
            Assert.True(await HasPrivAsync(mcp, "config.config_mute_rules", "INSERT", ct));
            Assert.True(await HasPrivAsync(mcp, "config.config_mute_rules", "UPDATE", ct));
            Assert.True(await HasPrivAsync(mcp, "config.config_mute_rules", "DELETE", ct));
            Assert.True(await HasPrivAsync(mcp, "config.config_alert_settings", "UPDATE", ct));
            Assert.False(await HasPrivAsync(mcp, "config.config_alert_settings", "INSERT", ct));
            Assert.False(await HasPrivAsync(mcp, "config.config_alert_settings", "DELETE", ct));

            /* Real round-trip as the mcp role: create + delete a mute rule (own-scoped id). */
            await ExecAsync(mcp, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc) VALUES ('sec-mcp-mute', true, now())", ct);
            await ExecAsync(mcp, "DELETE FROM config.config_mute_rules WHERE id = 'sec-mcp-mute'", ct);

            /* The mcp server-onboarding writes (add_servers / remove_server): full CRUD on config_monitored_servers,
               proven by a real INSERT/UPDATE/DELETE round-trip as the mcp role (own-scoped sentinel server_id). The
               INSERT fires trg_bump_monitored_servers -> config_bump_version AS mcp, so it doubles as proof the
               section-8 config_service beacon column-grant covers THIS write too (no extra config_service grant). */
            Assert.True(await HasPrivAsync(mcp, "config.config_monitored_servers", "INSERT", ct));
            Assert.True(await HasPrivAsync(mcp, "config.config_monitored_servers", "UPDATE", ct));
            Assert.True(await HasPrivAsync(mcp, "config.config_monitored_servers", "DELETE", ct));
            await ExecAsync(mcp, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (-424242, 'sec-mcp-onboard', 'sec-mcp-onboard-host')", ct);
            await ExecAsync(mcp, "UPDATE config.config_monitored_servers SET is_enabled = FALSE WHERE server_id = -424242", ct);
            await ExecAsync(mcp, "DELETE FROM config.config_monitored_servers WHERE server_id = -424242", ct);

            /* The load-bearing beacon proof: an UPDATE on config_alert_settings as the mcp role fires the
               statement-level bump trigger, which UPDATEs config_service.config_version AS the mcp role (the
               trigger function is SECURITY INVOKER). Without the column-level config_service beacon grant this
               would throw 42501 in production — and the superuser-run gated-live suites would never catch it.
               Seed both singletons first (owner, no-op if present), then a value-preserving UPDATE as mcp. */
            await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(owner, "INSERT INTO config.config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(mcp, "UPDATE config.config_alert_settings SET enabled = enabled WHERE id = 1", ct);

            /* #3314: the delivery cooldown, on config_notification rather than config_alert_settings, so
               update_alert_settings spans two tables. This is the one live check that separates the two ways
               the write can 42501 — a missing column SELECT and a missing column UPDATE raise the IDENTICAL
               "permission denied for table config_notification", so a single passing write attempt cannot
               attribute itself. The read half is asserted by the secret-column loop above (which SELECTs the
               whole non-secret set, email_cooldown_minutes included); this is the write half, and the
               sibling-column denials below are what prove the grant is really one column wide rather than
               the table-wide write it would be easiest to reach for. */
            await ExecAsync(owner, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(mcp, "UPDATE config.config_notification SET email_cooldown_minutes = email_cooldown_minutes WHERE id = 1", ct);

            foreach (var sibling in new[] { "smtp_encrypted_password = 'x'", "slack_url = 'x'", "smtp_host = 'x'" })
            {
                var siblingDenied = await Assert.ThrowsAsync<PostgresException>(async () =>
                    await ExecAsync(mcp, $"UPDATE config.config_notification SET {sibling} WHERE id = 1", ct));
                Assert.Equal("42501", siblingDenied.SqlState);
            }

            /* But mcp is STILL denied a write to a config table it was NOT granted (42501) — config_command is
               the service-credential pivot; the alert-tuning grants did not widen into a schema-wide config write. */
            var stillDenied = await Assert.ThrowsAsync<PostgresException>(async () =>
                await ExecAsync(mcp, "INSERT INTO config.config_command (command_type) VALUES ('sec-mcp-noop')", ct));
            Assert.Equal("42501", stillDenied.SqlState);

            /* And the config_service beacon grant is strictly COLUMN-level: mcp can bump config_version/updated_at
               (proven above) but CANNOT flip a real service flag like paused. */
            var beaconOnly = await Assert.ThrowsAsync<PostgresException>(async () =>
                await ExecAsync(mcp, "UPDATE config.config_service SET paused = NOT paused WHERE id = 1", ct));
            Assert.Equal("42501", beaconOnly.SqlState);

            bodySucceeded = true;
        }
        finally
        {
            /* Belt-and-suspenders cleanup (the DELETEs above already removed these on the happy path). */
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await ExecAsync(cleanup, $"DELETE FROM config.custom_views WHERE name = '{viewName}'", cleanupCt);
                await ExecAsync(cleanup, "DELETE FROM config.config_mute_rules WHERE id = 'sec-mcp-mute'", cleanupCt);
                await ExecAsync(cleanup, "DELETE FROM config.config_monitored_servers WHERE server_id = -424242", cleanupCt);
                await DropTestRolesAsync(cleanup, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task CompressedHypertable_SetSchema_StaysReadableByLeastPrivilegeRole()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);

        Assert.SkipUnless(await TimescaleSupport.DetectAsync(owner, ct),
            "TimescaleDB is not installed in the DARLING_TEST_PG store — the compressed-hypertable move validation needs it.");

        await CreateTestRolesAndGrantsAsync(owner, ct);
        const string table = "sec_split_compress";
        var bodySucceeded = false;
        try
        {
            /* Build a compressed hypertable in public, then move it to collect and confirm the least-
               privilege role still reads it THROUGH compression (the historically-missing propagation,
               fixed upstream — validated here on the pinned TimescaleDB). */
            await ExecAsync(owner, $"DROP TABLE IF EXISTS public.{table}, collect.{table}", ct);
            await ExecAsync(owner, $"CREATE TABLE public.{table} (server_id integer NOT NULL, collection_time timestamp NOT NULL, val integer)", ct);
            await ExecAsync(owner, $"SELECT create_hypertable('public.{table}', by_range('collection_time'))", ct);
            await ExecAsync(owner,
                $"INSERT INTO public.{table} SELECT 1, now() - (n || ' days')::interval, n FROM generate_series(1, 40) n", ct);
            await ExecAsync(owner, $"ALTER TABLE public.{table} SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

            /* Force at least one chunk to compress before the move (older_than covers the old rows). */
            await ExecAsync(owner,
                $"SELECT compress_chunk(c) FROM show_chunks('public.{table}', older_than => INTERVAL '7 days') c", ct);

            /* Grant the least-privilege role SELECT on the hypertable parent (propagates to chunks incl.
               the compressed half), then perform the schema move on the compressed hypertable. */
            await ExecAsync(owner, $"GRANT SELECT ON public.{table} TO {ViewerRole}", ct);
            await ExecAsync(owner, $"ALTER TABLE public.{table} SET SCHEMA collect", ct);

            Assert.Equal("collect", await SchemaOfAsync(owner, table, ct));

            /* The move preserved the rows, and the least-privilege role reads them through compression. */
            await using var viewer = new NpgsqlConnection(RoleConnectionString(connectionString, ViewerRole));
            await viewer.OpenAsync(ct);
            using var count = new NpgsqlCommand($"SELECT count(*) FROM collect.{table}", viewer);
            Assert.Equal(40L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));

            bodySucceeded = true;
        }
        finally
        {
            /* The table is created in public and MOVED to collect, so it can be standing in either schema
               depending on where the body got to — both are dropped, and both are verified (#1873). */
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.DropTableAsync(table, cleanupCt);
                await batch.DropTableAsync(table, cleanupCt, schema: "public");
                await DropTestRolesAsync(cleanup, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task RoleMarkerGuard_StampsFreshRole_IdempotentWhenMarked_RaisesOnUnmarkedCollision()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);

        /* A throwaway role name (NOT admin/viewer) so this exercises the guard MECHANISM with zero
           side effects on the shared store — the DarlingManagedRoles.BuildProvisioningSql string that
           applies this same pattern to admin/viewer is pinned separately by the ungated shape test. */
        const string role = "darling_marker_probe";
        string Guard() => $@"
DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
      CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD 'MarkerProbePw01';
      COMMENT ON ROLE {role} IS '{DarlingManagedRoles.RoleMarker}';
   ELSIF shobj_description((SELECT oid FROM pg_roles WHERE rolname = '{role}'), 'pg_authid') IS DISTINCT FROM '{DarlingManagedRoles.RoleMarker}' THEN
      RAISE EXCEPTION 'Role ""{role}"" already exists and was not created by Darling (missing the marker).';
   END IF;
END $$;";

        await ExecAsync(owner, $"DROP ROLE IF EXISTS {role}", ct);
        var bodySucceeded = false;
        try
        {
            /* Fresh: creates + stamps the marker (round-trips through shobj_description). */
            await ExecAsync(owner, Guard(), ct);
            Assert.Equal(DarlingManagedRoles.RoleMarker,
                await ScalarAsync(owner, $"SELECT shobj_description((SELECT oid FROM pg_roles WHERE rolname = '{role}'), 'pg_authid')", ct) as string);

            /* A marked role re-runs idempotently — no throw (password rotation stays possible). */
            await ExecAsync(owner, Guard(), ct);

            /* Simulate a foreign same-named role: strip the marker, re-run -> fail loud, not repurpose. */
            await ExecAsync(owner, $"COMMENT ON ROLE {role} IS NULL", ct);
            var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(owner, Guard(), ct));
            Assert.Equal("P0001", ex.SqlState); // raise_exception
            Assert.Contains("was not created by Darling", ex.MessageText, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).DropRolesAsync(
                    $"DROP ROLE IF EXISTS {role}", [role], cleanupCt));
        }
    }

    private static async Task CreateTestRolesAndGrantsAsync(NpgsqlConnection owner, System.Threading.CancellationToken ct)
    {
        /* Mirrors the DarlingManagedRoles grant model with distinct, disposable role names and no
           database-level REVOKE (so the test is independent of the store's database name). */
        var ddl = $@"
DO $do$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{AdminRole}') THEN
      CREATE ROLE {AdminRole} LOGIN NOSUPERUSER PASSWORD '{RolePassword}';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ViewerRole}') THEN
      CREATE ROLE {ViewerRole} LOGIN NOSUPERUSER PASSWORD '{RolePassword}';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{McpRole}') THEN
      CREATE ROLE {McpRole} LOGIN NOSUPERUSER PASSWORD '{RolePassword}';
   END IF;
END $do$;
GRANT USAGE ON SCHEMA collect, config TO {AdminRole}, {ViewerRole};
GRANT SELECT ON ALL TABLES IN SCHEMA collect TO {AdminRole}, {ViewerRole};
GRANT SELECT ON ALL TABLES IN SCHEMA config  TO {AdminRole}, {ViewerRole};
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO {AdminRole};
{DarlingManagedRoles.BuildViewerColumnAclSql("config", ViewerRole)}
-- The single viewer write (#1563): mirrors DarlingManagedRoles section 7. config.custom_views exists because
-- MigrateAsync (V31) ran before this helper.
GRANT INSERT, UPDATE, DELETE ON config.custom_views TO {ViewerRole};
ALTER DEFAULT PRIVILEGES FOR ROLE {OwnerRoleOf(owner)} IN SCHEMA collect GRANT SELECT ON TABLES TO {AdminRole}, {ViewerRole};

-- The mcp-role analog (darling-network-endpoints, D3-role): viewer's read surface + the same secret-column
-- carve + exactly the two narrow analysis INSERTs, mirroring DarlingManagedRoles.BuildProvisioningSql's
-- section 6. Distinct disposable role so the shared store is never touched.
GRANT USAGE ON SCHEMA collect, config TO {McpRole};
GRANT SELECT ON ALL TABLES IN SCHEMA collect TO {McpRole};
GRANT SELECT ON ALL TABLES IN SCHEMA config  TO {McpRole};
{DarlingManagedRoles.BuildViewerColumnAclSql("config", McpRole)}
GRANT INSERT ON collect.analysis_findings TO {McpRole};
GRANT INSERT ON config.analysis_muted TO {McpRole};
-- The mcp custom_views write (#1599): the MCP custom-view tools CRUD config.custom_views, so mcp gets the SAME
-- single-table write viewer has (DarlingManagedRoles section 7). Never widens to a schema-wide config write.
GRANT INSERT, UPDATE, DELETE ON config.custom_views TO {McpRole};
-- The mcp alert-tuning writes: CRUD on config_mute_rules + UPDATE on the singleton config_alert_settings + the
-- two config_service beacon columns (so the settings write's SECURITY-INVOKER bump trigger can UPDATE
-- config_service.config_version AS the mcp role). Mirrors DarlingManagedRoles section 8.
GRANT INSERT, UPDATE, DELETE ON config.config_mute_rules TO {McpRole};
GRANT UPDATE ON config.config_alert_settings TO {McpRole};
GRANT UPDATE (config_version, updated_at) ON config.config_service TO {McpRole};
-- #3314: the DELIVERY cooldown, the one alert knob stored on config_notification. COLUMN-level, because that
-- table holds the SMTP password blob and the Teams/Slack/generic webhook URLs and the PagerDuty routing key.
-- Mirrors DarlingManagedRoles section 8.
GRANT UPDATE (email_cooldown_minutes) ON config.config_notification TO {McpRole};
-- The mcp server-onboarding writes (add_servers / remove_server): CRUD on the single config_monitored_servers
-- table. Mirrors DarlingManagedRoles section 9. The beacon is already covered by the config_service column grant
-- above (a config_monitored_servers write fires the same SECURITY-INVOKER bump trigger).
GRANT INSERT, UPDATE, DELETE ON config.config_monitored_servers TO {McpRole};";
        await ExecAsync(owner, ddl, ct);
    }

    /// <summary>ALTER DEFAULT PRIVILEGES keys on the role that CREATEs the object — here the connected owner.</summary>
    private static string OwnerRoleOf(NpgsqlConnection owner)
        => new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";

    /// <summary>
    /// Drops the three disposable test roles, and CONFIRMS they are gone (#1873).
    ///
    /// <para>DROP OWNED BY revokes EVERY privilege granted to these roles — table, the column-level secret
    /// carve, schema-usage, and the owner's default privileges naming them — so the following DROP ROLE has no
    /// dependent grants to trip on (a plain table-level REVOKE would leave the column grants and block the
    /// drop).</para>
    ///
    /// <para>The swallow this replaces was load-bearing for a reason worth keeping: <c>DROP OWNED BY</c> has
    /// no <c>IF EXISTS</c> form, so on the pre-test call — before any of the three roles has been created — it
    /// throws <c>42704</c> every single time, and that is not a fault. Verifying the POSTCONDITION instead of
    /// classifying the error keeps that case free: the roles are absent, which is the whole objective, so the
    /// removal succeeded. What no longer passes silently is the case that matters — roles that survive. They
    /// are CLUSTER-wide, so a leak outlives even a DROP DATABASE and greets the next run on the same
    /// cluster.</para>
    /// </summary>
    private static async Task DropTestRolesAsync(NpgsqlConnection owner, System.Threading.CancellationToken ct)
        => await new LiveCleanupBatch(owner).DropRolesAsync(
            $@"
DROP OWNED BY {AdminRole}, {ViewerRole}, {McpRole};
DROP ROLE IF EXISTS {AdminRole};
DROP ROLE IF EXISTS {ViewerRole};
DROP ROLE IF EXISTS {McpRole};",
            [AdminRole, ViewerRole, McpRole],
            ct);

    /// <summary>The actual column names of a <c>config</c>-schema table (for the ACL drift/partition guard).</summary>
    private static async Task<HashSet<string>> ColumnsOfConfigTableAsync(
        NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
    {
        var columns = new HashSet<string>(StringComparer.Ordinal);
        using var command = new NpgsqlCommand(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'config' AND table_name = $1", connection);
        command.Parameters.AddWithValue(table);
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static string RoleConnectionString(string baseConnectionString, string role)
        => new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Username = role,
            Password = RolePassword,
            SearchPath = "collect,config,public",
            Pooling = false,
        }.ConnectionString;

    private static async Task<string?> SchemaOfAsync(NpgsqlConnection connection, string table, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1", connection);
        command.Parameters.AddWithValue(table);
        return (string?)await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>Whether the CONNECTED role has <paramref name="privilege"/> on <paramref name="table"/>
    /// (the 2-arg <c>has_table_privilege</c> checks current_user) — proves grant shape without needing the
    /// table's columns.</summary>
    private static async Task<bool> HasPrivAsync(
        NpgsqlConnection connection, string table, string privilege, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand("SELECT has_table_privilege($1, $2)", connection);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(privilege);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}
