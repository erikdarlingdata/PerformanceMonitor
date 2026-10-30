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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live fact mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the Phase C store rung (#5997, C1 of #5632) on a live store: a new store holds the whole rung, an upgraded store keeps its
/// rows and a second run changes nothing, and retention clears evidence with the brief at 90 days, keeps the evidence of an open
/// or claimed request, and purges signature days at 30 days. Tests name the rung by its name, never its number.
/// </summary>
public sealed class PhaseCStoreTests
{
    private const string RungName = "phase-c-store";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    /// <summary>A new store: every table and column, the journal's event id filled and unique, and the checks refusing what they
    /// are for.</summary>
    [Fact]
    public async Task ANewStore_HoldsTheWholeRung()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            Assert.Equal(StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture), await ScalarAsync(store, "SELECT max(version) FROM darling_schema_version", ct));
            Assert.Equal("3", await ScalarAsync(store,
                "SELECT count(*) FROM pg_class WHERE relnamespace = 'config'::regnamespace AND relname IN "
                + "('investigation_evidence', 'investigation_signature_days', 'investigation_signature_merges')", ct));
            Assert.Equal(AllColumns, await ScalarAsync(store, ColumnsSql, ct));

            /* Two journal rows get two different event ids. */
            await ExecAsync(store, "INSERT INTO config.investigation_journal (event, actor) VALUES ('opened', 'service'), ('joined', 'service')", ct);
            Assert.Equal("2", await ScalarAsync(store, "SELECT count(DISTINCT event_id) FROM config.investigation_journal WHERE event_id IS NOT NULL", ct));

            /* A route cannot name itself as its own fallback, and can name another. */
            await ExecAsync(store, "INSERT INTO config.config_notification_routes (metric_match) VALUES ('a'), ('b')", ct);
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store, "UPDATE config.config_notification_routes SET fallback_route_id = route_id WHERE metric_match = 'a'", ct));
            await ExecAsync(store, "UPDATE config.config_notification_routes SET fallback_route_id = (SELECT route_id FROM config.config_notification_routes WHERE metric_match = 'b') WHERE metric_match = 'a'", ct);

            /* A report's family id must name a request, and queries_run is held to 20 items of 2,000 characters. */
            await InsertRequestAsync(store, RequestA, ct: ct);
            await InsertReportAsync(store, RequestA, familyOf: null, queriesRun: "'[]'", ct);
            await ExecAsync(store, "DELETE FROM config.investigation_reports", ct);
            await Assert.ThrowsAsync<PostgresException>(() => InsertReportAsync(store, RequestA, familyOf: RequestB, queriesRun: null, ct));
            var twentyOne = "'[" + string.Join(",", Enumerable.Repeat("\"select 1\"", 21)) + "]'";
            await Assert.ThrowsAsync<PostgresException>(() => InsertReportAsync(store, RequestA, familyOf: null, queriesRun: twentyOne, ct));
            var tooLong = "'[\"" + new string('x', 2001) + "\"]'";
            await Assert.ThrowsAsync<PostgresException>(() => InsertReportAsync(store, RequestA, familyOf: null, queriesRun: tooLong, ct));
            var justRight = "'[\"" + new string('x', 2000) + "\"]'";
            await InsertReportAsync(store, RequestA, familyOf: RequestA, queriesRun: justRight, ct);

            /* Evidence is one row per section and request, and is held to 32 KB. */
            await ExecAsync(store, $"INSERT INTO config.investigation_evidence (request_id, section, state) VALUES ('{RequestA}', 'waits', 'pending')", ct);
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store, $"INSERT INTO config.investigation_evidence (request_id, section, state) VALUES ('{RequestA}', 'waits', 'ready')", ct));
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store,
                $"INSERT INTO config.investigation_evidence (request_id, section, state, content) VALUES ('{RequestA}', 'big', 'ready', to_jsonb('{new string('y', 33000)}'::text))", ct));
        });
    }

    /// <summary>A store one rung back holding a journal, a route, a notification row and procedure stats: it takes the rung with all
    /// of them kept, every journal row gets its own event id, and a second run changes no constraint, index or column.</summary>
    [Fact]
    public async Task AnUpgradedStore_KeepsItsRows_FillsEventIds_AndASecondRunChangesNothing()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            await RungRewindArtifacts.DropSentinelsAboveAsync(store, Rung.Version - 1, ct);
            await ExecAsync(store, "DELETE FROM darling_schema_version WHERE version >= " + Rung.Version.ToString(CultureInfo.InvariantCulture), ct);
            Assert.Null(await ScalarAsync(store, "SELECT to_regclass('config.investigation_evidence')::text", ct));
            Assert.Equal("0", await ScalarAsync(store, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'investigation_journal' AND column_name = 'event_id'", ct));

            await ExecAsync(store, "INSERT INTO config.investigation_journal (event, actor) VALUES ('opened', 'service'), ('joined', 'service'), ('resolved', 'service')", ct);
            await ExecAsync(store, "INSERT INTO config.config_notification_routes (metric_match, generic_url) VALUES ('kept', 'https://example.test/hook')", ct);

            await PgMigrations.MigrateAsync(store, ct);
            Assert.Equal(StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture), await ScalarAsync(store, "SELECT max(version) FROM darling_schema_version", ct));
            Assert.Equal(AllColumns, await ScalarAsync(store, ColumnsSql, ct));
            Assert.Equal("3", await ScalarAsync(store, "SELECT count(DISTINCT event_id) FROM config.investigation_journal", ct));
            Assert.Equal("https://example.test/hook", await ScalarAsync(store, "SELECT generic_url FROM config.config_notification_routes WHERE metric_match = 'kept'", ct));
            Assert.Equal(string.Empty, await ScalarAsync(store, "SELECT signing_key FROM config.config_notification_routes WHERE metric_match = 'kept'", ct));

            /* A second run (the migrator, then the rung's own text) changes nothing: the same objects, the same event ids. */
            const string Identity = "SELECT (SELECT string_agg(c.oid::text || ':' || c.conname, ',' ORDER BY c.conname) FROM pg_constraint c WHERE c.connamespace = 'config'::regnamespace) "
                + "|| '|' || (SELECT string_agg(i.indexrelid::text, ',' ORDER BY i.indexrelid) FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid WHERE t.relnamespace = 'config'::regnamespace) "
                + "|| '|' || (SELECT string_agg(event_id::text, ',' ORDER BY event_id) FROM config.investigation_journal)";
            var before = await ScalarAsync(store, Identity, ct);
            await PgMigrations.MigrateAsync(store, ct);
            await ExecAsync(store, Rung.Sql, ct);
            Assert.Equal(before, await ScalarAsync(store, Identity, ct));
            Assert.Equal(AllColumns, await ScalarAsync(store, ColumnsSql, ct));
        });
    }

    /// <summary>Evidence is cleared with the brief at 90 days; the request keeps its row until the year.</summary>
    [Fact]
    public async Task Retention_ClearsEvidenceWithTheBriefAt90Days_AndKeepsYoungerEvidence()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            await InsertRequestAsync(store, RequestA, openedDaysAgo: 100, state: "closed", ct: ct);
            await InsertRequestAsync(store, RequestB, openedDaysAgo: 30, state: "closed", ct: ct);
            foreach (var request in new[] { RequestA, RequestB })
            {
                await ExecAsync(store, $"UPDATE config.investigation_requests SET brief = '{{\"k\":1}}'::jsonb WHERE request_id = '{request}'", ct);
                await ExecAsync(store, $"INSERT INTO config.investigation_evidence (request_id, section, state, content) VALUES ('{request}', 'waits', 'ready', '{{\"k\":1}}'::jsonb), ('{request}', 'jobs', 'ready', '{{\"k\":2}}'::jsonb)", ct);
            }

            await PurgeAsync(store, ct);

            Assert.Equal("0", await ScalarAsync(store, $"SELECT count(*) FROM config.investigation_evidence WHERE request_id = '{RequestA}'", ct));
            Assert.Null(await ScalarAsync(store, $"SELECT brief::text FROM config.investigation_requests WHERE request_id = '{RequestA}'", ct));
            Assert.Equal("2", await ScalarAsync(store, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal("2", await ScalarAsync(store, $"SELECT count(*) FROM config.investigation_evidence WHERE request_id = '{RequestB}'", ct));
        });
    }

    /// <summary>An open or claimed request keeps its evidence however old it is.</summary>
    [Fact]
    public async Task Retention_KeepsTheEvidenceOfAnOpenOrClaimedRequest()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            const string Claimed = "cccccccc-0000-0000-0000-000000000001";
            await InsertRequestAsync(store, RequestA, openedDaysAgo: 100, state: "open", ct: ct);
            await InsertRequestAsync(store, Claimed, openedDaysAgo: 100, state: "claimed", ct: ct);
            await InsertRequestAsync(store, RequestB, openedDaysAgo: 100, state: "reported", ct: ct);
            foreach (var request in new[] { RequestA, Claimed, RequestB })
            {
                await ExecAsync(store, $"INSERT INTO config.investigation_evidence (request_id, section, state, content) VALUES ('{request}', 'waits', 'ready', '{{\"k\":1}}'::jsonb)", ct);
            }

            await PurgeAsync(store, ct);

            Assert.Equal($"{RequestA},{Claimed}", await ScalarAsync(store, "SELECT string_agg(request_id::text, ',' ORDER BY request_id) FROM config.investigation_evidence", ct));
        });
    }

    /// <summary>Signature days purge at 30 days; the merges that name them are decisions and stay.</summary>
    [Fact]
    public async Task Retention_PurgesSignatureDaysAt30Days_AndKeepsMerges()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            await ExecAsync(store,
                "INSERT INTO config.investigation_signature_days (server_id, signature, day, count) VALUES "
                + "(1, 'sig-a', (now() AT TIME ZONE 'UTC')::date - 35, 4), "
                + "(1, 'sig-a', (now() AT TIME ZONE 'UTC')::date - 40, 2), "
                + "(1, 'sig-a', (now() AT TIME ZONE 'UTC')::date - 25, 5), "
                + "(1, 'sig-b', (now() AT TIME ZONE 'UTC')::date, 1)", ct);
            await ExecAsync(store,
                "INSERT INTO config.investigation_signature_merges (server_id, signature, merged_into, reason, merged_by, merged_at) "
                + "VALUES (1, 'sig-a', 'sig-b', 'same procedure, two object sets', 'erik', now() - interval '400 days')", ct);

            await PurgeAsync(store, ct);

            Assert.Equal("sig-a:5,sig-b:1", await ScalarAsync(store,
                "SELECT string_agg(signature || ':' || count, ',' ORDER BY signature) FROM config.investigation_signature_days", ct));
            Assert.Equal("1", await ScalarAsync(store, "SELECT count(*) FROM config.investigation_signature_merges", ct));
        });
    }

    private const string RequestA = "aaaaaaaa-0000-0000-0000-000000000001";
    private const string RequestB = "bbbbbbbb-0000-0000-0000-000000000001";

    private const string ColumnsSql =
        "SELECT string_agg(table_name || '.' || column_name, ',' ORDER BY table_name, column_name) FROM information_schema.columns "
        + "WHERE (table_schema = 'config' AND ((table_name = 'investigation_journal' AND column_name = 'event_id') "
        + "OR (table_name = 'investigation_requests' AND column_name IN ('max_severity', 'evidence_state', 'follow_up_of', 'follow_up_at', 'follow_up_condition')) "
        + "OR (table_name = 'investigation_reports' AND column_name IN ('family_of', 'queries_run')) "
        + "OR (table_name IN ('config_notification', 'config_notification_routes') AND column_name IN ('signing_key', 'previous_signing_key', 'previous_signing_key_until')) "
        + "OR (table_name = 'config_notification_routes' AND column_name = 'fallback_route_id'))) "
        + "OR (table_schema = 'collect' AND table_name = 'procedure_stats' AND column_name = 'module_modify_date')";

    private const string AllColumns =
        "config_notification.previous_signing_key,config_notification.previous_signing_key_until,config_notification.signing_key,"
        + "config_notification_routes.fallback_route_id,config_notification_routes.previous_signing_key,config_notification_routes.previous_signing_key_until,"
        + "config_notification_routes.signing_key,investigation_journal.event_id,investigation_reports.family_of,investigation_reports.queries_run,"
        + "investigation_requests.evidence_state,investigation_requests.follow_up_at,investigation_requests.follow_up_condition,investigation_requests.follow_up_of,"
        + "investigation_requests.max_severity,procedure_stats.module_modify_date";

    private static async Task PurgeAsync(NpgsqlConnection store, CancellationToken ct)
    {
        await using var dataSource = NpgsqlDataSource.Create(store.ConnectionString);
        await DarlingRetention.PurgeAsync(dataSource, timescaleAvailable: false, logger: null, ct);
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Phase C store pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var store = new NpgsqlConnection(scratch.ConnectionString);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);

        var bodySucceeded = false;
        try
        {
            await body(store, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task InsertRequestAsync(
        NpgsqlConnection store, string requestId, CancellationToken ct, int openedDaysAgo = 0, string state = "open")
    {
        await ExecAsync(store,
            "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at, "
            + $"state, opened_at) VALUES ('{requestId}', 1, 5997, 'srv-a', 'blocking', 'blocking', now(), now(), now() + interval '30 minutes', now() + interval '4 hours', "
            + $"'{state}', now() - interval '{openedDaysAgo.ToString(CultureInfo.InvariantCulture)} days')", ct);
    }

    private static async Task InsertReportAsync(NpgsqlConnection store, string requestId, string? familyOf, string? queriesRun, CancellationToken ct)
    {
        var family = familyOf is null ? "NULL" : $"'{familyOf}'";
        var queries = queriesRun ?? "NULL";
        await ExecAsync(store,
            "INSERT INTO config.investigation_reports (request_id, report_format, agent_id, claim_id, verdict, root_cause, evidence, server_id, family_of, queries_run) "
            + $"VALUES ('{requestId}', 1, 'agent-1', gen_random_uuid(), 'real', 'a cause', '[{{\"k\":1}}]'::jsonb, 5997, {family}, {queries}::jsonb)", ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null! : Convert.ToString(value, CultureInfo.InvariantCulture)!;
    }
}

/// <summary>
/// The grants behind the Phase C store (#5997) on a real store and as real logins, in both grant sources: the managed batch and
/// the bring-your-own <c>provision-roles.sql</c> step 3l. The viewer and mcp roles cannot read the signing key columns of the
/// notification settings row or of a route; the service (the store owner) can. The fixed-name roles are cluster-wide, so the
/// class is in the <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class PhaseCStoreGrantLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres; the fixed admin/viewer/mcp roles
       are why the class sits in the pg-cluster-roles collection. */
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "mcp"];

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the Phase C grant live tests.");
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

    private static async Task CleanUpAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "phasec5997none", cleanupCt));

    private static async Task<bool> SkipIfRolesExistAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        var exist = await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) == "True";
        Assert.SkipWhen(exist, "A cluster-wide admin/viewer/mcp role already exists on this rig; the grants would adopt it.");
        return exist;
    }

    /// <summary>The rows exist; the owner (the service) reads the keys; viewer and mcp are refused the key columns of both
    /// tables, and read the non-secret columns beside them and the three new tables.</summary>
    private static async Task AssertKeysHiddenFromTheReadRolesAsync(
        NpgsqlConnection owner, ScratchPostgres scratch, string adminPassword, string viewerPassword, string mcpPassword, CancellationToken ct)
    {
        await ExecAsync(owner, "INSERT INTO config.config_notification (id, signing_key, previous_signing_key) VALUES (1, 'sealed-key', 'sealed-previous') ON CONFLICT (id) DO UPDATE SET signing_key = 'sealed-key', previous_signing_key = 'sealed-previous'", ct);
        await ExecAsync(owner, "INSERT INTO config.config_notification_routes (metric_match, signing_key, previous_signing_key) VALUES ('m', 'route-key', 'route-previous')", ct);

        Assert.Equal("sealed-key", await ScalarAsync(owner, "SELECT signing_key FROM config.config_notification WHERE id = 1", ct));
        Assert.Equal("route-previous", await ScalarAsync(owner, "SELECT previous_signing_key FROM config.config_notification_routes WHERE metric_match = 'm'", ct));

        foreach (var (role, password) in new[] { ("viewer", viewerPassword), ("mcp", mcpPassword) })
        {
            await using var login = await LoginAsync(scratch, role, password, ct);
            foreach (var secret in new[]
            {
                "SELECT signing_key FROM config.config_notification",
                "SELECT previous_signing_key FROM config.config_notification",
                "SELECT signing_key FROM config.config_notification_routes",
                "SELECT previous_signing_key FROM config.config_notification_routes",
                "SELECT * FROM config.config_notification_routes",
            })
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(login, secret, ct));
                Assert.Equal("42501", ex.SqlState);
            }

            await ExecAsync(login, "SELECT previous_signing_key_until FROM config.config_notification", ct);
            await ExecAsync(login, "SELECT previous_signing_key_until, fallback_route_id FROM config.config_notification_routes", ct);
            await ExecAsync(login, "SELECT count(*) FROM config.investigation_evidence", ct);
            await ExecAsync(login, "SELECT count(*) FROM config.investigation_signature_days", ct);
            await ExecAsync(login, "SELECT count(*) FROM config.investigation_signature_merges", ct);
        }

        /* The desktop Viewer connects as admin and manages the keys, so admin is not carved. */
        await using var admin = await LoginAsync(scratch, "admin", adminPassword, ct);
        await ExecAsync(admin, "SELECT signing_key, previous_signing_key FROM config.config_notification_routes", ct);

        /* #6006: the signature merge is the one Phase C write. mcp adds a merge row (only the columns it supplies); admin stamps the
           undo; nobody else writes, and mcp cannot rewrite one. #6401: undo_agent_change lets mcp stamp the undo too, on those two
           columns only. */
        await using (var mcp = await LoginAsync(scratch, "mcp", mcpPassword, ct))
        {
            await ExecAsync(mcp, "INSERT INTO config.investigation_signature_merges (server_id, signature, merged_into, reason, merged_by) VALUES (1, 'dl1-a', 'dl1-b', 'same', 'agent:a')", ct);
            await ExecAsync(mcp, "UPDATE config.investigation_signature_merges SET undone_at = now(), undone_by = 'agent:a'", ct);
            foreach (var denied in new[]
            {
                "UPDATE config.investigation_signature_merges SET merged_into = 'dl1-z'",
                "UPDATE config.investigation_signature_merges SET reason = 'other'",
                "DELETE FROM config.investigation_signature_merges",
                "INSERT INTO config.investigation_signature_days (server_id, signature, day, count) VALUES (1, 'dl1-a', current_date, 1)",
            })
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(mcp, denied, ct));
                Assert.Equal("42501", ex.SqlState);
            }
        }

        await using (var viewer = await LoginAsync(scratch, "viewer", viewerPassword, ct))
        {
            var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(viewer,
                "INSERT INTO config.investigation_signature_merges (server_id, signature, merged_into, reason, merged_by) VALUES (1, 'dl1-c', 'dl1-d', 'same', 'x')", ct));
            Assert.Equal("42501", ex.SqlState);
        }

        await ExecAsync(admin, "UPDATE config.investigation_signature_merges SET undone_at = now(), undone_by = 'person:erik'", ct);
        var adminDenied = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(admin, "DELETE FROM config.investigation_signature_merges", ct));
        Assert.Equal("42501", adminDenied.SqlState);
    }

    [Fact]
    public async Task TheManagedBatch_KeepsTheSigningKeysFromViewerAndMcp_AndTheServiceReadsThem()
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
            await SkipIfRolesExistAsync(owner, ct);
            var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
            await ExecAsync(owner,
                DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)), ct);
            await AssertKeysHiddenFromTheReadRolesAsync(
                owner, scratch, ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.ViewerPassword, ProvisioningTestSecrets.McpPassword, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheBringYourOwnScript_KeepsTheSigningKeysFromViewerAndMcp_AndTheServiceReadsThem()
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
            await SkipIfRolesExistAsync(owner, ct);
            Assert.SkipUnless(
                await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct) == "True",
                "The script names the store owner 'darling', and this rig has no such role.");

            const string AdminPassword = "ScriptAdmin5997", ViewerPassword = "ScriptViewer5997", McpPassword = "ScriptMcp5997";
            var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                    System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                .Replace("CHANGE_ME_ADMIN_PASSWORD", AdminPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", ViewerPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", McpPassword, StringComparison.Ordinal);
            await ExecAsync(owner, script, ct);
            await ExecAsync(owner, script, ct);
            await AssertKeysHiddenFromTheReadRolesAsync(owner, scratch, AdminPassword, ViewerPassword, McpPassword, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }
}
