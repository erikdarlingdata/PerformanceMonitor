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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live fact mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the store rung for Level 2 (#5909, Phase B of #5632): its place at the top of the ladder, the probe's sentinel, the
/// grant text in both grant sources, and on a live store: a route and a request accept level 2 and refuse a level outside
/// the range, the agent-actions switch reads on on a new and an upgraded store, a change to a threshold override bumps the
/// config_version beacon, feedback goes with its request, a practice request outlives the request it replays, retention
/// ages feedback with its request and never ages overrides, and a second run changes nothing. Tests name the rung by its
/// name, never its number.
/// </summary>
public sealed class AgentLevel2StoreRungTests
{
    private const string RungName = "agent-level-2-store";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    [Fact]
    public void TheRungIsRegistered_AtTheTopOfTheLadder_AndTheViewerGateRequiresIt()
    {
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.Equal(StorageVersion.SchemaVersion, Rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
    }

    [Fact]
    public void TheScript_IsGuarded_MovesNoRow_AndCarriesNoGrant()
    {
        var sql = Rung.Sql;

        /* Every CREATE and ADD COLUMN is guarded, and the two CHECK swaps run only when level 2 is not named yet. */
        foreach (var line in sql.Split('\n').Where(l => l.StartsWith("CREATE ", StringComparison.Ordinal)))
        {
            Assert.True(line.Contains("IF NOT EXISTS", StringComparison.Ordinal) || line.Contains("TRIGGER", StringComparison.Ordinal), line);
        }

        Assert.Contains("ADD COLUMN IF NOT EXISTS agent_actions_enabled boolean NOT NULL DEFAULT true", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS practice_of uuid;", sql, StringComparison.Ordinal);
        Assert.Contains("pg_get_constraintdef(oid) !~ 'ARRAY\\[0, 1, 2\\]'", sql, StringComparison.Ordinal);
        Assert.Contains("pg_get_constraintdef(oid) !~ 'ARRAY\\[1, 2\\]'", sql, StringComparison.Ordinal);
        foreach (var verb in new[] { "INSERT INTO", "UPDATE config", "DELETE FROM", "GRANT ", "REVOKE " })
        {
            Assert.DoesNotContain(verb, sql, StringComparison.Ordinal);
        }

        /* practice_of has NO foreign key; the override's request_id has none either; feedback cascades with its request. */
        Assert.DoesNotMatch(@"practice_of[^;]*REFERENCES", sql);
        Assert.DoesNotMatch(@"\brequest_id uuid,[^;]*FOREIGN KEY \(request_id\)[^;]*config_threshold_overrides", sql);
        Assert.Contains("REFERENCES config.investigation_requests (request_id) ON DELETE CASCADE", sql, StringComparison.Ordinal);
        Assert.Contains("(coalesce(database_name, ''))", sql, StringComparison.Ordinal);
        Assert.Contains("FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeCarriesTheSentinel_AndAStoreBehindItMapsOneRungBack()
    {
        Assert.Contains("to_regclass('config.investigation_feedback') IS NOT NULL", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        var ownOrdinal = Array.FindIndex(parameters, p => p.Name == "hasAgentLevel2Store");
        Assert.Equal(parameters.Length - 1, ownOrdinal);

        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ownOrdinal] = false;
        Assert.Equal(StorageVersion.SchemaVersion - 1, (int)method.Invoke(null, behind)!);
    }

    /// <summary>A new store: the levels, the switch, the unique override scope, the beacon, the cascades and the practice link.</summary>
    [Fact]
    public async Task ANewStore_HoldsTheWholeRung()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            /* The switch reads on on a new store. */
            Assert.Equal("True", await ScalarAsync(store, "SELECT agent_actions_enabled FROM config.config_service", ct));

            await ExecAsync(store, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (5909, 'srv-a', 'host-a'), (5910, 'srv-b', 'host-b')", ct);

            /* The route accepts 0 to 2 and refuses 3 and -1. */
            foreach (var level in new[] { "NULL", "0", "1", "2" })
            {
                await ExecAsync(store, $"INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('m{level}', {level})", ct);
            }

            foreach (var level in new[] { "3", "-1" })
            {
                var refusal = await Assert.ThrowsAsync<PostgresException>(() =>
                    ExecAsync(store, $"INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('bad{level}', {level})", ct));
                Assert.Equal("ck_config_notification_routes_agent", refusal.ConstraintName);
            }

            /* The request accepts 1 and 2 and refuses 0 and 3. */
            await InsertRequestAsync(store, "11111111-1111-1111-1111-111111111111", 1, ct: ct);
            await InsertRequestAsync(store, "22222222-2222-2222-2222-222222222222", 2, ct: ct);
            foreach (var level in new[] { 0, 3 })
            {
                var refusal = await Assert.ThrowsAsync<PostgresException>(() =>
                    InsertRequestAsync(store, Guid.NewGuid().ToString(), level, ct: ct));
                Assert.Equal("ck_investigation_requests_autonomy", refusal.ConstraintName);
            }

            /* Overrides: one value per server, database and key, NULL database included. */
            await ExecAsync(store, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (5909, NULL, 'blocking.seconds', 30, 'agent-1')", ct);
            await ExecAsync(store, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (5909, 'sales', 'blocking.seconds', 60, 'agent-1')", ct);
            await ExecAsync(store, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (5910, NULL, 'blocking.seconds', 45, 'agent-1')", ct);
            var duplicate = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (5909, NULL, 'blocking.seconds', 31, 'agent-2')", ct));
            Assert.Equal("23505", duplicate.SqlState);
            Assert.Equal("ux_config_threshold_overrides_scope", duplicate.ConstraintName);

            /* request_id names the request that asked, with no foreign key: it may name one that does not exist. */
            await ExecAsync(store, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by, request_id) VALUES (5910, 'hr', 'cpu.percent', 90, 'agent-1', gen_random_uuid())", ct);

            /* An override change bumps the beacon: insert, update and delete each move config_version. */
            var version = long.Parse(await ScalarAsync(store, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture);
            await ExecAsync(store, "UPDATE config.config_threshold_overrides SET value = 33, updated_at = now() WHERE server_id = 5909 AND database_name IS NULL", ct);
            var afterUpdate = long.Parse(await ScalarAsync(store, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture);
            Assert.True(afterUpdate > version, "an UPDATE bumps the beacon");
            await ExecAsync(store, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (5909, 'sales', 'cpu.percent', 80, 'agent-1')", ct);
            var afterInsert = long.Parse(await ScalarAsync(store, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture);
            Assert.True(afterInsert > afterUpdate, "an INSERT bumps the beacon");
            await ExecAsync(store, "DELETE FROM config.config_threshold_overrides WHERE server_id = 5909 AND database_name = 'sales' AND threshold_key = 'cpu.percent'", ct);
            var afterDelete = long.Parse(await ScalarAsync(store, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture);
            Assert.True(afterDelete > afterInsert, "a DELETE bumps the beacon");

            /* An override goes with its server (and only its server's). */
            await ExecAsync(store, "DELETE FROM config.config_monitored_servers WHERE server_id = 5910", ct);
            Assert.Equal("2", await ScalarAsync(store, "SELECT count(*) FROM config.config_threshold_overrides", ct));
            Assert.Equal("0", await ScalarAsync(store, "SELECT count(*) FROM config.config_threshold_overrides WHERE server_id = 5910", ct));

            /* Feedback: the verdict is right, partly or wrong; one row per request per author. */
            const string Request = "11111111-1111-1111-1111-111111111111";
            await ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, corrected_verdict, missed_step, comment, author) VALUES ('{Request}', 'partly', 'needs_person', 'check the plan', 'close', 'erik')", ct);
            await ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('{Request}', 'right', 'sam')", ct);
            var badVerdict = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('{Request}', 'maybe', 'kim')", ct));
            Assert.Equal("ck_investigation_feedback_verdict", badVerdict.ConstraintName);
            var twice = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('{Request}', 'wrong', 'erik')", ct));
            Assert.Equal("uq_investigation_feedback_author", twice.ConstraintName);
            var orphan = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES (gen_random_uuid(), 'right', 'erik')", ct));
            Assert.Equal("23503", orphan.SqlState);

            /* Deleting a request deletes its feedback. */
            await ExecAsync(store, $"DELETE FROM config.investigation_requests WHERE request_id = '{Request}'", ct);
            Assert.Equal("0", await ScalarAsync(store, "SELECT count(*) FROM config.investigation_feedback", ct));

            /* A practice request survives deletion of the request it replays, and may name one that never existed. */
            const string Replayed = "22222222-2222-2222-2222-222222222222";
            await InsertRequestAsync(store, "33333333-3333-3333-3333-333333333333", 2, practiceOf: Replayed, ct: ct);
            await InsertRequestAsync(store, "44444444-4444-4444-4444-444444444444", 2, practiceOf: Guid.NewGuid().ToString(), ct: ct);
            await ExecAsync(store, $"DELETE FROM config.investigation_requests WHERE request_id = '{Replayed}'", ct);
            Assert.Equal("33333333-3333-3333-3333-333333333333,44444444-4444-4444-4444-444444444444",
                await ScalarAsync(store, "SELECT string_agg(request_id::text, ',' ORDER BY request_id) FROM config.investigation_requests", ct));
            Assert.Equal(Replayed, await ScalarAsync(store, "SELECT practice_of FROM config.investigation_requests WHERE request_id = '33333333-3333-3333-3333-333333333333'", ct));
        });
    }

    /// <summary>A store one rung back holding a Level 1 route, a Level 1 request and a stopped service: it takes the rung with
    /// all of them kept, the switch reads on, level 2 is accepted, and a second run (the migrator and the script itself) changes
    /// no constraint.</summary>
    [Fact]
    public async Task AnUpgradedStore_KeepsItsRows_ReadsTheSwitchOn_AndASecondRunChangesNothing()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            await RungRewindArtifacts.DropV191SentinelAsync(store, ct);
            await ExecAsync(store, "DELETE FROM darling_schema_version WHERE version >= " + Rung.Version.ToString(CultureInfo.InvariantCulture), ct);
            await ExecAsync(store, "UPDATE config.config_service SET paused = TRUE", ct);
            await ExecAsync(store, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('kept', 1)", ct);
            await InsertRequestAsync(store, "55555555-5555-5555-5555-555555555555", 1, ct: ct);

            /* One rung back: level 2 is refused, the new things are absent. */
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('two', 2)", ct));
            Assert.Null(await ScalarAsync(store, "SELECT to_regclass('config.investigation_feedback')::text", ct));

            await PgMigrations.MigrateAsync(store, ct);
            Assert.Equal(StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture), await ScalarAsync(store, "SELECT max(version) FROM darling_schema_version", ct));
            Assert.Equal("True", await ScalarAsync(store, "SELECT agent_actions_enabled FROM config.config_service", ct));
            Assert.Equal("True", await ScalarAsync(store, "SELECT paused FROM config.config_service", ct));
            Assert.Equal("1", await ScalarAsync(store, "SELECT agent_level FROM config.config_notification_routes WHERE metric_match = 'kept'", ct));
            Assert.Equal("1", await ScalarAsync(store, "SELECT autonomy_level FROM config.investigation_requests WHERE request_id = '55555555-5555-5555-5555-555555555555'", ct));
            Assert.Null(await ScalarAsync(store, "SELECT practice_of FROM config.investigation_requests WHERE request_id = '55555555-5555-5555-5555-555555555555'", ct));
            await ExecAsync(store, "UPDATE config.config_notification_routes SET agent_level = 2 WHERE metric_match = 'kept'", ct);
            await ExecAsync(store, "UPDATE config.investigation_requests SET autonomy_level = 2 WHERE request_id = '55555555-5555-5555-5555-555555555555'", ct);
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store, "UPDATE config.config_notification_routes SET agent_level = 3 WHERE metric_match = 'kept'", ct));

            /* A second run changes nothing: the checks keep their catalog identity and the script raises nothing. */
            const string Identity = "SELECT string_agg(c.oid::text || ':' || c.conname, ',' ORDER BY c.conname) FROM pg_constraint c "
                + "WHERE c.conname IN ('ck_config_notification_routes_agent', 'ck_investigation_requests_autonomy')";
            var before = await ScalarAsync(store, Identity, ct);
            await PgMigrations.MigrateAsync(store, ct);
            await ExecAsync(store, Rung.Sql, ct);
            Assert.Equal(before, await ScalarAsync(store, Identity, ct));
            Assert.Equal("1", await ScalarAsync(store, "SELECT count(*) FROM pg_trigger WHERE tgname = 'trg_bump_threshold_overrides' AND NOT tgisinternal", ct));
        });
    }

    /// <summary>Retention: an aged request goes and takes its feedback, a younger request keeps its feedback, a practice request
    /// ages like any request, and the overrides are never aged.</summary>
    [Fact]
    public async Task Retention_AgesFeedbackWithItsRequest_AndNeverAgesOverrides()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            await ExecAsync(store, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (5909, 'srv-a', 'host-a')", ct);
            await ExecAsync(store, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by, created_at, updated_at) "
                + "VALUES (5909, NULL, 'blocking.seconds', 30, 'agent-1', now() - interval '800 days', now() - interval '800 days')", ct);

            const string Aged = "aaaaaaaa-0000-0000-0000-000000000001";
            const string AgedPractice = "aaaaaaaa-0000-0000-0000-000000000002";
            const string Young = "bbbbbbbb-0000-0000-0000-000000000001";
            await InsertRequestAsync(store, Aged, 2, openedDaysAgo: 400, state: "closed", ct: ct);
            await InsertRequestAsync(store, AgedPractice, 2, openedDaysAgo: 400, state: "closed", practiceOf: Young, ct: ct);
            await InsertRequestAsync(store, Young, 2, openedDaysAgo: 5, state: "closed", ct: ct);
            foreach (var request in new[] { Aged, Young })
            {
                await ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('{request}', 'right', 'erik')", ct);
            }

            await using var dataSource = NpgsqlDataSource.Create(store.ConnectionString);
            await DarlingRetention.PurgeAsync(dataSource, timescaleAvailable: false, logger: null, TestContext.Current.CancellationToken);

            Assert.Equal(Young, await ScalarAsync(store, "SELECT string_agg(request_id::text, ',' ORDER BY request_id) FROM config.investigation_requests", ct));
            Assert.Equal(Young, await ScalarAsync(store, "SELECT string_agg(request_id::text, ',' ORDER BY request_id) FROM config.investigation_feedback", ct));
            Assert.Equal("1", await ScalarAsync(store, "SELECT count(*) FROM config.config_threshold_overrides", ct));
        });
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Level 2 store pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var store = new NpgsqlConnection(scratch.ConnectionString);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);
        /* The service seeds the singleton row on its first start; the beacon trigger bumps nothing without it. */
        await ExecAsync(store, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);

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
        NpgsqlConnection store, string requestId, int level, CancellationToken ct,
        string? practiceOf = null, int openedDaysAgo = 0, string state = "open")
    {
        /* practice_of is named only when given: a store one rung back does not have the column. */
        var practiceColumn = practiceOf is null ? string.Empty : ", practice_of";
        var practiceValue = practiceOf is null ? string.Empty : $", '{practiceOf}'";
        await ExecAsync(store,
            "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at, "
            + $"state, autonomy_level, opened_at{practiceColumn}) VALUES ('{requestId}', 1, 5909, 'srv-a', 'blocking', 'blocking', now(), now(), now() + interval '30 minutes', now() + interval '4 hours', "
            + $"'{state}', {level.ToString(CultureInfo.InvariantCulture)}, now() - interval '{openedDaysAgo.ToString(CultureInfo.InvariantCulture)} days'{practiceValue})", ct);
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
/// The grants behind the Level 2 tables (#5909), on a real store and as real logins, in both grant sources: the managed
/// batch and the bring-your-own <c>provision-roles.sql</c> step 3j. The fixed-name <c>admin</c>, <c>viewer</c> and
/// <c>mcp</c> roles are cluster-wide, so the class is in the <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class AgentLevel2StoreGrantLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres; the fixed admin/viewer/mcp roles
       are why the class sits in the pg-cluster-roles collection. */
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "mcp"];

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the Level 2 grant live tests.");
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

    private static async Task AssertDeniedAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, sql, ct));
        Assert.Equal("42501", ex.SqlState);
    }

    private static async Task CleanUpAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "l2store5909none", cleanupCt));

    private static async Task<bool> SkipIfRolesExistAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        var exist = await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) == "True";
        Assert.SkipWhen(exist, "A cluster-wide admin/viewer/mcp role already exists on this rig; the grants would adopt it.");
        return exist;
    }

    [Fact]
    public void BothGrantSources_SayTheSame_AboutTheNewTables()
    {
        var managed = Regex.Replace(
            DarlingManagedRoles.BuildProvisioningSql(ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp),
            @"\s+", " ");
        var script = Regex.Replace(RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql"), @"\s+", " ");

        const string Both = "config.config_threshold_overrides, config.investigation_feedback";
        foreach (var source in new[] { managed, script })
        {
            Assert.Contains($"REVOKE ALL ON {Both} FROM PUBLIC, admin, viewer, mcp CASCADE;", source, StringComparison.Ordinal);
            Assert.Contains($"GRANT SELECT ON {Both} TO admin, viewer;", source, StringComparison.Ordinal);
            Assert.Contains($"GRANT SELECT ON {Both} TO mcp;", source, StringComparison.Ordinal);
            Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.config_threshold_overrides TO admin;", source, StringComparison.Ordinal);
            Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.investigation_feedback TO admin;", source, StringComparison.Ordinal);
            Assert.Contains("GRANT INSERT, UPDATE, DELETE ON config.config_threshold_overrides TO mcp;", source, StringComparison.Ordinal);

            /* Viewer gets no write, and mcp gets nothing on feedback beyond the read. */
            Assert.DoesNotMatch(@"GRANT [A-Z ,()a-z_]*(INSERT|UPDATE|DELETE)[^;]* ON config\.(config_threshold_overrides|investigation_feedback) TO [a-z_, ]*viewer", source);
            Assert.DoesNotMatch(@"GRANT [A-Z ,()a-z_]*(INSERT|UPDATE|DELETE)[^;]* ON config\.investigation_feedback TO [a-z_, ]*mcp", source);
        }

        /* The bring-your-own header names the overrides table among the grants a blind re-run does not cover. */
        Assert.Contains("config_threshold_overrides and investigation_feedback, #5909) are V191", script, StringComparison.Ordinal);
    }

    /// <summary>The whole matrix, read from the catalog: role by table by verb.</summary>
    private static async Task AssertMatrixAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        (string Role, string Table, string Verbs)[] expected =
        [
            ("admin", "config.config_threshold_overrides", "SELECT,INSERT,UPDATE,DELETE"),
            ("admin", "config.investigation_feedback", "SELECT,INSERT,UPDATE,DELETE"),
            ("viewer", "config.config_threshold_overrides", "SELECT"),
            ("viewer", "config.investigation_feedback", "SELECT"),
            ("mcp", "config.config_threshold_overrides", "SELECT,INSERT,UPDATE,DELETE"),
            ("mcp", "config.investigation_feedback", "SELECT"),
        ];
        foreach (var (role, table, verbs) in expected)
        {
            var held = string.Join(",", new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER" }
                .Where(verb => ScalarAsync(owner, $"SELECT has_table_privilege('{role}', '{table}', '{verb}')", ct).GetAwaiter().GetResult() == "True"));
            Assert.Equal(verbs, held);
        }
    }

    [Fact]
    public async Task TheManagedBatch_GivesAdminTheWrites_McpTheOverridesOnly_AndViewerNone()
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
            await AssertMatrixAsync(owner, ct);
            await ExerciseLoginsAsync(owner, scratch, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheBringYourOwnScript_Step3j_GrantsTheSameMatrix_AndSkipsOnAStoreBehindTheRung()
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
            await ExecAsync(owner, "CREATE ROLE admin NOLOGIN; CREATE ROLE viewer NOLOGIN; CREATE ROLE mcp NOLOGIN;", ct);

            var script = RepoFile.ReadRepoFile("Darling", "tools", "provision-roles.sql").Replace("\r\n", "\n", StringComparison.Ordinal);
            var start = script.IndexOf("-- 3j. Level 2 tables", StringComparison.Ordinal);
            var end = script.IndexOf("-- 3g. The password key tables", StringComparison.Ordinal);
            Assert.True(start > 0 && end > start, "step 3j sits just above step 3g");
            var block = script[start..end];

            /* Behind the rung (the tables are absent): the block warns and skips, and grants nothing. */
            await ExecAsync(owner, "ALTER TABLE config.investigation_feedback RENAME TO investigation_feedback_hidden", ct);
            await ExecAsync(owner, block, ct);
            Assert.Equal("False", await ScalarAsync(owner, "SELECT has_table_privilege('mcp', 'config.config_threshold_overrides', 'INSERT')", ct));
            await ExecAsync(owner, "ALTER TABLE config.investigation_feedback_hidden RENAME TO investigation_feedback", ct);

            await ExecAsync(owner, block, ct);
            await ExecAsync(owner, block, ct);
            await AssertMatrixAsync(owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>As the real logins: mcp writes overrides (the beacon trigger runs as mcp), viewer cannot, mcp cannot write feedback.</summary>
    private static async Task ExerciseLoginsAsync(NpgsqlConnection owner, ScratchPostgres scratch, CancellationToken ct)
    {
        await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
        await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (5909, 'srv-a', 'host-a')", ct);
        await ExecAsync(owner, "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at) "
            + "VALUES ('66666666-6666-6666-6666-666666666666', 1, 5909, 'srv-a', 'blocking', 'blocking', now(), now(), now(), now() + interval '1 hour')", ct);

        await using (var mcp = await LoginAsync(scratch, "mcp", ProvisioningTestSecrets.McpPassword, ct))
        {
            var version = long.Parse(await ScalarAsync(owner, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture);
            await ExecAsync(mcp, "INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (5909, 'blocking.seconds', 30, 'mcp-agent')", ct);
            await ExecAsync(mcp, "UPDATE config.config_threshold_overrides SET value = 40, updated_at = now() WHERE threshold_key = 'blocking.seconds'", ct);
            Assert.Equal("40", await ScalarAsync(mcp, "SELECT value FROM config.config_threshold_overrides WHERE threshold_key = 'blocking.seconds'", ct));
            Assert.True(long.Parse(await ScalarAsync(owner, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture) > version, "mcp's write reaches the beacon");
            await ExecAsync(mcp, "DELETE FROM config.config_threshold_overrides WHERE threshold_key = 'blocking.seconds'", ct);

            Assert.Equal("0", await ScalarAsync(mcp, "SELECT count(*) FROM config.investigation_feedback", ct));
            await AssertDeniedAsync(mcp, "INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('66666666-6666-6666-6666-666666666666', 'right', 'mcp-agent')", ct);
            await AssertDeniedAsync(mcp, "UPDATE config.investigation_feedback SET verdict = 'wrong'", ct);
            await AssertDeniedAsync(mcp, "DELETE FROM config.investigation_feedback", ct);
        }

        await ExecAsync(owner, "INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (5909, 'cpu.percent', 80, 'owner')", ct);
        await ExecAsync(owner, "INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('66666666-6666-6666-6666-666666666666', 'right', 'erik')", ct);
        await using (var viewer = await LoginAsync(scratch, "viewer", ProvisioningTestSecrets.ViewerPassword, ct))
        {
            Assert.Equal("1", await ScalarAsync(viewer, "SELECT count(*) FROM config.config_threshold_overrides", ct));
            Assert.Equal("1", await ScalarAsync(viewer, "SELECT count(*) FROM config.investigation_feedback", ct));
            await AssertDeniedAsync(viewer, "INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (5909, 'x', 1, 'viewer')", ct);
            await AssertDeniedAsync(viewer, "UPDATE config.config_threshold_overrides SET value = 1", ct);
            await AssertDeniedAsync(viewer, "DELETE FROM config.config_threshold_overrides", ct);
            await AssertDeniedAsync(viewer, "INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('66666666-6666-6666-6666-666666666666', 'right', 'viewer')", ct);
            await AssertDeniedAsync(viewer, "UPDATE config.investigation_feedback SET verdict = 'wrong'", ct);
            await AssertDeniedAsync(viewer, "DELETE FROM config.investigation_feedback", ct);
        }

        await using (var admin = await LoginAsync(scratch, "admin", ProvisioningTestSecrets.AdminPassword, ct))
        {
            await ExecAsync(admin, "UPDATE config.config_threshold_overrides SET value = 85 WHERE threshold_key = 'cpu.percent'", ct);
            await ExecAsync(admin, "INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('66666666-6666-6666-6666-666666666666', 'wrong', 'admin')", ct);
            await ExecAsync(admin, "UPDATE config.investigation_feedback SET comment = 'edited' WHERE author = 'admin'", ct);
            await ExecAsync(admin, "DELETE FROM config.investigation_feedback WHERE author = 'admin'", ct);
            await ExecAsync(admin, "DELETE FROM config.config_threshold_overrides", ct);
        }
    }
}
