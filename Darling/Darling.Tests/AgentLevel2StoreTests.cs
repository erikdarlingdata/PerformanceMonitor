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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live fact mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the store rung for Level 2 (#5909, Phase B of #5632): its place at the top of the ladder, the probe's sentinel,
/// grants in both grant sources (live, as real logins), and on a live store: a route and a request accept level 2 and refuse a level outside
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
    public void TheRungIsRegistered_AndTheViewerGateRequiresTheLadderTop()
    {
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.Equal(191, Rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
    }

    [Fact]
    public void TheProbeCarriesTheSentinel_AndAStoreBehindItMapsOneRungBack()
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        var ownOrdinal = Array.FindIndex(parameters, p => p.Name == "hasAgentLevel2Store");
        Assert.True(ownOrdinal >= 0);

        /* A later rung's sentinel (V192 and above) is absent: this rung is then the answer, and one rung back without its own. */
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        for (var later = ownOrdinal + 1; later < parameters.Length; later++)
        {
            all[later] = false;
        }

        Assert.Equal(191, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ownOrdinal] = false;
        Assert.Equal(190, (int)method.Invoke(null, behind)!);
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

            /* The route accepts 0 to 3 (V196 widened it from 2) and refuses 4 and -1. */
            foreach (var level in new[] { "NULL", "0", "1", "2", "3" })
            {
                await ExecAsync(store, $"INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('m{level}', {level})", ct);
            }

            foreach (var level in new[] { "4", "-1" })
            {
                var refusal = await Assert.ThrowsAsync<PostgresException>(() =>
                    ExecAsync(store, $"INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('bad{level}', {level})", ct));
                Assert.Equal("ck_config_notification_routes_agent", refusal.ConstraintName);
            }

            /* The request accepts 1 to 3 and refuses 0 and 4. */
            await InsertRequestAsync(store, "11111111-1111-1111-1111-111111111111", 1, ct: ct);
            await InsertRequestAsync(store, "22222222-2222-2222-2222-222222222222", 2, ct: ct);
            foreach (var level in new[] { 0, 4 })
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

            /* The value is double precision: a fractional threshold (a deadlock rate, a hold ratio) keeps its fraction. */
            await ExecAsync(store, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (5909, 'sales', 'deadlock.warn_per_hour', 7.5, 'agent-1')", ct);
            Assert.Equal("7.5", await ScalarAsync(store, "SELECT value FROM config.config_threshold_overrides WHERE threshold_key = 'deadlock.warn_per_hour'", ct));
            await ExecAsync(store, "DELETE FROM config.config_threshold_overrides WHERE threshold_key = 'deadlock.warn_per_hour'", ct);

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
            /* The family's identity shape and the report's verdict domain: a spaced or empty author, an unknown corrected verdict and an
               over-long text are refused by name; a different case of an author who already answered is the same author. */
            foreach (var bad in new[] { "'erik '", "''", "'a b'", "'" + new string('x', 65) + "'" })
            {
                var badAuthor = await Assert.ThrowsAsync<PostgresException>(() =>
                    ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('{Request}', 'right', {bad})", ct));
                Assert.Equal("ck_investigation_feedback_author", badAuthor.ConstraintName);
            }

            var sameAuthor = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('{Request}', 'wrong', 'Erik')", ct));
            Assert.Equal("uq_investigation_feedback_author", sameAuthor.ConstraintName);
            var badCorrected = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, corrected_verdict, author) VALUES ('{Request}', 'wrong', 'needs-person', 'lee')", ct));
            Assert.Equal("ck_investigation_feedback_corrected", badCorrected.ConstraintName);
            var longStep = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, missed_step, author) VALUES ('{Request}', 'wrong', repeat('x', 4001), 'lee')", ct));
            Assert.Equal("ck_investigation_feedback_missed_step", longStep.ConstraintName);
            var longComment = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, comment, author) VALUES ('{Request}', 'wrong', repeat('x', 4001), 'lee')", ct));
            Assert.Equal("ck_investigation_feedback_comment", longComment.ConstraintName);
            await ExecAsync(store, $"INSERT INTO config.investigation_feedback (request_id, verdict, missed_step, comment, author) VALUES ('{Request}', 'wrong', repeat('x', 4000), repeat('y', 4000), 'lee')", ct);
            await ExecAsync(store, $"DELETE FROM config.investigation_feedback WHERE author = 'lee'", ct);

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
            await RungRewindArtifacts.DropSentinelsAboveAsync(store, Rung.Version - 1, ct);
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
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store, "UPDATE config.config_notification_routes SET agent_level = 4 WHERE metric_match = 'kept'", ct));

            /* A second run changes nothing: the checks keep their catalog identity and the script raises nothing. V196 has widened the
               checks since, so the rung that runs again is the one that owns their shape now (V191's own guard would narrow them back). */
            const string Identity = "SELECT string_agg(c.oid::text || ':' || c.conname, ',' ORDER BY c.conname) FROM pg_constraint c "
                + "WHERE c.conname IN ('ck_config_notification_routes_agent', 'ck_investigation_requests_autonomy')";
            var before = await ScalarAsync(store, Identity, ct);
            await PgMigrations.MigrateAsync(store, ct);
            await ExecAsync(store, PgMigrations.Scripts.Single(m => m.Name == "level-3-server-action-switches").Sql, ct);
            Assert.Equal(before, await ScalarAsync(store, Identity, ct));

            /* The guard reads pg_get_constraintdef text (!~ 'ARRAY\[0, 1, 2, 3\]' and 'ARRAY\[1, 2, 3\]'). The columns are smallint, and
               this is the rendering they produce: no explicit casts inside the array, so the guard recognises its own
               constraint (the identity check above is the same fact, from the other side). */
            Assert.Matches(@"ARRAY\[0, 1, 2, 3\]", await ScalarAsync(store, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'ck_config_notification_routes_agent'", ct));
            Assert.Matches(@"ARRAY\[1, 2, 3\]", await ScalarAsync(store, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'ck_investigation_requests_autonomy'", ct));
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
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
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
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];

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
        var exist = await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True";
        Assert.SkipWhen(exist, "A cluster-wide admin/viewer/web/mcp role already exists on this rig; the grants would adopt it.");
        return exist;
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
            ("web", "config.config_threshold_overrides", "SELECT"),
            ("web", "config.investigation_feedback", "SELECT"),
            ("mcp", "config.config_threshold_overrides", "SELECT,INSERT,UPDATE,DELETE"),
            ("mcp", "config.investigation_feedback", "SELECT"),
            /* V196 (#5632): the per-server action switches. Every role reads them; none writes them (the store owner does), so no MCP
               tool, web route or login the service hands out can turn one on. */
            ("admin", "config.config_server_actions", "SELECT"),
            ("viewer", "config.config_server_actions", "SELECT"),
            ("web", "config.config_server_actions", "SELECT"),
            ("mcp", "config.config_server_actions", "SELECT"),
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
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct, emptyStore: true);
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
            await ExerciseLoginsAsync(owner, scratch, ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.ViewerPassword, ProvisioningTestSecrets.McpPassword, ct);
            await RunTheGateAsMcpAsync(owner, scratch, ProvisioningTestSecrets.McpPassword, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// A store provisioned by the shipped <c>provision-roles.sql</c> (run whole, its database-level statements retargeted at the
    /// scratch store): on a store whose Level 2 tables are not there yet the script runs clean and grants nothing on them; after
    /// the rung it grants the same matrix as managed provisioning, and the real admin, viewer and mcp logins it made exercise it.
    /// </summary>
    [Fact]
    public async Task TheBringYourOwnScript_GrantsTheSameMatrix_AndSkipsOnAStoreBehindTheRung()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct, emptyStore: true);
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

            const string AdminPassword = "ScriptAdmin5909", ViewerPassword = "ScriptViewer5909", WebPassword = "ScriptWeb5909", McpPassword = "ScriptMcp5909";
            var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                    System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                .Replace("CHANGE_ME_ADMIN_PASSWORD", AdminPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", ViewerPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_WEB_PASSWORD", WebPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", McpPassword, StringComparison.Ordinal);

            /* Behind the rung (the feedback table is absent): the script warns, skips the block and grants nothing on the overrides. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(owner, 190, ct);
            await ExecAsync(owner, script, ct);
            Assert.Equal("4", await ScalarAsync(owner, "SELECT count(*) FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp')", ct));

            /* #6170: the store's password-rules trigger and the edit function the script made must also run on a store that does not
               have the V203 columns yet (a script run before the service migrated it): a web add and a web edit both go through. */
            Assert.Equal("False", await ScalarAsync(owner,
                "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'config.config_monitored_servers'::regclass AND attname = 'hypothetical_index_username' AND NOT attisdropped)", ct));
            await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(owner, "SET ROLE web", ct);
            await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (5911, 'behind-v203', 'host-b')", ct);
            Assert.Equal("saved", await ScalarAsync(owner,
                "SELECT outcome FROM config.edit_monitored_server(5911, (SELECT modified_at FROM config.config_monitored_servers WHERE server_id = 5911), "
                + "ARRAY['name'], 'behind-v203-renamed', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", ct));
            await ExecAsync(owner, "RESET ROLE", ct);
            await ExecAsync(owner, "DELETE FROM config.config_monitored_servers WHERE server_id = 5911", ct);

            /* The rung lands (the migrator recreates the missing table), and a re-run of the script grants the matrix. */
            await ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version >= " + PgMigrations.Scripts.Single(m => m.Name == "agent-level-2-store").Version.ToString(CultureInfo.InvariantCulture), ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await ExecAsync(owner, script, ct);
            await ExecAsync(owner, script, ct);
            await AssertMatrixAsync(owner, ct);
            await ExerciseLoginsAsync(owner, scratch, AdminPassword, ViewerPassword, McpPassword, ct);
            await RunTheGateAsMcpAsync(owner, scratch, McpPassword, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// #5912 review round 1, changed by #6569: the gate's own statements against the real roles, whichever grant source made them. The
    /// MCP host connects as <c>mcp</c>, but the journal keeps <c>action_called</c> and <c>action_done</c> to the owner, so the tools hand the
    /// store the owner's data source and the whole gate runs on it: a call on <c>mcp</c>'s own data source is refused at <c>action_called</c>
    /// (42501) and changes nothing. A pass on the owner's journals both rows and a refusal (the live route sends the metric at Level 1)
    /// journals one refused row.
    /// </summary>
    private static async Task RunTheGateAsMcpAsync(NpgsqlConnection owner, ScratchPostgres scratch, string mcpPassword, CancellationToken ct)
    {
        var t0 = DateTime.UtcNow;
        var ownerStore = new InvestigationRequestStore(NpgsqlDataSource.Create(scratch.ConnectionString), 30);
        var opened = await ownerStore.OpenOrJoinAsync(
            new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", t0, Array.Empty<string>(), null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 2 }, ct);
        var request = opened.RequestId!.Value;

        var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = "mcp", Password = mcpPassword }.ConnectionString;
        await using var mcpSource = NpgsqlDataSource.Create(mcpString);
        var mcpStore = new InvestigationRequestStore(mcpSource, 30);
        /* #6569: the gate runs on the owner's data source, the way the MCP tools hand it one; claim stays mcp's own. */
        await using var ownerSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var gateStore = new InvestigationRequestStore(mcpSource, 30, agentActionSource: ownerSource);
        var claim = (await mcpStore.ClaimAsync(request, "agent-a", ct)).ClaimId!.Value;
        var writes = 0;
        AgentActionCall Call() => new("create_mute_rule", "mute", new AgentAction(request, claim, "agent-a", "known batch",
            "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"12\"}]"), new AgentActionTarget(7, "srv-7", null, null));
        AgentActionHooks Hooks(int level) => new()
        {
            LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => level),
            ReadBefore = (_, _, _) => Task.FromResult<string?>(null),
            Write = (_, _, _) => { writes++; return Task.FromResult<string?>("{}"); },
        };

        /* The same call on mcp's own data source stops at action_called, which the journal's trigger keeps to the owner: nothing is written. */
        var alone = await Assert.ThrowsAsync<PostgresException>(() => mcpStore.RunAgentActionAsync(Call(), Hooks(2), ct));
        Assert.Equal("42501", alone.SqlState);
        Assert.Equal(0, writes);

        var done = await gateStore.RunAgentActionAsync(Call(), Hooks(2), ct);
        Assert.True(done.Done, done.Refusal?.Message);
        Assert.Equal(1, writes);
        Assert.Equal("action_called|action_done", await ScalarAsync(owner,
            $"SELECT string_agg(event, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE request_id = '{request}' AND event LIKE 'action_%'", ct));

        var refused = await gateStore.RunAgentActionAsync(Call(), Hooks(1), ct);
        Assert.Equal(AgentActionCodes.NotLevel2, refused.Refusal?.Reason);
        Assert.Equal(1, writes);
        Assert.Equal("1", await ScalarAsync(owner, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'refused'", ct));
    }

    /// <summary>As the real logins, for each role and each new table: mcp inserts, updates and deletes overrides (the beacon trigger runs
    /// as mcp) and reads but cannot write feedback; viewer reads both and cannot insert, update or delete either; admin writes both.
    /// A refused verb is 42501.</summary>
    private static async Task ExerciseLoginsAsync(
        NpgsqlConnection owner, ScratchPostgres scratch, string adminPassword, string viewerPassword, string mcpPassword, CancellationToken ct)
    {
        await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
        await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (5909, 'srv-a', 'host-a')", ct);
        await ExecAsync(owner, "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at) "
            + "VALUES ('66666666-6666-6666-6666-666666666666', 1, 5909, 'srv-a', 'blocking', 'blocking', now(), now(), now(), now() + interval '1 hour')", ct);

        await using (var mcp = await LoginAsync(scratch, "mcp", mcpPassword, ct))
        {
            var version = long.Parse(await ScalarAsync(owner, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture);
            await ExecAsync(mcp, "INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (5909, 'blocking.seconds', 7.5, 'mcp-agent')", ct);
            await ExecAsync(mcp, "UPDATE config.config_threshold_overrides SET value = 12.25, updated_at = now() WHERE threshold_key = 'blocking.seconds'", ct);
            Assert.Equal("12.25", await ScalarAsync(mcp, "SELECT value FROM config.config_threshold_overrides WHERE threshold_key = 'blocking.seconds'", ct));
            Assert.True(long.Parse(await ScalarAsync(owner, "SELECT config_version FROM config.config_service", ct), CultureInfo.InvariantCulture) > version, "mcp's write reaches the beacon");
            await ExecAsync(mcp, "DELETE FROM config.config_threshold_overrides WHERE threshold_key = 'blocking.seconds'", ct);

            /* #6400: record_investigation_feedback runs as mcp, so mcp holds INSERT on the six feedback columns and UPDATE on the six it replaces.
               Nothing wider: not the request, and no DELETE. */
            Assert.Equal("0", await ScalarAsync(mcp, "SELECT count(*) FROM config.investigation_feedback", ct));
            await ExecAsync(mcp, "INSERT INTO config.investigation_feedback (request_id, verdict, corrected_verdict, missed_step, comment, author) VALUES ('66666666-6666-6666-6666-666666666666', 'right', NULL, NULL, 'c', 'mcp-agent')", ct);
            await ExecAsync(mcp, "UPDATE config.investigation_feedback SET verdict = 'wrong', corrected_verdict = NULL, missed_step = NULL, comment = 'edited', author = 'mcp-agent', created_at = now() WHERE author = 'mcp-agent'", ct);
            Assert.Equal("wrong", await ScalarAsync(mcp, "SELECT verdict FROM config.investigation_feedback WHERE author = 'mcp-agent'", ct));
            await AssertDeniedAsync(mcp, "UPDATE config.investigation_feedback SET request_id = gen_random_uuid()", ct);
            await AssertDeniedAsync(mcp, "DELETE FROM config.investigation_feedback", ct);

            /* V196 (#5632): the switch table is read by every role and written by none of them, so a role cannot turn an action on. */
            Assert.Equal("0", await ScalarAsync(mcp, "SELECT count(*) FROM config.config_server_actions", ct));
            await AssertDeniedAsync(mcp, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (5909, 'force_plan', true, 'mcp')", ct);
            await AssertDeniedAsync(mcp, "UPDATE config.config_server_actions SET enabled = true", ct);
            await AssertDeniedAsync(mcp, "DELETE FROM config.config_server_actions", ct);

            /* #6569: the rows of an agent action are the owner's alone, so mcp's own insert of any of them is refused. */
            foreach (var journalEvent in AgentActionEvents.OwnerOnly)
                await AssertDeniedAsync(mcp, AgentActionRowSql(journalEvent), ct);
        }

        await ExecAsync(owner, "DELETE FROM config.investigation_feedback", ct);
        await ExecAsync(owner, "INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (5909, 'cpu.percent', 80, 'owner')", ct);
        await ExecAsync(owner, "INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('66666666-6666-6666-6666-666666666666', 'right', 'erik')", ct);
        await using (var viewer = await LoginAsync(scratch, "viewer", viewerPassword, ct))
        {
            Assert.Equal("1", await ScalarAsync(viewer, "SELECT count(*) FROM config.config_threshold_overrides", ct));
            Assert.Equal("1", await ScalarAsync(viewer, "SELECT count(*) FROM config.investigation_feedback", ct));
            await AssertDeniedAsync(viewer, "INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (5909, 'x', 1, 'viewer')", ct);
            await AssertDeniedAsync(viewer, "UPDATE config.config_threshold_overrides SET value = 1", ct);
            await AssertDeniedAsync(viewer, "DELETE FROM config.config_threshold_overrides", ct);
            await AssertDeniedAsync(viewer, "INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('66666666-6666-6666-6666-666666666666', 'right', 'viewer')", ct);
            await AssertDeniedAsync(viewer, "UPDATE config.investigation_feedback SET verdict = 'wrong'", ct);
            await AssertDeniedAsync(viewer, "DELETE FROM config.investigation_feedback", ct);

            /* V196 (#5632): the switch table is read by every role and written by none of them, so a role cannot turn an action on. */
            Assert.Equal("0", await ScalarAsync(viewer, "SELECT count(*) FROM config.config_server_actions", ct));
            await AssertDeniedAsync(viewer, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (5909, 'force_plan', true, 'viewer')", ct);
            await AssertDeniedAsync(viewer, "UPDATE config.config_server_actions SET enabled = true", ct);
            await AssertDeniedAsync(viewer, "DELETE FROM config.config_server_actions", ct);
        }

        await using (var admin = await LoginAsync(scratch, "admin", adminPassword, ct))
        {
            await ExecAsync(admin, "INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (5909, 'memory.percent', 70, 'admin')", ct);
            await ExecAsync(admin, "UPDATE config.config_threshold_overrides SET value = 85 WHERE threshold_key = 'cpu.percent'", ct);
            await ExecAsync(admin, "INSERT INTO config.investigation_feedback (request_id, verdict, author) VALUES ('66666666-6666-6666-6666-666666666666', 'wrong', 'admin')", ct);
            await ExecAsync(admin, "UPDATE config.investigation_feedback SET comment = 'edited' WHERE author = 'admin'", ct);
            await ExecAsync(admin, "DELETE FROM config.investigation_feedback WHERE author = 'admin'", ct);
            await ExecAsync(admin, "DELETE FROM config.config_threshold_overrides", ct);

            /* V196 (#5632): the switch table is read by every role and written by none of them, so a role cannot turn an action on. */
            Assert.Equal("0", await ScalarAsync(admin, "SELECT count(*) FROM config.config_server_actions", ct));
            await AssertDeniedAsync(admin, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (5909, 'force_plan', true, 'admin')", ct);
            await AssertDeniedAsync(admin, "UPDATE config.config_server_actions SET enabled = true", ct);
            await AssertDeniedAsync(admin, "DELETE FROM config.config_server_actions", ct);

            /* #6569: nor does admin (the Viewer's login) write the rows of an agent action. */
            foreach (var journalEvent in AgentActionEvents.OwnerOnly)
                await AssertDeniedAsync(admin, AgentActionRowSql(journalEvent), ct);
        }
    }

    /// <summary>One journal row of the given event on the request <see cref="ExerciseLoginsAsync"/> makes, by an agent actor.</summary>
    private static string AgentActionRowSql(string journalEvent) =>
        "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor) "
        + $"VALUES (now(), '66666666-6666-6666-6666-666666666666', 5909, '{journalEvent}', 'agent:other')";
}
