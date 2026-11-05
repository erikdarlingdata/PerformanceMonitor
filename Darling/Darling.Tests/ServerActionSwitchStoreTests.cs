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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live fact mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the store rung for Level 3 and the per-server action switches (V196, step 5 of #5632): its place at the top of the ladder,
/// the two level CHECKs now allowing 3, the switch table (no row on a new store, so every action reads off; one row per server and
/// action; a fixed action list; deleted with its server; a reload beacon), and a second run changing nothing. Tests name the rung by
/// its name, never its number.
/// </summary>
public sealed class ServerActionSwitchStoreTests
{
    private const string RungName = "level-3-server-action-switches";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    [Fact]
    public void TheRungIsRegistered_AndTheViewerGateRequiresTheLadderTop()
    {
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        /* Not the top any more: V197 (the plan-force check link) sits above it. */
        Assert.Equal(196, Rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
    }

    [Fact]
    public void TheRungInsertsNoRow()
    {
        /* "No row means off" holds only if the migration seeds nothing: no INSERT in the text, and no DEFAULT that turns a row on. */
        Assert.DoesNotMatch(@"(?i)\bINSERT\s+INTO\b", Rung.Sql);
        Assert.Contains("enabled boolean NOT NULL DEFAULT false", Rung.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ANewStore_HasNoSwitchRow_SoEveryActionReadsOff_AndTheLevelChecksAllowThree()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            Assert.Equal("0", await ScalarAsync(store, "SELECT count(*) FROM config.config_server_actions", ct));

            await ExecAsync(store, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (5632, 'srv-a', 'host-a'), (5633, 'srv-b', 'host-b')", ct);
            await ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5632, 'force_plan', 'erik')", ct);
            Assert.Equal("False", await ScalarAsync(store, "SELECT enabled FROM config.config_server_actions WHERE server_id = 5632 AND action = 'force_plan'", ct));

            /* One row per server and action. */
            var duplicate = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5632, 'force_plan', 'erik')", ct));
            Assert.Equal("23505", duplicate.SqlState);

            /* A fixed action list, a named author, and a server that exists. */
            var unknown = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5632, 'drop_database', 'erik')", ct));
            Assert.Equal("ck_config_server_actions_action", unknown.ConstraintName);
            var anonymous = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5632, 'kill_session', ' ')", ct));
            Assert.Equal("ck_config_server_actions_set_by", anonymous.ConstraintName);
            var orphan = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (9999, 'kill_session', 'erik')", ct));
            Assert.Equal("23503", orphan.SqlState);

            /* A change moves the reload beacon, as every config change does. */
            var version = await ScalarAsync(store, "SELECT config_version FROM config.config_service", ct);
            await ExecAsync(store, "UPDATE config.config_server_actions SET enabled = true, updated_at = now() WHERE server_id = 5632", ct);
            Assert.NotEqual(version, await ScalarAsync(store, "SELECT config_version FROM config.config_service", ct));

            /* Deleted with its server. */
            await ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5633, 'evict_plan', 'erik')", ct);
            await ExecAsync(store, "DELETE FROM config.config_monitored_servers WHERE server_id = 5632", ct);
            Assert.Equal("1", await ScalarAsync(store, "SELECT count(*) FROM config.config_server_actions", ct));

            /* Level 3 is stored by a route and by a request; 4 is not. */
            await ExecAsync(store, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('l3', 3)", ct);
            var four = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('l4', 4)", ct));
            Assert.Equal("ck_config_notification_routes_agent", four.ConstraintName);
            await InsertRequestAsync(store, "33333333-3333-3333-3333-333333333333", 3, ct);
            var requestFour = await Assert.ThrowsAsync<PostgresException>(() => InsertRequestAsync(store, Guid.NewGuid().ToString(), 4, ct));
            Assert.Equal("ck_investigation_requests_autonomy", requestFour.ConstraintName);
        });
    }

    [Fact]
    public async Task AnUpgradedStore_KeepsItsRows_AddsNoSwitchRow_AndASecondRunChangesNothing()
    {
        await WithStoreAsync(async (store, ct) =>
        {
            await RungRewindArtifacts.DropSentinelsAboveAsync(store, Rung.Version - 1, ct);
            await ExecAsync(store, "DELETE FROM darling_schema_version WHERE version >= " + Rung.Version.ToString(CultureInfo.InvariantCulture), ct);
            await ExecAsync(store, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('kept', 2)", ct);
            await InsertRequestAsync(store, "55555555-5555-5555-5555-555555555555", 2, ct);

            /* One rung back: level 3 is refused and the table is gone. */
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store, "UPDATE config.config_notification_routes SET agent_level = 3 WHERE metric_match = 'kept'", ct));
            Assert.Null(await ScalarAsync(store, "SELECT to_regclass('config.config_server_actions')::text", ct));

            await PgMigrations.MigrateAsync(store, ct);
            Assert.Equal(StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture), await ScalarAsync(store, "SELECT max(version) FROM darling_schema_version", ct));
            Assert.Equal("0", await ScalarAsync(store, "SELECT count(*) FROM config.config_server_actions", ct));
            Assert.Equal("2", await ScalarAsync(store, "SELECT agent_level FROM config.config_notification_routes WHERE metric_match = 'kept'", ct));
            Assert.Equal("2", await ScalarAsync(store, "SELECT autonomy_level FROM config.investigation_requests WHERE request_id = '55555555-5555-5555-5555-555555555555'", ct));
            await ExecAsync(store, "UPDATE config.config_notification_routes SET agent_level = 3 WHERE metric_match = 'kept'", ct);

            /* A second run replaces no constraint and re-makes no row. */
            const string Identity = "SELECT string_agg(c.oid::text || ':' || c.conname, ',' ORDER BY c.conname) FROM pg_constraint c "
                + "WHERE c.conname IN ('ck_config_notification_routes_agent', 'ck_investigation_requests_autonomy', 'ck_config_server_actions_action')";
            var before = await ScalarAsync(store, Identity, ct);
            await PgMigrations.MigrateAsync(store, ct);
            await ExecAsync(store, Rung.Sql, ct);
            Assert.Equal(before, await ScalarAsync(store, Identity, ct));
            Assert.Equal("1", await ScalarAsync(store, "SELECT count(*) FROM pg_trigger WHERE tgname = 'trg_bump_server_actions' AND NOT tgisinternal", ct));
            Assert.Matches(@"ARRAY\[0, 1, 2, 3\]", await ScalarAsync(store, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'ck_config_notification_routes_agent'", ct));
            Assert.Matches(@"ARRAY\[1, 2, 3\]", await ScalarAsync(store, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'ck_investigation_requests_autonomy'", ct));
        });
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the server action switch store pins.");
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

    private static Task InsertRequestAsync(NpgsqlConnection store, string requestId, int level, CancellationToken ct) =>
        ExecAsync(store,
            "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at, "
            + $"state, autonomy_level, opened_at) VALUES ('{requestId}', 1, 5632, 'srv-a', 'blocking', 'blocking', now(), now(), now() + interval '30 minutes', now() + interval '4 hours', "
            + $"'open', {level.ToString(CultureInfo.InvariantCulture)}, now())", ct);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }
}
