/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Gated (DARLING_TEST_PG) proof that the V117 trigger is really installed and really fires — the MECHANISM
/// (<c>config_version</c> advanced) rather than a delivery outcome, so it is red the day the trigger leaves
/// the ladder and cannot be satisfied by anything else in the alert path.
///
/// <para>Two tests, discriminating different things. The first writes the three statement kinds itself inside
/// a transaction it ROLLS BACK, so it pins the trigger's event list against a store it leaves untouched. The
/// second goes through <see cref="PgMuteRuleStore"/> — the store the MCP tools construct and the service
/// holds — because those statements name <c>config_mute_rules</c> UNQUALIFIED and resolve through
/// <c>search_path</c>: a resolution that landed anywhere but <c>config</c> would leave the first test green
/// and the cache stale, which is precisely #3315's failure.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class MuteRuleReloadBeaconLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the mute-rule reload-beacon trigger test.";

    [Fact]
    public async Task EveryMuteRuleWriteKind_BumpsTheReloadBeacon_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Everything inside a transaction that ROLLS BACK — the shared store's singleton config_service row
           and its mute rules are left exactly as they were. Names are schema-qualified so resolution is
           search_path-independent (this session's own path is irrelevant to what is being asserted).
           Mirrors StoreConfigProviderTests' V17 bump-trigger test, which is the same claim one rung down. */
        await using var tx = await connection.BeginTransactionAsync(ct);

        await ExecAsync(connection, tx, ct,
            "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

        var ruleId = "mute_beacon_e2e_" + Guid.NewGuid().ToString("N");
        var before = await BeaconAsync(connection, tx, ct);

        await ExecAsync(connection, tx, ct,
            @"INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason)
VALUES ($1, TRUE, now() AT TIME ZONE 'UTC', 'reload-beacon trigger test')", ruleId);
        var afterInsert = await BeaconAsync(connection, tx, ct);
        Assert.True(afterInsert > before,
            "config_version must bump on a config_mute_rules INSERT — without it a created mute is persisted "
            + "and inert until an unrelated write or a service restart reloads the cache (#3315).");

        await ExecAsync(connection, tx, ct,
            "UPDATE config.config_mute_rules SET enabled = FALSE WHERE id = $1", ruleId);
        var afterUpdate = await BeaconAsync(connection, tx, ct);
        Assert.True(afterUpdate > afterInsert,
            "config_version must bump on a config_mute_rules UPDATE — disabling a rule is how the Manage Mute "
            + "Rules surface un-mutes without deleting.");

        await ExecAsync(connection, tx, ct, "DELETE FROM config.config_mute_rules WHERE id = $1", ruleId);
        var afterDelete = await BeaconAsync(connection, tx, ct);
        Assert.True(afterDelete > afterUpdate,
            "config_version must bump on a config_mute_rules DELETE — the dangerous direction: the operator "
            + "believes alerting is restored while a stale cache keeps suppressing.");

        await tx.RollbackAsync(ct);
    }

    [Fact]
    public async Task PgMuteRuleStoreWrites_ResolveToTheTriggeredTable_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, null, ct,
            "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var store = new PgMuteRuleStore(postgres);
        var rule = new MuteRule
        {
            Enabled = true,
            CreatedAtUtc = DateTime.UtcNow,
            Reason = "mute_beacon_store_e2e_" + Guid.NewGuid().ToString("N"),
            MetricName = "Deadlocks Detected",
        };

        /* NOT transaction-scoped: the store opens its own connections from the data source, which is the
           point — this asserts the production write path, not SQL this test composed. config_version is
           monotonic, so the two bumps it leaves behind are not state anything needs restored; the rule row
           is, and teardown removes it by id. */
        var bodySucceeded = false;
        try
        {
            var before = await BeaconAsync(connection, null, ct);

            await store.InsertAsync(rule);
            var afterInsert = await BeaconAsync(connection, null, ct);
            Assert.True(afterInsert > before,
                "PgMuteRuleStore.InsertAsync names config_mute_rules unqualified and resolves it through "
                + "search_path; the beacon not moving means it resolved somewhere the V117 trigger is not.");

            await store.DeleteAsync(rule.Id);
            var afterDelete = await BeaconAsync(connection, null, ct);
            Assert.True(afterDelete > afterInsert,
                "PgMuteRuleStore.DeleteAsync must reach the triggered table too — delete_mute_rule and "
                + "DeleteExpiredAsync are both this statement.");

            bodySucceeded = true;
        }
        finally
        {
            /* RunAsync, not RunOwnedAsync: it opens its own connection AND sets search_path, and a teardown
               that hand-opens one inherits "$user", public — where an unqualified store table is a 42P01 that
               passes or fails on whether Npgsql's pool happens to hand back an already-migrated session. The
               statement below is qualified anyway, which is the belt to that braces. */
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var command = new NpgsqlCommand("DELETE FROM config.config_mute_rules WHERE id = $1", cleanup);
                command.Parameters.AddWithValue(rule.Id);
                await command.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<long> BeaconAsync(
        NpgsqlConnection connection, NpgsqlTransaction? tx, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT config_version FROM config.config_service WHERE id = 1", connection, tx);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static async Task ExecAsync(
        NpgsqlConnection connection, NpgsqlTransaction? tx, CancellationToken ct, string sql, params object[] args)
    {
        using var command = new NpgsqlCommand(sql, connection, tx);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg);
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
