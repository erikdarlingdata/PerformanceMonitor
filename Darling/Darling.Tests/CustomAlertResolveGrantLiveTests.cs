/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3334 gated live proof (DARLING_TEST_PG): the resolve-on-delete recovery row is written even when the delete
/// runs on the LEAST-PRIVILEGE pool. This is what #3305's tests missed — they connected as the owner (which has
/// INSERT on all of config), so the viewer/mcp grant gap on <c>config_alert_log</c> never surfaced. These tests
/// connect as a DISPOSABLE low-privilege role (viewer/mcp-shaped: SELECT + a narrow custom_alert_rules write +
/// EXECUTE on the SECURITY DEFINER function, and pointedly NO direct <c>config_alert_log</c> INSERT) and assert
/// that both teardown paths — the caller-initiated delete AND the sweep reconcile — record the
/// <c>&lt;name&gt; Resolved</c> row through the definer function. A direct-INSERT denial is asserted first, so a
/// present row can only have come from the function, not an ambient grant.
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertResolveGrantLiveTests
{
    private const string LowPrivRole = "sec_resolve_test";
    private const string RolePassword = "ResolveGrantTestPw0123456789abcd"; // alnum, like the real generator

    private const string SampleDefinition =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":0.25},\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}";

    private sealed class NoopDeliverer : IAlertDeliverer
    {
        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser, can CREATE ROLE) to run the resolve-grant live tests.");
        return connectionString!;
    }

    private static async Task<NpgsqlDataSource> MigrateAndOpenAsync(string connectionString, CancellationToken ct)
    {
        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;

        return NpgsqlDataSource.Create(dataSourceConnectionString);
    }

    /// <summary>Creates the SECURITY DEFINER function (from the SHARED builder, so it is the real one) and a
    /// disposable low-privilege role granted the viewer/mcp-shaped subset a rule-delete needs — SELECT, the
    /// narrow custom_alert_rules write, and EXECUTE on the function — but NO direct config_alert_log INSERT.</summary>
    private static async Task ProvisionFunctionAndLowPrivRoleAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        var ddl = DarlingManagedRoles.BuildCustomAlertResolveFunctionSql("config") + $@"
DO $do$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{LowPrivRole}') THEN
      CREATE ROLE {LowPrivRole} LOGIN NOSUPERUSER PASSWORD '{RolePassword}';
   END IF;
END $do$;
GRANT USAGE ON SCHEMA collect, config TO {LowPrivRole};
GRANT SELECT ON ALL TABLES IN SCHEMA collect TO {LowPrivRole};
GRANT SELECT ON ALL TABLES IN SCHEMA config  TO {LowPrivRole};
GRANT INSERT, UPDATE, DELETE ON config.custom_alert_rules TO {LowPrivRole};
GRANT EXECUTE ON FUNCTION config.record_custom_alert_resolution(integer, text, text, text) TO {LowPrivRole};";
        await using var command = owner.CreateCommand(ddl);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static NpgsqlDataSource OpenLowPriv(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            Username = LowPrivRole,
            Password = RolePassword,
            SearchPath = "collect,config,public",
            Pooling = false,
        }.ConnectionString);

    private static CustomAlertRuleState Firing(int ruleVersion)
    {
        var evaluatedAt = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);
        return new CustomAlertRuleState(
            new PersistenceState(ConsecutiveBreaches: 3, ConsecutiveClears: 0, Firing: true),
            ruleVersion, "Critical", evaluatedAt, evaluatedAt.AddSeconds(60));
    }

    private static async Task<(int Count, bool AlertSent, string? NotificationType, bool Muted)> ReadResolveRowAsync(
        NpgsqlDataSource dataSource, string metricName, int serverId, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            "SELECT count(*), bool_or(alert_sent), min(notification_type), bool_or(muted) FROM config_alert_log WHERE metric_name = $1 AND server_id = $2");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = metricName });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        var count = Convert.ToInt32(reader.GetValue(0));
        return count == 0
            ? (0, false, null, false)
            : (count, reader.GetBoolean(1), reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetBoolean(3));
    }

    [Fact]
    public async Task Delete_AsLowPrivilegeRole_WritesResolveRow_ThroughTheDefinerFunction()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await MigrateAndOpenAsync(connectionString, ct);

        var rules = new CustomAlertRuleStore(owner);
        var state = new CustomAlertStateStore(owner);
        var ruleName = "car_grant_del_" + Guid.NewGuid().ToString("N");
        const int serverId = 991334;
        var bodySucceeded = false;
        try
        {
            await ProvisionFunctionAndLowPrivRoleAsync(owner, ct);

            /* Set up an OPEN incident as the owner (seeding custom_alert_state needs owner — viewer/mcp cannot
               write it, exactly as in production). */
            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(ruleName, null, SampleDefinition, true, "test", ct));
            var ruleId = created.Rule!.Id;
            await state.SaveAsync(ruleId, serverId, Firing(created.Rule!.Version), ct);

            await using var lowPriv = OpenLowPriv(connectionString);

            /* The role genuinely CANNOT write config_alert_log directly — 42501 — so a resolve row appearing
               after the delete can only have come through the SECURITY DEFINER function. */
            await using (var directInsert = lowPriv.CreateCommand(
                "INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json) " +
                "VALUES ((now() AT TIME ZONE 'UTC'), $1, 'x', 'direct-write-probe', 0, 0, false, 'none', NULL, false, NULL, NULL)"))
            {
                directInsert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                var denied = await Assert.ThrowsAsync<PostgresException>(async () => await directInsert.ExecuteNonQueryAsync(ct));
                Assert.Equal("42501", denied.SqlState); // insufficient_privilege
            }

            /* The delete path, run entirely on the low-privilege pool (mirrors the deployed mcp delete tool and
               the viewer web DELETE). Pre-fix, the resolve INSERT 42501'd, was swallowed, and the row was lost. */
            Assert.IsType<CustomAlertRuleResult.Ok>(
                await CustomAlertEvaluator.ResolveAndDeleteRuleAsync(lowPriv, ruleId, logger: null, ct));

            /* The recovery row IS present now, and is a proper no-channel resolution (alert_sent false,
               notification_type 'none', unmuted) — shape-identical to a normal resolve. */
            var row = await ReadResolveRowAsync(owner, ruleName + " Resolved", serverId, ct);
            Assert.Equal(1, row.Count);
            Assert.False(row.AlertSent);
            Assert.Equal("none", row.NotificationType);
            Assert.False(row.Muted);

            /* And the delete itself completed: rule gone, state cascaded away. */
            Assert.IsType<CustomAlertRuleResult.NotFound>(await rules.GetAsync(ruleId, ct));
            Assert.Empty(await state.ListForRuleAsync(ruleId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupAsync(cleanup, ruleName, serverId, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task ReconcileTeardown_WithLowPrivilegeHistoryPool_WritesResolveRow_ThroughTheDefinerFunction()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var owner = await MigrateAndOpenAsync(connectionString, ct);

        var rules = new CustomAlertRuleStore(owner);
        var state = new CustomAlertStateStore(owner);
        var ruleName = "car_grant_rec_" + Guid.NewGuid().ToString("N");
        const int serverId = 991335;
        var bodySucceeded = false;
        try
        {
            await ProvisionFunctionAndLowPrivRoleAsync(owner, ct);

            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(ruleName, null, SampleDefinition, true, "test", ct));
            var ruleId = created.Rule!.Id;
            await state.SaveAsync(ruleId, serverId, Firing(created.Rule!.Version), ct);

            /* Disable the rule so the reconcile sweep force-resolves its now-orphaned firing incident. */
            var disabled = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.UpdateAsync(ruleId, ruleName, null, SampleDefinition, enabled: false, created.Rule!.Version, "test", ct));
            Assert.False(disabled.Rule!.Enabled);

            var monitored = new Dictionary<int, (string StorageName, string DisplayName)> { [serverId] = ("syn_storage", "SynServer") };

            /* The evaluator's HISTORY writer is on the low-privilege pool — the seam under test — while its rule
               and state stores stay on the owner pool (reconcile's state cleanup DELETEs custom_alert_state, an
               owner-only write, exactly as in production). The teardown resolution must still land via the
               definer function. */
            await using var lowPriv = OpenLowPriv(connectionString);
            var evaluator = new CustomAlertEvaluator(
                rules, state, owner /* viewer: unused by reconcile */, new NoopDeliverer(),
                isAlertMuted: null, new PgAlertHistoryStore(lowPriv), defaultIntervalSeconds: 60,
                cacheTtl: TimeSpan.Zero, NullLogger.Instance);

            await evaluator.ReconcileStateAsync(monitored, ct);

            var row = await ReadResolveRowAsync(owner, ruleName + " Resolved", serverId, ct);
            Assert.Equal(1, row.Count);
            Assert.False(row.AlertSent);
            Assert.Equal("none", row.NotificationType);

            /* The incident was cleaned but the (disabled) rule row remains. */
            Assert.Empty(await state.ListForRuleAsync(ruleId, ct));
            Assert.IsType<CustomAlertRuleResult.Ok>(await rules.GetAsync(ruleId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupAsync(cleanup, ruleName, serverId, cleanupCt);
            });
        }
    }

    private static async Task CleanupAsync(NpgsqlConnection cleanup, string ruleName, int serverId, CancellationToken ct)
    {
        using (var rule = new NpgsqlCommand("DELETE FROM config.custom_alert_rules WHERE name = $1", cleanup))
        {
            rule.Parameters.AddWithValue(ruleName);
            await rule.ExecuteNonQueryAsync(ct);
        }

        using (var log = new NpgsqlCommand("DELETE FROM config.config_alert_log WHERE server_id = $1", cleanup))
        {
            log.Parameters.AddWithValue(serverId);
            await log.ExecuteNonQueryAsync(ct);
        }

        /* DROP OWNED BY revokes the role's grants (incl. the function EXECUTE) so DROP ROLE has no dependents.
           The function itself is owned by the store owner, not this role, so it is left in place (it is the real
           provisioned object). DROP OWNED BY has no IF EXISTS, so tolerate 42704 when the role never existed. */
        using (var dropOwned = new NpgsqlCommand($"DROP OWNED BY {LowPrivRole}", cleanup))
        {
            try
            {
                await dropOwned.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState == "42704")
            {
            }
        }

        using var dropRole = new NpgsqlCommand($"DROP ROLE IF EXISTS {LowPrivRole}", cleanup);
        await dropRole.ExecuteNonQueryAsync(ct);
    }
}
