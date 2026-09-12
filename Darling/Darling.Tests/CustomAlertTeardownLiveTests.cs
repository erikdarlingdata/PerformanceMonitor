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
/// #3305 gated live round-trips (DARLING_TEST_PG): force-resolve + reconcile custom-alert state on rule
/// disable/delete. Deleting a rule cascades its <c>custom_alert_state</c> rows away silently, so the delete path
/// must resolve any open incident (write the recovery row) BEFORE the cascade; disabling neither deletes nor
/// cascades, so a sweep reconcile force-resolves the orphaned incident and cleans its state. These prove both
/// against a real store (the JOIN queries and the FK cascade cannot be exercised without Postgres).
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertTeardownLiveTests
{
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
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the custom-alert teardown live tests.");
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

        var dataSource = NpgsqlDataSource.Create(dataSourceConnectionString);

        // #3334: the teardown resolution row is now written through the SECURITY DEFINER
        // config.record_custom_alert_resolution function -- a PROVISIONING artifact, not a migration. These
        // tests migrate but do not run role provisioning, so create the function here (as the owner, mirroring
        // production where provisioning runs after migration); otherwise ResolveAndDeleteRuleAsync's resolve
        // write would no-op against a missing function and the recovery-row assertions would fail.
        await using (var fn = dataSource.CreateCommand(DarlingManagedRoles.BuildCustomAlertResolveFunctionSql("config")))
        {
            await fn.ExecuteNonQueryAsync(ct);
        }

        return dataSource;
    }

    private static async Task<int> CountAlertLogAsync(NpgsqlDataSource dataSource, string metricName, int serverId, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            "SELECT COUNT(*) FROM config_alert_log WHERE metric_name = $1 AND server_id = $2");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = metricName });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct));
    }

    private static CustomAlertRuleState Firing(int ruleVersion)
    {
        var evaluatedAt = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);
        return new CustomAlertRuleState(
            new PersistenceState(ConsecutiveBreaches: 3, ConsecutiveClears: 0, Firing: true),
            ruleVersion, "Critical", evaluatedAt, evaluatedAt.AddSeconds(60));
    }

    [Fact]
    public async Task Delete_FiringRule_WritesRecoveryRow_ThenCascadesStateAway()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var rules = new CustomAlertRuleStore(dataSource);
        var state = new CustomAlertStateStore(dataSource);

        var ruleName = "car_del_" + Guid.NewGuid().ToString("N");
        const int serverId = 990101;
        var bodySucceeded = false;
        try
        {
            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(ruleName, null, SampleDefinition, true, "test", ct));
            var ruleId = created.Rule!.Id;
            await state.SaveAsync(ruleId, serverId, Firing(created.Rule!.Version), ct);

            // The delete path force-resolves the open incident, then deletes (cascade drops the state).
            Assert.IsType<CustomAlertRuleResult.Ok>(
                await CustomAlertEvaluator.ResolveAndDeleteRuleAsync(dataSource, ruleId, logger: null, ct));

            // A recovery row was written for the firing subject BEFORE the cascade removed its state.
            Assert.Equal(1, await CountAlertLogAsync(dataSource, ruleName + " Resolved", serverId, ct));
            // State is gone (FK cascade) and so is the rule.
            Assert.Empty(await state.ListForRuleAsync(ruleId, ct));
            Assert.IsType<CustomAlertRuleResult.NotFound>(await rules.GetAsync(ruleId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteTestRowsAsync(cleanup, ruleName, serverId, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task Reconcile_DisabledFiringRule_ResolvesAndClearsState_KeepingTheRuleRow()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var rules = new CustomAlertRuleStore(dataSource);
        var state = new CustomAlertStateStore(dataSource);

        var ruleName = "car_dis_" + Guid.NewGuid().ToString("N");
        const int serverId = 990202;
        var bodySucceeded = false;
        try
        {
            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(ruleName, null, SampleDefinition, true, "test", ct));
            var ruleId = created.Rule!.Id;
            await state.SaveAsync(ruleId, serverId, Firing(created.Rule!.Version), ct);

            // Disable the rule (does NOT delete or cascade — the firing state row persists, orphaned).
            var disabled = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.UpdateAsync(ruleId, ruleName, null, SampleDefinition, enabled: false, created.Rule!.Version, "test", ct));
            Assert.False(disabled.Rule!.Enabled);

            // Build the monitored map from the real registry PLUS this test's synthetic server, so a real
            // enabled rule on the store is left in scope (kept) — only the disabled rule is torn down, and by the
            // "disabled" reason rather than "out of scope".
            var monitored = await BuildMonitoredMapAsync(dataSource, ct);
            monitored[serverId] = ("syn_storage", "SynServer");

            var evaluator = new CustomAlertEvaluator(
                rules, state, dataSource /* viewer: unused by reconcile */, new NoopDeliverer(),
                isAlertMuted: null, new PgAlertHistoryStore(dataSource), defaultIntervalSeconds: 60,
                cacheTtl: TimeSpan.Zero /* force a fresh rule read */, NullLogger.Instance);

            await evaluator.ReconcileStateAsync(monitored, ct);

            // The orphaned incident was force-resolved and its state cleaned, but the (disabled) rule row remains.
            Assert.Equal(1, await CountAlertLogAsync(dataSource, ruleName + " Resolved", serverId, ct));
            Assert.Empty(await state.ListForRuleAsync(ruleId, ct));
            Assert.IsType<CustomAlertRuleResult.Ok>(await rules.GetAsync(ruleId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteTestRowsAsync(cleanup, ruleName, serverId, cleanupCt);
            });
        }
    }

    private static async Task<Dictionary<int, (string StorageName, string DisplayName)>> BuildMonitoredMapAsync(
        NpgsqlDataSource dataSource, CancellationToken ct)
    {
        var map = new Dictionary<int, (string, string)>();
        await using var command = dataSource.CreateCommand(
            "SELECT server_id, COALESCE(name, server_id::text) FROM config_monitored_servers");
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var name = reader.GetString(1);
            map[reader.GetInt32(0)] = (name, name);
        }

        return map;
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection cleanup, string ruleName, int serverId, CancellationToken ct)
    {
        using (var rule = new NpgsqlCommand("DELETE FROM config.custom_alert_rules WHERE name = $1", cleanup))
        {
            rule.Parameters.AddWithValue(ruleName);
            await rule.ExecuteNonQueryAsync(ct);
        }

        // config_alert_log has no FK to the rule, so the recovery rows this test wrote must be cleaned explicitly.
        using var log = new NpgsqlCommand("DELETE FROM config.config_alert_log WHERE server_id = $1", cleanup);
        log.Parameters.AddWithValue(serverId);
        await log.ExecuteNonQueryAsync(ct);
    }
}
