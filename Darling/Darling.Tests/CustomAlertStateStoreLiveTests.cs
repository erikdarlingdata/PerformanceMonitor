/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The gated live round-trip for the #3285 custom-alert state store (DARLING_TEST_PG). This exists because the
/// pure tests cannot: <see cref="CustomAlertStateStore.SaveAsync"/> binds the evaluator's
/// <c>DateTime.UtcNow</c>-derived timestamps to <c>config.custom_alert_state</c>'s <c>timestamp</c> (without
/// time zone) columns, and Npgsql REJECTS a Kind=Utc DateTime there — which threw AFTER the alert was delivered,
/// so a rule re-fired every sweep instead of firing once and holding. This round-trip passes a Kind=Utc
/// timestamp exactly as the evaluator does, so it goes red on the unfixed store, and it proves the FK cascade
/// tears the state row down when its rule is deleted.
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertStateStoreLiveTests
{
    private const string SampleDefinition =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":0.25},\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the custom-alert-state live tests.");
        return connectionString!;
    }

    [Fact]
    public async Task SaveThenLoad_RoundTripsUtcTimestamps_AndCascadesWhenTheRuleIsDeleted()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;

        await using var dataSource = NpgsqlDataSource.Create(dataSourceConnectionString);
        var rules = new CustomAlertRuleStore(dataSource);
        var state = new CustomAlertStateStore(dataSource);

        var ruleName = "car_live_" + Guid.NewGuid().ToString("N");
        const int serverId = 424242; // no FK to servers, so a synthetic id is fine
        long ruleId = 0;
        var bodySucceeded = false;
        try
        {
            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(ruleName, null, SampleDefinition, true, "test", ct));
            ruleId = created.Rule!.Id;
            var ruleVersion = created.Rule!.Version;

            /* Kind=Utc, and no sub-second component so the microsecond round-trip through PG is exact. This is
               the exact shape the evaluator passes; the unfixed store throws here. */
            var evaluatedAt = new DateTime(2026, 9, 11, 12, 0, 0, DateTimeKind.Utc);
            var nextDueAt = evaluatedAt.AddSeconds(60);

            var firing = new CustomAlertRuleState(
                new PersistenceState(ConsecutiveBreaches: 3, ConsecutiveClears: 0, Firing: true),
                ruleVersion, "Critical", evaluatedAt, nextDueAt);

            await state.SaveAsync(ruleId, serverId, firing, ct); // must NOT throw — the regression

            var loaded = await state.LoadAsync(ruleId, serverId, ruleVersion, ct);
            Assert.Equal(3, loaded.Persistence.ConsecutiveBreaches);
            Assert.True(loaded.Persistence.Firing);
            Assert.Equal("Critical", loaded.FiredSeverity);
            Assert.Equal(evaluatedAt, loaded.LastEvaluatedAt!.Value);
            Assert.Equal(nextDueAt, loaded.NextDueAt!.Value);

            /* The ON CONFLICT update path: flip to cleared/not-firing. */
            var cleared = firing with
            {
                Persistence = new PersistenceState(ConsecutiveBreaches: 0, ConsecutiveClears: 2, Firing: false),
                FiredSeverity = null,
            };
            await state.SaveAsync(ruleId, serverId, cleared, ct);

            var reloaded = await state.LoadAsync(ruleId, serverId, ruleVersion, ct);
            Assert.False(reloaded.Persistence.Firing);
            Assert.Null(reloaded.FiredSeverity);
            Assert.Equal(2, reloaded.Persistence.ConsecutiveClears);

            Assert.Contains(await state.ListForRuleAsync(ruleId, ct), r => r.ServerId == serverId);

            /* Deleting the rule cascades its state rows away (teardown). */
            Assert.IsType<CustomAlertRuleResult.Ok>(await rules.DeleteAsync(ruleId, ct));
            Assert.Empty(await state.ListForRuleAsync(ruleId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var command = new NpgsqlCommand("DELETE FROM config.custom_alert_rules WHERE name = $1", cleanup);
                command.Parameters.AddWithValue(ruleName);
                await command.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
