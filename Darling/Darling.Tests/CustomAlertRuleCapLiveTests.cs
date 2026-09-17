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
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3285 (Round-2) gated live round-trips (DARLING_TEST_PG) for the enabled-rule count cap. The cap's
/// count-then-write must be enforced against a REAL store (the count subquery, the advisory-locked transaction,
/// and the enabled-vs-disabled bookkeeping cannot be exercised without Postgres): creates and enables are
/// bounded at <see cref="CustomAlertRuleStore.EnabledRuleCap"/>, disabled creates are always allowed, and the
/// evaluator's read is bounded one beyond the cap. The pure ceiling-trim logic is in
/// <c>CustomAlertRuleCapTests</c>. All test rows are named with a per-run prefix and deleted in cleanup, so the
/// shared store is left as it was found.
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertRuleCapLiveTests
{
    private const int Cap = CustomAlertRuleStore.EnabledRuleCap;

    private const string SampleDefinition =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":0.25},\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the custom-alert cap live tests.");
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

    private static async Task<int> CountEnabledAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("SELECT count(*) FROM custom_alert_rules WHERE enabled");
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct));
    }

    /// <summary>Bulk-inserts <paramref name="count"/> ENABLED test rules named <c>{prefix}1..N</c> in one
    /// statement, DELIBERATELY bypassing the store's cap — this is setup that drives the store into the
    /// near-/over-cap state the store itself would refuse to create.</summary>
    private static async Task BulkInsertEnabledAsync(
        NpgsqlDataSource dataSource, string prefix, int count, CancellationToken ct)
    {
        if (count <= 0)
        {
            return;
        }

        await using var command = dataSource.CreateCommand(@"
INSERT INTO custom_alert_rules (name, definition, description, enabled, version, created_at, updated_at, updated_by)
SELECT $1 || g::text, $2, NULL, true, 1, (now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC'), 'captest'
FROM generate_series(1, $3) AS g");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = prefix });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = SampleDefinition });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = count });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteByPrefixAsync(NpgsqlConnection cleanup, string prefix, CancellationToken ct)
    {
        using var command = new NpgsqlCommand("DELETE FROM config.custom_alert_rules WHERE name LIKE $1", cleanup);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = prefix + "%" });
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task EnabledCap_BoundsCreatesAndEnables()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var store = new CustomAlertRuleStore(dataSource);
        var prefix = "carcap_" + Guid.NewGuid().ToString("N") + "_";
        var bodySucceeded = false;
        try
        {
            var current = await CountEnabledAsync(dataSource, ct);
            // Need two free slots below the cap to show "up to the cap -> Ok" through the store first.
            Assert.SkipWhen(current > Cap - 2,
                $"Store already holds {current} enabled rules; need <= {Cap - 2} to exercise the cap boundary.");

            // Fill to Cap-2 by direct insert, then re-count so a shift on the shared store skips rather than flakes.
            await BulkInsertEnabledAsync(dataSource, prefix, Cap - 2 - current, ct);
            var atFill = await CountEnabledAsync(dataSource, ct);
            Assert.SkipWhen(atFill != Cap - 2, $"Enabled count drifted to {atFill} during setup; expected {Cap - 2}.");

            // Up to the cap: two enabled creates THROUGH THE STORE succeed (reaching Cap-1, then exactly Cap).
            var ok1 = Assert.IsType<CustomAlertRuleResult.Ok>(
                await store.CreateAsync(prefix + "ok1", null, SampleDefinition, enabled: true, "test", ct));
            Assert.True(ok1.Rule!.Enabled);
            Assert.IsType<CustomAlertRuleResult.Ok>(
                await store.CreateAsync(prefix + "ok2", null, SampleDefinition, enabled: true, "test", ct));
            Assert.Equal(Cap, await CountEnabledAsync(dataSource, ct));

            // At the cap: the next ENABLED create is refused, and nothing is inserted.
            var capped = Assert.IsType<CustomAlertRuleResult.Invalid>(
                await store.CreateAsync(prefix + "over", null, SampleDefinition, enabled: true, "test", ct));
            Assert.Contains(Cap.ToString(), capped.Message, StringComparison.Ordinal);
            Assert.Equal(Cap, await CountEnabledAsync(dataSource, ct));

            // At the cap: a DISABLED create is still allowed (paused rules don't count against the cap).
            var disabled = Assert.IsType<CustomAlertRuleResult.Ok>(
                await store.CreateAsync(prefix + "disabled", null, SampleDefinition, enabled: false, "test", ct));
            Assert.False(disabled.Rule!.Enabled);
            Assert.Equal(Cap, await CountEnabledAsync(dataSource, ct));

            // At the cap: ENABLING that disabled rule is refused too, or the create cap would be trivially
            // bypassed by create-disabled-then-enable.
            var enableCapped = Assert.IsType<CustomAlertRuleResult.Invalid>(
                await store.UpdateAsync(disabled.Rule!.Id, disabled.Rule!.Name, null, SampleDefinition,
                    enabled: true, disabled.Rule!.Version, "test", ct));
            Assert.Contains(Cap.ToString(), enableCapped.Message, StringComparison.Ordinal);

            // The refused enable was a true no-op: the rule is still disabled at its original version.
            var reread = Assert.IsType<CustomAlertRuleResult.Ok>(await store.GetAsync(disabled.Rule!.Id, ct));
            Assert.False(reread.Rule!.Enabled);
            Assert.Equal(disabled.Rule!.Version, reread.Rule!.Version);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteByPrefixAsync(cleanup, prefix, cleanupCt);
            });
        }
    }

    [Fact]
    public async Task EnabledLoad_IsBoundedByTheCeiling_WhenTableExceedsTheCap()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var store = new CustomAlertRuleStore(dataSource);
        var prefix = "carceil_" + Guid.NewGuid().ToString("N") + "_";
        var bodySucceeded = false;
        try
        {
            var current = await CountEnabledAsync(dataSource, ct);
            Assert.SkipWhen(current > Cap + 2,
                $"Store already holds {current} enabled rules (> cap+2); cannot set up the ceiling test cleanly.");

            // Push the table OVER the cap by direct insert (bypassing the store), so cap+2 enabled rows exist.
            await BulkInsertEnabledAsync(dataSource, prefix, Cap + 2 - current, ct);

            // The store's enabled read is bounded at cap+1 (the cap plus the one-beyond overflow sentinel), never
            // the full over-cap set — so the evaluator can never load an unbounded number of enabled rules.
            var loaded = await store.ListEnabledAsync(ct);
            Assert.Equal(Cap + 1, loaded.Count);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteByPrefixAsync(cleanup, prefix, cleanupCt);
            });
        }
    }
}
