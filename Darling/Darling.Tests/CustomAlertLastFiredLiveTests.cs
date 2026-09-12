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
/// #3360 gated live round-trips (DARLING_TEST_PG) for the "last fired" correlation the rule LIST read
/// (<see cref="CustomAlertRuleStore.ListSql"/>) adds. The correlation is pure SQL — a LEFT JOIN of the rule set to
/// a grouped <c>config_alert_log</c> subquery on the immutable <c>Custom:&lt;id&gt;</c> fire key — so it can only be
/// exercised against a real Postgres: a fire row counts, a resolve row for the SAME rule does NOT (it carries a
/// different <c>metric_name</c>), one rule's fires never bleed into another's, and a never-fired rule reports null,
/// all resolved in ONE list read. All test rows are tagged with a per-run prefix / server name and deleted in
/// cleanup, so the shared store is left as it was found.
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertLastFiredLiveTests
{
    private const string SampleDefinition =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":0.25},\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the custom-alert last-fired live tests.");
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

    /// <summary>Inserts ONE <c>config_alert_log</c> row with the given metric_name + naive-UTC alert_time, tagged
    /// with <paramref name="serverName"/> so cleanup can delete exactly this run's rows. The other columns are the
    /// minimal valid no-channel shape (the correlation reads only metric_name + alert_time).</summary>
    private static async Task InsertAlertLogAsync(
        NpgsqlDataSource dataSource, string serverName, string metricName, DateTime alertTimeUtc, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1, 1, $2, $3, 0, 0, false, 'none', NULL, false, NULL, NULL)");
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(alertTimeUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serverName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = metricName });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<CustomAlertRule> CreateRuleAsync(
        CustomAlertRuleStore store, string name, CancellationToken ct)
    {
        var result = Assert.IsType<CustomAlertRuleResult.Ok>(
            await store.CreateAsync(name, description: null, SampleDefinition, enabled: true, "lastfiredtest", ct));
        return result.Rule!;
    }

    [Fact]
    public async Task ListAsync_ReportsLastFired_PerRule_ExcludingResolveRows_InOneRead()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var store = new CustomAlertRuleStore(dataSource);
        var prefix = "carlf_" + Guid.NewGuid().ToString("N") + "_";
        var bodySucceeded = false;
        try
        {
            // Three rules: alpha (fires twice, then a resolve), bravo (fires once), charlie (never fires).
            var alpha = await CreateRuleAsync(store, prefix + "alpha", ct);
            var bravo = await CreateRuleAsync(store, prefix + "bravo", ct);
            var charlie = await CreateRuleAsync(store, prefix + "charlie", ct);

            var t0 = new DateTime(2026, 1, 2, 3, 0, 0);
            var alphaFire1 = t0;                    // alpha first fire
            var bravoFire = t0.AddMinutes(5);       // bravo's only fire
            var alphaFire2 = t0.AddMinutes(10);     // alpha's LATER fire -> MAX for alpha
            var alphaResolveByName = t0.AddMinutes(20);  // resolve row for alpha (different metric_name) -> excluded
            var alphaResolveLikeKey = t0.AddMinutes(30); // 'Custom:<id> Resolved' -> matches LIKE but NOT the join key

            // alpha's fires (on its immutable fire key) — the later one is the answer.
            await InsertAlertLogAsync(dataSource, prefix, CustomAlertEvaluator.MetricNameFor(alpha.Id), alphaFire1, ct);
            await InsertAlertLogAsync(dataSource, prefix, CustomAlertEvaluator.MetricNameFor(alpha.Id), alphaFire2, ct);

            // A NATURAL resolve for alpha: metric_name '<name> Resolved' (DeliverResolveAsync). It is LATER than the
            // last fire, so if it were mistaken for a fire alpha's last_fired would be wrong. It must be excluded —
            // it does not even match LIKE 'Custom:%'.
            await InsertAlertLogAsync(dataSource, prefix, alpha.Name + " Resolved", alphaResolveByName, ct);

            // An ADVERSARIAL row 'Custom:<id> Resolved': it DOES match the LIKE 'Custom:%' pre-filter, and is the
            // latest row of all, but is not EXACTLY 'Custom:<id>', so the join key must reject it. This proves the
            // exact join (not the LIKE) is what correlates a fire.
            await InsertAlertLogAsync(
                dataSource, prefix, CustomAlertEvaluator.MetricNameFor(alpha.Id) + " Resolved", alphaResolveLikeKey, ct);

            // bravo fires once — its own key only.
            await InsertAlertLogAsync(dataSource, prefix, CustomAlertEvaluator.MetricNameFor(bravo.Id), bravoFire, ct);

            // charlie: no rows at all.

            // ONE read resolves last_fired for every rule (never a per-rule query).
            var summaries = await store.ListAsync(ct);
            var alphaSummary = Assert.Single(summaries, s => s.Id == alpha.Id);
            var bravoSummary = Assert.Single(summaries, s => s.Id == bravo.Id);
            var charlieSummary = Assert.Single(summaries, s => s.Id == charlie.Id);

            // alpha: MAX over its FIRE rows only — the second fire, NOT the (later) resolve rows.
            Assert.NotNull(alphaSummary.LastFired);
            Assert.Equal(alphaFire2, alphaSummary.LastFired!.Value);

            // bravo: its own single fire — alpha's fires do not bleed in.
            Assert.NotNull(bravoSummary.LastFired);
            Assert.Equal(bravoFire, bravoSummary.LastFired!.Value);

            // charlie: never fired -> null.
            Assert.Null(charlieSummary.LastFired);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using (var deleteLog = new NpgsqlCommand("DELETE FROM config.config_alert_log WHERE server_name = $1", cleanup))
                {
                    deleteLog.Parameters.Add(new NpgsqlParameter<string> { TypedValue = prefix });
                    await deleteLog.ExecuteNonQueryAsync(cleanupCt);
                }

                using var deleteRules = new NpgsqlCommand("DELETE FROM config.custom_alert_rules WHERE name LIKE $1", cleanup);
                deleteRules.Parameters.Add(new NpgsqlParameter<string> { TypedValue = prefix + "%" });
                await deleteRules.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
