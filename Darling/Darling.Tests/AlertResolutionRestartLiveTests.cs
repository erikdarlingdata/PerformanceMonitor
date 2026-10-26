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
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5825 L3 gated live round-trip (DARLING_TEST_PG): a clear handled after a service restart reads its fire from
/// <c>config.config_alert_log</c> through <see cref="PgAlertHistoryStore.GetOpenDeliveredFiresAsync"/>. Each fire goes out
/// on one <see cref="DarlingAlertDeliverer"/>, and each clear is handled by a NEW one over the same store, so the only
/// record of where the fire went is the stored row's <c>context_json</c> Route record. The clear goes to the route's
/// CURRENT destination; a fire whose route was deleted since sends nothing and its resolution row says why; and a second
/// clear after the resolution row was written finds no open fire and sends nothing again. Slack on loopback endpoints.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertResolutionRestartLiveTests
{
    private const int ServerId = -582511;
    private const string ServerKey = "-582511";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the alert resolution restart live test.");
        return connectionString!;
    }

    private static async Task<NpgsqlDataSource> MigrateAndOpenAsync(string connectionString, CancellationToken ct)
    {
        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        return NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString);
    }

    private static DarlingAlertDeliverer Build(DarlingConfig config, PgAlertHistoryStore history)
    {
        var settings = new DarlingAlertSettings(config);
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        return new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);
    }

    private static DarlingConfig Config(params NotificationRoute[] routes)
    {
        var config = new DarlingConfig();
        config.Webhooks.SlackSendResolutions = true;
        config.NotificationRoutes = routes;
        return config;
    }

    private static NotificationRoute SlackRoute(int id, string metric, string url) => new(id, metric, "", url, "", "", "", true);

    private static AlertOutcome Fire(string server, string metric) => new(
        ServerKey, server, metric, "97%", "90%", Context: null, DetailText: null,
        NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null);

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var delete = new NpgsqlCommand("DELETE FROM config.config_alert_log WHERE server_id = $1", connection);
        delete.Parameters.AddWithValue(ServerId);
        await delete.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task AClearAfterARestart_ReadsItsFireFromTheStore_AndGoesToTheRoutesCurrentDestination_OrSaysWhyNot()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);
        var server = "lane5825-" + Guid.NewGuid().ToString("N")[..8];
        var bodySucceeded = false;
        try
        {
            await using (var connection = await dataSource.OpenConnectionAsync(ct))
            {
                await DeleteAsync(connection, ct);
            }

            using var cpuBefore = new CapturingWebhookEndpoint();
            using var cpuAfter = new CapturingWebhookEndpoint();
            using var blocking = new CapturingWebhookEndpoint();
            var history = new PgAlertHistoryStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);

            /* Before the restart: High CPU fires through route 1, Blocking through route 2. */
            var before = Build(Config(SlackRoute(1, "High CPU", cpuBefore.Url), SlackRoute(2, "Blocking Detected", blocking.Url)), history);
            await before.DeliverAsync(Fire(server, "High CPU"), ct);
            await before.DeliverAsync(Fire(server, "Blocking Detected"), ct);
            Assert.Single(cpuBefore.Bodies);
            Assert.Single(blocking.Bodies);

            /* After the restart: route 1 now points somewhere else, and route 2 is gone. */
            var after = Config(SlackRoute(1, "High CPU", cpuAfter.Url));
            var restarted = Build(after, history);
            await restarted.DeliverResolutionAsync(
                new AlertResolution(ServerKey, server, "High CPU", "CPU Resolved", $"{server}: back to 12%"), cancellationToken: ct);
            await restarted.DeliverResolutionAsync(
                new AlertResolution(ServerKey, server, "Blocking Detected", "Blocking Cleared", $"{server}: no blocking"),
                cancellationToken: ct);

            /* The CPU clear went to route 1's current destination, with the stored fire's value; not to the old one. */
            Assert.Single(cpuBefore.Bodies);
            var cpuClear = Assert.Single(cpuAfter.Bodies);
            Assert.Contains("CPU Resolved", cpuClear, StringComparison.Ordinal);
            Assert.Contains("Last value: 97", cpuClear, StringComparison.Ordinal);
            /* The Blocking clear had no route left to go to. */
            Assert.Single(blocking.Bodies);

            /* A second restart and a second CPU clear: the resolution row closed the episode, so nothing goes out. */
            await Build(after, history).DeliverResolutionAsync(
                new AlertResolution(ServerKey, server, "High CPU", "CPU Resolved", $"{server}: back to 12%"), cancellationToken: ct);
            Assert.Single(cpuAfter.Bodies);

            var rows = new List<(string Metric, string Type, bool Sent, DateTime? SentAt, string? Channels, string? Outcome)>();
            await using (var read = dataSource.CreateCommand(@"
SELECT metric_name, notification_type, alert_sent, resolution_sent_at, resolution_channels, resolution_outcome
FROM   config.config_alert_log
WHERE  server_id = $1
AND    metric_name IN ('CPU Resolved', 'Blocking Cleared')
ORDER BY alert_time"))
            {
                read.Parameters.AddWithValue(ServerId);
                await using var reader = await read.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((
                        reader.GetString(0), reader.GetString(1), reader.GetBoolean(2),
                        reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                        reader.IsDBNull(4) ? null : reader.GetString(4),
                        reader.IsDBNull(5) ? null : reader.GetString(5)));
                }
            }

            Assert.Equal(3, rows.Count);
            var cpuSent = rows[0];
            Assert.Equal("CPU Resolved", cpuSent.Metric);
            Assert.Equal(AlertDelivery.ChannelWebhook, cpuSent.Type);
            Assert.True(cpuSent.Sent);
            Assert.NotNull(cpuSent.SentAt);
            Assert.Equal(NotificationRouter.SlackChannel, cpuSent.Channels);
            Assert.Equal("sent", cpuSent.Outcome);

            var blockingRow = rows[1];
            Assert.Equal("Blocking Cleared", blockingRow.Metric);
            Assert.Equal(AlertDelivery.ChannelNotApplicable, blockingRow.Type);
            Assert.False(blockingRow.Sent);
            Assert.Null(blockingRow.SentAt);
            Assert.Equal(NotificationRouter.SlackChannel, blockingRow.Channels);
            Assert.Equal("route deleted or disabled: Slack", blockingRow.Outcome);

            var cpuAgain = rows[2];
            Assert.Equal("CPU Resolved", cpuAgain.Metric);
            Assert.Equal(AlertDelivery.ChannelNotApplicable, cpuAgain.Type);
            Assert.Null(cpuAgain.SentAt);
            Assert.Null(cpuAgain.Channels);
            Assert.Null(cpuAgain.Outcome);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, DeleteAsync);
        }
    }
}
