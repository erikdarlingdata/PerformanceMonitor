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
/// #5825 gated live round-trip (DARLING_TEST_PG): a resolution's send lands on its <c>config_alert_log</c> row. A sent
/// clear writes <c>resolution_sent_at</c>, <c>resolution_channels</c> and <c>resolution_outcome</c> and the channel it
/// went out on; a muted clear writes <c>muted</c>; a clear with no delivered fire leaves all three null and stays
/// <c>none</c>. Through the real deliverer and <see cref="PgAlertHistoryStore"/>, with Slack on a loopback endpoint.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertResolutionHistoryLiveTests
{
    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the alert resolution history live test.");
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

    [Fact]
    public async Task SentMutedAndUnsentResolutions_WriteTheirColumnsOnTheHistoryRow()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);
        var server = "lane5825-" + Guid.NewGuid().ToString("N")[..8];
        var clearedAt = new DateTime(2026, 10, 15, 12, 1, 0, DateTimeKind.Utc);
        var bodySucceeded = false;
        try
        {
            using var slack = new CapturingWebhookEndpoint();
            var config = new DarlingConfig();
            config.Webhooks.SlackUrl = slack.Url;
            config.Webhooks.SlackSendResolutions = true;
            var settings = new DarlingAlertSettings(config);
            var history = new PgAlertHistoryStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
            var webhooks = new WebhookAlertService(
                settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
            var mute = false;
            var deliverer = new DarlingAlertDeliverer(
                settings, history, webhooks, NullLogger.Instance,
                isAlertMuted: context => mute && context.MetricName == "Blocking Detected", utcNow: () => clearedAt);

            AlertOutcome Fire(string metric) => new(
                server, server, metric, "97%", "90%", Context: null, DetailText: null,
                NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null);

            /* Sent: High CPU fires to Slack and clears. */
            await deliverer.DeliverAsync(Fire("High CPU"), ct);
            await deliverer.DeliverResolutionAsync(
                new AlertResolution(server, server, "High CPU", "CPU Resolved", $"{server}: back to 12%"), cancellationToken: ct);

            /* Muted: Blocking fires to Slack, a mute rule then matches it, and it clears. */
            await deliverer.DeliverAsync(Fire("Blocking Detected"), ct);
            mute = true;
            await deliverer.DeliverResolutionAsync(
                new AlertResolution(server, server, "Blocking Detected", "Blocking Cleared", $"{server}: no blocking"),
                cancellationToken: ct);

            /* No delivered fire: a clear for an alert that never fired. */
            await deliverer.DeliverResolutionAsync(
                new AlertResolution(server, server, "Deadlocks Detected", "Deadlocks Cleared", $"{server}: quiet"),
                cancellationToken: ct);

            var rows = new Dictionary<string, (string Type, bool Sent, bool Muted, DateTime? SentAt, string? Channels, string? Outcome)>();
            await using (var read = dataSource.CreateCommand(@"
SELECT metric_name, notification_type, alert_sent, muted, resolution_sent_at, resolution_channels, resolution_outcome
FROM   config.config_alert_log
WHERE  server_name = $1
AND    metric_name IN ('CPU Resolved', 'Blocking Cleared', 'Deadlocks Cleared')"))
            {
                read.Parameters.AddWithValue(server);
                await using var reader = await read.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    rows[reader.GetString(0)] = (
                        reader.GetString(1), reader.GetBoolean(2), reader.GetBoolean(3),
                        reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                        reader.IsDBNull(5) ? null : reader.GetString(5),
                        reader.IsDBNull(6) ? null : reader.GetString(6));
                }
            }

            Assert.Equal(3, rows.Count);
            var sent = rows["CPU Resolved"];
            Assert.Equal(AlertDelivery.ChannelWebhook, sent.Type);
            Assert.True(sent.Sent);
            Assert.Equal(clearedAt, sent.SentAt);
            Assert.Equal(NotificationRouter.SlackChannel, sent.Channels);
            Assert.Equal("sent", sent.Outcome);

            var muted = rows["Blocking Cleared"];
            Assert.Equal(AlertDelivery.ChannelMuted, muted.Type);
            Assert.True(muted.Muted);
            Assert.Null(muted.SentAt);
            Assert.Equal(NotificationRouter.SlackChannel, muted.Channels);
            Assert.Equal("muted", muted.Outcome);

            var none = rows["Deadlocks Cleared"];
            Assert.Equal(AlertDelivery.ChannelNotApplicable, none.Type);
            Assert.Null(none.SentAt);
            Assert.Null(none.Channels);
            Assert.Null(none.Outcome);

            /* Two fires and one clear reached Slack; the muted clear did not. */
            Assert.Equal(3, slack.Bodies.Count);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var delete = new NpgsqlCommand("DELETE FROM config.config_alert_log WHERE server_name = $1", cleanup);
                delete.Parameters.AddWithValue(server);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
