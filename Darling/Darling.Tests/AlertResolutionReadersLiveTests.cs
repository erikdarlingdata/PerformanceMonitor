/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5825 L3 gated live reads (DARLING_TEST_PG): the three resolution history columns (V187) reach every reader. A
/// resolution row carries <c>resolution_sent_at</c>, <c>resolution_channels</c> and <c>resolution_outcome</c>; a fire row
/// carries none. <c>get_alert_history</c> answers them (null on the fire row), and the viewer's
/// <see cref="ViewerDataService.GetAlertHistoryAsync"/> rows carry them into the grid's Resolution cell and the Alert
/// Details window, whose three rows show on the resolution row and are collapsed on the fire row.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertResolutionReadersLiveTests
{
    private const int ServerId = -582512;
    private const string ServerName = "lane5825-readers";
    private const string FireMetric = "High CPU";
    private const string ResolutionMetric = "CPU Resolved";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the alert resolution reader live tests.");
        return connectionString!;
    }

    /// <summary>A fire two minutes ago, and its resolution one minute ago that went to Slack and email.</summary>
    private static async Task<DateTime> SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await DeleteAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name, is_enabled = TRUE",
            ServerId, ServerName, DarlingMcpTestData.Naive(now));

        var sentAt = now.AddMinutes(-1);
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json,
     resolution_sent_at, resolution_channels, resolution_outcome)
VALUES ($1, $2, $3, $4, 97, 90, TRUE, 'webhook', NULL, FALSE, FALSE, NULL, NULL, NULL, NULL, NULL),
       ($5, $2, $3, $6, 12, 90, TRUE, 'email+webhook', NULL, FALSE, FALSE, NULL, NULL, $5, 'Slack,Email', 'sent')",
            DarlingMcpTestData.Naive(now.AddMinutes(-2)), ServerId, ServerName, FireMetric,
            DarlingMcpTestData.Naive(sentAt), ResolutionMetric);
        return sentAt;
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }

    private static async Task<NpgsqlConnection> OpenMigratedAsync(string cs, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    [Fact]
    public async Task GetAlertHistory_AnswersTheResolutionFields_OnTheResolutionRow_AndNullOnTheFireRow()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var sentAt = await SeedAsync(connection, now, ct);
            await using var postgres = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(cs)
            {
                SearchPath = PgSchemaGenerator.SearchPath,
            }.ConnectionString);

            var json = await DarlingMcpAlertTools.GetAlertHistory(postgres, server_name: ServerName, hours_back: 1, limit: 50, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var alerts = doc.RootElement.GetProperty("alerts").EnumerateArray().ToList();
            Assert.Equal(2, alerts.Count);

            var resolution = alerts.Single(a => a.GetProperty("metric_name").GetString() == ResolutionMetric);
            Assert.Equal(
                DateTime.SpecifyKind(sentAt, DateTimeKind.Utc),
                DateTime.Parse(resolution.GetProperty("resolution_sent_at").GetString()!, null,
                    System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal));
            Assert.Equal("Slack,Email", resolution.GetProperty("resolution_channels").GetString());
            Assert.Equal("sent", resolution.GetProperty("resolution_outcome").GetString());

            var fire = alerts.Single(a => a.GetProperty("metric_name").GetString() == FireMetric);
            foreach (var field in new[] { "resolution_sent_at", "resolution_channels", "resolution_outcome" })
            {
                Assert.True(fire.TryGetProperty(field, out var value), field + " is missing from the fire row");
                Assert.Equal(JsonValueKind.Null, value.ValueKind);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }

    [Fact]
    public async Task TheViewerRows_CarryTheResolutionFields_IntoTheGridCellAndTheAlertDetailsWindow()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var sentAt = await SeedAsync(connection, now, ct);
            await using var viewer = new ViewerDataService(cs);

            var rows = await viewer.GetAlertHistoryAsync(now.AddHours(-1), ServerId, 50, ct);
            Assert.Equal(2, rows.Count);
            var resolution = rows.Single(r => r.MetricName == ResolutionMetric);
            var fire = rows.Single(r => r.MetricName == FireMetric);

            Assert.Equal(DateTime.SpecifyKind(sentAt, DateTimeKind.Utc), resolution.ResolutionSentAt);
            Assert.Equal(DateTimeKind.Utc, resolution.ResolutionSentAt!.Value.Kind);
            Assert.Equal("Slack,Email", resolution.ResolutionChannels);
            Assert.Equal("sent", resolution.ResolutionOutcome);
            Assert.Equal("sent (Slack,Email)", resolution.ResolutionDisplay);
            Assert.Equal(sentAt.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + " UTC",
                resolution.ResolutionSentAtDisplay);

            Assert.Null(fire.ResolutionSentAt);
            Assert.Null(fire.ResolutionChannels);
            Assert.Null(fire.ResolutionOutcome);
            Assert.Equal("", fire.ResolutionDisplay);
            Assert.Equal("", fire.ResolutionSentAtDisplay);

            /* Alert Details: the three rows show the resolution's send, and are collapsed on the fire row. */
            StaTestThread.Run(() =>
            {
                var shown = new AlertDetailWindow(resolution);
                try
                {
                    Assert.Equal(Visibility.Visible, shown.ResolutionOutcomeLabel.Visibility);
                    Assert.Equal(Visibility.Visible, shown.ResolutionChannelsText.Visibility);
                    Assert.Equal(Visibility.Visible, shown.ResolutionSentAtText.Visibility);
                    Assert.Equal("sent", shown.ResolutionOutcomeText.Text);
                    Assert.Equal("Slack,Email", shown.ResolutionChannelsText.Text);
                    Assert.Equal(resolution.ResolutionSentAtDisplay, shown.ResolutionSentAtText.Text);
                }
                finally
                {
                    shown.Close();
                }

                var hidden = new AlertDetailWindow(fire);
                try
                {
                    foreach (var element in new UIElement[]
                             {
                                 hidden.ResolutionSentAtLabel, hidden.ResolutionSentAtText, hidden.ResolutionChannelsLabel,
                                 hidden.ResolutionChannelsText, hidden.ResolutionOutcomeLabel, hidden.ResolutionOutcomeText,
                             })
                    {
                        Assert.Equal(Visibility.Collapsed, element.Visibility);
                    }
                }
                finally
                {
                    hidden.Close();
                }
            });

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteAsync);
        }
    }
}
