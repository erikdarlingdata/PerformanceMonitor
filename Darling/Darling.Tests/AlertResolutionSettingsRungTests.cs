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
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts mint their own scratch database through ScratchPostgres and never touch another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The "send resolutions" setting (V187, #5825): the rule that decides whether a channel sends a resolution for a route, and
/// on a live store the rung's columns and defaults (fresh and upgraded), and the setting's round trips through the viewer, the
/// service's config provider, the MCP settings tools and the route store.
/// </summary>
public sealed class AlertResolutionSettingsRungTests
{
    private const int RungVersion = 187;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] FlagChannels =
    {
        NotificationRouter.SlackChannel, NotificationRouter.TeamsChannel, NotificationRouter.GenericChannel, NotificationRouter.EmailChannel,
    };

    private static ResolutionChannelFlags AllChannels(bool on) => new(on, on, on, on, PagerDutyAutoResolve: false);

    private static NotificationRoute Route(bool? sendResolutions) =>
        new(1, "Deadlocks Detected", "", "", "", "", "", true) { SendResolutions = sendResolutions };

    [Theory]
    [InlineData(null, false, false)]
    [InlineData(null, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, true)]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    public void ARouteOverride_Decides_AndNullFollowsTheChannel(bool? routeOverride, bool channelOn, bool expected)
    {
        foreach (var channel in FlagChannels)
        {
            Assert.Equal(expected, NotificationRouteResolutions.SendsResolutions(Route(routeOverride), channel, AllChannels(channelOn)));
        }
    }

    [Fact]
    public void WithNoRoute_EachChannelFollowsOnlyItsOwnFlag()
    {
        var slackOnly = new ResolutionChannelFlags(Slack: true, Teams: false, Generic: false, Email: false, PagerDutyAutoResolve: false);

        Assert.True(NotificationRouteResolutions.SendsResolutions(null, NotificationRouter.SlackChannel, slackOnly));
        Assert.False(NotificationRouteResolutions.SendsResolutions(null, NotificationRouter.TeamsChannel, slackOnly));
        Assert.False(NotificationRouteResolutions.SendsResolutions(null, NotificationRouter.GenericChannel, slackOnly));
        Assert.False(NotificationRouteResolutions.SendsResolutions(null, NotificationRouter.EmailChannel, slackOnly));
        Assert.False(NotificationRouteResolutions.SendsResolutions(null, "Unknown", AllChannels(true)));
    }

    [Fact]
    public void PagerDuty_FollowsAutoResolve_WhateverTheRouteSays()
    {
        var autoResolve = ResolutionChannelFlags.None with { PagerDutyAutoResolve = true };

        Assert.True(NotificationRouteResolutions.SendsResolutions(Route(false), NotificationRouter.PagerDutyChannel, autoResolve));
        Assert.False(NotificationRouteResolutions.SendsResolutions(Route(true), NotificationRouter.PagerDutyChannel, ResolutionChannelFlags.None));
    }

    [Fact]
    public void TheFlags_ComeFromTheStoredRowsTwoHalves()
    {
        var flags = ResolutionChannelFlags.From(
            new WebhooksConfig { SlackSendResolutions = true, GenericSendResolutions = true, PagerDutyAutoResolve = true },
            new SmtpConfig { SendResolutions = true });

        Assert.Equal(new ResolutionChannelFlags(Slack: true, Teams: false, Generic: true, Email: true, PagerDutyAutoResolve: true), flags);
    }

    [Fact]
    public async Task OnALiveStore_AFreshAndAnUpgradedStore_EndWithTheColumnsAndTheirDefaults()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V187 rung's live test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* Fresh: every column, its type, its nullability and its default. */
            await AssertColumnsAsync(connection, ct);
            Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));

            /* A store one rung behind, holding a notification row, a route and an alert-history row from before the rung. */
            await RungRewindArtifacts.DropV188SentinelAsync(connection, ct);
            await RungRewindArtifacts.DropV187SentinelAsync(connection, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 187", ct);
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, smtp_recipients) VALUES ('Deadlocks Detected', 'ops@example.com')", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, notification_type) "
                + "VALUES ((now() AT TIME ZONE 'UTC'), 1, 'sql-a', 'Deadlocks Detected', 2, 1, 'none')", ct);
            Assert.Equal(RungVersion - 1, await ProbedVersionAsync(connection, ct));

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));
            await AssertColumnsAsync(connection, ct);
            Assert.Equal("f|f|f|f", await ScalarAsync(connection,
                "SELECT concat_ws('|', slack_send_resolutions, teams_send_resolutions, generic_send_resolutions, email_send_resolutions) FROM config.config_notification WHERE id = 1", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM config.config_notification_routes WHERE send_resolutions IS NULL", ct));
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM config.config_alert_log WHERE resolution_sent_at IS NULL AND resolution_channels IS NULL AND resolution_outcome IS NULL", ct));

            /* The rung runs again on a store that already has everything. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 187", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await AssertColumnsAsync(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task OnALiveStore_TheSettingAndTheOverride_RoundTripThroughEverySurface()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V187 round trips (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var provider = new StoreConfigProvider(dataSource);
            await provider.SeedIfEmptyAsync(new DarlingConfig(), ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* Off by default, on every surface. */
            var seeded = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(seeded);
            Assert.Equal(ResolutionChannelFlags.None, ResolutionChannelFlags.From(seeded.Webhooks, seeded.Smtp));
            Assert.Equal((false, false, false, false), await McpFlagsAsync(dataSource));

            /* The viewer's Settings save writes the four flags; the provider and the MCP read see them. */
            var row = await viewer.GetNotificationAsync(ct);
            Assert.NotNull(row);
            Assert.False(row.SlackSendResolutions || row.TeamsSendResolutions || row.GenericSendResolutions || row.EmailSendResolutions);
            row.SlackSendResolutions = true;
            row.EmailSendResolutions = true;
            await viewer.UpsertNotificationAsync(row, ct);

            var reread = await viewer.GetNotificationAsync(ct);
            Assert.NotNull(reread);
            Assert.True(reread.SlackSendResolutions && reread.EmailSendResolutions);
            Assert.False(reread.TeamsSendResolutions || reread.GenericSendResolutions);

            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.Equal(new ResolutionChannelFlags(Slack: true, Teams: false, Generic: false, Email: true, PagerDutyAutoResolve: false),
                ResolutionChannelFlags.From(view.Webhooks, view.Smtp));
            Assert.Equal((true, false, false, true), await McpFlagsAsync(dataSource));

            /* update_alert_settings changes only the flags it names. */
            var updated = await DarlingMcpAlertTools.UpdateAlertSettings(dataSource,
                "{\"delivery\":{\"send_resolutions\":{\"teams\":true,\"slack\":false}}}");
            Assert.False(McpHelpers.IsErrorEnvelope(updated), updated);
            Assert.Equal((false, true, false, true), await McpFlagsAsync(dataSource));
            var refused = await DarlingMcpAlertTools.UpdateAlertSettings(dataSource, "{\"delivery\":{\"send_resolutions\":{\"pagerduty\":true}}}");
            Assert.Contains("delivery.send_resolutions.pagerduty", refused, StringComparison.Ordinal);
            Assert.Equal((false, true, false, true), await McpFlagsAsync(dataSource));

            view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.True(view.Webhooks.TeamsSendResolutions);
            Assert.False(view.Webhooks.SlackSendResolutions);

            /* A route's override: written by the viewer, read back by the viewer, the provider, the route store and the MCP tool. */
            var routeId = await viewer.InsertNotificationRouteAsync(new NotificationRouteRow
            {
                MetricMatch = "Deadlocks Detected",
                SmtpRecipients = "ops@example.com",
                Enabled = true,
                SendResolutions = false,
            }, ct);
            await AssertOverrideAsync(viewer, provider, dataSource, routeId, false, ct);

            /* An edit of another field keeps the override; disabling the route keeps it too. */
            var stored = (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == routeId).Clone();
            stored.SmtpRecipients = "oncall@example.com";
            await viewer.UpdateNotificationRouteAsync(stored, ct);
            await AssertOverrideAsync(viewer, provider, dataSource, routeId, false, ct);
            Assert.Equal(1, await new PgNotificationRouteStore(dataSource).SetEnabledAsync(routeId, false, ct));
            Assert.False((await new PgNotificationRouteStore(dataSource).LoadSummariesAsync(ct)).Single().SendResolutions);

            /* Always, then back to the channel's setting. */
            stored = (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == routeId).Clone();
            stored.Enabled = true;
            stored.SendResolutions = true;
            await viewer.UpdateNotificationRouteAsync(stored, ct);
            await AssertOverrideAsync(viewer, provider, dataSource, routeId, true, ct);

            stored.SendResolutions = null;
            await viewer.UpdateNotificationRouteAsync(stored, ct);
            await AssertOverrideAsync(viewer, provider, dataSource, routeId, null, ct);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task AssertOverrideAsync(
        ViewerDataService viewer, StoreConfigProvider provider, NpgsqlDataSource dataSource, int routeId, bool? expected, CancellationToken ct)
    {
        Assert.Equal(expected, (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == routeId).SendResolutions);

        var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
        Assert.NotNull(view);
        Assert.Equal(expected, view.NotificationRoutes.Single(r => r.RouteId == routeId).SendResolutions);

        Assert.Equal(expected, (await new PgNotificationRouteStore(dataSource).LoadSummariesAsync(ct)).Single(r => r.RouteId == routeId).SendResolutions);

        using var json = JsonDocument.Parse(await DarlingMcpAlertTools.GetNotificationRoutes(dataSource, ct));
        var route = json.RootElement.GetProperty("routes").EnumerateArray().Single(r => r.GetProperty("route_id").GetInt32() == routeId);
        var value = route.GetProperty("send_resolutions");
        Assert.Equal(expected, value.ValueKind == JsonValueKind.Null ? null : value.GetBoolean());
    }

    private static async Task<(bool Slack, bool Teams, bool Webhook, bool Email)> McpFlagsAsync(NpgsqlDataSource dataSource)
    {
        var settings = await DarlingMcpAlertTools.GetAlertSettings(dataSource);
        Assert.False(McpHelpers.IsErrorEnvelope(settings), settings);
        using var json = JsonDocument.Parse(settings);
        var flags = json.RootElement.GetProperty("delivery").GetProperty("send_resolutions");
        return (flags.GetProperty("slack").GetBoolean(), flags.GetProperty("teams").GetBoolean(),
            flags.GetProperty("webhook").GetBoolean(), flags.GetProperty("email").GetBoolean());
    }

    private static async Task AssertColumnsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var columns = await ScalarAsync(connection, @"
SELECT string_agg(table_name || '.' || column_name || ':' || data_type || ':' || is_nullable || ':' || coalesce(column_default, '-'), ';'
                  ORDER BY table_name, column_name)
FROM information_schema.columns
WHERE table_schema = 'config'
AND   (column_name LIKE '%send_resolutions' OR column_name LIKE 'resolution\_%')", ct);

        Assert.Equal(string.Join(";",
            "config_alert_log.resolution_channels:text:YES:-",
            "config_alert_log.resolution_outcome:text:YES:-",
            "config_alert_log.resolution_sent_at:timestamp without time zone:YES:-",
            "config_notification.email_send_resolutions:boolean:NO:false",
            "config_notification.generic_send_resolutions:boolean:NO:false",
            "config_notification.slack_send_resolutions:boolean:NO:false",
            "config_notification.teams_send_resolutions:boolean:NO:false",
            "config_notification_routes.send_resolutions:boolean:YES:-"), columns);
        Assert.Equal(NotificationRouteResolutions.HistoryColumns.OrderBy(c => c, StringComparer.Ordinal),
            new[] { "resolution_channels", "resolution_outcome", "resolution_sent_at" });
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
