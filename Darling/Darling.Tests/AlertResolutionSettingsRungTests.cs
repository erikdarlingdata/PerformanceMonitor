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
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct)); /* the ladder's top: V188 (#5821) and V189 (#5867) sit above this rung */

            /* A store one rung behind, holding a notification row, a route and an alert-history row from before the rung. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
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

            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct)); /* the ladder's top: V188 (#5821) and V189 (#5867) sit above this rung */
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

    [Fact]
    public async Task OnAnEmptyStore_TheFirstStartSeedsTheGenericPagerDutyAndSendResolutionsSettingsFromTheFile()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the #6715 seed round trip (it mints its own scratch database).");
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
            var file = new DarlingConfig
            {
                Smtp = new SmtpConfig { SendResolutions = false },
                Webhooks = new WebhooksConfig
                {
                    GenericUrl = "https://hooks.example.com/alert",
                    GenericHeaders = "{\"Authorization\":\"Bearer abc\"}",
                    GenericBodyTemplate = "{\"text\":\"{{metric}} on {{server}}\"}",
                    GenericProxy = "http://proxy.example.com:8080",
                    PagerDutyRoutingKey = "0123456789abcdef0123456789abcdef",
                    PagerDutyUseEuRegion = true,
                    PagerDutyProxy = "http://pd-proxy.example.com:8080",
                    // Mixed on purpose: all-true values cannot see a swap among the six boolean parameters the seed binds (#6715).
                    PagerDutyAutoResolve = false,
                    SlackSendResolutions = true,
                    TeamsSendResolutions = false,
                    GenericSendResolutions = true,
                },
            };

            var provider = new StoreConfigProvider(dataSource);
            await provider.SeedIfEmptyAsync(file, ct);

            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            var w = view.Webhooks;
            Assert.Equal("https://hooks.example.com/alert", w.GenericUrl);
            Assert.Equal("{\"Authorization\":\"Bearer abc\"}", w.GenericHeaders);
            Assert.Equal("{\"text\":\"{{metric}} on {{server}}\"}", w.GenericBodyTemplate);
            Assert.Equal("http://proxy.example.com:8080", w.GenericProxy);
            Assert.Equal("0123456789abcdef0123456789abcdef", w.PagerDutyRoutingKey);
            Assert.True(w.PagerDutyUseEuRegion);
            Assert.Equal("http://pd-proxy.example.com:8080", w.PagerDutyProxy);
            Assert.False(w.PagerDutyAutoResolve);
            Assert.True(w.SlackSendResolutions);
            Assert.False(w.TeamsSendResolutions);
            Assert.True(w.GenericSendResolutions);
            Assert.False(view.Smtp.SendResolutions);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private const string SeedTeams = "https://hooks.example.com/teams";
    private const string SeedSlack = "https://hooks.example.com/slack";
    private const string SeedGeneric = "https://hooks.example.com/alert";
    private const string SeedHeaders = "{\"Authorization\":\"Bearer abc\"}";
    private const string SeedPagerDuty = "0123456789abcdef0123456789abcdef";

    private static readonly string[] s_webhookColumns = { "teams_url", "slack_url", "generic_url", "generic_headers", "pagerduty_routing_key" };

    private static DarlingConfig WebhookSeedFile() => new()
    {
        Webhooks = new WebhooksConfig
        {
            TeamsUrl = SeedTeams,
            TeamsProxy = "http://proxy.example.com:8080",
            SlackUrl = SeedSlack,
            GenericUrl = SeedGeneric,
            GenericHeaders = SeedHeaders,
            GenericProxy = "http://generic-proxy.example.com:8080",
            PagerDutyRoutingKey = SeedPagerDuty,
            PagerDutyProxy = "http://pd-proxy.example.com:8080",
        },
    };

    private static async Task<string[]> ReadWebhookColumnsAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            "SELECT teams_url, slack_url, generic_url, generic_headers, pagerduty_routing_key FROM config_notification WHERE id = 1");
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return Enumerable.Range(0, s_webhookColumns.Length).Select(reader.GetString).ToArray();
    }

    private static async Task WithSeededScratchStoreAsync(Func<StoreConfigProvider, NpgsqlDataSource, CancellationToken, Task> body)
    {
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
            await provider.SeedIfEmptyAsync(WebhookSeedFile(), ct);
            await body(provider, dataSource, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AfterTheRingLoads_TheStartSealsTheWebhookValuesTheSeedCopiedAsPlainText_AndTheyOpenToTheFileValues()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the #6715 start seal (it mints its own scratch database).");
        await WithSeededScratchStoreAsync(async (provider, dataSource, ct) =>
        {
            Assert.All(await ReadWebhookColumnsAsync(dataSource, ct), value => Assert.False(PasswordSeal.IsSealed(value)));

            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);
            await provider.SealPlainWebhookSettingsAsync(ring, ct);

            var stored = await ReadWebhookColumnsAsync(dataSource, ct);
            for (var i = 0; i < stored.Length; i++)
            {
                Assert.True(PasswordSeal.IsSealed(stored[i]), $"{s_webhookColumns[i]} is still plain text after the start's seal.");
            }

            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            var opened = DarlingWebhookSecrets.Open(view.Webhooks, view.NotificationRoutes, ring);
            Assert.Empty(opened.Failures);
            Assert.Equal(SeedTeams, opened.Webhooks.TeamsUrl);
            Assert.Equal(SeedSlack, opened.Webhooks.SlackUrl);
            Assert.Equal(SeedGeneric, opened.Webhooks.GenericUrl);
            Assert.Equal(SeedHeaders, opened.Webhooks.GenericHeaders);
            Assert.Equal(SeedPagerDuty, opened.Webhooks.PagerDutyRoutingKey);

            // A second start finds nothing plain and writes nothing.
            await provider.SealPlainWebhookSettingsAsync(ring, ct);
            Assert.Equal(stored, await ReadWebhookColumnsAsync(dataSource, ct));
        });
    }

    [Fact]
    public async Task ARingThatCannotSeal_LeavesTheSeededPlainTextValuesInPlace_AndALaterStartSealsThem()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the #6715 start seal (it mints its own scratch database).");
        await WithSeededScratchStoreAsync(async (provider, dataSource, ct) =>
        {
            await provider.SealPlainWebhookSettingsAsync(DarlingPasswordKey.Refusing(DarlingPasswordKey.NotReadyReason), ct);
            Assert.Equal(new[] { SeedTeams, SeedSlack, SeedGeneric, SeedHeaders, SeedPagerDuty }, await ReadWebhookColumnsAsync(dataSource, ct));

            using var key = PasswordPrivateKey.Generate();
            await provider.SealPlainWebhookSettingsAsync(DarlingPasswordKey.FromPrivateKey(key), ct);
            Assert.All(await ReadWebhookColumnsAsync(dataSource, ct), value => Assert.True(PasswordSeal.IsSealed(value)));
        });
    }

    [Fact]
    public async Task AValueSavedBetweenTheReadAndTheWrite_IsNotOverwritten_AndTheHeadersStayWithTheirUrl()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the #6715 start seal (it mints its own scratch database).");
        await WithSeededScratchStoreAsync(async (provider, dataSource, ct) =>
        {
            const string SavedTeams = "https://hooks.example.com/teams-saved-meanwhile";
            const string SavedGeneric = "https://hooks.example.com/generic-saved-meanwhile";
            using var key = PasswordPrivateKey.Generate();

            // A save that lands after the pass read the row: a new Teams URL, and a new generic URL with the old headers kept.
            await provider.SealPlainWebhookSettingsAsync(DarlingPasswordKey.FromPrivateKey(key), ct, async token =>
            {
                await using var save = dataSource.CreateCommand("UPDATE config_notification SET teams_url = $1, generic_url = $2 WHERE id = 1");
                save.Parameters.AddWithValue(SavedTeams);
                save.Parameters.AddWithValue(SavedGeneric);
                Assert.Equal(1, await save.ExecuteNonQueryAsync(token));
            });

            var stored = await ReadWebhookColumnsAsync(dataSource, ct);
            Assert.Equal(SavedTeams, stored[0]);
            Assert.Equal(SavedGeneric, stored[2]);
            // The headers were sealed for the URL the pass read, so they are not written once that URL changed.
            Assert.Equal(SeedHeaders, stored[3]);
            // The values the save did not touch are sealed.
            Assert.True(PasswordSeal.IsSealed(stored[1]));
            Assert.True(PasswordSeal.IsSealed(stored[4]));
        });
    }

    [Fact]
    public async Task WhileASigningKeyIsSaved_TheStartLeavesTheGenericUrlAndHeadersAsPlainText_AndTheKeyStillOpens()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the #6715 start seal (it mints its own scratch database).");
        await WithSeededScratchStoreAsync(async (provider, dataSource, ct) =>
        {
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);

            /* Save a key bound to the generic URL's plain text, with the Viewer's own binding, sealer and write statement. The Viewer's
               Generate action refuses a URL that is not sealed yet, so the test composes its steps itself. */
            await using var viewer = new ViewerDataService(dataSource.ConnectionString);
            var state = await viewer.GetSettingsSigningKeyStateAsync(ct);
            Assert.NotNull(state);
            Assert.Equal(SeedGeneric, state.StoredUrl);
            var signingKey = WebhookSigning.NewKey();
            var sealedKey = new ViewerPasswordSealer(key.PublicKey)
                .SealWebhook(signingKey, ViewerWebhookSigningKeys.BindingFor(PasswordBinding.WebhookSettingsRow, state));
            await using (var write = dataSource.CreateCommand(ViewerDataService.SettingsSigningKeyWriteSql))
            {
                write.Parameters.AddWithValue(sealedKey);
                write.Parameters.AddWithValue(state.StoredUrl);
                write.Parameters.AddWithValue(state.Proxy);
                write.Parameters.AddWithValue(WebhookSigning.RotationOverlapDays);
                Assert.Equal(1, await write.ExecuteNonQueryAsync(ct));
            }

            await provider.SealPlainWebhookSettingsAsync(ring, ct);

            // The URL stays plain text, so the key keeps opening, and the headers (bound to the URL's text) stay plain text with it.
            var stored = await ReadWebhookColumnsAsync(dataSource, ct);
            Assert.Equal(SeedGeneric, stored[2]);
            Assert.Equal(SeedHeaders, stored[3]);
            // The values with no key behind them are sealed.
            Assert.True(PasswordSeal.IsSealed(stored[0]));
            Assert.True(PasswordSeal.IsSealed(stored[1]));
            Assert.True(PasswordSeal.IsSealed(stored[4]));

            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            var opened = DarlingWebhookSecrets.Open(view.Webhooks, view.NotificationRoutes, ring);
            Assert.Empty(opened.Failures);
            Assert.Equal(SeedGeneric, opened.Webhooks.GenericUrl);
            Assert.Equal(SeedHeaders, opened.Webhooks.GenericHeaders);
            Assert.Equal(signingKey, opened.Webhooks.GenericSigning!.Key);
        });
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
