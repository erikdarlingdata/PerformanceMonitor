/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5825: an alert's clear goes to the channels its delivered fire reached, when the store sends resolutions. Driven
/// through <see cref="DarlingAlertDeliverer"/> against loopback endpoints (and PagerDuty's capture seam), reading the
/// posts the endpoints received and the history rows the deliverer wrote. One test per acceptance test in the issue.
/// </summary>
public sealed class AlertResolutionDeliveryTests
{
    private const string ServerKey = "7";
    private const string Server = "SQL01";
    private const string Metric = "High CPU";

    private static readonly DateTime FiredAt = new(2026, 10, 15, 12, 0, 0, DateTimeKind.Utc);

    private static AlertResolution CpuResolution() =>
        new(ServerKey, Server, Metric, "CPU Resolved", $"{Server}: Total CPU back to 12%");

    /// <summary>With the setting off, a delivered fire's clear sends nothing and its row keeps <c>none</c>.</summary>
    [Fact]
    public async Task WithTheSettingOff_NoChannelReceivesAResolution_AndTheRowStaysNone()
    {
        using var slack = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        var rig = Rig.Build(config);

        await rig.FireAsync();
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Single(slack.Bodies);
        var row = rig.History.Records[^1];
        Assert.Equal(AlertDelivery.ChannelNotApplicable, row.NotificationType);
        Assert.Null(row.ResolutionOutcome);
        Assert.Null(row.ResolutionChannels);
        Assert.Null(row.ResolutionSentAtUtc);
    }

    /// <summary>With the setting on, a fire and its clear one minute apart both send: one fire post and one resolution
    /// post, the resolution in the resolution's own words with when it fired, cleared and how long it lasted. Its row
    /// records the channel and the outcome.</summary>
    [Fact]
    public async Task AFireAndItsClearOneMinuteApart_BothSend_AndTheRowRecordsTheChannelAndOutcome()
    {
        using var slack = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        var rig = Rig.Build(config);

        await rig.FireAsync();
        rig.Now = FiredAt.AddMinutes(1);
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, slack.Bodies.Count);
        var clear = slack.Bodies[1];
        Assert.Contains("CPU Resolved", clear, StringComparison.Ordinal);
        Assert.Contains("Total CPU back to 12%", clear, StringComparison.Ordinal);
        Assert.Contains("Fired: 2026-10-15 12:00:00 UTC", clear, StringComparison.Ordinal);
        Assert.Contains("Cleared: 2026-10-15 12:01:00 UTC", clear, StringComparison.Ordinal);
        Assert.Contains("Lasted: 1m 0s", clear, StringComparison.Ordinal);
        Assert.Contains("Last value: 97%", clear, StringComparison.Ordinal);

        var row = rig.History.Records[^1];
        Assert.Equal("CPU Resolved", row.MetricName);
        Assert.Equal(AlertDelivery.ChannelWebhook, row.NotificationType);
        Assert.True(row.AlertSent);
        Assert.Equal(NotificationRouter.SlackChannel, row.ResolutionChannels);
        Assert.Equal("sent", row.ResolutionOutcome);
        Assert.Equal(FiredAt.AddMinutes(1), row.ResolutionSentAtUtc);
    }

    /// <summary>The clear goes only where the fire went: Teams is set to send resolutions but the fire never reached it
    /// (it had no Teams destination then), so Teams gets nothing. The clear's email goes to the fire's recipients.</summary>
    [Fact]
    public async Task TheClearGoesOnlyToTheDestinationsTheFireReached()
    {
        using var slack = new CapturingWebhookEndpoint();
        using var teams = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        config.Webhooks.TeamsSendResolutions = true;
        config.Smtp.SendResolutions = true;
        ConfigureSmtp(config, smtp);
        var rig = Rig.Build(config);

        await rig.FireAsync();
        /* Teams is set up only now, after the fire: the settings read the config live. */
        config.Webhooks.TeamsUrl = teams.Url;
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Empty(teams.Bodies);
        Assert.Equal(2, slack.Bodies.Count);
        Assert.Equal(2, smtp.Messages.Count);
        Assert.Contains("CPU Resolved", smtp.Messages[1], StringComparison.Ordinal);
        var row = rig.History.Records[^1];
        Assert.Equal(AlertDelivery.ChannelEmailAndWebhook, row.NotificationType);
        Assert.Equal("Slack,Email", row.ResolutionChannels);
        Assert.Equal("sent", row.ResolutionOutcome);
    }

    /// <summary>A clear whose alert had no delivered fire sends nothing: here the fire was muted, so nothing went out,
    /// and the clear stays a <c>none</c> row. A clear with no fire at all is the same.</summary>
    [Fact]
    public async Task AClearWhoseAlertHadNoDeliveredFire_SendsNothing()
    {
        using var slack = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        var rig = Rig.Build(config);

        await rig.Deliverer.DeliverAsync(Fire() with { Muted = true }, TestContext.Current.CancellationToken);
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);
        await rig.Deliverer.DeliverResolutionAsync(
            new AlertResolution(ServerKey, Server, "Blocking Detected", "Blocking Cleared", $"{Server}: no blocking"),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Empty(slack.Bodies);
        Assert.All(rig.History.Records.Skip(1), row =>
        {
            Assert.Equal(AlertDelivery.ChannelNotApplicable, row.NotificationType);
            Assert.Null(row.ResolutionOutcome);
        });
    }

    /// <summary>A mute rule that matches the alert stops its resolution, and the row records it as muted with the
    /// channels it would have gone to.</summary>
    [Fact]
    public async Task AMuteRuleThatMatchesTheAlert_StopsItsResolution_AndTheRowSaysMuted()
    {
        using var slack = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        var muteOn = false;
        var rig = Rig.Build(config, isMuted: context => muteOn && context.MetricName == Metric && context.ServerId == 7);

        await rig.FireAsync();
        muteOn = true;
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Single(slack.Bodies);
        var row = rig.History.Records[^1];
        Assert.Equal(AlertDelivery.ChannelMuted, row.NotificationType);
        Assert.True(row.Muted);
        Assert.Equal("muted", row.ResolutionOutcome);
        Assert.Equal(NotificationRouter.SlackChannel, row.ResolutionChannels);
        Assert.Null(row.ResolutionSentAtUtc);
    }

    /// <summary>A route that turns resolutions off for its destinations overrides the channel setting; one that turns
    /// them on overrides a channel that is off.</summary>
    [Theory]
    [InlineData(true, false, 1)]
    [InlineData(false, true, 2)]
    public async Task ARouteOverride_BeatsTheChannelSetting(bool channelFlag, bool routeOverride, int slackPosts)
    {
        using var routed = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackSendResolutions = channelFlag;
        config.NotificationRoutes = new[]
        {
            new NotificationRoute(1, Metric, "", routed.Url, "", "", "", true) { SendResolutions = routeOverride },
        };
        var rig = Rig.Build(config);

        await rig.FireAsync();
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(slackPosts, routed.Bodies.Count);
    }

    /// <summary>The deliverer that delivered the fire still remembers it at the clear (no restart between). A route
    /// disabled or deleted since the fire gets nothing, exactly as after a restart: no post to the destination the fire
    /// reached, and the row says why (#5855 review: this path used to post the clear and record it sent).</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AClearWhoseRouteWasDisabledOrDeletedSinceTheFire_SendsNothing_AndTheRowSaysWhy_WithoutARestart(bool deleted)
    {
        using var routed = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackSendResolutions = true;
        var route = new NotificationRoute(1, Metric, "", routed.Url, "", "", "", true);
        config.NotificationRoutes = new[] { route };
        var rig = Rig.Build(config);

        await rig.FireAsync();
        Assert.Single(routed.Bodies);
        config.NotificationRoutes = deleted ? Array.Empty<NotificationRoute>() : new[] { route with { Enabled = false } };
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Single(routed.Bodies);
        var row = rig.History.Records[^1];
        Assert.Equal("route deleted or disabled: Slack", row.ResolutionOutcome);
        Assert.Equal(NotificationRouter.SlackChannel, row.ResolutionChannels);
        Assert.Null(row.ResolutionSentAtUtc);
        Assert.False(row.AlertSent);
    }

    /// <summary>A resolution whose post fails says so on its row, naming the channel and not the error (which can carry
    /// the URL), and records no sent time.</summary>
    [Fact]
    public async Task AResolutionWhosePostFails_IsRecordedAsFailed()
    {
        using var slack = new CapturingWebhookEndpoint(statusSequence: new[] { 200, 500 });
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        var rig = Rig.Build(config);

        await rig.FireAsync();
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        var row = rig.History.Records[^1];
        Assert.Equal(AlertDelivery.ChannelFailed, row.NotificationType);
        Assert.Equal("failed: Slack", row.ResolutionOutcome);
        Assert.Null(row.ResolutionSentAtUtc);
    }

    /// <summary>"Server Restored" and "AG Replica Reconnected" are each sent once, as today: the recovery alert is the
    /// clear, and a resolution of the firing side sends nothing more.</summary>
    [Theory]
    [InlineData("Server Unreachable", "Server Restored")]
    [InlineData("AG Replica Disconnected", "AG Replica Reconnected")]
    public async Task TheTwoRecoveryPairs_AreEachSentOnce(string firing, string recovery)
    {
        using var slack = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        var rig = Rig.Build(config);

        await rig.Deliverer.DeliverAsync(Fire() with { MetricName = firing }, TestContext.Current.CancellationToken);
        await rig.Deliverer.DeliverAsync(Fire() with { MetricName = recovery }, TestContext.Current.CancellationToken);
        await rig.Deliverer.DeliverResolutionAsync(
            new AlertResolution(ServerKey, Server, firing, recovery, $"{Server}: {recovery}"),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, slack.Bodies.Count);
        Assert.Single(slack.Bodies, body => body.Contains(recovery, StringComparison.Ordinal));
        Assert.Equal(AlertDelivery.ChannelNotApplicable, rig.History.Records[^1].NotificationType);
    }

    /// <summary>A clear from another service (#5654) follows the same setting: the drain hands it over with its source
    /// (so a source-scoped mute rule can match it) and without the PagerDuty close, which the drain already sent.</summary>
    [Fact]
    public async Task AClearFromAnotherService_FollowsTheSameSetting()
    {
        const string external = "External:backup:nightly";
        using var slack = new CapturingWebhookEndpoint();
        var pagerDuty = new List<string>();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        config.Webhooks.PagerDutyRoutingKey = "rk-test";
        config.Webhooks.PagerDutyAutoResolve = true;
        string? mutedSource = null;
        var rig = Rig.Build(config, pagerDuty, context =>
        {
            mutedSource = context.ExternalSource;
            return false;
        });

        await rig.Deliverer.DeliverAsync(Fire() with { MetricName = external }, TestContext.Current.CancellationToken);
        await rig.Deliverer.DeliverResolutionAsync(
            new AlertResolution(ServerKey, Server, external, "External resolved: backup nightly", $"{Server}: nightly"),
            new AlertResolutionSendOptions("backup", ClosePagerDuty: false, ContextJson: "{}"),
            TestContext.Current.CancellationToken);

        Assert.Equal(2, slack.Bodies.Count);
        Assert.Single(pagerDuty);
        Assert.Equal("backup", mutedSource);
        var row = rig.History.Records[^1];
        /* #6003, D7: the clear's row names the metric it resolves, and the caller's empty context adds nothing else. */
        Assert.Equal("{\"Resolves\":\"External:backup:nightly\"}", row.ContextJson);
        Assert.Equal(NotificationRouter.SlackChannel, row.ResolutionChannels);
        Assert.Equal("sent", row.ResolutionOutcome);
    }

    /// <summary>With <c>pagerduty_auto_resolve</c> on, the clear of an engine alert closes the PagerDuty incident its fire
    /// opened: a <c>resolve</c> on the trigger's dedup key, to the fire's service.</summary>
    [Fact]
    public async Task WithAutoResolveOn_AnEngineAlertsClear_ClosesItsPagerDutyIncident()
    {
        var pagerDuty = new List<string>();
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyRoutingKey = "rk-test";
        config.Webhooks.PagerDutyAutoResolve = true;
        var rig = Rig.Build(config, pagerDuty);

        await rig.FireAsync();
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, pagerDuty.Count);
        var trigger = JsonDocument.Parse(pagerDuty[0]).RootElement;
        var resolve = JsonDocument.Parse(pagerDuty[1]).RootElement;
        Assert.Equal("trigger", trigger.GetProperty("event_action").GetString());
        Assert.Equal("resolve", resolve.GetProperty("event_action").GetString());
        Assert.Equal(trigger.GetProperty("dedup_key").GetString(), resolve.GetProperty("dedup_key").GetString());
        Assert.Equal("rk-test", resolve.GetProperty("routing_key").GetString());
        var row = rig.History.Records[^1];
        Assert.Equal(NotificationRouter.PagerDutyChannel, row.ResolutionChannels);
        Assert.Equal("sent", row.ResolutionOutcome);
    }

    /// <summary>With <c>pagerduty_auto_resolve</c> off, PagerDuty behaves as today: the fire triggers, the clear posts
    /// nothing, whatever the other channels' settings say, and the row stays <c>none</c>.</summary>
    [Fact]
    public async Task WithAutoResolveOff_PagerDutyBehavesAsToday()
    {
        var pagerDuty = new List<string>();
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyRoutingKey = "rk-test";
        config.Webhooks.SlackSendResolutions = true;
        config.Webhooks.TeamsSendResolutions = true;
        config.Webhooks.GenericSendResolutions = true;
        var rig = Rig.Build(config, pagerDuty);

        await rig.FireAsync();
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Single(pagerDuty);
        Assert.Equal("trigger", JsonDocument.Parse(pagerDuty[0]).RootElement.GetProperty("event_action").GetString());
        Assert.Equal(AlertDelivery.ChannelNotApplicable, rig.History.Records[^1].NotificationType);
    }

    /// <summary>The clear reaches each destination once per episode: a second clear with no fire between sends nothing.</summary>
    [Fact]
    public async Task ASecondClearWithNoFireBetween_SendsNothing()
    {
        using var slack = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        var rig = Rig.Build(config);

        await rig.FireAsync();
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, slack.Bodies.Count);
        Assert.Equal(AlertDelivery.ChannelNotApplicable, rig.History.Records[^1].NotificationType);
    }

    /// <summary>#6928: a clear goes to the channels the fire reached, not to wherever the severity filter would send it now. The fire
    /// was Critical and went to the Critical-only route's endpoint. The routes' filters are then swapped (the other endpoint's route has the lower id, so a re-match at the clear would pick it), and the clear still goes to
    /// that endpoint and never to the other.</summary>
    [Fact]
    public async Task AClear_GoesToTheChannelTheFireReached_WhateverTheSeverityFiltersSayAtTheClear()
    {
        using var criticalEndpoint = new CapturingWebhookEndpoint();
        using var warningEndpoint = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackSendResolutions = true;
        NotificationRoute[] RoutesWith(string first, string second) => new[]
        {
            new NotificationRoute(2, Metric, "", criticalEndpoint.Url, "", "", "", true) { Severities = first },
            new NotificationRoute(1, Metric, "", warningEndpoint.Url, "", "", "", true) { Severities = second },
        };
        config.NotificationRoutes = RoutesWith("critical", "warning");
        var rig = Rig.Build(config);

        await rig.Deliverer.DeliverAsync(Fire() with { Severity = AlertSeverityLevel.Critical }, TestContext.Current.CancellationToken);
        Assert.Single(criticalEndpoint.Bodies);
        Assert.Empty(warningEndpoint.Bodies);

        config.NotificationRoutes = RoutesWith("warning", "critical");
        await rig.Deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, criticalEndpoint.Bodies.Count);
        Assert.Empty(warningEndpoint.Bodies);
    }

    private static AlertOutcome Fire() =>
        new(ServerKey, Server, Metric, "97%", "90%", Context: null, DetailText: null,
            NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null);

    private static void ConfigureSmtp(DarlingConfig config, CapturingSmtpEndpoint smtp)
    {
        config.Smtp.Host = "127.0.0.1";
        config.Smtp.Port = smtp.Port;
        config.Smtp.UseSsl = false;
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";
    }

    private sealed class Rig
    {
        public required DarlingAlertDeliverer Deliverer { get; init; }

        public required CapturingHistoryStore History { get; init; }

        public DateTime Now { get; set; } = FiredAt;

        public Task FireAsync() => Deliverer.DeliverAsync(Fire(), TestContext.Current.CancellationToken);

        public static Rig Build(DarlingConfig config, List<string>? pagerDuty = null, Func<AlertMuteContext, bool>? isMuted = null)
        {
            var settings = new DarlingAlertSettings(config);
            var history = new CapturingHistoryStore();
            var webhooks = new WebhookAlertService(
                settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
            webhooks.PostPagerDutyAsyncOverride = (_, payload) =>
            {
                pagerDuty?.Add(payload);
                return Task.FromResult<string?>(null);
            };
            Rig? rig = null;
            var deliverer = new DarlingAlertDeliverer(
                settings, history, webhooks, NullLogger.Instance, isAlertMuted: isMuted, utcNow: () => rig!.Now);
            rig = new Rig { Deliverer = deliverer, History = history };
            return rig;
        }
    }
}
