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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5825 L3: a clear handled after a service restart (a NEW deliverer over the same history) still goes where its fire
/// went, a route deleted since the fire sends nothing and says why, a mute rule scoped to the fire's database stops the
/// clear, and every channel's resolution carries the facts. Driven through <see cref="DarlingAlertDeliverer"/> against
/// loopback endpoints, over a history store that answers <see cref="IDeliveredFireReader"/> from the rows it was given.
/// </summary>
public sealed class AlertResolutionRestartTests
{
    private const string ServerKey = "7";
    private const string Server = "SQL01";
    private const string Metric = "High CPU";

    private static readonly DateTime FiredAt = new(2026, 10, 15, 12, 0, 0, DateTimeKind.Utc);

    private static AlertResolution CpuResolution() =>
        new(ServerKey, Server, Metric, "CPU Resolved", $"{Server}: Total CPU back to 12%");

    private static AlertOutcome Fire(string? detailText = null) =>
        new(ServerKey, Server, Metric, "97%", "90%", Context: null, DetailText: detailText,
            NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null);

    /// <summary>The fire goes out on one deliverer; the clear is handled by a new one (a restart) that never saw it. The
    /// clear still reaches Slack, with the fire's time and value, and a second clear sends nothing.</summary>
    [Fact]
    public async Task AClearAfterARestart_GoesToTheDestinationTheFireReached()
    {
        using var slack = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        var store = new HistoryBackedStore();

        await Build(config, store).DeliverAsync(Fire(), TestContext.Current.CancellationToken);
        store.Now = FiredAt.AddMinutes(1);
        var restarted = Build(config, store);
        await restarted.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, slack.Bodies.Count);
        Assert.Contains("CPU Resolved", slack.Bodies[1], StringComparison.Ordinal);
        Assert.Contains("Fired: 2026-10-15 12:00:00 UTC", slack.Bodies[1], StringComparison.Ordinal);
        Assert.Contains("Lasted: 1m 0s", slack.Bodies[1], StringComparison.Ordinal);
        Assert.Contains("Last value: 97", slack.Bodies[1], StringComparison.Ordinal);
        var row = store.Records[^1];
        Assert.Equal("sent", row.ResolutionOutcome);
        Assert.Equal(NotificationRouter.SlackChannel, row.ResolutionChannels);
        Assert.NotNull(row.ResolutionSentAtUtc);

        /* The resolution row closes the episode: a second clear after another restart finds no open fire. */
        await Build(config, store).DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, slack.Bodies.Count);
        Assert.Null(store.Records[^1].ResolutionOutcome);
    }

    /// <summary>The fire went out through a route that is deleted before the clear: nothing is sent, and the row says
    /// which channel lost its route.</summary>
    [Fact]
    public async Task AClearAfterARestart_WhoseRouteWasDeleted_SendsNothing_AndTheRowSaysWhy()
    {
        using var routed = new CapturingWebhookEndpoint();
        var before = new DarlingConfig();
        before.Webhooks.SlackSendResolutions = true;
        before.NotificationRoutes = new[] { new NotificationRoute(1, Metric, "", routed.Url, "", "", "", true) };
        var after = new DarlingConfig();
        after.Webhooks.SlackSendResolutions = true;
        var store = new HistoryBackedStore();

        await Build(before, store).DeliverAsync(Fire(), TestContext.Current.CancellationToken);
        store.Now = FiredAt.AddMinutes(5);
        await Build(after, store).DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Single(routed.Bodies);
        var row = store.Records[^1];
        Assert.Equal("route deleted or disabled: Slack", row.ResolutionOutcome);
        Assert.Equal(NotificationRouter.SlackChannel, row.ResolutionChannels);
        Assert.Null(row.ResolutionSentAtUtc);
        Assert.False(row.AlertSent);
    }

    /// <summary>A mute rule scoped to the fire's database stops the clear, from memory and after a restart alike; a rule on
    /// another database does not.</summary>
    [Theory]
    [InlineData(false, "Sales", 1)]
    [InlineData(true, "Sales", 1)]
    [InlineData(false, "Other", 2)]
    [InlineData(true, "Other", 2)]
    public async Task ADatabaseScopedMuteRule_MatchesTheClear_AsItMatchesTheFire(bool restart, string mutedDatabase, int slackPosts)
    {
        using var slack = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = slack.Url;
        config.Webhooks.SlackSendResolutions = true;
        var store = new HistoryBackedStore();
        var muteOn = false;
        bool IsMuted(AlertMuteContext context) =>
            muteOn && context.MetricName == Metric && string.Equals(context.DatabaseName, mutedDatabase, StringComparison.Ordinal);

        var first = Build(config, store, IsMuted);
        await first.DeliverAsync(Fire("CPU is high.\n  Database: Sales"), TestContext.Current.CancellationToken);
        muteOn = true;
        var clearing = restart ? Build(config, store, IsMuted) : first;
        await clearing.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(slackPosts, slack.Bodies.Count);
        Assert.Equal(slackPosts == 1 ? "muted" : "sent", store.Records[^1].ResolutionOutcome);
    }

    /// <summary>Each channel's resolution says which alert cleared, when it fired and cleared, how long it lasted, its last
    /// value and the fire's triage link, and carries no empty threshold row.</summary>
    [Theory]
    [InlineData(NotificationRouter.SlackChannel)]
    [InlineData(NotificationRouter.TeamsChannel)]
    [InlineData(NotificationRouter.GenericChannel)]
    [InlineData(NotificationRouter.EmailChannel)]
    public async Task EachChannelsResolution_CarriesTheFacts(string channel)
    {
        using var webhook = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();
        var config = new DarlingConfig();
        config.Web.Enabled = true;
        config.Web.PublicBaseUrl = "https://monitor.example.invalid";
        switch (channel)
        {
            case NotificationRouter.SlackChannel:
                config.Webhooks.SlackUrl = webhook.Url;
                config.Webhooks.SlackSendResolutions = true;
                break;
            case NotificationRouter.TeamsChannel:
                config.Webhooks.TeamsUrl = webhook.Url;
                config.Webhooks.TeamsSendResolutions = true;
                break;
            case NotificationRouter.GenericChannel:
                config.Webhooks.GenericUrl = webhook.Url;
                config.Webhooks.GenericSendResolutions = true;
                break;
            default:
                config.Smtp.Host = "127.0.0.1";
                config.Smtp.Port = smtp.Port;
                config.Smtp.UseSsl = false;
                config.Smtp.From = "monitor@example.invalid";
                config.Smtp.To = "operator@example.invalid";
                config.Smtp.SendResolutions = true;
                break;
        }

        var store = new HistoryBackedStore();
        var deliverer = Build(config, store);
        await deliverer.DeliverAsync(Fire(), TestContext.Current.CancellationToken);
        store.Now = FiredAt.AddMinutes(61);
        await deliverer.DeliverResolutionAsync(CpuResolution(), cancellationToken: TestContext.Current.CancellationToken);

        var sent = channel == NotificationRouter.EmailChannel ? smtp.Messages : webhook.Bodies;
        Assert.Equal(2, sent.Count);
        var clear = sent[1];
        Assert.Equal("sent", store.Records[^1].ResolutionOutcome);
        Assert.Contains("CPU Resolved", clear, StringComparison.Ordinal);
        Assert.Contains(Server, clear, StringComparison.Ordinal);
        Assert.Contains(Metric, clear, StringComparison.Ordinal);
        Assert.Contains("2026-10-15 12:00:00", clear, StringComparison.Ordinal);
        Assert.Contains("2026-10-15 13:01:00", clear, StringComparison.Ordinal);
        Assert.Contains("1h 1m", clear, StringComparison.Ordinal);
        Assert.Contains("97%", clear, StringComparison.Ordinal);
        /* The generic webhook's default template carries no triage link for a fire either; {{triage_url}} is there for a
           template that wants it. */
        if (channel != NotificationRouter.GenericChannel)
        {
            Assert.Contains("monitor.example.invalid", clear, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("Threshold", clear, StringComparison.Ordinal);
        if (channel == NotificationRouter.EmailChannel)
        {
            Assert.DoesNotContain("Current Value", clear, StringComparison.Ordinal);
            Assert.DoesNotContain("cooldown", clear, StringComparison.Ordinal);
        }
    }

    private static DarlingAlertDeliverer Build(
        DarlingConfig config, HistoryBackedStore store, Func<AlertMuteContext, bool>? isMuted = null)
    {
        var settings = new DarlingAlertSettings(config);
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, store);
        return new DarlingAlertDeliverer(settings, store, webhooks, NullLogger.Instance, isAlertMuted: isMuted, utcNow: () => store.Now);
    }

    /// <summary>A history that outlives the deliverer, as the store does across a restart: it keeps every row with the
    /// time it was written and answers <see cref="IDeliveredFireReader"/> the way the store's query does (delivered,
    /// unmuted fire rows of the alert after its last resolution row, newest first).</summary>
    private sealed class HistoryBackedStore : IAlertHistoryStore, IDeliveredFireReader
    {
        private readonly List<(DateTime At, AlertHistoryRecord Record)> _rows = new();

        public DateTime Now { get; set; } = FiredAt;

        public IReadOnlyList<AlertHistoryRecord> Records => _rows.Select(r => r.Record).ToList();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            _rows.Add((Now, record));
            return Task.CompletedTask;
        }

        public Task<DeliveredFireHistory?> GetOpenDeliveredFiresAsync(
            string serverKey, string metricName, string resolutionTitle, CancellationToken cancellationToken = default)
        {
            var lastClear = _rows.FindLastIndex(r => r.Record.ServerId == serverKey && r.Record.MetricName == resolutionTitle);
            var open = _rows.Skip(lastClear + 1)
                .Where(r => r.Record.ServerId == serverKey && r.Record.MetricName == metricName && r.Record.AlertSent && !r.Record.Muted)
                .Reverse()
                .ToList();
            return Task.FromResult(open.Count == 0
                ? null
                : new DeliveredFireHistory(
                    open[^1].At,
                    open.Select(r => new DeliveredFireRow(r.At, r.Record.NumericCurrentValue, r.Record.DetailText, r.Record.ContextJson)).ToList()));
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }
}
