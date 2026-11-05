/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7086: <see cref="AlertOutcome.Hold"/> through the real <see cref="DarlingAlertDeliverer"/>, a loopback webhook and a
/// loopback SMTP server. A held fire is one digest row and nothing on the wire, it leaves the send core's cooldown alone so a
/// real page for the same alert right after it goes out, and a mute rule wins over the hold.
/// </summary>
public sealed class AlertHoldDeliveryTests
{
    private const string Metric = "Store Checkpointer Pressure";
    private const string Reason = "Held for the digest: the condition has stood 5 of 30 minutes.";

    private static AlertOutcome Outcome(bool muted = false, AlertHold? hold = null, AlertContext? context = null) =>
        new("store:checkpointer", "Monitor Store", Metric, "9 s", "4 s",
            Context: context, DetailText: "The checkpointer is slow.", NumericCurrentValue: 9, NumericThresholdValue: 4,
            Muted: muted, Severity: AlertSeverityLevel.Warning, Hold: hold);

    private static (DarlingAlertDeliverer Deliverer, CapturingHistoryStore History) Build(
        CapturingWebhookEndpoint endpoint, CapturingSmtpEndpoint smtp, AlertNotificationMode mode = AlertNotificationMode.Summary)
    {
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = endpoint.Url;
        config.Smtp.Host = "127.0.0.1";
        config.Smtp.Port = smtp.Port;
        config.Smtp.UseSsl = false;
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";
        config.Alerts.DeliveryMode = mode;

        var settings = new DarlingAlertSettings(config);
        var history = new CapturingHistoryStore();
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        return (new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance), history);
    }

    [Fact]
    public async Task AHeldFire_WritesOneDigestRow_SendsNothing_AndARealPageRightAfterItGoesOut()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();
        var (deliverer, history) = Build(endpoint, smtp);
        var ct = TestContext.Current.CancellationToken;

        var held = await deliverer.DeliverAndReportAsync(Outcome(hold: new AlertHold(Reason)), ct);

        Assert.NotNull(held);
        Assert.False(held!.Sent);
        Assert.Equal(AlertDelivery.ChannelDigest, held.Channel);
        Assert.Empty(endpoint.Bodies);
        Assert.Empty(smtp.Messages);
        var row = Assert.Single(history.Records);
        Assert.False(row.AlertSent);
        Assert.False(row.Muted);
        Assert.Equal(AlertDelivery.ChannelDigest, row.NotificationType);
        var routing = AlertContextSerializer.TryReadRouting(row.ContextJson);
        Assert.NotNull(routing);
        Assert.Equal("digest", routing!.Route);
        Assert.Equal(Reason, routing.Reason);

        // The held fire did not stamp the cooldown: the same alert, not held, goes straight out on both channels.
        var page = await deliverer.DeliverAndReportAsync(Outcome(), ct);

        Assert.NotNull(page);
        Assert.True(page!.Sent);
        Assert.Single(endpoint.Bodies);
        Assert.Single(smtp.Messages);
        Assert.Equal(2, history.Records.Count);
        Assert.True(history.Records[1].AlertSent);
    }

    [Fact]
    public async Task AMutedAndHeldFire_RecordsAsMuted()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();
        var (deliverer, history) = Build(endpoint, smtp);

        var delivery = await deliverer.DeliverAndReportAsync(
            Outcome(muted: true, hold: new AlertHold(Reason)), TestContext.Current.CancellationToken);

        Assert.NotNull(delivery);
        Assert.Equal(AlertDelivery.ChannelMuted, delivery!.Channel);
        Assert.Empty(endpoint.Bodies);
        Assert.Empty(smtp.Messages);
        var row = Assert.Single(history.Records);
        Assert.True(row.Muted);
        Assert.Equal(AlertDelivery.ChannelMuted, row.NotificationType);
        Assert.Null(AlertContextSerializer.TryReadRouting(row.ContextJson));
    }

    [Fact]
    public async Task AHeldFireWithIncidents_InPerEventMode_IsStillOneDigestRow()
    {
        using var endpoint = new CapturingWebhookEndpoint();
        using var smtp = new CapturingSmtpEndpoint();
        var (deliverer, history) = Build(endpoint, smtp, AlertNotificationMode.PerEvent);
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident> { new("a", new[] { "dbo.Users" }), new("b", new[] { "dbo.Posts" }) },
        };

        await deliverer.DeliverAsync(Outcome(hold: new AlertHold(Reason), context: context), TestContext.Current.CancellationToken);

        var row = Assert.Single(history.Records);
        Assert.Equal(AlertDelivery.ChannelDigest, row.NotificationType);
        Assert.Empty(endpoint.Bodies);
    }
}
