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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Notification Channel Failing self-alert for the SMTP relay (#8245). The webhook destinations got the alert in
/// #4750: three failures in a row raise it once, and the next delivery writes the Recovered row. Alert email failed the same
/// way and raised nothing, because the evaluator's seam read only the webhook counts. The email core now counts its sends on
/// the ledger that seam reads, as the Email entry, and the alert's text names email and the email core's log lines, never a
/// relay or an address. The failures here are the send refusing a recipient list before any connection; the recovery is a
/// delivery to the loopback relay.
/// </summary>
public sealed class EmailChannelFailingSelfAlertTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private const string FailingName = "Notification Channel Failing";
    private const string RecoveredName = "Notification Channel Recovered";

    /// <summary>A recipient list the send refuses before it connects anywhere (two addresses joined by a semicolon).</summary>
    private const string Refused = "ops@example.invalid;second@example.invalid";

    private const string Accepted = "ops@example.invalid";

    /// <summary>SMTP-only settings the test can change, which is how an operator turns the channel off or fixes it.</summary>
    private sealed class EmailSettings : IAlertSettings
    {
        public bool SmtpEnabled { get; set; } = true;
        public string SmtpServer { get; set; } = "127.0.0.1";
        public int SmtpPort { get; set; } = 25;
        public string SmtpRecipients { get; set; } = Refused;
        public IReadOnlyList<NotificationRoute> NotificationRoutes => Array.Empty<NotificationRoute>();

        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "monitor@example.invalid";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled => false;
        public string PagerDutyRoutingKey => "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity => 0.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 10;
        public string TriageBaseUrl => "";
    }

    /// <summary>The real webhook service and email core on SMTP-only settings, and an evaluator reading the service's counts
    /// through the seam the worker wires.</summary>
    private sealed class Rig
    {
        public EmailSettings Email { get; } = new();
        public DarlingSelfAlertTests.FakeSettings Settings { get; } = new();
        public DarlingSelfAlertTests.RecordingDeliverer Deliverer { get; } = new();
        public DarlingSelfAlertTests.FakeHistoryStore History { get; } = new();
        public WebhookAlertService Webhooks { get; }
        public EmailSendCore Core { get; }
        public DarlingSelfAlertEvaluator Evaluator { get; }

        public Rig()
        {
            Webhooks = new WebhookAlertService(Email, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);
            Core = new EmailSendCore(Email, new DiscardingHistoryStore(), Webhooks, DarlingAlertDeliverer.Branding, NullLogger.Instance);
            Evaluator = new DarlingSelfAlertEvaluator(
                Settings, Deliverer, History, _ => false,
                webhookChannelFailures: Webhooks.GetChannelFailureCounts);
        }

        /// <summary>One alert email from a distinct server, so no cooldown stands between two sends.</summary>
        public async Task<AlertChannelOutcome> SendAsync()
        {
            var server = "SQL" + Interlocked.Increment(ref s_serverSequence);
            var result = await Core.TrySendAsync("High CPU", server, "97%", "90%", serverId: server, context: null, attemptChannels: true);
            return result.EmailOutcome;
        }

        public async Task FailAsync(int times)
        {
            for (var i = 0; i < times; i++)
            {
                Assert.Equal(AlertChannelOutcome.Failed, await SendAsync());
            }
        }
    }

    private static int s_serverSequence;

    [Fact]
    public async Task ThreeRefusedEmails_RaiseTheAlertOnceForEmail_AndADeliveryWritesTheRecoveredRow()
    {
        using var relay = new CapturingSmtpEndpoint();
        var rig = new Rig();

        await rig.FailAsync(2);
        await rig.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Empty(rig.Deliverer.Outcomes);

        await rig.FailAsync(1);
        await rig.Evaluator.ApplyNotificationChannelsAsync(Ct);

        var fired = Assert.Single(rig.Deliverer.Outcomes);
        Assert.Equal(FailingName, fired.MetricName);
        Assert.Contains(NotificationRouter.EmailChannel, fired.ServerKey, StringComparison.Ordinal);
        Assert.Contains("Email channel", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("3 times in a row", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("ALERT EMAIL FAILED", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("webhook", fired.DetailText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("example.invalid", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("email", fired.ShortMessage, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(3, fired.NumericCurrentValue);
        Assert.Equal(WebhookAlertService.FailingChannelThreshold, fired.NumericThresholdValue);
        Assert.Empty(rig.History.Records);

        /* Still failing: the edge does not re-fire. */
        await rig.FailAsync(2);
        await rig.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.Deliverer.Outcomes);

        rig.Email.SmtpRecipients = Accepted;
        rig.Email.SmtpPort = relay.Port;
        Assert.Equal(AlertChannelOutcome.Delivered, await rig.SendAsync());
        await rig.Evaluator.ApplyNotificationChannelsAsync(Ct);

        var recovered = Assert.Single(rig.History.Records);
        Assert.Equal(RecoveredName, recovered.MetricName);
        Assert.Contains(NotificationRouter.EmailChannel, recovered.ServerId, StringComparison.Ordinal);
        Assert.Contains("Email channel", recovered.DetailText, StringComparison.Ordinal);
        Assert.Contains("delivering again", recovered.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("webhook", recovered.DetailText, StringComparison.OrdinalIgnoreCase);
        Assert.Single(rig.Deliverer.Outcomes);

        await rig.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.History.Records);
    }

    [Fact]
    public async Task TurningSmtpOffWhileFailing_ClosesTheAlertAsTurnedOff()
    {
        var rig = new Rig();

        await rig.FailAsync(3);
        await rig.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.Deliverer.Outcomes);

        rig.Email.SmtpEnabled = false;
        await rig.Evaluator.ApplyNotificationChannelsAsync(Ct);

        var closed = Assert.Single(rig.History.Records);
        Assert.Equal(RecoveredName, closed.MetricName);
        Assert.Contains("Email channel was turned off", closed.DetailText, StringComparison.Ordinal);
        Assert.Contains("SMTP", closed.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("webhook", closed.DetailText, StringComparison.OrdinalIgnoreCase);

        /* Off and quiet: nothing more is written, and the count is forgotten with the destination. */
        await rig.Evaluator.ApplyNotificationChannelsAsync(Ct);
        Assert.Single(rig.History.Records);
        rig.Email.SmtpEnabled = true;
        Assert.Equal(0, rig.Webhooks.GetChannelFailureCounts().Single(c => c.Channel == NotificationRouter.EmailChannel).ConsecutiveFailures);
    }
}
