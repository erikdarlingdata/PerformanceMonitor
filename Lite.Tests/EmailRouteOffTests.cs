/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the email path's reading of a route that turns email off (#8246). The webhook fan-out reads an off channel as
/// "send nothing": no fall-through to a less specific route or to the settings row. The email path fell back to the
/// settings row's default recipient list, so a route that turned email off for its alerts would have mailed them to the
/// defaults. No route store writes Email into a route's off channels today, so the pin is the fallback's safety. The
/// default list here is one the send refuses before it connects anywhere, so an attempted send is a Failed outcome and
/// costs no network.
/// </summary>
public sealed class EmailRouteOffTests
{
    private sealed class Settings : IAlertSettings
    {
        public IReadOnlyList<NotificationRoute> NotificationRoutes { get; set; } = Array.Empty<NotificationRoute>();

        public bool SmtpEnabled => true;
        public string SmtpServer => "127.0.0.1";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "monitor@example.invalid";
        public string SmtpRecipients => "ops@example.invalid;second@example.invalid";
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

    private sealed class NoHistory : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }

    private static EmailSendCore Core(Settings settings)
    {
        var webhooks = new WebhookAlertService(settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
        return new EmailSendCore(settings, new NoHistory(), webhooks, EmailAlertService.Branding, new AppLoggerAdapter<EmailSendCore>());
    }

    [Fact]
    public async Task ARouteThatTurnsEmailOff_SendsNoEmail_AndDoesNotFallBackToTheDefaultList()
    {
        var settings = new Settings
        {
            NotificationRoutes = new[]
            {
                new NotificationRoute(RouteId: 1, MetricMatch: "High CPU", TeamsUrl: "", SlackUrl: "", GenericUrl: "", PagerDutyRoutingKey: "",
                    SmtpRecipients: "", Enabled: true)
                {
                    OffChannels = new[] { NotificationRouter.EmailChannel },
                },
            },
        };
        var core = Core(settings);

        var off = await core.TrySendAsync("High CPU", "SQL01", "97%", "90%", serverId: "SQL01", context: null, attemptChannels: true);

        Assert.Equal(AlertChannelOutcome.NotAttempted, off.EmailOutcome);
        Assert.False(off.EmailAttempted);
        Assert.Null(off.SendError);
        Assert.NotNull(off.Route);
        Assert.Equal(1, off.Route!.Email.RouteId);
        Assert.Null(off.Route.Email.Destination);

        /* An alert the route does not cover still goes to the default list: here that list is refused by the send, so the
           attempt is a Failed outcome, which is the point: it was attempted. */
        var covered = await core.TrySendAsync("Low Disk Space", "SQL02", "1 GB", "5 GB", serverId: "SQL02", context: null, attemptChannels: true);

        Assert.Equal(AlertChannelOutcome.Failed, covered.EmailOutcome);
        Assert.NotNull(covered.Route);
        Assert.Null(covered.Route!.Email.RouteId);
    }
}
