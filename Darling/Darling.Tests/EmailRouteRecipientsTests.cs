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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4751: a notification route that names email recipients as its only destination never sent when the
/// default recipient list was blank. The send core's "is SMTP configured" gate, and Darling's
/// <c>SmtpEnabled</c>, both required the default list, so the branch that reads the route's recipients never
/// ran — while the route editor and the README say only the host, the from address and the credentials come
/// from the main settings.
///
/// <para>These drive <see cref="EmailSendCore"/> directly against a loopback SMTP endpoint, with the real
/// <see cref="DarlingAlertSettings"/> over a <see cref="DarlingConfig"/>: a covering route sends to its own
/// recipients; a firing nothing covers is skipped as "not attempted" rather than thrown at
/// <c>SendEmailAsync</c> (which throws on an empty list) and counted as a failed row; and that skip leaves
/// no trace, so the very next firing that IS covered sends.</para>
/// </summary>
public sealed class EmailRouteRecipientsTests
{
    private const string CoveredMetric = "Blocking Detected";
    private const string UncoveredMetric = "High CPU";
    private const string CoveringRecipients = "blocking-a@example.invalid, blocking-b@example.invalid";

    private static NotificationRoute EmailRoute(int id, string match, string recipients, bool enabled = true) =>
        new(id, match, "", "", "", "", recipients, enabled);

    [Fact]
    public async Task ARouteThatNamesRecipients_SendsToThem_WhenTheDefaultListIsBlank()
    {
        using var rig = new Rig(new[] { EmailRoute(1, CoveredMetric, CoveringRecipients) });

        var result = await rig.FireAsync(CoveredMetric);

        Assert.Equal(AlertChannelOutcome.Delivered, result.EmailOutcome);
        Assert.Null(result.SendError);
        Assert.True(result.AnyChannelConfigured);

        var raw = Assert.Single(rig.Smtp.RawMessages);
        Assert.Equal(
            new[] { "blocking-a@example.invalid", "blocking-b@example.invalid" },
            ToHeaderRecipients(raw));

        /* The routing record names the route that supplied the recipients. */
        Assert.NotNull(result.Route);
        Assert.Equal(1, result.Route!.Email.RouteId);
        Assert.Equal(RouteSource.Exact, result.Route.Email.Source);
        Assert.Equal((0, (string?)null), rig.Core.GetEmailHealth());
    }

    /// <summary>
    /// The negative half, and the one that would have turned silence into failed rows: SMTP is now "enabled"
    /// on host and from alone, so a firing no route covers reaches the recipient resolution with nobody to
    /// send to. It must end as <see cref="AlertChannelOutcome.NotAttempted"/> — not thrown at the send, not
    /// <see cref="AlertChannelOutcome.Failed"/>, not counted in the consecutive-failure health — and it must
    /// stamp no cooldown, or the next firing (once a route or the default list covers it) would be throttled
    /// for a window in which nothing was sent.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AFiringNoRouteCovers_IsNotAttempted_NotFailed_AndLeavesNoCooldown(bool coverWithARoute)
    {
        using var rig = new Rig(new[] { EmailRoute(1, CoveredMetric, CoveringRecipients) });

        var uncovered = await rig.FireAsync(UncoveredMetric);

        Assert.Equal(AlertChannelOutcome.NotAttempted, uncovered.EmailOutcome);
        Assert.Null(uncovered.SendError);
        Assert.Equal(AlertChannelOutcome.NotAttempted, uncovered.WebhookOutcome);
        Assert.True(uncovered.AnyChannelConfigured);
        Assert.Empty(rig.Smtp.RawMessages);
        Assert.Equal((0, (string?)null), rig.Core.GetEmailHealth());

        /* The routing record is kept: the alert log's route context still says what the firing resolved to,
           and that was no destination at all. */
        Assert.NotNull(uncovered.Route);
        Assert.Null(uncovered.Route!.Email.Destination);
        Assert.Equal(RouteSource.None, uncovered.Route.Email.Source);

        /* The stored row: a configured channel that nothing consulted. This is the `undelivered` shape a
           webhook-only set of routes already produced for an alert none of them covers. */
        var row = AlertDelivery.FromFanout(uncovered, muted: false, trayChannelPresent: false);
        Assert.Equal(AlertDelivery.ChannelUndelivered, row.Channel);
        Assert.Null(row.SendError);

        /* Now cover the same metric and fire again: delivered, which it could not be had the skip stamped. */
        string expectedRecipient;
        if (coverWithARoute)
        {
            expectedRecipient = "cpu@example.invalid";
            rig.Config.NotificationRoutes = new[]
            {
                EmailRoute(1, CoveredMetric, CoveringRecipients),
                EmailRoute(2, UncoveredMetric, expectedRecipient),
            };
        }
        else
        {
            expectedRecipient = "dba@example.invalid";
            rig.Config.Smtp.To = expectedRecipient;
        }

        var covered = await rig.FireAsync(UncoveredMetric);

        Assert.Equal(AlertChannelOutcome.Delivered, covered.EmailOutcome);
        var raw = Assert.Single(rig.Smtp.RawMessages);
        Assert.Equal(new[] { expectedRecipient }, ToHeaderRecipients(raw));
    }

    /// <summary>
    /// The repeat budget's half of "leaves no trace". A repeat under Summary delivery reserves the metric's
    /// window BEFORE the send, so a concurrent sibling folds instead of posting a second card; a send that
    /// delivers nothing must give the reservation back, or the metric's next repeat is folded into a card
    /// nobody was sent. The seeded history (a last email 20 minutes ago, against a 15 minute window) makes
    /// the firing a repeat rather than a first notice, which is what reserves.
    /// </summary>
    [Fact]
    public async Task AFiringNoRouteCovers_GivesBackTheRepeatWindow_SoTheNextFiringIsNotFolded()
    {
        var history = new SeededEmailHistoryStore(DateTime.UtcNow.AddMinutes(-20));
        using var rig = new Rig(new[] { EmailRoute(1, CoveredMetric, CoveringRecipients) }, history);
        Assert.Equal(15, rig.Settings.EmailCooldownMinutes);

        var uncovered = await rig.FireAsync(UncoveredMetric, AlertNotificationMode.Summary);
        Assert.Equal(AlertChannelOutcome.NotAttempted, uncovered.EmailOutcome);

        rig.Config.NotificationRoutes = new[]
        {
            EmailRoute(1, CoveredMetric, CoveringRecipients),
            EmailRoute(2, UncoveredMetric, "cpu@example.invalid"),
        };

        var covered = await rig.FireAsync(UncoveredMetric, AlertNotificationMode.Summary);

        Assert.Equal(AlertChannelOutcome.Delivered, covered.EmailOutcome);
        Assert.Single(rig.Smtp.RawMessages);
    }

    /// <summary>
    /// Where nobody is named anywhere — no default list and no enabled route with recipients — email is
    /// still not a configured channel, exactly as before: no routing record, no message, and a row that does
    /// not claim a configured channel went unused. A disabled route counts as if it were not there.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NoRecipientsAnywhere_IsNotAConfiguredChannel(bool withADisabledRoute)
    {
        using var rig = withADisabledRoute
            ? new Rig(new[] { EmailRoute(1, CoveredMetric, CoveringRecipients, enabled: false) })
            : new Rig();

        var result = await rig.FireAsync(CoveredMetric);

        Assert.Equal(AlertChannelOutcome.NotAttempted, result.EmailOutcome);
        Assert.False(result.AnyChannelConfigured);
        Assert.Null(result.Route);
        Assert.Empty(rig.Smtp.RawMessages);
        Assert.Equal((0, (string?)null), rig.Core.GetEmailHealth());
    }

    [Fact]
    public void SmtpEnabled_NeedsOnlyTheHostAndFrom_AndABlankDefaultListResolvesToNoDestination()
    {
        var config = new DarlingConfig();
        config.Smtp.Host = "mail.example.invalid";
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "";
        var settings = new DarlingAlertSettings(config);

        Assert.True(settings.SmtpEnabled);

        /* The router needed no change: a blank default arrives as SmtpRecipients, and the last arm of the
           per-channel resolution turns it into a null destination. Whitespace is blank. */
        var blank = NotificationRouter.Resolve(UncoveredMetric, settings.NotificationRoutes, settings);
        Assert.Null(blank.Email.Destination);
        Assert.Equal(RouteSource.None, blank.Email.Source);

        config.Smtp.To = "   ";
        var whitespace = NotificationRouter.Resolve(UncoveredMetric, settings.NotificationRoutes, settings);
        Assert.Null(whitespace.Email.Destination);
        Assert.Equal(RouteSource.None, whitespace.Email.Source);

        /* A covering route still supplies its own recipients, and a set default list is still the fall-through. */
        config.NotificationRoutes = new[] { EmailRoute(7, UncoveredMetric, "cpu@example.invalid") };
        var routed = NotificationRouter.Resolve(UncoveredMetric, settings.NotificationRoutes, settings);
        Assert.Equal("cpu@example.invalid", routed.Email.Destination);
        Assert.Equal(7, routed.Email.RouteId);

        config.Smtp.To = "dba@example.invalid";
        var fallThrough = NotificationRouter.Resolve(CoveredMetric, settings.NotificationRoutes, settings);
        Assert.Equal("dba@example.invalid", fallThrough.Email.Destination);
        Assert.Equal(RouteSource.Default, fallThrough.Email.Source);

        /* And without a from address it is still not enabled, whatever else is set. */
        config.Smtp.From = "";
        Assert.False(settings.SmtpEnabled);
    }

    [Fact]
    public void AnyRouteConfiguresEmail_CountsOnlyAnEnabledRouteThatNamesRecipients()
    {
        Assert.False(NotificationRouter.AnyRouteConfiguresEmail(null));
        Assert.False(NotificationRouter.AnyRouteConfiguresEmail(Array.Empty<NotificationRoute>()));

        /* A blank recipient column inherits, so it configures nothing; nor does a disabled route, nor a route
           that only names a webhook. */
        Assert.False(NotificationRouter.AnyRouteConfiguresEmail(new[] { EmailRoute(1, CoveredMetric, "   ") }));
        Assert.False(NotificationRouter.AnyRouteConfiguresEmail(new[] { EmailRoute(1, CoveredMetric, "a@example.invalid", enabled: false) }));
        Assert.False(NotificationRouter.AnyRouteConfiguresEmail(
            new[] { new NotificationRoute(1, CoveredMetric, "https://teams.example.invalid/x", "", "", "", "", true) }));

        Assert.True(NotificationRouter.AnyRouteConfiguresEmail(new[] { EmailRoute(1, CoveredMetric, "a@example.invalid") }));
        Assert.True(NotificationRouter.AnyRouteConfiguresEmail(new[]
        {
            EmailRoute(1, CoveredMetric, "", enabled: true),
            EmailRoute(2, UncoveredMetric, "b@example.invalid"),
        }));
    }

    /// <summary>The addresses on the top-level <c>To:</c> header of one raw SMTP DATA payload.</summary>
    private static IReadOnlyList<string> ToHeaderRecipients(string rawData)
    {
        var headerEnd = rawData.IndexOf("\r\n\r\n", StringComparison.Ordinal);
        var headers = headerEnd < 0 ? rawData : rawData.Substring(0, headerEnd);

        /* RFC 5322 unfolding: a CRLF followed by whitespace continues the previous line. */
        headers = Regex.Replace(headers, @"\r\n[ \t]+", " ");

        var to = headers.Split("\r\n").Single(l => l.StartsWith("To:", StringComparison.OrdinalIgnoreCase));
        return to.Substring("To:".Length)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    /// <summary>
    /// A send core over the real Darling settings, pointed at a loopback SMTP endpoint, with host and from
    /// set and the default recipient list blank unless a test says otherwise. <see cref="Config"/> stays live
    /// (the settings read it on every call), so a test can add a covering route or a default list between
    /// firings.
    /// </summary>
    private sealed class Rig : IDisposable
    {
        public Rig(NotificationRoute[]? routes = null, IAlertHistoryStore? history = null, string defaultRecipients = "")
        {
            Smtp = new CapturingSmtpEndpoint();
            Config = new DarlingConfig();
            Config.Smtp.Host = "127.0.0.1";
            Config.Smtp.Port = Smtp.Port;
            Config.Smtp.UseSsl = false;
            Config.Smtp.From = "monitor@example.invalid";
            Config.Smtp.To = defaultRecipients;
            Config.NotificationRoutes = routes ?? Array.Empty<NotificationRoute>();

            Settings = new DarlingAlertSettings(Config);
            var store = history ?? new DiscardingHistoryStore();
            var webhooks = new WebhookAlertService(
                Settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, store);
            Core = new EmailSendCore(Settings, store, webhooks, DarlingAlertDeliverer.Branding, NullLogger.Instance);
        }

        public CapturingSmtpEndpoint Smtp { get; }

        public DarlingConfig Config { get; }

        public DarlingAlertSettings Settings { get; }

        public EmailSendCore Core { get; }

        public Task<EmailFanoutResult> FireAsync(string metricName, AlertNotificationMode? deliveryMode = null) =>
            Core.TrySendAsync(
                metricName, "SRV1", "95%", "80%", "srv1", context: null, attemptChannels: true,
                deliveryMode: deliveryMode);

        public void Dispose() => Smtp.Dispose();
    }

    /// <summary>A history store whose email seed answers with one fixed last-sent time, for every key.</summary>
    private sealed class SeededEmailHistoryStore : IAlertHistoryStore
    {
        private readonly DateTime _lastEmailUtc;

        public SeededEmailHistoryStore(DateTime lastEmailUtc) => _lastEmailUtc = lastEmailUtc;

        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(_lastEmailUtc);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);
    }
}
